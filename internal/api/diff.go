package api

import (
	"errors"
	"fmt"
	"net/http"
	"slices"

	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/store"
)

// A changed key appears in both removed and added (old row closed, new opened). Apply removals then
// additions by (moduleAddress, operatorIndex, index) and the result equals a fresh dump at `to`.

func (s *Server) handleKeysDiff(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	since := q.Get("since")
	if since == "" {
		writeError(w, http.StatusBadRequest, []string{"since is required: a block number, a block hash, or finalized"})
		return
	}

	differ, ok := s.store.(store.DiffStore)
	if !ok {
		writeError(w, http.StatusNotImplemented, "this backend keeps no version history")
		return
	}

	moduleRef := q.Get("module")
	if moduleRef == "" {
		moduleRef = r.PathValue("module_id")
	}

	ctx := r.Context()

	// Buffered so the counts can lead in meta. Usually kilobytes, but `since` at the oldest retained
	// version after a module rewrite is the whole registry twice, so the buffer is capped.
	var removed, added []model.Key
	meta, err := differ.DiffKeys(ctx, since, q.Get("to"), moduleRef, func(kind store.DiffKind, k *model.Key) error {
		if len(removed)+len(added) >= s.maxDiffKeys() {
			return errDiffTooLarge
		}
		copied := *k
		copied.Key = model.EncodeHex(k.PubkeyBin)
		copied.DepositSignature = model.EncodeHex(k.SignatureBin)
		copied.PubkeyBin, copied.SignatureBin = nil, nil

		if kind == store.DiffRemoved {
			removed = append(removed, copied)
		} else {
			added = append(added, copied)
		}
		return nil
	})
	if errors.Is(err, errDiffTooLarge) {
		writeStatusMessage(w, http.StatusRequestEntityTooLarge, fmt.Sprintf(
			"the diff holds more than %d keys; read a full dump at the newer version instead", s.maxDiffKeys()))
		return
	}
	if err != nil {
		if !writeStoreError(w, err) {
			writeError(w, http.StatusBadRequest, err.Error())
		}
		return
	}

	if removed == nil {
		removed = []model.Key{}
	}
	if added == nil {
		added = []model.Key{}
	}

	w.Header().Set("Content-Type", "application/json")
	bw, _ := s.newStreamWriter(w)
	defer bw.Flush()

	bw.WriteString(`{ "meta": `)
	bw.Write(marshalCompact(meta))
	bw.WriteString(`, "data": { "removed": `)
	bw.Write(marshalCompact(removed))
	bw.WriteString(`, "added": `)
	bw.Write(marshalCompact(added))
	bw.WriteString(`}}`)
}

const maxDiffKeys = 100_000

var errDiffTooLarge = errors.New("diff too large")

func (s *Server) maxDiffKeys() int {
	if s.cfg.MaxDiffKeys > 0 {
		return s.cfg.MaxDiffKeys
	}
	return maxDiffKeys
}

// handleModulesDiff is trustworthy where canonical lastChangedBlockHash is not: the stamp moves on
// content change, not on re-read.
func (s *Server) handleModulesDiff(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	since := q.Get("since")
	if since == "" {
		writeError(w, http.StatusBadRequest, []string{"since is required: a block number, a block hash, or finalized"})
		return
	}

	app, ok := s.store.(*store.App)
	if !ok {
		writeError(w, http.StatusNotImplemented, "this backend keeps no version history")
		return
	}

	changed, err := app.ModuleDiff(r.Context(), since, q.Get("to"))
	if err != nil {
		if !writeStoreError(w, err) {
			writeError(w, http.StatusBadRequest, err.Error())
		}
		return
	}

	ids := make([]int32, 0, len(changed))
	for id := range changed {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	writeJSON(w, map[string]any{"data": map[string]any{"changedModules": ids}})
}
