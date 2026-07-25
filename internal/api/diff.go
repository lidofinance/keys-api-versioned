package api

import (
	"bufio"
	"net/http"
	"slices"

	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/store"
)

// The diff endpoint is the one that changes what a consumer costs. Today a poller
// that wants to stay current re-downloads the whole dump — 211 MiB on mainnet — for
// a chain that usually changed nothing and occasionally changed a handful of keys.
// With `since` it downloads the handful.
//
// The contract is deliberately small:
//
//	GET /v1/keys/diff?since=<version>[&to=<version>][&module=<id|address>]
//
//	{ "meta": { "from": {…}, "to": {…}, "removed": N, "added": M },
//	  "data": { "removed": [ … ], "added": [ … ] } }
//
// A key whose value changed appears on both sides, because that is what the storage
// did: the old row was closed and a new one opened. Apply removals then additions by
// key identity — (moduleAddress, operatorIndex, index) — and the result equals a
// fresh dump at `to`.

// handleKeysDiff serves what changed between two versions.
func (s *Server) handleKeysDiff(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	since := q.Get("since")
	if since == "" {
		writeError(w, http.StatusBadRequest, []string{"since is required: a block number, a block hash, or safe"})
		return
	}

	differ, ok := s.store.(store.DiffStore)
	if !ok {
		writeError(w, http.StatusNotImplemented, "this backend keeps no version history")
		return
	}

	// The module filter is the common case, not an afterthought: a consumer polls
	// /v1/modules, sees one module's nonce move, and asks for that module's changes.
	moduleRef := q.Get("module")
	if moduleRef == "" {
		moduleRef = r.PathValue("module_id")
	}

	ctx := r.Context()

	// Both sides are buffered before anything is written, because the counts belong
	// in the meta block at the front and a diff is small by construction — that is
	// the whole point of the endpoint. A diff large enough to matter here means the
	// consumer is better off with the dump, and the response says so via the counts.
	var removed, added []model.Key
	meta, err := differ.DiffKeys(ctx, since, q.Get("to"), moduleRef, func(kind store.DiffKind, k *model.Key) error {
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
	bw := bufio.NewWriterSize(w, streamBufferSize)
	defer bw.Flush()

	bw.WriteString(`{ "meta": `)
	bw.Write(marshalCompact(meta))
	bw.WriteString(`, "data": { "removed": `)
	bw.Write(marshalCompact(removed))
	bw.WriteString(`, "added": `)
	bw.Write(marshalCompact(added))
	bw.WriteString(`}}`)
}

// handleModulesDiff reports which modules changed between two versions.
//
// This is the cheap poll a consumer should do first: a kilobyte that says which
// modules are worth asking about. It is trustworthy here in a way the canonical
// per-module lastChangedBlockHash is not, because the writer moves that stamp from
// content rather than from the act of re-reading.
func (s *Server) handleModulesDiff(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	since := q.Get("since")
	if since == "" {
		writeError(w, http.StatusBadRequest, []string{"since is required: a block number, a block hash, or safe"})
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
	// Sorted so the response is stable between calls.
	slices.Sort(ids)

	writeJSON(w, map[string]any{"data": map[string]any{"changedModules": ids}})
}
