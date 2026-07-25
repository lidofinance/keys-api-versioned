package api

import (
	"bufio"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/store"
)

// streamBufferSize is the write buffer for dump endpoints. Large enough to
// amortize syscalls on multi-hundred-megabyte responses, small enough that
// concurrent dumps do not dominate the resident set.
const streamBufferSize = 256 << 10

// parseFilter reads the query filters, rejecting the same inputs as canonical.
func parseFilter(r *http.Request, withAddresses bool) (model.KeysFilter, error) {
	var f model.KeysFilter
	q := r.URL.Query()

	if v := q.Get("used"); v != "" {
		switch v {
		case "true":
			t := true
			f.Used = &t
		case "false":
			t := false
			f.Used = &t
		default:
			return f, errBadRequest{[]string{"used must be a boolean value"}}
		}
	}

	if v := q.Get("operatorIndex"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return f, errBadRequest{[]string{"operatorIndex must be an integer number", "operatorIndex must not be less than 0"}}
		}
		idx := int32(n)
		f.OperatorIndex = &idx
	}

	if withAddresses {
		for _, v := range q["moduleAddresses"] {
			if v == "" {
				continue
			}
			f.ModuleAddress = append(f.ModuleAddress, strings.ToLower(v))
		}
	}

	return f, nil
}

type errBadRequest struct{ messages []string }

func (e errBadRequest) Error() string { return strings.Join(e.messages, ", ") }

// handleKeys streams every key of every module.
//
// Framing is byte-identical to the canonical service, including whitespace:
// `{ "meta": {...}, "data": [` … `]}`.
func (s *Server) handleKeys(w http.ResponseWriter, r *http.Request) {
	filter, err := parseFilter(r, true)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.(errBadRequest).messages)
		return
	}

	// Paging is opt-in: without limit or cursor this is the canonical full dump.
	page, err := parsePage(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.(errBadRequest).messages)
		return
	}
	if page.enabled {
		s.handleKeysPage(w, r, filter, page)
		return
	}

	ctx := r.Context()
	err = s.snapshot(ctx, r, func(snap store.Snapshot) error {
		markStale(w, snap)

		meta, err := snap.ELMeta(ctx)
		if err != nil {
			return err
		}
		if meta == nil {
			tooEarly(w)
			return nil
		}

		modules, err := snap.Modules(ctx, filter.ModuleAddress)
		if err != nil {
			return err
		}

		w.Header().Set("Content-Type", "application/json")
		bw := bufio.NewWriterSize(w, streamBufferSize)
		defer bw.Flush()

		bw.WriteString(`{ "meta": {"elBlockSnapshot":`)
		bw.Write(marshalCompact(meta))
		bw.WriteString(`}, "data": [`)

		first := true
		for i := range modules {
			err := snap.StreamKeys(ctx, modules[i].RawAddress(), filter, func(k *model.Key) error {
				if !first {
					bw.WriteByte(',')
				}
				first = false
				writeKey(bw, k)
				return nil
			})
			if err != nil {
				return err
			}
		}

		bw.WriteString(`]}`)
		return nil
	})
	if err != nil && !writeStoreError(w, err) {
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

// handleModuleKeys streams keys of a single module.
func (s *Server) handleModuleKeys(w http.ResponseWriter, r *http.Request) {
	filter, err := parseFilter(r, false)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.(errBadRequest).messages)
		return
	}

	ref := r.PathValue("module_id")
	ctx := r.Context()

	err = s.snapshot(ctx, r, func(snap store.Snapshot) error {
		module, err := snap.ModuleByRef(ctx, ref)
		if err != nil {
			var nf store.ErrNotFound
			if asNotFound(err, &nf) {
				writeError(w, http.StatusNotFound, "Module with moduleId "+ref+" is not supported")
				return nil
			}
			return err
		}

		markStale(w, snap)

		meta, err := snap.ELMeta(ctx)
		if err != nil {
			return err
		}
		if meta == nil {
			tooEarly(w)
			return nil
		}

		w.Header().Set("Content-Type", "application/json")
		bw := bufio.NewWriterSize(w, streamBufferSize)
		defer bw.Flush()

		bw.WriteString(`{ "meta": {"elBlockSnapshot":`)
		bw.Write(marshalCompact(meta))
		bw.WriteString(`}, "data": { "module": `)
		bw.Write(marshalCompact(module))
		bw.WriteString(`, "keys": [`)

		first := true
		err = snap.StreamKeys(ctx, module.RawAddress(), filter, func(k *model.Key) error {
			if !first {
				bw.WriteByte(',')
			}
			first = false
			writeKey(bw, k)
			return nil
		})
		if err != nil {
			return err
		}

		bw.WriteString(`]}}`)
		return nil
	})
	if err != nil && !writeStoreError(w, err) {
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

// handleGroupedKeys returns keys grouped by module.
//
// Streamed, unlike the canonical service, which builds the whole response in memory
// and dies at a 512 MiB limit on mainnet data. Ours did too for one commit: an
// earlier version collected every key into a slice, which only looked survivable
// while a bug left the hex strings empty — 511 877 keys with their pubkeys and
// signatures materialised is around half a gigabyte of strings.
//
// The bytes are identical to the collected version, envelope and property order
// included; only the memory is different.
func (s *Server) handleGroupedKeys(w http.ResponseWriter, r *http.Request) {
	filter, err := parseFilter(r, false)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.(errBadRequest).messages)
		return
	}

	ctx := r.Context()
	err = s.snapshot(ctx, r, func(snap store.Snapshot) error {
		markStale(w, snap)

		elMeta, err := snap.ELMeta(ctx)
		if err != nil {
			return err
		}
		if elMeta == nil {
			tooEarly(w)
			return nil
		}

		modules, err := snap.Modules(ctx, nil)
		if err != nil {
			return err
		}

		w.Header().Set("Content-Type", "application/json")
		bw := bufio.NewWriterSize(w, streamBufferSize)
		defer bw.Flush()

		bw.WriteString(`{"data":[`)
		for i := range modules {
			if i > 0 {
				bw.WriteByte(',')
			}
			bw.WriteString(`{"keys":[`)

			first := true
			if err := snap.StreamKeys(ctx, modules[i].RawAddress(), filter, func(k *model.Key) error {
				if !first {
					bw.WriteByte(',')
				}
				first = false
				writeKey(bw, k)
				return nil
			}); err != nil {
				return err
			}

			bw.WriteString(`],"module":`)
			bw.Write(marshalCompact(modules[i]))
			bw.WriteByte('}')
		}
		bw.WriteString(`],"meta":{"elBlockSnapshot":`)
		bw.Write(marshalCompact(elMeta))
		bw.WriteString(`}}`)
		return nil
	})
	if err != nil && !writeStoreError(w, err) {
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

type meta struct {
	ELBlockSnapshot *model.ELBlockSnapshot `json:"elBlockSnapshot"`
}

// handleKeyByPubkey returns every key matching one pubkey across modules.
func (s *Server) handleKeyByPubkey(w http.ResponseWriter, r *http.Request) {
	pubkey := strings.ToLower(r.PathValue("pubkey"))
	s.findKeys(w, r, []string{pubkey}, true)
}

// handleKeysFind returns keys matching a pubkey list across modules.
func (s *Server) handleKeysFind(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Pubkeys []string `json:"pubkeys"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, []string{"pubkeys must be an array"})
		return
	}
	s.findKeys(w, r, body.Pubkeys, false)
}

func (s *Server) findKeys(w http.ResponseWriter, r *http.Request, pubkeys []string, notFoundWhenEmpty bool) {
	type response struct {
		Data []model.Key `json:"data"`
		Meta meta        `json:"meta"`
	}

	ctx := r.Context()
	err := s.snapshot(ctx, r, func(snap store.Snapshot) error {
		markStale(w, snap)

		elMeta, err := snap.ELMeta(ctx)
		if err != nil {
			return err
		}
		if elMeta == nil {
			tooEarly(w)
			return nil
		}

		modules, err := snap.Modules(ctx, nil)
		if err != nil {
			return err
		}

		collected := make([]model.Key, 0, len(pubkeys))
		for i := range modules {
			keys, err := snap.KeysByPubkeys(ctx, modules[i].RawAddress(), pubkeys)
			if err != nil {
				return err
			}
			collected = append(collected, keys...)
		}

		if notFoundWhenEmpty && len(collected) == 0 {
			writeError(w, http.StatusNotFound,
				"There are no keys with "+strings.Join(pubkeys, ",")+" public key in db.")
			return nil
		}

		return writeJSON(w, response{Data: collected, Meta: meta{ELBlockSnapshot: elMeta}})
	})
	if err != nil && !writeStoreError(w, err) {
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

// handleModuleKeysFind returns keys of one module matching a pubkey list.
func (s *Server) handleModuleKeysFind(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Pubkeys []string `json:"pubkeys"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, []string{"pubkeys must be an array"})
		return
	}

	type data struct {
		Keys   []model.Key         `json:"keys"`
		Module model.StakingModule `json:"module"`
	}
	type response struct {
		Data data `json:"data"`
		Meta meta `json:"meta"`
	}

	ref := r.PathValue("module_id")
	ctx := r.Context()

	err := s.snapshot(ctx, r, func(snap store.Snapshot) error {
		module, err := snap.ModuleByRef(ctx, ref)
		if err != nil {
			var nf store.ErrNotFound
			if asNotFound(err, &nf) {
				writeError(w, http.StatusNotFound, "Module with moduleId "+ref+" is not supported")
				return nil
			}
			return err
		}

		markStale(w, snap)

		elMeta, err := snap.ELMeta(ctx)
		if err != nil {
			return err
		}
		if elMeta == nil {
			tooEarly(w)
			return nil
		}

		keys, err := snap.KeysByPubkeys(ctx, module.RawAddress(), body.Pubkeys)
		if err != nil {
			return err
		}
		if keys == nil {
			keys = []model.Key{}
		}

		return writeJSON(w, response{
			Data: data{Keys: keys, Module: *module},
			Meta: meta{ELBlockSnapshot: elMeta},
		})
	})
	if err != nil && !writeStoreError(w, err) {
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}
