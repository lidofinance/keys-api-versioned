package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/store"
)

// Large enough to amortize syscalls on multi-hundred-MiB dumps, small enough that concurrent
// dumps do not dominate RSS.
const streamBufferSize = 256 << 10

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

// handleKeys framing is byte-identical to canonical, whitespace included.
func (s *Server) handleKeys(w http.ResponseWriter, r *http.Request) {
	filter, err := parseFilter(r, true)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.(errBadRequest).messages)
		return
	}

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
		bw, out := s.newStreamWriter(w)
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
				return out.Err()
			})
			if err != nil {
				return err
			}
		}

		bw.WriteString(`]}`)
		return nil
	})
	if err != nil && !writeStoreError(w, err) {
		s.internalError(w, r, err)
	}
}

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
		bw, out := s.newStreamWriter(w)
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
			return out.Err()
		})
		if err != nil {
			return err
		}

		bw.WriteString(`]}}`)
		return nil
	})
	if err != nil && !writeStoreError(w, err) {
		s.internalError(w, r, err)
	}
}

// handleGroupedKeys streams; canonical builds it in memory and dies at a 512 MiB limit on mainnet
// (~512k keys materialised as strings is about half a gigabyte).
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
		bw, out := s.newStreamWriter(w)
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
				return out.Err()
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
		s.internalError(w, r, err)
	}
}

type meta struct {
	ELBlockSnapshot *model.ELBlockSnapshot `json:"elBlockSnapshot"`
}

// Shape is validated before lookup, as canonical does: malformed is 400, well-formed unknown is
// 404. The message is canonical's own.
const badPubkeyMessage = "Each pubkey must be a valid 0x-prefixed 48-byte hex string"

func validPubkey(s string) bool {
	if !strings.HasPrefix(s, "0x") || len(s) != 2+2*48 {
		return false
	}
	for _, c := range s[2:] {
		isHex := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
		if !isHex {
			return false
		}
	}
	return true
}

func (s *Server) handleKeyByPubkey(w http.ResponseWriter, r *http.Request) {
	pubkey := strings.ToLower(r.PathValue("pubkey"))
	if !validPubkey(pubkey) {
		writeError(w, http.StatusBadRequest, []string{badPubkeyMessage})
		return
	}
	s.findKeys(w, r, []string{pubkey}, true)
}

func (s *Server) handleKeysFind(w http.ResponseWriter, r *http.Request) {
	pubkeys, ok := decodeFindBody(w, r)
	if !ok {
		return
	}
	s.findKeys(w, r, pubkeys, false)
}

// maxFindBodyBytes is Fastify's default body limit, the canonical service's.
const maxFindBodyBytes = 1 << 20

// One valid element is at least 101 bytes (a quoted pubkey and a comma), so under the body limit
// this cap is never reached by valid input; it holds if the limit is raised.
const maxFindPubkeys = maxFindBodyBytes/101 + 1

// decodeFindBody validates like canonical's class-validator, reporting every failed constraint at
// once; an empty list or wrong field used to answer 200 with no keys.
func decodeFindBody(w http.ResponseWriter, r *http.Request) ([]string, bool) {
	if r.ContentLength > maxFindBodyBytes {
		bodyTooLarge(w)
		return nil, false
	}
	var body struct {
		Pubkeys *[]string `json:"pubkeys"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxFindBodyBytes)).Decode(&body); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			bodyTooLarge(w)
			return nil, false
		}
		writeError(w, http.StatusBadRequest, []string{"pubkeys must be an array"})
		return nil, false
	}
	if body.Pubkeys == nil {
		// Missing or the wrong field name: every constraint on the field failed, and
		// canonical reports all three.
		writeError(w, http.StatusBadRequest, []string{
			badPubkeyMessage,
			"pubkeys must contain at least 1 elements",
			"pubkeys must be an array",
		})
		return nil, false
	}
	pubkeys := *body.Pubkeys
	if len(pubkeys) == 0 {
		writeError(w, http.StatusBadRequest, []string{"pubkeys must contain at least 1 elements"})
		return nil, false
	}
	for _, pubkey := range pubkeys {
		// Case-sensitive prefix, as canonical's startsWith('0x').
		if !validPubkey(pubkey) {
			writeError(w, http.StatusBadRequest, []string{badPubkeyMessage})
			return nil, false
		}
	}
	if len(pubkeys) > maxFindPubkeys {
		bodyTooLarge(w)
		return nil, false
	}
	return pubkeys, true
}

// bodyTooLarge is what NestJS makes of Fastify's body-limit error.
func bodyTooLarge(w http.ResponseWriter) {
	writeStatusMessage(w, http.StatusRequestEntityTooLarge, "Request body is too large")
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
		s.internalError(w, r, err)
	}
}

func (s *Server) handleModuleKeysFind(w http.ResponseWriter, r *http.Request) {
	pubkeys, ok := decodeFindBody(w, r)
	if !ok {
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

		keys, err := snap.KeysByPubkeys(ctx, module.RawAddress(), pubkeys)
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
		s.internalError(w, r, err)
	}
}
