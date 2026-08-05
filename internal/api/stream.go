package api

import (
	"bufio"
	"net/http"

	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/store"
)

// The two streams here are what lido-mev-monitoring reads. Its registry sync pulls
// /v2/modules/operators/keys — one response carrying every module, every operator
// and every key — and parses it incrementally with stream-json. The v1 per-module
// variant is the same data scoped to one module.
//
// The framing matches the canonical service down to the separators, because the
// consumer parses a stream and any shape difference shows up as a parse error
// rather than as a bad value. Canonical builds these with JSONStream, whose
// defaults are "[\n", "\n,\n" and "\n]".

const (
	streamOpen  = "[\n"
	streamSep   = "\n,\n"
	streamClose = "\n]"
)

// handleModuleOperatorsKeys streams one module with its operators and keys.
func (s *Server) handleModuleOperatorsKeys(w http.ResponseWriter, r *http.Request) {
	filter, err := parseFilter(r, false)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.(errBadRequest).messages)
		return
	}
	operatorsFilter, err := parseOperatorsFilter(r)
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

		elMeta, err := snap.ELMeta(ctx)
		if err != nil {
			return err
		}
		if elMeta == nil {
			tooEarly(w)
			return nil
		}

		operators, err := operatorsOf(ctx, snap, module.RawAddress(), operatorsFilter)
		if err != nil {
			return err
		}

		w.Header().Set("Content-Type", "application/json")
		bw := bufio.NewWriterSize(w, streamBufferSize)
		defer bw.Flush()

		bw.WriteString(`{ "meta": {"elBlockSnapshot":`)
		bw.Write(marshalCompact(elMeta))
		bw.WriteString(`}, "data": { "module": `)
		bw.Write(marshalCompact(module))
		bw.WriteString(`, "operators": `)
		bw.Write(marshalCompact(renderOperators(r, operators)))
		bw.WriteString(`, "keys": [`)

		first := true
		if err := snap.StreamKeys(ctx, module.RawAddress(), filter, func(k *model.Key) error {
			if !first {
				bw.WriteByte(',')
			}
			first = false
			writeKey(bw, k)
			return nil
		}); err != nil {
			return err
		}

		bw.WriteString(`]}}`)
		return nil
	})
	if err != nil && !writeStoreError(w, err) {
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

// handleModulesOperatorsKeysStream streams everything as one array of records.
//
// The record shape is fixed and mostly null: the first record of a module carries
// the module and, once, the meta; then one record per key; then one per operator.
// It is a strange shape, and it is the shape mev-monitoring already parses, so it
// stays exactly as it is.
func (s *Server) handleModulesOperatorsKeysStream(w http.ResponseWriter, r *http.Request) {
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

		w.Header().Set("Content-Type", "application/json")
		bw := bufio.NewWriterSize(w, streamBufferSize)
		defer bw.Flush()

		bw.WriteString(streamOpen)

		first := true
		next := func() {
			if !first {
				bw.WriteString(streamSep)
			}
			first = false
		}

		metaSent := false
		for i := range modules {
			next()
			bw.WriteString(`{"stakingModule":`)
			bw.Write(marshalCompact(modules[i]))
			bw.WriteString(`,"meta":`)
			if metaSent {
				bw.WriteString(`null`)
			} else {
				bw.WriteString(`{"elBlockSnapshot":`)
				bw.Write(marshalCompact(elMeta))
				bw.WriteByte('}')
				metaSent = true
			}
			bw.WriteString(`,"key":null,"operator":null}`)

			if err := snap.StreamKeys(ctx, modules[i].RawAddress(), model.KeysFilter{}, func(k *model.Key) error {
				next()
				bw.WriteString(`{"stakingModule":null,"meta":null,"key":`)
				writeKey(bw, k)
				bw.WriteString(`,"operator":null}`)
				return nil
			}); err != nil {
				return err
			}

			operators, err := operatorsOf(ctx, snap, modules[i].RawAddress(), model.OperatorsFilter{})
			if err != nil {
				return err
			}
			for j := range operators {
				next()
				bw.WriteString(`{"stakingModule":null,"meta":null,"key":null,"operator":`)
				bw.Write(marshalCompact(renderOperator(r, operators[j])))
				bw.WriteByte('}')
			}
		}

		bw.WriteString(streamClose)
		return nil
	})
	if err != nil && !writeStoreError(w, err) {
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}
