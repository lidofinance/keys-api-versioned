package api

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/store"
)

// The operator endpoints exist because consumers need them, not for completeness.
// ethereum-validators-monitoring reads /v1/operators for the names it labels every
// validator with, and it pairs them with /v1/keys?used=true; without operators it
// cannot attribute a validator to anyone.

// parseOperatorsFilter reads the operator query filters.
func parseOperatorsFilter(r *http.Request) (model.OperatorsFilter, error) {
	var f model.OperatorsFilter
	if v := r.URL.Query().Get("operatorIndex"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return f, errBadRequest{[]string{"operatorIndex must be an integer number", "operatorIndex must not be less than 0"}}
		}
		idx := int32(n)
		f.Index = &idx
	}
	return f, nil
}

// operatorsOf reads one module's operators, mapping the "this backend has none"
// case to a 501 rather than an empty list.
func operatorsOf(ctx context.Context, snap store.Snapshot, moduleAddress string, f model.OperatorsFilter) ([]model.Operator, error) {
	operators, err := snap.Operators(ctx, moduleAddress, f)
	if err != nil {
		return nil, err
	}
	if operators == nil {
		operators = []model.Operator{}
	}
	return operators, nil
}

// handleOperators returns every module's operators, grouped by module.
func (s *Server) handleOperators(w http.ResponseWriter, r *http.Request) {
	type group struct {
		Operators []model.Operator    `json:"operators"`
		Module    model.StakingModule `json:"module"`
	}
	type response struct {
		Data []group `json:"data"`
		Meta meta    `json:"meta"`
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

		out := response{Data: make([]group, 0, len(modules)), Meta: meta{ELBlockSnapshot: elMeta}}
		for i := range modules {
			operators, err := operatorsOf(ctx, snap, modules[i].RawAddress(), model.OperatorsFilter{})
			if err != nil {
				return err
			}
			out.Data = append(out.Data, group{Operators: operators, Module: modules[i]})
		}

		return writeJSON(w, out)
	})
	if err != nil && !writeStoreError(w, err) {
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

// handleModuleOperators returns one module's operators.
func (s *Server) handleModuleOperators(w http.ResponseWriter, r *http.Request) {
	type data struct {
		Operators []model.Operator    `json:"operators"`
		Module    model.StakingModule `json:"module"`
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

		operators, err := operatorsOf(ctx, snap, module.RawAddress(), model.OperatorsFilter{})
		if err != nil {
			return err
		}

		return writeJSON(w, response{
			Data: data{Operators: operators, Module: *module},
			Meta: meta{ELBlockSnapshot: elMeta},
		})
	})
	if err != nil && !writeStoreError(w, err) {
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

// handleModuleOperator returns one operator of one module.
func (s *Server) handleModuleOperator(w http.ResponseWriter, r *http.Request) {
	type data struct {
		Operator model.Operator      `json:"operator"`
		Module   model.StakingModule `json:"module"`
	}
	type response struct {
		Data data `json:"data"`
		Meta meta `json:"meta"`
	}

	ref := r.PathValue("module_id")
	operatorRef := r.PathValue("operator_id")

	index, err := strconv.Atoi(operatorRef)
	if err != nil || index < 0 {
		writeError(w, http.StatusBadRequest, []string{"operator_id must be an integer number"})
		return
	}
	wanted := int32(index)

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

		operators, err := operatorsOf(ctx, snap, module.RawAddress(), model.OperatorsFilter{Index: &wanted})
		if err != nil {
			return err
		}
		if len(operators) == 0 {
			writeError(w, http.StatusNotFound,
				"Operator with index "+operatorRef+" is not found for module with moduleId "+ref)
			return nil
		}

		return writeJSON(w, response{
			Data: data{Operator: operators[0], Module: *module},
			Meta: meta{ELBlockSnapshot: elMeta},
		})
	})
	if err != nil && !writeStoreError(w, err) {
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

// asNotImplemented reports whether err is a backend capability gap.
func asNotImplemented(err error) bool {
	var ni store.ErrNotImplemented
	return errors.As(err, &ni)
}
