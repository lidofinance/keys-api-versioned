package api

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/store"
)

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

// extOperator: canonical fields in order, then groupId. Absent = module has no group registry;
// null = in no group (on-chain NO_GROUP_ID is 0, which names no group); else the group.
type extOperator struct {
	model.Operator
	GroupID *model.GroupID `json:"groupId,omitempty"`
}

func renderOperators(r *http.Request, operators []model.Operator) any {
	if !onExtSurface(r) {
		return operators
	}
	out := make([]extOperator, len(operators))
	for i := range operators {
		out[i] = extOperator{Operator: operators[i], GroupID: operators[i].Group}
	}
	return out
}

func renderOperator(r *http.Request, operator model.Operator) any {
	if !onExtSurface(r) {
		return operator
	}
	return extOperator{Operator: operator, GroupID: operator.Group}
}

// operatorsOf maps "this backend has none" to 501, not an empty list.
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

func (s *Server) handleOperators(w http.ResponseWriter, r *http.Request) {
	type group struct {
		Operators any                 `json:"operators"`
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
			out.Data = append(out.Data, group{Operators: renderOperators(r, operators), Module: modules[i]})
		}

		return writeJSON(w, out)
	})
	if err != nil && !writeStoreError(w, err) {
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

func (s *Server) handleModuleOperators(w http.ResponseWriter, r *http.Request) {
	type data struct {
		Operators any                 `json:"operators"`
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
			Data: data{Operators: renderOperators(r, operators), Module: *module},
			Meta: meta{ELBlockSnapshot: elMeta},
		})
	})
	if err != nil && !writeStoreError(w, err) {
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

func (s *Server) handleModuleOperator(w http.ResponseWriter, r *http.Request) {
	type data struct {
		Operator any                 `json:"operator"`
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
			Data: data{Operator: renderOperator(r, operators[0]), Module: *module},
			Meta: meta{ELBlockSnapshot: elMeta},
		})
	})
	if err != nil && !writeStoreError(w, err) {
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

func asNotImplemented(err error) bool {
	var ni store.ErrNotImplemented
	return errors.As(err, &ni)
}
