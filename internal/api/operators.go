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

// extOperator is an operator as the extended surface renders it: every canonical
// field in its canonical order, then the node operator group.
//
// A field rather than a whole endpoint, and on /ext/v1 rather than on /v1, because
// those two surfaces promise different things. /v1 is the canonical service's
// contract byte for byte — a field it does not send is a difference a parity run
// would flag and a strict consumer could choke on — while /ext/v1 is where this
// service's own additions live. A consumer that wants groups changes one prefix.
//
// Three states, because two of them are different facts:
//
//	field absent      the module has no group registry. Groups do not exist for
//	                  modules 1–3, and `"groupId": null` there would read as "this
//	                  operator was left out of one".
//	"groupId": null   the module has groups and this operator is in none. On chain
//	                  that is NO_GROUP_ID, which is 0 — a number that names no
//	                  group, so serving it would be worse than serving nothing.
//	"groupId": 17     the group the operator belongs to.
type extOperator struct {
	model.Operator
	GroupID *model.GroupID `json:"groupId,omitempty"`
}

// renderOperators picks the representation for the surface a request came in on.
//
// The return is `any` because the two shapes are different types; the alternative,
// one struct with a flag the marshaller reads, needs a custom MarshalJSON for the
// whole operator and would put the surface rule somewhere no one looks.
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

// renderOperator is renderOperators for the single-operator endpoint.
func renderOperator(r *http.Request, operator model.Operator) any {
	if !onExtSurface(r) {
		return operator
	}
	return extOperator{Operator: operator, GroupID: operator.Group}
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

// handleModuleOperators returns one module's operators.
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

// handleModuleOperator returns one operator of one module.
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

// asNotImplemented reports whether err is a backend capability gap.
func asNotImplemented(err error) bool {
	var ni store.ErrNotImplemented
	return errors.As(err, &ni)
}
