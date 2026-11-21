package api

import (
	"errors"
	"net/http"

	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/store"
)

func asNotFound(err error, target *store.ErrNotFound) bool {
	return errors.As(err, target)
}

// Canonical puts elBlockSnapshot at the top level here, not under meta.
func (s *Server) handleModules(w http.ResponseWriter, r *http.Request) {
	type response struct {
		Data            []model.StakingModule  `json:"data"`
		ELBlockSnapshot *model.ELBlockSnapshot `json:"elBlockSnapshot"`
	}

	ctx := r.Context()
	var body []byte
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
		if modules == nil {
			modules = []model.StakingModule{}
		}

		return deferJSON(&body, response{Data: modules, ELBlockSnapshot: elMeta})
	})
	if err != nil {
		if !writeStoreError(w, err) {
			s.internalError(w, r, err)
		}
		return
	}
	writeBody(w, body)
}

func (s *Server) handleModule(w http.ResponseWriter, r *http.Request) {
	type response struct {
		Data            model.StakingModule    `json:"data"`
		ELBlockSnapshot *model.ELBlockSnapshot `json:"elBlockSnapshot"`
	}

	ref := r.PathValue("module_id")
	ctx := r.Context()

	var body []byte
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

		return deferJSON(&body, response{Data: *module, ELBlockSnapshot: elMeta})
	})
	if err != nil {
		if !writeStoreError(w, err) {
			s.internalError(w, r, err)
		}
		return
	}
	writeBody(w, body)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var body []byte
	err := s.snapshot(ctx, r, func(snap store.Snapshot) error {
		markStale(w, snap)

		elMeta, err := snap.ELMeta(ctx)
		if err != nil {
			return err
		}

		return deferJSON(&body, model.Status{
			AppVersion:      s.cfg.AppVersion,
			ChainID:         s.cfg.ChainID,
			ELBlockSnapshot: elMeta,
			CLBlockSnapshot: nil,
		})
	})
	if err != nil {
		if !writeStoreError(w, err) {
			s.internalError(w, r, err)
		}
		return
	}
	writeBody(w, body)
}
