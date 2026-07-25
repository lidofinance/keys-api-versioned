package store

import "context"

// PinnedStore is a store that can serve one request at an explicit version.
//
// The pin is what makes a consumer's multi-request work safe: a dump, then a
// follow-up query, then a page, all against the same version, with a definite
// answer (409 or 410) if that version is gone rather than a silently different one.
// Backends that hold only the current state do not implement it.
type PinnedStore interface {
	Store
	PinnedSnapshot(ctx context.Context, pin string, fn func(Snapshot) error) error
}

// ErrPinUnsupported is returned when a request asks for a version but the backend
// keeps no history.
type ErrPinUnsupported struct{}

func (ErrPinUnsupported) Error() string { return "this backend serves only the current version" }
