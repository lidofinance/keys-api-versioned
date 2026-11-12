package store

import "context"

// PinnedStore serves a request at an explicit version, answering 409/410 if it is gone rather
// than silently serving a different one.
type PinnedStore interface {
	Store
	PinnedSnapshot(ctx context.Context, pin string, fn func(Snapshot) error) error
}

type ErrPinUnsupported struct{}

func (ErrPinUnsupported) Error() string { return "this backend serves only the current version" }
