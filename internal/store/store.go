// Package store reads the keys registry from published versions.
//
// One implementation: App, over the schema this service owns. The comparison
// backends this repo used to carry — the canonical schema read directly, an
// in-process snapshot, and two embedded log-structured stores — were removed once
// versioned Postgres was decided on; the measurements they produced are in the
// README and their code is in git history.
//
// The interface stays because it is what let those exist side by side, and it is
// what a read replica or a read-side cache would implement next.
package store

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/lidofinance/keys-api-versioned/internal/model"
)

// Version lifecycle, following the shape lido-mev-monitoring already uses: a
// version is invisible while it is being written and becomes current by a status
// change, not by the arrival of its rows. That decouples how long a version takes
// to write from when readers start seeing it.
const (
	StatusBuilding  = "building"
	StatusPublished = "published"
	// StatusStale marks the head as no longer trusted to reflect the chain — a
	// resync was found to be needed. It stays served: its rows are still internally
	// consistent, and a consistent-but-known-stale answer beats both an error and
	// silently pretending it is fresh. The staleness travels to the client instead
	// of being hidden.
	StatusStale = "stale"
	// StatusSuperseded means a reorg took the version off the chain. Ordinary
	// succession does not use it: an older version stays published and readable by
	// an explicit pin for as long as retention keeps it.
	StatusSuperseded = "superseded"
)

// pinReader is the subset of a connection that pin resolution needs.
type pinReader interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// Store is a source of registry data.
type Store interface {
	// Snapshot runs fn against a consistent view of the registry.
	Snapshot(ctx context.Context, fn func(Snapshot) error) error
	Close()
}

// Snapshot is a consistent read view: every call inside one Snapshot sees the
// same execution layer block.
type Snapshot interface {
	ELMeta(ctx context.Context) (*model.ELBlockSnapshot, error)
	Modules(ctx context.Context, lowercaseAddresses []string) ([]model.StakingModule, error)
	// ModuleByRef resolves a module by numeric id or by contract address.
	ModuleByRef(ctx context.Context, ref string) (*model.StakingModule, error)
	// StreamKeys calls fn for every matching key of one module. The Key passed
	// to fn is reused between calls and must not be retained.
	StreamKeys(ctx context.Context, moduleAddress string, f model.KeysFilter, fn func(*model.Key) error) error
	KeysByPubkeys(ctx context.Context, moduleAddress string, pubkeys []string) ([]model.Key, error)
	// Operators returns a module's operators. ethereum-validators-monitoring reads
	// them for the names it labels validators with, so they are part of the surface
	// a consumer needs, not an extra.
	//
	// The comparison backends in this repo return ErrNotImplemented: they exist to
	// measure the key path against the canonical service and were never given an
	// operator store.
	Operators(ctx context.Context, moduleAddress string, f model.OperatorsFilter) ([]model.Operator, error)
}

// ErrNotImplemented is returned by a backend that does not serve part of the
// surface. The API answers 501 rather than pretending the answer is empty.
type ErrNotImplemented struct{ What string }

func (e ErrNotImplemented) Error() string { return e.What + " is not implemented by this backend" }

// ErrBadRequest is returned for a reference the store cannot interpret at all: a pin
// that is neither a block, a hash nor a label, or a module reference that is neither an
// id nor an address.
//
// It is separate from ErrNotFound because the consumer's correct reaction differs — one
// says "ask for something that exists", the other "ask properly" — and because without
// it these landed on the wrong statuses: a malformed pin fell through to 500, and a
// malformed module id answered 404 where the canonical service answers 400. Neither
// showed up in parity runs, since cmd/diff compares successful responses.
type ErrBadRequest struct {
	What string
	Ref  string
}

func (e ErrBadRequest) Error() string { return e.What }

// ErrNotFound is returned when a reference does not resolve: a module that does not
// exist, or a pin naming a version that was never published.
//
// The two need different words. A pin that never existed used to answer "module
// <block> is not supported", which points a consumer at a module problem it does not
// have — and the pin case is the one a lagging consumer meets, since a block number it
// took from meta.elBlockSnapshot is the head rather than a version's identity.
type ErrNotFound struct {
	Ref string
	// What names the kind of reference. Empty means a staking module, which is the
	// canonical service's own 404 and therefore the wording that has to stay put.
	What string
}

func (e ErrNotFound) Error() string {
	if e.What == "" {
		return "module " + e.Ref + " is not supported"
	}
	return e.What + " " + e.Ref + " is not a published version"
}

// ErrSuperseded is returned for a pinned read on a version that a reorg took
// off the canonical chain. The head fields let the consumer jump straight to
// the current version instead of polling to discover it.
type ErrSuperseded struct {
	Ref                  string
	HeadBlockNumber      int64
	HeadLastChangedBlock string
}

func (e ErrSuperseded) Error() string {
	return "snapshot " + e.Ref + " was superseded by a reorg"
}

// ErrPruned is returned for a pinned read on a version that fell out of the
// retention window. Distinct from ErrSuperseded: the data was valid, it is
// simply no longer kept.
type ErrPruned struct{ Ref string }

func (e ErrPruned) Error() string { return "snapshot " + e.Ref + " is no longer retained" }
