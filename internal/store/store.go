// Package store reads the keys registry from published versions.
package store

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/lidofinance/keys-api-versioned/internal/model"
)

// A version is invisible while written and becomes current by a status change, not by the
// arrival of its rows.
const (
	StatusBuilding  = "building"
	StatusPublished = "published"
	// StatusStale: a resync is needed, but it stays served; rows are still consistent, and the
	// staleness travels to the client.
	StatusStale = "stale"
	// StatusSuperseded means a reorg took the version off the chain; ordinary succession keeps
	// older versions published.
	StatusSuperseded = "superseded"
)

type pinReader interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

type Store interface {
	Snapshot(ctx context.Context, fn func(Snapshot) error) error
	Close()
}

// Snapshot is a consistent read view: every call sees the same execution layer block.
type Snapshot interface {
	ELMeta(ctx context.Context) (*model.ELBlockSnapshot, error)
	Modules(ctx context.Context, lowercaseAddresses []string) ([]model.StakingModule, error)
	ModuleByRef(ctx context.Context, ref string) (*model.StakingModule, error)
	// The Key passed to fn is reused between calls and must not be retained.
	StreamKeys(ctx context.Context, moduleAddress string, f model.KeysFilter, fn func(*model.Key) error) error
	KeysByPubkeys(ctx context.Context, moduleAddress string, pubkeys []string) ([]model.Key, error)
	Operators(ctx context.Context, moduleAddress string, f model.OperatorsFilter) ([]model.Operator, error)
}

// ErrNotImplemented answers 501 rather than pretending the answer is empty.
type ErrNotImplemented struct{ What string }

func (e ErrNotImplemented) Error() string { return e.What + " is not implemented by this backend" }

// ErrBadRequest is an uninterpretable pin or module ref (400), distinct from ErrNotFound; parity
// runs miss these statuses because they compare successful responses only.
type ErrBadRequest struct {
	What string
	Ref  string
}

func (e ErrBadRequest) Error() string { return e.What }

// ErrNotFound covers a missing module or a pin that was never a published version; a lagging
// consumer meets the latter by pinning meta.elBlockSnapshot, which is the head.
type ErrNotFound struct {
	Ref string
	// Empty What means a staking module: the canonical 404 wording, which must stay put.
	What string
}

func (e ErrNotFound) Error() string {
	if e.What == "" {
		return "module " + e.Ref + " is not supported"
	}
	return e.What + " " + e.Ref + " is not a published version"
}

// ErrSuperseded carries the head so a consumer can jump to the current version without polling.
type ErrSuperseded struct {
	Ref                  string
	HeadBlockNumber      int64
	HeadLastChangedBlock string
}

func (e ErrSuperseded) Error() string {
	return "snapshot " + e.Ref + " was superseded by a reorg"
}

// ErrPruned: the version was valid but fell out of retention (unlike ErrSuperseded).
type ErrPruned struct{ Ref string }

func (e ErrPruned) Error() string { return "snapshot " + e.Ref + " is no longer retained" }
