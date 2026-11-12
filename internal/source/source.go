// Package source is the seam between the service and whatever holds the keys
// (lidocore: staking router modules; static: a JSON file).
package source

import (
	"context"
	"fmt"

	"github.com/lidofinance/keys-api-versioned/internal/model"
)

// Provider reads registry state at a point on a chain. Every read except Head takes the Ref of
// the hash Head returned, so one cycle sees one branch.
type Provider interface {
	Name() string

	Head(ctx context.Context) (model.ELBlockSnapshot, error)

	BlockHashAt(ctx context.Context, block int64) (string, error)

	// Modules must keep (id, address) stable, and a module's Nonce must move iff its keys or
	// operators change: a module whose nonce did not move is never re-read.
	Modules(ctx context.Context, at Ref) ([]model.StakingModule, error)

	Operators(ctx context.Context, at Ref, module model.StakingModule) ([]model.Operator, error)

	// Keys must keep (operator index, key index) stable for a key's life: the writer diffs on it.
	// fn is called from the calling goroutine only.
	Keys(ctx context.Context, at Ref, module model.StakingModule, fn func(model.Key) error) error
}

// OperatorWatcher reports operator metadata changes (renames, reward addresses), which move no
// nonce. Implement only if it can be answered cheaply.
type OperatorWatcher interface {
	OperatorsChanged(ctx context.Context, module model.StakingModule, fromBlock, toBlock int64) (bool, error)
}

// GroupedOperatorWatcher answers OperatorWatcher's question for all modules in one round trip.
type GroupedOperatorWatcher interface {
	OperatorsChangedAny(ctx context.Context, modules []model.StakingModule, fromBlock, toBlock int64) (bool, error)
}

// SourceChangeReporter reports reading from different contracts (a redeploy or version bump can keep
// every nonce equal); reading the report clears it.
type SourceChangeReporter interface {
	SourceChanged() (string, bool)
}

// ChainIdentifier reports the chain id for /v1/status.
type ChainIdentifier interface {
	ChainID(ctx context.Context) (int64, error)
}

// FinalityReporter: reading everything at one finalized hash is as consistent as at head; what is
// forbidden is mixing a pinned block and a floating tag in one cycle.
type FinalityReporter interface {
	// A zero BlockNumber means the source has no finality to report.
	FinalizedBlock(ctx context.Context) (model.ELBlockSnapshot, error)
}

// Ref is the point a read happens at. A height alone does not pin the branch; the hash does.
type Ref struct {
	BlockHash   string
	BlockNumber int64
}

// ErrUnsupportedModule is an error, not a skip: consumers cannot tell "no keys" from "not read".
type ErrUnsupportedModule struct {
	ModuleID   int32
	ModuleType string
	Address    string
}

func (e ErrUnsupportedModule) Error() string {
	return fmt.Sprintf("module %d (%s) at %s: no reader for this module type",
		e.ModuleID, e.ModuleType, e.Address)
}
