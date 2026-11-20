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

// TailKeyReader reads each operator's keys from an offset on. Deposited keys cannot change, so a
// writer that already holds them only needs the rest.
type TailKeyReader interface {
	// KeysFrom reads keys at index >= from[operator] (absent = 0) and returns the offsets it
	// applied. An offset above the operator's deposited count at `at` is not trusted: that operator
	// is read in full and its applied offset is 0.
	KeysFrom(ctx context.Context, at Ref, module model.StakingModule, from map[int32]int32, fn func(model.Key) error) (map[int32]int32, error)
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

// StakingLimitVetting marks a source whose keys are vetted exactly when their index is below the
// operator's StakingLimit. A writer can then correct the flags after an operator read alone: a
// module can lower the vetted count without moving its nonce.
type StakingLimitVetting interface {
	VettedBelowStakingLimit()
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

// ChainVerifier: an error for an endpoint on another chain, else how many are on the right one.
type ChainVerifier interface {
	VerifyChain(ctx context.Context) (verified int, err error)
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
