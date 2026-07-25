// Package source is the seam between the service and whatever holds the keys.
//
// Everything above this package — versioning, freshness, retention, the HTTP
// surface — is written against Provider and knows nothing about contracts,
// chains or ABIs. Everything below it is one implementation:
//
//	source/lidocore  Lido core: staking router modules, read from an execution node
//	source/static    a JSON file, for development and tests
//
// A different key registry — stVaults, whose keys hang off vaults rather than
// staking modules — is another implementation of this interface. It reads its own
// contracts, and it writes through the same version writer into the same schema,
// so consumers see one abstraction whatever produced it: modules, their
// operators, their keys. That is what lets ethereum-validators-monitoring, the
// head watcher and mev-monitoring point at either without knowing the difference.
//
// # What a new source has to guarantee
//
// The contract is small, and three of the four items exist because the layers
// above trade correctness for cheapness and need the source to hold up its end:
//
//  1. A module's Nonce is the per-module change counter. It must move whenever
//     that module's keys or operators change, and it must not move otherwise.
//     This is the whole freshness mechanism: one cycle reads nonces and nothing
//     else, and a module whose nonce did not move is not re-read. A source
//     without a natural counter supplies its own — a validator count, a digest
//     folded into an integer — as long as it obeys that rule. Consumers already
//     rely on it too: ethereum-validators-monitoring sums the nonces to decide
//     whether to refetch at all.
//  2. Module identity is stable. The pair (id, address) is what versions,
//     operators and keys are keyed by; a source that renumbers modules between
//     cycles makes every version look like a full rewrite.
//  3. Key identity within a module is (operator index, key index), and it is
//     stable for the life of the key. The writer diffs on it, so an unstable
//     index turns a one-key change into a whole-module rewrite.
//  4. Reads at a Ref are consistent. One cycle passes one block hash to every
//     call, and the source must answer as of that point rather than as of now,
//     or a version will mix two states of the world.
//
// Nothing in the interface requires an execution layer as such. A source that has
// no chain still has to name the point it read at — see source/static, which
// derives one from the content — because a version's identity is what pins,
// reorg detection and retention are all built on.
package source

import (
	"context"
	"fmt"

	"github.com/lidofinance/keys-api-versioned/internal/model"
)

// Provider reads registry state at a point on a chain.
//
// Every read except Head takes a Ref, and callers always pass the block hash
// they got from Head, so one cycle sees one branch even if the chain moves
// underneath it.
type Provider interface {
	// Name identifies the implementation in logs and metrics.
	Name() string

	// Head returns the block a cycle should read at, with its timestamp.
	Head(ctx context.Context) (model.ELBlockSnapshot, error)

	// BlockHashAt returns the canonical hash at a height, which is how the sync
	// loop notices that a version it wrote is no longer on the chain.
	BlockHashAt(ctx context.Context, block int64) (string, error)

	// Modules returns the module set at a block, nonces included. This is the
	// cheap per-cycle read: a handful of calls, no keys.
	Modules(ctx context.Context, at Ref) ([]model.StakingModule, error)

	// Operators returns the operators of one module.
	Operators(ctx context.Context, at Ref, module model.StakingModule) ([]model.Operator, error)

	// Keys streams the keys of one module. The callback may be invoked from the
	// calling goroutine only.
	Keys(ctx context.Context, at Ref, module model.StakingModule, fn func(model.Key) error) error
}

// OperatorWatcher is an optional Provider capability: report whether operator
// metadata changed in a block range, without reading operators.
//
// Nonce covers keys and deposits, but not an operator being renamed or its
// reward address changing — those move no nonce and still have to be picked up,
// because consumers read operator names. A provider that cannot answer cheaply
// should not implement this; the sync loop then falls back to re-reading
// operators on every rebuild only.
type OperatorWatcher interface {
	OperatorsChanged(ctx context.Context, module model.StakingModule, fromBlock, toBlock int64) (bool, error)
}

// ChainIdentifier is an optional Provider capability: the chain id to report in
// /v1/status. A provider without one leaves the configured value alone.
type ChainIdentifier interface {
	ChainID(ctx context.Context) (int64, error)
}

// Ref identifies the point a read happens at. A hash pins the branch; a height
// alone does not, so the sync loop always carries the hash.
type Ref struct {
	BlockHash   string
	BlockNumber int64
}

// ErrUnsupportedModule is returned for a module the provider cannot read.
//
// It is deliberately an error rather than a silent skip: a version missing a
// module's keys is worse than no version at all, because consumers cannot tell
// "this operator has no keys" from "we could not read them".
type ErrUnsupportedModule struct {
	ModuleID   int32
	ModuleType string
	Address    string
}

func (e ErrUnsupportedModule) Error() string {
	return fmt.Sprintf("module %d (%s) at %s: no reader for this module type",
		e.ModuleID, e.ModuleType, e.Address)
}
