// Package ingest keeps a store in step with a source.
package ingest

import (
	"context"
	"errors"
	"fmt"

	"github.com/lidofinance/keys-api-versioned/internal/model"
)

type Store interface {
	Migrate(ctx context.Context) error

	// Latest returns the newest published or stale version, nil when the store is empty.
	Latest(ctx context.Context) (*StoredVersion, error)

	// Recent returns published and stale versions below block, newest first.
	Recent(ctx context.Context, below int64, limit int) ([]StoredVersion, error)

	// Begin opens a version invisible to readers until Publish. Rows left open by the previous
	// version stay open, so a scoped rebuild inherits every other module for free.
	Begin(ctx context.Context, at model.ELBlockSnapshot) (Build, error)

	// Extend advances the observed block only: data and lastChangedBlockHash stay. One row update.
	Extend(ctx context.Context, v StoredVersion, at model.ELBlockSnapshot, nonces map[string]int64, finalized int64) error

	// MarkStale keeps the version served, with the staleness surfaced to the client.
	MarkStale(ctx context.Context, v StoredVersion) error

	// SupersedeFrom takes versions at or above block off the chain and rolls row ranges back to it.
	SupersedeFrom(ctx context.Context, block int64) (Superseded, error)

	Prune(ctx context.Context, keep int) (Pruned, error)

	Close()
}

// Build is one version being filled. Per module: keys, operators, then the module row, whose
// lastChangedBlockHash depends on whether the first two moved.
type Build interface {
	Block() int64
	BlockHash() string

	Keys(ctx context.Context, module model.StakingModule) (ModuleKeys, error)

	// Operators reports how many rows actually changed.
	Operators(ctx context.Context, module model.StakingModule, operators []model.Operator) (int, error)

	// Module moves lastChangedBlockHash only when content or fields changed, unlike the canonical
	// service, which stamps every re-read module with the same block after a restart.
	Module(ctx context.Context, m model.StakingModule, contentChanged bool) error

	// Publish makes the version current with one status change; finalized is 0 when unknown.
	Publish(ctx context.Context, label string, finalized int64) error

	// Abort leaves written rows behind: they are invisible and retention collects them.
	Abort(ctx context.Context)
}

type ModuleKeys interface {
	Add(ctx context.Context, k model.Key) error
	// Commit closes changed or gone rows and opens new or changed ones; identical rows are untouched.
	Commit(ctx context.Context) (KeyDiff, error)
	Abort(ctx context.Context)
}

// SettledKeys (optional on Build) lets a rebuild read only what follows each operator's deposited
// keys. A key deposited at a finalized block cannot change on chain; the periodic audit still
// reads everything, so a wrong row cannot outlive it.
type SettledKeys interface {
	// SettledKeys returns, per operator, n such that the current rows for keys 0..n-1 are all
	// deposited and were written at a block at or below finalized.
	SettledKeys(ctx context.Context, module model.StakingModule, finalized int64) (map[int32]int32, error)
}

// PrefixKeeper (optional on ModuleKeys): rows for keys below keep[operator] were not read this
// time, so Commit leaves them open instead of closing them as gone.
type PrefixKeeper interface {
	KeepPrefix(keep map[int32]int32)
}

// PublishedModules (optional on Store) returns the module rows a version serves. A writer that just
// started compares them with what the router reports; without it, the first rebuild sets the baseline.
type PublishedModules interface {
	PublishedModules(ctx context.Context, v StoredVersion) ([]model.StakingModule, error)
}

// VettedFromOperators (optional on Build) re-derives the vetted flag of a module's kept keys from the
// operator rows just written: key index below the operator's staking limit. Only for sources that
// derive the flag that way (source.StakingLimitVetting). Reports the rows changed.
type VettedFromOperators interface {
	VettedFromOperators(ctx context.Context, module model.StakingModule) (int64, error)
}

// LeftoverReporter (optional) lists modules with rows above block published (0 = all), which only a
// build that never published can have written.
type LeftoverReporter interface {
	Leftovers(ctx context.Context, published int64) ([]Leftover, error)
}

type Leftover struct {
	ModuleID int32
	// Complete: the attempt wrote the module row, which comes after its keys and operators, and
	// nothing later touched the module. Block, BlockHash and Nonce are that row's.
	Complete  bool
	Block     int64
	BlockHash string
	Nonce     int64
	Canonical bool
}

func (l Leftover) reusable(m model.StakingModule) bool {
	return l.Complete && l.Canonical && l.Nonce == int64(m.Nonce)
}

type StoredVersion struct {
	ID int64
	// The point the data was read at: the version's identity, never moves.
	BlockNumber int64
	BlockHash   string
	Timestamp   int64
	// The newest block the version was confirmed current at; moves every quiet cycle.
	HeadBlockNumber      int64
	HeadBlockHash        string
	HeadTimestamp        int64
	FinalizedBlockNumber int64
	LastChangedBlockHash string
	Label                string
	Status               string
	ModuleNonces         map[string]int64
}

func (v StoredVersion) AsVersion() Version {
	return Version{
		BlockNumber:  v.BlockNumber,
		BlockHash:    v.BlockHash,
		ModuleNonces: v.ModuleNonces,
	}
}

type KeyDiff struct {
	// Kept: deposited keys left as they are without being read.
	Kept      int64
	Staged    int64
	Closed    int64
	Opened    int64
	Unchanged int64
}

func (d KeyDiff) Changed() bool { return d.Closed > 0 || d.Opened > 0 }

type Superseded struct {
	Versions int64
	Dropped  int64
	Reopened int64
}

type Pruned struct {
	OldestKept int64
	Versions   int64
	Rows       int64
	// HeldAt is the block a reader's lease held retention back to (0 = none). A leaked lease looks
	// like healthy back-pressure, except that storage keeps growing.
	HeldAt int64
}

// ErrBuildBehind: the store holds a version or rows above the block a build was asked for.
var ErrBuildBehind = errors.New("the store holds data above this block; a build here would rewrite it")

// ErrVersionExists: the sync loop treats it as "nothing to do this cycle".
var ErrVersionExists = errors.New("a version with this block identity already exists")

// ExistingVersionError is ErrVersionExists with the row's status: a building or superseded row is
// an attempt that did not finish, and skipping it must not count as having built the version.
type ExistingVersionError struct{ Status string }

func (e ExistingVersionError) Error() string {
	return fmt.Sprintf("%s (status %s)", ErrVersionExists, e.Status)
}

func (e ExistingVersionError) Is(target error) bool { return target == ErrVersionExists }

func (e ExistingVersionError) Done() bool { return e.Status == "published" || e.Status == "stale" }

// BuildCounter is optional: versions in building status, for a gauge.
type BuildCounter interface {
	UnfinishedBuilds(ctx context.Context) (int64, error)
}
