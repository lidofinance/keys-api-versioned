// Package ingest keeps a store in step with a source.
package ingest

import (
	"context"
	"errors"

	"github.com/lidofinance/keys-api-versioned/internal/model"
)

type Store interface {
	Migrate(ctx context.Context) error

	// Latest returns the newest published or stale version, nil when the store is empty.
	Latest(ctx context.Context) (*StoredVersion, error)

	// Recent returns versions newest first, including superseded ones.
	Recent(ctx context.Context, limit int) ([]StoredVersion, error)

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

// ErrVersionExists: the sync loop treats it as "nothing to do this cycle".
var ErrVersionExists = errors.New("a version with this block identity already exists")
