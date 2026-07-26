// Package ingest keeps a store in step with a source.
//
// Three pieces: the freshness rules that decide whether the published version is
// still current (freshness.go), the loop that applies that decision (sync.go), and
// the versioned store it writes to — the Store interface here, with the Postgres
// implementation in pgstore.go and pgbuild.go.
//
// Nothing here knows where the data came from. Whichever source.Provider produced
// the modules, operators and keys, they land in one schema through one interface.
package ingest

import (
	"context"
	"errors"

	"github.com/lidofinance/keys-api-versioned/internal/model"
)

// Store is the versioned store the sync loop writes to.
//
// The interface carries no SQL, which is what let this repo measure four storage
// engines against each other before settling on Postgres. Published versions,
// staleness, reorg rollback and retention were the expensive parts to get right, and
// they live above this line — worth exactly one implementation whatever the storage
// underneath turns out to be.
type Store interface {
	// Migrate creates what is missing. Idempotent, safe on every boot.
	Migrate(ctx context.Context) error

	// Latest returns the newest version that is published or stale, or nil when
	// the store is empty.
	Latest(ctx context.Context) (*StoredVersion, error)

	// Recent returns versions newest first, including superseded ones. The reorg
	// walk needs them to find the fork point.
	Recent(ctx context.Context, limit int) ([]StoredVersion, error)

	// Begin opens a version at a block. It is invisible to readers — status
	// `building` — until Publish, however many transactions it takes to fill.
	//
	// Rows left open by the previous version stay open, so a version that
	// rebuilds one module inherits every other module for free. That is what
	// makes a scoped rebuild cheap: on mainnet one module holds about 90 % of the
	// keys, so a change confined to the others touches a tenth of the data.
	Begin(ctx context.Context, at model.ELBlockSnapshot) (Build, error)

	// Extend advances a published version's observed block without touching its
	// data or its lastChangedBlockHash. This is the "nothing changed" path, and it
	// is one row update.
	Extend(ctx context.Context, v StoredVersion, at model.ELBlockSnapshot, nonces map[string]int64) error

	// MarkStale flags a version as no longer reflecting the chain. It keeps being
	// served, with the staleness surfaced to the client, because a consistent
	// known-stale answer beats an error.
	MarkStale(ctx context.Context, v StoredVersion) error

	// SupersedeFrom takes every version at or above a block off the chain and
	// rolls the row ranges back to that block. Used after a reorg.
	SupersedeFrom(ctx context.Context, block int64) (Superseded, error)

	// Prune drops versions outside the retention window.
	Prune(ctx context.Context, keep int) (Pruned, error)

	Close()
}

// Build is one version being filled.
//
// The per-module order the sync loop uses is keys, then operators, then the module
// row — because whether the module row's lastChangedBlockHash moves depends on
// whether the keys or operators moved.
type Build interface {
	// Block is the version's block number.
	Block() int64
	// BlockHash is the version's block hash, which is also its identity.
	BlockHash() string

	// Keys opens a per-module key write. Keys are staged in batches, so memory
	// does not scale with the module's size.
	Keys(ctx context.Context, module model.StakingModule) (ModuleKeys, error)

	// Operators replaces a module's operator set in this version and reports how
	// many rows actually changed.
	Operators(ctx context.Context, module model.StakingModule, operators []model.Operator) (int, error)

	// Module writes the module row. contentChanged says whether the module's keys
	// or operators moved in this version; together with a field comparison it
	// decides whether the module's lastChangedBlockHash moves.
	//
	// This is the field the canonical service gets wrong: there it moves whenever
	// a module is re-read, so a restart or a stall past the overlap window stamps
	// every module with the same block. Deriving it from content instead means a
	// re-read that found nothing leaves it alone.
	Module(ctx context.Context, m model.StakingModule, contentChanged bool) error

	// Publish makes the version current with a single status change.
	Publish(ctx context.Context, label string) error

	// Abort discards the version. Rows already written stay: they are invisible,
	// no published version claims their block, and retention collects them.
	Abort(ctx context.Context)
}

// ModuleKeys accumulates one module's keys and applies them as a diff.
type ModuleKeys interface {
	// Add stages one key.
	Add(ctx context.Context, k model.Key) error
	// Commit applies the staged set: rows that changed or disappeared are closed,
	// rows that are new or changed are opened, and rows that are identical are
	// left untouched — which is the point, since almost all of them are.
	Commit(ctx context.Context) (KeyDiff, error)
	// Abort discards the staged set.
	Abort(ctx context.Context)
}

// StoredVersion is a version as the store holds it.
type StoredVersion struct {
	ID int64
	// BlockNumber, BlockHash and Timestamp are the point the data was read at: the
	// version's identity, which never moves.
	BlockNumber int64
	BlockHash   string
	Timestamp   int64
	// HeadBlockNumber, HeadBlockHash and HeadTimestamp are the newest block the
	// version was confirmed current at. These move on every quiet cycle.
	HeadBlockNumber      int64
	HeadBlockHash        string
	HeadTimestamp        int64
	LastChangedBlockHash string
	Label                string
	Status               string
	ModuleNonces         map[string]int64
}

// AsVersion converts to the shape the freshness rules take.
func (v StoredVersion) AsVersion() Version {
	return Version{
		BlockNumber:  v.BlockNumber,
		BlockHash:    v.BlockHash,
		ModuleNonces: v.ModuleNonces,
	}
}

// KeyDiff reports what a module's key write changed.
type KeyDiff struct {
	Staged    int64
	Closed    int64
	Opened    int64
	Unchanged int64
}

// Changed reports whether anything moved.
func (d KeyDiff) Changed() bool { return d.Closed > 0 || d.Opened > 0 }

// Superseded reports what a reorg rollback removed.
type Superseded struct {
	Versions int64
	Dropped  int64
	Reopened int64
}

// Pruned reports what retention removed.
type Pruned struct {
	// OldestKept is the block number of the oldest version still held.
	OldestKept int64
	Versions   int64
	Rows       int64
	// HeldAt is the version block a reader's lease held retention back to, 0 when
	// nothing was held. It is reported rather than hidden because a lease that never
	// goes away — a leaked one — looks exactly like healthy back-pressure otherwise,
	// and the difference is whether storage keeps growing.
	HeldAt int64
}

// ErrVersionExists is returned by Begin when a version with that block identity is
// already stored. The sync loop treats it as "nothing to do this cycle" rather
// than as a failure.
var ErrVersionExists = errors.New("a version with this block identity already exists")
