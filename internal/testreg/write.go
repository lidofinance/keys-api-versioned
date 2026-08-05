package testreg

import (
	"context"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/ingest"
	"github.com/lidofinance/keys-api-versioned/internal/model"
)

// At builds the block identity for a version.
func At(block int64) model.ELBlockSnapshot {
	return model.ELBlockSnapshot{
		BlockNumber: block,
		BlockHash:   HashAt(block),
		Timestamp:   block * 12,
	}
}

// Publish writes a full version of every module and publishes it.
//
// No finalized block is recorded, which is what most tests want: `?snapshot=finalized` is
// then unavailable and cannot quietly satisfy an assertion meant for `latest`. Tests
// about finality use PublishFinal.
func (s Spec) Publish(ctx context.Context, tb testing.TB, pg *ingest.PGStore, at model.ELBlockSnapshot) {
	tb.Helper()
	build := s.Fill(ctx, tb, pg, at, s.ModuleIDs()...)
	if err := build.Publish(ctx, "latest", 0); err != nil {
		tb.Fatalf("publish version %d: %v", at.BlockNumber, err)
	}
}

// PublishFinal publishes and records the finalized block observed at the time, which is
// what `?snapshot=finalized` resolves against.
func (s Spec) PublishFinal(ctx context.Context, tb testing.TB, pg *ingest.PGStore, at model.ELBlockSnapshot, finalized int64) {
	tb.Helper()
	build := s.Fill(ctx, tb, pg, at, s.ModuleIDs()...)
	if err := build.Publish(ctx, "latest", finalized); err != nil {
		tb.Fatalf("publish version %d: %v", at.BlockNumber, err)
	}
}

// PublishModules writes only the named modules and publishes. Everything outside the
// list is inherited from the previous version at no cost, which is the scoped rebuild
// the sync loop performs when one module's nonce moved.
func (s Spec) PublishModules(ctx context.Context, tb testing.TB, pg *ingest.PGStore, at model.ELBlockSnapshot, ids ...int) {
	tb.Helper()
	build := s.Fill(ctx, tb, pg, at, ids...)
	if err := build.Publish(ctx, "latest", 0); err != nil {
		tb.Fatalf("publish version %d: %v", at.BlockNumber, err)
	}
}

// Fill writes a version's keys, operators and module rows but does not publish it.
//
// The unpublished build is what makes an interleaving test deterministic rather than
// a race: the expensive part — the COPY of every key and the two diff statements —
// happens before the reader is blocked, so the only thing left to land inside the
// window is Publish, which is one row update. A test can therefore say "publish
// exactly here" and mean it.
//
// Modules outside ids still get their module row written, so the version describes the
// whole registry; only their keys are inherited.
func (s Spec) Fill(ctx context.Context, tb testing.TB, pg *ingest.PGStore, at model.ELBlockSnapshot, ids ...int) ingest.Build {
	tb.Helper()

	build, err := pg.Begin(ctx, at)
	if err != nil {
		tb.Fatalf("begin version %d: %v", at.BlockNumber, err)
	}

	rewrite := map[int]bool{}
	for _, id := range ids {
		rewrite[id] = true
	}

	for _, module := range s.ModuleSet() {
		id := int(module.ID)
		if rewrite[id] {
			s.writeModule(ctx, tb, build, module)
		}
		// The flag says whether this module's content moved, which is what decides
		// its lastChangedBlockHash — the signal a consumer polls to skip work.
		if err := build.Module(ctx, module, rewrite[id]); err != nil {
			build.Abort(ctx)
			tb.Fatalf("module row %d: %v", id, err)
		}
	}
	return build
}

func (s Spec) writeModule(ctx context.Context, tb testing.TB, build ingest.Build, module model.StakingModule) {
	tb.Helper()
	id := int(module.ID)

	keys, err := build.Keys(ctx, module)
	if err != nil {
		build.Abort(ctx)
		tb.Fatalf("open keys of module %d: %v", id, err)
	}
	for op := 0; op < s.OperatorsPerModule; op++ {
		for i := 0; i < s.KeysPerOperator; i++ {
			k := s.Key(id, op, i)
			if err := keys.Add(ctx, k); err != nil {
				build.Abort(ctx)
				tb.Fatalf("add key %s: %v", Identity{id, op, i, s.Salt}, err)
			}
		}
	}
	if _, err := keys.Commit(ctx); err != nil {
		build.Abort(ctx)
		tb.Fatalf("commit keys of module %d: %v", id, err)
	}

	if _, err := build.Operators(ctx, module, s.Operators(id)); err != nil {
		build.Abort(ctx)
		tb.Fatalf("operators of module %d: %v", id, err)
	}
}
