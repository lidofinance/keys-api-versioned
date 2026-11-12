package testreg

import (
	"context"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/ingest"
	"github.com/lidofinance/keys-api-versioned/internal/model"
)

func At(block int64) model.ELBlockSnapshot {
	return model.ELBlockSnapshot{
		BlockNumber: block,
		BlockHash:   HashAt(block),
		Timestamp:   block * 12,
	}
}

// Publish records no finalized block, so ?snapshot=finalized cannot satisfy a `latest` assertion.
func (s Spec) Publish(ctx context.Context, tb testing.TB, pg *ingest.PGStore, at model.ELBlockSnapshot) {
	tb.Helper()
	build := s.Fill(ctx, tb, pg, at, s.ModuleIDs()...)
	if err := build.Publish(ctx, "latest", 0); err != nil {
		tb.Fatalf("publish version %d: %v", at.BlockNumber, err)
	}
}

func (s Spec) PublishFinal(ctx context.Context, tb testing.TB, pg *ingest.PGStore, at model.ELBlockSnapshot, finalized int64) {
	tb.Helper()
	build := s.Fill(ctx, tb, pg, at, s.ModuleIDs()...)
	if err := build.Publish(ctx, "latest", finalized); err != nil {
		tb.Fatalf("publish version %d: %v", at.BlockNumber, err)
	}
}

func (s Spec) PublishModules(ctx context.Context, tb testing.TB, pg *ingest.PGStore, at model.ELBlockSnapshot, ids ...int) {
	tb.Helper()
	build := s.Fill(ctx, tb, pg, at, ids...)
	if err := build.Publish(ctx, "latest", 0); err != nil {
		tb.Fatalf("publish version %d: %v", at.BlockNumber, err)
	}
}

// Fill does not publish: the COPY and diffs happen before a reader blocks, so only Publish (one row
// update) lands inside the window, which makes interleaving tests deterministic.
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
