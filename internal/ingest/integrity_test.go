package ingest_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/ingest"
	"github.com/lidofinance/keys-api-versioned/internal/testpg"
	"github.com/lidofinance/keys-api-versioned/internal/testreg"
)

// Restarting with KEYS_API_BUILD_AT_FINALIZED switched on asks for exactly this.
func TestABuildBelowANewerVersionIsRefused(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 1, KeysPerOperator: 4}
	newer := testreg.At(db.Block(5))
	spec.Publish(ctx, t, db.Writer, newer)

	build, err := db.Writer.Begin(ctx, testreg.At(db.Block(3)))
	if err == nil {
		older := spec.WithSalt(1)
		build.Abort(ctx)
		older.Publish(ctx, t, db.Writer, testreg.At(db.Block(4)))
		verifyVersion(t, db, newer.BlockNumber, spec)
		t.Fatal("Begin accepted a block below the published version")
	}
	if !errors.Is(err, ingest.ErrBuildBehind) {
		t.Fatalf("Begin below the published version: %v, want ErrBuildBehind", err)
	}
	verifyVersion(t, db, newer.BlockNumber, spec)
}

func TestABuildBelowAnAbortedAttemptIsRefused(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 1, KeysPerOperator: 4}
	spec.Publish(ctx, t, db.Writer, testreg.At(db.Block(1)))
	spec.WithSalt(1).Fill(ctx, t, db.Writer, testreg.At(db.Block(7)), 1).Abort(ctx)

	if _, err := db.Writer.Begin(ctx, testreg.At(db.Block(6))); !errors.Is(err, ingest.ErrBuildBehind) {
		t.Fatalf("Begin below an aborted attempt's rows: %v, want ErrBuildBehind", err)
	}
	if _, err := db.Writer.Begin(ctx, testreg.At(db.Block(8))); err != nil {
		t.Fatalf("Begin above it: %v", err)
	}
}

func verifyVersion(t *testing.T, db *testpg.DB, block int64, spec testreg.Spec) {
	t.Helper()
	srv := testpg.NewServer(t, db.Reader(t, fmt.Sprint(block), 2))
	resp := srv.Get(t, "/v1/keys")
	defer resp.Body.Close()
	verifier := testreg.NewVerifier(spec)
	if _, err := verifier.V1Keys(resp.Body); err != nil {
		t.Errorf("version %d no longer holds what was published: %v", block, err)
		return
	}
	if err := verifier.Done(); err != nil {
		t.Errorf("version %d is incomplete: %v", block, err)
	}
}
