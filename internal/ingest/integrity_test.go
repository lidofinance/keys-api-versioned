package ingest_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lidofinance/keys-api-versioned/internal/ingest"
	"github.com/lidofinance/keys-api-versioned/internal/store"
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

// A pin resolved just before a prune commits must end in 410, not in a response missing what the
// prune deleted: here the module rows, which would make a dump empty under a 200.
func TestAPinResolvedWhilePruningGets410(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 1, KeysPerOperator: 4}
	old := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, old)
	spec.WithSalt(1).Publish(ctx, t, db.Writer, testreg.At(db.Block(1)))

	reader := db.Reader(t, fmt.Sprint(old.BlockNumber), 2)
	read := make(chan error, 1)
	defer ingest.SetPruneBeforeCommitForTest(func() {
		go func() {
			read <- reader.Snapshot(ctx, func(snap store.Snapshot) error {
				modules, err := snap.Modules(ctx, nil)
				if err == nil && len(modules) == 0 {
					return errors.New("the pinned version answered with no modules")
				}
				return err
			})
		}()
		waitForALeaseWaiter(t, db)
	})()

	pruned, err := db.Writer.Prune(ctx, 1)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if pruned.Versions != 1 {
		t.Fatalf("prune dropped %d versions, want 1", pruned.Versions)
	}

	err = <-read
	var gone store.ErrPruned
	if !errors.As(err, &gone) {
		t.Fatalf("the read pinned to the pruned version: %v, want 410 (ErrPruned)", err)
	}
}

func TestStagingKeysHoldsNoTransactionOpen(t *testing.T) {
	db := testpg.New(t, testpg.WithKeyStageBatch(2))
	ctx := context.Background()

	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 1, KeysPerOperator: 5}
	build, err := db.Writer.Begin(ctx, testreg.At(db.Block(0)))
	if err != nil {
		t.Fatal(err)
	}
	defer build.Abort(ctx)
	keys, err := build.Keys(ctx, spec.Module(1))
	if err != nil {
		t.Fatal(err)
	}
	defer keys.Abort(ctx)
	for i := 0; i < spec.KeysPerOperator; i++ {
		if err := keys.Add(ctx, spec.Key(1, 0, i)); err != nil {
			t.Fatal(err)
		}
	}

	var open int
	if err := db.Admin().QueryRow(ctx, `select count(*) from pg_stat_activity
		where datname = current_database() and state like 'idle in transaction%'
		  and query like '%staged_keys%'`).Scan(&open); err != nil {
		t.Fatal(err)
	}
	if open != 0 {
		t.Errorf("%d session(s) idle in a transaction while keys are staged", open)
	}
}

func waitForALeaseWaiter(t *testing.T, db *testpg.DB) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting bool
		if err := db.Admin().QueryRow(context.Background(), `select exists (select 1 from pg_locks
			where locktype = 'advisory' and not granted
			  and database = (select oid from pg_database where datname = current_database()))`).
			Scan(&waiting); err != nil {
			t.Errorf("read pg_locks: %v", err)
			return
		}
		if waiting {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("the reader never waited for the version the prune claimed")
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

// Leases were single-bigint locks read database-wide: any advisory lock with that key held retention.
func TestOnlyThisSchemasLeasesHoldRetention(t *testing.T) {
	db := testpg.New(t)
	other := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 1, KeysPerOperator: 2}
	old := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, old)
	spec.WithSalt(1).Publish(ctx, t, db.Writer, testreg.At(db.Block(1)))

	conn, err := other.ReaderPool(t, 2).Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `select pg_advisory_lock_shared($1)`, old.BlockNumber); err != nil {
		t.Fatal(err)
	}
	release, err := store.LeaseVersion(ctx, conn, old.BlockNumber)
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	if leased := db.Leases(t); len(leased) != 0 {
		t.Errorf("this schema reports leases %v; they belong to another schema or to nobody", leased)
	}
	pruned, err := db.Writer.Prune(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if pruned.HeldAt != 0 || pruned.Versions != 1 {
		t.Errorf("prune held at %d and dropped %d versions, want 0 and 1", pruned.HeldAt, pruned.Versions)
	}
}
