package e2e

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/lidofinance/keys-api-versioned/internal/testpg"
	"github.com/lidofinance/keys-api-versioned/internal/testreg"
)

// HTTP-level lease properties the store test cannot see: a lock left on a pooled connection, or not
// returned when a client disappears. All exclusive: advisory locks are per-database.

func TestRetentionHoldsForAStreamingReaderOverHTTP(t *testing.T) {
	testpg.Exclusive(t)

	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Streaming()
	old := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, old)

	srv := testpg.NewServer(t, db.Reader(t, fmt.Sprint(old.BlockNumber), 3))

	resp, body := srv.GetHeld(t, "/v1/keys", 64<<10)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/keys: status %d, want 200", resp.StatusCode)
	}
	if srv.Pending() != 1 {
		t.Fatalf("%d requests in flight, want the held dump to be one", srv.Pending())
	}
	if leases := db.Leases(t); len(leases) != 1 || leases[0] != old.BlockNumber {
		t.Fatalf("leases are %v, want just the version being streamed (%d)", leases, old.BlockNumber)
	}

	// Without the lease, keeping only the newer version deletes rows the held dump is reading.
	next := spec.WithSalt(spec.Salt + 1)
	next.Publish(ctx, t, db.Writer, testreg.At(db.Block(1)))

	pruned, err := db.Writer.Prune(ctx, 1)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if pruned.HeldAt != old.BlockNumber {
		t.Errorf("Pruned.HeldAt = %d, want %d: retention did not see the reader's lease",
			pruned.HeldAt, old.BlockNumber)
	}

	verifier := testreg.NewVerifier(spec)
	if _, err := verifier.V1Keys(body()); err != nil {
		t.Fatalf("verify the held dump after retention ran: %v", err)
	}
	if err := verifier.Done(); err != nil {
		t.Fatalf("retention took rows from under the reader: %v", err)
	}

	// Control: once the reader is gone the same prune must delete.
	srv.WaitForEvents(t, 1, waitLong)
	waitForNoLeases(t, db, waitShort)

	pruned, err = db.Writer.Prune(ctx, 1)
	if err != nil {
		t.Fatalf("prune after the reader left: %v", err)
	}
	if pruned.HeldAt != 0 {
		t.Errorf("Pruned.HeldAt = %d after the reader left, want 0", pruned.HeldAt)
	}
	if pruned.Versions == 0 && pruned.Rows == 0 {
		t.Fatal("nothing was pruned once the lease was gone, so the check above proved nothing")
	}
}

// A pooled connection keeps session state, so a leaked advisory lock outlives its request and looks
// like healthy back-pressure, except it never goes away.
func TestALeaseLeftOnAPooledConnectionHoldsRetention(t *testing.T) {
	testpg.Exclusive(t)

	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 1, KeysPerOperator: 4}
	old := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, old)
	spec.WithSalt(1).Publish(ctx, t, db.Writer, testreg.At(db.Block(1)))

	pool := db.ReaderPool(t, 2)
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := conn.Exec(ctx, `select pg_advisory_lock_shared($1)`, old.BlockNumber); err != nil {
		t.Fatalf("take a lease by hand: %v", err)
	}
	conn.Release() // back to the pool, lock still held

	pruned, err := db.Writer.Prune(ctx, 1)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if pruned.HeldAt != old.BlockNumber {
		t.Errorf("Pruned.HeldAt = %d, want %d: a lock on a pooled connection is still a lease",
			pruned.HeldAt, old.BlockNumber)
	}
	if pruned.Versions != 0 {
		t.Errorf("prune dropped %d versions while a lease was held", pruned.Versions)
	}

	released, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire to release: %v", err)
	}
	if _, err := released.Exec(ctx, `select pg_advisory_unlock_all()`); err != nil {
		t.Fatalf("release the lease: %v", err)
	}
	released.Release()
	waitForNoLeases(t, db, waitShort)

	pruned, err = db.Writer.Prune(ctx, 1)
	if err != nil {
		t.Fatalf("prune after release: %v", err)
	}
	if pruned.HeldAt != 0 || pruned.Versions == 0 {
		t.Errorf("after the lease went: HeldAt=%d versions=%d, want 0 and at least one",
			pruned.HeldAt, pruned.Versions)
	}
}

// Release is eventual: pgx may destroy a cancelled connection rather than unlock on it, and the lock
// goes when the backend session ends. Hence polling.
func TestACancelledDumpReleasesItsLease(t *testing.T) {
	testpg.Exclusive(t)

	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Streaming()
	version := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, version)

	srv := testpg.NewServer(t, db.Reader(t, "latest", 3))

	resp, _ := srv.GetHeld(t, "/v1/keys", 64<<10)
	if leases := db.Leases(t); len(leases) != 1 {
		resp.Body.Close()
		t.Fatalf("leases are %v, want one for the dump in flight", leases)
	}

	resp.Body.Close()

	waitForNoLeases(t, db, waitLong)
}

func waitForNoLeases(t *testing.T, db *testpg.DB, within time.Duration) {
	t.Helper()

	deadline := time.Now().Add(within)
	for {
		leases := db.Leases(t)
		if len(leases) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("waited %s for the leases to go, still held: %v", within, leases)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
