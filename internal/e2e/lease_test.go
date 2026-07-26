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

// The lease is what keeps retention from deleting rows a reader is still streaming. The
// store-level version of this lives in internal/ingest; these are the HTTP-level and
// lifetime properties, which is where the mechanism can go wrong in ways the store test
// cannot see: a lock left on a pooled connection, or one not returned when a client
// disappears.
//
// All of them are exclusive. Advisory locks are per-database rather than per-schema, and
// retention lowers its boundary to any leased block below it, so another test's lease
// would show up as this one's.

func TestRetentionHoldsForAStreamingReaderOverHTTP(t *testing.T) {
	testpg.Exclusive(t)

	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Streaming()
	old := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, old)

	// The pin is explicit, so this reader is on the version about to leave the window
	// rather than on whatever is newest.
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

	// A newer version, then retention keeping only it: without the lease this deletes
	// the rows the held dump is in the middle of reading.
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

	// Control: once the reader is gone, the same prune must delete. Otherwise the
	// assertion above would also pass on a retention that had nothing to do.
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

// TestALeaseLeftOnAPooledConnectionHoldsRetention encodes the pgx rule that makes a
// leaked lease dangerous: a connection returned to the pool keeps its session state, so
// an advisory lock outlives the request that took it and looks exactly like healthy
// back-pressure. The visible difference is that it never goes away, which is why
// retention reports what held it.
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

	// Released by hand, and then retention proceeds — which is the observable difference
	// between a slow reader and a leak.
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

// A client that disappears mid-dump must not hold retention open. The release is
// eventual rather than immediate: pgx may destroy a connection whose query was
// cancelled instead of running the unlock on it, in which case the lock goes when the
// backend session ends — so this polls rather than sampling once.
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

	// The client goes away without reading the rest.
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
