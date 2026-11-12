// Package e2e drives writer, store and HTTP against real Postgres; skipped without TEST_DATABASE_URL.
// One package, so per-database contention (advisory locks) stays under one parallelism budget.
package e2e

import (
	"context"
	"net/http"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/testpg"
	"github.com/lidofinance/keys-api-versioned/internal/testreg"
)

// The harness proving itself: when this fails, debug it before anything subtler in the package.
func TestTheHarnessServesAVerifiedDump(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Small()
	version := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, version)

	srv := testpg.NewServer(t, db.Reader(t, "latest", 4))

	resp := srv.Get(t, "/v1/keys")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/keys: status %d, want 200", resp.StatusCode)
	}

	verifier := testreg.NewVerifier(spec)
	body, err := verifier.V1Keys(resp.Body)
	if err != nil {
		t.Fatalf("verify the dump: %v", err)
	}
	if err := verifier.Done(); err != nil {
		t.Fatalf("the dump was not the registry: %v", err)
	}
	if verifier.Keys() != spec.TotalKeys() {
		t.Errorf("dump held %d keys, want %d", verifier.Keys(), spec.TotalKeys())
	}

	// Meta names the version's own block, not the head it was last confirmed at.
	if body.Meta == nil {
		t.Fatal("the response carried no meta")
	}
	if body.Meta.BlockNumber != version.BlockNumber {
		t.Errorf("meta reports block %d, want %d", body.Meta.BlockNumber, version.BlockNumber)
	}

	srv.WaitForEvents(t, 1, waitShort)
	event, _ := srv.LastEvent()
	if event.Status != http.StatusOK || event.Bytes == 0 {
		t.Errorf("the service reported status %d and %d bytes", event.Status, event.Bytes)
	}
	if srv.Pending() != 0 {
		t.Errorf("%d request(s) still in flight after the body was read", srv.Pending())
	}
}

// The verifier proving itself on hand-built keys rather than a hoped-for interleaving.
func TestTheGeneratedRegistryCatchesAKeyFromAnotherVersion(t *testing.T) {
	spec := testreg.Small()

	verifier := testreg.NewVerifier(spec)
	if err := verifier.Key(ptr(spec.Key(1, 0, 0))); err != nil {
		t.Fatalf("a key of the expected version was rejected: %v", err)
	}

	stale := spec.WithSalt(spec.Salt+1).Key(1, 0, 1)
	if err := verifier.Key(&stale); err == nil {
		t.Fatal("a key from another version of the module passed verification")
	}

	misattributed := spec.Key(2, 0, 0)
	misattributed.ModuleAddress = testreg.ModuleAddress(3)
	if err := testreg.NewVerifier(spec).Key(&misattributed); err == nil {
		t.Fatal("a key served under the wrong module passed verification")
	}

	// A key served twice must fail: paging depends on it.
	twice := testreg.NewVerifier(spec)
	first := spec.Key(1, 0, 0)
	if err := twice.Key(&first); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	if err := twice.Key(&first); err == nil {
		t.Fatal("the same key twice passed verification")
	}
}

func ptr[T any](v T) *T { return &v }
