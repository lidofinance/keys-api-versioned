// Package e2e drives the whole service — writer, store and HTTP surface — against a
// real Postgres.
//
// It is one package on purpose. These tests contend on things that are per-database
// rather than per-schema (advisory locks, and therefore retention's back-off), and one
// package keeps them under a single parallelism budget the harness controls instead of
// letting `go test ./...` interleave them with each other.
//
// Skipped unless TEST_DATABASE_URL is set:
//
//	TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55433/keys_api_test go test ./internal/e2e/
package e2e

import (
	"context"
	"net/http"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/testpg"
	"github.com/lidofinance/keys-api-versioned/internal/testreg"
)

// TestTheHarnessServesAVerifiedDump is the harness proving itself: a version written by
// the writer, read back through the HTTP surface, and checked key by key against the
// registry that generated it. Everything else in this package builds on these four
// pieces working, so when they do not, this is the test that should fail rather than
// something subtler.
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

	// The meta names the version's own block, not the head it was last confirmed at:
	// a consumer reconciling against the chain has to know which block the keys are
	// from, and this is where it reads it.
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

// TestTheGeneratedRegistryCatchesAKeyFromAnotherVersion is the verifier proving itself.
// A stitched response — one module's keys from an older version — is the defect the
// consistency tests are about, so the check that would catch it must be shown to
// actually catch it, on a key built by hand rather than on a hoped-for interleaving.
func TestTheGeneratedRegistryCatchesAKeyFromAnotherVersion(t *testing.T) {
	spec := testreg.Small()

	verifier := testreg.NewVerifier(spec)
	if err := verifier.Key(ptr(spec.Key(1, 0, 0))); err != nil {
		t.Fatalf("a key of the expected version was rejected: %v", err)
	}

	// The same key from the next generation of the same module.
	stale := spec.WithSalt(spec.Salt+1).Key(1, 0, 1)
	if err := verifier.Key(&stale); err == nil {
		t.Fatal("a key from another version of the module passed verification")
	}

	// And a key served under the wrong module, which is what a mismapped module id
	// would produce.
	misattributed := spec.Key(2, 0, 0)
	misattributed.ModuleAddress = testreg.ModuleAddress(3)
	if err := testreg.NewVerifier(spec).Key(&misattributed); err == nil {
		t.Fatal("a key served under the wrong module passed verification")
	}

	// A key served twice has to be a failure too: paging depends on it.
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
