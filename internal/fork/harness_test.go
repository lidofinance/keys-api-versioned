//go:build fork

package fork

import (
	"net/http"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/ingest"
)

// TestTheLoopPublishesAVersionFromTheChain is the harness proving itself, and it is
// the first time in this repo that the sync loop runs against Postgres at all: the
// scripted double in internal/ingest drives it against a fake store, and internal/e2e
// drives the writer directly with no loop and no chain.
//
// Everything else in this package builds on these pieces working together, so when
// they do not, this is the test that should fail rather than something subtler.
func TestTheLoopPublishesAVersionFromTheChain(t *testing.T) {
	h := NewHarness(t, HarnessOpts{})
	h.requireSandboxPreconditions()

	result := h.Cycle()
	if !result.Rebuilt {
		t.Fatalf("the first cycle did not build anything: %s", result.Reason)
	}
	if result.VersionBlock != ForkBlock() {
		t.Errorf("version at block %d, want the forked block %d", result.VersionBlock, ForkBlock())
	}

	keys := h.Keys(SandboxAddress)
	if len(keys) != SandboxKeys {
		t.Errorf("published %d keys, want %d", len(keys), SandboxKeys)
	}

	// The keys are the chain's, not a fixture's: every one of them has to be a key
	// the contract itself reports, at the same operator and index.
	onChain := map[string]bool{}
	for op := int64(0); op < SandboxOperators; op++ {
		for _, key := range h.Sandbox.SigningKeys(op, 0, int64(h.Sandbox.Operator(op).TotalSigningKeys)) {
			onChain["0x"+hexBytes(key)] = true
		}
	}
	for _, key := range keys {
		if !onChain[key.Key] {
			t.Errorf("published a key the contract does not hold: %s", key.Key)
		}
	}

	// A second cycle with nothing moving must be an extend, not a rebuild. If it is a
	// rebuild, the cheap signals are not being trusted and every other timing
	// assertion in this package is measuring the wrong thing.
	h.Anvil.Mine(1)
	quiet := h.Cycle()
	if quiet.Action != ingest.ActionExtend {
		t.Errorf("a quiet cycle decided %q (%s), want extend", quiet.Action, quiet.Reason)
	}
	if quiet.Rebuilt {
		t.Error("a quiet cycle rebuilt")
	}
}

// TestTheServedSurfaceAnswersFromAForkedVersion checks the one thing the store-level
// helpers in this package skip: that what the loop published is reachable over HTTP.
// The response bytes themselves are internal/e2e's subject, not this package's.
func TestTheServedSurfaceAnswersFromAForkedVersion(t *testing.T) {
	h := NewHarness(t, HarnessOpts{})
	h.Cycle()

	srv := h.Server()
	resp := srv.Get(t, "/v1/modules")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/modules: status %d, want 200", resp.StatusCode)
	}
}

// requireSandboxPreconditions fails loudly if the pinned block no longer holds what
// the scenarios were written against.
//
// Without this, upstream drift or a re-pin turns a scenario into a test that still
// passes while asserting something else — the failure mode the repo has already paid
// for once, when a per-module run could not reproduce a regression the previous module
// created.
func (h *Harness) requireSandboxPreconditions() {
	h.tb.Helper()

	if got := h.Sandbox.OperatorCount(); got != SandboxOperators {
		h.tb.Fatalf("Sandbox has %d operators at block %d, want %d: re-pin the fork block and re-check the preconditions",
			got, ForkBlock(), SandboxOperators)
	}

	op := h.Sandbox.Operator(SandboxOperator)
	switch {
	case op.TotalSigningKeys != SandboxOperatorKeys:
		h.tb.Fatalf("Sandbox operator %d holds %d keys, want %d", SandboxOperator, op.TotalSigningKeys, SandboxOperatorKeys)
	case op.UsedSigningKeys != 0:
		h.tb.Fatalf("Sandbox operator %d has %d deposited keys: removeSigningKeys refuses to touch those, so the scenarios need an operator with none",
			SandboxOperator, op.UsedSigningKeys)
	case !op.Active:
		h.tb.Fatalf("Sandbox operator %d is inactive: its reward address can no longer manage keys", SandboxOperator)
	}
}
