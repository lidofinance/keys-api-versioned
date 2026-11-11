//go:build fork

package fork

import (
	"net/http"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/ingest"
)

// The first test that runs the sync loop against Postgres; if the harness is broken, this fails.
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

	// A quiet cycle that rebuilds means the cheap signals are not trusted, and every other timing
	// assertion here measures the wrong thing.
	h.Anvil.Mine(1)
	quiet := h.Cycle()
	if quiet.Action != ingest.ActionExtend {
		t.Errorf("a quiet cycle decided %q (%s), want extend", quiet.Action, quiet.Reason)
	}
	if quiet.Rebuilt {
		t.Error("a quiet cycle rebuilt")
	}
}

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

// requireSandboxPreconditions catches upstream drift or a re-pin that would leave scenarios
// passing while asserting something else.
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
