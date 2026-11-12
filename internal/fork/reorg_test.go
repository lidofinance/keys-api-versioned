//go:build fork

package fork

import (
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/ingest"
)

// anvil_reorg re-mines with the original transactions: hashes change, state does not diverge.
// evm_revert diverges state but the head jumps back, so scenarios mine back to the old height.

// Both branches move the nonce by exactly one, so only a re-read can tell them apart.
func TestAReorgThatSwapsAKeyIsNotServedFromTheLosingBranch(t *testing.T) {
	h := NewHarness(t, HarnessOpts{})
	h.requireSandboxPreconditions()

	h.Anvil.Mine(2)
	h.Cycle()

	baseNonce := h.Sandbox.Nonce()
	index := int(h.Sandbox.Operator(SandboxOperator).TotalSigningKeys)
	fork := h.Anvil.Snapshot()

	branchA := MintKey(SandboxModuleID, SandboxOperator, index, 1)
	h.Sandbox.AddKeys(SandboxOperator, [][]byte{branchA})
	if got := h.Sandbox.Nonce(); got != baseNonce+1 {
		t.Fatalf("adding one key moved the nonce %d → %d, want one increment", baseNonce, got)
	}
	branchABlock := h.Anvil.BlockNumber()

	published := h.Cycle()
	if !published.Rebuilt {
		t.Fatalf("the cycle after a key was added did not rebuild: %s", published.Reason)
	}
	if !h.holdsKey(branchA) {
		t.Fatal("branch A's key was added and read, but is not in the published version")
	}

	h.Anvil.Revert(fork)
	branchB := MintKey(SandboxModuleID, SandboxOperator, index, 2)
	h.Sandbox.AddKeys(SandboxOperator, [][]byte{branchB})

	if got := h.Anvil.BlockNumber(); got != branchABlock {
		t.Fatalf("branch B ends at block %d and branch A ended at %d: the two have to reach "+
			"the same height or this is a shorter chain rather than a fork", got, branchABlock)
	}
	if got := h.Sandbox.Nonce(); got != baseNonce+1 {
		t.Fatalf("the new branch's nonce is %d, want %d: the branches have to be "+
			"indistinguishable by nonce or this test is not about what it claims", got, baseNonce+1)
	}
	if keys := h.Sandbox.SigningKeys(SandboxOperator, int64(index), 1); len(keys) != 1 ||
		hexBytes(keys[0]) != hexBytes(branchB) {
		t.Fatalf("the chain does not hold branch B's key at index %d after the reorg", index)
	}

	after := h.Cycle()
	t.Logf("the cycle after the swap: action=%s reorg_from=%d keys_staged=%d reason=%q",
		after.Action, after.ReorgFrom, after.KeysStaged, after.Reason)

	if after.ReorgFrom == 0 {
		t.Error("the reorg was not detected: the published version's block was re-mined with a different hash")
	}
	if h.holdsKey(branchA) {
		t.Error("the losing branch's key is still served: a reorg left the registry holding a key " +
			"that no longer exists on the chain")
	}
	if !h.holdsKey(branchB) {
		t.Error("the winning branch's key is not served: the nonce is identical on both branches, " +
			"so a cycle comparing only nonces cannot tell it has to re-read")
	}
}

// Control for the test above: a fork whose only change vanished with its block costs nothing.
func TestAReorgThatDropsItsOnlyChangeCostsNoKeyReads(t *testing.T) {
	h := NewHarness(t, HarnessOpts{})
	h.requireSandboxPreconditions()

	h.Anvil.Mine(2)
	h.Cycle()

	baseNonce := h.Sandbox.Nonce()
	index := int(h.Sandbox.Operator(SandboxOperator).TotalSigningKeys)
	fork := h.Anvil.Snapshot()

	doomed := MintKey(SandboxModuleID, SandboxOperator, index, 1)
	h.Sandbox.AddKeys(SandboxOperator, [][]byte{doomed})
	losingHead := h.Anvil.BlockNumber()

	h.Cycle()
	if !h.holdsKey(doomed) {
		t.Fatal("the key to be reorged away is not in the published version")
	}

	h.Anvil.Revert(fork)
	h.Anvil.Mine(int(losingHead - h.Anvil.BlockNumber()))

	if got := h.Sandbox.Nonce(); got != baseNonce {
		t.Fatalf("the nonce is %d after the rollback, want %d", got, baseNonce)
	}

	after := h.Cycle()
	if after.ReorgFrom == 0 {
		t.Fatal("the reorg was not detected")
	}
	if after.Action != ingest.ActionExtend {
		t.Errorf("a reorg whose only change was dropped decided %q (%s), want extend",
			after.Action, after.Reason)
	}
	if after.KeysStaged != 0 {
		t.Errorf("a reorg with nothing to re-read staged %d keys, want 0", after.KeysStaged)
	}
	if h.holdsKey(doomed) {
		t.Error("a key that left the chain with its block is still served")
	}
}

// detectReorg probes only the version's own height, so a reorg above it is invisible. Safe: a
// change in that range would have moved the nonce.
func TestAReorgAboveThePublishedVersionIsNotEvenNoticed(t *testing.T) {
	h := NewHarness(t, HarnessOpts{})
	h.requireSandboxPreconditions()

	h.Anvil.Mine(2)
	first := h.Cycle()
	versionBlock := first.VersionBlock

	h.Anvil.Mine(3)
	hashBefore := h.Anvil.BlockHash(versionBlock)
	tipBefore := h.Anvil.BlockHash(h.Anvil.BlockNumber())

	h.Anvil.Reorg(2)

	if got := h.Anvil.BlockHash(versionBlock); got != hashBefore {
		t.Fatalf("the version's own block %d changed hash: the reorg was supposed to stay above it",
			versionBlock)
	}
	if got := h.Anvil.BlockHash(h.Anvil.BlockNumber()); got == tipBefore {
		t.Fatal("the tip did not change hash: the reorg did nothing")
	}

	after := h.Cycle()
	if after.ReorgFrom != 0 {
		t.Errorf("a reorg above the published version reported a rollback to %d", after.ReorgFrom)
	}
	if after.Action != ingest.ActionExtend {
		t.Errorf("a reorg that changed no registry state decided %q (%s), want extend",
			after.Action, after.Reason)
	}
	if after.KeysStaged != 0 {
		t.Errorf("a reorg that changed no registry state staged %d keys, want 0", after.KeysStaged)
	}
	if got := len(h.Keys(SandboxAddress)); got != SandboxKeys {
		t.Errorf("the registry holds %d keys after the reorg, want %d", got, SandboxKeys)
	}
}

// holdsKey reports whether the published latest version holds a pubkey.
func (h *Harness) holdsKey(pubkey []byte) bool {
	h.tb.Helper()

	want := "0x" + hexBytes(pubkey)
	for _, key := range h.Keys(SandboxAddress) {
		if sameAddress(key.Key, want) {
			return true
		}
	}
	return false
}
