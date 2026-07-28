//go:build fork

package fork

import (
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/ingest"
)

// The reorg family. What a fork adds over the scripted double in internal/ingest is
// the only thing that matters here: a branch whose blocks have the same heights, hashes
// that differ, and *different registry content* produced by the same contract. The
// double can express the first two — hashAt flips a letter — and structurally cannot
// express the third, because its nonce is an int32 with no causal link to any key.
//
// docs/next-steps.md:112 records the open question these are written against:
//
//	"Nonce equality across a reorg is not sufficient on its own. A counter can roll
//	 back two increments and take two different ones on the new branch, arriving at
//	 the same value with different content."
//
// # Two primitives, and they are not interchangeable
//
// anvil has both anvil_reorg and evm_snapshot/evm_revert, and picking the wrong one
// makes a test that proves nothing. Measured here rather than taken from the docs:
//
//   - anvil_reorg re-mines the rewound blocks *with their original transactions*. The
//     block hashes change and the head keeps moving forward, which is what a node
//     reports during a real reorg — but the state does not diverge. It is the right
//     tool for the common mainnet case, a short reorg that touched nothing of ours,
//     and the wrong one for a branch that has to hold different keys.
//   - evm_revert restores the state, so the new branch can be built with different
//     content. The cost is that the head jumps backwards to the snapshot, which the
//     sync loop reads as a lagging endpoint rather than a fork, so the scenario has to
//     mine back up to the old height before the next cycle.
//
// It also only rewrites blocks anvil produced itself; Anvil.Reorg checks that rather
// than letting it fail as a bare "Resource not found".

// TestAReorgThatSwapsAKeyIsNotServedFromTheLosingBranch is the scenario the caveat
// describes, built on the chain rather than argued about.
//
// One key is added on branch A and the version that reads it is published; then the
// same height is re-mined carrying a *different* key. Both branches move the nonce by
// exactly one, so the counter is identical on either side of the fork and cannot tell
// them apart. What the service serves afterwards has to be branch B's key.
func TestAReorgThatSwapsAKeyIsNotServedFromTheLosingBranch(t *testing.T) {
	h := NewHarness(t, HarnessOpts{})
	h.requireSandboxPreconditions()

	h.Anvil.Mine(2)
	h.Cycle()

	baseNonce := h.Sandbox.Nonce()
	index := int(h.Sandbox.Operator(SandboxOperator).TotalSigningKeys)
	fork := h.Anvil.Snapshot()

	// Branch A: one key, one nonce increment, one version that holds it.
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

	// Branch B: the same height, a different key.
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

// TestAReorgThatDropsItsOnlyChangeCostsNoKeyReads is the control for the test above,
// and the threshold E2 was written around: a fork whose change vanished with its block
// must cost nothing.
//
// It is a control in the strict sense — it fails on the behaviour the other test is
// about. If the rollback did not restore the pre-fork content this one would see a
// nonce difference and rebuild, and if the loop rebuilt unconditionally after a
// rollback, which is what it did before the fix recorded at sync.go:194, it would
// report a rebuild too.
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

	// The winning branch reaches the same height with nothing in it.
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

// TestAReorgAboveThePublishedVersionIsNotEvenNoticed pins the shape of the detector,
// and it is the mainnet-common case: the published version is some blocks old and a
// one-or-two-block reorg happens at the tip.
//
// detectReorg probes the hash at the published version's *own* height and nowhere
// else, so a reorg entirely above it is invisible to the loop. That is safe rather
// than lucky, and the test says why by checking the other half: the registry is
// untouched and the cycle extends. Had the reorged range changed anything of ours the
// module's nonce would have moved, and the ordinary freshness check — not the reorg
// detector — is what would rebuild.
//
// Two things this depends on, both measured rather than assumed: anvil_reorg re-mines
// the rewound blocks with their original transactions, so the state on the new branch
// is identical; and it refuses to reach the fork block, which Anvil.Reorg checks.
func TestAReorgAboveThePublishedVersionIsNotEvenNoticed(t *testing.T) {
	h := NewHarness(t, HarnessOpts{})
	h.requireSandboxPreconditions()

	h.Anvil.Mine(2)
	first := h.Cycle()
	versionBlock := first.VersionBlock

	// Three blocks of headroom, then rewrite two of them. The version's own block stays
	// below the reorged range.
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
