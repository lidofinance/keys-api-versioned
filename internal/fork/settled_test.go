//go:build fork

package fork

import (
	"context"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/source"
)

// anvil reports finalized two epochs (64 blocks) behind its head.
const anvilFinalityDepth = 64

// A deposit after the previous ones are finalized reads only the keys after them, and what is served
// still matches a full read of the registry at the same block.
func TestADepositAfterFinalityReadsOnlyTheKeysAfterTheDepositedOnes(t *testing.T) {
	h := NewHarness(t, HarnessOpts{})
	h.requireSandboxPreconditions()

	h.Sandbox.Vet(SandboxOperator, 2)
	h.Sandbox.Deposit(1)
	cold := h.Cycle()
	if cold.KeysKept != 0 {
		t.Fatalf("the cold sync kept %d keys, want a full read", cold.KeysKept)
	}

	h.Anvil.Mine(anvilFinalityDepth + 2)
	if r := h.Cycle(); r.FinalizedBlock < cold.VersionBlock {
		t.Fatalf("finalized %d is still below the version at %d: the scenario cannot test the prefix",
			r.FinalizedBlock, cold.VersionBlock)
	}

	h.Sandbox.Deposit(1)
	if got := h.Sandbox.Operator(SandboxOperator).UsedSigningKeys; got != 2 {
		t.Fatalf("the operator reports %d deposited keys, want 2", got)
	}

	r := h.Cycle()
	if !r.Rebuilt {
		t.Fatalf("the deposit did not rebuild: %s", r.Reason)
	}
	t.Logf("deposit rebuild: kept %d, read %d, changed %d rows, finalized %d, version %d",
		r.KeysKept, r.KeysStaged, r.KeysChanged, r.FinalizedBlock, r.VersionBlock)

	served := h.Keys(SandboxAddress)
	if r.KeysKept < 1 {
		t.Errorf("kept %d keys, want at least the operator's first, finalized deposit", r.KeysKept)
	}
	if got := r.KeysKept + r.KeysStaged; got != int64(len(served)) {
		t.Errorf("kept %d + read %d = %d, want every one of the %d served keys accounted for once",
			r.KeysKept, r.KeysStaged, got, len(served))
	}
	// Key 1 of the operator flipped to used: one row closed, one opened.
	if r.KeysChanged != 2 {
		t.Errorf("%d rows changed, want 2", r.KeysChanged)
	}

	assertServedMatchesAFullRead(t, h, served)
}

func assertServedMatchesAFullRead(t *testing.T, h *Harness, served []model.Key) {
	t.Helper()
	ctx := context.Background()

	head := h.Anvil.BlockNumber()
	at := source.Ref{BlockNumber: head, BlockHash: h.Anvil.BlockHash(head)}
	modules, err := h.Provider.Modules(ctx, at)
	if err != nil {
		t.Fatalf("modules: %v", err)
	}
	var sandbox *model.StakingModule
	for i := range modules {
		if modules[i].ID == SandboxModuleID {
			sandbox = &modules[i]
		}
	}
	if sandbox == nil {
		t.Fatal("the sandbox module is not in the module set")
	}

	type identity struct{ operator, index int32 }
	chain := map[identity]model.Key{}
	if err := h.Provider.Keys(ctx, at, *sandbox, func(k model.Key) error {
		chain[identity{k.OperatorIndex, k.Index}] = k
		return nil
	}); err != nil {
		t.Fatalf("full read: %v", err)
	}

	if len(served) != len(chain) {
		t.Errorf("%d keys served, the chain holds %d", len(served), len(chain))
	}
	for _, k := range served {
		want, ok := chain[identity{k.OperatorIndex, k.Index}]
		switch {
		case !ok:
			t.Errorf("operator %d key %d is served and not on the chain", k.OperatorIndex, k.Index)
		case model.EncodeHex(want.PubkeyBin) != k.Key || model.EncodeHex(want.SignatureBin) != k.DepositSignature:
			t.Errorf("operator %d key %d: served pubkey or signature differs from the chain's", k.OperatorIndex, k.Index)
		case want.Used != k.Used || want.Vetted != k.Vetted:
			t.Errorf("operator %d key %d: served used %t vetted %t, chain used %t vetted %t",
				k.OperatorIndex, k.Index, k.Used, k.Vetted, want.Used, want.Vetted)
		}
	}
}
