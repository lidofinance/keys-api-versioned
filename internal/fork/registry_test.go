//go:build fork

package fork

import (
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/ingest"
	"github.com/lidofinance/keys-api-versioned/internal/model"
)

// TestARenameIsPickedUpEvenThoughItMovesNoNonce exercises the one path in the loop
// that no other test in this repo reaches at all.
//
// The scripted double in internal/ingest does not implement source.OperatorWatcher, so
// operatorEvents short-circuits and the whole operator-event branch — the log scan at
// provider.go:335, the "assume it changed" fallback at sync.go:236, the rule at
// freshness.go:117 — has never run under a test. It is also the path docs/next-steps.md
// flags as the one where a mistake is invisible: a wrong event signature yields a wrong
// topic hash, which returns no logs and looks exactly like "nothing changed".
//
// setNodeOperatorName moves no nonce. Verified on the chain here rather than read off
// the contract, because the whole test is void if it does.
func TestARenameIsPickedUpEvenThoughItMovesNoNonce(t *testing.T) {
	h := NewHarness(t, HarnessOpts{})
	h.requireSandboxPreconditions()

	h.Cycle()

	before := h.Sandbox.Nonce()
	renamed := "renamed by the fork suite"
	h.Sandbox.Rename(SandboxOperator, renamed)

	if after := h.Sandbox.Nonce(); after != before {
		t.Fatalf("the rename moved the nonce %d → %d: this test is about the case where "+
			"it does not, so either the contract changed or the wrong call was used", before, after)
	}

	result := h.Cycle()
	if !result.Rebuilt {
		t.Errorf("a rename that moves no nonce did not trigger a rebuild: %s", result.Reason)
	}
	if got := h.operatorName(SandboxOperator); got != renamed {
		t.Errorf("the served operator name is %q, want %q: the only thing that can notice "+
			"a rename is the operator event scan", got, renamed)
	}
}

// TestARenameGoesUnnoticedWithoutTheEventScan is the control for the test above, and
// it is what makes it worth having.
//
// With source.OperatorWatcher hidden the loop has only the nonce, which did not move,
// so the rename must not be picked up. If this one also saw the new name, the test
// above would be passing for some other reason — a coincidental rebuild — and would
// keep passing with a wrong topic hash, which is the exact failure it is written to
// rule out.
func TestARenameGoesUnnoticedWithoutTheEventScan(t *testing.T) {
	h := NewHarness(t, HarnessOpts{NoOperatorWatcher: true})
	h.requireSandboxPreconditions()

	h.Cycle()
	original := h.operatorName(SandboxOperator)

	h.Sandbox.Rename(SandboxOperator, "renamed with the scan switched off")

	result := h.Cycle()
	if result.Action != ingest.ActionExtend {
		t.Errorf("without the event scan a rename decided %q (%s), want extend",
			result.Action, result.Reason)
	}
	if got := h.operatorName(SandboxOperator); got != original {
		t.Errorf("the served name changed to %q without the event scan: the nonce did not "+
			"move, so nothing should have noticed", got)
	}
}

// TestRemovingAKeyMovesTheLastKeyIntoItsSlot is the contract semantic that breaks an
// assumption the service is written on.
//
// source.go:33 states that key identity is (operator index, key index) and that it is
// "stable for the life of the key". NodeOperatorsRegistry disagrees: removeSigningKeys
// moves the *last* key into the vacated slot and shortens the array, so one removal
// changes which key lives at that index. The keys here are minted, so the test can say
// exactly which key ended up where rather than counting.
func TestRemovingAKeyMovesTheLastKeyIntoItsSlot(t *testing.T) {
	h := NewHarness(t, HarnessOpts{})
	h.requireSandboxPreconditions()

	h.Cycle()

	first := int(h.Sandbox.Operator(SandboxOperator).TotalSigningKeys)
	added := MintKeys(SandboxModuleID, SandboxOperator, first, 3, 1)
	h.Sandbox.AddKeys(SandboxOperator, added)
	h.Cycle()

	for i, key := range added {
		if !h.holdsKey(key) {
			t.Fatalf("the key added at index %d is not in the published version", first+i)
		}
	}

	// Remove the first of the three. The contract moves the third into its slot.
	h.Sandbox.RemoveKeys(SandboxOperator, int64(first), 1)

	onChain := h.Sandbox.SigningKeys(SandboxOperator, int64(first), 2)
	if len(onChain) != 2 {
		t.Fatalf("the operator holds %d keys from index %d, want 2", len(onChain), first)
	}
	if hexBytes(onChain[0]) != hexBytes(added[2]) {
		t.Fatalf("the contract did not move the last key into the vacated slot: index %d holds %s, want %s. "+
			"If this changed, the assumption in source.go:33 may now hold and this test is obsolete",
			first, KeyLabel(onChain[0]), KeyLabel(added[2]))
	}

	result := h.Cycle()
	if !result.Rebuilt {
		t.Fatalf("removing a key did not rebuild: %s", result.Reason)
	}

	// What the service serves has to be the chain's set: two of the three added keys,
	// the removed one gone, no duplicate of the key that moved.
	if h.holdsKey(added[0]) {
		t.Errorf("the removed key %s is still served", KeyLabel(added[0]))
	}
	for _, key := range [][]byte{added[1], added[2]} {
		if !h.holdsKey(key) {
			t.Errorf("%s survived on the chain but is not served", KeyLabel(key))
		}
	}
	if got, want := len(h.Keys(SandboxAddress)), SandboxKeys+2; got != want {
		t.Errorf("the module serves %d keys, want %d", got, want)
	}
	if dup := h.duplicateKeys(); len(dup) > 0 {
		t.Errorf("a key that moved slots is served twice: %v", dup)
	}

	// The index the moved key is served under has to be its new one. Getting this
	// wrong is invisible to a key count, which is what makes it worth asserting.
	moved := h.keyAt(SandboxOperator, int32(first))
	if moved == nil {
		t.Fatalf("nothing is served at operator %d index %d", SandboxOperator, first)
	}
	if moved.Key != "0x"+hexBytes(added[2]) {
		t.Errorf("index %d serves %s, want the key the contract moved there, %s",
			first, KeyLabel(mustDecode(t, moved.Key)), KeyLabel(added[2]))
	}
}

// TestADepositFlipsUsedOnExactlyOneKey is the other mutation that moves the nonce, and
// the one a consumer reads most: `used` is what ethereum-validators-monitoring filters
// on.
//
// Vetting and depositing are two separate roles — the limit is an Aragon role, the
// deposit is the staking router's — and both are taken by impersonation. No ether
// moves; obtainDepositData is what flips the flag and bumps the counter.
func TestADepositFlipsUsedOnExactlyOneKey(t *testing.T) {
	h := NewHarness(t, HarnessOpts{})
	h.requireSandboxPreconditions()

	h.Cycle()
	usedBefore := h.usedKeys()

	// One key becomes depositable, then is deposited.
	h.Sandbox.Vet(SandboxOperator, 1)
	h.Cycle()
	h.Sandbox.Deposit(1)

	operator := h.Sandbox.Operator(SandboxOperator)
	if operator.UsedSigningKeys != 1 {
		t.Fatalf("the operator reports %d deposited keys after one deposit, want 1: "+
			"the scenario did not deposit what it meant to", operator.UsedSigningKeys)
	}

	result := h.Cycle()
	if !result.Rebuilt {
		t.Fatalf("a deposit did not rebuild: %s", result.Reason)
	}

	if got, want := len(h.usedKeys()), len(usedBefore)+1; got != want {
		t.Errorf("%d keys are served as used, want %d", got, want)
	}
	deposited := h.keyAt(SandboxOperator, 0)
	if deposited == nil {
		t.Fatal("nothing is served at the deposited slot")
	}
	if !deposited.Used {
		t.Error("the deposited key is not served as used")
	}
}

// operatorName is the served name of one Sandbox operator.
func (h *Harness) operatorName(index int32) string {
	h.tb.Helper()

	for _, op := range h.Operators(SandboxAddress) {
		if op.Index == index {
			return op.Name
		}
	}
	h.tb.Fatalf("operator %d is not in the published version", index)
	return ""
}

// keyAt is the served key at one operator slot, or nil.
func (h *Harness) keyAt(operator, index int32) *model.Key {
	h.tb.Helper()

	for _, key := range h.Keys(SandboxAddress) {
		if key.OperatorIndex == operator && key.Index == index {
			found := key
			return &found
		}
	}
	return nil
}

// usedKeys is every served key marked deposited.
func (h *Harness) usedKeys() []model.Key {
	h.tb.Helper()

	var out []model.Key
	for _, key := range h.Keys(SandboxAddress) {
		if key.Used {
			out = append(out, key)
		}
	}
	return out
}

// duplicateKeys reports pubkeys served more than once for this module.
//
// Worth checking rather than assuming: Hoodi's CSM genuinely holds duplicate pubkeys
// across operators, so duplication is not by itself a defect — but a key duplicated
// inside one module after a slot move would be.
func (h *Harness) duplicateKeys() []string {
	h.tb.Helper()

	seen := map[string]int{}
	for _, key := range h.Keys(SandboxAddress) {
		seen[key.Key]++
	}
	var dup []string
	for key, n := range seen {
		if n > 1 {
			dup = append(dup, key)
		}
	}
	return dup
}

func mustDecode(t *testing.T, hex string) []byte {
	t.Helper()
	out, err := decodeHex(hex)
	if err != nil {
		t.Fatalf("decode %s: %v", hex, err)
	}
	return out
}
