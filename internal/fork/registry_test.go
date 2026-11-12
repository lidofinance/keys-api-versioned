//go:build fork

package fork

import (
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/ingest"
	"github.com/lidofinance/keys-api-versioned/internal/model"
)

// The only test reaching the operator-event path. A wrong event signature returns no logs and
// looks exactly like "nothing changed".
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

// Control: without the event scan the rename must go unnoticed, or the test above passes for
// another reason.
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

// removeSigningKeys moves the last key into the vacated slot, so key identity by
// (operator, index) is not stable.
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

	// The moved key's served index must be its new one; a key count cannot see this.
	moved := h.keyAt(SandboxOperator, int32(first))
	if moved == nil {
		t.Fatalf("nothing is served at operator %d index %d", SandboxOperator, first)
	}
	if moved.Key != "0x"+hexBytes(added[2]) {
		t.Errorf("index %d serves %s, want the key the contract moved there, %s",
			first, KeyLabel(mustDecode(t, moved.Key)), KeyLabel(added[2]))
	}
}

func TestADepositFlipsUsedOnExactlyOneKey(t *testing.T) {
	h := NewHarness(t, HarnessOpts{})
	h.requireSandboxPreconditions()

	h.Cycle()
	usedBefore := h.usedKeys()

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

// Hoodi's CSM holds duplicate pubkeys across operators; within one module after a slot move it
// would be a defect.
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
