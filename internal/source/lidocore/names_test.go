package lidocore

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

// A name the canonical service cannot decode becomes `invalidName<index>`; here too.
func TestCuratedUndecodableNamesGetThePlaceholder(t *testing.T) {
	for _, tc := range []struct {
		name string
		want string
	}{
		{"Lido node operator", "Lido node operator"},
		{"bad\xc3", "invalidName7"},
		{"overlong\xc0\xaf", "invalidName7"},
		{"surrogate\xed\xa0\x80", "invalidName7"},
		{"nul\x00", "invalidName7"},
		{"", ""},
	} {
		values := []any{true, tc.name, common.Address{}, big.NewInt(3), big.NewInt(0), big.NewInt(5), big.NewInt(2)}
		op, err := decodeCuratedOperator(7, "0xmodule", values)
		if err != nil {
			t.Fatalf("%q: %v", tc.name, err)
		}
		if op.Name != tc.want {
			t.Errorf("%q decoded as %q, want %q", tc.name, op.Name, tc.want)
		}
		// The curated registry has no withdrawn-keys counter: null on the wire.
		if op.TotalWithdrawnKeys != nil {
			t.Errorf("%q: curated operator carries totalWithdrawnKeys %d, want nil", tc.name, *op.TotalWithdrawnKeys)
		}
	}
}

func TestUndecodableModuleNamesAndTypesGetAPlaceholder(t *testing.T) {
	type tuple struct {
		Id                   uint16
		StakingModuleAddress [20]byte
		Name                 string
	}
	modules, err := decodeStakingModules([]tuple{{Id: 3, Name: "nul\x00name"}, {Id: 4, Name: "bad\xc3"}, {Id: 5, Name: "fine"}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"invalidName3", "invalidName4", "fine"} {
		if modules[i].name != want {
			t.Errorf("module %d name %q, want %q", modules[i].id, modules[i].name, want)
		}
	}

	var raw [32]byte
	copy(raw[:], "curated\x00\xffv1")
	if got, _ := decodeModuleType(raw); got != "invalidType" {
		t.Errorf("type with NUL and invalid UTF-8 decoded as %q", got)
	}
	copy(raw[:], "curated-onchain-v1")
	if got, _ := decodeModuleType([32]byte(append([]byte("curated-onchain-v1"), make([]byte, 14)...))); got != "curated-onchain-v1" {
		t.Errorf("a padded type decoded as %q", got)
	}
}
