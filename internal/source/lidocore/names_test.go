package lidocore

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

// The curated registry returns the name inside getNodeOperator. A name the canonical
// service cannot decode becomes `invalidName<index>` there, and has to here too.
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
