//go:build fork

package fork

import (
	"crypto/sha256"
	"fmt"

	"github.com/lidofinance/keys-api-versioned/internal/testreg"
)

// Self-describing keys, on a real registry.
//
// internal/testreg's whole leverage is that a generated pubkey carries the module,
// operator, index and content generation it belongs to, so a wrong key fails on the
// key alone rather than on a comparison against a stored dump. That looked like it
// could not survive the move to a chain, where keys are whatever the operators put
// there.
//
// It survives, because NodeOperatorsRegistry does not validate what a pubkey is. The
// only checks in SigningKeys.saveKeysSigs are the array lengths and that the key is
// not all zero — BLS validity is the deposit contract's business, not the registry's.
// So a fork test injects keys it can recognise into the real contract, and every
// assertion about "which key came back" is local.
//
// The identity layout is testreg's, reused rather than reimplemented, so a failure
// message reads the same on both sides of the suite.

// MintKey builds a recognisable 48-byte pubkey. Generation stands for "the content
// moved": two keys at the same slot with different generations are the two branches
// of a reorg, which is what makes T1 assertable.
func MintKey(module, operator, index int, generation uint64) []byte {
	return testreg.Pubkey(module, operator, index, generation)
}

// MintKeys builds a run of keys starting at an index.
func MintKeys(module, operator, firstIndex, count int, generation uint64) [][]byte {
	out := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		out = append(out, MintKey(module, operator, firstIndex+i, generation))
	}
	return out
}

// Signature derives the deposit signature from the pubkey, so a key whose signature
// was taken from another key does not pass either. The registry does not check it;
// the tests do.
func Signature(pubkey []byte) []byte {
	sig := make([]byte, 96)
	block := sha256.Sum256(append([]byte("fork-sig"), pubkey[:16]...))
	for off := 0; off < len(sig); off += len(block) {
		copy(sig[off:], block[:])
		block = sha256.Sum256(block[:])
	}
	return sig
}

// Identity is what a minted key says about itself.
type Identity = testreg.Identity

// Describe decodes a minted key, and reports false for a key of the chain's own —
// which is most of them, since a fork test adds a handful to a module that already
// holds dozens.
func Describe(pubkey []byte) (Identity, bool) { return testreg.Decode(pubkey) }

// KeyLabel renders a key for a failure message: its identity if we minted it, its
// first bytes otherwise.
func KeyLabel(pubkey []byte) string {
	if id, ok := Describe(pubkey); ok {
		return fmt.Sprintf("minted(module=%d operator=%d index=%d generation=%d)",
			id.Module, id.Operator, id.Index, id.Salt)
	}
	if len(pubkey) > 6 {
		return fmt.Sprintf("chain key 0x%x…", pubkey[:6])
	}
	return fmt.Sprintf("0x%x", pubkey)
}
