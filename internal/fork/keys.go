//go:build fork

package fork

import (
	"crypto/sha256"
	"fmt"

	"github.com/lidofinance/keys-api-versioned/internal/testreg"
)

// NodeOperatorsRegistry does not validate pubkeys (only lengths and non-zero), so tests inject
// testreg's self-describing keys into the real contract.

// MintKey: two keys at one slot with different generations are the two branches of a reorg.
func MintKey(module, operator, index int, generation uint64) []byte {
	return testreg.Pubkey(module, operator, index, generation)
}

func MintKeys(module, operator, firstIndex, count int, generation uint64) [][]byte {
	out := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		out = append(out, MintKey(module, operator, firstIndex+i, generation))
	}
	return out
}

// Signature is derived from the pubkey so a borrowed signature fails; the registry doesn't check it.
func Signature(pubkey []byte) []byte {
	sig := make([]byte, 96)
	block := sha256.Sum256(append([]byte("fork-sig"), pubkey[:16]...))
	for off := 0; off < len(sig); off += len(block) {
		copy(sig[off:], block[:])
		block = sha256.Sum256(block[:])
	}
	return sig
}

type Identity = testreg.Identity

func Describe(pubkey []byte) (Identity, bool) { return testreg.Decode(pubkey) }

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
