// Package ethaddr implements EIP-55 address checksumming.
//
// The canonical service calls ethers.utils.getAddress() once per key while
// serializing the keys stream, which means a keccak256 hash per key. Module
// addresses repeat across millions of keys, so this package memoizes them.
package ethaddr

import (
	"strings"
	"sync"

	"golang.org/x/crypto/sha3"
)

var cache sync.Map // lowercase address -> checksummed address

// Checksum returns the EIP-55 representation of a hex address.
// Results are memoized, so the hash is computed once per distinct address.
func Checksum(address string) string {
	if v, ok := cache.Load(address); ok {
		return v.(string)
	}
	out := checksum(address)
	cache.Store(address, out)
	return out
}

func checksum(address string) string {
	lower := strings.ToLower(strings.TrimPrefix(address, "0x"))

	h := sha3.NewLegacyKeccak256()
	h.Write([]byte(lower))
	hash := h.Sum(nil)

	out := make([]byte, 0, len(lower)+2)
	out = append(out, '0', 'x')
	for i := 0; i < len(lower); i++ {
		c := lower[i]
		if c >= 'a' && c <= 'f' {
			// Nibble i of the hash decides the case of character i.
			nibble := hash[i/2]
			if i%2 == 0 {
				nibble >>= 4
			} else {
				nibble &= 0x0f
			}
			if nibble >= 8 {
				c -= 32 // to upper case
			}
		}
		out = append(out, c)
	}
	return string(out)
}
