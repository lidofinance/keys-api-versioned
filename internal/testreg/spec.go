// Package testreg builds a deterministic registry for tests and verifies responses
// against it.
//
// The generated keys describe themselves: the first bytes of a pubkey carry the
// module, operator and key index it belongs to, plus a salt that stands for "the
// content moved". Everything else is a hash over those bytes. So a key served under
// the wrong module, or bytes from a version the response was not supposed to be
// reading, fail on the key alone — no comparison against a stored expectation, and
// no need to hold a 200 MiB dump in memory to notice.
//
// That is what makes the interesting tests possible. A response stitched together
// from two versions at a module boundary — the defect this schema was designed
// against — shows up as one module's keys carrying a different salt from another's,
// which is a local check on two keys rather than a diff of two dumps.
package testreg

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/lidofinance/keys-api-versioned/internal/model"
)

const (
	pubkeyLen    = 48
	signatureLen = 96
	// identityLen is how much of a pubkey is the identity; the rest is its hash.
	identityLen = 16
)

// Spec describes a registry: how many modules, operators and keys, and which keys
// count as deposited.
//
// Salt is the content generation. Bumping it changes every generated pubkey, which
// is how a test says "this module was rewritten"; flipping a key through Used changes
// one row, which is how it says "one deposit landed". The writer's diff distinguishes
// the two, so tests need both.
type Spec struct {
	Modules            int
	OperatorsPerModule int
	KeysPerOperator    int
	Salt               uint64
	// Used reports whether a key is deposited. Nil means none are.
	Used func(module, operator, index int) bool
	// Group reports an operator's node operator group. Nil, or a nil result, means
	// the module has no group registry — which is what every module but
	// curated-onchain-v2 looks like, and therefore the default.
	Group func(module, operator int) *model.GroupID
}

// Small is the default for semantics tests: enough shape to have module and operator
// boundaries, small enough that a whole version is written in milliseconds.
func Small() Spec {
	return Spec{Modules: 4, OperatorsPerModule: 4, KeysPerOperator: 25}
}

// Streaming is sized so a dump does not fit in the server's 256 KiB write buffer or
// in a socket buffer, which is what tests that hold a response open depend on: each
// module is several MiB, so the reader is genuinely inside module 1 when it blocks.
func Streaming() Spec {
	return Spec{Modules: 3, OperatorsPerModule: 8, KeysPerOperator: 2500}
}

// WithSalt returns the same registry with different content.
func (s Spec) WithSalt(salt uint64) Spec { s.Salt = salt; return s }

// WithUsed returns the same registry with a different deposited set.
func (s Spec) WithUsed(used func(module, operator, index int) bool) Spec {
	s.Used = used
	return s
}

// WithGroups returns the same registry with node operator groups.
func (s Spec) WithGroups(group func(module, operator int) *model.GroupID) Spec {
	s.Group = group
	return s
}

// TotalKeys is how many keys a full dump of this registry holds.
func (s Spec) TotalKeys() int { return s.Modules * s.OperatorsPerModule * s.KeysPerOperator }

// KeysPerModule is how many keys one module holds.
func (s Spec) KeysPerModule() int { return s.OperatorsPerModule * s.KeysPerOperator }

// ModuleIDs lists the module ids, which start at 1 like the real router's.
func (s Spec) ModuleIDs() []int {
	out := make([]int, 0, s.Modules)
	for id := 1; id <= s.Modules; id++ {
		out = append(out, id)
	}
	return out
}

// ModuleAddress is the address of a module id: derived from the id so a test can go
// from a served address back to the module without a lookup table.
func ModuleAddress(id int) string { return fmt.Sprintf("0x%040x", id) }

// ModuleIDFromAddress reverses ModuleAddress. Case-insensitive, because the API
// serves checksummed addresses and the store holds lowercase ones.
func ModuleIDFromAddress(address string) (int, bool) {
	n, err := strconv.ParseInt(trimHex(address), 16, 64)
	if err != nil {
		return 0, false
	}
	return int(n), true
}

// Module is the module row for an id. The nonce moves with the salt, because the
// freshness rules treat it as the per-module change counter and a version whose
// content moved without its nonce moving would be a lie the tests should not tell.
func (s Spec) Module(id int) model.StakingModule {
	m := model.StakingModule{
		ID:                        int32(id),
		Nonce:                     int32(100*id + int(s.Salt)),
		Type:                      "curated-onchain-v1",
		Name:                      fmt.Sprintf("module-%d", id),
		Active:                    true,
		WithdrawalCredentialsType: 1,
		TargetShare:               100,
		ModuleFee:                 5,
		TreasuryFee:               5,
	}
	m.SetRawAddress(ModuleAddress(id))
	return m
}

// ModuleSet is every module, in id order.
func (s Spec) ModuleSet() []model.StakingModule {
	out := make([]model.StakingModule, 0, s.Modules)
	for _, id := range s.ModuleIDs() {
		out = append(out, s.Module(id))
	}
	return out
}

// Operators are the operators of one module, with the counters implied by the spec.
func (s Spec) Operators(moduleID int) []model.Operator {
	out := make([]model.Operator, 0, s.OperatorsPerModule)
	for op := 0; op < s.OperatorsPerModule; op++ {
		used := 0
		for i := 0; i < s.KeysPerOperator; i++ {
			if s.used(moduleID, op, i) {
				used++
			}
		}
		out = append(out, model.Operator{
			Name:                       fmt.Sprintf("operator-%d-%d", moduleID, op),
			RewardAddress:              fmt.Sprintf("0x%036x%04x", moduleID, op),
			StakingLimit:               int32(s.KeysPerOperator),
			TotalSigningKeys:           int32(s.KeysPerOperator),
			UsedSigningKeys:            int32(used),
			Index:                      int32(op),
			Active:                     true,
			ModuleAddress:              ModuleAddress(moduleID),
			DepositableValidatorsCount: int32(s.KeysPerOperator - used),
			Group:                      s.group(moduleID, op),
		})
	}
	return out
}

func (s Spec) group(module, operator int) *model.GroupID {
	if s.Group == nil {
		return nil
	}
	return s.Group(module, operator)
}

func (s Spec) used(module, operator, index int) bool {
	if s.Used == nil {
		return false
	}
	return s.Used(module, operator, index)
}

// Key is the key at (module, operator, index) for this spec's salt.
func (s Spec) Key(moduleID, operator, index int) model.Key {
	pubkey := Pubkey(moduleID, operator, index, s.Salt)
	return model.Key{
		PubkeyBin:     pubkey,
		SignatureBin:  signatureFor(pubkey),
		OperatorIndex: int32(operator),
		Index:         int32(index),
		Used:          s.used(moduleID, operator, index),
		Vetted:        true,
		ModuleAddress: ModuleAddress(moduleID),
	}
}

// Pubkey builds the self-describing pubkey. The identity is plain in the first bytes
// so a failure message can say what the key claims to be; the tail is a hash of it so
// a corrupted or fabricated key does not pass.
func Pubkey(moduleID, operator, index int, salt uint64) []byte {
	key := make([]byte, pubkeyLen)
	binary.BigEndian.PutUint16(key[0:2], uint16(moduleID))
	binary.BigEndian.PutUint32(key[2:6], uint32(operator))
	binary.BigEndian.PutUint32(key[6:10], uint32(index))
	putUint48(key[10:16], salt)

	sum := sha256.Sum256(key[:identityLen])
	copy(key[identityLen:], sum[:])
	return key
}

// signatureFor is derived from the pubkey, so a key whose signature was taken from
// another key does not pass either.
func signatureFor(pubkey []byte) []byte {
	sig := make([]byte, signatureLen)
	block := sha256.Sum256(append([]byte("sig"), pubkey[:identityLen]...))
	for off := 0; off < signatureLen; off += len(block) {
		copy(sig[off:], block[:])
		block = sha256.Sum256(block[:])
	}
	return sig
}

// Identity is what a generated pubkey carries.
type Identity struct {
	Module   int
	Operator int
	Index    int
	Salt     uint64
}

func (i Identity) String() string {
	return fmt.Sprintf("module %d operator %d index %d salt %d", i.Module, i.Operator, i.Index, i.Salt)
}

// Decode reads the identity out of a pubkey and checks its hash tail. The second
// return is false for anything this package did not generate.
func Decode(pubkey []byte) (Identity, bool) {
	if len(pubkey) != pubkeyLen {
		return Identity{}, false
	}
	sum := sha256.Sum256(pubkey[:identityLen])
	for i := identityLen; i < pubkeyLen; i++ {
		if pubkey[i] != sum[i-identityLen] {
			return Identity{}, false
		}
	}
	return Identity{
		Module:   int(binary.BigEndian.Uint16(pubkey[0:2])),
		Operator: int(binary.BigEndian.Uint32(pubkey[2:6])),
		Index:    int(binary.BigEndian.Uint32(pubkey[6:10])),
		Salt:     uint48(pubkey[10:16]),
	}, true
}

// HashAt is a block hash derived from a block number, so a test's chain identities
// are stable and readable in failure messages.
func HashAt(block int64) string { return fmt.Sprintf("0x%064x", block) }

func putUint48(b []byte, v uint64) {
	b[0] = byte(v >> 40)
	b[1] = byte(v >> 32)
	b[2] = byte(v >> 24)
	b[3] = byte(v >> 16)
	b[4] = byte(v >> 8)
	b[5] = byte(v)
}

func uint48(b []byte) uint64 {
	return uint64(b[0])<<40 | uint64(b[1])<<32 | uint64(b[2])<<24 |
		uint64(b[3])<<16 | uint64(b[4])<<8 | uint64(b[5])
}

func trimHex(s string) string {
	if len(s) >= 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X') {
		return s[2:]
	}
	return s
}
