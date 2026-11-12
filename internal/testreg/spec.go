// Package testreg generates self-describing keys: a pubkey's first bytes carry module, operator,
// index and salt, the rest hashes them, so a key from the wrong module or version fails alone.
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

// Spec: bumping Salt rewrites every pubkey (a module rewrite); flipping Used changes one row.
type Spec struct {
	Modules            int
	OperatorsPerModule int
	KeysPerOperator    int
	Salt               uint64
	// Nil means none deposited.
	Used func(module, operator, index int) bool
	// Group nil means no group registry (every module but curated-onchain-v2).
	Group func(module, operator int) *model.GroupID
	// Withdrawn nil means no counter (curated-onchain-v1).
	Withdrawn func(module, operator int) *int32
}

func Small() Spec {
	return Spec{Modules: 4, OperatorsPerModule: 4, KeysPerOperator: 25}
}

// Streaming outgrows the 256 KiB server buffer and socket buffers: each module is several MiB.
func Streaming() Spec {
	return Spec{Modules: 3, OperatorsPerModule: 8, KeysPerOperator: 2500}
}

func (s Spec) WithSalt(salt uint64) Spec { s.Salt = salt; return s }

func (s Spec) WithUsed(used func(module, operator, index int) bool) Spec {
	s.Used = used
	return s
}

func (s Spec) WithGroups(group func(module, operator int) *model.GroupID) Spec {
	s.Group = group
	return s
}

func (s Spec) WithWithdrawn(withdrawn func(module, operator int) *int32) Spec {
	s.Withdrawn = withdrawn
	return s
}

func (s Spec) TotalKeys() int { return s.Modules * s.OperatorsPerModule * s.KeysPerOperator }

func (s Spec) KeysPerModule() int { return s.OperatorsPerModule * s.KeysPerOperator }

// Module ids start at 1, like the real router's.
func (s Spec) ModuleIDs() []int {
	out := make([]int, 0, s.Modules)
	for id := 1; id <= s.Modules; id++ {
		out = append(out, id)
	}
	return out
}

func ModuleAddress(id int) string { return fmt.Sprintf("0x%040x", id) }

// Case-insensitive: the API serves checksummed addresses, the store lowercase ones.
func ModuleIDFromAddress(address string) (int, bool) {
	n, err := strconv.ParseInt(trimHex(address), 16, 64)
	if err != nil {
		return 0, false
	}
	return int(n), true
}

// The nonce moves with the salt: content changing without its nonce is a lie tests should not tell.
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

func (s Spec) ModuleSet() []model.StakingModule {
	out := make([]model.StakingModule, 0, s.Modules)
	for _, id := range s.ModuleIDs() {
		out = append(out, s.Module(id))
	}
	return out
}

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
			TotalWithdrawnKeys:         s.withdrawn(moduleID, op),
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

func (s Spec) withdrawn(module, operator int) *int32 {
	if s.Withdrawn == nil {
		return nil
	}
	return s.Withdrawn(module, operator)
}

func (s Spec) used(module, operator, index int) bool {
	if s.Used == nil {
		return false
	}
	return s.Used(module, operator, index)
}

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

// The identity is plain in the first bytes for readable failures; the tail hashes it so fakes fail.
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

// Derived from the pubkey, so a signature taken from another key fails.
func signatureFor(pubkey []byte) []byte {
	sig := make([]byte, signatureLen)
	block := sha256.Sum256(append([]byte("sig"), pubkey[:identityLen]...))
	for off := 0; off < signatureLen; off += len(block) {
		copy(sig[off:], block[:])
		block = sha256.Sum256(block[:])
	}
	return sig
}

type Identity struct {
	Module   int
	Operator int
	Index    int
	Salt     uint64
}

func (i Identity) String() string {
	return fmt.Sprintf("module %d operator %d index %d salt %d", i.Module, i.Operator, i.Index, i.Salt)
}

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
