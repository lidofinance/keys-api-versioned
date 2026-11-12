// Package model holds the API wire types. Struct field order matches the canonical DTOs' JSON
// property order, so the diff runner can compare raw bytes.
package model

import (
	"encoding/hex"
	"strconv"
	"strings"
)

// Key carries either the hex strings or PubkeyBin/SignatureBin (binary store), never both.
type Key struct {
	Key              string `json:"key"`
	DepositSignature string `json:"depositSignature"`
	OperatorIndex    int32  `json:"operatorIndex"`
	Used             bool   `json:"used"`
	ModuleAddress    string `json:"moduleAddress"`
	Index            int32  `json:"index"`
	Vetted           bool   `json:"vetted"`

	PubkeyBin    []byte `json:"-"`
	SignatureBin []byte `json:"-"`
}

// EnsureHex is needed on encoding/json paths: a binary-store key otherwise marshals "key": "".
func (k *Key) EnsureHex() {
	if k.Key == "" && k.PubkeyBin != nil {
		k.Key = EncodeHex(k.PubkeyBin)
	}
	if k.DepositSignature == "" && k.SignatureBin != nil {
		k.DepositSignature = EncodeHex(k.SignatureBin)
	}
}

func EncodeHex(b []byte) string {
	if b == nil {
		return ""
	}
	return "0x" + hex.EncodeToString(b)
}

func DecodeHex(s string) ([]byte, error) {
	return hex.DecodeString(strings.TrimPrefix(strings.ToLower(s), "0x"))
}

// Operator: FinalizedUsedSigningKeys is never read or served (the canonical DTO omits it).
type Operator struct {
	Name                       string `json:"name"`
	RewardAddress              string `json:"rewardAddress"`
	StakingLimit               int32  `json:"stakingLimit"`
	StoppedValidators          int32  `json:"stoppedValidators"`
	TotalSigningKeys           int32  `json:"totalSigningKeys"`
	UsedSigningKeys            int32  `json:"usedSigningKeys"`
	Index                      int32  `json:"index"`
	Active                     bool   `json:"active"`
	ModuleAddress              string `json:"moduleAddress"`
	DepositableValidatorsCount int32  `json:"depositableValidatorsCount"`
	// TotalWithdrawnKeys is null for curated-onchain-v1, which has no such counter,
	// and a number (0 included) for the CSM-shaped modules. Canonical since 5.0.0.
	TotalWithdrawnKeys *int32 `json:"totalWithdrawnKeys"`

	FinalizedUsedSigningKeys int32 `json:"-"`

	// Group is nil when the module has no group registry (all but curated-onchain-v2). Untagged: /v1
	// and /v2 stay byte-identical to the canonical DTO; the extended surface renders it.
	Group *GroupID `json:"-"`
}

// NoGroupID is the on-chain MetaRegistry sentinel for "no group": literally zero.
const NoGroupID GroupID = 0

// GroupID renders as null for no group: serving 0 would invite grouping all ungrouped operators.
type GroupID int32

func (g GroupID) MarshalJSON() ([]byte, error) {
	if g == NoGroupID {
		return []byte("null"), nil
	}
	return strconv.AppendInt(nil, int64(g), 10), nil
}

type ELBlockSnapshot struct {
	BlockNumber          int64  `json:"blockNumber"`
	BlockHash            string `json:"blockHash"`
	Timestamp            int64  `json:"timestamp"`
	LastChangedBlockHash string `json:"lastChangedBlockHash"`
}

type StakingModule struct {
	Nonce                     int32  `json:"nonce"`
	Type                      string `json:"type"`
	ID                        int32  `json:"id"`
	StakingModuleAddress      string `json:"stakingModuleAddress"`
	ModuleFee                 int32  `json:"moduleFee"`
	TreasuryFee               int32  `json:"treasuryFee"`
	TargetShare               int32  `json:"targetShare"`
	Status                    int32  `json:"status"`
	Name                      string `json:"name"`
	LastDepositAt             int64  `json:"lastDepositAt"`
	LastDepositBlock          int64  `json:"lastDepositBlock"`
	ExitedValidatorsCount     int32  `json:"exitedValidatorsCount"`
	Active                    bool   `json:"active"`
	WithdrawalCredentialsType int32  `json:"withdrawalCredentialsType"`
	LastChangedBlockHash      string `json:"lastChangedBlockHash"`

	// rawAddress is the lowercase DB form; StakingModuleAddress is EIP-55 checksummed.
	rawAddress string `json:"-"`
}

func (m *StakingModule) SetRawAddress(a string) { m.rawAddress = a }

func (m *StakingModule) RawAddress() string { return m.rawAddress }

type Status struct {
	AppVersion      string           `json:"appVersion"`
	ChainID         int64            `json:"chainId"`
	ELBlockSnapshot *ELBlockSnapshot `json:"elBlockSnapshot"`
	CLBlockSnapshot *CLBlockSnapshot `json:"clBlockSnapshot"`
}

// CLBlockSnapshot is always null: validators are not ingested.
type CLBlockSnapshot struct {
	EpochNumber   int64  `json:"epoch"`
	Root          string `json:"root"`
	Slot          int64  `json:"slot"`
	BlockNumber   int64  `json:"blockNumber"`
	Timestamp     int64  `json:"timestamp"`
	BlockHash     string `json:"blockHash"`
	SlotStateRoot string `json:"slotStateRoot"`
}

type KeysFilter struct {
	Used          *bool
	OperatorIndex *int32
	ModuleAddress []string // lowercase addresses; empty means every module
}

type OperatorsFilter struct {
	Index *int32
}
