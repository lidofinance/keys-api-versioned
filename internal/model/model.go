// Package model holds the wire types served by the API.
//
// Field order in every struct matches the JSON property order produced by the
// canonical lido-keys-api DTOs (property assignment order in their
// constructors, since it is compiled with target es2017). Keeping the order
// identical lets the diff runner compare raw bytes, not only parsed values.
package model

import (
	"encoding/hex"
	"strings"
)

// Key is a single signing key as served by /v1/keys and /v1/modules/{id}/keys.
//
// A binary store fills PubkeyBin/SignatureBin instead of the string fields, and
// the renderer hex-encodes straight into the response buffer. Only one of the
// two forms is populated; the JSON output is identical either way.
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

// EncodeHex renders bytes the way the API does: lowercase hex with a 0x prefix.
func EncodeHex(b []byte) string {
	if b == nil {
		return ""
	}
	return "0x" + hex.EncodeToString(b)
}

// DecodeHex parses a 0x-prefixed hex string into bytes.
func DecodeHex(s string) ([]byte, error) {
	return hex.DecodeString(strings.TrimPrefix(strings.ToLower(s), "0x"))
}

// Operator is a node operator as served by /v1/operators and the module
// operator endpoints.
//
// Field order follows the canonical Operator DTO constructor, so responses are
// comparable byte for byte. FinalizedUsedSigningKeys is here for completeness and
// is neither read nor served: the canonical DTO omits it, and reading it costs a
// second round of per-operator calls at the finalized block tag.
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

	FinalizedUsedSigningKeys int32 `json:"-"`
}

// ELBlockSnapshot describes the execution layer block the data is consistent with.
type ELBlockSnapshot struct {
	BlockNumber          int64  `json:"blockNumber"`
	BlockHash            string `json:"blockHash"`
	Timestamp            int64  `json:"timestamp"`
	LastChangedBlockHash string `json:"lastChangedBlockHash"`
}

// StakingModule is a staking router module as served by /v1/modules.
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

	// rawAddress keeps the lowercase address from the database; the JSON field
	// above carries the EIP-55 checksummed form.
	rawAddress string `json:"-"`
}

// SetRawAddress stores the lowercase module address used for database lookups.
func (m *StakingModule) SetRawAddress(a string) { m.rawAddress = a }

// RawAddress returns the lowercase module address used for database lookups.
func (m *StakingModule) RawAddress() string { return m.rawAddress }

// Status is the /v1/status payload.
type Status struct {
	AppVersion      string           `json:"appVersion"`
	ChainID         int64            `json:"chainId"`
	ELBlockSnapshot *ELBlockSnapshot `json:"elBlockSnapshot"`
	CLBlockSnapshot *CLBlockSnapshot `json:"clBlockSnapshot"`
}

// CLBlockSnapshot mirrors the consensus layer meta of the canonical service.
// The PoC does not ingest validators, so it is always null for now.
type CLBlockSnapshot struct {
	EpochNumber   int64  `json:"epoch"`
	Root          string `json:"root"`
	Slot          int64  `json:"slot"`
	BlockNumber   int64  `json:"blockNumber"`
	Timestamp     int64  `json:"timestamp"`
	BlockHash     string `json:"blockHash"`
	SlotStateRoot string `json:"slotStateRoot"`
}

// KeysFilter carries the query filters supported by the keys endpoints.
type KeysFilter struct {
	Used          *bool
	OperatorIndex *int32
	ModuleAddress []string // lowercase addresses; empty means every module
}

// OperatorsFilter carries the query filters supported by the operator endpoints.
type OperatorsFilter struct {
	Index *int32
}
