package lidocore

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"reflect"
	"strings"

	"github.com/lidofinance/keys-api-versioned/internal/model"
)

// The ABI decoder builds anonymous struct types at runtime, so tuple returns are
// read by reflection rather than by type assertion. Field names come from the ABI
// files, which are the canonical service's own — see internal/eth/abi.

const (
	pubkeyBytes    = 48
	signatureBytes = 96
)

type rawModule struct {
	id                        int32
	address                   string
	moduleFee                 int32
	treasuryFee               int32
	targetShare               int32
	status                    int32
	name                      string
	lastDepositAt             int64
	lastDepositBlock          int64
	exitedValidatorsCount     int32
	withdrawalCredentialsType int32
}

func decodeStakingModules(v any) ([]rawModule, error) {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice {
		return nil, fmt.Errorf("getStakingModules: expected a slice, got %T", v)
	}

	out := make([]rawModule, 0, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		item := rv.Index(i)
		m := rawModule{
			id:                    int32(fieldInt(item, "Id")),
			address:               strings.ToLower(fieldAddress(item, "StakingModuleAddress")),
			moduleFee:             int32(fieldInt(item, "StakingModuleFee")),
			treasuryFee:           int32(fieldInt(item, "TreasuryFee")),
			targetShare:           int32(fieldInt(item, "StakeShareLimit")),
			status:                int32(fieldInt(item, "Status")),
			name:                  fieldString(item, "Name"),
			lastDepositAt:         fieldInt(item, "LastDepositAt"),
			lastDepositBlock:      fieldInt(item, "LastDepositBlock"),
			exitedValidatorsCount: int32(fieldInt(item, "ExitedValidatorsCount")),
		}
		// Present only from staking router v4; older versions imply type 1.
		if f := item.FieldByName("WithdrawalCredentialsType"); f.IsValid() {
			m.withdrawalCredentialsType = int32(fieldInt(item, "WithdrawalCredentialsType"))
		} else {
			m.withdrawalCredentialsType = 1
		}
		if m.withdrawalCredentialsType == 0 {
			m.withdrawalCredentialsType = 1
		}
		out = append(out, m)
	}
	return out, nil
}

func decodeModuleType(v any) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case [32]byte:
		// getType returns bytes32 on some modules; trim the padding.
		return strings.TrimRight(string(t[:]), "\x00"), nil
	default:
		return "", fmt.Errorf("getType: unexpected type %T", v)
	}
}

// decodeCuratedOperator reads getNodeOperator(i, true), whose outputs are, in
// ABI order: active, name, rewardAddress, totalVettedValidators,
// totalExitedValidators, totalAddedValidators, totalDepositedValidators.
func decodeCuratedOperator(index int32, moduleAddress string, values []any) (model.Operator, error) {
	if len(values) < 7 {
		return model.Operator{}, fmt.Errorf("getNodeOperator: expected 7 values, got %d", len(values))
	}

	active, ok := values[0].(bool)
	if !ok {
		return model.Operator{}, fmt.Errorf("getNodeOperator: active is %T", values[0])
	}
	name, ok := values[1].(string)
	if !ok {
		return model.Operator{}, fmt.Errorf("getNodeOperator: name is %T", values[1])
	}

	return model.Operator{
		Name:              name,
		RewardAddress:     strings.ToLower(addressOf(values[2])),
		StakingLimit:      int32(intOf(values[3])),
		StoppedValidators: int32(intOf(values[4])),
		TotalSigningKeys:  int32(intOf(values[5])),
		UsedSigningKeys:   int32(intOf(values[6])),
		Index:             index,
		Active:            active,
		ModuleAddress:     moduleAddress,
	}, nil
}

// decodeCommunityOperator reads the CSM getNodeOperator struct.
//
// CSM has no notion of an inactive operator — getNodeOperatorIsActive only checks
// that the id exists — and we only ask for ids below the count, so active is true
// by construction. The canonical service reasons the same way.
func decodeCommunityOperator(index int32, moduleAddress string, values []any) (model.Operator, error) {
	if len(values) == 0 {
		return model.Operator{}, fmt.Errorf("getNodeOperator: empty result")
	}
	s := reflect.ValueOf(values[0])
	if s.Kind() != reflect.Struct {
		return model.Operator{}, fmt.Errorf("getNodeOperator: expected a struct, got %T", values[0])
	}

	return model.Operator{
		RewardAddress:     strings.ToLower(fieldAddress(s, "RewardAddress")),
		StakingLimit:      int32(fieldInt(s, "TotalVettedKeys")),
		StoppedValidators: int32(fieldInt(s, "TotalExitedKeys")),
		TotalSigningKeys:  int32(fieldInt(s, "TotalAddedKeys")),
		UsedSigningKeys:   int32(fieldInt(s, "TotalDepositedKeys")),
		Index:             index,
		Active:            true,
		ModuleAddress:     moduleAddress,
	}, nil
}

// depositableFromSummary picks depositableValidatorsCount out of
// getNodeOperatorSummary, whose last output it is in both ABIs.
func depositableFromSummary(values []any) int64 {
	if len(values) == 0 {
		return 0
	}
	return intOf(values[len(values)-1])
}

// metadataName picks the name out of the MetaRegistry metadata struct.
func metadataName(values []any) string {
	if len(values) == 0 {
		return ""
	}
	s := reflect.ValueOf(values[0])
	if s.Kind() != reflect.Struct {
		return ""
	}
	return fieldString(s, "Name")
}

// decodeCuratedKeys splits the packed return of getSigningKeys into keys.
//
// The contract returns all pubkeys concatenated (48 bytes each), all signatures
// concatenated (96 bytes each) and a used flag per key. A key is vetted when its
// index is below the operator's vetted count — the same rule the canonical
// service applies.
func decodeCuratedKeys(values []any, moduleAddress string, op model.Operator, offset int32) ([]model.Key, error) {
	if len(values) < 3 {
		return nil, fmt.Errorf("getSigningKeys: expected 3 values, got %d", len(values))
	}

	pubkeys, ok := values[0].([]byte)
	if !ok {
		return nil, fmt.Errorf("getSigningKeys: pubkeys is %T", values[0])
	}
	signatures, ok := values[1].([]byte)
	if !ok {
		return nil, fmt.Errorf("getSigningKeys: signatures is %T", values[1])
	}
	used, ok := values[2].([]bool)
	if !ok {
		return nil, fmt.Errorf("getSigningKeys: used is %T", values[2])
	}

	if len(pubkeys) != len(used)*pubkeyBytes || len(signatures) != len(used)*signatureBytes {
		return nil, fmt.Errorf("getSigningKeys: %d keys, %d pubkey bytes, %d signature bytes",
			len(used), len(pubkeys), len(signatures))
	}

	out := make([]model.Key, 0, len(used))
	for i := range used {
		index := offset + int32(i)
		out = append(out, model.Key{
			// Kept as bytes all the way through: the contract returns bytes, the
			// store holds bytes, and the response encoder writes the hex straight
			// into the output buffer. Nothing in between allocates a string.
			PubkeyBin:     pubkeys[i*pubkeyBytes : (i+1)*pubkeyBytes],
			SignatureBin:  signatures[i*signatureBytes : (i+1)*signatureBytes],
			OperatorIndex: op.Index,
			Used:          used[i],
			ModuleAddress: moduleAddress,
			Index:         index,
			Vetted:        index < op.StakingLimit,
		})
	}
	return out, nil
}

// decodeCommunityKeys splits the packed return of getSigningKeysWithSignatures.
//
// There is no used array here: a key is deposited when its index is below the
// operator's deposited count, and vetted when it is below the vetted count.
func decodeCommunityKeys(values []any, moduleAddress string, op model.Operator, offset int32) ([]model.Key, error) {
	if len(values) < 2 {
		return nil, fmt.Errorf("getSigningKeysWithSignatures: expected 2 values, got %d", len(values))
	}

	pubkeys, ok := values[0].([]byte)
	if !ok {
		return nil, fmt.Errorf("getSigningKeysWithSignatures: keys is %T", values[0])
	}
	signatures, ok := values[1].([]byte)
	if !ok {
		return nil, fmt.Errorf("getSigningKeysWithSignatures: signatures is %T", values[1])
	}

	if len(pubkeys)%pubkeyBytes != 0 || len(signatures)%signatureBytes != 0 {
		return nil, fmt.Errorf("getSigningKeysWithSignatures: %d pubkey bytes, %d signature bytes",
			len(pubkeys), len(signatures))
	}
	count := len(pubkeys) / pubkeyBytes
	if len(signatures)/signatureBytes != count {
		return nil, fmt.Errorf("getSigningKeysWithSignatures: %d keys against %d signatures",
			count, len(signatures)/signatureBytes)
	}

	out := make([]model.Key, 0, count)
	for i := 0; i < count; i++ {
		index := offset + int32(i)
		out = append(out, model.Key{
			PubkeyBin:     pubkeys[i*pubkeyBytes : (i+1)*pubkeyBytes],
			SignatureBin:  signatures[i*signatureBytes : (i+1)*signatureBytes],
			OperatorIndex: op.Index,
			Used:          index < op.UsedSigningKeys,
			ModuleAddress: moduleAddress,
			Index:         index,
			Vetted:        index < op.StakingLimit,
		})
	}
	return out, nil
}

// reflection helpers

func fieldInt(v reflect.Value, name string) int64 {
	f := v.FieldByName(name)
	if !f.IsValid() {
		return 0
	}
	return intOf(f.Interface())
}

func fieldString(v reflect.Value, name string) string {
	f := v.FieldByName(name)
	if !f.IsValid() {
		return ""
	}
	s, _ := f.Interface().(string)
	return s
}

func fieldAddress(v reflect.Value, name string) string {
	f := v.FieldByName(name)
	if !f.IsValid() {
		return ""
	}
	return addressOf(f.Interface())
}

func intOf(v any) int64 {
	switch t := v.(type) {
	case *big.Int:
		return t.Int64()
	case uint8:
		return int64(t)
	case uint16:
		return int64(t)
	case uint32:
		return int64(t)
	case uint64:
		return int64(t)
	case int64:
		return t
	case int:
		return int64(t)
	default:
		return 0
	}
}

// addressOf renders any 20-byte address representation as lowercase hex.
func addressOf(v any) string {
	switch t := v.(type) {
	case string:
		return strings.ToLower(t)
	case [20]byte:
		return "0x" + hex.EncodeToString(t[:])
	default:
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Array && rv.Len() == 20 {
			buf := make([]byte, 20)
			for i := 0; i < 20; i++ {
				buf[i] = byte(rv.Index(i).Uint())
			}
			return "0x" + hex.EncodeToString(buf)
		}
		return ""
	}
}
