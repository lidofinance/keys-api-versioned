package lidocore

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/lidofinance/keys-api-versioned/internal/model"
)

// The ABI decoder builds anonymous struct types at runtime, so tuples are read by reflection.

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

const defaultWithdrawalCredentialsType = 1

func decodeStakingModules(v any, version int64) ([]rawModule, error) {
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
		// Decided by the router version reported this cycle, not by whether our ABI declares the field:
		// the tuple is a property of the chain, the ABI of this build.
		f := item.FieldByName("WithdrawalCredentialsType")
		switch {
		case version >= srVersionWithWCType && f.IsValid():
			m.withdrawalCredentialsType = int32(fieldInt(item, "WithdrawalCredentialsType"))
		default:
			m.withdrawalCredentialsType = defaultWithdrawalCredentialsType
		}
		if m.withdrawalCredentialsType == 0 {
			// v4 with the field unset: between the upgrade and the vote that sets it.
			m.withdrawalCredentialsType = defaultWithdrawalCredentialsType
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
		return strings.TrimRight(string(t[:]), "\x00"), nil
	default:
		return "", fmt.Errorf("getType: unexpected type %T", v)
	}
}

// decodeCuratedOperator outputs, in ABI order: active, name, rewardAddress, totalVetted,
// totalExited, totalAdded, totalDeposited.
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
		Name:              operatorName(name, index),
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

// CSM has no inactive operator (getNodeOperatorIsActive only checks the id exists), so active
// is true for every id below the count.
func decodeCommunityOperator(index int32, moduleAddress string, values []any) (model.Operator, error) {
	if len(values) == 0 {
		return model.Operator{}, fmt.Errorf("getNodeOperator: empty result")
	}
	s := reflect.ValueOf(values[0])
	if s.Kind() != reflect.Struct {
		return model.Operator{}, fmt.Errorf("getNodeOperator: expected a struct, got %T", values[0])
	}

	withdrawn := int32(fieldInt(s, "TotalWithdrawnKeys"))
	return model.Operator{
		RewardAddress:      strings.ToLower(fieldAddress(s, "RewardAddress")),
		StakingLimit:       int32(fieldInt(s, "TotalVettedKeys")),
		StoppedValidators:  int32(fieldInt(s, "TotalExitedKeys")),
		TotalSigningKeys:   int32(fieldInt(s, "TotalAddedKeys")),
		UsedSigningKeys:    int32(fieldInt(s, "TotalDepositedKeys")),
		Index:              index,
		Active:             true,
		ModuleAddress:      moduleAddress,
		TotalWithdrawnKeys: &withdrawn,
	}, nil
}

// operatorName: Postgres text rejects invalid UTF-8 and NUL, so one such name would stop the
// ingest; the canonical placeholder replaces it.
func operatorName(name string, index int32) string {
	if !utf8.ValidString(name) || strings.ContainsRune(name, 0) {
		return "invalidName" + strconv.Itoa(int(index))
	}
	return name
}

// depositableValidatorsCount is the last output of getNodeOperatorSummary in both ABIs.
func depositableFromSummary(values []any) int64 {
	if len(values) == 0 {
		return 0
	}
	return intOf(values[len(values)-1])
}

func firstValue(values []any) any {
	if len(values) == 0 {
		return nil
	}
	return values[0]
}

func metadataName(values []any, index int32) string {
	if len(values) == 0 {
		return ""
	}
	s := reflect.ValueOf(values[0])
	if s.Kind() != reflect.Struct {
		return ""
	}
	return operatorName(fieldString(s, "Name"), index)
}

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
			// Kept as bytes end to end; the response encoder writes hex straight into its buffer.
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
