//go:build fork

package fork

import (
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// Write ABI kept out of internal/eth/abi (canonical, read-only) so tests can't widen the production ABI.
const registryWriteABI = `[
  {"name":"addSigningKeys","type":"function","inputs":[
    {"name":"_nodeOperatorId","type":"uint256"},{"name":"_keysCount","type":"uint256"},
    {"name":"_publicKeys","type":"bytes"},{"name":"_signatures","type":"bytes"}]},
  {"name":"removeSigningKeys","type":"function","inputs":[
    {"name":"_nodeOperatorId","type":"uint256"},{"name":"_fromIndex","type":"uint256"},
    {"name":"_keysCount","type":"uint256"}]},
  {"name":"setNodeOperatorName","type":"function","inputs":[
    {"name":"_nodeOperatorId","type":"uint256"},{"name":"_name","type":"string"}]},
  {"name":"setNodeOperatorRewardAddress","type":"function","inputs":[
    {"name":"_nodeOperatorId","type":"uint256"},{"name":"_rewardAddress","type":"address"}]},
  {"name":"setNodeOperatorStakingLimit","type":"function","inputs":[
    {"name":"_nodeOperatorId","type":"uint256"},{"name":"_vettedSigningKeysCount","type":"uint64"}]},
  {"name":"deactivateNodeOperator","type":"function","inputs":[
    {"name":"_nodeOperatorId","type":"uint256"}]},
  {"name":"obtainDepositData","type":"function","inputs":[
    {"name":"_depositsCount","type":"uint256"},{"name":"_depositCalldata","type":"bytes"}],
   "outputs":[{"name":"publicKeys","type":"bytes"},{"name":"signatures","type":"bytes"}]},
  {"name":"getNonce","type":"function","stateMutability":"view","inputs":[],
   "outputs":[{"name":"","type":"uint256"}]},
  {"name":"getNodeOperatorsCount","type":"function","stateMutability":"view","inputs":[],
   "outputs":[{"name":"","type":"uint256"}]},
  {"name":"getNodeOperator","type":"function","stateMutability":"view",
   "inputs":[{"name":"_id","type":"uint256"},{"name":"_fullInfo","type":"bool"}],
   "outputs":[{"name":"active","type":"bool"},{"name":"name","type":"string"},
     {"name":"rewardAddress","type":"address"},{"name":"stakingLimit","type":"uint64"},
     {"name":"stoppedValidators","type":"uint64"},{"name":"totalSigningKeys","type":"uint64"},
     {"name":"usedSigningKeys","type":"uint64"}]},
  {"name":"getSigningKeys","type":"function","stateMutability":"view",
   "inputs":[{"name":"_nodeOperatorId","type":"uint256"},{"name":"_offset","type":"uint256"},
     {"name":"_limit","type":"uint256"}],
   "outputs":[{"name":"pubkeys","type":"bytes"},{"name":"signatures","type":"bytes"},
     {"name":"used","type":"bool[]"}]},
  {"name":"kernel","type":"function","stateMutability":"view","inputs":[],
   "outputs":[{"name":"","type":"address"}]}
]`

const (
	kernelABIJSON = `[{"name":"acl","type":"function","stateMutability":"view","inputs":[],
	  "outputs":[{"name":"","type":"address"}]}]`

	aclABIJSON = `[
	  {"name":"getPermissionManager","type":"function","stateMutability":"view",
	   "inputs":[{"name":"_app","type":"address"},{"name":"_role","type":"bytes32"}],
	   "outputs":[{"name":"","type":"address"}]},
	  {"name":"grantPermission","type":"function",
	   "inputs":[{"name":"_entity","type":"address"},{"name":"_app","type":"address"},
	     {"name":"_role","type":"bytes32"}]},
	  {"name":"hasPermission","type":"function","stateMutability":"view",
	   "inputs":[{"name":"_who","type":"address"},{"name":"_where","type":"address"},
	     {"name":"_what","type":"bytes32"}],
	   "outputs":[{"name":"","type":"bool"}]}]`

	routerABIJSON = `[
	  {"name":"setStakingModuleStatus","type":"function",
	   "inputs":[{"name":"_stakingModuleId","type":"uint256"},{"name":"_status","type":"uint8"}]},
	  {"name":"getStakingModule","type":"function","stateMutability":"view",
	   "inputs":[{"name":"_stakingModuleId","type":"uint256"}],
	   "outputs":[{"name":"","type":"tuple","components":[
	     {"name":"id","type":"uint24"},{"name":"stakingModuleAddress","type":"address"},
	     {"name":"stakingModuleFee","type":"uint16"},{"name":"treasuryFee","type":"uint16"},
	     {"name":"stakeShareLimit","type":"uint16"},{"name":"status","type":"uint8"},
	     {"name":"name","type":"string"},{"name":"lastDepositAt","type":"uint64"},
	     {"name":"lastDepositBlock","type":"uint256"},
	     {"name":"exitedValidatorsCount","type":"uint256"},
	     {"name":"priorityExitShareThreshold","type":"uint16"},
	     {"name":"maxDepositsPerBlock","type":"uint64"},
	     {"name":"minDepositBlockDistance","type":"uint64"}]}]}]`
)

var (
	registryABI = mustABI(registryWriteABI)
	kernelABI   = mustABI(kernelABIJSON)
	aclABI      = mustABI(aclABIJSON)
	routerABI   = mustABI(routerABIJSON)
)

func mustABI(s string) abi.ABI {
	parsed, err := abi.JSON(strings.NewReader(s))
	if err != nil {
		panic("fork: bad ABI: " + err.Error())
	}
	return parsed
}

type Registry struct {
	Address string

	tb testing.TB
	a  *Anvil
}

func NewRegistry(tb testing.TB, a *Anvil, address string) *Registry {
	return &Registry{Address: address, tb: tb, a: a}
}

// Operator holds the raw counters, more than the served DTO.
type Operator struct {
	Active           bool
	Name             string
	RewardAddress    string
	StakingLimit     uint64
	Stopped          uint64
	TotalSigningKeys uint64
	UsedSigningKeys  uint64
}

func (r *Registry) Operator(id int64) Operator {
	r.tb.Helper()

	out := r.call("getNodeOperator", big.NewInt(id), true)
	return Operator{
		Active:           out[0].(bool),
		Name:             out[1].(string),
		RewardAddress:    strings.ToLower(out[2].(common.Address).Hex()),
		StakingLimit:     out[3].(uint64),
		Stopped:          out[4].(uint64),
		TotalSigningKeys: out[5].(uint64),
		UsedSigningKeys:  out[6].(uint64),
	}
}

func (r *Registry) OperatorCount() int64 {
	r.tb.Helper()
	return r.call("getNodeOperatorsCount")[0].(*big.Int).Int64()
}

func (r *Registry) Nonce() int64 {
	r.tb.Helper()
	return r.call("getNonce")[0].(*big.Int).Int64()
}

// SigningKeys reads the contract through the harness client: the oracle for the served dump.
func (r *Registry) SigningKeys(operator int64, offset, limit int64) [][]byte {
	r.tb.Helper()

	out := r.call("getSigningKeys", big.NewInt(operator), big.NewInt(offset), big.NewInt(limit))
	packed := out[0].([]byte)

	keys := make([][]byte, 0, limit)
	for off := 0; off+48 <= len(packed); off += 48 {
		key := make([]byte, 48)
		copy(key, packed[off:off+48])
		keys = append(keys, key)
	}
	return keys
}

// Sent from the reward address, which the registry accepts without a role while active.
func (r *Registry) AddKeys(operator int64, keys [][]byte) {
	r.tb.Helper()

	pubkeys := make([]byte, 0, len(keys)*48)
	signatures := make([]byte, 0, len(keys)*96)
	for _, key := range keys {
		if len(key) != 48 {
			r.tb.Fatalf("pubkey is %d bytes, want 48", len(key))
		}
		pubkeys = append(pubkeys, key...)
		signatures = append(signatures, Signature(key)...)
	}

	data := r.pack("addSigningKeys", big.NewInt(operator), big.NewInt(int64(len(keys))), pubkeys, signatures)
	r.a.Send(r.Operator(operator).RewardAddress, r.Address, data)
}

// AddKeysCalldata is for anvil_reorg, which takes the transaction rather than a submission.
func (r *Registry) AddKeysCalldata(operator int64, keys [][]byte) (from string, data []byte) {
	r.tb.Helper()

	pubkeys := make([]byte, 0, len(keys)*48)
	signatures := make([]byte, 0, len(keys)*96)
	for _, key := range keys {
		pubkeys = append(pubkeys, key...)
		signatures = append(signatures, Signature(key)...)
	}
	return r.Operator(operator).RewardAddress,
		r.pack("addSigningKeys", big.NewInt(operator), big.NewInt(int64(len(keys))), pubkeys, signatures)
}

// RemoveKeys: the contract moves the last key into the vacated slot, so (operator, index)
// identity breaks.
func (r *Registry) RemoveKeys(operator, fromIndex, count int64) {
	r.tb.Helper()

	data := r.pack("removeSigningKeys", big.NewInt(operator), big.NewInt(fromIndex), big.NewInt(count))
	r.a.Send(r.Operator(operator).RewardAddress, r.Address, data)
}

// Rename moves no nonce; only the operator event scan can notice it.
func (r *Registry) Rename(operator int64, name string) {
	r.tb.Helper()

	r.grant(RoleManageNodeOperator)
	r.a.Send(TestSender, r.Address, r.pack("setNodeOperatorName", big.NewInt(operator), name))
}

func (r *Registry) Vet(operator int64, count uint64) {
	r.tb.Helper()

	r.grant(RoleSetOperatorLimit)
	r.a.Send(TestSender, r.Address, r.pack("setNodeOperatorStakingLimit", big.NewInt(operator), count))
}

// Deposit: only the staking router may call it. No ether moves; the point is `used` and the nonce.
func (r *Registry) Deposit(count int64) {
	r.tb.Helper()

	data := r.pack("obtainDepositData", big.NewInt(count), []byte{})
	r.a.Send(StakingRouterAddress, r.Address, data)
}

// grant impersonates the role's permission manager, whoever holds it today.
func (r *Registry) grant(role string) {
	r.tb.Helper()

	acl := r.acl()
	roleHash := roleHash(role)

	var has bool
	out, err := aclABI.Unpack("hasPermission", r.a.Call(acl,
		mustPack(aclABI, "hasPermission", common.HexToAddress(TestSender), common.HexToAddress(r.Address), roleHash)))
	if err == nil && len(out) > 0 {
		has, _ = out[0].(bool)
	}
	if has {
		return
	}

	managerOut, err := aclABI.Unpack("getPermissionManager", r.a.Call(acl,
		mustPack(aclABI, "getPermissionManager", common.HexToAddress(r.Address), roleHash)))
	if err != nil {
		r.tb.Fatalf("read the permission manager for %s: %v", role, err)
	}
	manager := managerOut[0].(common.Address)
	if manager == (common.Address{}) {
		r.tb.Fatalf("%s on %s has no permission manager: it cannot be granted on this fork", role, r.Address)
	}

	r.a.Send(manager.Hex(), acl,
		mustPack(aclABI, "grantPermission", common.HexToAddress(TestSender), common.HexToAddress(r.Address), roleHash))
}

// Walks module -> kernel -> acl rather than carrying an address an upgrade would invalidate.
func (r *Registry) acl() string {
	r.tb.Helper()

	kernel := r.call("kernel")[0].(common.Address)
	out, err := kernelABI.Unpack("acl", r.a.Call(kernel.Hex(), mustPack(kernelABI, "acl")))
	if err != nil {
		r.tb.Fatalf("resolve the acl: %v", err)
	}
	return out[0].(common.Address).Hex()
}

func (r *Registry) call(method string, args ...any) []any {
	r.tb.Helper()

	out, err := registryABI.Unpack(method, r.a.Call(r.Address, r.pack(method, args...)))
	if err != nil {
		r.tb.Fatalf("unpack %s: %v", method, err)
	}
	return out
}

func (r *Registry) pack(method string, args ...any) []byte {
	r.tb.Helper()

	data, err := registryABI.Pack(method, args...)
	if err != nil {
		r.tb.Fatalf("pack %s: %v", method, err)
	}
	return data
}

// SetModuleStatus moves no module nonce: the status lives in the router, the nonce in the module.
func SetModuleStatus(tb testing.TB, a *Anvil, moduleID int64, status uint8) {
	tb.Helper()

	data := mustPack(routerABI, "setStakingModuleStatus", big.NewInt(moduleID), status)
	// Router roles are OpenZeppelin, held by the agent on Hoodi.
	a.Send(AgentAddress, StakingRouterAddress, data)
}

const AgentAddress = "0x0534aA41907c9631fae990960bCC72d75fA7cfeD"

func mustPack(a abi.ABI, method string, args ...any) []byte {
	data, err := a.Pack(method, args...)
	if err != nil {
		panic("fork: pack " + method + ": " + err.Error())
	}
	return data
}

func roleHash(name string) [32]byte {
	var out [32]byte
	copy(out[:], crypto.Keccak256([]byte(name)))
	return out
}
