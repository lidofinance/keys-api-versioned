//go:build fork

package fork

import (
	"encoding/hex"
	"strings"
)

// Hoodi, not mainnet: five modules (incl. curated-onchain-v2 with keys) and a 36-key module.
// Pinned so anvil's RPC cache serves reruns and every test's preconditions stay true.
const (
	defaultForkBlock = 3_290_000
	hoodiChainID     = 560048

	LocatorAddress = "0xe2EF9536DAAAEBFf5b1c130957AB3E80056b06D8"
	// StakingRouterAddress holds STAKING_ROUTER_ROLE, so it sends obtainDepositData.
	StakingRouterAddress = "0xCc820558B39ee15C7C45B59390B503b83fb499A8"
)

// Sandbox: operator 1 has 30 keys, none vetted or deposited, the only state removeSigningKeys
// accepts.
const (
	SandboxModuleID     = 3
	SandboxAddress      = "0x682e94d2630846a503bdee8b6810df71c9806891"
	SandboxOperator     = 1
	SandboxOperators    = 4
	SandboxKeys         = 36
	SandboxOperatorKeys = 30
)

const (
	RoleManageNodeOperator = "MANAGE_NODE_OPERATOR_ROLE"
	RoleSetOperatorLimit   = "SET_NODE_OPERATOR_LIMIT_ROLE"
	RoleManageSigningKeys  = "MANAGE_SIGNING_KEYS"
)

// TestSender is not a real account: anvil impersonates it and the harness funds it.
const TestSender = "0x00000000000000000000000000000000f0f0f0f0"

func hexBytes(b []byte) string { return hex.EncodeToString(b) }

func decodeHex(s string) ([]byte, error) {
	return hex.DecodeString(strings.TrimPrefix(strings.ToLower(s), "0x"))
}

// The API serves EIP-55 casing, the store lowercase.
func sameAddress(a, b string) bool { return strings.EqualFold(a, b) }
