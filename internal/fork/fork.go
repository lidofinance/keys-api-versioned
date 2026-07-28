//go:build fork

package fork

import (
	"encoding/hex"
	"strings"
)

// The chain these tests run against, pinned.
//
// Hoodi rather than mainnet for two reasons: it has five modules against mainnet's
// four, two of which mainnet cannot exercise at all (a third curated registry, and
// curated-onchain-v2 holding keys); and its smallest module is 36 keys, which is what
// makes a scenario cost seconds instead of minutes.
//
// The block is pinned rather than tracking head so that anvil serves the second run
// out of ~/.foundry/cache/rpc — a cold fetch of a module's keys is minutes, the same
// read against a warm cache is milliseconds — and so that the preconditions each test
// asserts stay true. Re-pinning means re-warming and re-checking every precondition;
// docs/fork-tests.md says how.
const (
	defaultForkBlock = 3_290_000
	hoodiChainID     = 560048

	// LocatorAddress resolves everything else, and is what the service is pointed at.
	LocatorAddress = "0xe2EF9536DAAAEBFf5b1c130957AB3E80056b06D8"
	// StakingRouterAddress holds STAKING_ROUTER_ROLE on the registries, so it is the
	// sender for the calls only the router may make — obtainDepositData above all.
	StakingRouterAddress = "0xCc820558B39ee15C7C45B59390B503b83fb499A8"
)

// Sandbox is the module the mutation scenarios use: a real curated-onchain-v1
// registry with four operators and thirty-six keys, so a full module read is cheap
// and a single key is a visible fraction of it.
//
// The operator that matters is 1 — thirty keys, none vetted and none deposited, which
// is the only state removeSigningKeys accepts (it refuses to touch a deposited key)
// and the one that leaves room to vet and deposit on purpose.
const (
	SandboxModuleID  = 3
	SandboxAddress   = "0x682e94d2630846a503bdee8b6810df71c9806891"
	SandboxOperator  = 1
	SandboxOperators = 4
	SandboxKeys      = 36
	// SandboxOperatorKeys is operator 1's key count at the pinned block.
	SandboxOperatorKeys = 30
)

// Aragon role identifiers, as keccak256 of the name.
const (
	RoleManageNodeOperator = "MANAGE_NODE_OPERATOR_ROLE"
	RoleSetOperatorLimit   = "SET_NODE_OPERATOR_LIMIT_ROLE"
	RoleManageSigningKeys  = "MANAGE_SIGNING_KEYS"
)

// TestSender is the address the harness grants roles to and sends from. It is not a
// real account anywhere; anvil impersonates it and the harness funds it.
const TestSender = "0x00000000000000000000000000000000f0f0f0f0"

func hexBytes(b []byte) string { return hex.EncodeToString(b) }

func decodeHex(s string) ([]byte, error) {
	return hex.DecodeString(strings.TrimPrefix(strings.ToLower(s), "0x"))
}

// sameAddress compares addresses without caring about EIP-55 casing, which the API
// serves and the store does not.
func sameAddress(a, b string) bool { return strings.EqualFold(a, b) }
