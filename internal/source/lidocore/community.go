package lidocore

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"strconv"
	"sync"

	"github.com/lidofinance/keys-api-versioned/internal/eth"
	"github.com/lidofinance/keys-api-versioned/internal/model"
)

// CommunityReader reads the CSM-shaped modules: community-onchain-v1 (CSM) and
// curated-onchain-v2 (CMv2). They share one contract interface, which is why the
// canonical service points both types at one implementation, and so does this.
//
//	getNodeOperatorsCount                            → how many operators
//	getNodeOperator(i)                               → counters, reward address
//	getNodeOperatorSummary(i)                        → depositableValidatorsCount
//	getSigningKeysWithSignatures(op, start, count)   → packed pubkeys, signatures
//
// Two differences from the curated registry matter:
//
//   - There is no `used` array. A key counts as deposited when its index is below
//     the operator's totalDepositedKeys, the same rule the canonical service uses.
//   - Operators have no on-chain name in CSM. CSM gets a generated one; CMv2
//     resolves it through the module's META_REGISTRY, which is a second contract.
//     The same registry holds node operator groups, so name and group are read
//     together, from the same batch. Which of the two applies is decided by the
//     module type, as in the canonical service.
type CommunityReader struct {
	KeysBatch       uint64
	CallsPerRequest int
	KeysPerRequest  uint64
	Concurrency     int
	Log             *slog.Logger

	// watched is the META_REGISTRY address per CMv2 module as last read, which the
	// operator event scan adds to its filter. Not a read cache: Operators reads the
	// address at its own block every time.
	watchMu sync.Mutex
	watched map[string]string
}

// typeWithMetaRegistry is the module type whose names and groups live in the
// module's META_REGISTRY.
const typeWithMetaRegistry = "curated-onchain-v2"

func (r *CommunityReader) Types() []string {
	return []string{"community-onchain-v1", typeWithMetaRegistry}
}

// OperatorEventTopics are the canonical service's: the reward address and vetted
// count events of the module, and the MetaRegistry's metadata event, which carries
// CMv2 names. One filter covers both contracts — see OperatorEventAddresses.
func (r *CommunityReader) OperatorEventTopics() []string {
	return []string{
		"NodeOperatorRewardAddressChanged",
		"VettedSigningKeysCountChanged(uint256,uint256)",
		"OperatorMetadataSet(uint256,(string,string,bool))",
	}
}

// OperatorEventAddresses is the module, plus its MetaRegistry for CMv2. The address
// is the one the last operator read used; before any read in this process it is
// resolved at toBlock, once.
func (r *CommunityReader) OperatorEventAddresses(ctx context.Context, c *eth.Client, m model.StakingModule, toBlock int64) ([]string, error) {
	if m.Type != typeWithMetaRegistry {
		return []string{m.RawAddress()}, nil
	}
	r.watchMu.Lock()
	registry, ok := r.watched[m.RawAddress()]
	r.watchMu.Unlock()
	if !ok {
		var err error
		registry, err = r.metaRegistry(ctx, c, eth.BlockRef{Number: big.NewInt(toBlock)}, m.RawAddress())
		if err != nil {
			return nil, err
		}
	}
	return []string{m.RawAddress(), registry}, nil
}

// Operators reads every operator of a community module.
func (r *CommunityReader) Operators(ctx context.Context, c *eth.Client, at eth.BlockRef, m model.StakingModule) ([]model.Operator, error) {
	moduleAddress := m.RawAddress()
	out, err := c.CallOne(ctx, at, eth.Call{
		To: moduleAddress, ABI: &eth.CSMABI, Method: "getNodeOperatorsCount",
	})
	if err != nil {
		return nil, fmt.Errorf("getNodeOperatorsCount: %w", err)
	}
	count, ok := out[0].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("unexpected operator count type %T", out[0])
	}
	n := int(count.Int64())

	operators := make([]model.Operator, n)
	for start := 0; start < n; start += operatorBatchSize {
		end := min(start+operatorBatchSize, n)

		calls := make([]eth.Call, 0, (end-start)*2)
		for i := start; i < end; i++ {
			calls = append(calls, eth.Call{
				To: moduleAddress, ABI: &eth.CSMABI, Method: "getNodeOperator",
				Args: []any{big.NewInt(int64(i))},
			})
		}
		for i := start; i < end; i++ {
			calls = append(calls, eth.Call{
				To: moduleAddress, ABI: &eth.CSMABI, Method: "getNodeOperatorSummary",
				Args: []any{big.NewInt(int64(i))},
			})
		}

		results, err := c.CallBatch(ctx, at, calls)
		if err != nil {
			return nil, fmt.Errorf("operators %d–%d: %w", start, end, err)
		}

		width := end - start
		for i := 0; i < width; i++ {
			op, err := decodeCommunityOperator(int32(start+i), moduleAddress, results[i])
			if err != nil {
				return nil, err
			}
			op.DepositableValidatorsCount = int32(depositableFromSummary(results[width+i]))
			operators[start+i] = op
		}
	}

	if m.Type != typeWithMetaRegistry {
		// CSM has no name on chain, and the canonical service generates this one, so
		// consumers already see it. No groups either: Group stays nil.
		for i := range operators {
			operators[i].Name = "CSM Operator " + strconv.Itoa(int(operators[i].Index))
		}
	} else if err := r.fillMetaRegistryFields(ctx, c, at, moduleAddress, operators); err != nil {
		return nil, err
	}
	if r.Log != nil {
		r.Log.Debug("community operators read", "module", moduleAddress, "count", len(operators))
	}
	return operators, nil
}

// fillMetaRegistryFields resolves the operator fields that live off the module: the
// name and the node operator group.
//
// Two calls per operator, in one batch each round — the same two-kinds-of-call
// layout Operators uses above, so adding groups costs no extra round trip beyond
// what the batch size already implies.
//
// The operator event scan watches OperatorMetadataSet, so a rename is picked up
// within a cycle. A group edit is not: its event is not watched, so it waits for
// the next key change or the periodic full audit.
func (r *CommunityReader) fillMetaRegistryFields(ctx context.Context, c *eth.Client, at eth.BlockRef, moduleAddress string, operators []model.Operator) error {
	// Read even with no operators, so the event scan knows what to watch.
	registry, err := r.metaRegistry(ctx, c, at, moduleAddress)
	if err != nil {
		return err
	}

	for start := 0; start < len(operators); start += operatorBatchSize {
		end := min(start+operatorBatchSize, len(operators))

		calls := make([]eth.Call, 0, (end-start)*2)
		for i := start; i < end; i++ {
			calls = append(calls, eth.Call{
				To: registry, ABI: &eth.MetaRegistryABI, Method: "getOperatorMetadata",
				Args: []any{big.NewInt(int64(operators[i].Index))},
			})
		}
		for i := start; i < end; i++ {
			calls = append(calls, eth.Call{
				To: registry, ABI: &eth.MetaRegistryABI, Method: "getNodeOperatorGroupId",
				Args: []any{big.NewInt(int64(operators[i].Index))},
			})
		}
		results, err := c.CallBatch(ctx, at, calls)
		if err != nil {
			return fmt.Errorf("operator metadata and groups %d–%d: %w", start, end, err)
		}

		width := end - start
		for i := 0; i < width; i++ {
			operators[start+i].Name = metadataName(results[i], operators[start+i].Index)
			group := model.GroupID(intOf(firstValue(results[width+i])))
			operators[start+i].Group = &group
		}
	}
	if r.Log != nil {
		r.Log.Debug("operator groups read", "module", moduleAddress,
			"registry", registry, "operators", len(operators))
	}
	return nil
}

// metaRegistry reads the module's META_REGISTRY at a block and records it for the
// event scan.
//
// Read every time rather than cached: a module upgrade can point it elsewhere, and
// the canonical service stopped caching it for that reason. Any error is returned —
// treating a failed read as "no registry" served generated names for a CMv2 module.
func (r *CommunityReader) metaRegistry(ctx context.Context, c *eth.Client, at eth.BlockRef, moduleAddress string) (string, error) {
	out, err := c.CallOne(ctx, at, eth.Call{
		To: moduleAddress, ABI: &eth.CMv2ABI, Method: "META_REGISTRY",
	})
	if err != nil {
		return "", fmt.Errorf("META_REGISTRY of %s: %w", moduleAddress, err)
	}
	address := ""
	if len(out) > 0 {
		address = addressOf(out[0])
	}
	if address == "" {
		return "", fmt.Errorf("META_REGISTRY of %s: unexpected result %v", moduleAddress, out)
	}

	r.watchMu.Lock()
	if r.watched == nil {
		r.watched = map[string]string{}
	}
	r.watched[moduleAddress] = address
	r.watchMu.Unlock()
	return address, nil
}

// Keys streams the keys of a community module — the walk is in keys.go.
func (r *CommunityReader) Keys(ctx context.Context, c *eth.Client, at eth.BlockRef, moduleAddress string, operators []model.Operator, fn func(model.Key) error) error {
	return streamKeys(ctx, c, at, moduleAddress, operators,
		keysSpec{abi: &eth.CSMABI, method: "getSigningKeysWithSignatures", decode: decodeCommunityKeys},
		keysPacing{
			keysBatch:       r.KeysBatch,
			callsPerRequest: r.CallsPerRequest,
			keysPerRequest:  r.KeysPerRequest,
			concurrency:     r.Concurrency,
		},
		r.Log, fn)
}

var _ ModuleReader = (*CommunityReader)(nil)
