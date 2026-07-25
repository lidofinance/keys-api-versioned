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
type CommunityReader struct {
	KeysBatch       uint64
	CallsPerRequest int
	KeysPerRequest  uint64
	Concurrency     int
	Log             *slog.Logger

	// metaRegistry caches the META_REGISTRY address per module, since it is an
	// immutable pointer in practice and resolving it costs a call.
	metaMu       sync.Mutex
	metaRegistry map[string]string
}

func (r *CommunityReader) Types() []string {
	return []string{"community-onchain-v1", "curated-onchain-v2"}
}

// OperatorEventTopics is the one event these contracts emit for operator
// metadata; names live off-module, so a rename is not observable here.
func (r *CommunityReader) OperatorEventTopics() []string {
	return []string{"NodeOperatorRewardAddressChanged"}
}

// Operators reads every operator of a community module.
func (r *CommunityReader) Operators(ctx context.Context, c *eth.Client, at eth.BlockRef, moduleAddress string) ([]model.Operator, error) {
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

	if err := r.fillNames(ctx, c, at, moduleAddress, operators); err != nil {
		return nil, err
	}
	if r.Log != nil {
		r.Log.Debug("community operators read", "module", moduleAddress, "count", len(operators))
	}
	return operators, nil
}

// fillNames resolves operator names.
//
// CSM has none on chain, and the canonical service generates "CSM Operator <i>",
// so consumers already see that string — matching it keeps responses comparable.
// CMv2 keeps names in a MetaRegistry, one call per operator.
func (r *CommunityReader) fillNames(ctx context.Context, c *eth.Client, at eth.BlockRef, moduleAddress string, operators []model.Operator) error {
	registry, err := r.resolveMetaRegistry(ctx, c, at, moduleAddress)
	if err != nil {
		return err
	}
	if registry == "" {
		for i := range operators {
			operators[i].Name = "CSM Operator " + strconv.Itoa(int(operators[i].Index))
		}
		return nil
	}

	for start := 0; start < len(operators); start += operatorBatchSize {
		end := min(start+operatorBatchSize, len(operators))

		calls := make([]eth.Call, 0, end-start)
		for i := start; i < end; i++ {
			calls = append(calls, eth.Call{
				To: registry, ABI: &eth.MetaRegistryABI, Method: "getOperatorMetadata",
				Args: []any{big.NewInt(int64(operators[i].Index))},
			})
		}
		results, err := c.CallBatch(ctx, at, calls)
		if err != nil {
			return fmt.Errorf("getOperatorMetadata %d–%d: %w", start, end, err)
		}
		for i, values := range results {
			operators[start+i].Name = metadataName(values)
		}
	}
	return nil
}

// resolveMetaRegistry returns the module's MetaRegistry address, or an empty
// string when the module has none (plain CSM).
func (r *CommunityReader) resolveMetaRegistry(ctx context.Context, c *eth.Client, at eth.BlockRef, moduleAddress string) (string, error) {
	r.metaMu.Lock()
	if r.metaRegistry == nil {
		r.metaRegistry = map[string]string{}
	}
	cached, ok := r.metaRegistry[moduleAddress]
	r.metaMu.Unlock()
	if ok {
		return cached, nil
	}

	// A module without the getter reverts, which is the signal that names are
	// generated rather than stored.
	out, err := c.CallOne(ctx, at, eth.Call{
		To: moduleAddress, ABI: &eth.CMv2ABI, Method: "META_REGISTRY",
	})
	address := ""
	if err == nil && len(out) > 0 {
		address = addressOf(out[0])
	}

	r.metaMu.Lock()
	r.metaRegistry[moduleAddress] = address
	r.metaMu.Unlock()
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
