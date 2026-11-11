package lidocore

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"

	"github.com/lidofinance/keys-api-versioned/internal/eth"
	"github.com/lidofinance/keys-api-versioned/internal/model"
)

// CuratedReader reads the curated registry (NodeOperatorsRegistry): the Lido
// module and SimpleDVT.
//
//	getNodeOperatorsCount                     → how many operators
//	getNodeOperator(i, true)                  → name, reward address, counters
//	getNodeOperatorSummary(i)                 → depositableValidatorsCount
//	getSigningKeys(op, offset, limit)         → packed pubkeys, signatures, used
//
// A key is vetted when its index is below the operator's vetted count, which is
// the rule the canonical service applies.
type CuratedReader struct {
	KeysBatch       uint64
	CallsPerRequest int
	KeysPerRequest  uint64
	Concurrency     int
	Log             *slog.Logger
}

func (r *CuratedReader) Types() []string { return []string{"curated-onchain-v1"} }

// OperatorEventTopics are the curated events that change operator metadata
// without moving the nonce. Same three the canonical service filters on.
func (r *CuratedReader) OperatorEventTopics() []string {
	return []string{"NodeOperatorAdded", "NodeOperatorNameSet", "NodeOperatorRewardAddressSet"}
}

// Operators reads every operator, two calls each, batched.
//
// finalizedUsedSigningKeys is deliberately not read. The canonical service fetches
// it with a second round of per-operator calls at the finalized block tag, and no
// endpoint we serve exposes it — the operator DTO omits it. Skipping it halves the
// per-operator round trips.
func (r *CuratedReader) Operators(ctx context.Context, c *eth.Client, at eth.BlockRef, m model.StakingModule) ([]model.Operator, error) {
	moduleAddress := m.RawAddress()
	out, err := c.CallOne(ctx, at, eth.Call{
		To: moduleAddress, ABI: &eth.RegistryABI, Method: "getNodeOperatorsCount",
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
				To: moduleAddress, ABI: &eth.RegistryABI, Method: "getNodeOperator",
				Args: []any{big.NewInt(int64(i)), true},
			})
		}
		for i := start; i < end; i++ {
			calls = append(calls, eth.Call{
				To: moduleAddress, ABI: &eth.RegistryABI, Method: "getNodeOperatorSummary",
				Args: []any{big.NewInt(int64(i))},
			})
		}

		results, err := c.CallBatch(ctx, at, calls)
		if err != nil {
			return nil, fmt.Errorf("operators %d–%d: %w", start, end, err)
		}

		width := end - start
		for i := 0; i < width; i++ {
			op, err := decodeCuratedOperator(int32(start+i), moduleAddress, results[i])
			if err != nil {
				return nil, err
			}
			op.DepositableValidatorsCount = int32(depositableFromSummary(results[width+i]))
			operators[start+i] = op
		}
	}
	return operators, nil
}

// Keys streams the module's keys — see keys.go for the walk both readers share.
func (r *CuratedReader) Keys(ctx context.Context, c *eth.Client, at eth.BlockRef, moduleAddress string, operators []model.Operator, fn func(model.Key) error) error {
	return streamKeys(ctx, c, at, moduleAddress, operators,
		keysSpec{abi: &eth.RegistryABI, method: "getSigningKeys", decode: decodeCuratedKeys},
		keysPacing{
			keysBatch:       r.KeysBatch,
			callsPerRequest: r.CallsPerRequest,
			keysPerRequest:  r.KeysPerRequest,
			concurrency:     r.Concurrency,
		},
		r.Log, fn)
}

var _ ModuleReader = (*CuratedReader)(nil)
