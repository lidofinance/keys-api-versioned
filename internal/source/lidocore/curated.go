package lidocore

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"

	"github.com/lidofinance/keys-api-versioned/internal/eth"
	"github.com/lidofinance/keys-api-versioned/internal/model"
)

// CuratedReader reads NodeOperatorsRegistry modules (the Lido module and SimpleDVT).
type CuratedReader struct {
	KeysBatch       uint64
	CallsPerRequest int
	KeysPerRequest  uint64
	Concurrency     int
	Log             *slog.Logger
}

func (r *CuratedReader) Types() []string { return []string{"curated-onchain-v1"} }

func (r *CuratedReader) OperatorEventTopics() []string {
	return []string{"NodeOperatorAdded", "NodeOperatorNameSet", "NodeOperatorRewardAddressSet"}
}

// finalizedUsedSigningKeys is not read: no endpoint exposes it, and skipping it halves the
// per-operator round trips.
func (r *CuratedReader) Operators(ctx context.Context, c *eth.Client, at eth.BlockRef, m model.StakingModule) ([]model.Operator, error) {
	moduleAddress := m.RawAddress()
	out, err := c.CallOne(ctx, at, eth.Call{
		To: moduleAddress, ABI: &eth.RegistryABI, Method: "getNodeOperatorsCount",
	})
	if err != nil {
		return nil, fmt.Errorf("getNodeOperatorsCount: %w", err)
	}
	n, err := operatorCount(out)
	if err != nil {
		return nil, err
	}

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
			op.DepositableValidatorsCount, err = depositableFromSummary(int32(start+i), results[width+i])
			if err != nil {
				return nil, err
			}
			operators[start+i] = op
		}
	}
	return operators, nil
}

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
