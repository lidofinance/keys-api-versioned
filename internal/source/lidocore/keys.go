package lidocore

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"

	"github.com/lidofinance/keys-api-versioned/internal/eth"
	"github.com/lidofinance/keys-api-versioned/internal/model"
)

// Key spans are packed across operators, not within one: cold sync is bound by round trips per
// operator (mainnet: 731 operators, 609 in CSM, most with a handful of keys), not key volume.

type keysSpec struct {
	abi    *abi.ABI
	method string
	decode func(values []any, moduleAddress string, op model.Operator, offset int32) ([]model.Key, error)
}

type keysPacing struct {
	keysBatch       uint64
	callsPerRequest int
	// keysPerRequest keeps a group inside the node's eth_call gas cap; a group always carries at
	// least one call, so a larger span still goes out.
	keysPerRequest uint64
	concurrency    int
}

func (p keysPacing) withDefaults() keysPacing {
	if p.keysBatch == 0 {
		p.keysBatch = KeysBatchSize
	}
	if p.callsPerRequest <= 0 {
		p.callsPerRequest = CallsPerRequest
	}
	if p.keysPerRequest == 0 {
		p.keysPerRequest = KeysPerRequest
	}
	if p.concurrency <= 0 {
		p.concurrency = 4
	}
	return p
}

type keyRead struct {
	op   model.Operator
	span keySpan
}

type keySpan struct{ offset, limit uint64 }

func planKeyReads(operators []model.Operator, from map[int32]int32, batch uint64) []keyRead {
	var reads []keyRead
	for _, op := range operators {
		start := uint64(max(from[op.Index], 0))
		if uint64(op.TotalSigningKeys) <= start {
			continue
		}
		for _, span := range batchSpans(start, uint64(op.TotalSigningKeys), batch) {
			reads = append(reads, keyRead{op: op, span: span})
		}
	}
	return reads
}

func groupKeyReads(reads []keyRead, maxCalls int, maxKeys uint64) [][]keyRead {
	var (
		groups [][]keyRead
		group  []keyRead
		keys   uint64
	)
	for _, read := range reads {
		full := len(group) >= maxCalls || (len(group) > 0 && keys+read.span.limit > maxKeys)
		if full {
			groups = append(groups, group)
			group, keys = nil, 0
		}
		group = append(group, read)
		keys += read.span.limit
	}
	if len(group) > 0 {
		groups = append(groups, group)
	}
	return groups
}

// streamKeys runs groups concurrently but emits in plan order; in-flight memory is bounded by
// concurrency × keysPerRequest.
func streamKeys(
	ctx context.Context,
	c *eth.Client,
	at eth.BlockRef,
	moduleAddress string,
	operators []model.Operator,
	from map[int32]int32,
	spec keysSpec,
	pacing keysPacing,
	log *slog.Logger,
	fn func(model.Key) error,
) error {
	pacing = pacing.withDefaults()
	if err := checkKeyTotals(operators, from); err != nil {
		return err
	}
	groups := groupKeyReads(planKeyReads(operators, from, pacing.keysBatch), pacing.callsPerRequest, pacing.keysPerRequest)
	emitted := make(map[int32]int64, len(operators))

	started := time.Now()
	var read int
	for start := 0; start < len(groups); start += pacing.concurrency {
		end := min(start+pacing.concurrency, len(groups))
		window := groups[start:end]
		fetched := make([][]model.Key, len(window))

		var (
			wg      sync.WaitGroup
			firstMu sync.Mutex
			firstEr error
		)
		for i, group := range window {
			wg.Add(1)
			go func(i int, group []keyRead) {
				defer wg.Done()
				// A panic here would bypass the cycle's recover and stop the process.
				defer func() {
					if r := recover(); r != nil {
						err := fmt.Errorf("panic reading %s keys: %v", moduleAddress, r)
						firstMu.Lock()
						if firstEr == nil {
							firstEr = err
						}
						firstMu.Unlock()
					}
				}()
				keys, err := readKeyGroup(ctx, c, at, moduleAddress, spec, group)
				if err != nil {
					firstMu.Lock()
					if firstEr == nil {
						firstEr = err
					}
					firstMu.Unlock()
					return
				}
				fetched[i] = keys
			}(i, group)
		}
		wg.Wait()
		if firstEr != nil {
			return firstEr
		}

		for _, keys := range fetched {
			for i := range keys {
				if err := fn(keys[i]); err != nil {
					return err
				}
				emitted[keys[i].OperatorIndex]++
				read++
			}
		}

		// Without this a slow node was indistinguishable from a hang.
		lastGroup := window[len(window)-1]
		logProgress(log, moduleAddress, lastGroup[len(lastGroup)-1], end, len(groups), read, started)
	}
	for _, op := range operators {
		if want := int64(op.TotalSigningKeys - from[op.Index]); emitted[op.Index] != want {
			return fmt.Errorf("operator %d of %s: read %d keys from offset %d, totalSigningKeys is %d",
				op.Index, moduleAddress, emitted[op.Index], from[op.Index], op.TotalSigningKeys)
		}
	}
	return nil
}

// checkKeyTotals runs before planning: the plan holds one entry per batch of keys.
func checkKeyTotals(operators []model.Operator, from map[int32]int32) error {
	var total int64
	for _, op := range operators {
		if op.TotalSigningKeys < 0 || int64(op.TotalSigningKeys) > maxKeysPerOperator {
			return fmt.Errorf("operator %d: totalSigningKeys %d is out of range [0, %d]",
				op.Index, op.TotalSigningKeys, maxKeysPerOperator)
		}
		if offset := from[op.Index]; offset < 0 || offset > op.TotalSigningKeys {
			return fmt.Errorf("operator %d: offset %d is out of range [0, %d]",
				op.Index, offset, op.TotalSigningKeys)
		}
		total += int64(op.TotalSigningKeys)
	}
	if total > maxKeysPerModule {
		return fmt.Errorf("module holds %d keys, more than %d", total, maxKeysPerModule)
	}
	return nil
}

func readKeyGroup(
	ctx context.Context,
	c *eth.Client,
	at eth.BlockRef,
	moduleAddress string,
	spec keysSpec,
	group []keyRead,
) ([]model.Key, error) {
	calls := make([]eth.Call, 0, len(group))
	for _, read := range group {
		calls = append(calls, eth.Call{
			To: moduleAddress, ABI: spec.abi, Method: spec.method,
			Args: []any{
				big.NewInt(int64(read.op.Index)),
				new(big.Int).SetUint64(read.span.offset),
				new(big.Int).SetUint64(read.span.limit),
			},
			// Cost decides what may share one aggregate's gas budget: 200 one-key reads can, two full
			// batches cannot.
			Cost: int(read.span.limit),
		})
	}

	results, err := c.CallBatch(ctx, at, calls)
	if err != nil {
		first, last := group[0], group[len(group)-1]
		return nil, fmt.Errorf("%s(operators %d–%d, from offset %d): %w",
			spec.method, first.op.Index, last.op.Index, first.span.offset, err)
	}

	var keys []model.Key
	for i, read := range group {
		decoded, err := spec.decode(results[i], moduleAddress, read.op, int32(read.span.offset))
		if err != nil {
			return nil, fmt.Errorf("%s(op %d, offset %d): %w",
				spec.method, read.op.Index, read.span.offset, err)
		}
		if uint64(len(decoded)) != read.span.limit {
			return nil, fmt.Errorf("%s(op %d, offset %d): got %d keys, asked for %d",
				spec.method, read.op.Index, read.span.offset, len(decoded), read.span.limit)
		}
		keys = append(keys, decoded...)
	}
	return keys, nil
}

func batchSpans(start, total, batch uint64) []keySpan {
	var spans []keySpan
	for offset := start; offset < total; offset += batch {
		spans = append(spans, keySpan{offset: offset, limit: min(batch, total-offset)})
	}
	return spans
}

func logProgress(log *slog.Logger, moduleAddress string, last keyRead, done, groups, read int, started time.Time) {
	if log == nil {
		return
	}
	elapsed := time.Since(started)
	rate := 0
	if elapsed > 0 {
		rate = int(float64(read) / elapsed.Seconds())
	}
	log.Debug("keys progress",
		"module", moduleAddress, "request_groups", groups, "groups_done", done,
		"operator", last.op.Index, "operator_keys", last.op.TotalSigningKeys,
		"keys_read", read,
		"elapsed", elapsed.Round(time.Millisecond).String(), "keys/s", rate)
}
