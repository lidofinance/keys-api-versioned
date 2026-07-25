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

// Reading a module's keys is the same walk for both module families: split every
// operator's key range into contract-call-sized spans, read the spans, emit the
// keys in operator order. Only the getter and the decoder differ, so the walk
// lives here and each reader supplies those two.
//
// What matters about the shape is that spans are packed across operators rather
// than within one. The measured limit on a cold sync is round trips per operator,
// not key volume: mainnet has 731 operators across four modules, 609 of them in
// CSM, and most CSM operators hold a handful of keys. One request per operator
// therefore spends the whole sync waiting on latency for reads that would fit
// together many times over. Packed, the tail collapses — with Multicall3 the pack
// becomes a single eth_call, and without it a JSON-RPC batch, which is why the
// reader hands whole groups to the client and lets it decide how they travel.

// keysSpec is what differs between module families: the getter to call and the
// decoder that turns its return values into keys.
type keysSpec struct {
	abi    *abi.ABI
	method string
	decode func(values []any, moduleAddress string, op model.Operator, offset int32) ([]model.Key, error)
}

// keysPacing bounds how much goes into one request and how many run at once.
type keysPacing struct {
	// keysBatch is how many keys one contract call asks for.
	keysBatch uint64
	// callsPerRequest caps how many calls are packed into one request group.
	callsPerRequest int
	// keysPerRequest caps how many keys those calls may return in total, which is
	// what keeps a group inside the node's eth_call gas cap and response size limit
	// when the calls are large. A group always carries at least one call, so a span
	// bigger than this cap still goes out.
	keysPerRequest uint64
	// concurrency is how many request groups are in flight.
	concurrency int
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

// keyRead is one getter call: a contiguous range of one operator's keys.
type keyRead struct {
	op   model.Operator
	span keySpan
}

type keySpan struct{ offset, limit uint64 }

// planKeyReads lists the calls a module's keys need, in emission order.
func planKeyReads(operators []model.Operator, batch uint64) []keyRead {
	var reads []keyRead
	for _, op := range operators {
		if op.TotalSigningKeys == 0 {
			continue
		}
		for _, span := range batchSpans(uint64(op.TotalSigningKeys), batch) {
			reads = append(reads, keyRead{op: op, span: span})
		}
	}
	return reads
}

// groupKeyReads packs the calls into request-sized groups, keeping order.
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

// streamKeys reads a module's keys and hands them to fn in operator order.
//
// Groups run concurrently but are emitted in plan order, so the callback sees a
// stable sequence and the writer streams straight into the store. In-flight
// memory is bounded by concurrency × keysPerRequest.
func streamKeys(
	ctx context.Context,
	c *eth.Client,
	at eth.BlockRef,
	moduleAddress string,
	operators []model.Operator,
	spec keysSpec,
	pacing keysPacing,
	log *slog.Logger,
	fn func(model.Key) error,
) error {
	pacing = pacing.withDefaults()
	groups := groupKeyReads(planKeyReads(operators, pacing.keysBatch), pacing.callsPerRequest, pacing.keysPerRequest)

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
				read++
			}
		}

		// One line per window at debug. A cold sync spends minutes here and used to
		// say nothing at all, which made a slow node indistinguishable from a hang.
		lastGroup := window[len(window)-1]
		logProgress(log, moduleAddress, lastGroup[len(lastGroup)-1], end, len(groups), read, started)
	}
	return nil
}

// readKeyGroup performs one group's calls and decodes them in order.
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
			// Keys asked for is what this read costs, and the client needs it to
			// decide what may share one aggregate's gas budget: 200 one-key reads
			// can, two full batches cannot.
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
		keys = append(keys, decoded...)
	}
	return keys, nil
}

func batchSpans(total, batch uint64) []keySpan {
	var spans []keySpan
	for offset := uint64(0); offset < total; offset += batch {
		spans = append(spans, keySpan{offset: offset, limit: min(batch, total-offset)})
	}
	return spans
}

// logProgress reports where a module's key read has got to, and how fast.
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
