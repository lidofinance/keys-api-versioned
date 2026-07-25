package eth

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

// MulticallAddress is the Multicall3 deployment. It is the same address on every
// chain it exists on, mainnet and Hoodi included, because it is deployed with a
// fixed nonce from a fixed deployer.
//
// Devnets built from scratch usually have nothing there, which is why presence is
// probed rather than assumed.
const MulticallAddress = "0xca11bde05977b3631167028862be2a173976ca11"

// defaultMulticallMaxCalls caps how many reads travel in one aggregate, and
// defaultMulticallMaxCost caps what they may return in total.
//
// The cost budget is the one that binds. An aggregate's inner calls share a single
// eth_call gas budget, and reading keys is what spends it: measured against mainnet
// geth, two reads of 1100 curated keys in one aggregate come back "out of gas",
// while 200 reads of one key each are fine. So the budget is set below one full key
// batch, which puts the split exactly where the win is — the tail of small
// operators aggregates, the big sequential reads do not, and they never needed to,
// because separate eth_calls in a JSON-RPC batch each get their own gas.
//
// Neither number is a correctness bound: an endpoint that refuses an aggregate has
// it bisected and the budget lowered to what worked.
const (
	defaultMulticallMaxCalls = 200
	defaultMulticallMaxCost  = 1000
)

// maxAggregateFallbacks is how many times a lone aggregated call may fail before
// the client stops aggregating altogether.
//
// The limits that make an aggregate fail are set by whoever runs the endpoint —
// a hosted provider caps eth_call gas, response size or compute units far below a
// self-hosted node — and one that refuses an aggregate of one refuses all of them.
// Without this latch the client would pay the whole bisection ladder on every group
// for the rest of the process; with it, an endpoint that dislikes aggregation costs
// three wasted requests and then behaves exactly as it did before this existed.
const maxAggregateFallbacks = 3

// Call3 and multicallResult mirror Multicall3's own structs. The field names have
// to match the ABI's, since that is how the tuple coder maps them.
type call3 struct {
	Target       common.Address
	AllowFailure bool
	CallData     []byte
}

type multicallResult struct {
	Success    bool
	ReturnData []byte
}

// aggregateReady reports whether reads can go through Multicall3.
//
// One eth_getCode answers it, and the answer is cached for the life of the client:
// a deployment does not come and go. A failed probe is not cached — a node hiccup
// at the wrong moment would otherwise cost aggregation for the whole process.
// The lock is held across the probe so a burst of concurrent batches produces one
// probe rather than one each.
func (c *Client) aggregateReady(ctx context.Context, at BlockRef) bool {
	if !c.multicall {
		return false
	}

	c.mcMu.Lock()
	defer c.mcMu.Unlock()
	if c.mcProbed {
		return c.mcPresent
	}

	code, err := c.Code(ctx, c.mcAddress, at)
	if err != nil {
		c.log.Warn("multicall probe failed, sending calls plainly for now",
			"address", c.mcAddress, "err", err)
		return false
	}

	c.mcProbed = true
	c.mcPresent = len(code) > 0
	if c.mcPresent {
		c.log.Debug("multicall available", "address", c.mcAddress, "code_bytes", len(code))
	} else {
		c.log.Info("no multicall contract on this chain, calls go one per request",
			"address", c.mcAddress)
	}
	return c.mcPresent
}

// Code returns the deployed bytecode at an address, empty if there is none.
func (c *Client) Code(ctx context.Context, address string, at BlockRef) ([]byte, error) {
	res, err := c.do(ctx, []rpcRequest{{
		JSONRPC: "2.0", ID: c.id(), Method: "eth_getCode",
		Params: []any{address, at.param()},
	}})
	if err != nil {
		return nil, err
	}
	var hexCode string
	if err := json.Unmarshal(res[0], &hexCode); err != nil {
		return nil, err
	}
	return common.FromHex(hexCode), nil
}

// callAggregate performs reads by packing them into aggregate3, in chunks bounded
// by cost, and sends whatever is too expensive to aggregate on the plain path.
//
// The budget is re-read each time round because a refused aggregate lowers it: once
// an endpoint has shown what it tolerates, later chunks start there instead of
// rediscovering it by bisection.
func (c *Client) callAggregate(ctx context.Context, at BlockRef, calls []Call, out [][]any) error {
	for start := 0; start < len(calls); {
		if !c.aggregationEnabled() {
			return c.callPlain(ctx, at, calls[start:], out[start:])
		}

		budget := int(c.mcMaxCost.Load())
		end := c.aggregateChunkEnd(calls, start, budget)

		// A chunk of one gains nothing from being wrapped: a lone read costs the same
		// eth_call either way, and one too expensive to share a gas budget cannot be
		// aggregated at all. Those go on the plain path — where a JSON-RPC batch still
		// packs them into one request and each gets its own gas — and the run extends
		// over every following read too big to pair with another.
		if end == start+1 {
			for end < len(calls) && 2*calls[end].cost() > budget {
				end++
			}
			if err := c.callPlain(ctx, at, calls[start:end], out[start:end]); err != nil {
				return err
			}
			start = end
			continue
		}

		if err := c.aggregateChunk(ctx, at, calls[start:end], out[start:end]); err != nil {
			return err
		}
		start = end
	}
	return nil
}

// aggregateChunkEnd finds how far a chunk starting at start may reach. A chunk
// always carries at least one call, whatever it costs.
func (c *Client) aggregateChunkEnd(calls []Call, start, budget int) int {
	cost := 0
	end := start
	for end < len(calls) && end-start < c.mcMaxCalls {
		next := cost + calls[end].cost()
		if end > start && next > budget {
			break
		}
		cost = next
		end++
	}
	return end
}

// aggregateChunk sends one aggregate3 and unpacks each result with its own ABI.
//
// Calls go in with allowFailure set, so one revert comes back as a flag on that
// result instead of failing the whole aggregate — and is then reported naming the
// call that reverted, which the plain path cannot do because a revert there fails
// the JSON-RPC batch as a whole.
func (c *Client) aggregateChunk(ctx context.Context, at BlockRef, calls []Call, out [][]any) error {
	packed := make([]call3, 0, len(calls))
	for _, call := range calls {
		data, err := call.ABI.Pack(call.Method, call.Args...)
		if err != nil {
			return fmt.Errorf("pack %s: %w", call.Method, err)
		}
		packed = append(packed, call3{
			Target:       common.HexToAddress(call.To),
			AllowFailure: true,
			CallData:     data,
		})
	}

	input, err := MulticallABI.Pack("aggregate3", packed)
	if err != nil {
		return fmt.Errorf("pack aggregate3: %w", err)
	}

	c.stats.aggregates.Add(1)
	res, err := c.do(ctx, []rpcRequest{{
		JSONRPC: "2.0", ID: c.id(), Method: "eth_call",
		Params: []any{
			map[string]any{"to": c.mcAddress, "data": "0x" + common.Bytes2Hex(input)},
			at.param(),
		},
	}})
	if err != nil {
		return c.aggregateFallback(ctx, at, calls, out, err)
	}

	var hexResult string
	if err := json.Unmarshal(res[0], &hexResult); err != nil {
		return err
	}
	values, err := MulticallABI.Unpack("aggregate3", common.FromHex(hexResult))
	if err != nil {
		return c.aggregateFallback(ctx, at, calls, out, fmt.Errorf("unpack aggregate3: %w", err))
	}
	results, ok := abi.ConvertType(values[0], new([]multicallResult)).(*[]multicallResult)
	if !ok {
		return fmt.Errorf("aggregate3 returned %T", values[0])
	}
	if len(*results) != len(calls) {
		return fmt.Errorf("aggregate3 returned %d results for %d calls", len(*results), len(calls))
	}

	for i, r := range *results {
		if !r.Success {
			return fmt.Errorf("%s at %s reverted inside the aggregate%s",
				calls[i].Method, calls[i].To, revertSuffix(r.ReturnData))
		}
		unpacked, err := calls[i].ABI.Unpack(calls[i].Method, r.ReturnData)
		if err != nil {
			return fmt.Errorf("unpack %s: %w", calls[i].Method, err)
		}
		out[i] = unpacked
	}
	return nil
}

// aggregateFallback recovers from an endpoint refusing an aggregate.
//
// The ceilings that cause it — eth_call gas cap, response size, a hosted
// provider's own request limits — are per-endpoint and invisible from here, so the
// size that works is found by bisection instead of being configured, and the
// working size is remembered. One call that still fails goes out as a plain
// eth_call: at that point the aggregate is not the problem, and the plain path
// reports whatever is.
func (c *Client) aggregateFallback(ctx context.Context, at BlockRef, calls []Call, out [][]any, cause error) error {
	if ctx.Err() != nil {
		return cause
	}

	if len(calls) == 1 {
		c.stats.fallbacks.Add(1)
		c.log.Warn("aggregate failed for a single call, sending it plainly",
			"method", calls[0].Method, "to", calls[0].To, "err", cause)
		if c.stats.fallbacks.Load() >= maxAggregateFallbacks {
			c.disableAggregation("this endpoint refuses aggregated calls even one at a time")
		}
		return c.callPlain(ctx, at, calls, out)
	}

	mid := len(calls) / 2
	c.stats.splits.Add(1)
	c.lowerBudget(totalCost(calls[:mid]))
	c.log.Warn("aggregate refused, halving it and retrying",
		"calls", len(calls), "cost", totalCost(calls),
		"method", calls[0].Method, "err", cause)

	if err := c.aggregateChunk(ctx, at, calls[:mid], out[:mid]); err != nil {
		return err
	}
	return c.aggregateChunk(ctx, at, calls[mid:], out[mid:])
}

// aggregationEnabled reports whether aggregation is still in use. It can be turned
// off mid-run by disableAggregation.
func (c *Client) aggregationEnabled() bool {
	c.mcMu.Lock()
	defer c.mcMu.Unlock()
	return c.mcPresent
}

// disableAggregation stops aggregating for the life of the client and says why.
// Info rather than warn: falling back to one call per read is the behaviour every
// deployment had before aggregation existed, so it degrades throughput, not
// correctness.
func (c *Client) disableAggregation(reason string) {
	c.mcMu.Lock()
	defer c.mcMu.Unlock()
	if !c.mcPresent {
		return
	}
	c.mcPresent = false
	c.log.Info("multicall disabled, calls go one per read from here on",
		"reason", reason, "fallbacks", c.stats.fallbacks.Load())
}

// lowerBudget lowers the cost budget to the half being retried. One-way: the
// budget only ever comes down, because an endpoint's limits do not move during a
// run, and a budget that keeps bouncing back up would pay the bisection ladder on
// every chunk.
func (c *Client) lowerBudget(to int) {
	if to < 1 {
		to = 1
	}
	for {
		current := c.mcMaxCost.Load()
		if current <= int64(to) {
			return
		}
		if c.mcMaxCost.CompareAndSwap(current, int64(to)) {
			c.log.Info("lowering the aggregate cost budget for this endpoint",
				"budget", to, "was", current)
			return
		}
	}
}

// totalCost is what a chunk asks the endpoint to return.
func totalCost(calls []Call) int {
	total := 0
	for _, call := range calls {
		total += call.cost()
	}
	return total
}

// revertSuffix renders a revert reason if the returned data carries one.
func revertSuffix(data []byte) string {
	reason := revertReason(data)
	if reason == "" {
		return ""
	}
	return ": " + reason
}

// revertReason decodes the standard Error(string) revert payload. Custom errors
// and empty reverts come back as the raw selector, which is still more than
// nothing when a getter starts refusing.
func revertReason(data []byte) string {
	if len(data) < 4 {
		return ""
	}
	const errorStringSelector = "08c379a0"
	if common.Bytes2Hex(data[:4]) != errorStringSelector {
		return "revert data 0x" + common.Bytes2Hex(data[:min(len(data), 36)])
	}
	values, err := abi.Arguments{{Type: abi.Type{T: abi.StringTy}}}.Unpack(data[4:])
	if err != nil || len(values) == 0 {
		return ""
	}
	text, _ := values[0].(string)
	return strings.TrimSpace(text)
}
