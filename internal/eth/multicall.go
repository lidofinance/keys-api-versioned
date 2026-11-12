package eth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

// MulticallAddress is the same on every chain that has Multicall3 (fixed deployer and nonce).
// Fresh devnets usually have nothing there, hence the presence probe.
const MulticallAddress = "0xca11bde05977b3631167028862be2a173976ca11"

// The cost cap binds: inner calls share one eth_call gas budget, and two 1100-key reads in one
// aggregate run out of gas on mainnet geth. Neither is a correctness bound; refusals bisect.
const (
	defaultMulticallMaxCalls = 200
	defaultMulticallMaxCost  = 1000
)

// An endpoint refusing an aggregate of one refuses all of them: after maxAggregateFallbacks such
// refusals within the window, aggregation pauses, then resumes with the configured budget.
const (
	maxAggregateFallbacks   = 3
	aggregateFallbackWindow = 10 * time.Minute
	aggregationPause        = 30 * time.Minute
)

// Field names must match the ABI's: that is how the tuple coder maps them.
type call3 struct {
	Target       common.Address
	AllowFailure bool
	CallData     []byte
}

type multicallResult struct {
	Success    bool
	ReturnData []byte
}

// aggregateReady caches a successful eth_getCode probe for the client's life; a failed probe is
// not cached. The lock is held across it so concurrent batches probe once.
func (c *Client) aggregateReady(ctx context.Context, at BlockRef) bool {
	if !c.multicall {
		return false
	}

	c.mcMu.Lock()
	defer c.mcMu.Unlock()
	if c.mcProbed {
		return c.aggregationOnLocked()
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

// callAggregate re-reads the budget each chunk: a refused aggregate lowers it.
func (c *Client) callAggregate(ctx context.Context, at BlockRef, calls []Call, out [][]any) error {
	for start := 0; start < len(calls); {
		if !c.aggregationEnabled() {
			return c.callPlain(ctx, at, calls[start:], out[start:])
		}

		budget := int(c.mcMaxCost.Load())
		end := c.aggregateChunkEnd(calls, start, budget)

		// A chunk of one gains nothing from wrapping; it goes plain, along with every following read too
		// big to pair with another.
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

// allowFailure is set so a revert names the call that reverted; on the plain path a revert fails
// the whole JSON-RPC batch.
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
		if !aggregateRefused(err) {
			return err
		}
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

// aggregateRefused: an outage read as a size limit would shrink the budget for the rest of the run.
func aggregateRefused(err error) bool {
	var status *httpStatusError
	if errors.As(err, &status) {
		return status.status == http.StatusRequestEntityTooLarge
	}
	var rpcErr *jsonRPCError
	if !errors.As(err, &rpcErr) {
		return false
	}
	return !endpointFailure(rpcErr.code, rpcErr.message) || sizeRefusal(rpcErr.message)
}

// aggregateFallback finds the size an endpoint tolerates by bisection and remembers it. A single
// call that still fails goes plain, which reports the real error.
func (c *Client) aggregateFallback(ctx context.Context, at BlockRef, calls []Call, out [][]any, cause error) error {
	if ctx.Err() != nil {
		return cause
	}

	if len(calls) == 1 {
		c.stats.fallbacks.Add(1)
		c.log.Warn("aggregate failed for a single call, sending it plainly",
			"method", calls[0].Method, "to", calls[0].To, "err", cause)
		c.noteSingleCallRefusal()
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

// Aggregation reports the aggregate cost budget and whether aggregates go out now: false before
// the presence probe, without Multicall3, or while paused.
func (c *Client) Aggregation() (budget int64, enabled bool) {
	c.mcMu.Lock()
	defer c.mcMu.Unlock()
	paused := !c.mcPausedUntil.IsZero() && c.now().Before(c.mcPausedUntil)
	return c.mcMaxCost.Load(), c.multicall && c.mcPresent && !paused
}

func (c *Client) aggregationEnabled() bool {
	c.mcMu.Lock()
	defer c.mcMu.Unlock()
	return c.aggregationOnLocked()
}

func (c *Client) aggregationOnLocked() bool {
	if !c.mcPresent {
		return false
	}
	if c.mcPausedUntil.IsZero() {
		return true
	}
	if c.now().Before(c.mcPausedUntil) {
		return false
	}
	c.mcPausedUntil = time.Time{}
	c.mcMaxCost.Store(c.mcInitialCost)
	c.log.Info("multicall resumed", "budget", c.mcInitialCost)
	return true
}

// Info, not warn: one call per read degrades throughput, not correctness.
func (c *Client) noteSingleCallRefusal() {
	c.mcMu.Lock()
	defer c.mcMu.Unlock()
	now := c.now()
	if now.Sub(c.mcWindowStart) > aggregateFallbackWindow {
		c.mcWindowStart, c.mcFallbacks = now, 0
	}
	c.mcFallbacks++
	if c.mcFallbacks < maxAggregateFallbacks || now.Before(c.mcPausedUntil) {
		return
	}
	c.mcPausedUntil = now.Add(aggregationPause)
	c.mcFallbacks = 0
	c.log.Info("multicall paused, calls go one per read: the endpoint refuses aggregates even one at a time",
		"pause", aggregationPause.String(), "fallbacks", c.stats.fallbacks.Load())
}

// lowerBudget only lowers: a limit seen once holds until a pause or an endpoint rotation resets it.
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

func totalCost(calls []Call) int {
	total := 0
	for _, call := range calls {
		total += call.cost()
	}
	return total
}

func revertSuffix(data []byte) string {
	reason := revertReason(data)
	if reason == "" {
		return ""
	}
	return ": " + reason
}

// Custom errors and empty reverts come back as the raw selector.
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
