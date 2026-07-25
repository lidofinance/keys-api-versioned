// Package eth is a minimal execution-layer client: JSON-RPC over HTTP plus ABI
// coding for the handful of calls the ingest needs.
//
// The ABI files are the ones the canonical service uses, copied verbatim, so
// both implementations are guaranteed to call the same methods with the same
// signatures. Only the pieces actually needed are wired up. The one exception is
// multicall3.json, which the canonical service has no use for: it is the standard
// Multicall3 interface, and only aggregate3 is declared.
package eth

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

//go:embed abi/*.json
var abiFS embed.FS

// ABIs loaded once at startup.
var (
	StakingRouterABI abi.ABI
	RegistryABI      abi.ABI
	LocatorABI       abi.ABI
	StakingModuleABI abi.ABI
	CSMABI           abi.ABI
	CMv2ABI          abi.ABI
	MetaRegistryABI  abi.ABI
	MulticallABI     abi.ABI
	loadedABIs       []*abi.ABI
	loadABIsOnce     sync.Once
	loadABIsErr      error
)

func loadABIs() error {
	loadABIsOnce.Do(func() {
		for _, spec := range []struct {
			file string
			out  *abi.ABI
		}{
			{"abi/staking-router.json", &StakingRouterABI},
			{"abi/registry.json", &RegistryABI},
			{"abi/lido-locator.json", &LocatorABI},
			{"abi/IStakingModule.json", &StakingModuleABI},
			{"abi/csm.json", &CSMABI},
			{"abi/cmv2.json", &CMv2ABI},
			{"abi/meta-registry.json", &MetaRegistryABI},
			{"abi/multicall3.json", &MulticallABI},
		} {
			raw, err := abiFS.ReadFile(spec.file)
			if err != nil {
				loadABIsErr = err
				return
			}
			parsed, err := abi.JSON(bytes.NewReader(raw))
			if err != nil {
				loadABIsErr = fmt.Errorf("%s: %w", spec.file, err)
				return
			}
			*spec.out = parsed
			loadedABIs = append(loadedABIs, spec.out)
		}
	})
	return loadABIsErr
}

// EventTopic resolves an event name to its topic hash.
//
// The name is looked up across every loaded ABI, so a caller names the event it
// cares about ("NodeOperatorNameSet") without also having to say which contract
// declares it. Names are unique across these files.
func EventTopic(name string) (common.Hash, error) {
	if err := loadABIs(); err != nil {
		return common.Hash{}, err
	}
	for _, parsed := range loadedABIs {
		if ev, ok := parsed.Events[name]; ok {
			return ev.ID, nil
		}
	}
	return common.Hash{}, fmt.Errorf("no event %q in the loaded ABIs", name)
}

// Client talks to one or more execution nodes over HTTP.
//
// Requests are aggregated into JSON-RPC batches and a semaphore bounds how many
// are in flight, because the nodes are shared and the ingest is the one thing
// here capable of hammering them.
type Client struct {
	urls    []string
	http    *http.Client
	sem     chan struct{}
	batchSz int
	log     *slog.Logger

	// Multicall3 aggregation: whether it was asked for, where it lives, what the
	// presence probe found, and how much of it the endpoint turned out to tolerate.
	multicall  bool
	mcAddress  string
	mcMaxCalls int
	mcMaxCost  atomic.Int64
	mcMu       sync.Mutex
	mcProbed   bool
	mcPresent  bool

	stats stats

	nextID int
	mu     sync.Mutex
}

// Options configure request pacing.
type Options struct {
	// Concurrency caps in-flight HTTP requests.
	Concurrency int
	// BatchSize caps how many calls travel in one JSON-RPC request.
	BatchSize int
	Timeout   time.Duration
	// Multicall packs a multi-call read into one Multicall3.aggregate3 instead of a
	// JSON-RPC batch, which turns N round trips into one. Off by default because it
	// depends on a contract being deployed; when on, presence is probed and the
	// plain path stays as the fallback.
	Multicall bool
	// MulticallAddress overrides the standard deployment address. Only a chain that
	// has Multicall3 somewhere else needs this.
	MulticallAddress string
	// MulticallMaxCalls caps how many reads go into one aggregate, and
	// MulticallMaxCost caps their total Cost — the one that binds in practice, since
	// an aggregate's inner calls share a single gas budget. A read whose own cost is
	// above the budget is never aggregated.
	MulticallMaxCalls int
	MulticallMaxCost  int
	// Log receives one debug line per JSON-RPC request. At debug level this is the
	// only place that shows what a long sync is actually waiting on, which matters
	// because a cold sync spends minutes inside these calls and says nothing.
	Log *slog.Logger
}

// stats counts what the client sent. Round trips are the figure the ingest
// throughput work is about: a cold sync's cost is dominated by how many requests
// it makes, not by how many bytes they carry.
type stats struct {
	requests   atomic.Int64
	calls      atomic.Int64
	aggregates atomic.Int64
	splits     atomic.Int64
	fallbacks  atomic.Int64
}

// Stats is a snapshot of the request counters.
type Stats struct {
	// Requests is HTTP round trips, Calls the contract reads they carried.
	Requests, Calls int64
	// Aggregates is how many requests carried an aggregate3, Splits how many were
	// halved after a node refused one, Fallbacks how many single calls ended up on
	// the plain path.
	Aggregates, Splits, Fallbacks int64
}

// Stats reports the counters so far.
func (c *Client) Stats() Stats {
	return Stats{
		Requests:   c.stats.requests.Load(),
		Calls:      c.stats.calls.Load(),
		Aggregates: c.stats.aggregates.Load(),
		Splits:     c.stats.splits.Load(),
		Fallbacks:  c.stats.fallbacks.Load(),
	}
}

// NewClient builds a client over a comma-separated endpoint list.
func NewClient(urls string, o Options) *Client {
	if o.Concurrency <= 0 {
		o.Concurrency = 4
	}
	if o.BatchSize <= 0 {
		o.BatchSize = 20
	}
	if o.Timeout == 0 {
		o.Timeout = 60 * time.Second
	}
	if o.MulticallAddress == "" {
		o.MulticallAddress = MulticallAddress
	}
	if o.MulticallMaxCalls <= 0 {
		o.MulticallMaxCalls = defaultMulticallMaxCalls
	}
	if o.MulticallMaxCost <= 0 {
		o.MulticallMaxCost = defaultMulticallMaxCost
	}

	var list []string
	for _, u := range strings.Split(urls, ",") {
		if u = strings.TrimSpace(u); u != "" {
			list = append(list, u)
		}
	}

	if o.Log == nil {
		o.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	c := &Client{
		urls:       list,
		http:       &http.Client{Timeout: o.Timeout},
		sem:        make(chan struct{}, o.Concurrency),
		batchSz:    o.BatchSize,
		log:        o.Log,
		multicall:  o.Multicall,
		mcAddress:  strings.ToLower(o.MulticallAddress),
		mcMaxCalls: o.MulticallMaxCalls,
	}
	c.mcMaxCost.Store(int64(o.MulticallMaxCost))
	return c
}

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
}

type rpcResponse struct {
	ID     int             `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (c *Client) id() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextID++
	return c.nextID
}

// do sends a batch and returns the results in request order. On a transport
// failure it moves to the next endpoint, which is what the fallback list is for.
func (c *Client) do(ctx context.Context, reqs []rpcRequest) ([]json.RawMessage, error) {
	select {
	case c.sem <- struct{}{}:
		defer func() { <-c.sem }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	body, err := json.Marshal(reqs)
	if err != nil {
		return nil, err
	}

	method := "batch"
	if len(reqs) > 0 {
		method = reqs[0].Method
	}

	var lastErr error
	for i, url := range c.urls {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")

		started := time.Now()
		c.stats.requests.Add(1)
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			// An endpoint failing is worth a line at warn: the fallback hides it from
			// everything else, and a node quietly out of the rotation is the kind of
			// thing that only shows up as "the sync got slower".
			c.log.Warn("rpc endpoint failed, trying the next one",
				"endpoint", hostOf(url), "method", method, "calls", len(reqs), "err", err)
			continue
		}

		var out []rpcResponse
		err = json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		took := time.Since(started)
		if err != nil {
			lastErr = fmt.Errorf("decode: %w", err)
			c.log.Warn("rpc response could not be decoded",
				"endpoint", hostOf(url), "method", method, "err", err)
			continue
		}

		c.log.Debug("rpc",
			"method", method, "calls", len(reqs), "endpoint", hostOf(url),
			"took", took.Round(time.Millisecond).String(), "request_bytes", len(body),
			"fallback_index", i)

		byID := make(map[int]rpcResponse, len(out))
		for _, r := range out {
			byID[r.ID] = r
		}

		results := make([]json.RawMessage, len(reqs))
		for i, r := range reqs {
			got, ok := byID[r.ID]
			if !ok {
				return nil, fmt.Errorf("no response for id %d (%s)", r.ID, r.Method)
			}
			if got.Error != nil {
				return nil, fmt.Errorf("%s: rpc error %d: %s", r.Method, got.Error.Code, got.Error.Message)
			}
			results[i] = got.Result
		}
		return results, nil
	}

	return nil, fmt.Errorf("all endpoints failed: %w", lastErr)
}

// hostOf trims an endpoint to its host, so a log line stays readable and a URL with
// credentials in it does not end up in a log.
func hostOf(url string) string {
	trimmed := strings.TrimPrefix(strings.TrimPrefix(url, "http://"), "https://")
	if at := strings.LastIndex(trimmed, "@"); at >= 0 {
		trimmed = trimmed[at+1:]
	}
	if slash := strings.IndexByte(trimmed, '/'); slash >= 0 {
		trimmed = trimmed[:slash]
	}
	return trimmed
}

// BlockRef identifies the block a read happens at. A hash pins the branch, which
// is what every registry read should use.
type BlockRef struct {
	Hash   string
	Number *big.Int
	Tag    string
}

func (b BlockRef) param() any {
	if b.Hash != "" {
		return map[string]any{"blockHash": b.Hash, "requireCanonical": false}
	}
	if b.Number != nil {
		return "0x" + b.Number.Text(16)
	}
	if b.Tag != "" {
		return b.Tag
	}
	return "latest"
}

// Head returns the current head block number and hash.
func (c *Client) Head(ctx context.Context) (*big.Int, string, error) {
	res, err := c.do(ctx, []rpcRequest{{
		JSONRPC: "2.0", ID: c.id(), Method: "eth_getBlockByNumber",
		Params: []any{"latest", false},
	}})
	if err != nil {
		return nil, "", err
	}

	var block struct {
		Number string `json:"number"`
		Hash   string `json:"hash"`
	}
	if err := json.Unmarshal(res[0], &block); err != nil {
		return nil, "", err
	}

	n, ok := new(big.Int).SetString(strings.TrimPrefix(block.Number, "0x"), 16)
	if !ok {
		return nil, "", fmt.Errorf("bad block number %q", block.Number)
	}
	return n, block.Hash, nil
}

// ChainID returns the chain the endpoints serve.
func (c *Client) ChainID(ctx context.Context) (int64, error) {
	res, err := c.do(ctx, []rpcRequest{{
		JSONRPC: "2.0", ID: c.id(), Method: "eth_chainId", Params: []any{},
	}})
	if err != nil {
		return 0, err
	}
	var hexID string
	if err := json.Unmarshal(res[0], &hexID); err != nil {
		return 0, err
	}
	n, ok := new(big.Int).SetString(strings.TrimPrefix(hexID, "0x"), 16)
	if !ok {
		return 0, fmt.Errorf("bad chain id %q", hexID)
	}
	return n.Int64(), nil
}

// BlockHashByNumber returns the canonical hash at a height, or an empty string if
// the node does not have that block.
//
// This is the reorg probe: a stored version's hash that no longer matches the
// hash at its height means the branch it was built on is gone.
func (c *Client) BlockHashByNumber(ctx context.Context, number *big.Int) (string, error) {
	res, err := c.do(ctx, []rpcRequest{{
		JSONRPC: "2.0", ID: c.id(), Method: "eth_getBlockByNumber",
		Params: []any{"0x" + number.Text(16), false},
	}})
	if err != nil {
		return "", err
	}
	var block struct {
		Hash string `json:"hash"`
	}
	if err := json.Unmarshal(res[0], &block); err != nil {
		return "", err
	}
	return block.Hash, nil
}

// HasLogs reports whether an address emitted any of the named events in a block
// range. It asks for one log, because the answer is only ever used as a flag.
func (c *Client) HasLogs(ctx context.Context, address string, eventNames []string, fromBlock, toBlock int64) (bool, error) {
	topics := make([]string, 0, len(eventNames))
	for _, name := range eventNames {
		topic, err := EventTopic(name)
		if err != nil {
			return false, err
		}
		topics = append(topics, topic.Hex())
	}

	res, err := c.do(ctx, []rpcRequest{{
		JSONRPC: "2.0", ID: c.id(), Method: "eth_getLogs",
		Params: []any{map[string]any{
			"address":   address,
			"fromBlock": "0x" + big.NewInt(fromBlock).Text(16),
			"toBlock":   "0x" + big.NewInt(toBlock).Text(16),
			"topics":    []any{topics},
		}},
	}})
	if err != nil {
		return false, err
	}

	var logs []json.RawMessage
	if err := json.Unmarshal(res[0], &logs); err != nil {
		return false, err
	}
	return len(logs) > 0, nil
}

// BlockTimestamp returns the timestamp of a block identified by hash.
func (c *Client) BlockTimestamp(ctx context.Context, hash string) (int64, error) {
	res, err := c.do(ctx, []rpcRequest{{
		JSONRPC: "2.0", ID: c.id(), Method: "eth_getBlockByHash",
		Params: []any{hash, false},
	}})
	if err != nil {
		return 0, err
	}
	var block struct {
		Timestamp string `json:"timestamp"`
	}
	if err := json.Unmarshal(res[0], &block); err != nil {
		return 0, err
	}
	n, ok := new(big.Int).SetString(strings.TrimPrefix(block.Timestamp, "0x"), 16)
	if !ok {
		return 0, fmt.Errorf("bad timestamp %q", block.Timestamp)
	}
	return n.Int64(), nil
}

// Call is one contract read.
type Call struct {
	To     string
	ABI    *abi.ABI
	Method string
	Args   []any

	// Cost is how much this read returns, in whatever unit the caller counts in —
	// keys for a key read, and 0 (meaning one) for a plain getter.
	//
	// It exists because aggregation and batching have different economics.
	// Separate eth_calls in a JSON-RPC batch each get their own gas budget; the
	// inner calls of an aggregate share one, and the ceiling is real: on mainnet
	// geth, two reads of 1100 curated keys in one aggregate come back "out of gas"
	// while either alone is fine. So the client packs by cost, not by count, and a
	// read too expensive to share goes out on its own.
	Cost int
}

func (c Call) cost() int {
	if c.Cost <= 0 {
		return 1
	}
	return c.Cost
}

// CallBatch performs reads at one block and unpacks each result with its own ABI.
// Results come back in request order.
//
// How they travel depends on whether aggregating would save a round trip.
// Multicall3 is used only when the plain path would need more than one request —
// more calls than BatchSize allows — because that is the only case where it wins:
// wrapping calls that already fit one JSON-RPC batch measured slightly *worse* on
// the freshness cycle (0.99 s against 0.87 s per quiet cycle), the node having to
// execute a dispatching contract for reads it would otherwise answer directly.
//
// The rule also explains why a hosted provider changes the picture without any
// configuration: dRPC's free plan caps a batch at three requests, so a group of 200
// key reads exceeds it by a wide margin and aggregation carries all of them in one
// call.
func (c *Client) CallBatch(ctx context.Context, at BlockRef, calls []Call) ([][]any, error) {
	if err := loadABIs(); err != nil {
		return nil, err
	}
	c.stats.calls.Add(int64(len(calls)))

	out := make([][]any, len(calls))
	if len(calls) > c.batchSz && c.aggregateReady(ctx, at) {
		if err := c.callAggregate(ctx, at, calls, out); err != nil {
			return nil, err
		}
		return out, nil
	}
	if err := c.callPlain(ctx, at, calls, out); err != nil {
		return nil, err
	}
	return out, nil
}

// callPlain sends the reads as JSON-RPC batches, one eth_call per read.
func (c *Client) callPlain(ctx context.Context, at BlockRef, calls []Call, out [][]any) error {
	for start := 0; start < len(calls); start += c.batchSz {
		end := min(start+c.batchSz, len(calls))
		chunk := calls[start:end]

		reqs := make([]rpcRequest, 0, len(chunk))
		for _, call := range chunk {
			data, err := call.ABI.Pack(call.Method, call.Args...)
			if err != nil {
				return fmt.Errorf("pack %s: %w", call.Method, err)
			}
			reqs = append(reqs, rpcRequest{
				JSONRPC: "2.0", ID: c.id(), Method: "eth_call",
				Params: []any{
					map[string]any{"to": call.To, "data": "0x" + common.Bytes2Hex(data)},
					at.param(),
				},
			})
		}

		results, err := c.do(ctx, reqs)
		if err != nil {
			return err
		}

		for i, raw := range results {
			var hexResult string
			if err := json.Unmarshal(raw, &hexResult); err != nil {
				return err
			}
			values, err := chunk[i].ABI.Unpack(chunk[i].Method, common.FromHex(hexResult))
			if err != nil {
				return fmt.Errorf("unpack %s: %w", chunk[i].Method, err)
			}
			out[start+i] = values
		}
	}
	return nil
}

// CallOne is CallBatch for a single read.
func (c *Client) CallOne(ctx context.Context, at BlockRef, call Call) ([]any, error) {
	res, err := c.CallBatch(ctx, at, []Call{call})
	if err != nil {
		return nil, err
	}
	return res[0], nil
}
