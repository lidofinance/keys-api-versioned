// Package eth is a minimal execution-layer client. ABI files are the canonical service's,
// copied verbatim, except multicall3.json (standard interface, aggregate3 only).
package eth

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	neturl "net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

//go:embed abi/*.json
var abiFS embed.FS

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

// EventTopic looks the name up across every loaded ABI (names are unique across them); a full
// signature containing "(" is hashed as is.
func EventTopic(name string) (common.Hash, error) {
	if strings.Contains(name, "(") {
		return crypto.Keccak256Hash([]byte(name)), nil
	}
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

type Client struct {
	// Own lock: `mu` serialises id allocation and every request takes it.
	urlsMu  sync.RWMutex
	urls    []string
	http    *http.Client
	sem     chan struct{}
	batchSz int
	log     *slog.Logger

	multicall  bool
	metered    bool
	mcAddress  string
	mcMaxCalls int
	mcMaxCost  atomic.Int64
	mcMu       sync.Mutex
	mcProbed   bool
	mcPresent  bool

	observe func(RequestInfo)

	stats stats

	nextID int
	mu     sync.Mutex
}

// endpoints is copied under the lock: callers walk it during network calls.
func (c *Client) endpoints() []string {
	c.urlsMu.RLock()
	defer c.urlsMu.RUnlock()
	out := make([]string, len(c.urls))
	copy(out, c.urls)
	return out
}

// SetURLs: in-flight requests keep their endpoints; an empty list is refused.
func (c *Client) SetURLs(urls string) error {
	list := splitURLs(urls)
	if len(list) == 0 {
		return errors.New("no usable endpoints")
	}
	c.urlsMu.Lock()
	defer c.urlsMu.Unlock()
	c.urls = list
	return nil
}

type Options struct {
	Concurrency int
	BatchSize   int
	Timeout     time.Duration
	// Multicall packs a multi-call read into one aggregate3; presence is probed and the plain path
	// stays as the fallback.
	Multicall        bool
	MulticallAddress string
	// Metered: endpoints bill per call, not per round trip (see shouldAggregate). The binary
	// defaults it to true; the zero value suits a node you own.
	Metered bool
	// MulticallMaxCost is the cap that binds: an aggregate's inner calls share one gas budget.
	MulticallMaxCalls int
	MulticallMaxCost  int
	Log               *slog.Logger
	// Observe is called once per HTTP round trip, failed and retried ones included.
	Observe func(RequestInfo)
}

type RequestInfo struct {
	Method string
	Calls  int
	// Result matches the canonical fetch middleware's label: "success" or "error".
	Result   string
	Duration time.Duration
	// Endpoint is the host only: hosted providers carry the API key in the path.
	Endpoint string
}

type stats struct {
	requests   atomic.Int64
	calls      atomic.Int64
	aggregates atomic.Int64
	splits     atomic.Int64
	fallbacks  atomic.Int64
}

type Stats struct {
	Requests, Calls int64
	// Splits: requests halved after a node refused an aggregate; Fallbacks: single calls on the plain path.
	Aggregates, Splits, Fallbacks int64
}

func (c *Client) Stats() Stats {
	return Stats{
		Requests:   c.stats.requests.Load(),
		Calls:      c.stats.calls.Load(),
		Aggregates: c.stats.aggregates.Load(),
		Splits:     c.stats.splits.Load(),
		Fallbacks:  c.stats.fallbacks.Load(),
	}
}

func splitURLs(urls string) []string {
	var list []string
	for _, u := range strings.Split(urls, ",") {
		if u = strings.TrimSpace(u); u != "" {
			list = append(list, u)
		}
	}
	return list
}

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

	list := splitURLs(urls)

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
		metered:    o.Metered,
		mcAddress:  strings.ToLower(o.MulticallAddress),
		mcMaxCalls: o.MulticallMaxCalls,
		observe:    o.Observe,
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
	endpoints := c.endpoints()
	for i, url := range endpoints {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")

		started := time.Now()
		c.stats.requests.Add(1)
		resp, err := c.http.Do(req)
		if err != nil {
			c.observed(method, len(reqs), "error", hostOf(url), time.Since(started))
			// *url.Error embeds the full URL, key included: redact before anything logs or wraps it.
			lastErr = redactErr(err)
			c.log.Warn("rpc endpoint failed, trying the next one",
				"endpoint", hostOf(url), "method", method, "calls", len(reqs), "err", lastErr)
			continue
		}

		var out []rpcResponse
		err = json.NewDecoder(resp.Body).Decode(&out)
		status := resp.StatusCode
		resp.Body.Close()
		took := time.Since(started)
		if err != nil {
			c.observed(method, len(reqs), "error", hostOf(url), took)
			lastErr = fmt.Errorf("decode: %w", err)
			c.log.Warn("rpc response could not be decoded",
				"endpoint", hostOf(url), "method", method, "status", status, "err", err)
			continue
		}

		// A 429 with a body that happens to parse would otherwise read as a valid empty answer.
		if status < 200 || status >= 300 {
			c.observed(method, len(reqs), "error", hostOf(url), took)
			lastErr = fmt.Errorf("http %d from %s", status, hostOf(url))
			c.log.Warn("rpc endpoint answered with a non-2xx status, trying the next one",
				"endpoint", hostOf(url), "method", method, "status", status)
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
		var (
			callErr error
			refused bool
			refusal string
			refCode int
		)
		for i, r := range reqs {
			got, ok := byID[r.ID]
			if !ok {
				c.observed(method, len(reqs), "error", hostOf(url), took)
				return nil, fmt.Errorf("no response for id %d (%s)", r.ID, r.Method)
			}
			if got.Error != nil {
				// A call error inside a successful round trip still counts as an error, as in the canonical
				// middleware: a node answering "out of gas" to everything is broken.
				c.observed(method, len(reqs), "error", hostOf(url), took)
				err := fmt.Errorf("%s: rpc error %d: %s", r.Method, got.Error.Code, got.Error.Message)

				// Endpoint failure: try the next endpoint. Call failure: every endpoint would answer the same.
				if endpointFailure(got.Error.Code, got.Error.Message) {
					refused, refusal, refCode = true, got.Error.Message, got.Error.Code
					lastErr = err
				} else {
					callErr = err
				}
				break
			}
			results[i] = got.Result
		}
		if callErr != nil {
			return nil, callErr
		}
		if refused {
			c.log.Warn("rpc endpoint refused the request, trying the next one",
				"endpoint", hostOf(url), "method", method, "calls", len(reqs),
				"code", refCode, "err", refusal)
			continue
		}
		c.observed(method, len(reqs), "success", hostOf(url), took)
		return results, nil
	}

	return nil, fmt.Errorf("all %d endpoint(s) failed, last: %w", len(endpoints), lastErr)
}

func (c *Client) observed(method string, calls int, result, endpoint string, d time.Duration) {
	if c.observe == nil {
		return
	}
	c.observe(RequestInfo{
		Method: method, Calls: calls, Result: result, Duration: d, Endpoint: endpoint,
	})
}

// redactErr rewrites the URL in place, so errors.Is(err, context.Canceled) still works.
func redactErr(err error) error {
	var uerr *neturl.Error
	if errors.As(err, &uerr) {
		uerr.URL = schemeHostOf(uerr.URL)
	}
	return err
}

func schemeHostOf(raw string) string {
	scheme := "http://"
	if strings.HasPrefix(raw, "https://") {
		scheme = "https://"
	}
	return scheme + hostOf(raw)
}

func hostOf(url string) string {
	trimmed := strings.TrimPrefix(strings.TrimPrefix(url, "http://"), "https://")
	// Query and fragment end the host too: a key passed as ?key= with no path stayed in.
	if end := strings.IndexAny(trimmed, "/?#"); end >= 0 {
		trimmed = trimmed[:end]
	}
	if at := strings.LastIndex(trimmed, "@"); at >= 0 {
		trimmed = trimmed[at+1:]
	}
	return trimmed
}

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

func (c *Client) Head(ctx context.Context) (*big.Int, string, int64, error) {
	res, err := c.do(ctx, []rpcRequest{{
		JSONRPC: "2.0", ID: c.id(), Method: "eth_getBlockByNumber",
		Params: []any{"latest", false},
	}})
	if err != nil {
		return nil, "", 0, err
	}

	// The timestamp comes in this same header; no follow-up eth_getBlockByHash.
	var block struct {
		Number    string `json:"number"`
		Hash      string `json:"hash"`
		Timestamp string `json:"timestamp"`
	}
	if err := json.Unmarshal(res[0], &block); err != nil {
		return nil, "", 0, err
	}

	n, ok := new(big.Int).SetString(strings.TrimPrefix(block.Number, "0x"), 16)
	if !ok {
		return nil, "", 0, fmt.Errorf("bad block number %q", block.Number)
	}
	ts, _ := new(big.Int).SetString(strings.TrimPrefix(block.Timestamp, "0x"), 16)
	var seconds int64
	if ts != nil {
		seconds = ts.Int64()
	}
	return n, block.Hash, seconds, nil
}

// FinalizedBlock returns 0 when the node has no finalized block (null or error). It only labels
// built versions; reading registry data at the floating tag breaks cross-counter invariants.
func (c *Client) FinalizedBlock(ctx context.Context) (int64, string, int64, error) {
	res, err := c.do(ctx, []rpcRequest{{
		JSONRPC: "2.0", ID: c.id(), Method: "eth_getBlockByNumber",
		Params: []any{"finalized", false},
	}})
	if err != nil {
		return 0, "", 0, err
	}

	var block *struct {
		Number    string `json:"number"`
		Hash      string `json:"hash"`
		Timestamp string `json:"timestamp"`
	}
	if err := json.Unmarshal(res[0], &block); err != nil {
		return 0, "", 0, err
	}
	if block == nil || block.Number == "" {
		return 0, "", 0, nil
	}

	n, ok := new(big.Int).SetString(strings.TrimPrefix(block.Number, "0x"), 16)
	if !ok {
		return 0, "", 0, fmt.Errorf("bad finalized block number %q", block.Number)
	}
	ts, _ := new(big.Int).SetString(strings.TrimPrefix(block.Timestamp, "0x"), 16)
	var seconds int64
	if ts != nil {
		seconds = ts.Int64()
	}
	return n.Int64(), block.Hash, seconds, nil
}

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

// BlockHashByNumber returns "" when the node does not have the block yet.
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

// HasAnyLogs asks for all addresses in one eth_getLogs. Callers group by module family, so no
// contract is asked about events it cannot emit.
func (c *Client) HasAnyLogs(ctx context.Context, addresses []string, eventNames []string, fromBlock, toBlock int64) (bool, error) {
	if len(addresses) == 0 || len(eventNames) == 0 {
		return false, nil
	}

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
			"address":   addresses,
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

type Call struct {
	To     string
	ABI    *abi.ABI
	Method string
	Args   []any

	// Cost counts what the read returns (keys; 0 = one). Aggregate inner calls share one gas budget:
	// two 1100-key reads in one aggregate run out of gas on mainnet geth, either alone is fine.
	Cost int
}

func (c Call) cost() int {
	if c.Cost <= 0 {
		return 1
	}
	return c.Cost
}

// CallBatch returns results in request order.
func (c *Client) CallBatch(ctx context.Context, at BlockRef, calls []Call) ([][]any, error) {
	if err := loadABIs(); err != nil {
		return nil, err
	}
	c.stats.calls.Add(int64(len(calls)))

	out := make([][]any, len(calls))
	if c.shouldAggregate(len(calls)) && c.aggregateReady(ctx, at) {
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

// Own node: aggregate only when one batch can't hold the group (measured 0.87 s plain vs 0.99 s
// aggregated). Metered: always aggregate, it bills one call instead of N.
func (c *Client) shouldAggregate(calls int) bool {
	if c.metered {
		return calls > 1
	}
	return calls > c.batchSz
}

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

func (c *Client) CallOne(ctx context.Context, at BlockRef, call Call) ([]any, error) {
	res, err := c.CallBatch(ctx, at, []Call{call})
	if err != nil {
		return nil, err
	}
	return res[0], nil
}
