package eth

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

// fakeNode can refuse aggregates the way an endpoint with a low gas or response ceiling does.
type fakeNode struct {
	// Empty code means the contract is not deployed.
	code []byte
	// Zero means no limit.
	maxAggregate int
	refuseAll    bool
	// refusal replaces "out of gas" as the message an aggregate is refused with.
	refusal string

	mu sync.Mutex
	// outage is the number of next requests answered with a 503.
	outage     int
	requests   int
	plainCalls int
	aggregates []int
}

func (n *fakeNode) set(f func(n *fakeNode)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	f(n)
}

func (n *fakeNode) counts() (requests, plainCalls int, aggregates []int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.requests, n.plainCalls, append([]int(nil), n.aggregates...)
}

func (n *fakeNode) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var reqs []rpcRequest
	if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	n.mu.Lock()
	n.requests++
	down := n.outage > 0
	if down {
		n.outage--
	}
	n.mu.Unlock()
	if down {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}

	out := make([]rpcResponse, 0, len(reqs))
	for _, req := range reqs {
		out = append(out, n.answer(req))
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (n *fakeNode) answer(req rpcRequest) rpcResponse {
	switch req.Method {
	case "eth_getCode":
		return result(req.ID, "0x"+common.Bytes2Hex(n.code))
	case "eth_call":
		call, _ := req.Params[0].(map[string]any)
		to, _ := call["to"].(string)
		data := common.FromHex(fmt.Sprint(call["data"]))
		if strings.EqualFold(to, MulticallAddress) {
			return n.answerAggregate(req.ID, data)
		}
		n.mu.Lock()
		n.plainCalls++
		n.mu.Unlock()
		return result(req.ID, encodeNonce(to))
	default:
		return rpcError(req.ID, "unexpected method "+req.Method)
	}
}

func (n *fakeNode) answerAggregate(id int, data []byte) rpcResponse {
	values, err := MulticallABI.Methods["aggregate3"].Inputs.Unpack(data[4:])
	if err != nil {
		return rpcError(id, "bad aggregate3 input: "+err.Error())
	}
	calls := *abi.ConvertType(values[0], new([]call3)).(*[]call3)

	n.mu.Lock()
	refuse := n.refuseAll || (n.maxAggregate > 0 && len(calls) > n.maxAggregate)
	refusal := n.refusal
	n.mu.Unlock()
	if refuse {
		if refusal == "" {
			refusal = "out of gas"
		}
		return rpcError(id, refusal)
	}

	n.mu.Lock()
	n.aggregates = append(n.aggregates, len(calls))
	n.mu.Unlock()

	results := make([]multicallResult, 0, len(calls))
	for _, call := range calls {
		results = append(results, multicallResult{
			Success:    true,
			ReturnData: common.FromHex(encodeNonce(call.Target.Hex())),
		})
	}
	packed, err := MulticallABI.Methods["aggregate3"].Outputs.Pack(results)
	if err != nil {
		return rpcError(id, err.Error())
	}
	return result(id, "0x"+common.Bytes2Hex(packed))
}

func result(id int, hexValue string) rpcResponse {
	raw, _ := json.Marshal(hexValue)
	return rpcResponse{ID: id, Result: raw}
}

func rpcError(id int, message string) rpcResponse {
	resp := rpcResponse{ID: id}
	resp.Error = &struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}{Code: -32000, Message: message}
	return resp
}

// A per-address answer, so a mismatched result order is visible.
func encodeNonce(address string) string {
	addr := common.HexToAddress(address)
	return "0x" + common.Bytes2Hex(common.LeftPadBytes(big.NewInt(int64(addr[19])).Bytes(), 32))
}

func nonceCalls(n int) []Call {
	if err := loadABIs(); err != nil {
		panic(err)
	}
	calls := make([]Call, 0, n)
	for i := 1; i <= n; i++ {
		calls = append(calls, Call{
			To:     common.BytesToAddress([]byte{byte(i)}).Hex(),
			ABI:    &RegistryABI,
			Method: "getNonce",
		})
	}
	return calls
}

func assertNonces(t *testing.T, calls []Call, results [][]any) {
	t.Helper()
	if len(results) != len(calls) {
		t.Fatalf("got %d results for %d calls", len(results), len(calls))
	}
	for i, values := range results {
		if len(values) != 1 {
			t.Fatalf("call %d: got %d return values", i, len(values))
		}
		got, ok := values[0].(*big.Int)
		if !ok {
			t.Fatalf("call %d: got %T", i, values[0])
		}
		if want := int64(i + 1); got.Int64() != want {
			t.Errorf("call %d (%s): got nonce %d, want %d", i, calls[i].To, got.Int64(), want)
		}
	}
}

func newTestClient(t *testing.T, node *fakeNode, multicall bool) *Client {
	t.Helper()
	server := httptest.NewServer(node)
	t.Cleanup(server.Close)
	return NewClient(server.URL, Options{Concurrency: 1, BatchSize: 2, Multicall: multicall})
}

func TestCallBatchAggregatesIntoOneRequest(t *testing.T) {
	node := &fakeNode{code: []byte{0x60, 0x00}}
	client := newTestClient(t, node, true)

	calls := nonceCalls(5)
	results, err := client.CallBatch(context.Background(), BlockRef{Tag: "latest"}, calls)
	if err != nil {
		t.Fatalf("CallBatch: %v", err)
	}
	assertNonces(t, calls, results)

	requests, _, aggregates := node.counts()
	// One probe plus one aggregate; the probe is paid once per client.
	if requests != 2 {
		t.Errorf("got %d requests, want 2 (probe plus aggregate)", requests)
	}
	if len(aggregates) != 1 || aggregates[0] != 5 {
		t.Errorf("got aggregates %v, want one of 5", aggregates)
	}
	if stats := client.Stats(); stats.Aggregates != 1 || stats.Calls != 5 {
		t.Errorf("got stats %+v, want 1 aggregate carrying 5 calls", stats)
	}
}

func TestCallBatchWithoutMulticallDeployed(t *testing.T) {
	node := &fakeNode{} // no code at the multicall address
	client := newTestClient(t, node, true)

	calls := nonceCalls(5)
	results, err := client.CallBatch(context.Background(), BlockRef{Tag: "latest"}, calls)
	if err != nil {
		t.Fatalf("CallBatch: %v", err)
	}
	assertNonces(t, calls, results)

	_, plainCalls, aggregates := node.counts()
	if len(aggregates) != 0 {
		t.Errorf("got aggregates %v on a chain without the contract", aggregates)
	}
	if plainCalls != 5 {
		t.Errorf("got %d plain calls, want 5", plainCalls)
	}
}

func TestSingleCallSkipsAggregation(t *testing.T) {
	node := &fakeNode{code: []byte{0x60, 0x00}}
	client := newTestClient(t, node, true)

	calls := nonceCalls(1)
	results, err := client.CallBatch(context.Background(), BlockRef{Tag: "latest"}, calls)
	if err != nil {
		t.Fatalf("CallBatch: %v", err)
	}
	assertNonces(t, calls, results)

	requests, _, aggregates := node.counts()
	// No probe either: a lone read never asks whether aggregation is available.
	if requests != 1 || len(aggregates) != 0 {
		t.Errorf("got %d requests and aggregates %v, want one plain call", requests, aggregates)
	}
}

func TestAggregateBisectsWhenRefused(t *testing.T) {
	node := &fakeNode{code: []byte{0x60, 0x00}, maxAggregate: 2}
	client := newTestClient(t, node, true)

	calls := nonceCalls(5)
	results, err := client.CallBatch(context.Background(), BlockRef{Tag: "latest"}, calls)
	if err != nil {
		t.Fatalf("CallBatch: %v", err)
	}
	assertNonces(t, calls, results)

	_, _, aggregates := node.counts()
	for _, size := range aggregates {
		if size > node.maxAggregate {
			t.Errorf("node accepted an aggregate of %d, above its limit %d", size, node.maxAggregate)
		}
	}
	if stats := client.Stats(); stats.Splits == 0 {
		t.Errorf("got stats %+v, want at least one split", stats)
	}
	if got := client.mcMaxCost.Load(); got > 2 {
		t.Errorf("cost budget stayed at %d after a refusal", got)
	}
}

func TestReadsTooBigToShareGasSkipAggregation(t *testing.T) {
	node := &fakeNode{code: []byte{0x60, 0x00}}
	client := newTestClient(t, node, true)

	// Only the one-key tail may share a gas budget; the two full key batches must stay out.
	calls := nonceCalls(202)
	calls[0].Cost = 1100
	calls[1].Cost = 1100

	results, err := client.CallBatch(context.Background(), BlockRef{Tag: "latest"}, calls)
	if err != nil {
		t.Fatalf("CallBatch: %v", err)
	}
	assertNonces(t, calls, results)

	_, plainCalls, aggregates := node.counts()
	if plainCalls != 2 {
		t.Errorf("got %d plain calls, want the two expensive reads", plainCalls)
	}
	if len(aggregates) != 1 || aggregates[0] != 200 {
		t.Errorf("got aggregates %v, want one of 200", aggregates)
	}
}

func TestAggregationStopsAfterRepeatedRefusals(t *testing.T) {
	node := &fakeNode{code: []byte{0x60, 0x00}, refuseAll: true}
	client := newTestClient(t, node, true)

	for round := 0; round < 3; round++ {
		calls := nonceCalls(4)
		results, err := client.CallBatch(context.Background(), BlockRef{Tag: "latest"}, calls)
		if err != nil {
			t.Fatalf("round %d: CallBatch: %v", round, err)
		}
		assertNonces(t, calls, results)
	}

	if stats := client.Stats(); stats.Fallbacks < maxAggregateFallbacks {
		t.Fatalf("got stats %+v, want at least %d fallbacks", stats, maxAggregateFallbacks)
	}
	if client.aggregationEnabled() {
		t.Error("aggregation is still on after an endpoint refused it repeatedly")
	}

	before := client.Stats()
	calls := nonceCalls(4)
	results, err := client.CallBatch(context.Background(), BlockRef{Tag: "latest"}, calls)
	if err != nil {
		t.Fatalf("CallBatch after aggregation was disabled: %v", err)
	}
	assertNonces(t, calls, results)
	if after := client.Stats(); after.Aggregates != before.Aggregates {
		t.Errorf("sent %d more aggregates after aggregation was disabled",
			after.Aggregates-before.Aggregates)
	}
}

func TestAggregateReportsTheCallThatReverted(t *testing.T) {
	node := &revertingNode{fakeNode: fakeNode{code: []byte{0x60, 0x00}}, failAt: 2, reason: "OPERATOR_NOT_FOUND"}
	server := httptest.NewServer(node)
	t.Cleanup(server.Close)
	client := NewClient(server.URL, Options{Concurrency: 1, BatchSize: 2, Multicall: true})

	calls := nonceCalls(4)
	_, err := client.CallBatch(context.Background(), BlockRef{Tag: "latest"}, calls)
	if err == nil {
		t.Fatal("a reverting call inside an aggregate returned no error")
	}
	// A JSON-RPC batch cannot say which read failed; an aggregate can.
	if !strings.Contains(err.Error(), calls[2].To) || !strings.Contains(err.Error(), "OPERATOR_NOT_FOUND") {
		t.Errorf("error does not identify the failing call and its reason: %v", err)
	}
}

type revertingNode struct {
	fakeNode
	failAt int
	reason string
}

func (n *revertingNode) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var reqs []rpcRequest
	if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	out := make([]rpcResponse, 0, len(reqs))
	for _, req := range reqs {
		resp := n.fakeNode.answer(req)
		if req.Method == "eth_call" && resp.Error == nil {
			if patched, ok := n.patch(req, resp); ok {
				resp = patched
			}
		}
		out = append(out, resp)
	}
	_ = json.NewEncoder(w).Encode(out)
}

func (n *revertingNode) patch(req rpcRequest, resp rpcResponse) (rpcResponse, bool) {
	call, _ := req.Params[0].(map[string]any)
	to, _ := call["to"].(string)
	if !strings.EqualFold(to, MulticallAddress) {
		return resp, false
	}

	var hexResult string
	if err := json.Unmarshal(resp.Result, &hexResult); err != nil {
		return resp, false
	}
	values, err := MulticallABI.Unpack("aggregate3", common.FromHex(hexResult))
	if err != nil {
		return resp, false
	}
	results := *abi.ConvertType(values[0], new([]multicallResult)).(*[]multicallResult)
	if n.failAt >= len(results) {
		return resp, false
	}

	reason, err := abi.Arguments{{Type: abi.Type{T: abi.StringTy}}}.Pack(n.reason)
	if err != nil {
		return resp, false
	}
	results[n.failAt] = multicallResult{
		Success:    false,
		ReturnData: append(common.FromHex("0x08c379a0"), reason...),
	}

	packed, err := MulticallABI.Methods["aggregate3"].Outputs.Pack(results)
	if err != nil {
		return resp, false
	}
	return result(req.ID, "0x"+common.Bytes2Hex(packed)), true
}

func TestMeteredAggregatesGroupsThatFitOneBatch(t *testing.T) {
	for _, tc := range []struct {
		name           string
		metered        bool
		wantAggregates []int
		wantPlain      int
	}{
		{"own node keeps the plain path", false, nil, 8},
		{"metered wraps them into one aggregate", true, []int{8}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := &fakeNode{code: []byte{0x60, 0x00}}
			server := httptest.NewServer(node)
			defer server.Close()

			client := NewClient(server.URL, Options{
				Concurrency: 1, BatchSize: 100, Multicall: true, Metered: tc.metered,
			})

			if _, err := client.CallBatch(context.Background(), BlockRef{Tag: "latest"}, nonceCalls(8)); err != nil {
				t.Fatalf("CallBatch: %v", err)
			}

			_, plain, aggregates := node.counts()
			if plain != tc.wantPlain {
				t.Errorf("%d plain calls, want %d", plain, tc.wantPlain)
			}
			if len(aggregates) != len(tc.wantAggregates) {
				t.Fatalf("aggregates %v, want %v", aggregates, tc.wantAggregates)
			}
			for i, want := range tc.wantAggregates {
				if aggregates[i] != want {
					t.Errorf("aggregate %d carried %d calls, want %d", i, aggregates[i], want)
				}
			}
		})
	}
}

func TestMeteredDoesNotWrapASingleCall(t *testing.T) {
	node := &fakeNode{code: []byte{0x60, 0x00}}
	server := httptest.NewServer(node)
	defer server.Close()

	client := NewClient(server.URL, Options{
		Concurrency: 1, BatchSize: 100, Multicall: true, Metered: true,
	})

	if _, err := client.CallBatch(context.Background(), BlockRef{Tag: "latest"}, nonceCalls(1)); err != nil {
		t.Fatalf("CallBatch: %v", err)
	}

	_, plain, aggregates := node.counts()
	if plain != 1 || len(aggregates) != 0 {
		t.Errorf("plain=%d aggregates=%v, want one plain call and no aggregate", plain, aggregates)
	}
}

// An outage is not a size limit: bisecting on it once left the budget at 1 for the rest of the run.
func TestAnOutageDoesNotShrinkTheAggregateBudget(t *testing.T) {
	node := &fakeNode{code: []byte{0x60, 0x00}}
	client := newTestClient(t, node, true)
	ctx := context.Background()

	if _, err := client.CallBatch(ctx, BlockRef{Tag: "latest"}, nonceCalls(8)); err != nil {
		t.Fatalf("warm-up: %v", err)
	}
	budget := client.mcMaxCost.Load()

	node.set(func(n *fakeNode) { n.outage = 1 })
	if _, err := client.CallBatch(ctx, BlockRef{Tag: "latest"}, nonceCalls(8)); err == nil {
		t.Fatal("got an answer through a 503 from the only endpoint")
	}
	if got := client.mcMaxCost.Load(); got != budget {
		t.Errorf("budget went from %d to %d after a 503", budget, got)
	}
	if stats := client.Stats(); stats.Splits != 0 || stats.Fallbacks != 0 {
		t.Errorf("got stats %+v, want no split and no fallback for an outage", stats)
	}

	calls := nonceCalls(8)
	results, err := client.CallBatch(ctx, BlockRef{Tag: "latest"}, calls)
	if err != nil {
		t.Fatalf("after the outage: %v", err)
	}
	assertNonces(t, calls, results)
	if _, _, aggregates := node.counts(); aggregates[len(aggregates)-1] != 8 {
		t.Errorf("got aggregates %v, want the last one to carry all 8 calls", aggregates)
	}
}

// Providers report a size limit as an endpoint error; it must still bisect, not fail the read.
func TestASizeRefusalStillBisects(t *testing.T) {
	node := &fakeNode{code: []byte{0x60, 0x00}, maxAggregate: 2, refusal: "response size exceeded"}
	client := newTestClient(t, node, true)

	calls := nonceCalls(5)
	results, err := client.CallBatch(context.Background(), BlockRef{Tag: "latest"}, calls)
	if err != nil {
		t.Fatalf("CallBatch: %v", err)
	}
	assertNonces(t, calls, results)
	if stats := client.Stats(); stats.Splits == 0 {
		t.Errorf("got stats %+v, want the refused aggregate split", stats)
	}
}

func TestAggregationResumesAfterThePause(t *testing.T) {
	node := &fakeNode{code: []byte{0x60, 0x00}, refuseAll: true}
	client := newTestClient(t, node, true)
	clock := newFakeClock()
	client.now = clock.Now
	ctx := context.Background()

	for round := 0; round < maxAggregateFallbacks; round++ {
		if _, err := client.CallBatch(ctx, BlockRef{Tag: "latest"}, nonceCalls(4)); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
	}
	if client.aggregationEnabled() {
		t.Fatal("aggregation still on after repeated single-call refusals")
	}

	node.set(func(n *fakeNode) { n.refuseAll = false })
	clock.advance(aggregationPause + time.Second)
	if !client.aggregationEnabled() {
		t.Fatal("aggregation still off after the pause")
	}
	if got := client.mcMaxCost.Load(); got != defaultMulticallMaxCost {
		t.Errorf("budget is %d after the pause, want the configured %d", got, defaultMulticallMaxCost)
	}
	before := client.Stats().Aggregates
	if _, err := client.CallBatch(ctx, BlockRef{Tag: "latest"}, nonceCalls(4)); err != nil {
		t.Fatalf("after the pause: %v", err)
	}
	if client.Stats().Aggregates == before {
		t.Error("no aggregate went out after the pause")
	}
}

func TestRefusalsSpreadOverTimeDoNotPauseAggregation(t *testing.T) {
	client := NewClient("http://unused", Options{Multicall: true})
	clock := newFakeClock()
	client.now = clock.Now
	client.mcProbed, client.mcPresent = true, true

	for i := 0; i < 2*maxAggregateFallbacks; i++ {
		client.noteSingleCallRefusal()
		if !client.aggregationEnabled() {
			t.Fatalf("paused after refusal %d, %s apart", i+1, aggregateFallbackWindow/2)
		}
		clock.advance(aggregateFallbackWindow/2 + time.Second)
	}
}

func TestSetURLsForgetsWhatWasLearnedAboutTheOldEndpoints(t *testing.T) {
	bare := &fakeNode{}
	bareServer := httptest.NewServer(bare)
	t.Cleanup(bareServer.Close)
	withMulticall := &fakeNode{code: []byte{0x60, 0x00}}
	multicallServer := httptest.NewServer(withMulticall)
	t.Cleanup(multicallServer.Close)

	client := NewClient(bareServer.URL, Options{Concurrency: 1, BatchSize: 2, Multicall: true})
	ctx := context.Background()
	if _, err := client.CallBatch(ctx, BlockRef{Tag: "latest"}, nonceCalls(4)); err != nil {
		t.Fatalf("before rotation: %v", err)
	}
	client.lowerBudget(1)
	client.endpointFailed(bareServer.URL)

	if err := client.SetURLs(multicallServer.URL); err != nil {
		t.Fatalf("SetURLs: %v", err)
	}
	if got := client.mcMaxCost.Load(); got != defaultMulticallMaxCost {
		t.Errorf("budget is %d after rotation, want the configured %d", got, defaultMulticallMaxCost)
	}
	if len(client.health) != 0 {
		t.Errorf("failure counts of the old endpoints survived rotation: %v", client.health)
	}
	calls := nonceCalls(4)
	results, err := client.CallBatch(ctx, BlockRef{Tag: "latest"}, calls)
	if err != nil {
		t.Fatalf("after rotation: %v", err)
	}
	assertNonces(t, calls, results)
	if _, _, aggregates := withMulticall.counts(); len(aggregates) == 0 {
		t.Error("the new endpoint has multicall but the old probe answer was kept")
	}
}
