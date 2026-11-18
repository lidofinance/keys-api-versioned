package lidocore

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/lidofinance/keys-api-versioned/internal/eth"
	"github.com/lidofinance/keys-api-versioned/internal/model"
)

func TestGroupKeyReadsRespectsBothCaps(t *testing.T) {
	operators := []model.Operator{
		{Index: 0, TotalSigningKeys: 2500},
		{Index: 1, TotalSigningKeys: 0}, // no keys: no call at all
		{Index: 2, TotalSigningKeys: 1},
		{Index: 3, TotalSigningKeys: 1},
		{Index: 4, TotalSigningKeys: 1},
	}

	reads := planKeyReads(operators, nil, 1000)
	if len(reads) != 6 {
		t.Fatalf("got %d reads, want 6 (3 spans for operator 0, one each for 2–4)", len(reads))
	}

	groups := groupKeyReads(reads, 4, 2000)
	// Operator 0's third span travels with the one-key operators.
	want := [][]uint64{{1000, 1000}, {500, 1, 1, 1}}
	if len(groups) != len(want) {
		t.Fatalf("got %d groups, want %d: %v", len(groups), len(want), sizes(groups))
	}
	for i, group := range groups {
		if len(group) != len(want[i]) {
			t.Fatalf("group %d: got %v, want %v", i, sizes(groups), want)
		}
		for j, read := range group {
			if read.span.limit != want[i][j] {
				t.Errorf("group %d call %d: got limit %d, want %d", i, j, read.span.limit, want[i][j])
			}
		}
	}
}

func TestGroupKeyReadsAlwaysCarriesOneCall(t *testing.T) {
	// A span larger than the key cap still goes out: the batch size decides a span's width.
	reads := planKeyReads([]model.Operator{{Index: 0, TotalSigningKeys: 5000}}, nil, 5000)
	groups := groupKeyReads(reads, 200, 100)
	if len(groups) != 1 || len(groups[0]) != 1 {
		t.Fatalf("got %v, want a single group of one call", sizes(groups))
	}
}

func TestPlanKeyReadsStartsAtEachOperatorsOffset(t *testing.T) {
	operators := []model.Operator{
		{Index: 0, TotalSigningKeys: 2500},
		{Index: 1, TotalSigningKeys: 10}, // all deposited: nothing to read
		{Index: 2, TotalSigningKeys: 5},  // no offset: read from 0
	}
	reads := planKeyReads(operators, map[int32]int32{0: 1200, 1: 10}, 1000)

	want := []struct {
		op            int32
		offset, limit uint64
	}{{0, 1200, 1000}, {0, 2200, 300}, {2, 0, 5}}
	if len(reads) != len(want) {
		t.Fatalf("got %d reads, want %d", len(reads), len(want))
	}
	for i, w := range want {
		r := reads[i]
		if r.op.Index != w.op || r.span.offset != w.offset || r.span.limit != w.limit {
			t.Errorf("read %d: operator %d offset %d limit %d, want %d %d %d",
				i, r.op.Index, r.span.offset, r.span.limit, w.op, w.offset, w.limit)
		}
	}
}

// An offset is a claim about the store; the chain at the read block decides whether it holds.
func TestSettledOffsetsKeepOnlyDepositedAndVettedPrefixes(t *testing.T) {
	operators := []model.Operator{
		{Index: 0, TotalSigningKeys: 10, UsedSigningKeys: 5, StakingLimit: 7},
		{Index: 1, TotalSigningKeys: 10, UsedSigningKeys: 5, StakingLimit: 7},
		{Index: 2, TotalSigningKeys: 10, UsedSigningKeys: 5, StakingLimit: 3},
		{Index: 3, TotalSigningKeys: 10, UsedSigningKeys: 5, StakingLimit: 7},
	}
	got := settledOffsets(operators, map[int32]int32{
		0: 5,  // the whole deposited prefix
		1: 6,  // more than the chain has deposited: keys removed, or the store is from another branch
		2: 4,  // deposited but not vetted, which the contracts do not allow
		3: -1, // nonsense
		9: 3,  // an operator the chain does not have
	})
	if len(got) != 1 || got[0] != 5 {
		t.Errorf("applied %v, want only operator 0 at 5", got)
	}
}

// From an offset, the right keys under the right indices, and the count check still holds.
func TestStreamKeysFromOffsets(t *testing.T) {
	operators := []model.Operator{
		{Index: 0, TotalSigningKeys: 2500, StakingLimit: 2500},
		{Index: 1, TotalSigningKeys: 3, StakingLimit: 3},
		{Index: 2, TotalSigningKeys: 4, StakingLimit: 4},
	}
	from := map[int32]int32{0: 1500, 1: 3}

	node := &keysNode{}
	server := httptest.NewServer(node)
	defer server.Close()
	client := eth.NewClient(server.URL, eth.Options{Concurrency: 4, BatchSize: 2})
	reader := &CuratedReader{KeysBatch: 1000, CallsPerRequest: 4, KeysPerRequest: 2000, Concurrency: 3}

	var got []model.Key
	err := reader.Keys(context.Background(), client, eth.BlockRef{Tag: "latest"},
		moduleUnderTest, operators, from, func(k model.Key) error {
			got = append(got, k)
			return nil
		})
	if err != nil {
		t.Fatalf("Keys: %v", err)
	}

	i := 0
	for _, op := range operators {
		for index := from[op.Index]; index < op.TotalSigningKeys; index++ {
			if i >= len(got) {
				t.Fatalf("got %d keys, ran out at operator %d index %d", len(got), op.Index, index)
			}
			key := got[i]
			if key.OperatorIndex != op.Index || key.Index != index || !equalBytes(key.PubkeyBin, pubkeyFor(op.Index, index)) {
				t.Fatalf("key %d: got operator %d index %d, want operator %d index %d",
					i, key.OperatorIndex, key.Index, op.Index, index)
			}
			i++
		}
	}
	if i != len(got) || len(got) != 1000+4 {
		t.Errorf("got %d keys, want 1004 (1000 after operator 0's offset, none of operator 1, all 4 of operator 2)", len(got))
	}

	if err := reader.Keys(context.Background(), client, eth.BlockRef{Tag: "latest"},
		moduleUnderTest, operators, map[int32]int32{2: 5}, func(model.Key) error { return nil }); err == nil {
		t.Error("an offset past the operator's keys was accepted")
	}
}

func sizes(groups [][]keyRead) [][]uint64 {
	out := make([][]uint64, 0, len(groups))
	for _, group := range groups {
		limits := make([]uint64, 0, len(group))
		for _, read := range group {
			limits = append(limits, read.span.limit)
		}
		out = append(out, limits)
	}
	return out
}

// Packing and concurrent groups must still emit keys in operator order with the right index;
// a mix-up would misattribute keys and no endpoint test would notice.
func TestStreamKeysOrder(t *testing.T) {
	operators := []model.Operator{
		{Index: 0, TotalSigningKeys: 2500, StakingLimit: 2000},
		{Index: 1, TotalSigningKeys: 0},
		{Index: 2, TotalSigningKeys: 1, StakingLimit: 1},
		{Index: 3, TotalSigningKeys: 3, StakingLimit: 0},
		{Index: 4, TotalSigningKeys: 1100, StakingLimit: 1100},
	}

	for _, multicall := range []bool{false, true} {
		name := "plain"
		if multicall {
			name = "aggregated"
		}
		t.Run(name, func(t *testing.T) {
			node := &keysNode{}
			server := httptest.NewServer(node)
			defer server.Close()

			// Aggregation engages only when the plain path needs more than one request, so a large batch
			// would make the aggregated subtest identical to the plain one.
			client := eth.NewClient(server.URL, eth.Options{
				Concurrency: 4, BatchSize: 2, Multicall: multicall,
			})
			reader := &CuratedReader{
				KeysBatch: 1000, CallsPerRequest: 4, KeysPerRequest: 2000, Concurrency: 3,
			}

			var got []model.Key
			err := reader.Keys(context.Background(), client, eth.BlockRef{Tag: "latest"},
				moduleUnderTest, operators, nil, func(k model.Key) error {
					got = append(got, k)
					return nil
				})
			if err != nil {
				t.Fatalf("Keys: %v", err)
			}

			var want int
			for _, op := range operators {
				want += int(op.TotalSigningKeys)
			}
			if len(got) != want {
				t.Fatalf("got %d keys, want %d", len(got), want)
			}

			i := 0
			for _, op := range operators {
				for index := int32(0); index < op.TotalSigningKeys; index++ {
					key := got[i]
					if key.OperatorIndex != op.Index || key.Index != index {
						t.Fatalf("key %d: got operator %d index %d, want operator %d index %d",
							i, key.OperatorIndex, key.Index, op.Index, index)
					}
					if wantKey := pubkeyFor(op.Index, index); !equalBytes(key.PubkeyBin, wantKey) {
						t.Fatalf("key %d: got pubkey %x, want %x", i, key.PubkeyBin, wantKey)
					}
					if wantVetted := index < op.StakingLimit; key.Vetted != wantVetted {
						t.Errorf("key %d: got vetted %t, want %t", i, key.Vetted, wantVetted)
					}
					i++
				}
			}

			stats := client.Stats()
			if multicall && stats.Aggregates == 0 {
				t.Error("aggregation was on and nothing was aggregated")
			}
			if !multicall && stats.Aggregates != 0 {
				t.Errorf("aggregation was off and %d aggregates went out", stats.Aggregates)
			}
			if stats.Requests >= stats.Calls {
				t.Errorf("got %d requests for %d calls, want fewer requests than calls",
					stats.Requests, stats.Calls)
			}
		})
	}
}

const moduleUnderTest = "0x0000000000000000000000000000000000000dad"

// pubkeyFor makes a key's bytes identify the key, so a misattributed one is visible.
func pubkeyFor(operator, index int32) []byte {
	key := make([]byte, 48)
	key[0] = byte(operator)
	key[1] = byte(index >> 8)
	key[2] = byte(index)
	return key
}

func signatureFor(operator, index int32) []byte {
	sig := make([]byte, 96)
	sig[0] = byte(operator)
	sig[1] = byte(index >> 8)
	sig[2] = byte(index)
	return sig
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// delta changes how many keys an answer carries for an operator, against what was asked.
type keysNode struct {
	delta map[int32]int
}

type aggregateCall struct {
	Target       common.Address
	AllowFailure bool
	CallData     []byte
}

type aggregateResult struct {
	Success    bool
	ReturnData []byte
}

func (n *keysNode) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var reqs []struct {
		ID     int    `json:"id"`
		Method string `json:"method"`
		Params []any  `json:"params"`
	}
	if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	out := make([]map[string]any, 0, len(reqs))
	for _, req := range reqs {
		value, err := n.answer(req.Method, req.Params)
		if err != nil {
			out = append(out, map[string]any{
				"jsonrpc": "2.0", "id": req.ID,
				"error": map[string]any{"code": -32000, "message": err.Error()},
			})
			continue
		}
		out = append(out, map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": value})
	}
	_ = json.NewEncoder(w).Encode(out)
}

func (n *keysNode) answer(method string, params []any) (string, error) {
	switch method {
	case "eth_getCode":
		return "0x6000", nil // something is deployed at the multicall address
	case "eth_call":
		call, _ := params[0].(map[string]any)
		to, _ := call["to"].(string)
		data := common.FromHex(fmt.Sprint(call["data"]))
		if strings.EqualFold(to, eth.MulticallAddress) {
			return n.answerAggregate(data)
		}
		return n.answerKeys(data)
	default:
		return "", fmt.Errorf("unexpected method %s", method)
	}
}

func (n *keysNode) answerAggregate(data []byte) (string, error) {
	values, err := eth.MulticallABI.Methods["aggregate3"].Inputs.Unpack(data[4:])
	if err != nil {
		return "", err
	}
	calls := *abi.ConvertType(values[0], new([]aggregateCall)).(*[]aggregateCall)

	results := make([]aggregateResult, 0, len(calls))
	for _, call := range calls {
		encoded, err := n.answerKeys(call.CallData)
		if err != nil {
			return "", err
		}
		results = append(results, aggregateResult{Success: true, ReturnData: common.FromHex(encoded)})
	}

	packed, err := eth.MulticallABI.Methods["aggregate3"].Outputs.Pack(results)
	if err != nil {
		return "", err
	}
	return "0x" + common.Bytes2Hex(packed), nil
}

func (n *keysNode) answerKeys(data []byte) (string, error) {
	method := eth.RegistryABI.Methods["getSigningKeys"]
	args, err := method.Inputs.Unpack(data[4:])
	if err != nil {
		return "", err
	}
	operator := int32(args[0].(*big.Int).Int64())
	offset := int32(args[1].(*big.Int).Int64())
	limit := int(args[2].(*big.Int).Int64()) + n.delta[operator]

	var (
		pubkeys    []byte
		signatures []byte
		used       = make([]bool, 0, limit)
	)
	for i := 0; i < limit; i++ {
		index := offset + int32(i)
		pubkeys = append(pubkeys, pubkeyFor(operator, index)...)
		signatures = append(signatures, signatureFor(operator, index)...)
		used = append(used, index%2 == 0)
	}

	packed, err := method.Outputs.Pack(pubkeys, signatures, used)
	if err != nil {
		return "", err
	}
	return "0x" + common.Bytes2Hex(packed), nil
}
