package lidocore

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/lidofinance/keys-api-versioned/internal/eth"
	"github.com/lidofinance/keys-api-versioned/internal/model"
)

// Node operator groups live in the module's MetaRegistry, not in the module, and
// only curated-onchain-v2 has one. Both halves of that matter to a consumer: a
// group has to arrive, and a module without groups has to say so rather than
// report everyone as ungrouped.

func TestCMv2OperatorsCarryTheirGroup(t *testing.T) {
	node := &operatorsNode{registry: metaRegistryUnderTest, operators: 4, groups: map[int64]int64{
		0: 7,
		1: 7,
		2: 0, // NO_GROUP_ID: the module has groups and this operator is in none
		3: 3,
	}}
	server := httptest.NewServer(node)
	defer server.Close()

	client := eth.NewClient(server.URL, eth.Options{Concurrency: 2, BatchSize: 8})
	reader := &CommunityReader{}

	operators, err := reader.Operators(context.Background(), client,
		eth.BlockRef{Tag: "latest"}, moduleUnderTest)
	if err != nil {
		t.Fatalf("Operators: %v", err)
	}
	if len(operators) != 4 {
		t.Fatalf("got %d operators, want 4", len(operators))
	}

	for _, want := range []struct {
		index int32
		group model.GroupID
		json  string
	}{
		{0, 7, "7"},
		{1, 7, "7"},
		{2, model.NoGroupID, "null"},
		{3, 3, "3"},
	} {
		got := operators[want.index].Group
		if got == nil {
			t.Fatalf("operator %d: no group read from a module that has a registry", want.index)
		}
		if *got != want.group {
			t.Errorf("operator %d: got group %d, want %d", want.index, *got, want.group)
		}
		// The sentinel is a real on-chain value and a real group id is not: the
		// renderer has to be able to tell them apart, which is the whole point of
		// the type carrying its own JSON form.
		encoded, err := got.MarshalJSON()
		if err != nil {
			t.Fatalf("operator %d: marshal group: %v", want.index, err)
		}
		if string(encoded) != want.json {
			t.Errorf("operator %d: group renders as %s, want %s", want.index, encoded, want.json)
		}
	}
}

// A module with no MetaRegistry has no groups, and the cost of finding that out
// must not scale with the operator count — one reverting probe per module, not one
// failed call per operator. This is the CSM case, and modules 1 and 2 never get
// here at all because CuratedReader has no registry to ask.
func TestAModuleWithoutAMetaRegistryReportsNoGroupsAndAsksOnce(t *testing.T) {
	node := &operatorsNode{operators: 5}
	server := httptest.NewServer(node)
	defer server.Close()

	client := eth.NewClient(server.URL, eth.Options{Concurrency: 2, BatchSize: 8})
	reader := &CommunityReader{}
	ctx := context.Background()

	for cycle := 0; cycle < 2; cycle++ {
		operators, err := reader.Operators(ctx, client, eth.BlockRef{Tag: "latest"}, moduleUnderTest)
		if err != nil {
			t.Fatalf("cycle %d: Operators: %v", cycle, err)
		}
		for _, o := range operators {
			if o.Group != nil {
				t.Errorf("cycle %d: operator %d reported group %d on a module with no registry",
					cycle, o.Index, *o.Group)
			}
			if !strings.HasPrefix(o.Name, "CSM Operator ") {
				t.Errorf("cycle %d: operator %d is named %q, want the generated CSM name",
					cycle, o.Index, o.Name)
			}
		}
	}

	if got := node.groupCalls.Load(); got != 0 {
		t.Errorf("%d group calls went to a module with no registry, want 0", got)
	}
	// The probe is cached, so the second cycle costs nothing — a module that reverts
	// on every cycle is log spam and a wasted round trip per cycle forever.
	if got := node.registryProbes.Load(); got != 1 {
		t.Errorf("META_REGISTRY was probed %d times over two cycles, want 1", got)
	}
}

// The group read has to travel in the batch the names already pay for. A call per
// operator is what the reader is written to avoid, and it is invisible in a
// correctness assertion — mainnet's module 4 would simply be 45 round trips slower.
func TestGroupsCostNoExtraRoundTrip(t *testing.T) {
	node := &operatorsNode{registry: metaRegistryUnderTest, operators: 20, groups: map[int64]int64{}}
	server := httptest.NewServer(node)
	defer server.Close()

	client := eth.NewClient(server.URL, eth.Options{Concurrency: 1, BatchSize: 100})
	reader := &CommunityReader{}

	if _, err := reader.Operators(context.Background(), client,
		eth.BlockRef{Tag: "latest"}, moduleUnderTest); err != nil {
		t.Fatalf("Operators: %v", err)
	}

	stats := client.Stats()
	if stats.Calls != 82 {
		t.Fatalf("got %d calls, want 82 (count, registry, and four per operator)", stats.Calls)
	}
	// count + META_REGISTRY are one call each; the 40 module reads and the 40
	// registry reads are one request each at this batch size.
	if stats.Requests > 4 {
		t.Errorf("got %d requests for %d calls, want the reads batched into at most 4",
			stats.Requests, stats.Calls)
	}
}

const metaRegistryUnderTest = "0x00000000000000000000000000000000000ce115"

// operatorsNode answers the calls CommunityReader.Operators makes. A module with an
// empty registry address reverts META_REGISTRY, the way plain CSM does.
type operatorsNode struct {
	registry  string
	operators int64
	groups    map[int64]int64

	registryProbes atomic.Int64
	groupCalls     atomic.Int64
}

func (n *operatorsNode) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

func (n *operatorsNode) answer(method string, params []any) (string, error) {
	if method != "eth_call" {
		return "", fmt.Errorf("unexpected method %s", method)
	}
	call, _ := params[0].(map[string]any)
	to, _ := call["to"].(string)
	data := common.FromHex(fmt.Sprint(call["data"]))
	if len(data) < 4 {
		return "", fmt.Errorf("call to %s carries no selector", to)
	}
	selector := [4]byte(data[:4])

	switch {
	case selector == [4]byte(eth.CMv2ABI.Methods["META_REGISTRY"].ID):
		n.registryProbes.Add(1)
		if n.registry == "" {
			return "", fmt.Errorf("execution reverted")
		}
		return packAddress(n.registry), nil

	case selector == [4]byte(eth.CSMABI.Methods["getNodeOperatorsCount"].ID):
		return packUint(big.NewInt(n.operators)), nil

	case selector == [4]byte(eth.CSMABI.Methods["getNodeOperator"].ID):
		return packNodeOperator(argID(data))

	case selector == [4]byte(eth.CSMABI.Methods["getNodeOperatorSummary"].ID):
		return packSummary()

	case selector == [4]byte(eth.MetaRegistryABI.Methods["getOperatorMetadata"].ID):
		return packMetadata(fmt.Sprintf("operator-%d", argID(data)))

	case selector == [4]byte(eth.MetaRegistryABI.Methods["getNodeOperatorGroupId"].ID):
		n.groupCalls.Add(1)
		return packUint(big.NewInt(n.groups[argID(data)])), nil
	}
	return "", fmt.Errorf("unexpected selector %x at %s", selector, to)
}

func argID(data []byte) int64 {
	return new(big.Int).SetBytes(data[4:36]).Int64()
}

func packUint(v *big.Int) string {
	return "0x" + common.Bytes2Hex(common.LeftPadBytes(v.Bytes(), 32))
}

func packAddress(a string) string {
	return "0x" + common.Bytes2Hex(common.LeftPadBytes(common.HexToAddress(a).Bytes(), 32))
}

// nodeOperator mirrors the CSM tuple; only the fields the decoder reads are set.
type nodeOperator struct {
	TotalAddedKeys             uint32
	TotalWithdrawnKeys         uint32
	TotalDepositedKeys         uint32
	TotalVettedKeys            uint32
	StuckValidatorsCount       uint32
	DepositableValidatorsCount uint32
	TargetLimit                uint32
	TargetLimitMode            uint8
	TotalExitedKeys            uint32
	EnqueuedCount              uint32
	ManagerAddress             common.Address
	ProposedManagerAddress     common.Address
	RewardAddress              common.Address
	ProposedRewardAddress      common.Address
	ExtendedManagerPermissions bool
	UsedPriorityQueue          bool
}

func packNodeOperator(id int64) (string, error) {
	op := nodeOperator{
		TotalAddedKeys:     2,
		TotalDepositedKeys: 1,
		TotalVettedKeys:    2,
		RewardAddress:      common.BigToAddress(big.NewInt(id + 1)),
	}
	packed, err := eth.CSMABI.Methods["getNodeOperator"].Outputs.Pack(op)
	if err != nil {
		return "", err
	}
	return "0x" + common.Bytes2Hex(packed), nil
}

func packSummary() (string, error) {
	zero := big.NewInt(0)
	packed, err := eth.CSMABI.Methods["getNodeOperatorSummary"].Outputs.Pack(
		zero, zero, zero, zero, zero, zero, zero, big.NewInt(1))
	if err != nil {
		return "", err
	}
	return "0x" + common.Bytes2Hex(packed), nil
}

type operatorMetadata struct {
	Name                 string
	Description          string
	OwnerEditsRestricted bool
}

func packMetadata(name string) (string, error) {
	packed, err := eth.MetaRegistryABI.Methods["getOperatorMetadata"].Outputs.Pack(
		operatorMetadata{Name: name})
	if err != nil {
		return "", err
	}
	return "0x" + common.Bytes2Hex(packed), nil
}
