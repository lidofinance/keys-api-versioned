package lidocore

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/lidofinance/keys-api-versioned/internal/eth"
	"github.com/lidofinance/keys-api-versioned/internal/model"
)

// Only curated-onchain-v2 has groups (in its MetaRegistry); a module without them must say so
// rather than report everyone as ungrouped.

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
		eth.BlockRef{Tag: "latest"}, cmv2UnderTest)
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
		// The sentinel is a real on-chain value; the renderer must tell it apart from a group id.
		encoded, err := got.MarshalJSON()
		if err != nil {
			t.Fatalf("operator %d: marshal group: %v", want.index, err)
		}
		if string(encoded) != want.json {
			t.Errorf("operator %d: group renders as %s, want %s", want.index, encoded, want.json)
		}
	}
}

// CSM: names are generated and the registry is never probed. Probing conflated a failed read with
// "no registry" and served generated names for CMv2.
func TestCSMReportsNoGroupsAndNeverProbesARegistry(t *testing.T) {
	node := &operatorsNode{operators: 5}
	server := httptest.NewServer(node)
	defer server.Close()

	client := eth.NewClient(server.URL, eth.Options{Concurrency: 2, BatchSize: 8})
	reader := &CommunityReader{}

	operators, err := reader.Operators(context.Background(), client, eth.BlockRef{Tag: "latest"}, csmUnderTest)
	if err != nil {
		t.Fatalf("Operators: %v", err)
	}
	for _, o := range operators {
		if o.Group != nil {
			t.Errorf("operator %d reported group %d on a module with no registry", o.Index, *o.Group)
		}
		if want := fmt.Sprintf("CSM Operator %d", o.Index); o.Name != want {
			t.Errorf("operator %d is named %q, want %q", o.Index, o.Name, want)
		}
	}
	if got := node.registryProbes.Load(); got != 0 {
		t.Errorf("META_REGISTRY was called %d times on a CSM module, want 0", got)
	}
	if got := node.groupCalls.Load(); got != 0 {
		t.Errorf("%d group calls went to a module with no registry, want 0", got)
	}
}

// A module upgrade can repoint META_REGISTRY, so it is read on every operator read.
func TestCMv2ReadsItsMetaRegistryOnEveryRead(t *testing.T) {
	node := &operatorsNode{registry: metaRegistryUnderTest, operators: 2, groups: map[int64]int64{}}
	server := httptest.NewServer(node)
	defer server.Close()

	client := eth.NewClient(server.URL, eth.Options{Concurrency: 2, BatchSize: 8})
	reader := &CommunityReader{}
	ctx := context.Background()

	if _, err := reader.Operators(ctx, client, eth.BlockRef{Tag: "latest"}, cmv2UnderTest); err != nil {
		t.Fatalf("first read: %v", err)
	}
	const upgraded = "0x00000000000000000000000000000000000ce116"
	node.setRegistry(upgraded)
	if _, err := reader.Operators(ctx, client, eth.BlockRef{Tag: "latest"}, cmv2UnderTest); err != nil {
		t.Fatalf("second read: %v", err)
	}

	if got := node.registryProbes.Load(); got != 2 {
		t.Errorf("META_REGISTRY read %d times over two reads, want 2", got)
	}
	if got := node.lastMetadataTo(); got != upgraded {
		t.Errorf("metadata read from %s after the upgrade, want %s", got, upgraded)
	}
	addresses, err := reader.OperatorEventAddresses(ctx, client, cmv2UnderTest, 1)
	if err != nil {
		t.Fatalf("OperatorEventAddresses: %v", err)
	}
	if len(addresses) != 2 || addresses[1] != upgraded {
		t.Errorf("event scan watches %v, want the module and %s", addresses, upgraded)
	}
}

func TestCMv2FailsWhenItsRegistryCannotBeRead(t *testing.T) {
	node := &operatorsNode{operators: 2} // META_REGISTRY reverts
	server := httptest.NewServer(node)
	defer server.Close()

	client := eth.NewClient(server.URL, eth.Options{Concurrency: 2, BatchSize: 8})
	reader := &CommunityReader{}

	operators, err := reader.Operators(context.Background(), client, eth.BlockRef{Tag: "latest"}, cmv2UnderTest)
	if err == nil {
		t.Fatalf("got %d operators and no error, want the META_REGISTRY failure", len(operators))
	}
}

// Undecodable names get the canonical placeholder instead of failing the write:
// Postgres text accepts neither invalid UTF-8 nor a NUL byte.
func TestCMv2UndecodableNamesGetThePlaceholder(t *testing.T) {
	node := &operatorsNode{registry: metaRegistryUnderTest, operators: 3, groups: map[int64]int64{},
		names: map[int64]string{0: "A\xffB", 1: "nul\x00name", 2: "Ünïcode ok"}}
	server := httptest.NewServer(node)
	defer server.Close()

	client := eth.NewClient(server.URL, eth.Options{Concurrency: 2, BatchSize: 8})
	reader := &CommunityReader{}

	operators, err := reader.Operators(context.Background(), client, eth.BlockRef{Tag: "latest"}, cmv2UnderTest)
	if err != nil {
		t.Fatalf("Operators: %v", err)
	}
	for i, want := range []string{"invalidName0", "invalidName1", "Ünïcode ok"} {
		if operators[i].Name != want {
			t.Errorf("operator %d named %q, want %q", i, operators[i].Name, want)
		}
	}
}

func TestCommunityOperatorsCarryTotalWithdrawnKeys(t *testing.T) {
	node := &operatorsNode{operators: 3}
	server := httptest.NewServer(node)
	defer server.Close()

	client := eth.NewClient(server.URL, eth.Options{Concurrency: 2, BatchSize: 8})
	reader := &CommunityReader{}

	operators, err := reader.Operators(context.Background(), client, eth.BlockRef{Tag: "latest"}, csmUnderTest)
	if err != nil {
		t.Fatalf("Operators: %v", err)
	}
	for i, o := range operators {
		if o.TotalWithdrawnKeys == nil {
			t.Fatalf("operator %d: totalWithdrawnKeys missing", i)
		}
		if *o.TotalWithdrawnKeys != int32(i) {
			t.Errorf("operator %d: totalWithdrawnKeys %d, want %d", i, *o.TotalWithdrawnKeys, i)
		}
	}
}

// A call per operator is invisible to correctness assertions: mainnet's module 4 would be 45
// round trips slower.
func TestGroupsCostNoExtraRoundTrip(t *testing.T) {
	node := &operatorsNode{registry: metaRegistryUnderTest, operators: 20, groups: map[int64]int64{}}
	server := httptest.NewServer(node)
	defer server.Close()

	client := eth.NewClient(server.URL, eth.Options{Concurrency: 1, BatchSize: 100})
	reader := &CommunityReader{}

	if _, err := reader.Operators(context.Background(), client,
		eth.BlockRef{Tag: "latest"}, cmv2UnderTest); err != nil {
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

var (
	cmv2UnderTest = stakingModule(4, "curated-onchain-v2", moduleUnderTest)
	csmUnderTest  = stakingModule(3, "community-onchain-v1", moduleUnderTest)
)

// A module with an empty registry address reverts META_REGISTRY, the way plain CSM does.
type operatorsNode struct {
	mu         sync.Mutex
	registry   string
	operators  int64
	groups     map[int64]int64
	names      map[int64]string
	metadataTo string

	registryProbes atomic.Int64
	groupCalls     atomic.Int64
}

func (n *operatorsNode) setRegistry(address string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.registry = address
}

func (n *operatorsNode) lastMetadataTo() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.metadataTo
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
		n.mu.Lock()
		registry := n.registry
		n.mu.Unlock()
		if registry == "" {
			return "", fmt.Errorf("execution reverted")
		}
		return packAddress(registry), nil

	case selector == [4]byte(eth.CSMABI.Methods["getNodeOperatorsCount"].ID):
		return packUint(big.NewInt(n.operators)), nil

	case selector == [4]byte(eth.CSMABI.Methods["getNodeOperator"].ID):
		return packNodeOperator(argID(data))

	case selector == [4]byte(eth.CSMABI.Methods["getNodeOperatorSummary"].ID):
		return packSummary()

	case selector == [4]byte(eth.MetaRegistryABI.Methods["getOperatorMetadata"].ID):
		n.mu.Lock()
		n.metadataTo = strings.ToLower(to)
		name, ok := n.names[argID(data)]
		n.mu.Unlock()
		if !ok {
			name = fmt.Sprintf("operator-%d", argID(data))
		}
		return packMetadata(name)

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
		TotalWithdrawnKeys: uint32(id),
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
