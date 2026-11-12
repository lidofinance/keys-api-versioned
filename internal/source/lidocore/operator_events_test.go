package lidocore

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/eth"
	"github.com/lidofinance/keys-api-versioned/internal/model"
)

// What matters is not the boolean but how many eth_getLogs it costs, and that each contract is
// asked only for topics it can emit.

type logsNode struct {
	hits map[string]bool

	mu            sync.Mutex
	filters       []logFilter
	registryReads int
}

type logFilter struct {
	addresses []string
	topics    []string
}

func (n *logsNode) recorded() []logFilter {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]logFilter(nil), n.filters...)
}

func (n *logsNode) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var reqs []struct {
		ID     int               `json:"id"`
		Method string            `json:"method"`
		Params []json.RawMessage `json:"params"`
	}
	if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	type response struct {
		ID     int `json:"id"`
		Result any `json:"result"`
	}
	out := make([]response, 0, len(reqs))
	for _, req := range reqs {
		if len(req.Params) == 0 {
			http.Error(w, "no params", http.StatusBadRequest)
			return
		}
		// The CMv2 MetaRegistry, resolved once per process for the scan.
		if req.Method == "eth_call" {
			n.mu.Lock()
			n.registryReads++
			n.mu.Unlock()
			out = append(out, response{ID: req.ID, Result: packAddress(metaRegistryUnderTest)})
			continue
		}
		if req.Method != "eth_getLogs" {
			http.Error(w, "unexpected method "+req.Method, http.StatusBadRequest)
			return
		}
		var params struct {
			Address any     `json:"address"`
			Topics  [][]any `json:"topics"`
		}
		if err := json.Unmarshal(req.Params[0], &params); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		filter := logFilter{}
		switch addr := params.Address.(type) {
		case string:
			filter.addresses = []string{strings.ToLower(addr)}
		case []any:
			for _, a := range addr {
				filter.addresses = append(filter.addresses, strings.ToLower(a.(string)))
			}
		}
		if len(params.Topics) > 0 {
			for _, topic := range params.Topics[0] {
				filter.topics = append(filter.topics, topic.(string))
			}
		}

		n.mu.Lock()
		n.filters = append(n.filters, filter)
		n.mu.Unlock()

		logs := []any{}
		for _, address := range filter.addresses {
			if n.hits[address] {
				logs = append(logs, map[string]any{"address": address})
				break
			}
		}
		out = append(out, response{ID: req.ID, Result: logs})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// Mainnet's four modules are two reader families: curated + SimpleDVT, CSM + CMv2.
var eventScanModules = []model.StakingModule{
	stakingModule(1, "curated-onchain-v1", "0x1111111111111111111111111111111111111111"),
	stakingModule(2, "curated-onchain-v1", "0x2222222222222222222222222222222222222222"),
	stakingModule(3, "community-onchain-v1", "0x3333333333333333333333333333333333333333"),
	stakingModule(4, "curated-onchain-v2", "0x4444444444444444444444444444444444444444"),
}

// The scan never reads the locator; only CMv2's MetaRegistry is resolved, once per process.
const locatorForEventScan = "0x00000000000000000000000000000000000010c8"

func stakingModule(id int32, moduleType, address string) model.StakingModule {
	m := model.StakingModule{ID: id, Type: moduleType}
	m.SetRawAddress(address)
	return m
}

func TestOperatorScanCostsOneRequestPerModuleFamily(t *testing.T) {
	node := &logsNode{}
	server := httptest.NewServer(node)
	defer server.Close()

	client := eth.NewClient(server.URL, eth.Options{Concurrency: 2, BatchSize: 8})
	provider := NewProvider(client, locatorForEventScan, Options{})

	changed, err := provider.OperatorsChangedAny(context.Background(), eventScanModules, 100, 200)
	if err != nil {
		t.Fatalf("OperatorsChangedAny: %v", err)
	}
	if changed {
		t.Fatal("no module emitted anything, want changed=false")
	}

	filters := node.recorded()
	if len(filters) != 2 {
		t.Fatalf("got %d eth_getLogs for 4 modules, want 2 — one per module family: %+v", len(filters), filters)
	}

	var curated, community *logFilter
	for i := range filters {
		if slices.Contains(filters[i].addresses, "0x1111111111111111111111111111111111111111") {
			curated = &filters[i]
		} else {
			community = &filters[i]
		}
	}
	if curated == nil || community == nil {
		t.Fatalf("filters do not split by family: %+v", filters)
	}
	if len(curated.topics) != 3 || len(curated.addresses) != 2 {
		t.Fatalf("curated filter %+v, want three topics over both curated modules", *curated)
	}

	wantAddresses := []string{
		"0x3333333333333333333333333333333333333333",
		"0x4444444444444444444444444444444444444444",
		metaRegistryUnderTest,
	}
	if !slices.Equal(community.addresses, wantAddresses) {
		t.Fatalf("community filter asked about %v, want %v", community.addresses, wantAddresses)
	}
	wantTopics := []string{
		eventTopicHex(t, "NodeOperatorRewardAddressChanged"),
		eventTopicHex(t, "VettedSigningKeysCountChanged(uint256,uint256)"),
		eventTopicHex(t, "OperatorMetadataSet(uint256,(string,string,bool))"),
	}
	if !slices.Equal(community.topics, wantTopics) {
		t.Fatalf("community filter topics %v, want %v", community.topics, wantTopics)
	}

	if _, err := provider.OperatorsChangedAny(context.Background(), eventScanModules, 201, 300); err != nil {
		t.Fatalf("second scan: %v", err)
	}
	node.mu.Lock()
	reads := node.registryReads
	node.mu.Unlock()
	if reads != 1 {
		t.Errorf("META_REGISTRY read %d times over two scans, want 1", reads)
	}
}

// Hashes computed independently with `cast keccak`, so a signature typo fails here.
func TestOperatorEventTopicsHashToTheCanonicalValues(t *testing.T) {
	for signature, want := range map[string]string{
		"VettedSigningKeysCountChanged(uint256,uint256)":    "0x947f955eec7e1f626bee3afd2aa47b5de04ddcdd3fe78dc8838213015ef58dfd",
		"OperatorMetadataSet(uint256,(string,string,bool))": "0x27c4b5559e9eb92ed9a6cba3ab99a51ef6b41103e9eaae8877132a3d680dc8c5",
	} {
		if got := eventTopicHex(t, signature); got != want {
			t.Errorf("%s hashes to %s, want %s", signature, got, want)
		}
	}
}

func eventTopicHex(t *testing.T, name string) string {
	t.Helper()
	topic, err := eth.EventTopic(name)
	if err != nil {
		t.Fatalf("EventTopic(%q): %v", name, err)
	}
	return topic.Hex()
}

func TestOperatorScanStopsAtTheFirstChange(t *testing.T) {
	// The curated family reports a change first, so the community filter is never sent.
	node := &logsNode{hits: map[string]bool{"0x2222222222222222222222222222222222222222": true}}
	server := httptest.NewServer(node)
	defer server.Close()

	client := eth.NewClient(server.URL, eth.Options{Concurrency: 2, BatchSize: 8})
	provider := NewProvider(client, locatorForEventScan, Options{})

	changed, err := provider.OperatorsChangedAny(context.Background(), eventScanModules, 100, 200)
	if err != nil {
		t.Fatalf("OperatorsChangedAny: %v", err)
	}
	if !changed {
		t.Fatal("a curated module emitted NodeOperatorNameSet, want changed=true")
	}
	if got := len(node.recorded()); got != 1 {
		t.Fatalf("got %d requests, want 1: the scan should stop at the first family that changed", got)
	}
}

func TestOperatorScanSkipsUnreadableModules(t *testing.T) {
	node := &logsNode{}
	server := httptest.NewServer(node)
	defer server.Close()

	client := eth.NewClient(server.URL, eth.Options{Concurrency: 2, BatchSize: 8})
	provider := NewProvider(client, locatorForEventScan, Options{})

	// A module with no reader holds no keys in any version; it must not turn the scan into an error.
	modules := []model.StakingModule{
		stakingModule(9, "some-future-module-type", "0x9999999999999999999999999999999999999999"),
	}
	changed, err := provider.OperatorsChangedAny(context.Background(), modules, 100, 200)
	if err != nil {
		t.Fatalf("OperatorsChangedAny: %v", err)
	}
	if changed {
		t.Fatal("want changed=false for a module with no reader")
	}
	if got := len(node.recorded()); got != 0 {
		t.Fatalf("got %d requests, want none", got)
	}
}
