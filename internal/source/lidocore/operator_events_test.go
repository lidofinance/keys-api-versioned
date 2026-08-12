package lidocore

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/eth"
	"github.com/lidofinance/keys-api-versioned/internal/model"
)

// The operator event scan is what a quiet cycle spends most of its requests on, and
// eth_getLogs is the method a metered provider bills hardest. What matters here is
// not that the boolean is right — one filter is one filter — but how many requests
// the answer costs, and that the topics asked of each contract are the ones it can
// actually emit.

// logsNode answers eth_getLogs and records the filters it was asked.
type logsNode struct {
	// hits addresses that should return a log, lower-cased.
	hits map[string]bool

	mu      sync.Mutex
	filters []logFilter
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
		ID     int    `json:"id"`
		Method string `json:"method"`
		Params []struct {
			Address any     `json:"address"`
			Topics  [][]any `json:"topics"`
		} `json:"params"`
	}
	if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	type response struct {
		ID     int   `json:"id"`
		Result []any `json:"result"`
	}
	out := make([]response, 0, len(reqs))
	for _, req := range reqs {
		if req.Method != "eth_getLogs" || len(req.Params) == 0 {
			http.Error(w, "unexpected method "+req.Method, http.StatusBadRequest)
			return
		}

		filter := logFilter{}
		switch addr := req.Params[0].Address.(type) {
		case string:
			filter.addresses = []string{strings.ToLower(addr)}
		case []any:
			for _, a := range addr {
				filter.addresses = append(filter.addresses, strings.ToLower(a.(string)))
			}
		}
		if len(req.Params[0].Topics) > 0 {
			for _, topic := range req.Params[0].Topics[0] {
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

// The four mainnet modules are two families: curated and SimpleDVT share the curated
// reader, CSM and CMv2 the community one.
var eventScanModules = []model.StakingModule{
	stakingModule(1, "curated-onchain-v1", "0x1111111111111111111111111111111111111111"),
	stakingModule(2, "curated-onchain-v1", "0x2222222222222222222222222222222222222222"),
	stakingModule(3, "community-onchain-v1", "0x3333333333333333333333333333333333333333"),
	stakingModule(4, "curated-onchain-v2", "0x4444444444444444444444444444444444444444"),
}

// The locator is never read here: the scan needs no router address, which is itself
// worth pinning — a filter is a filter, and resolving contracts for it would be a
// request the answer does not need.
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

	// Curated carries three topics and two addresses; community one topic and two.
	byTopicCount := map[int]logFilter{}
	for _, f := range filters {
		byTopicCount[len(f.topics)] = f
	}
	curated, ok := byTopicCount[3]
	if !ok {
		t.Fatalf("no filter with the three curated topics: %+v", filters)
	}
	if len(curated.addresses) != 2 {
		t.Fatalf("curated filter asked about %v, want both curated module addresses", curated.addresses)
	}
	community, ok := byTopicCount[1]
	if !ok {
		t.Fatalf("no filter with the single community topic: %+v", filters)
	}
	if len(community.addresses) != 2 {
		t.Fatalf("community filter asked about %v, want both CSM-shaped module addresses", community.addresses)
	}
}

func TestOperatorScanStopsAtTheFirstChange(t *testing.T) {
	// The curated family is scanned first and reports a change, so the community
	// filter is never sent: the caller wants one boolean and a rebuild follows either
	// way.
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

	// A module type this provider has no reader for holds no keys in any version, so
	// it has no operator metadata here to go stale — and it must not turn the scan
	// into an error, which is what the per-module path avoided by never asking.
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
