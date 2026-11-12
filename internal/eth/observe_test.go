package eth

import (
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// One observation per HTTP round trip, failed and retried ones included: metered plans bill them.

type observed struct {
	mu   sync.Mutex
	seen []RequestInfo
}

func (o *observed) add(r RequestInfo) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.seen = append(o.seen, r)
}

func (o *observed) all() []RequestInfo {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]RequestInfo(nil), o.seen...)
}

func TestObserveCountsOneRoundTripPerRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reqs []rpcRequest
		_ = json.NewDecoder(r.Body).Decode(&reqs)
		out := make([]rpcResponse, 0, len(reqs))
		for _, req := range reqs {
			out = append(out, result(req.ID, "0x1"))
		}
		_ = json.NewEncoder(w).Encode(out)
	}))
	defer server.Close()

	seen := &observed{}
	client := NewClient(server.URL, Options{Concurrency: 1, BatchSize: 8, Observe: seen.add})

	if _, err := client.BlockHashByNumber(context.Background(), big.NewInt(1)); err == nil {
		// Decoding fails on the fake answer; only the round trip is counted here.
		_ = err
	}

	all := seen.all()
	if len(all) != 1 {
		t.Fatalf("got %d observations for one request, want 1: %+v", len(all), all)
	}
	if all[0].Method != "eth_getBlockByNumber" {
		t.Errorf("method label is %q, want the JSON-RPC method", all[0].Method)
	}
	if all[0].Result != "success" {
		t.Errorf("result label is %q, want success", all[0].Result)
	}
	if all[0].Calls != 1 {
		t.Errorf("calls is %d, want 1", all[0].Calls)
	}
}

func TestObserveCountsTheEndpointThatFailed(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "no hijack", http.StatusInternalServerError)
			return
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			return
		}
		conn.Close()
	}))
	defer dead.Close()

	alive := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reqs []rpcRequest
		_ = json.NewDecoder(r.Body).Decode(&reqs)
		out := make([]rpcResponse, 0, len(reqs))
		for _, req := range reqs {
			out = append(out, result(req.ID, "0x2a"))
		}
		_ = json.NewEncoder(w).Encode(out)
	}))
	defer alive.Close()

	seen := &observed{}
	client := NewClient(dead.URL+","+alive.URL, Options{Concurrency: 1, BatchSize: 8, Observe: seen.add})

	if _, err := client.ChainID(context.Background()); err != nil {
		t.Fatalf("ChainID: %v", err)
	}

	all := seen.all()
	if len(all) != 2 {
		t.Fatalf("got %d observations, want 2 — the failed endpoint and the one that answered: %+v", len(all), all)
	}
	if all[0].Result != "error" {
		t.Errorf("first observation is %q, want error", all[0].Result)
	}
	if all[1].Result != "success" {
		t.Errorf("second observation is %q, want success", all[1].Result)
	}
}

func TestObserveMarksACallErrorAsAnError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reqs []rpcRequest
		_ = json.NewDecoder(r.Body).Decode(&reqs)
		out := make([]rpcResponse, 0, len(reqs))
		for _, req := range reqs {
			out = append(out, rpcError(req.ID, "out of gas"))
		}
		_ = json.NewEncoder(w).Encode(out)
	}))
	defer server.Close()

	seen := &observed{}
	client := NewClient(server.URL, Options{Concurrency: 1, BatchSize: 8, Observe: seen.add})

	if _, err := client.ChainID(context.Background()); err == nil {
		t.Fatal("want the rpc error surfaced to the caller")
	}

	all := seen.all()
	if len(all) != 1 {
		t.Fatalf("got %d observations, want 1: %+v", len(all), all)
	}
	if all[0].Result != "error" {
		t.Errorf("result label is %q, want error", all[0].Result)
	}
}

func TestHasAnyLogsAsksOneFilterForEveryAddress(t *testing.T) {
	var (
		mu        sync.Mutex
		requests  int
		addresses any
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reqs []struct {
			ID     int              `json:"id"`
			Method string           `json:"method"`
			Params []map[string]any `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&reqs)

		mu.Lock()
		requests++
		addresses = reqs[0].Params[0]["address"]
		mu.Unlock()

		_, _ = w.Write([]byte(`[{"id":` + itoa(reqs[0].ID) + `,"result":[]}]`))
	}))
	defer server.Close()

	client := NewClient(server.URL, Options{Concurrency: 1, BatchSize: 8})
	changed, err := client.HasAnyLogs(context.Background(),
		[]string{"0xaaaa", "0xbbbb", "0xcccc"}, []string{"NodeOperatorAdded"}, 1, 2)
	if err != nil {
		t.Fatalf("HasAnyLogs: %v", err)
	}
	if changed {
		t.Fatal("no logs returned, want changed=false")
	}

	mu.Lock()
	defer mu.Unlock()
	if requests != 1 {
		t.Fatalf("got %d requests for three addresses, want 1", requests)
	}
	list, ok := addresses.([]any)
	if !ok || len(list) != 3 {
		t.Fatalf("address param is %#v, want a list of three", addresses)
	}
}

func itoa(n int) string {
	return big.NewInt(int64(n)).String()
}
