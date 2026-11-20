package eth

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestEndpointFailureClassification(t *testing.T) {
	for _, tc := range []struct {
		name     string
		code     int
		message  string
		endpoint bool
	}{
		{"drpc daily limit", 8, "Your key daily limit exceeded", true},
		{"infura limit", -32005, "daily request count exceeded", true},
		{"alchemy rate limit", 429, "Your app has exceeded its compute units per second capacity", true},
		{"drpc batch cap", -32600, "Batch of more than 3 requests are not allowed on free plan", true},
		{"bad key", -32000, "invalid api key", true},
		{"method missing", -32601, "the method eth_getProof does not exist/is not available", true},
		{"archive state", -32000, "missing trie node", true},
		{"block hash not imported yet", -32000, "header for hash not found", true},
		{"range too wide", -32005, "query returned more than 10000 results", true},

		// Load bearing: the Multicall3 bisect is driven by "out of gas".
		{"out of gas", -32000, "out of gas", false},
		{"reverted", 3, "execution reverted: NOT_ENOUGH_KEYS", false},
		{"gas required", -32000, "gas required exceeds allowance 50000000", false},
		{"intrinsic gas", -32000, "intrinsic gas too low", false},

		{"unknown", -32000, "something nobody has seen before", false},

		{"node failure, unknown wording", -32603, "kaboom", true},
		{"reflected gateway status", 502, "Bad Gateway", true},
		{"revert code, unknown wording", 3, "0x08c379a0", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := endpointFailure(tc.code, tc.message); got != tc.endpoint {
				t.Errorf("endpointFailure(%d, %q) = %v, want %v", tc.code, tc.message, got, tc.endpoint)
			}
		})
	}
}

// A quota error arrives inside a 200 OK.
func TestAnExhaustedEndpointFallsThroughToTheNextOne(t *testing.T) {
	var exhaustedHits, healthyHits atomic.Int64

	exhausted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		exhaustedHits.Add(1)
		var reqs []rpcRequest
		_ = json.NewDecoder(r.Body).Decode(&reqs)
		out := make([]rpcResponse, 0, len(reqs))
		for _, req := range reqs {
			out = append(out, rpcError(req.ID, "Your key daily limit exceeded"))
		}
		_ = json.NewEncoder(w).Encode(out)
	}))
	defer exhausted.Close()

	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		healthyHits.Add(1)
		var reqs []rpcRequest
		_ = json.NewDecoder(r.Body).Decode(&reqs)
		out := make([]rpcResponse, 0, len(reqs))
		for _, req := range reqs {
			out = append(out, result(req.ID, "0x1"))
		}
		_ = json.NewEncoder(w).Encode(out)
	}))
	defer healthy.Close()

	client := NewClient(exhausted.URL+","+healthy.URL, Options{Concurrency: 1, BatchSize: 8})

	if _, err := client.ChainID(context.Background()); err != nil {
		t.Fatalf("ChainID: %v — an exhausted first endpoint must not fail the call", err)
	}
	if exhaustedHits.Load() != 1 {
		t.Errorf("the exhausted endpoint got %d requests, want 1", exhaustedHits.Load())
	}
	if healthyHits.Load() != 1 {
		t.Errorf("the healthy endpoint got %d requests, want 1", healthyHits.Load())
	}
}

func TestACallLevelErrorIsNotRetriedElsewhere(t *testing.T) {
	var first, second atomic.Int64

	handler := func(counter *atomic.Int64) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			counter.Add(1)
			var reqs []rpcRequest
			_ = json.NewDecoder(r.Body).Decode(&reqs)
			out := make([]rpcResponse, 0, len(reqs))
			for _, req := range reqs {
				out = append(out, rpcError(req.ID, "out of gas"))
			}
			_ = json.NewEncoder(w).Encode(out)
		}
	}
	a := httptest.NewServer(handler(&first))
	defer a.Close()
	b := httptest.NewServer(handler(&second))
	defer b.Close()

	client := NewClient(a.URL+","+b.URL, Options{Concurrency: 1, BatchSize: 8})

	_, err := client.ChainID(context.Background())
	if err == nil {
		t.Fatal("want the call-level error returned")
	}
	if !strings.Contains(err.Error(), "out of gas") {
		t.Errorf("err = %v, want it to carry the call's own message", err)
	}
	if first.Load() != 1 {
		t.Errorf("first endpoint got %d requests, want 1", first.Load())
	}
	if second.Load() != 0 {
		t.Errorf("second endpoint got %d requests, want none: the answer would be identical", second.Load())
	}
}

func TestEveryEndpointExhaustedFailsWithTheReason(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reqs []rpcRequest
		_ = json.NewDecoder(r.Body).Decode(&reqs)
		out := make([]rpcResponse, 0, len(reqs))
		for _, req := range reqs {
			out = append(out, rpcError(req.ID, "Your key daily limit exceeded"))
		}
		_ = json.NewEncoder(w).Encode(out)
	}))
	defer server.Close()

	client := NewClient(server.URL+","+server.URL, Options{Concurrency: 1, BatchSize: 8})

	_, err := client.ChainID(context.Background())
	if err == nil {
		t.Fatal("want an error when no endpoint can answer")
	}
	for _, want := range []string{"2 endpoint(s) failed", "daily limit"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("err = %v, want it to contain %q", err, want)
		}
	}
}

func TestANon2xxStatusFallsThrough(t *testing.T) {
	var healthyHits atomic.Int64

	limited := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`[]`))
	}))
	defer limited.Close()

	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		healthyHits.Add(1)
		var reqs []rpcRequest
		_ = json.NewDecoder(r.Body).Decode(&reqs)
		out := make([]rpcResponse, 0, len(reqs))
		for _, req := range reqs {
			out = append(out, result(req.ID, "0x1"))
		}
		_ = json.NewEncoder(w).Encode(out)
	}))
	defer healthy.Close()

	client := NewClient(limited.URL+","+healthy.URL, Options{Concurrency: 1, BatchSize: 8})

	if _, err := client.ChainID(context.Background()); err != nil {
		t.Fatalf("ChainID: %v", err)
	}
	if healthyHits.Load() != 1 {
		t.Errorf("the healthy endpoint got %d requests, want 1", healthyHits.Load())
	}
}

func TestAnUnclassifiedErrorIsCountedAndLogged(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reqs []rpcRequest
		_ = json.NewDecoder(r.Body).Decode(&reqs)
		_ = json.NewEncoder(w).Encode([]rpcResponse{rpcErrorWithCode(reqs[0].ID, -32000, "the frobnicator is cold")})
	}))
	defer srv.Close()
	var logged strings.Builder
	client := NewClient(srv.URL, Options{Concurrency: 1, Log: slog.New(slog.NewTextHandler(&logged, nil))})

	if _, err := client.ChainID(context.Background()); err == nil {
		t.Fatal("an error answer was taken as a result")
	}
	if got := client.Stats().Unclassified; got != 1 {
		t.Errorf("Unclassified = %d, want 1", got)
	}
	if !strings.Contains(logged.String(), "frobnicator") || !strings.Contains(logged.String(), "level=WARN") {
		t.Errorf("the unknown message was not logged at warn:\n%s", logged.String())
	}
}

func rpcErrorWithCode(id, code int, message string) rpcResponse {
	r := rpcResponse{ID: id}
	r.Error = &struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}{code, message}
	return r
}
