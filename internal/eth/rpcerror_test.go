package eth

import (
	"context"
	"encoding/json"
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
		// The one that cost a day of downtime, verbatim from the log.
		{"drpc daily limit", 8, "Your key daily limit exceeded", true},
		{"infura limit", -32005, "daily request count exceeded", true},
		{"alchemy rate limit", 429, "Your app has exceeded its compute units per second capacity", true},
		{"drpc batch cap", -32600, "Batch of more than 3 requests are not allowed on free plan", true},
		{"bad key", -32000, "invalid api key", true},
		{"method missing", -32601, "the method eth_getProof does not exist/is not available", true},
		{"archive state", -32000, "missing trie node", true},
		{"range too wide", -32005, "query returned more than 10000 results", true},

		// Call-level. Every endpoint answers the same, and the first of these is load
		// bearing: the Multicall3 bisect is driven by seeing it.
		{"out of gas", -32000, "out of gas", false},
		{"reverted", 3, "execution reverted: NOT_ENOUGH_KEYS", false},
		{"gas required", -32000, "gas required exceeds allowance 50000000", false},
		{"intrinsic gas", -32000, "intrinsic gas too low", false},

		// Unknown wording stays call-level: retrying it on every endpoint spends
		// requests and changes nothing.
		{"unknown", -32000, "something nobody has seen before", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := endpointFailure(tc.code, tc.message); got != tc.endpoint {
				t.Errorf("endpointFailure(%d, %q) = %v, want %v", tc.code, tc.message, got, tc.endpoint)
			}
		})
	}
}

// A quota error arrives inside a 200 OK, which is why the fallback never used to
// engage: the loop only advanced on transport failures. With a healthy endpoint behind
// the exhausted one, a cycle should not notice.
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
		// 200 OK, exactly as dRPC answers.
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

// The other half of the rule: a call-level answer must not be multiplied across the
// endpoint list. The aggregate bisect depends on `out of gas` coming straight back, and
// spending a request on every provider to be told the same thing is pure waste.
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

// Every endpoint out of quota is the case that has to fail loudly and name what
// happened, because "all endpoints failed" on its own sends someone to look at the
// network.
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

// A gateway that rate-limits with a non-2xx status and a body that still parses used to
// be read as a valid empty answer, which is worse than an error: the caller decodes
// nothing and believes it.
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
