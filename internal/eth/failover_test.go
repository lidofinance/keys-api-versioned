package eth

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Unix(1_700_000_000, 0)} }

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

// answering is an endpoint that answers every call with 0x1 and counts its requests.
func answering(hits *atomic.Int64) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		var reqs []rpcRequest
		_ = json.NewDecoder(r.Body).Decode(&reqs)
		out := make([]rpcResponse, 0, len(reqs))
		for _, req := range reqs {
			out = append(out, result(req.ID, "0x1"))
		}
		_ = json.NewEncoder(w).Encode(out)
	}
}

func TestAnOversizedResponseFailsOverInsteadOfBeingBuffered(t *testing.T) {
	huge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chunk := []byte(strings.Repeat(" ", 1<<20))
		_, _ = w.Write([]byte("["))
		for written := 0; written <= maxResponseBytes; written += len(chunk) {
			if _, err := w.Write(chunk); err != nil {
				return
			}
		}
		_, _ = w.Write([]byte("]"))
	}))
	defer huge.Close()
	var healthyHits atomic.Int64
	healthy := httptest.NewServer(answering(&healthyHits))
	defer healthy.Close()

	client := NewClient(huge.URL, Options{Concurrency: 1, BatchSize: 8})
	_, err := client.ChainID(context.Background())
	if err == nil || !strings.Contains(err.Error(), "response larger than 64 MiB") {
		t.Fatalf("got %v, want the oversized answer refused", err)
	}

	client = NewClient(huge.URL+","+healthy.URL, Options{Concurrency: 1, BatchSize: 8})
	if _, err := client.ChainID(context.Background()); err != nil {
		t.Fatalf("ChainID: %v, want the next endpoint to answer", err)
	}
	if healthyHits.Load() != 1 {
		t.Errorf("the healthy endpoint got %d requests, want 1", healthyHits.Load())
	}
}

// A redirect would re-send the request body, and with it the read, to a host the provider picks.
func TestARedirectIsNotFollowed(t *testing.T) {
	var targetHits atomic.Int64
	target := httptest.NewServer(answering(&targetHits))
	defer target.Close()
	redirecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirecting.Close()

	client := NewClient(redirecting.URL, Options{Concurrency: 1, BatchSize: 8})
	_, err := client.ChainID(context.Background())
	if err == nil || !strings.Contains(err.Error(), "http 307") {
		t.Fatalf("got %v, want the 307 reported as an endpoint failure", err)
	}
	if targetHits.Load() != 0 {
		t.Errorf("the redirect target got %d requests, want none", targetHits.Load())
	}
}

func TestAMissingResponseIDFailsOverToTheNextEndpoint(t *testing.T) {
	wrongID := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reqs []rpcRequest
		_ = json.NewDecoder(r.Body).Decode(&reqs)
		_ = json.NewEncoder(w).Encode([]rpcResponse{result(reqs[0].ID+1000, "0x1")})
	}))
	defer wrongID.Close()
	var healthyHits atomic.Int64
	healthy := httptest.NewServer(answering(&healthyHits))
	defer healthy.Close()

	client := NewClient(wrongID.URL+","+healthy.URL, Options{Concurrency: 1, BatchSize: 8})
	if _, err := client.ChainID(context.Background()); err != nil {
		t.Fatalf("ChainID: %v, want the next endpoint to answer", err)
	}
	if healthyHits.Load() != 1 {
		t.Errorf("the healthy endpoint got %d requests, want 1", healthyHits.Load())
	}
}

// A primary that hangs before headers used to cost the full request timeout on every request.
func TestAnEndpointSilentBeforeHeadersFailsOverAtTheHeaderTimeout(t *testing.T) {
	silent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The server notices a closed connection only once the body is consumed.
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
		case <-time.After(10 * time.Second):
		}
	}))
	defer silent.Close()
	var healthyHits atomic.Int64
	healthy := httptest.NewServer(answering(&healthyHits))
	defer healthy.Close()

	client := NewClient(silent.URL+","+healthy.URL, Options{
		Concurrency: 1, BatchSize: 8, Timeout: 30 * time.Second, ResponseHeaderTimeout: 100 * time.Millisecond,
	})
	started := time.Now()
	if _, err := client.ChainID(context.Background()); err != nil {
		t.Fatalf("ChainID: %v", err)
	}
	if took := time.Since(started); took > 2*time.Second {
		t.Errorf("failover took %s, want about the 100ms header timeout", took)
	}
}

func TestAnEndpointThatKeepsFailingIsTriedLastDuringItsCooldown(t *testing.T) {
	var downHits, upHits atomic.Int64
	var upFailing atomic.Bool
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downHits.Add(1)
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}))
	defer down.Close()
	answer := answering(&upHits)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if upFailing.Load() {
			upHits.Add(1)
			http.Error(w, "bad gateway", http.StatusBadGateway)
			return
		}
		answer(w, r)
	}))
	defer up.Close()

	clock := newFakeClock()
	client := NewClient(down.URL+","+up.URL, Options{Concurrency: 1, BatchSize: 8})
	client.now = clock.Now
	call := func() {
		t.Helper()
		if _, err := client.ChainID(context.Background()); err != nil {
			t.Fatalf("ChainID: %v", err)
		}
	}

	for i := 0; i < breakerThreshold; i++ {
		call()
	}
	if downHits.Load() != breakerThreshold {
		t.Fatalf("the failing primary got %d requests, want %d", downHits.Load(), breakerThreshold)
	}

	call()
	if downHits.Load() != breakerThreshold {
		t.Errorf("the primary was tried during its cool-down (%d requests)", downHits.Load())
	}

	// Last resort: with the healthy endpoint failing too, the cooling one is still asked.
	upFailing.Store(true)
	if _, err := client.ChainID(context.Background()); err == nil {
		t.Fatal("got an answer from two failing endpoints")
	}
	if downHits.Load() != breakerThreshold+1 {
		t.Errorf("the cooling endpoint got %d requests when everything else failed, want %d",
			downHits.Load(), breakerThreshold+1)
	}
	upFailing.Store(false)

	clock.advance(breakerCooldown + time.Second)
	before := downHits.Load()
	call()
	if downHits.Load() != before+1 {
		t.Errorf("the primary was not retried after its cool-down")
	}
	call()
	if downHits.Load() != before+1 {
		t.Errorf("one more failure after the cool-down did not send the primary back")
	}
}

func TestACanceledRequestDoesNotCountAgainstTheEndpoint(t *testing.T) {
	var hits atomic.Int64
	server := httptest.NewServer(answering(&hits))
	defer server.Close()
	client := NewClient(server.URL, Options{Concurrency: 1, BatchSize: 8})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < breakerThreshold+1; i++ {
		_, _ = client.do(ctx, []rpcRequest{{JSONRPC: "2.0", ID: client.id(), Method: "eth_chainId"}})
	}
	client.healthMu.Lock()
	defer client.healthMu.Unlock()
	if h := client.health[server.URL]; h != nil {
		t.Errorf("canceled requests recorded %d failures against the endpoint", h.failures)
	}
}
