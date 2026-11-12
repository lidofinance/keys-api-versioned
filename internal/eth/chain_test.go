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

func node(chain string, hits *atomic.Int64) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reqs []rpcRequest
		_ = json.NewDecoder(r.Body).Decode(&reqs)
		out := make([]rpcResponse, 0, len(reqs))
		for _, req := range reqs {
			if req.Method == "eth_chainId" {
				out = append(out, result(req.ID, chain))
				continue
			}
			if hits != nil {
				hits.Add(1)
			}
			out = append(out, result(req.ID, "0x1"))
		}
		_ = json.NewEncoder(w).Encode(out)
	}))
}

// The chain id was checked once, on whichever endpoint answered first, and a failure was a warning:
// a fallback on another network served reads whenever the primary was down.
func TestAnEndpointOnAnotherChainServesNoReads(t *testing.T) {
	var hoodiHits, mainnetHits atomic.Int64
	hoodi := node("0x88bb0", &hoodiHits)
	defer hoodi.Close()
	mainnet := node("0x1", &mainnetHits)
	defer mainnet.Close()

	client := NewClient(hoodi.URL+","+mainnet.URL, Options{ChainID: 1, Concurrency: 1})
	for i := 0; i < 3; i++ {
		read := []rpcRequest{{JSONRPC: "2.0", ID: client.id(), Method: "eth_blockNumber", Params: []any{}}}
		if _, err := client.do(context.Background(), read); err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
	}
	if hoodiHits.Load() != 0 {
		t.Errorf("the endpoint on chain 560048 served %d reads", hoodiHits.Load())
	}
	if mainnetHits.Load() != 3 {
		t.Errorf("the endpoint on chain 1 served %d reads, want 3", mainnetHits.Load())
	}

	verified, err := client.VerifyChain(context.Background())
	if err == nil || !strings.Contains(err.Error(), "chain 560048") {
		t.Errorf("VerifyChain: %v (verified %d), want the endpoint on chain 560048 named", err, verified)
	}
}

func TestARotationToAnotherChainIsRefused(t *testing.T) {
	mainnet := node("0x1", nil)
	defer mainnet.Close()
	hoodi := node("0x88bb0", nil)
	defer hoodi.Close()

	client := NewClient(mainnet.URL, Options{ChainID: 1, Concurrency: 1})
	if err := client.SetURLs(mainnet.URL + "," + hoodi.URL); err == nil {
		t.Fatal("a list with an endpoint on chain 560048 was accepted")
	}
	if got := client.endpoints(); len(got) != 1 || got[0] != mainnet.URL {
		t.Errorf("endpoints after the refused rotation: %v", got)
	}
	if err := client.SetURLs(mainnet.URL + ",http://127.0.0.1:1"); err != nil {
		t.Errorf("an unreachable endpoint is checked on use, not refused: %v", err)
	}
}
