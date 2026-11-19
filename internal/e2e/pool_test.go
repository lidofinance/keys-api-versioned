package e2e

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/lidofinance/keys-api-versioned/internal/api"
	"github.com/lidofinance/keys-api-versioned/internal/store"
	"github.com/lidofinance/keys-api-versioned/internal/testpg"
	"github.com/lidofinance/keys-api-versioned/internal/testreg"
)

// Stalled responses can exhaust the pool; a request then failed only when its client gave up.
func TestAnExhaustedPoolFailsARequestInsteadOfQueueingForever(t *testing.T) {
	db := testpg.New(t)
	pool := db.ReaderPool(t, 1)
	reader := store.NewAppWithPool(pool, "")
	reader.AcquireTimeout = 200 * time.Millisecond

	held, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()

	started := time.Now()
	err = reader.Snapshot(context.Background(), func(store.Snapshot) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "acquire") {
		t.Fatalf("got %v, want the acquire to time out", err)
	}
	if took := time.Since(started); took > 5*time.Second {
		t.Errorf("the acquire gave up after %s, want about 200ms", took)
	}
}

// Slow readers used to hold every pooled connection: readiness then waited out the acquire timeout
// and failed, and the pod left the Service.
func TestHeldDumpsLeaveThePoolForProbesAndStatus(t *testing.T) {
	testpg.Exclusive(t)

	db := testpg.New(t)
	ctx := context.Background()
	testreg.Streaming().Publish(ctx, t, db.Writer, testreg.At(db.Block(0)))

	pool := db.ReaderPool(t, 4)
	srv := testpg.NewServerWithConfig(t, store.NewAppWithPool(pool, ""), func(cfg *api.Config) {
		cfg.MaxStreams = 2
		cfg.StreamWait = 300 * time.Millisecond
	})

	var held []io.Closer
	for range 2 {
		resp, _ := srv.GetHeld(t, "/v1/keys", 64<<10)
		held = append(held, resp.Body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("held GET /v1/keys: status %d, want 200", resp.StatusCode)
		}
	}
	if leases := db.Leases(t); len(leases) != 1 {
		t.Fatalf("leases %v: want the held dumps to be reading", leases)
	}

	resp := srv.Get(t, "/v1/keys")
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("a third dump: status %d, want 503", resp.StatusCode)
	}

	for _, path := range []string{"/health/ready", "/v1/status", "/v1/modules"} {
		started := time.Now()
		resp := srv.Get(t, path)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s with the dumps held: status %d, want 200", path, resp.StatusCode)
		}
		if took := time.Since(started); took > 2*time.Second {
			t.Errorf("GET %s took %s: it waited for a pooled connection", path, took)
		}
	}

	for _, body := range held {
		body.Close()
	}
	// A connection cancelled mid-query is destroyed in the background.
	srv.WaitForEvents(t, 6, waitLong)
	for deadline := time.Now().Add(waitLong); pool.Stat().AcquiredConns() > 0; {
		if time.Now().After(deadline) {
			t.Fatalf("%d connection(s) still acquired after the held dumps ended", pool.Stat().AcquiredConns())
		}
		time.Sleep(20 * time.Millisecond)
	}
}
