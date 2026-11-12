package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/lidofinance/keys-api-versioned/internal/store"
	"github.com/lidofinance/keys-api-versioned/internal/testpg"
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
