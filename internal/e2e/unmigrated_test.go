package e2e

import (
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lidofinance/keys-api-versioned/internal/store"
	"github.com/lidofinance/keys-api-versioned/internal/testpg"
)

// In KEYS_API_MODE=all the API listens while the writer still waits for its lock and migrates, and a
// schema rebuild drops the tables under it. Before any version exists that is the canonical 425,
// not a 500 naming a missing relation.
func TestTheAPIBeforeTheFirstMigrationAnswersTooEarly(t *testing.T) {
	cfg, err := pgxpool.ParseConfig(testpg.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = "never_migrated"
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	srv := testpg.NewServer(t, store.NewAppWithPool(pool, "latest"))

	for path, want := range map[string]int{
		"/v1/keys":      http.StatusTooEarly,
		"/v1/modules":   http.StatusTooEarly,
		"/v1/status":    http.StatusOK,
		"/health/ready": http.StatusServiceUnavailable,
	} {
		resp := srv.Get(t, path)
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("GET %s before the first migration: %d %s, want %d", path, resp.StatusCode, body, want)
		}
	}
}
