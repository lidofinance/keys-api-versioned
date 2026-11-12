package main

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/ingest"
	"github.com/lidofinance/keys-api-versioned/internal/metrics"
)

func TestACycleSetsTheRetentionAndUnfinishedBuildGauges(t *testing.T) {
	m := metrics.New(metricsNamespace)
	observeCycle(m, ingest.CycleResult{RetentionHeldAt: 26_000_000, UnfinishedBuilds: 2})
	observeCycle(m, ingest.CycleResult{RetentionHeldAt: 26_000_000, UnfinishedBuilds: -1})

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	for _, want := range []string{"keys_api_retention_held_at_block 2.6e+07", "keys_api_unfinished_builds 2"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("scrape is missing %s", want)
		}
	}
}
