package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// A typo in a canonical name fails nothing; it silently yields an empty panel.
func TestScrapeCarriesTheCanonicalNames(t *testing.T) {
	m := New("keys_api")
	m.SetBuildInfo("keys-api-versioned", "0.1.0", "dev", "1")
	m.SetNonce(1, 42)
	m.SetOperatorKeys(1, 7, 100, 5)
	m.ObserveJob("Update Staking Router Modules keys", "success", time.Second)
	m.ObserveModuleUpdate("0xdead", 1000, 2*time.Second)
	m.ObserveRPC("eth_getLogs", "success", "lb.drpc.org", 1, 20*time.Millisecond)
	m.ObserveRequest("GET", "/v1/keys", 200, 4096, 30*time.Millisecond)
	m.BindRPCStats("keys_api", func() RPCStats {
		return RPCStats{Requests: 10, Calls: 200, Aggregates: 2, Splits: 1, Fallbacks: 3}
	})

	body := scrape(t, m)

	for _, want := range []string{
		// Canonical, unprefixed.
		`el_rpc_requests_duration_seconds_count{result="success"} 1`,
		`http_requests_duration_seconds_count{method="GET",statusCode="200"} 1`,
		`build_info{env="dev",name="keys-api-versioned",network="1",version="0.1.0"} 1`,
		// Canonical, prefixed with the canonical prefix rather than ours.
		`lido_keys_api_job_duration_seconds_count{job="Update Staking Router Modules keys",result="success"} 1`,
		`lido_keys_api_sr_module_key_update_duration_seconds_count{srModuleAddress="0xdead",totalKeysAmount="1000"} 1`,
		`lido_keys_api_nonce_value{srModuleId="1"} 42`,
		`lido_keys_api_keys_by_sr_module_and_operator{operator="7",srModuleId="1",used="true"} 100`,
		`lido_keys_api_keys_by_sr_module_and_operator{operator="7",srModuleId="1",used="false"} 5`,
		`keys_api_rpc_requests_total{method="eth_getLogs",result="success"} 1`,
		`keys_api_rpc_calls_total 1`,
		`keys_api_rpc_endpoint_requests_total{endpoint="lb.drpc.org",result="success"} 1`,
		`keys_api_rpc_aggregates_total 2`,
		`keys_api_rpc_splits_total 1`,
		`keys_api_rpc_fallbacks_total 3`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape is missing %s", want)
		}
	}
}

func TestBothHTTPSurfacesAreRecorded(t *testing.T) {
	m := New("keys_api")
	m.ObserveRequest("GET", "/v1/keys", 200, 1, time.Millisecond)
	m.ObserveRequest("GET", "/v1/modules", 200, 1, time.Millisecond)

	body := scrape(t, m)
	if !strings.Contains(body, `http_requests_duration_seconds_count{method="GET",statusCode="200"} 2`) {
		t.Error("the canonical histogram should aggregate both routes under one status")
	}
	if !strings.Contains(body, `keys_api_http_request_duration_seconds_count{route="/v1/keys",status="200"} 1`) {
		t.Error("the route-labelled histogram should keep the routes apart")
	}
}

func TestInventedMethodsShareOneSeries(t *testing.T) {
	m := New("keys_api")
	for _, method := range []string{"FOO", "BAR", "get"} {
		m.ObserveRequest(method, "unmatched", 405, 0, time.Millisecond)
	}

	body := scrape(t, m)
	if !strings.Contains(body, `http_requests_duration_seconds_count{method="other",statusCode="405"} 3`) {
		t.Errorf("invented methods should collapse into one series:\n%s", body)
	}
	if strings.Contains(body, `method="FOO"`) {
		t.Error("a client-chosen method reached a label")
	}
}

func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, err := io.ReadAll(rec.Body)
	if err != nil {
		t.Fatalf("read scrape: %v", err)
	}
	return string(body)
}

func TestPoolAggregationAndInFlightAreExported(t *testing.T) {
	m := New("keys_api")
	m.BindPoolStats("keys_api", func() PoolStats {
		return PoolStats{Acquired: 10, Max: 10, EmptyAcquires: 7, AcquireWait: 3 * time.Second}
	})
	m.BindAggregation("keys_api", func() (int64, bool) { return 250, false })

	release := make(chan struct{})
	entered := make(chan struct{})
	handler := m.CountInFlight(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(entered)
		<-release
	}))
	go handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/keys", nil))
	<-entered
	body := scrape(t, m)
	close(release)

	for _, want := range []string{
		`keys_api_db_pool_acquired_connections 10`,
		`keys_api_db_pool_max_connections 10`,
		`keys_api_db_pool_empty_acquires_total 7`,
		`keys_api_db_pool_acquire_wait_seconds_total 3`,
		`keys_api_rpc_aggregate_budget 250`,
		`keys_api_rpc_aggregation_enabled 0`,
		`keys_api_http_requests_in_flight 1`,
		`keys_api_retention_held_at_block 0`,
		`keys_api_unfinished_builds 0`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape is missing %s", want)
		}
	}
}
