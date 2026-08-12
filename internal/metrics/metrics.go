// Package metrics is the Prometheus surface of the service.
//
// The gauges are chosen so the questions an on-call person actually asks can be
// answered without reading logs: is the data fresh, is it trusted, is the writer
// making progress, and how long does a dump take.
//
// confirmed_age_seconds is the one to alert on: it climbs whether the cause is a
// stalled node, a failing rebuild or a dead writer. published_version_age_seconds is
// how old the data is, which legitimately climbs whenever the registry is quiet —
// alerting on that pages someone for nothing happening.
//
// # Two prefixes, on purpose
//
// The series above are this service's own, under its own namespace. Alongside them
// this package exports the canonical service's metrics under the canonical service's
// names — `lido_keys_api_*`, plus the three it leaves unprefixed
// (`el_rpc_requests_duration_seconds`, `http_requests_duration_seconds`,
// `build_info`). That is not duplication for its own sake: the existing dashboards
// and alerts are written against those names, and a replacement that renames them
// makes every one of them a porting task. A deployment can point Grafana at this
// service and see the panels it already has.
//
// Where the canonical semantics and ours differ, the canonical name keeps the
// canonical meaning and the difference is documented on the field. Two are worth
// knowing about without reading further:
//
//   - keys_by_sr_module_and_operator is refreshed for the modules a rebuild read,
//     not reset-and-refilled every cycle. The canonical service re-reads it from its
//     database on every iteration; here a quiet cycle reads no operators at all,
//     because a version nothing changed cannot have different operator counts.
//   - validators_registry_* has no counterpart. That is the CL-side registry, which
//     this service does not have, and exporting the gauges with an invented value
//     would be worse than their absence.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the collectors.
type Metrics struct {
	registry *prometheus.Registry

	CyclesTotal   *prometheus.CounterVec
	CycleDuration prometheus.Histogram
	KeysRead      prometheus.Counter
	KeysChanged   prometheus.Counter

	VersionBlock  prometheus.Gauge
	VersionAge    prometheus.Gauge
	ConfirmedAge  prometheus.Gauge
	HeadBlock     prometheus.Gauge
	BlocksBehind  prometheus.Gauge
	VersionStale  prometheus.Gauge
	RebuildsTotal prometheus.Counter
	ReorgsTotal   prometheus.Counter

	RequestDuration *prometheus.HistogramVec
	ResponseBytes   *prometheus.CounterVec

	// RPC series. RPCRequests is the one that answers "how many requests does a
	// cycle make"; RPCCalls against it is how much packing is buying, since a call is
	// a contract read and a request is a round trip.
	RPCRequests *prometheus.CounterVec
	RPCCalls    prometheus.Counter

	RouterChanges  prometheus.Counter
	RouterVersion  prometheus.Gauge
	VersionChanges prometheus.Counter

	// Canonical parity. Names and labels are the canonical service's, so its
	// dashboards and alerts work against this one unchanged.
	ELRPCDuration     *prometheus.HistogramVec
	CanonicalHTTP     *prometheus.HistogramVec
	BuildInfo         *prometheus.GaugeVec
	JobDuration       *prometheus.HistogramVec
	ModuleKeyUpdate   *prometheus.HistogramVec
	LastUpdate        prometheus.Gauge
	LastBlockNumber   prometheus.Gauge
	Nonce             *prometheus.GaugeVec
	KeysByModuleAndOp *prometheus.GaugeVec
}

// CanonicalPrefix is the canonical service's metric prefix: its APP_NAME with the
// dashes replaced, which is `lido-keys-api` unless npm hands it something else.
// Hard-coded rather than derived from our own namespace, because the point of these
// series is that an existing dashboard finds them under the name it already queries.
const CanonicalPrefix = "lido_keys_api"

// RPCStats is a snapshot of the execution client's own counters.
//
// A snapshot rather than events: the client counts these internally and there is no
// callback at the moment one changes, so they are read at scrape time. Declared here
// rather than taken from the eth package so that metrics stays a leaf.
type RPCStats struct {
	// Requests is HTTP round trips, Calls the contract reads they carried.
	Requests, Calls int64
	// Aggregates is requests that carried a Multicall3 aggregate, Splits how many
	// were halved after a node refused one, Fallbacks single calls that ended up on
	// the plain path. Splits climbing is the signal that the aggregate budget is set
	// above what this endpoint tolerates.
	Aggregates, Splits, Fallbacks int64
}

// New builds the registry with the Go and process collectors alongside ours.
func New(namespace string) *Metrics {
	if namespace == "" {
		namespace = "keys_api"
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := &Metrics{
		registry: reg,
		CyclesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "sync_cycles_total",
			Help: "Sync cycles by action (extend, rebuild, supersede) and outcome.",
		}, []string{"action", "outcome"}),
		CycleDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Name: "sync_cycle_duration_seconds",
			Help:    "Duration of one sync cycle.",
			Buckets: []float64{0.05, 0.1, 0.5, 1, 5, 15, 60, 300, 900},
		}),
		KeysRead: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "keys_read_total",
			Help: "Keys read from the source.",
		}),
		KeysChanged: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "key_rows_written_total",
			Help: "Key rows opened or closed. The ratio against keys_read_total is what per-module scoping buys.",
		}),
		VersionBlock: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "published_version_block",
			Help: "Block number of the published version.",
		}),
		VersionAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "published_version_age_seconds",
			Help: "Seconds since the published version's own block: how old the data is. Climbs while the chain is quiet, so not an alert on its own.",
		}),
		ConfirmedAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "confirmed_age_seconds",
			Help: "Seconds since a cycle last confirmed the version against the chain. This is the alerting signal: it climbs for a stalled node, a failing rebuild or a dead writer alike.",
		}),
		HeadBlock: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "chain_head_block",
			Help: "Block number the source reports as head.",
		}),
		BlocksBehind: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "blocks_behind_head",
			Help: "How far the published version is behind head.",
		}),
		VersionStale: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "published_version_stale",
			Help: "1 when the published version is known not to reflect the chain and a rebuild is in flight.",
		}),
		RebuildsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "rebuilds_total",
			Help: "Versions published by a rebuild.",
		}),
		ReorgsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "reorgs_total",
			Help: "Reorgs that rolled versions back.",
		}),
		RequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "http_request_duration_seconds",
			Help:    "Request duration by route and status.",
			Buckets: []float64{0.001, 0.005, 0.025, 0.1, 0.25, 1, 2.5, 10, 30},
		}, []string{"route", "status"}),
		ResponseBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "http_response_bytes_total",
			Help: "Response bytes by route. Dumps are the reason this matters.",
		}, []string{"route"}),
		RPCRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "rpc_requests_total",
			Help: "Execution-layer HTTP round trips by JSON-RPC method and outcome, including the failed ones a fallback endpoint hid. This is what a provider meters.",
		}, []string{"method", "result"}),
		RPCCalls: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "rpc_calls_total",
			Help: "Contract reads sent. Against rpc_requests_total this is the packing ratio: one request carrying 200 calls is the point of the batching.",
		}),
		RouterChanges: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "staking_router_changes_total",
			Help: "Times the locator started answering with a different staking router. Non-zero means a protocol deploy replaced the router and this service followed it; it should agree with a deploy that actually happened.",
		}),
		RouterVersion: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "staking_router_contract_version",
			Help: "The staking router's own contract version, as read every cycle. It decides how the module tuple is interpreted, so an alert on a change is an alert on the registry being read differently.",
		}),
		VersionChanges: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "staking_router_version_changes_total",
			Help: "Times the staking router reported a different contract version under this process. Counts the first read too, so 1 is the healthy steady state.",
		}),
		ELRPCDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "el_rpc_requests_duration_seconds",
			Help: "EL RPC request duration. Canonical name and buckets; its _count is the request rate.",
			// The canonical buckets, so a panel comparing the two services compares
			// like with like.
			Buckets: []float64{0.1, 0.2, 0.3, 0.6, 1, 1.5, 2, 5},
		}, []string{"result"}),
		CanonicalHTTP: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_requests_duration_seconds",
			Help:    "Duration of http requests. Canonical name and labels; the route-labelled series is this service's own.",
			Buckets: []float64{0.01, 0.1, 0.2, 0.5, 1, 1.5, 2, 5},
		}, []string{"statusCode", "method"}),
		BuildInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "build_info",
			Help: "Build information.",
		}, []string{"name", "version", "env", "network"}),
		JobDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: CanonicalPrefix, Name: "job_duration_seconds",
			Help:    "Job execution duration. The sync cycle is the job the canonical service calls 'Update Staking Router Modules keys'.",
			Buckets: []float64{0.2, 0.6, 1, 2, 3, 5, 8, 13, 30, 60, 120, 180},
		}, []string{"result", "job"}),
		ModuleKeyUpdate: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: CanonicalPrefix, Name: "sr_module_key_update_duration_seconds",
			Help:    "Time taken to update a certain number of keys for a staking module.",
			Buckets: []float64{0.2, 0.6, 1, 2, 3, 5, 8, 13, 30, 60, 120, 180},
		}, []string{"srModuleAddress", "totalKeysAmount"}),
		LastUpdate: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: CanonicalPrefix, Name: "last_update_timestamp",
			Help: "Block timestamp for which the last update was made.",
		}),
		LastBlockNumber: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: CanonicalPrefix, Name: "last_block_number",
			Help: "Block number for which the last update was made.",
		}),
		Nonce: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: CanonicalPrefix, Name: "nonce_value",
			Help: "Nonce of Staking Router module during last update.",
		}, []string{"srModuleId"}),
		KeysByModuleAndOp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: CanonicalPrefix, Name: "keys_by_sr_module_and_operator",
			Help: "Amount of keys by sr module and operator id. Refreshed for the modules a rebuild read, not on every cycle.",
		}, []string{"operator", "srModuleId", "used"}),
	}

	reg.MustRegister(m.CyclesTotal, m.CycleDuration, m.KeysRead, m.KeysChanged,
		m.VersionBlock, m.VersionAge, m.ConfirmedAge, m.HeadBlock, m.BlocksBehind, m.VersionStale,
		m.RebuildsTotal, m.ReorgsTotal, m.RequestDuration, m.ResponseBytes,
		m.RPCRequests, m.RPCCalls, m.RouterChanges, m.RouterVersion, m.VersionChanges,
		m.ELRPCDuration, m.CanonicalHTTP, m.BuildInfo, m.JobDuration, m.ModuleKeyUpdate,
		m.LastUpdate, m.LastBlockNumber, m.Nonce, m.KeysByModuleAndOp)
	return m
}

// BindRPCStats publishes the execution client's cumulative counters, read at scrape
// time.
//
// CounterFuncs rather than counters this package increments: the numbers already
// exist inside the client, and mirroring them here would mean two sources of the
// same truth that can disagree. Idempotent per registry — calling it twice panics on
// duplicate registration, which is the correct outcome for a wiring mistake.
func (m *Metrics) BindRPCStats(namespace string, snapshot func() RPCStats) {
	if namespace == "" {
		namespace = "keys_api"
	}
	for _, spec := range []struct {
		name string
		help string
		read func(RPCStats) int64
	}{
		{"rpc_aggregates_total", "Requests that carried a Multicall3 aggregate.", func(s RPCStats) int64 { return s.Aggregates }},
		{"rpc_splits_total", "Aggregates halved after an endpoint refused one. Climbing means the aggregate budget is above what this endpoint tolerates.", func(s RPCStats) int64 { return s.Splits }},
		{"rpc_fallbacks_total", "Calls that went out plainly rather than aggregated.", func(s RPCStats) int64 { return s.Fallbacks }},
	} {
		read := spec.read
		m.registry.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: namespace, Name: spec.name, Help: spec.help,
		}, func() float64 { return float64(read(snapshot())) }))
	}
}

// ObserveRPC records one execution-layer round trip, in both surfaces: ours, which
// is labelled by method because "which call is the expensive one" is the question
// that follows, and the canonical histogram, which is not.
func (m *Metrics) ObserveRPC(method, result string, calls int, d time.Duration) {
	m.RPCRequests.WithLabelValues(method, result).Inc()
	m.RPCCalls.Add(float64(calls))
	m.ELRPCDuration.WithLabelValues(result).Observe(d.Seconds())
}

// ObserveJob records one sync cycle as the canonical job histogram.
func (m *Metrics) ObserveJob(job, result string, d time.Duration) {
	m.JobDuration.WithLabelValues(result, job).Observe(d.Seconds())
}

// ObserveModuleUpdate records one module's key read, and refreshes the per-operator
// key gauges from the operators that read returned.
//
// The label on the histogram carries the key count, which is the canonical service's
// choice and not a good one — every distinct count is a new series. Kept anyway,
// because this exists so its panels work; a deployment that cares can drop the label
// in the scrape config.
func (m *Metrics) ObserveModuleUpdate(address string, keys int64, d time.Duration) {
	m.ModuleKeyUpdate.WithLabelValues(address, strconv.FormatInt(keys, 10)).Observe(d.Seconds())
}

// SetOperatorKeys publishes one operator's used and unused key counts.
func (m *Metrics) SetOperatorKeys(moduleID, operator int32, used, unused int32) {
	id := strconv.FormatInt(int64(moduleID), 10)
	index := strconv.FormatInt(int64(operator), 10)
	m.KeysByModuleAndOp.WithLabelValues(index, id, "true").Set(float64(used))
	m.KeysByModuleAndOp.WithLabelValues(index, id, "false").Set(float64(unused))
}

// SetNonce publishes a module's nonce.
func (m *Metrics) SetNonce(moduleID int32, nonce int32) {
	m.Nonce.WithLabelValues(strconv.FormatInt(int64(moduleID), 10)).Set(float64(nonce))
}

// SetBuildInfo publishes the canonical build_info gauge.
func (m *Metrics) SetBuildInfo(name, version, env, network string) {
	m.BuildInfo.WithLabelValues(name, version, env, network).Set(1)
}

// Handler serves the exposition format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// ObserveRequest records one served request, in both the route-labelled surface and
// the canonical statusCode/method one.
func (m *Metrics) ObserveRequest(method, route string, status int, bytes int64, d time.Duration) {
	m.RequestDuration.WithLabelValues(route, strconv.Itoa(status)).Observe(d.Seconds())
	m.ResponseBytes.WithLabelValues(route).Add(float64(bytes))
	m.CanonicalHTTP.WithLabelValues(strconv.Itoa(status), method).Observe(d.Seconds())
}
