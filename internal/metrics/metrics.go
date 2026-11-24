// Package metrics also exports the canonical service's series under its names, so existing
// dashboards work. validators_registry_* is absent: this service has no CL-side registry.
package metrics

import (
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	registry *prometheus.Registry

	CyclesTotal   *prometheus.CounterVec
	CyclePanics   prometheus.Counter
	CycleDuration prometheus.Histogram
	KeysRead      prometheus.Counter
	KeysChanged   prometheus.Counter

	VersionBlock prometheus.Gauge
	VersionAge   prometheus.Gauge
	ConfirmedAge prometheus.GaugeFunc
	// confirmedAt is the unix time of the block the last good cycle confirmed; the age is taken at
	// scrape, so it climbs while cycles fail or stop.
	confirmedAt   atomic.Int64
	now           func() time.Time
	HeadBlock     prometheus.Gauge
	BlocksBehind  prometheus.Gauge
	VersionStale  prometheus.Gauge
	RebuildsTotal prometheus.Counter
	ReorgsTotal   prometheus.Counter

	AuditsTotal          prometheus.Counter
	AuditDivergenceTotal prometheus.Counter

	RequestDuration *prometheus.HistogramVec
	ResponseBytes   *prometheus.CounterVec
	InFlight        prometheus.Gauge

	RetentionHeldAt  prometheus.Gauge
	UnfinishedBuilds prometheus.Gauge

	RPCRequests *prometheus.CounterVec

	SecretsReloads *prometheus.CounterVec
	RPCCalls       prometheus.Counter

	EndpointRequests *prometheus.CounterVec

	RouterChanges  prometheus.Counter
	RouterVersion  prometheus.Gauge
	VersionChanges prometheus.Counter

	// Canonical parity: the canonical service's names and labels.
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

// CanonicalPrefix is hard-coded: existing dashboards query this name, whatever our namespace.
const CanonicalPrefix = "lido_keys_api"

// RPCStats is read at scrape time; declared here so metrics stays a leaf package.
type RPCStats struct {
	Requests, Calls               int64
	Aggregates, Splits, Fallbacks int64
	Unclassified                  int64
}

func New(namespace string) *Metrics {
	if namespace == "" {
		namespace = "keys_api"
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := &Metrics{
		registry: reg,
		now:      time.Now,
		CyclesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "sync_cycles_total",
			Help: "Sync cycles by action (extend, rebuild, supersede) and outcome.",
		}, []string{"action", "outcome"}),
		CyclePanics: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "sync_cycle_panics_total",
			Help: "Sync cycles that panicked. The cycle counts as failed and the API keeps serving; any increase is a bug or a malformed source answer.",
		}),
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
		AuditsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "audits_total",
			Help: "Rebuilds triggered by the block-gap rule rather than by a signal. The audit re-reads everything on purpose, so this climbing steadily is healthy.",
		}),
		AuditDivergenceTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "audit_divergence_total",
			Help: "Key rows an audit had to change. It should stay at zero: the audit trusts none of the cheap signals, so anything it finds is something a module nonce failed to report — a key added or removed without the counter moving. Alert on any increase; it invalidates the assumption the whole freshness mechanism rests on.",
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
		InFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "http_requests_in_flight",
			Help: "Requests being served, dumps included.",
		}),
		RetentionHeldAt: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "retention_held_at_block",
			Help: "The block the last prune stopped at below its window, 0 when it did not. A reader's lease or the finalized version holds it; a value that never moves is a leaked lease, and storage grows.",
		}),
		UnfinishedBuilds: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "unfinished_builds",
			Help: "Versions left in building between cycles: a writer that died mid-build. They are never served and retention drops them.",
		}),
		SecretsReloads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "secrets_reloads_total",
			Help: "Secrets picked up from the file, by outcome (success, failure, restart_required). A rising restart_required means the file holds a value this process is not using.",
		}, []string{"status"}),
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
		EndpointRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "rpc_endpoint_requests_total",
			Help: "Execution-layer round trips by endpoint host and outcome. Success on a fallback host means the primary is refusing — a provider out of quota looks healthy from every other metric.",
		}, []string{"endpoint", "result"}),
		RouterVersion: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "staking_router_contract_version",
			Help: "The staking router's own contract version, as read every cycle. It decides how the module tuple is interpreted, so an alert on a change is an alert on the registry being read differently.",
		}),
		VersionChanges: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "staking_router_version_changes_total",
			Help: "Times the staking router reported a different contract version under this process. Counts the first read too, so 1 is the healthy steady state.",
		}),
		ELRPCDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "el_rpc_requests_duration_seconds",
			Help:    "EL RPC request duration. Canonical name and buckets; its _count is the request rate.",
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

	m.ConfirmedAge = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: namespace, Name: "confirmed_age_seconds",
		Help: "Seconds since a cycle last confirmed the version against the chain. This is the alerting signal: it climbs for a stalled node, a failing rebuild or a dead writer alike. 0 until this process writes.",
	}, func() float64 {
		at := m.confirmedAt.Load()
		if at == 0 {
			return 0
		}
		return float64(m.now().Unix() - at)
	})

	reg.MustRegister(m.CyclesTotal, m.CyclePanics, m.CycleDuration, m.KeysRead, m.KeysChanged,
		m.VersionBlock, m.VersionAge, m.ConfirmedAge, m.HeadBlock, m.BlocksBehind, m.VersionStale,
		m.RebuildsTotal, m.ReorgsTotal, m.AuditsTotal, m.AuditDivergenceTotal,
		m.RequestDuration, m.ResponseBytes, m.InFlight, m.RetentionHeldAt, m.UnfinishedBuilds,
		m.SecretsReloads, m.RPCRequests, m.RPCCalls, m.EndpointRequests, m.RouterChanges, m.RouterVersion, m.VersionChanges,
		m.ELRPCDuration, m.CanonicalHTTP, m.BuildInfo, m.JobDuration, m.ModuleKeyUpdate,
		m.LastUpdate, m.LastBlockNumber, m.Nonce, m.KeysByModuleAndOp)
	return m
}

// BindRPCStats reads the client's own counters at scrape time instead of mirroring them.
// Calling it twice panics on duplicate registration.
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
		{"rpc_unclassified_errors_total", "JSON-RPC errors no rule recognised, treated as the call's own and not failed over. The log line names the message, so a new provider wording can be added to the rules.", func(s RPCStats) int64 { return s.Unclassified }},
	} {
		read := spec.read
		m.registry.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: namespace, Name: spec.name, Help: spec.help,
		}, func() float64 { return float64(read(snapshot())) }))
	}
}

// PoolStats is pgxpool.Stat, copied so metrics stays a leaf package.
type PoolStats struct {
	Acquired, Idle, Total, Max int32
	Acquires, EmptyAcquires    int64
	CanceledAcquires           int64
	AcquireWait                time.Duration
}

// BindPoolStats: a request holds a connection for its whole response, so saturation (acquired at
// max, empty acquires climbing) is what a stalled consumer or a slow database looks like.
func (m *Metrics) BindPoolStats(namespace string, snapshot func() PoolStats) {
	gauges := []struct {
		name, help string
		read       func(PoolStats) float64
	}{
		{"db_pool_acquired_connections", "Connections handed out: a dump holds one until its last byte.", func(s PoolStats) float64 { return float64(s.Acquired) }},
		{"db_pool_idle_connections", "Connections open and free.", func(s PoolStats) float64 { return float64(s.Idle) }},
		{"db_pool_connections", "Connections open.", func(s PoolStats) float64 { return float64(s.Total) }},
		{"db_pool_max_connections", "The pool's ceiling (DATABASE_MAX_CONNS).", func(s PoolStats) float64 { return float64(s.Max) }},
	}
	for _, g := range gauges {
		read := g.read
		m.registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: namespace, Name: g.name, Help: g.help,
		}, func() float64 { return read(snapshot()) }))
	}
	counters := []struct {
		name, help string
		read       func(PoolStats) float64
	}{
		{"db_pool_acquires_total", "Connections acquired.", func(s PoolStats) float64 { return float64(s.Acquires) }},
		{"db_pool_empty_acquires_total", "Acquires that had to wait for a connection.", func(s PoolStats) float64 { return float64(s.EmptyAcquires) }},
		{"db_pool_canceled_acquires_total", "Acquires given up before a connection came free, the acquire timeout included.", func(s PoolStats) float64 { return float64(s.CanceledAcquires) }},
		{"db_pool_acquire_wait_seconds_total", "Time spent acquiring connections.", func(s PoolStats) float64 { return s.AcquireWait.Seconds() }},
	}
	for _, c := range counters {
		read := c.read
		m.registry.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: namespace, Name: c.name, Help: c.help,
		}, func() float64 { return read(snapshot()) }))
	}
}

// BindAggregation exports the client's aggregate state. A budget stuck low or aggregation off on a
// metered provider multiplies billed calls, and nothing else shows it.
func (m *Metrics) BindAggregation(namespace string, read func() (budget int64, enabled bool)) {
	m.registry.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: namespace, Name: "rpc_aggregate_budget",
			Help: "Keys one Multicall3 aggregate may return. It drops when an endpoint refuses a bigger one and comes back after a pause or a rotation.",
		}, func() float64 { budget, _ := read(); return float64(budget) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: namespace, Name: "rpc_aggregation_enabled",
			Help: "1 while reads go out as Multicall3 aggregates; 0 before the presence probe, without Multicall3, or while paused after repeated refusals.",
		}, func() float64 {
			if _, enabled := read(); enabled {
				return 1
			}
			return 0
		}))
}

// CountInFlight wraps the HTTP handler: dumps are long, so the request rate alone hides how many
// are open at once.
func (m *Metrics) CountInFlight(next http.Handler) http.Handler {
	return promhttp.InstrumentHandlerInFlight(m.InFlight, next)
}

// ObserveRPC: our series is labelled by method, the canonical histogram is not.
func (m *Metrics) ObserveRPC(method, result, endpoint string, calls int, d time.Duration) {
	m.RPCRequests.WithLabelValues(method, result).Inc()
	m.RPCCalls.Add(float64(calls))
	m.ELRPCDuration.WithLabelValues(result).Observe(d.Seconds())
	if endpoint != "" {
		m.EndpointRequests.WithLabelValues(endpoint, result).Inc()
	}
}

func (m *Metrics) ObserveJob(job, result string, d time.Duration) {
	m.JobDuration.WithLabelValues(result, job).Observe(d.Seconds())
}

// The key-count label is the canonical choice and makes a series per distinct count; kept for
// panel parity, droppable in the scrape config.
func (m *Metrics) ObserveModuleUpdate(address string, keys int64, d time.Duration) {
	m.ModuleKeyUpdate.WithLabelValues(address, strconv.FormatInt(keys, 10)).Observe(d.Seconds())
}

func (m *Metrics) SetOperatorKeys(moduleID, operator int32, used, unused int32) {
	id := strconv.FormatInt(int64(moduleID), 10)
	index := strconv.FormatInt(int64(operator), 10)
	m.KeysByModuleAndOp.WithLabelValues(index, id, "true").Set(float64(used))
	m.KeysByModuleAndOp.WithLabelValues(index, id, "false").Set(float64(unused))
}

func (m *Metrics) SetNonce(moduleID int32, nonce int32) {
	m.Nonce.WithLabelValues(strconv.FormatInt(int64(moduleID), 10)).Set(float64(nonce))
}

func (m *Metrics) SetBuildInfo(name, version, env, network string) {
	m.BuildInfo.WithLabelValues(name, version, env, network).Set(1)
}

// SetBuildCommit is separate from build_info, whose label set is the canonical service's.
func (m *Metrics) SetBuildCommit(namespace, version, commit string) {
	if namespace == "" {
		namespace = "keys_api"
	}
	info := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace, Name: "build_info",
		Help: "The build this process runs: semantic version and the commit it was built from.",
	}, []string{"version", "commit"})
	m.registry.MustRegister(info)
	info.WithLabelValues(version, commit).Set(1)
}

// Confirmed records the timestamp of the block a cycle confirmed the version at.
func (m *Metrics) Confirmed(blockTimestamp int64) { m.confirmedAt.Store(blockTimestamp) }

// StartConfirming starts the age when a writer starts, so one that never confirms still climbs.
func (m *Metrics) StartConfirming() { m.confirmedAt.CompareAndSwap(0, m.now().Unix()) }

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) ObserveRequest(method, route string, status int, bytes int64, d time.Duration) {
	m.RequestDuration.WithLabelValues(route, strconv.Itoa(status)).Observe(d.Seconds())
	m.ResponseBytes.WithLabelValues(route).Add(float64(bytes))
	m.CanonicalHTTP.WithLabelValues(strconv.Itoa(status), methodLabel(method)).Observe(d.Seconds())
}

// methodLabel bounds the label: the method is client input, and each invented one was a new series.
func methodLabel(method string) string {
	switch method {
	case "GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS":
		return method
	}
	return "other"
}
