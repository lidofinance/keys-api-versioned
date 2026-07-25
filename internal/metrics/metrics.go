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
	}

	reg.MustRegister(m.CyclesTotal, m.CycleDuration, m.KeysRead, m.KeysChanged,
		m.VersionBlock, m.VersionAge, m.ConfirmedAge, m.HeadBlock, m.BlocksBehind, m.VersionStale,
		m.RebuildsTotal, m.ReorgsTotal, m.RequestDuration, m.ResponseBytes)
	return m
}

// Handler serves the exposition format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// ObserveRequest records one served request.
func (m *Metrics) ObserveRequest(route string, status int, bytes int64, d time.Duration) {
	m.RequestDuration.WithLabelValues(route, strconv.Itoa(status)).Observe(d.Seconds())
	m.ResponseBytes.WithLabelValues(route).Add(float64(bytes))
}
