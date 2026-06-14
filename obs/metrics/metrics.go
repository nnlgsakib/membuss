// Package metrics is the Phase 10 observability facade for Membuss.
//
// It defines a single Metrics value that owns the Prometheus
// collectors, exposes typed methods for instrumenting the hot
// paths (DHT provides, Memex transfers, GC runs, store size,
// peer counts), and serves a /metrics-compatible http.Handler.
//
// The package depends on github.com/prometheus/client_golang,
// which is already an indirect dep of libp2p's IPFS plumbing.
// Using its own registry (not the global default) keeps
// collector registration deterministic across tests.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics is the live instrumentation handle held by the daemon
// and passed to subsystems that want to record events. A nil
// Metrics is a valid no-op; subsystems can call into it
// unconditionally. The noop field flips the value into test
// mode where the underlying collectors are uninitialized but
// every method remains safe to call.
type Metrics struct {
	noop     bool
	registry *prometheus.Registry

	storedMIDs           prometheus.Gauge
	storedBytes          prometheus.Gauge
	peersConnected       prometheus.Gauge
	dhtProvides          prometheus.Counter
	memexBlocksSent      prometheus.Counter
	memexBlocksReceived  prometheus.Counter
	gcRuns               prometheus.Counter
	memexTransferDuration prometheus.Histogram
	addRequestDuration   prometheus.Histogram
}

// noopGuard returns true when the Metrics value is either nil
// (caller has not configured metrics) or in noop mode. All record
// methods consult this guard before touching the underlying
// Prometheus collectors.
func (m *Metrics) noopGuard() bool { return m == nil || m.noop }

// New constructs a fresh Metrics. The returned value is safe for
// concurrent use; calls into it before any Set* call are no-ops.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{registry: reg}
	m.storedMIDs = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "membuss_stored_mids_total",
		Help: "Number of MIDs currently held in the local block store.",
	})
	m.storedBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "membuss_stored_bytes_total",
		Help: "Total bytes held in the local block store.",
	})
	m.peersConnected = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "membuss_peers_connected",
		Help: "Number of libp2p peers currently connected.",
	})
	m.dhtProvides = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "membuss_dht_provides_total",
		Help: "Cumulative count of DHT provider announcements.",
	})
	m.memexBlocksSent = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "membuss_memex_blocks_sent_total",
		Help: "Cumulative count of blocks pushed to remote peers via Memex.",
	})
	m.memexBlocksReceived = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "membuss_memex_blocks_received_total",
		Help: "Cumulative count of blocks received from remote peers via Memex.",
	})
	m.gcRuns = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "membuss_gc_runs_total",
		Help: "Cumulative count of garbage-collection runs.",
	})
	m.memexTransferDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "membuss_memex_transfer_duration_seconds",
		Help:    "Memex transfer duration in seconds.",
		Buckets: prometheus.DefBuckets,
	})
	m.addRequestDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "membuss_add_request_duration_seconds",
		Help:    "Time spent ingesting a new piece of content (chunking + DAG + store).",
		Buckets: prometheus.DefBuckets,
	})
	reg.MustRegister(
		m.storedMIDs, m.storedBytes, m.peersConnected,
		m.dhtProvides,
		m.memexBlocksSent, m.memexBlocksReceived,
		m.gcRuns,
		m.memexTransferDuration, m.addRequestDuration,
	)
	// Standard process collectors give operators free Go runtime
	// and process metrics.
	reg.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	reg.MustRegister(prometheus.NewGoCollector())
	return m
}

// Noop returns a Metrics value that is safe to call but does not
// register or expose any collectors. Useful for tests.
func Noop() *Metrics { return &Metrics{noop: true} }

// SetStoredMIDs updates the gauge of locally stored MIDs.
func (m *Metrics) SetStoredMIDs(n int64) {
	if m.noopGuard() { return }
	m.storedMIDs.Set(float64(n))
}

// SetStoredBytes updates the gauge of locally stored bytes.
func (m *Metrics) SetStoredBytes(n uint64) {
	if m.noopGuard() { return }
	m.storedBytes.Set(float64(n))
}

// SetPeersConnected updates the gauge of currently connected peers.
func (m *Metrics) SetPeersConnected(n int) {
	if m.noopGuard() { return }
	m.peersConnected.Set(float64(n))
}

// IncDHTProvide records one DHT provider announcement.
func (m *Metrics) IncDHTProvide() {
	if m.noopGuard() { return }
	m.dhtProvides.Inc()
}

// IncMemexBlocksSent records n blocks pushed outbound via Memex.
func (m *Metrics) IncMemexBlocksSent(n int) {
	if m.noopGuard() || n <= 0 { return }
	m.memexBlocksSent.Add(float64(n))
}

// IncMemexBlocksReceived records n blocks received inbound via Memex.
func (m *Metrics) IncMemexBlocksReceived(n int) {
	if m.noopGuard() || n <= 0 { return }
	m.memexBlocksReceived.Add(float64(n))
}

// IncGCRuns records one garbage-collection run.
func (m *Metrics) IncGCRuns() {
	if m.noopGuard() { return }
	m.gcRuns.Inc()
}

// ObserveMemexTransfer records a single Memex transfer duration.
func (m *Metrics) ObserveMemexTransfer(seconds float64) {
	if m.noopGuard() { return }
	m.memexTransferDuration.Observe(seconds)
}

// ObserveAddRequest records a single Add (ingest) request duration.
func (m *Metrics) ObserveAddRequest(seconds float64) {
	if m.noopGuard() { return }
	m.addRequestDuration.Observe(seconds)
}

// Handler returns an http.Handler that serves the registry in the
// Prometheus text format. A nil receiver returns a handler that
// responds with 503 Service Unavailable.
func (m *Metrics) Handler() http.Handler {
	if m.noopGuard() {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "metrics disabled", http.StatusServiceUnavailable)
		})
	}
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// Registry returns the underlying registry, for tests that want to
// gather values directly. nil for noop metrics.
func (m *Metrics) Registry() *prometheus.Registry {
	if m == nil { return nil }
	return m.registry
}
