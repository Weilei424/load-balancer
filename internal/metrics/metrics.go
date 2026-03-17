// Package metrics exposes Prometheus-style text metrics with no external library.
package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

// histBucketBounds are the upper bounds (in seconds) for the 9 finite latency buckets.
// A 10th +Inf slot is always appended when recording.
var histBucketBounds = []float64{0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0}

// histBucketLabels are the Prometheus `le` label strings corresponding to histBucketBounds.
var histBucketLabels = []string{"0.001", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1"}

// Histogram is a fixed-bucket latency histogram backed by one atomic.Int64 per bucket.
// counts[i] is cumulative: it holds the count of observations whose value is ≤ histBucketBounds[i].
// counts[9] is the +Inf bucket (equals total count).
type Histogram struct {
	counts   [10]atomic.Int64 // indices 0–8 = finite bounds; index 9 = +Inf
	sumNanos atomic.Int64     // sum of all observations in nanoseconds
	count    atomic.Int64     // total observations (mirrors counts[9])
}

// Observe records one observation of duration d.
func (h *Histogram) Observe(d time.Duration) {
	h.sumNanos.Add(d.Nanoseconds())
	h.count.Add(1)
	sec := d.Seconds()
	for i, bound := range histBucketBounds {
		if sec <= bound {
			h.counts[i].Add(1)
		}
	}
	h.counts[9].Add(1) // +Inf always incremented
}

// HistogramData is an exported snapshot of a Histogram for rendering and testing.
type HistogramData struct {
	BucketCounts [10]int64 // cumulative; BucketCounts[9] == Count == +Inf
	SumSeconds   float64
	Count        int64
}

// Metrics holds all observable counters for the LB node.
type Metrics struct {
	RequestsTotal int64 // total proxied requests
	ErrorsTotal   int64 // proxy errors (no backend, dial fail)

	RaftTerm int64 // current Raft term
	RaftRole int32 // 0=follower,1=candidate,2=leader

	mu              sync.RWMutex
	backendRequests map[string]*int64     // per-backend request counters
	backendErrors   map[string]*int64     // per-backend error counters
	backendLatency  map[string]*Histogram // per-backend latency histogram; protected by mu

	// rate window: updated every second by rateLoop.
	rw rateWindow

	// backendConns holds pointers to Backend.ConnCount atomics, keyed by URL.
	// Updated by SetBackends from the periodic broadcast goroutine.
	bcMu         sync.RWMutex
	backendConns map[string]*int64
}

// rateWindow stores the previous total and the computed req/s rate.
type rateWindow struct {
	mu        sync.Mutex
	prevTotal int64
	rate      float64
}

// New creates a zero-valued Metrics instance and starts the rate-tracking goroutine.
func New() *Metrics {
	m := &Metrics{
		backendRequests: make(map[string]*int64),
		backendErrors:   make(map[string]*int64),
		backendLatency:  make(map[string]*Histogram),
		backendConns:    make(map[string]*int64),
	}
	go m.rateLoop()
	return m
}

// rateLoop samples RequestsTotal every second and computes the req/s rate.
func (m *Metrics) rateLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		current := atomic.LoadInt64(&m.RequestsTotal)
		m.rw.mu.Lock()
		m.rw.rate = float64(current - m.rw.prevTotal)
		m.rw.prevTotal = current
		m.rw.mu.Unlock()
	}
}

// RequestRate returns the current requests-per-second rate (computed over the last second).
func (m *Metrics) RequestRate() float64 {
	m.rw.mu.Lock()
	defer m.rw.mu.Unlock()
	return m.rw.rate
}

// SetBackends updates the set of backend ConnCount pointers used for /metrics output.
// conns maps backend URL → pointer to the Backend's atomic ConnCount field.
// Called from the periodic broadcast goroutine in node.go.
func (m *Metrics) SetBackends(conns map[string]*int64) {
	m.bcMu.Lock()
	m.backendConns = conns
	m.bcMu.Unlock()
}

// IncRequest increments the global and per-backend request counter.
func (m *Metrics) IncRequest(backendURL string) {
	atomic.AddInt64(&m.RequestsTotal, 1)
	m.mu.RLock()
	c := m.backendRequests[backendURL]
	m.mu.RUnlock()
	if c == nil {
		m.mu.Lock()
		c = m.backendRequests[backendURL]
		if c == nil {
			var v int64
			c = &v
			m.backendRequests[backendURL] = c
		}
		m.mu.Unlock()
	}
	atomic.AddInt64(c, 1)
}

// IncError increments the global and per-backend error counter.
func (m *Metrics) IncError(backendURL string) {
	atomic.AddInt64(&m.ErrorsTotal, 1)
	m.mu.RLock()
	c := m.backendErrors[backendURL]
	m.mu.RUnlock()
	if c == nil {
		m.mu.Lock()
		c = m.backendErrors[backendURL]
		if c == nil {
			var v int64
			c = &v
			m.backendErrors[backendURL] = c
		}
		m.mu.Unlock()
	}
	atomic.AddInt64(c, 1)
}

// ObserveLatency records a latency observation for the given backend URL.
func (m *Metrics) ObserveLatency(backendURL string, d time.Duration) {
	m.mu.RLock()
	h := m.backendLatency[backendURL]
	m.mu.RUnlock()
	if h == nil {
		m.mu.Lock()
		h = m.backendLatency[backendURL]
		if h == nil {
			h = &Histogram{}
			m.backendLatency[backendURL] = h
		}
		m.mu.Unlock()
	}
	h.Observe(d)
}

// SnapshotHistograms returns a stable copy of all per-backend latency histograms.
func (m *Metrics) SnapshotHistograms() map[string]*HistogramData {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]*HistogramData, len(m.backendLatency))
	for url, h := range m.backendLatency {
		d := &HistogramData{
			SumSeconds: float64(h.sumNanos.Load()) / 1e9,
			Count:      h.count.Load(),
		}
		for i := range d.BucketCounts {
			d.BucketCounts[i] = h.counts[i].Load()
		}
		out[url] = d
	}
	return out
}

// SetRaft updates the raft term and role gauges.
func (m *Metrics) SetRaft(term int, role int32) {
	atomic.StoreInt64(&m.RaftTerm, int64(term))
	atomic.StoreInt32(&m.RaftRole, role)
}

// Snapshot returns a stable copy for rendering.
func (m *Metrics) Snapshot() map[string]int64 {
	result := map[string]int64{
		"lb_requests_total":      atomic.LoadInt64(&m.RequestsTotal),
		"lb_errors_total":        atomic.LoadInt64(&m.ErrorsTotal),
		"raft_term":              atomic.LoadInt64(&m.RaftTerm),
		"raft_role":              int64(atomic.LoadInt32(&m.RaftRole)),
		"lb_requests_per_second": int64(m.RequestRate()),
	}
	m.mu.RLock()
	for url, c := range m.backendRequests {
		result["lb_backend_requests{url=\""+url+"\"}"] = atomic.LoadInt64(c)
	}
	for url, c := range m.backendErrors {
		result["lb_backend_errors{url=\""+url+"\"}"] = atomic.LoadInt64(c)
	}
	m.mu.RUnlock()

	m.bcMu.RLock()
	for url, ptr := range m.backendConns {
		result["lb_backend_conn_count{url=\""+url+"\"}"] = atomic.LoadInt64(ptr)
	}
	m.bcMu.RUnlock()

	return result
}
