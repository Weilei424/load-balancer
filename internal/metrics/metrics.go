// Package metrics exposes Prometheus-style text metrics with no external library.
package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

// Metrics holds all observable counters for the LB node.
type Metrics struct {
	RequestsTotal int64 // total proxied requests
	ErrorsTotal   int64 // proxy errors (no backend, dial fail)

	RaftTerm int64 // current Raft term
	RaftRole int32 // 0=follower,1=candidate,2=leader

	mu              sync.RWMutex
	backendRequests map[string]*int64 // per-backend request counters
	backendErrors   map[string]*int64 // per-backend error counters

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
