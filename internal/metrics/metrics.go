// Package metrics exposes Prometheus-style text metrics with no external library.
package metrics

import (
	"sync"
	"sync/atomic"
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
}

// New creates a zero-valued Metrics instance.
func New() *Metrics {
	return &Metrics{
		backendRequests: make(map[string]*int64),
		backendErrors:   make(map[string]*int64),
	}
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
		"lb_requests_total": atomic.LoadInt64(&m.RequestsTotal),
		"lb_errors_total":   atomic.LoadInt64(&m.ErrorsTotal),
		"raft_term":         atomic.LoadInt64(&m.RaftTerm),
		"raft_role":         int64(atomic.LoadInt32(&m.RaftRole)),
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for url, c := range m.backendRequests {
		result["lb_backend_requests{url=\""+url+"\"}"] = atomic.LoadInt64(c)
	}
	for url, c := range m.backendErrors {
		result["lb_backend_errors{url=\""+url+"\"}"] = atomic.LoadInt64(c)
	}
	return result
}
