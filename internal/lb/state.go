package lb

import (
	"encoding/json"
	"fmt"
	"sync"
)

// Algorithm names for routing selection.
const (
	AlgoRoundRobin = "round_robin"
	AlgoLeastConn  = "least_conn"
)

// Backend represents a single upstream backend server.
type Backend struct {
	URL       string
	Weight    int
	Healthy   bool
	ConnCount int64 // atomic; in-flight connections (for least_conn)

	// swrr is the smooth weighted round-robin current weight.
	// Modified only while ConfigState.swrrMu is held.
	swrr int
}

// ConfigState holds the applied configuration for this LB node.
// It is mutated only by Apply() (called from the Raft applier goroutine)
// and read by the proxy and dashboard.
type ConfigState struct {
	mu        sync.RWMutex
	backends  []*Backend
	algorithm string

	// swrrMu serialises the smooth weighted round-robin per-backend current-weight
	// updates across concurrent Pick calls.
	swrrMu sync.Mutex
}

// NewConfigState creates an empty ConfigState with round-robin as default.
func NewConfigState() *ConfigState {
	return &ConfigState{algorithm: AlgoRoundRobin}
}

// Apply updates the state machine according to cmd.
// Called sequentially by the Raft applier — no concurrent Apply calls.
func (cs *ConfigState) Apply(cmd Command) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	switch cmd.Op {
	case OpAddBackend:
		if cs.findBackend(cmd.URL) == nil {
			w := cmd.Weight
			if w <= 0 {
				w = 1
			}
			cs.backends = append(cs.backends, &Backend{
				URL:     cmd.URL,
				Weight:  w,
				Healthy: true,
			})
		}
	case OpRemoveBackend:
		cs.backends = removeBackend(cs.backends, cmd.URL)
	case OpSetWeight:
		if b := cs.findBackend(cmd.URL); b != nil {
			w := cmd.Weight
			if w <= 0 {
				w = 1
			}
			b.Weight = w
		}
	case OpSetAlgorithm:
		if cmd.Algorithm == AlgoRoundRobin || cmd.Algorithm == AlgoLeastConn {
			cs.algorithm = cmd.Algorithm
		}
	}
}

// Snapshot returns a read-only copy of all backends and the algorithm.
func (cs *ConfigState) Snapshot() ([]*Backend, string) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	cp := make([]*Backend, len(cs.backends))
	copy(cp, cs.backends)
	return cp, cs.algorithm
}

// HealthyBackends returns only healthy backends (caller must not mutate).
func (cs *ConfigState) HealthyBackends() []*Backend {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	out := make([]*Backend, 0, len(cs.backends))
	for _, b := range cs.backends {
		if b.Healthy {
			out = append(out, b)
		}
	}
	return out
}

// AllBackends returns all backends including unhealthy ones.
func (cs *ConfigState) AllBackends() []*Backend {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	cp := make([]*Backend, len(cs.backends))
	copy(cp, cs.backends)
	return cp
}

// Algorithm returns the current routing algorithm name.
func (cs *ConfigState) Algorithm() string {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.algorithm
}

// SetHealthy marks a backend healthy or unhealthy by URL.
// Returns true if the health state changed.
func (cs *ConfigState) SetHealthy(url string, healthy bool) bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if b := cs.findBackend(url); b != nil {
		if b.Healthy != healthy {
			b.Healthy = healthy
			return true
		}
	}
	return false
}

// findBackend returns the backend with the given URL, or nil. Caller holds mu.
func (cs *ConfigState) findBackend(url string) *Backend {
	for _, b := range cs.backends {
		if b.URL == url {
			return b
		}
	}
	return nil
}

func removeBackend(backends []*Backend, url string) []*Backend {
	out := backends[:0]
	for _, b := range backends {
		if b.URL != url {
			out = append(out, b)
		}
	}
	return out
}

type configSnapshotData struct {
	Backends  []backendSnapshotEntry `json:"backends"`
	Algorithm string                 `json:"algorithm"`
}

type backendSnapshotEntry struct {
	URL     string `json:"url"`
	Weight  int    `json:"weight"`
	Healthy bool   `json:"healthy"`
}

// SnapshotData serialises the current config state into a JSON byte slice.
func (cs *ConfigState) SnapshotData() []byte {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	snap := configSnapshotData{Algorithm: cs.algorithm}
	for _, b := range cs.backends {
		snap.Backends = append(snap.Backends, backendSnapshotEntry{
			URL:     b.URL,
			Weight:  b.Weight,
			Healthy: b.Healthy,
		})
	}
	data, _ := json.Marshal(snap)
	return data
}

// RestoreSnapshot replaces the current config state with the contents of data.
func (cs *ConfigState) RestoreSnapshot(data []byte) error {
	var snap configSnapshotData
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("restore snapshot: %w", err)
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.backends = cs.backends[:0]
	for _, b := range snap.Backends {
		cs.backends = append(cs.backends, &Backend{
			URL:     b.URL,
			Weight:  b.Weight,
			Healthy: b.Healthy,
		})
	}
	if snap.Algorithm != "" {
		cs.algorithm = snap.Algorithm
	}
	return nil
}
