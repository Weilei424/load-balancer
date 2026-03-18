package lb

import "sync/atomic"

// Pick selects the next backend according to the configured algorithm.
// Returns nil if there are no healthy backends.
//
// Backends whose circuit breaker is blocked (Open, timeout not yet elapsed, or
// HalfOpen with a probe already in flight) are excluded from the candidate
// pool. If every healthy backend is currently blocked, the full healthy slice
// is used as a fallback so that probe requests can still fire via Allow().
func (cs *ConfigState) Pick() *Backend {
	healthy := cs.HealthyBackends()
	if len(healthy) == 0 {
		return nil
	}

	// Build candidate pool excluding circuit-blocked backends.
	available := make([]*Backend, 0, len(healthy))
	for _, b := range healthy {
		if b.CB == nil || !b.CB.ShouldSkip() {
			available = append(available, b)
		}
	}
	if len(available) == 0 {
		// All healthy backends are circuit-open; fall back to the full pool so
		// probe requests can eventually transition circuits to HalfOpen.
		available = healthy
	}

	algo := cs.Algorithm()
	switch algo {
	case AlgoLeastConn:
		return pickLeastConn(available)
	default:
		return pickWeightedRoundRobin(cs, available)
	}
}

// pickWeightedRoundRobin implements Nginx's smooth weighted round-robin (SWRR).
//
// Each call:
//  1. Adds each backend's Weight to its swrr current weight.
//  2. Picks the backend with the highest current weight.
//  3. Subtracts the total weight sum from the winner's current weight.
//
// This distributes requests in exact proportion to weights while keeping the
// sequence interleaved (no bursts). For equal weights it degenerates to a
// simple round-robin.
//
// cs.swrrMu serialises the per-backend swrr mutations across concurrent Picks.
func pickWeightedRoundRobin(cs *ConfigState, healthy []*Backend) *Backend {
	cs.swrrMu.Lock()
	defer cs.swrrMu.Unlock()

	total := 0
	for _, b := range healthy {
		w := b.Weight
		if w <= 0 {
			w = 1
		}
		b.swrr += w
		total += w
	}

	best := healthy[0]
	for _, b := range healthy[1:] {
		if b.swrr > best.swrr {
			best = b
		}
	}
	best.swrr -= total
	return best
}

// pickLeastConn selects the backend with the lowest weighted connection score:
//
//	score = active_connections / weight
//
// A backend with weight=2 can handle twice as many concurrent connections as a
// weight=1 backend before being considered "busier". Equal scores break in
// favour of the first backend in the list.
func pickLeastConn(healthy []*Backend) *Backend {
	best := healthy[0]
	bw := best.Weight
	if bw <= 0 {
		bw = 1
	}
	bestScore := float64(atomic.LoadInt64(&best.ConnCount)) / float64(bw)

	for _, b := range healthy[1:] {
		w := b.Weight
		if w <= 0 {
			w = 1
		}
		score := float64(atomic.LoadInt64(&b.ConnCount)) / float64(w)
		if score < bestScore {
			bestScore = score
			best = b
		}
	}
	return best
}
