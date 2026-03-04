package lb

import "sync/atomic"

// Pick selects the next backend according to the configured algorithm.
// Returns nil if there are no healthy backends.
func (cs *ConfigState) Pick() *Backend {
	healthy := cs.HealthyBackends()
	if len(healthy) == 0 {
		return nil
	}
	algo := cs.Algorithm()
	switch algo {
	case AlgoLeastConn:
		return pickLeastConn(healthy)
	default:
		return pickWeightedRoundRobin(cs, healthy)
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
