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
		return pickRoundRobin(cs, healthy)
	}
}

func pickRoundRobin(cs *ConfigState, healthy []*Backend) *Backend {
	idx := cs.NextRRCursor() % uint64(len(healthy))
	return healthy[idx]
}

func pickLeastConn(healthy []*Backend) *Backend {
	best := healthy[0]
	min := atomic.LoadInt64(&best.ConnCount)
	for _, b := range healthy[1:] {
		c := atomic.LoadInt64(&b.ConnCount)
		if c < min {
			min = c
			best = b
		}
	}
	return best
}
