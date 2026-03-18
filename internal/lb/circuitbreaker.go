package lb

import (
	"sync"
	"time"
)

type cbState int

const (
	cbClosed cbState = iota
	cbOpen
	cbHalfOpen
)

const (
	failureThreshold = 5
	windowDuration   = 10 * time.Second
	openTimeout      = 15 * time.Second
)

// CircuitBreaker implements a three-state (Closed→Open→HalfOpen) per-backend
// circuit breaker. Opens after failureThreshold consecutive errors within
// windowDuration; stays open for openTimeout; then allows one probe request.
type CircuitBreaker struct {
	mu            sync.Mutex
	state         cbState
	failures      int              // consecutive failures in current window
	windowStart   time.Time        // time of first failure in current consecutive run
	openedAt      time.Time        // time the circuit last transitioned to Open
	probeInFlight bool             // one probe request is in flight (HalfOpen only)
	nowFn         func() time.Time // injectable for tests; defaults to time.Now
}

func newCircuitBreaker() *CircuitBreaker {
	return &CircuitBreaker{nowFn: time.Now}
}

// Allow returns true if the request should proceed to the backend.
// Closed: always true.
// Open: false until openTimeout elapses, then transitions to HalfOpen and
//
//	returns true for the first caller (the probe request).
//
// HalfOpen: true for exactly one in-flight probe; false for all others.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.state {
	case cbClosed:
		return true
	case cbOpen:
		if cb.nowFn().Sub(cb.openedAt) >= openTimeout {
			cb.state = cbHalfOpen
			cb.probeInFlight = true
			return true
		}
		return false
	case cbHalfOpen:
		if !cb.probeInFlight {
			cb.probeInFlight = true
			return true
		}
		return false
	}
	return false
}

// RecordSuccess transitions to Closed and resets failure tracking.
// Safe to call from any state.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = cbClosed
	cb.failures = 0
	cb.probeInFlight = false
	cb.windowStart = time.Time{}
}

// RecordFailure advances the state machine on a backend error.
// HalfOpen: probe failed → reopen.
// Closed: increment consecutive failure count within window; open on threshold.
// Open: no-op (circuit already open).
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.state {
	case cbHalfOpen:
		cb.state = cbOpen
		cb.openedAt = cb.nowFn()
		cb.probeInFlight = false
	case cbClosed:
		now := cb.nowFn()
		if cb.failures == 0 || now.Sub(cb.windowStart) > windowDuration {
			// Start a fresh window: either first ever failure or window expired.
			cb.failures = 1
			cb.windowStart = now
		} else {
			cb.failures++
		}
		if cb.failures >= failureThreshold {
			cb.state = cbOpen
			cb.openedAt = now
			cb.failures = 0
		}
		// cbOpen: no-op
	}
}

// IsOpen returns true only when the circuit is in the Open state.
// HalfOpen is treated as 0 for the Prometheus gauge (per spec).
func (cb *CircuitBreaker) IsOpen() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state == cbOpen
}

// ShouldSkip returns true when the circuit should be excluded from the routing
// pool without transitioning state. Unlike Allow(), this is side-effect-free
// and safe to call from the router.
//
//   - Closed: false — backend is available.
//   - Open, timeout not elapsed: true — exclude from pool.
//   - Open, timeout elapsed: false — Allow() will transition to HalfOpen on
//     the next call (probe request).
//   - HalfOpen, probe in flight: true — only one probe at a time.
//   - HalfOpen, no probe in flight: false — Allow() will grant the probe.
func (cb *CircuitBreaker) ShouldSkip() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.state {
	case cbOpen:
		return cb.nowFn().Sub(cb.openedAt) < openTimeout
	case cbHalfOpen:
		return cb.probeInFlight
	default: // cbClosed
		return false
	}
}
