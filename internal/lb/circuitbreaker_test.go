package lb

import (
	"testing"
	"time"
)

// fakeNow returns a closure over a *time.Time so tests can advance time by
// assigning to the pointed-at value.
func fakeNow(t *time.Time) func() time.Time {
	return func() time.Time { return *t }
}

func TestCircuitBreakerOpens(t *testing.T) {
	now := time.Now()
	cb := &CircuitBreaker{nowFn: fakeNow(&now)}

	// 4 failures — circuit must stay closed.
	for i := 0; i < failureThreshold-1; i++ {
		cb.RecordFailure()
		if !cb.Allow() {
			t.Fatalf("circuit should be closed after %d failures", i+1)
		}
	}

	// 5th failure — circuit must open.
	cb.RecordFailure()
	if cb.Allow() {
		t.Fatal("circuit must be open after failureThreshold failures")
	}
	if !cb.IsOpen() {
		t.Fatal("IsOpen must return true when circuit is open")
	}
}

func TestCircuitBreakerHalfOpen(t *testing.T) {
	now := time.Now()
	cb := &CircuitBreaker{nowFn: fakeNow(&now)}

	// Drive to Open.
	for i := 0; i < failureThreshold; i++ {
		cb.RecordFailure()
	}
	if cb.Allow() {
		t.Fatal("circuit should be open immediately after threshold")
	}

	// Advance past openTimeout — next Allow() transitions to HalfOpen.
	now = now.Add(openTimeout + time.Millisecond)
	if !cb.Allow() {
		t.Fatal("first Allow() after openTimeout must return true (probe)")
	}
	// Second concurrent Allow() must be blocked (probe already in flight).
	if cb.Allow() {
		t.Fatal("second Allow() in HalfOpen must return false")
	}

	// Probe fails — must reopen.
	cb.RecordFailure()
	if cb.Allow() {
		t.Fatal("circuit must reopen after HalfOpen probe failure")
	}
}

func TestCircuitBreakerRecovery(t *testing.T) {
	now := time.Now()
	cb := &CircuitBreaker{nowFn: fakeNow(&now)}

	// Drive to Open.
	for i := 0; i < failureThreshold; i++ {
		cb.RecordFailure()
	}

	// Advance past openTimeout.
	now = now.Add(openTimeout + time.Millisecond)

	// Probe succeeds — must transition to Closed.
	if !cb.Allow() {
		t.Fatal("probe Allow() must return true")
	}
	cb.RecordSuccess()

	if !cb.Allow() {
		t.Fatal("circuit must be closed after successful probe")
	}
	if cb.IsOpen() {
		t.Fatal("IsOpen must return false after recovery")
	}
}
