package lb

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRoundRobinDistribution(t *testing.T) {
	cs := NewConfigState()
	cs.Apply(Command{Op: OpAddBackend, URL: "http://b1", Weight: 1})
	cs.Apply(Command{Op: OpAddBackend, URL: "http://b2", Weight: 1})
	cs.Apply(Command{Op: OpAddBackend, URL: "http://b3", Weight: 1})

	counts := map[string]int{}
	n := 300
	for i := 0; i < n; i++ {
		b := cs.Pick()
		if b == nil {
			t.Fatal("Pick returned nil")
		}
		counts[b.URL]++
	}
	// Each backend should get ~100 requests; allow ±20 variance.
	for url, c := range counts {
		if c < 80 || c > 120 {
			t.Errorf("backend %s got %d/%d requests (want ~100)", url, c, n)
		}
	}
}

func TestLeastConnPrefersIdle(t *testing.T) {
	cs := &ConfigState{algorithm: AlgoLeastConn}
	b1 := &Backend{URL: "http://b1", Weight: 1, Healthy: true}
	b2 := &Backend{URL: "http://b2", Weight: 1, Healthy: true}
	atomic.StoreInt64(&b1.ConnCount, 10)
	atomic.StoreInt64(&b2.ConnCount, 0)
	cs.backends = []*Backend{b1, b2}

	for i := 0; i < 20; i++ {
		b := cs.Pick()
		if b == nil || b.URL != "http://b2" {
			t.Fatalf("expected b2 (idle), got %v", b)
		}
	}
}

func TestPickReturnsNilWithNoBackends(t *testing.T) {
	cs := NewConfigState()
	if cs.Pick() != nil {
		t.Fatal("Pick must return nil when there are no backends")
	}
}

func TestPickReturnsNilWhenAllUnhealthy(t *testing.T) {
	cs := NewConfigState()
	cs.Apply(Command{Op: OpAddBackend, URL: "http://b1"})
	cs.SetHealthy("http://b1", false)
	if cs.Pick() != nil {
		t.Fatal("Pick must return nil when all backends are unhealthy")
	}
}

func TestConcurrentPick(t *testing.T) {
	cs := NewConfigState()
	cs.Apply(Command{Op: OpAddBackend, URL: "http://b1"})
	cs.Apply(Command{Op: OpAddBackend, URL: "http://b2"})

	var wg sync.WaitGroup
	var failures int64
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if cs.Pick() == nil {
				atomic.AddInt64(&failures, 1)
			}
		}()
	}
	wg.Wait()
	if failures > 0 {
		t.Fatalf("Pick returned nil %d times under concurrent access", failures)
	}
}

func TestApplyAddRemoveBackend(t *testing.T) {
	cs := NewConfigState()
	cs.Apply(Command{Op: OpAddBackend, URL: "http://b1", Weight: 2})
	backends := cs.AllBackends()
	if len(backends) != 1 || backends[0].URL != "http://b1" || backends[0].Weight != 2 {
		t.Fatalf("unexpected state after AddBackend: %+v", backends)
	}

	// Duplicate add should be a no-op.
	cs.Apply(Command{Op: OpAddBackend, URL: "http://b1"})
	if len(cs.AllBackends()) != 1 {
		t.Fatal("duplicate AddBackend should be no-op")
	}

	cs.Apply(Command{Op: OpRemoveBackend, URL: "http://b1"})
	if len(cs.AllBackends()) != 0 {
		t.Fatal("backend should be removed")
	}
}

func TestWeightedRoundRobin(t *testing.T) {
	cs := NewConfigState()
	cs.Apply(Command{Op: OpAddBackend, URL: "http://b1", Weight: 2})
	cs.Apply(Command{Op: OpAddBackend, URL: "http://b2", Weight: 1})

	counts := map[string]int{}
	n := 300
	for i := 0; i < n; i++ {
		b := cs.Pick()
		if b == nil {
			t.Fatal("Pick returned nil")
		}
		counts[b.URL]++
	}
	// b1 (weight=2) should get ~200 requests, b2 (weight=1) ~100.
	// Allow ±20 variance (±10% of total).
	if counts["http://b1"] < 180 || counts["http://b1"] > 220 {
		t.Errorf("b1 got %d/%d requests (want ~200)", counts["http://b1"], n)
	}
	if counts["http://b2"] < 80 || counts["http://b2"] > 120 {
		t.Errorf("b2 got %d/%d requests (want ~100)", counts["http://b2"], n)
	}
}

func TestWeightedLeastConn(t *testing.T) {
	cs := &ConfigState{algorithm: AlgoLeastConn}
	// b1: weight=2, conns=4 → score = 4/2 = 2.0
	// b2: weight=1, conns=1 → score = 1/1 = 1.0
	// b2 should always win because its weighted score is lower.
	b1 := &Backend{URL: "http://b1", Weight: 2, Healthy: true}
	b2 := &Backend{URL: "http://b2", Weight: 1, Healthy: true}
	atomic.StoreInt64(&b1.ConnCount, 4)
	atomic.StoreInt64(&b2.ConnCount, 1)
	cs.backends = []*Backend{b1, b2}

	for i := 0; i < 20; i++ {
		b := cs.Pick()
		if b == nil || b.URL != "http://b2" {
			t.Fatalf("expected b2 (lower weighted score), got %v", b)
		}
	}
}

func TestPickExcludesOpenCircuitLeastConn(t *testing.T) {
	now := time.Now()
	openCB := &CircuitBreaker{nowFn: fakeNow(&now)}
	for i := 0; i < failureThreshold; i++ {
		openCB.RecordFailure()
	}

	cs := &ConfigState{algorithm: AlgoLeastConn}
	// "open" is first so least_conn tie-breaking (equal ConnCount=0) would
	// always pick it if CB filtering were absent — that was the starvation bug.
	open := &Backend{URL: "http://open:80", Weight: 1, Healthy: true, CB: openCB}
	good := &Backend{URL: "http://good:80", Weight: 1, Healthy: true, CB: newCircuitBreaker()}
	cs.backends = []*Backend{open, good}

	for i := 0; i < 20; i++ {
		got := cs.Pick()
		if got == nil {
			t.Fatal("Pick() returned nil with healthy backends")
		}
		if got.URL == open.URL {
			t.Fatalf("Pick() returned open-circuit backend on attempt %d", i+1)
		}
	}
}

func TestPickHalfOpenProbeAfterTimeout(t *testing.T) {
	now := time.Now()
	cb := &CircuitBreaker{nowFn: fakeNow(&now)}
	for i := 0; i < failureThreshold; i++ {
		cb.RecordFailure()
	}

	cs := &ConfigState{algorithm: AlgoLeastConn}
	// "open" first in list so least_conn picks it once both are in the pool.
	open := &Backend{URL: "http://open:80", Weight: 1, Healthy: true, CB: cb}
	good := &Backend{URL: "http://good:80", Weight: 1, Healthy: true, CB: newCircuitBreaker()}
	cs.backends = []*Backend{open, good}

	// Before openTimeout: "open" is excluded from pool; only "good" is returned.
	for i := 0; i < 5; i++ {
		got := cs.Pick()
		if got == nil || got.URL != good.URL {
			t.Fatalf("before openTimeout Pick() must return healthy backend; got %v", got)
		}
	}

	// Advance past openTimeout: ShouldSkip() now returns false, "open" re-enters pool.
	now = now.Add(openTimeout + time.Millisecond)

	// With ConnCount=0 for both, least_conn picks "open" (first in list).
	got := cs.Pick()
	if got == nil || got.URL != open.URL {
		t.Fatalf("after openTimeout Pick() must return the previously-open backend; got %v", got)
	}
	// Allow() transitions to HalfOpen and grants the probe.
	if !cb.Allow() {
		t.Fatal("Allow() must return true for the first probe after openTimeout")
	}
}

func TestApplySetAlgorithm(t *testing.T) {
	cs := NewConfigState()
	cs.Apply(Command{Op: OpSetAlgorithm, Algorithm: AlgoLeastConn})
	if cs.Algorithm() != AlgoLeastConn {
		t.Fatalf("expected least_conn, got %s", cs.Algorithm())
	}
	// Unknown algorithm should be ignored.
	cs.Apply(Command{Op: OpSetAlgorithm, Algorithm: "random"})
	if cs.Algorithm() != AlgoLeastConn {
		t.Fatal("unknown algorithm should not change the setting")
	}
}
