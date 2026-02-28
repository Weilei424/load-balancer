package lb

import (
	"sync"
	"sync/atomic"
	"testing"
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
