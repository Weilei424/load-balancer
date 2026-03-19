package lb

import (
	"fmt"
	"testing"
	"time"
)

func TestConsistentHashStickiness(t *testing.T) {
	cs := NewConfigState()
	cs.Apply(Command{Op: OpAddBackend, URL: "http://a", Weight: 1})
	cs.Apply(Command{Op: OpAddBackend, URL: "http://b", Weight: 1})
	cs.Apply(Command{Op: OpAddBackend, URL: "http://c", Weight: 1})
	cs.Apply(Command{Op: OpSetAlgorithm, Algorithm: AlgoConsistentHash})

	for i := 0; i < 10; i++ {
		ip := fmt.Sprintf("10.0.0.%d", i+1)
		var first string
		for j := 0; j < 100; j++ {
			b := cs.Pick(ip)
			if b == nil {
				t.Fatalf("Pick returned nil for ip=%s", ip)
			}
			if j == 0 {
				first = b.URL
			} else if b.URL != first {
				t.Fatalf("ip=%s: got %s on call %d, want %s (stickiness broken)", ip, b.URL, j+1, first)
			}
		}
	}
}

func TestConsistentHashRemoveBackend(t *testing.T) {
	cs := NewConfigState()
	cs.Apply(Command{Op: OpAddBackend, URL: "http://a", Weight: 1})
	cs.Apply(Command{Op: OpAddBackend, URL: "http://b", Weight: 1})
	cs.Apply(Command{Op: OpAddBackend, URL: "http://c", Weight: 1})
	cs.Apply(Command{Op: OpSetAlgorithm, Algorithm: AlgoConsistentHash})

	// Record initial mapping for 100 distinct keys.
	initial := make(map[string]string, 100)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		b := cs.Pick(key)
		if b == nil {
			t.Fatalf("Pick returned nil for key=%s", key)
		}
		initial[key] = b.URL
	}

	// Remove backend B.
	cs.Apply(Command{Op: OpRemoveBackend, URL: "http://b"})

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		b := cs.Pick(key)
		if b == nil {
			t.Fatalf("Pick returned nil after remove for key=%s", key)
		}
		if b.URL == "http://b" {
			t.Fatalf("key=%s still maps to removed backend http://b", key)
		}
		// Keys that mapped to A or C should remain stable.
		if initial[key] != "http://b" && b.URL != initial[key] {
			t.Errorf("key=%s: was %s, now %s (unexpected instability)", key, initial[key], b.URL)
		}
	}
}

// TestConsistentHashHealthTransition verifies that marking a backend unhealthy
// rebuilds the ring immediately so Pick() never returns the down backend.
func TestConsistentHashHealthTransition(t *testing.T) {
	cs := NewConfigState()
	cs.Apply(Command{Op: OpAddBackend, URL: "http://a", Weight: 1})
	cs.Apply(Command{Op: OpAddBackend, URL: "http://b", Weight: 1})
	cs.Apply(Command{Op: OpAddBackend, URL: "http://c", Weight: 1})
	cs.Apply(Command{Op: OpSetAlgorithm, Algorithm: AlgoConsistentHash})

	// Find a key that maps to http://b while all backends are healthy.
	var keyForB string
	for i := 0; i < 10000; i++ {
		k := fmt.Sprintf("hk-%d", i)
		if b := cs.Pick(k); b != nil && b.URL == "http://b" {
			keyForB = k
			break
		}
	}
	if keyForB == "" {
		t.Fatal("could not find a key that maps to http://b")
	}

	// Mark http://b unhealthy — ring must be rebuilt.
	cs.SetHealthy("http://b", false)

	// All picks for keyForB must now return a non-nil, non-b backend.
	for i := 0; i < 50; i++ {
		b := cs.Pick(keyForB)
		if b == nil {
			t.Fatal("Pick returned nil after backend went unhealthy")
		}
		if b.URL == "http://b" {
			t.Fatalf("Pick returned unhealthy backend http://b on attempt %d", i+1)
		}
	}
}

func TestConsistentHashCBFallback(t *testing.T) {
	now := time.Now()
	openCB := func() *CircuitBreaker {
		cb := &CircuitBreaker{nowFn: fakeNow(&now)}
		for i := 0; i < failureThreshold; i++ {
			cb.RecordFailure()
		}
		return cb
	}

	// Find a key that hashes to backend A so we can verify fallback.
	// Build a ring with all three backends and probe keys until one lands on A.
	ring := newHashRing([]*Backend{
		{URL: "http://a", Weight: 1, Healthy: true},
		{URL: "http://b", Weight: 1, Healthy: true},
		{URL: "http://c", Weight: 1, Healthy: true},
	})
	var keyForA string
	for i := 0; i < 10000; i++ {
		k := fmt.Sprintf("probe-%d", i)
		if u := ring.Get(k, nil); u == "http://a" {
			keyForA = k
			break
		}
	}
	if keyForA == "" {
		t.Fatal("could not find a key that maps to http://a")
	}

	cbA := openCB()
	cbB := newCircuitBreaker()
	cbC := newCircuitBreaker()

	cs := &ConfigState{algorithm: AlgoConsistentHash}
	cs.backends = []*Backend{
		{URL: "http://a", Weight: 1, Healthy: true, CB: cbA},
		{URL: "http://b", Weight: 1, Healthy: true, CB: cbB},
		{URL: "http://c", Weight: 1, Healthy: true, CB: cbC},
	}
	cs.rebuildRing()

	// A is open → should land on B or C.
	b := cs.Pick(keyForA)
	if b == nil {
		t.Fatal("Pick returned nil with A open")
	}
	if b.URL == "http://a" {
		t.Fatal("Pick returned open-circuit backend A")
	}

	// Open B as well → should land on C.
	cbB2 := openCB()
	cs.backends[1].CB = cbB2

	b = cs.Pick(keyForA)
	if b == nil {
		t.Fatal("Pick returned nil with A and B open")
	}
	if b.URL == "http://a" || b.URL == "http://b" {
		t.Fatalf("Pick returned open-circuit backend %s", b.URL)
	}
	if b.URL != "http://c" {
		t.Fatalf("expected http://c, got %s", b.URL)
	}

	// Open all backends → fallback without skip must still return non-nil.
	cbC2 := openCB()
	cs.backends[2].CB = cbC2

	b = cs.Pick(keyForA)
	if b == nil {
		t.Fatal("Pick must return non-nil when all backends are open (probe fallback)")
	}
}
