package lb

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// startBackend registers a backend in cs and returns its URL.
// Backends added via Apply start as Healthy=true.
func startBackend(cs *ConfigState, url string) {
	cs.Apply(Command{Op: OpAddBackend, URL: url, Weight: 1})
}

// newFastChecker creates a HealthChecker with a short interval for testing.
func newFastChecker(cs *ConfigState, interval time.Duration) *HealthChecker {
	hc := NewHealthChecker(cs, interval, zerolog.Nop())
	return hc
}

// waitCondition polls fn every 10 ms until it returns true or timeout expires.
func waitCondition(t *testing.T, timeout time.Duration, fn func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// TestHealthCheckerMarksUnhealthy verifies that a backend returning 500 is
// marked unhealthy after one check interval.
func TestHealthCheckerMarksUnhealthy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cs := NewConfigState()
	startBackend(cs, srv.URL)

	hc := newFastChecker(cs, 50*time.Millisecond)
	hc.Start()
	defer hc.Stop()

	ok := waitCondition(t, 500*time.Millisecond, func() bool {
		return len(cs.HealthyBackends()) == 0
	})
	if !ok {
		t.Fatal("backend was not marked unhealthy within 500ms")
	}
}

// TestHealthCheckerRecovery verifies that a backend which starts returning 200
// is marked healthy again after a subsequent check interval.
func TestHealthCheckerRecovery(t *testing.T) {
	var returnOK atomic.Int32 // 0 = return 500, 1 = return 200

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if returnOK.Load() == 1 {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer srv.Close()

	cs := NewConfigState()
	startBackend(cs, srv.URL)

	hc := newFastChecker(cs, 50*time.Millisecond)
	hc.Start()
	defer hc.Stop()

	// Wait for the backend to be marked unhealthy.
	if !waitCondition(t, 500*time.Millisecond, func() bool {
		return len(cs.HealthyBackends()) == 0
	}) {
		t.Fatal("backend was not marked unhealthy")
	}

	// Now make the backend healthy again.
	returnOK.Store(1)

	// Wait for recovery.
	if !waitCondition(t, 500*time.Millisecond, func() bool {
		return len(cs.HealthyBackends()) == 1
	}) {
		t.Fatal("backend did not recover to healthy within 500ms")
	}
}

// TestHealthCheckerTimeout verifies that a backend whose /health endpoint
// hangs longer than the client timeout is treated as unhealthy.
func TestHealthCheckerTimeout(t *testing.T) {
	// Handler blocks until the client gives up.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cs := NewConfigState()
	startBackend(cs, srv.URL)

	hc := newFastChecker(cs, 50*time.Millisecond)
	// Use a short client timeout so the test doesn't wait 3 s.
	hc.client.Timeout = 150 * time.Millisecond
	hc.Start()
	defer hc.Stop()

	// Backend should be marked unhealthy after the client times out.
	if !waitCondition(t, time.Second, func() bool {
		return len(cs.HealthyBackends()) == 0
	}) {
		t.Fatal("backend was not marked unhealthy after client timeout")
	}
}
