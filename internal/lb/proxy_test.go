package lb

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/Weilei424/load-balancer/internal/metrics"
)

func TestProxyCachesReverseProxy(t *testing.T) {
	p := NewProxy(NewConfigState(), metrics.New(), zerolog.Nop())
	rp1 := p.getProxy("http://127.0.0.1:9999")
	rp2 := p.getProxy("http://127.0.0.1:9999")
	if rp1 != rp2 {
		t.Fatal("getProxy must return the same *ReverseProxy on repeated calls for the same URL")
	}
}

func TestProxyServes503WhenNoBackends(t *testing.T) {
	p := NewProxy(NewConfigState(), metrics.New(), zerolog.Nop())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	p.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}
}

func TestProxyServes502WhenAllBackendsFail(t *testing.T) {
	// Start a server and immediately close it so the port is unreachable.
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := dead.URL
	dead.Close()

	cs := NewConfigState()
	cs.Apply(Command{Op: OpAddBackend, URL: deadURL, Weight: 1})

	p := NewProxy(cs, metrics.New(), zerolog.Nop())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	p.ServeHTTP(w, req)
	if w.Code != http.StatusBadGateway {
		t.Fatalf("expected 502 after all retries exhausted, got %d", w.Code)
	}
}

func TestProxyRetrySucceedsOnSecondBackend(t *testing.T) {
	// good returns 200 with a body.
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "ok") //nolint:errcheck
	}))
	defer good.Close()

	// Start a server and immediately close it so the port is unreachable.
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := dead.URL
	dead.Close()

	// Set backends directly so we control SWRR ordering: dead is healthy[0]
	// (picked first by SWRR for equal weights).
	cs := &ConfigState{algorithm: AlgoRoundRobin}
	cs.backends = []*Backend{
		{URL: deadURL, Weight: 1, Healthy: true},
		{URL: good.URL, Weight: 1, Healthy: true},
	}

	p := NewProxy(cs, metrics.New(), zerolog.Nop())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	p.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 after retry on second backend, got %d", w.Code)
	}

	// The dead backend must be marked unhealthy after the failed attempt.
	for _, b := range cs.HealthyBackends() {
		if b.URL == deadURL {
			t.Fatal("dead backend should be marked unhealthy after proxy error")
		}
	}
}

func TestProxy5xxOpensCircuit(t *testing.T) {
	// Backend always returns 500. The circuit breaker must count these as
	// failures; the backend remains healthy (SetHealthy is not called for
	// responses that start writing), so Pick() keeps returning it until the
	// circuit opens after failureThreshold requests.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cs := NewConfigState()
	cs.Apply(Command{Op: OpAddBackend, URL: srv.URL, Weight: 1})
	p := NewProxy(cs, metrics.New(), zerolog.Nop())

	for i := 0; i < failureThreshold; i++ {
		p.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	}

	backends, _ := cs.Snapshot()
	if len(backends) == 0 {
		t.Fatal("no backends configured")
	}
	if !backends[0].CB.IsOpen() {
		t.Fatal("circuit must be open after failureThreshold 5xx responses")
	}
}

func TestProxyResponseHeaderTimeoutOpensCircuit(t *testing.T) {
	// Backend accepts the connection but never writes response headers,
	// simulating a stalled slow backend.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	defer srv.Close()

	// Shorten the response-header timeout so the test completes in ~250 ms.
	// backendTransport is read by getProxy() on first use; overriding it before
	// the first ServeHTTP call is sufficient because each test gets a fresh Proxy.
	old := backendTransport
	backendTransport = &http.Transport{ResponseHeaderTimeout: 50 * time.Millisecond}
	defer func() { backendTransport = old }()

	cs := NewConfigState()
	cs.Apply(Command{Op: OpAddBackend, URL: srv.URL, Weight: 1})
	p := NewProxy(cs, metrics.New(), zerolog.Nop())

	// Each ServeHTTP: transport times out → tracker.started=false →
	// RecordFailure() + SetHealthy(false). Re-enable between calls to
	// accumulate failureThreshold failures against the circuit breaker.
	for i := 0; i < failureThreshold; i++ {
		cs.SetHealthy(srv.URL, true)
		p.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	}

	backends, _ := cs.Snapshot()
	if len(backends) == 0 {
		t.Fatal("no backends configured")
	}
	if !backends[0].CB.IsOpen() {
		t.Fatal("circuit must be open after failureThreshold response-header timeouts")
	}
}

func TestProxyDoesNotRetryAfterResponseStarted(t *testing.T) {
	// Backend writes headers then closes — simulates a mid-stream failure.
	// The proxy should NOT retry because tracker.started is true.
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusOK)
		// Don't write body — the client gets the 200 header.
	}))
	defer srv.Close()

	cs := NewConfigState()
	cs.Apply(Command{Op: OpAddBackend, URL: srv.URL, Weight: 1})

	p := NewProxy(cs, metrics.New(), zerolog.Nop())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	p.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if calls != 1 {
		t.Fatalf("backend should be called exactly once, got %d", calls)
	}
}
