package lb

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/Weilei424/load-balancer/internal/metrics"
)

// backendTransport is the shared http.Transport for all reverse proxies.
// A 5 s dial timeout and a 10 s response-header timeout bound the worst-case
// latency of a slow or stalled backend so the circuit breaker accumulates
// failures against it rather than blocking goroutines indefinitely.
var backendTransport http.RoundTripper = &http.Transport{
	DialContext: (&net.Dialer{
		Timeout: 5 * time.Second,
	}).DialContext,
	ResponseHeaderTimeout: 10 * time.Second,
	MaxIdleConns:          100,
	IdleConnTimeout:       90 * time.Second,
}

// proxyMaxRetries is the number of additional backend attempts after the first
// failure. Total attempts = 1 + proxyMaxRetries.
const proxyMaxRetries = 2

// Proxy is an HTTP reverse-proxy handler that routes requests to backends.
type Proxy struct {
	config  *ConfigState
	metrics *metrics.Metrics
	log     zerolog.Logger

	// proxies caches one *httputil.ReverseProxy per backend URL so each
	// backend's underlying http.Transport connection pool is reused across
	// requests rather than recreated on every call.
	proxies sync.Map // string → *httputil.ReverseProxy
}

// NewProxy creates a new Proxy handler.
func NewProxy(config *ConfigState, m *metrics.Metrics, log zerolog.Logger) *Proxy {
	return &Proxy{config: config, metrics: m, log: log}
}

// getProxy returns a cached *httputil.ReverseProxy for rawURL, creating one on
// first use. The cached proxy has a no-op ErrorHandler; error detection is
// handled by responseWriterTracker in ServeHTTP so the retry loop controls
// what gets written to the client.
func (p *Proxy) getProxy(rawURL string) *httputil.ReverseProxy {
	if v, ok := p.proxies.Load(rawURL); ok {
		return v.(*httputil.ReverseProxy)
	}
	target, _ := url.Parse(rawURL) // URL already validated when backend was added
	rp := httputil.NewSingleHostReverseProxy(target)
	rp.Transport = backendTransport
	// Suppress the default 502 write so ServeHTTP controls the error response.
	rp.ErrorHandler = func(http.ResponseWriter, *http.Request, error) {}
	v, _ := p.proxies.LoadOrStore(rawURL, rp)
	return v.(*httputil.ReverseProxy)
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	tracker := &responseWriterTracker{ResponseWriter: w}
	var lastBackend *Backend

	key := clientIP(r)
	for attempt := 0; attempt <= proxyMaxRetries; attempt++ {
		backend := p.config.Pick(key)
		if backend == nil {
			// No healthy backend available — could be the very first attempt
			// (nothing configured) or a mid-retry exhaustion (all marked down).
			break
		}
		lastBackend = backend

		// Circuit breaker: skip this backend without making a network call.
		// Counts as a retry (attempt increments on next iteration) per spec.
		if backend.CB != nil && !backend.CB.Allow() {
			p.log.Warn().
				Str("backend", backend.URL).
				Int("attempt", attempt+1).
				Msg("circuit open; skipping backend")
			continue
		}

		start := time.Now() // per-backend: measures time-to-first-byte for this attempt only
		atomic.AddInt64(&backend.ConnCount, 1)
		p.metrics.IncRequest(backend.URL)
		p.getProxy(backend.URL).ServeHTTP(tracker, r)
		atomic.AddInt64(&backend.ConnCount, -1)

		if tracker.started {
			// Response bytes reached the client — cannot retry regardless of
			// status. Record success or failure for the circuit breaker based
			// on the upstream status code: 5xx counts as a failure so repeated
			// error responses eventually open the circuit.
			p.metrics.ObserveLatency(backend.URL, tracker.firstByte.Sub(start))
			if backend.CB != nil {
				if tracker.statusCode >= 500 {
					backend.CB.RecordFailure()
				} else {
					backend.CB.RecordSuccess()
				}
			}
			return
		}

		// tracker.started is false: the backend's ErrorHandler fired before
		// any bytes were written, so the failure is recoverable.
		p.log.Warn().
			Str("backend", backend.URL).
			Int("attempt", attempt+1).
			Msg("backend unreachable; marking down and retrying")
		p.metrics.IncError(backend.URL)
		if backend.CB != nil {
			backend.CB.RecordFailure()
		}
		// Temporarily mark the backend unhealthy so subsequent picks avoid
		// it. The health checker re-enables it once probes succeed again.
		p.config.SetHealthy(backend.URL, false)
	}

	if tracker.started {
		return
	}

	if lastBackend == nil {
		// Pick() returned nil on the very first attempt: no backends configured.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"error": "no healthy backends configured"}) //nolint:errcheck
		p.metrics.IncError("")
		return
	}

	// We attempted at least one backend but all failed before any response started.
	p.log.Error().Str("backend", lastBackend.URL).Msg("all proxy attempts failed")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadGateway)
	json.NewEncoder(w).Encode(map[string]string{"error": "bad gateway"}) //nolint:errcheck
}

// clientIP extracts the client IP from the request. It prefers the
// X-Real-IP header (set by a trusted upstream proxy) and falls back to
// the TCP remote address.
func clientIP(r *http.Request) string {
	if ip := r.Header.Get("X-Real-IP"); ip != "" {
		return ip
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// responseWriterTracker wraps an http.ResponseWriter and records whether any
// response bytes have been sent to the client. The proxy retry loop uses
// tracker.started to decide if a backend failure is recoverable: if no bytes
// have reached the client yet, we can switch to a different backend and retry.
// firstByte records the wall-clock time of the first WriteHeader or Write call,
// used to compute time-to-first-byte latency.
// statusCode holds the HTTP status written by the upstream (0 if Write was
// called without a prior WriteHeader, treated as 200 for CB purposes).
type responseWriterTracker struct {
	http.ResponseWriter
	started    bool
	firstByte  time.Time
	statusCode int
}

func (t *responseWriterTracker) WriteHeader(code int) {
	if !t.started {
		t.firstByte = time.Now()
		t.started = true
		t.statusCode = code
	}
	t.ResponseWriter.WriteHeader(code)
}

func (t *responseWriterTracker) Write(b []byte) (int, error) {
	if !t.started {
		t.firstByte = time.Now()
		t.started = true
	}
	return t.ResponseWriter.Write(b)
}

// Flush forwards to the underlying writer if it implements http.Flusher.
// Required so streaming responses (e.g. SSE) work when the proxy is in the
// chain.
func (t *responseWriterTracker) Flush() {
	if f, ok := t.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
