package lb

import (
	"encoding/json"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"

	"github.com/rs/zerolog"

	"github.com/Weilei424/load-balancer/internal/metrics"
)

// Proxy is an HTTP reverse-proxy handler that routes requests to backends.
type Proxy struct {
	config  *ConfigState
	metrics *metrics.Metrics
	log     zerolog.Logger
}

// NewProxy creates a new Proxy handler.
func NewProxy(config *ConfigState, m *metrics.Metrics, log zerolog.Logger) *Proxy {
	return &Proxy{config: config, metrics: m, log: log}
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	backend := p.config.Pick()
	if backend == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"error": "no healthy backends configured"}) //nolint:errcheck
		p.metrics.IncError("")
		return
	}

	target, err := url.Parse(backend.URL)
	if err != nil {
		p.log.Error().Err(err).Str("url", backend.URL).Msg("bad backend URL")
		http.Error(w, "internal error", http.StatusInternalServerError)
		p.metrics.IncError(backend.URL)
		return
	}

	atomic.AddInt64(&backend.ConnCount, 1)
	defer atomic.AddInt64(&backend.ConnCount, -1)

	rp := httputil.NewSingleHostReverseProxy(target)
	rp.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		p.log.Warn().Err(err).Str("backend", backend.URL).Msg("proxy error")
		p.metrics.IncError(backend.URL)
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}
	p.metrics.IncRequest(backend.URL)
	rp.ServeHTTP(w, r)
}
