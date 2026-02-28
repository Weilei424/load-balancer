package lb

import (
	"net/http"
	"time"

	"github.com/rs/zerolog"
)

// HealthChecker periodically checks backend /health endpoints.
type HealthChecker struct {
	config   *ConfigState
	interval time.Duration
	client   *http.Client
	log      zerolog.Logger
	stopCh   chan struct{}
}

// NewHealthChecker creates a HealthChecker that probes every interval.
func NewHealthChecker(config *ConfigState, interval time.Duration, log zerolog.Logger) *HealthChecker {
	return &HealthChecker{
		config:   config,
		interval: interval,
		client:   &http.Client{Timeout: 3 * time.Second},
		log:      log,
		stopCh:   make(chan struct{}),
	}
}

// Start begins the background health-check loop.
func (hc *HealthChecker) Start() {
	go hc.run()
}

// Stop halts the health-check loop.
func (hc *HealthChecker) Stop() {
	close(hc.stopCh)
}

func (hc *HealthChecker) run() {
	ticker := time.NewTicker(hc.interval)
	defer ticker.Stop()
	for {
		select {
		case <-hc.stopCh:
			return
		case <-ticker.C:
			hc.checkAll()
		}
	}
}

func (hc *HealthChecker) checkAll() {
	for _, b := range hc.config.AllBackends() {
		go hc.checkOne(b)
	}
}

func (hc *HealthChecker) checkOne(b *Backend) {
	resp, err := hc.client.Get(b.URL + "/health")
	healthy := err == nil && resp.StatusCode == http.StatusOK
	if resp != nil {
		resp.Body.Close()
	}
	if b.Healthy != healthy {
		hc.log.Info().Str("url", b.URL).Bool("healthy", healthy).Msg("backend health changed")
		hc.config.SetHealthy(b.URL, healthy)
	}
}
