package lb

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/Weilei424/load-balancer/internal/metrics"
)

// TestLatencyHistogramRecorded sends 100 requests through the proxy to a backend
// that sleeps 20ms and verifies that the le="0.025" bucket captures all requests
// while the le="0.01" bucket is empty.
func TestLatencyHistogramRecorded(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(20 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cs := NewConfigState()
	cs.Apply(Command{Op: OpAddBackend, URL: srv.URL, Weight: 1})

	m := metrics.New()
	p := NewProxy(cs, m, zerolog.Nop())

	for i := 0; i < 100; i++ {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		w := httptest.NewRecorder()
		p.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("request %d: expected 200, got %d", i, w.Code)
		}
	}

	hists := m.SnapshotHistograms()
	h, ok := hists[srv.URL]
	if !ok {
		t.Fatalf("no histogram recorded for backend %s", srv.URL)
	}

	// BucketCounts[2] = le="0.01" (10ms): 20ms requests should NOT fall here.
	if h.BucketCounts[2] > 5 {
		t.Errorf("le=0.01 bucket: expected ≈0, got %d", h.BucketCounts[2])
	}
	// BucketCounts[3] = le="0.025" (25ms): all 20ms requests should fall here.
	if h.BucketCounts[3] < 90 {
		t.Errorf("le=0.025 bucket: expected ≈100, got %d", h.BucketCounts[3])
	}
	if h.Count != 100 {
		t.Errorf("count: expected 100, got %d", h.Count)
	}
	if h.SumSeconds < 1.0 { // 100 × 20ms ≈ 2s; allow generous slack for CI jitter
		t.Errorf("sum: expected ≥1.0s, got %f", h.SumSeconds)
	}
}
