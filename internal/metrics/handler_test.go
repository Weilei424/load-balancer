package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestHandlerHistogramOutput verifies that the /metrics handler emits correct
// Prometheus histogram lines (HELP, TYPE, _bucket, _sum, _count, +Inf).
func TestHandlerHistogramOutput(t *testing.T) {
	m := New()
	backendURL := "http://backend.test:8080"

	// Record 3 observations: 5ms, 15ms, 30ms
	m.ObserveLatency(backendURL, 5*time.Millisecond)
	m.ObserveLatency(backendURL, 15*time.Millisecond)
	m.ObserveLatency(backendURL, 30*time.Millisecond)

	w := httptest.NewRecorder()
	m.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(w.Body)
	out := string(body)

	// Must have HELP and TYPE declarations.
	if !strings.Contains(out, "# HELP lb_backend_request_duration_seconds") {
		t.Error("missing # HELP line for histogram")
	}
	if !strings.Contains(out, "# TYPE lb_backend_request_duration_seconds histogram") {
		t.Error("missing # TYPE histogram line")
	}

	// le="0.005" (5ms): only the 5ms observation falls here → count 1
	le5 := `lb_backend_request_duration_seconds_bucket{url="` + backendURL + `",le="0.005"} 1`
	if !strings.Contains(out, le5) {
		t.Errorf("expected %q in output:\n%s", le5, out)
	}

	// le="0.025" (25ms): 5ms + 15ms fall here → count 2
	le25 := `lb_backend_request_duration_seconds_bucket{url="` + backendURL + `",le="0.025"} 2`
	if !strings.Contains(out, le25) {
		t.Errorf("expected %q in output:\n%s", le25, out)
	}

	// le="+Inf": all 3 observations
	leInf := `lb_backend_request_duration_seconds_bucket{url="` + backendURL + `",le="+Inf"} 3`
	if !strings.Contains(out, leInf) {
		t.Errorf("expected %q in output:\n%s", leInf, out)
	}

	// _count must be 3
	countLine := `lb_backend_request_duration_seconds_count{url="` + backendURL + `"} 3`
	if !strings.Contains(out, countLine) {
		t.Errorf("expected %q in output:\n%s", countLine, out)
	}

	// _sum must be present and non-zero
	if !strings.Contains(out, `lb_backend_request_duration_seconds_sum{url="`+backendURL+`"}`) {
		t.Errorf("missing _sum line in output:\n%s", out)
	}
}
