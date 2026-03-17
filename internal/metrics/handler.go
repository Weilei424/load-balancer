package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
)

// gaugeMetrics lists base metric names that are gauges (not monotonic counters).
var gaugeMetrics = map[string]bool{
	"raft_term":              true,
	"raft_role":              true,
	"lb_requests_per_second": true,
	"lb_backend_conn_count":  true,
}

// metricHelp provides human-readable descriptions for Prometheus # HELP lines.
var metricHelp = map[string]string{
	"lb_backend_request_duration_seconds": "Request latency to a backend.",
	"lb_requests_total":                   "Total number of proxied requests.",
	"lb_errors_total":                     "Total number of proxy errors (no backend, dial failure).",
	"lb_backend_requests":                 "Total requests forwarded to a specific backend.",
	"lb_backend_errors":                   "Total proxy errors for a specific backend.",
	"lb_requests_per_second":              "Current request rate in requests per second (sampled over the last second).",
	"lb_backend_conn_count":               "Current number of active in-flight connections to a backend.",
	"raft_term":                           "Current Raft term.",
	"raft_role":                           "Current Raft role: 0=follower, 1=candidate, 2=leader.",
}

// Handler returns an http.HandlerFunc that writes Prometheus text format metrics.
// No external Prometheus library is used — hand-rolled per CLAUDE.md.
func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		snap := m.Snapshot()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

		keys := make([]string, 0, len(snap))
		for k := range snap {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		// Track which base metric names have already had # HELP / # TYPE emitted.
		emittedType := make(map[string]bool)
		for _, k := range keys {
			// Extract base name: strip label set (everything from '{' onward).
			baseName := k
			if i := strings.IndexByte(k, '{'); i >= 0 {
				baseName = k[:i]
			}
			if !emittedType[baseName] {
				typ := "counter"
				if gaugeMetrics[baseName] {
					typ = "gauge"
				}
				if help, ok := metricHelp[baseName]; ok {
					fmt.Fprintf(w, "# HELP %s %s\n", baseName, help)
				}
				fmt.Fprintf(w, "# TYPE %s %s\n", baseName, typ)
				emittedType[baseName] = true
			}
			fmt.Fprintf(w, "%s %d\n", k, snap[k])
		}

		// Render per-backend latency histograms (Prometheus histogram format).
		histSnap := m.SnapshotHistograms()
		if len(histSnap) > 0 {
			urls := make([]string, 0, len(histSnap))
			for url := range histSnap {
				urls = append(urls, url)
			}
			sort.Strings(urls)

			const family = "lb_backend_request_duration_seconds"
			fmt.Fprintf(w, "# HELP %s %s\n", family, metricHelp[family])
			fmt.Fprintf(w, "# TYPE %s histogram\n", family)

			for _, url := range urls {
				d := histSnap[url]
				for i, le := range histBucketLabels {
					fmt.Fprintf(w, "%s_bucket{url=%q,le=%q} %d\n", family, url, le, d.BucketCounts[i])
				}
				fmt.Fprintf(w, "%s_bucket{url=%q,le=\"+Inf\"} %d\n", family, url, d.BucketCounts[9])
				fmt.Fprintf(w, "%s_sum{url=%q} %g\n", family, url, d.SumSeconds)
				fmt.Fprintf(w, "%s_count{url=%q} %d\n", family, url, d.Count)
			}
		}
	})
}
