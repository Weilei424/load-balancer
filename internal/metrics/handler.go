package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
)

// gaugeMetrics lists base metric names that are gauges (not monotonic counters).
var gaugeMetrics = map[string]bool{
	"raft_term": true,
	"raft_role": true,
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

		// Track which base metric names have already had a # TYPE line emitted.
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
				fmt.Fprintf(w, "# TYPE %s %s\n", baseName, typ)
				emittedType[baseName] = true
			}
			fmt.Fprintf(w, "%s %d\n", k, snap[k])
		}
	})
}
