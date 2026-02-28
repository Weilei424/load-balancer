package metrics

import (
	"fmt"
	"net/http"
	"sort"
)

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

		for _, k := range keys {
			fmt.Fprintf(w, "# TYPE %s counter\n", k)
			fmt.Fprintf(w, "%s %d\n", k, snap[k])
		}
	})
}
