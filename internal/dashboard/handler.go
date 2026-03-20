package dashboard

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/Weilei424/load-balancer/internal/lb"
	"github.com/Weilei424/load-balancer/internal/metrics"
	"github.com/Weilei424/load-balancer/internal/raftlite"
)

//go:embed static/index.html
var dashboardHTML string

// Handler serves the dashboard HTML and SSE stream.
type Handler struct {
	node        *raftlite.Node
	config      *lb.ConfigState
	metrics     *metrics.Metrics
	broadcaster *Broadcaster
}

// NewHandler creates a dashboard Handler.
func NewHandler(node *raftlite.Node, config *lb.ConfigState, m *metrics.Metrics, b *Broadcaster) *Handler {
	return &Handler{node: node, config: config, metrics: m, broadcaster: b}
}

// ServeHTTP dispatches to the HTML or SSE handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/events" {
		h.serveSSE(w, r)
		return
	}
	h.serveHTML(w, r)
}

func (h *Handler) serveHTML(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(dashboardHTML)) //nolint:errcheck
}

func (h *Handler) serveSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	ch := h.broadcaster.Subscribe()
	defer h.broadcaster.Unsubscribe(ch)

	// Send current state immediately.
	snap := h.buildSnapshot()
	h.writeSSEEvent(w, snap)
	flusher.Flush()

	for {
		select {
		case data, ok := <-ch:
			if !ok {
				return
			}
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func (h *Handler) writeSSEEvent(w http.ResponseWriter, snap NodeSnapshot) {
	data, _ := json.Marshal(snap)
	fmt.Fprintf(w, "data: %s\n\n", data)
}

// p95ms computes the p95 latency in milliseconds by aggregating all per-backend
// histograms. Returns 0 when no observations have been recorded.
// boundsMs maps histogram bucket index (0–9) to an upper-bound in milliseconds;
// index 9 is the +Inf sentinel displayed as 2000 ms.
func p95ms(hists map[string]*metrics.HistogramData) float64 {
	var agg [10]int64
	var total int64
	for _, h := range hists {
		for i, c := range h.BucketCounts {
			agg[i] += c
		}
		total += h.Count
	}
	if total == 0 {
		return 0
	}
	boundsMs := [10]float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2000}
	// ceil ensures a single slow observation is not misclassified into the first bucket.
	threshold := int64(math.Ceil(float64(total) * 0.95))
	for i := 0; i < 9; i++ { // indices 0–8 are finite bounds
		if agg[i] >= threshold {
			return boundsMs[i]
		}
	}
	return boundsMs[9] // +Inf bucket fallback
}

// BuildSnapshot builds the current dashboard snapshot. Exported so cmd/lb/node.go can use it.
func (h *Handler) BuildSnapshot() NodeSnapshot {
	return h.buildSnapshot()
}

func (h *Handler) buildSnapshot() NodeSnapshot {
	status := h.node.Status()
	backends, algo := h.config.Snapshot()

	bsnaps := make([]BackendSnapshot, 0, len(backends))
	for _, b := range backends {
		bsnaps = append(bsnaps, BackendSnapshot{
			URL:       b.URL,
			Weight:    b.Weight,
			Healthy:   b.Healthy,
			ConnCount: atomic.LoadInt64(&b.ConnCount),
		})
	}

	m := h.metrics.Snapshot()
	hists := h.metrics.SnapshotHistograms()
	return NodeSnapshot{
		ID:                status.ID,
		Role:              status.Role,
		Term:              status.Term,
		LeaderID:          status.LeaderID,
		CommitIndex:       status.CommitIndex,
		LastApplied:       status.LastApplied,
		LogLength:         status.LogLength,
		Algorithm:         algo,
		Backends:          bsnaps,
		RequestsTotal:     m["lb_requests_total"],
		RequestsPerSecond: h.metrics.RequestRate(),
		ErrorsTotal:       m["lb_errors_total"],
		P95LatencyMs:      p95ms(hists),
		Timestamp:         time.Now().UnixMilli(),
	}
}
