package dashboard

import (
	"testing"

	"github.com/Weilei424/load-balancer/internal/metrics"
)

func TestP95msNoObservations(t *testing.T) {
	// Empty map: no backends have recorded any latency.
	got := p95ms(map[string]*metrics.HistogramData{})
	if got != 0 {
		t.Errorf("expected 0 for empty histogram map, got %v", got)
	}

	// Non-empty map but all counts zero.
	got = p95ms(map[string]*metrics.HistogramData{
		"http://localhost:9101": {Count: 0},
	})
	if got != 0 {
		t.Errorf("expected 0 for zero-count histogram, got %v", got)
	}
}

func TestP95msCorrectBucket(t *testing.T) {
	// 100 observations: 95 fall in the ≤10ms bucket (index 2), 5 in the +Inf bucket.
	// BucketCounts are cumulative, so BucketCounts[0..2] = 95, rest follow suit.
	h := &metrics.HistogramData{Count: 100}
	// Cumulative: first 3 finite buckets (≤1ms, ≤5ms, ≤10ms) each hold 95.
	h.BucketCounts[0] = 95
	h.BucketCounts[1] = 95
	h.BucketCounts[2] = 95
	// Remaining finite buckets and +Inf hold all 100.
	for i := 3; i < 10; i++ {
		h.BucketCounts[i] = 100
	}

	got := p95ms(map[string]*metrics.HistogramData{"http://b1": h})
	// threshold = int64(100 * 0.95) = 95; first bucket where agg[i] >= 95 is index 0 (1 ms).
	want := 1.0
	if got != want {
		t.Errorf("expected p95 = %v ms, got %v ms", want, got)
	}
}

func TestP95msInfFallback(t *testing.T) {
	// All 10 observations are slower than 1s (only +Inf bucket is non-zero).
	h := &metrics.HistogramData{Count: 10}
	// Finite buckets 0–8 have 0 cumulative count; only index 9 (+Inf) = 10.
	h.BucketCounts[9] = 10

	got := p95ms(map[string]*metrics.HistogramData{"http://b1": h})
	// threshold = 9; no finite bucket reaches 9; falls through to boundsMs[9] = 2000.
	want := 2000.0
	if got != want {
		t.Errorf("expected p95 = %v ms (Inf sentinel), got %v ms", want, got)
	}
}

func TestP95msAggregatesAcrossBackends(t *testing.T) {
	// Two backends, each with 10 observations all in the ≤25ms bucket.
	// Aggregated: 20 total, threshold = 19, agg[3] (≤25ms) = 20 >= 19 → 25ms.
	makeHist := func(count int64) *metrics.HistogramData {
		h := &metrics.HistogramData{Count: count}
		// Cumulative: buckets 0–3 (≤1ms, ≤5ms, ≤10ms, ≤25ms) all hold `count`.
		for i := 0; i <= 3; i++ {
			h.BucketCounts[i] = count
		}
		for i := 4; i < 10; i++ {
			h.BucketCounts[i] = count
		}
		return h
	}

	got := p95ms(map[string]*metrics.HistogramData{
		"http://b1": makeHist(10),
		"http://b2": makeHist(10),
	})
	want := 1.0 // agg[0] = 20 >= 19 immediately (all in ≤1ms bucket)
	if got != want {
		t.Errorf("expected p95 = %v ms, got %v ms", want, got)
	}
}
