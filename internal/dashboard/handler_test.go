package dashboard

import (
	"testing"

	"github.com/Weilei424/load-balancer/internal/metrics"
)

// makeCumHist builds a HistogramData whose BucketCounts are cumulative.
// `countBelow[i]` is the number of observations whose value is <= histBucketBounds[i].
// The caller must supply exactly 10 values; index 9 is the +Inf bucket (== total).
func makeCumHist(countBelow [10]int64) *metrics.HistogramData {
	h := &metrics.HistogramData{Count: countBelow[9]}
	h.BucketCounts = countBelow
	return h
}

func TestP95msNoObservations(t *testing.T) {
	// Empty map: no backends.
	if got := p95ms(map[string]*metrics.HistogramData{}); got != 0 {
		t.Errorf("empty map: want 0, got %v", got)
	}
	// Non-empty map, all counts zero.
	if got := p95ms(map[string]*metrics.HistogramData{"http://b": {Count: 0}}); got != 0 {
		t.Errorf("zero-count histogram: want 0, got %v", got)
	}
}

func TestP95msSingleSlowObservation(t *testing.T) {
	// 1 observation that is slower than 1s (only the +Inf bucket is non-zero).
	// With ceil(0.95*1)=1, the threshold is 1. No finite bucket reaches 1,
	// so p95ms must return the +Inf sentinel (2000ms), not 1ms.
	h := makeCumHist([10]int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 1})
	got := p95ms(map[string]*metrics.HistogramData{"http://b": h})
	if got != 2000 {
		t.Errorf("single >1s observation: want 2000 (Inf sentinel), got %v", got)
	}
}

func TestP95msCorrectBucket(t *testing.T) {
	// 100 observations: 95 are <=10ms (bucket index 2), 5 are >1s (only +Inf).
	// Cumulative counts: buckets 0,1 are 0 (none <=1ms or <=5ms),
	// bucket 2 onward through 8 hold 95, bucket 9 (+Inf) holds 100.
	counts := [10]int64{0, 0, 95, 95, 95, 95, 95, 95, 95, 100}
	h := makeCumHist(counts)
	got := p95ms(map[string]*metrics.HistogramData{"http://b": h})
	// threshold = ceil(100*0.95) = 95; first bucket where agg[i] >= 95 is index 2 → 10ms.
	if got != 10 {
		t.Errorf("95 obs <=10ms: want 10ms, got %v", got)
	}
}

func TestP95msInfFallback(t *testing.T) {
	// All 10 observations are >1s — finite buckets are all 0, only +Inf = 10.
	h := makeCumHist([10]int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 10})
	got := p95ms(map[string]*metrics.HistogramData{"http://b": h})
	// threshold = ceil(10*0.95) = 10; no finite bucket reaches 10 → 2000ms sentinel.
	if got != 2000 {
		t.Errorf("all obs >1s: want 2000 (Inf sentinel), got %v", got)
	}
}

func TestP95msAggregatesAcrossBackends(t *testing.T) {
	// Backend b1: 8 observations all <=50ms (bucket index 4); +Inf = 8.
	// Backend b2: 12 observations all <=50ms; +Inf = 12.
	// Aggregated: total=20, threshold=ceil(19)=19.
	// agg[4] = 8+12 = 20 >= 19 → 50ms. Earlier buckets are 0 for both backends.
	b1 := makeCumHist([10]int64{0, 0, 0, 0, 8, 8, 8, 8, 8, 8})
	b2 := makeCumHist([10]int64{0, 0, 0, 0, 12, 12, 12, 12, 12, 12})
	got := p95ms(map[string]*metrics.HistogramData{"http://b1": b1, "http://b2": b2})
	if got != 50 {
		t.Errorf("two backends, all obs <=50ms: want 50ms, got %v", got)
	}
}
