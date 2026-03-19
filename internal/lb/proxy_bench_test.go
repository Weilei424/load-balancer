package lb

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/rs/zerolog"

	"github.com/Weilei424/load-balancer/internal/metrics"
)

func benchmarkProxy(b *testing.B, algorithm string) {
	b.Helper()
	srvs := make([]*httptest.Server, 3)
	for i := range srvs {
		srvs[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
	}
	b.Cleanup(func() {
		for _, s := range srvs {
			s.Close()
		}
	})

	cs := NewConfigState()
	for _, s := range srvs {
		cs.Apply(Command{Op: OpAddBackend, URL: s.URL, Weight: 1})
	}
	cs.Apply(Command{Op: OpSetAlgorithm, Algorithm: algorithm})

	p := NewProxy(cs, metrics.New(), zerolog.Nop())
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if algorithm == AlgoConsistentHash {
				// Cycle through 256 source IPs so keys are distributed;
				// avoids pinning all parallel goroutines to one backend.
				req.Header.Set("X-Real-IP", "10.0.0."+strconv.Itoa(i%256))
			}
			p.ServeHTTP(httptest.NewRecorder(), req)
			i++
		}
	})
}

func BenchmarkProxyRoundRobin(b *testing.B)     { benchmarkProxy(b, AlgoRoundRobin) }
func BenchmarkProxyLeastConn(b *testing.B)      { benchmarkProxy(b, AlgoLeastConn) }
func BenchmarkProxyConsistentHash(b *testing.B) { benchmarkProxy(b, AlgoConsistentHash) }
