// Package integration contains end-to-end tests that boot multiple raftlite
// nodes in-process and exercise the full LB stack (raft + config state machine
// + proxy).  It lives in a separate package to avoid the lb→raftlite→lb import
// cycle that would arise if the tests were placed inside either package.
package integration

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/Weilei424/load-balancer/internal/lb"
	"github.com/Weilei424/load-balancer/internal/metrics"
	"github.com/Weilei424/load-balancer/internal/raftlite"
)

// clusterNode bundles a raftlite.Node with its HTTP server for in-process testing.
type clusterNode struct {
	*raftlite.Node
	ln  net.Listener
	srv *http.Server
}

func (c *clusterNode) stop() {
	c.Node.Stop()
	c.srv.Close()
	c.ln.Close()
}

// bootCluster creates n raftlite nodes with pre-allocated listeners so every
// node knows all peer addresses before it starts.  Returns the cluster nodes
// and per-node apply channels for wiring up lb.ConfigState appliers.
func bootCluster(t *testing.T, n int) ([]*clusterNode, []chan raftlite.LogEntry) {
	t.Helper()

	lns := make([]net.Listener, n)
	for i := range lns {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		lns[i] = ln
	}

	ids := make([]string, n)
	addrs := make(map[string]string, n)
	for i := range ids {
		ids[i] = string(rune('a' + i)) // "a", "b", "c"
		addrs[ids[i]] = "http://" + lns[i].Addr().String()
	}

	nodes := make([]*clusterNode, n)
	applyChans := make([]chan raftlite.LogEntry, n)
	for i, id := range ids {
		peers := make(map[string]string, n-1)
		for pid, pa := range addrs {
			if pid != id {
				peers[pid] = pa
			}
		}
		applyCh := make(chan raftlite.LogEntry, 64)
		applyChans[i] = applyCh
		proposeCh := make(chan raftlite.ProposeReq, 8)
		nd, err := raftlite.NewNode(raftlite.Config{
			ID:        id,
			Peers:     peers,
			HTTPPeers: make(map[string]string),
			DataDir:   t.TempDir(),
		}, applyCh, proposeCh, zerolog.Nop())
		if err != nil {
			t.Fatalf("NewNode %s: %v", id, err)
		}
		srv := &http.Server{Handler: nd.Handler()}
		go srv.Serve(lns[i]) //nolint:errcheck
		go nd.Run()
		nodes[i] = &clusterNode{Node: nd, ln: lns[i], srv: srv}
	}

	t.Cleanup(func() {
		for _, cn := range nodes {
			cn.stop()
		}
	})

	return nodes, applyChans
}

// waitForLeader polls nodes until one reports IsLeader, up to timeout.
func waitForLeader(t *testing.T, nodes []*clusterNode, timeout time.Duration) *clusterNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.IsLeader() {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no leader elected within %v", timeout)
	return nil
}

// attachConfigStates wires an lb.ConfigState applier goroutine to each apply
// channel and returns the per-node ConfigState slice.
func attachConfigStates(applyChans []chan raftlite.LogEntry) []*lb.ConfigState {
	states := make([]*lb.ConfigState, len(applyChans))
	for i, ch := range applyChans {
		cs := lb.NewConfigState()
		states[i] = cs
		go func(ch chan raftlite.LogEntry, cs *lb.ConfigState) {
			for entry := range ch {
				var cmd lb.Command
				if err := json.Unmarshal(entry.Command, &cmd); err != nil {
					continue
				}
				cs.Apply(cmd)
			}
		}(ch, cs)
	}
	return states
}

// waitForConvergence polls until every ConfigState has at least wantCount
// backends, or fatally fails after timeout.
func waitForConvergence(t *testing.T, states []*lb.ConfigState, wantCount int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for _, cs := range states {
			if len(cs.AllBackends()) < wantCount {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("config did not converge to %d backends within %v", wantCount, timeout)
}

// TestLBConfigConvergence boots a 3-node cluster, proposes 3 add-backend
// commands through the leader, and verifies that every node's ConfigState
// converges to the same 3 backends with LastApplied == CommitIndex.
func TestLBConfigConvergence(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	nodes, applyChans := bootCluster(t, 3)
	states := attachConfigStates(applyChans)

	leader := waitForLeader(t, nodes, 3*time.Second)

	backendURLs := []string{
		"http://backend1:8001",
		"http://backend2:8002",
		"http://backend3:8003",
	}
	for _, u := range backendURLs {
		cmd, _ := json.Marshal(lb.Command{Op: lb.OpAddBackend, URL: u, Weight: 1})
		if err := leader.Propose(cmd); err != nil {
			t.Fatalf("Propose(%s): %v", u, err)
		}
	}

	waitForConvergence(t, states, 3, 3*time.Second)

	// Assert all nodes have identical backend counts and LastApplied == CommitIndex.
	for i, cs := range states {
		if got := len(cs.AllBackends()); got != 3 {
			t.Errorf("node %d: want 3 backends, got %d", i, got)
		}
		s := nodes[i].Status()
		if s.LastApplied != s.CommitIndex {
			t.Errorf("node %d: LastApplied=%d != CommitIndex=%d", i, s.LastApplied, s.CommitIndex)
		}
	}
}

// TestProxyDistribution boots a 3-node cluster, adds 3 real httptest backends,
// and verifies that 300 requests through one node's proxy are distributed
// within ±20% of equal across all three backends.
func TestProxyDistribution(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Three real backends counting incoming requests.
	counts := make([]int64, 3)
	servers := make([]*httptest.Server, 3)
	for i := range servers {
		idx := i
		servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			atomic.AddInt64(&counts[idx], 1)
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(servers[i].Close)
	}

	nodes, applyChans := bootCluster(t, 3)
	states := attachConfigStates(applyChans)

	leader := waitForLeader(t, nodes, 3*time.Second)

	for _, srv := range servers {
		cmd, _ := json.Marshal(lb.Command{Op: lb.OpAddBackend, URL: srv.URL, Weight: 1})
		if err := leader.Propose(cmd); err != nil {
			t.Fatalf("Propose: %v", err)
		}
	}

	waitForConvergence(t, states, 3, 3*time.Second)

	// Send 300 requests through node 0's proxy (real HTTP roundtrips to backends).
	met := metrics.New()
	proxy := lb.NewProxy(states[0], met, zerolog.Nop())

	const total = 300
	for i := 0; i < total; i++ {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rr := httptest.NewRecorder()
		proxy.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("request %d: want 200, got %d", i, rr.Code)
		}
	}

	// Each backend should receive total/3 = 100 requests ± 20 (20%).
	want := total / len(servers)
	tol := want / 5
	for i := range servers {
		got := int(atomic.LoadInt64(&counts[i]))
		if got < want-tol || got > want+tol {
			t.Errorf("backend %d: got %d requests, want %d ± %d", i, got, want, tol)
		}
	}
}
