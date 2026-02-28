package raftlite

import (
	"encoding/json"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// clusterNode bundles a Node with its HTTP server for in-process testing.
type clusterNode struct {
	*Node
	ln  net.Listener
	srv *http.Server
}

func startClusterNode(t *testing.T, id string, peers map[string]string) *clusterNode {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	addr := "http://" + ln.Addr().String()
	// Build self-excluding peers map with real addresses.
	selfPeers := make(map[string]string)
	for pid, pa := range peers {
		if pid != id {
			selfPeers[pid] = pa
		}
	}

	applyCh := make(chan LogEntry, 64)
	proposeCh := make(chan ProposeReq, 4)
	n, err := NewNode(Config{
		ID:        id,
		Peers:     selfPeers,
		HTTPPeers: make(map[string]string),
		DataDir:   t.TempDir(),
	}, applyCh, proposeCh, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewNode %s: %v", id, err)
	}

	srv := &http.Server{Handler: n.Handler()}
	go srv.Serve(ln) //nolint:errcheck
	go n.Run()

	_ = addr
	return &clusterNode{Node: n, ln: ln, srv: srv}
}

func (c *clusterNode) Addr() string {
	return "http://" + c.ln.Addr().String()
}

func (c *clusterNode) Stop() {
	c.Node.Stop()
	c.srv.Close()
	c.ln.Close()
}

// waitForLeader polls nodes until one is leader, up to timeout.
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

func TestThreeNodeElection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Phase 1: allocate listeners to get real addresses before creating nodes.
	lns := make([]net.Listener, 3)
	for i := range lns {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		lns[i] = ln
	}

	ids := []string{"n1", "n2", "n3"}
	addrs := make(map[string]string)
	for i, id := range ids {
		addrs[id] = "http://" + lns[i].Addr().String()
	}

	var clusterNodes []*clusterNode
	for i, id := range ids {
		peers := make(map[string]string)
		for pid, pa := range addrs {
			if pid != id {
				peers[pid] = pa
			}
		}
		applyCh := make(chan LogEntry, 64)
		proposeCh := make(chan ProposeReq, 4)
		n, err := NewNode(Config{
			ID:        id,
			Peers:     peers,
			HTTPPeers: make(map[string]string),
			DataDir:   t.TempDir(),
		}, applyCh, proposeCh, zerolog.Nop())
		if err != nil {
			t.Fatalf("NewNode %s: %v", id, err)
		}

		srv := &http.Server{Handler: n.Handler()}
		go srv.Serve(lns[i]) //nolint:errcheck
		go n.Run()

		cn := &clusterNode{Node: n, ln: lns[i], srv: srv}
		clusterNodes = append(clusterNodes, cn)
	}
	defer func() {
		for _, cn := range clusterNodes {
			cn.Stop()
		}
	}()

	// Wait for leader election.
	leader := waitForLeader(t, clusterNodes, 3*time.Second)
	t.Logf("leader elected: %s (term %d)", leader.Status().ID, leader.Status().Term)

	// Verify at most one leader.
	leaderCount := 0
	for _, n := range clusterNodes {
		if n.IsLeader() {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", leaderCount)
	}
}

func TestLogReplicationOnThreeNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	lns := make([]net.Listener, 3)
	for i := range lns {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		lns[i] = ln
	}

	ids := []string{"n1", "n2", "n3"}
	addrs := make(map[string]string)
	for i, id := range ids {
		addrs[id] = "http://" + lns[i].Addr().String()
	}

	applyChans := make([]chan LogEntry, 3)
	var clusterNodes []*clusterNode
	for i, id := range ids {
		peers := make(map[string]string)
		for pid, pa := range addrs {
			if pid != id {
				peers[pid] = pa
			}
		}
		applyCh := make(chan LogEntry, 64)
		applyChans[i] = applyCh
		proposeCh := make(chan ProposeReq, 4)
		n, err := NewNode(Config{
			ID:        id,
			Peers:     peers,
			HTTPPeers: make(map[string]string),
			DataDir:   t.TempDir(),
		}, applyCh, proposeCh, zerolog.Nop())
		if err != nil {
			t.Fatalf("NewNode %s: %v", id, err)
		}
		srv := &http.Server{Handler: n.Handler()}
		go srv.Serve(lns[i]) //nolint:errcheck
		go n.Run()
		clusterNodes = append(clusterNodes, &clusterNode{Node: n, ln: lns[i], srv: srv})
	}
	defer func() {
		for _, cn := range clusterNodes {
			cn.Stop()
		}
	}()

	leader := waitForLeader(t, clusterNodes, 3*time.Second)

	// Propose a command through the leader.
	cmd, _ := json.Marshal(map[string]string{"op": "test", "value": "hello"})
	errCh := make(chan error, 1)
	go func() { errCh <- leader.Propose(cmd) }()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Propose error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Propose timed out")
	}

	// Wait for all nodes to apply the entry.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		applied := 0
		for _, cn := range clusterNodes {
			if cn.Status().LastApplied >= 1 {
				applied++
			}
		}
		if applied == 3 {
			return // success
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Check final state.
	for _, cn := range clusterNodes {
		s := cn.Status()
		t.Logf("node %s: lastApplied=%d commitIndex=%d", s.ID, s.LastApplied, s.CommitIndex)
	}
	t.Fatal("not all nodes applied the committed entry within 2s")
}

func TestLeaderFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	lns := make([]net.Listener, 3)
	for i := range lns {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		lns[i] = ln
	}

	ids := []string{"n1", "n2", "n3"}
	addrs := make(map[string]string)
	for i, id := range ids {
		addrs[id] = "http://" + lns[i].Addr().String()
	}

	var clusterNodes []*clusterNode
	for i, id := range ids {
		peers := make(map[string]string)
		for pid, pa := range addrs {
			if pid != id {
				peers[pid] = pa
			}
		}
		applyCh := make(chan LogEntry, 64)
		proposeCh := make(chan ProposeReq, 4)
		n, err := NewNode(Config{
			ID:        id,
			Peers:     peers,
			HTTPPeers: make(map[string]string),
			DataDir:   t.TempDir(),
		}, applyCh, proposeCh, zerolog.Nop())
		if err != nil {
			t.Fatalf("NewNode %s: %v", id, err)
		}
		srv := &http.Server{Handler: n.Handler()}
		go srv.Serve(lns[i]) //nolint:errcheck
		go n.Run()
		clusterNodes = append(clusterNodes, &clusterNode{Node: n, ln: lns[i], srv: srv})
	}

	leader := waitForLeader(t, clusterNodes, 3*time.Second)
	leaderID := leader.Status().ID
	t.Logf("initial leader: %s", leaderID)

	// Kill the leader.
	leader.Stop()

	// Remaining nodes should elect a new leader.
	var remaining []*clusterNode
	for _, cn := range clusterNodes {
		if cn.Status().ID != leaderID {
			remaining = append(remaining, cn)
		}
	}

	newLeader := waitForLeader(t, remaining, 3*time.Second)
	newLeaderID := newLeader.Status().ID
	t.Logf("new leader: %s (term %d)", newLeaderID, newLeader.Status().Term)

	if newLeaderID == leaderID {
		t.Fatal("new leader should be different from the killed one")
	}
	t.Logf("failover: %s → %s", leaderID, newLeaderID)
}
