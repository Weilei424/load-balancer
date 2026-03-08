package raftlite

import (
	"encoding/json"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// runBatchCatchupTest boots a 3-node cluster, stops a follower, commits
// numEntries on the remaining majority, then restarts the follower with a
// fresh log and asserts it catches up in ≤ maxRPCs AppendEntries RPCs.
func runBatchCatchupTest(t *testing.T, numEntries, maxRPCs int) {
	t.Helper()

	// Pre-allocate all 3 listeners so addresses are known before nodes start.
	lns := make([]net.Listener, 3)
	for i := range lns {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		lns[i] = ln
	}

	ids := []string{"n1", "n2", "n3"}
	addrs := make(map[string]string) // id → "http://host:port"
	for i, id := range ids {
		addrs[id] = "http://" + lns[i].Addr().String()
	}

	// Start all 3 nodes so a leader can be elected.
	var nodes []*clusterNode
	for i, id := range ids {
		peers := make(map[string]string)
		for pid, pa := range addrs {
			if pid != id {
				peers[pid] = pa
			}
		}
		applyCh := make(chan LogEntry, 128)
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
		nodes = append(nodes, &clusterNode{Node: n, ln: lns[i], srv: srv})
	}

	leader := waitForLeader(t, nodes, 3*time.Second)

	// Pick any follower to stop.
	var followerIdx int
	var followerNode *clusterNode
	for i, cn := range nodes {
		if !cn.IsLeader() {
			followerIdx = i
			followerNode = cn
			break
		}
	}
	followerID := ids[followerIdx]
	followerAddr := lns[followerIdx].Addr().String()
	followerPeers := make(map[string]string)
	for pid, pa := range addrs {
		if pid != followerID {
			followerPeers[pid] = pa
		}
	}

	// Stop the follower. The remaining two nodes (leader + 1 follower) have
	// a 2/3 majority and can still commit.
	followerNode.Stop()

	defer func() {
		for i, cn := range nodes {
			if i != followerIdx {
				cn.Stop()
			}
		}
	}()

	// Propose numEntries and wait for all of them to commit.
	cmd, _ := json.Marshal(map[string]string{"op": "batch-test"})
	for i := 0; i < numEntries; i++ {
		if err := leader.Propose(cmd); err != nil {
			t.Fatalf("Propose %d: %v", i, err)
		}
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if leader.Status().CommitIndex >= numEntries {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if leader.Status().CommitIndex < numEntries {
		t.Fatalf("leader commitIndex=%d, want >=%d", leader.Status().CommitIndex, numEntries)
	}

	// Restart the follower with a fresh (empty) log and a counting handler.
	var appendRPCCount int64

	newLn, err := net.Listen("tcp", followerAddr)
	if err != nil {
		t.Fatalf("rebind %s: %v", followerAddr, err)
	}

	applyCh2 := make(chan LogEntry, 128)
	proposeCh2 := make(chan ProposeReq, 4)
	n2, err := NewNode(Config{
		ID:        followerID,
		Peers:     followerPeers,
		HTTPPeers: make(map[string]string),
		DataDir:   t.TempDir(), // fresh — no prior log
	}, applyCh2, proposeCh2, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewNode (restart) %s: %v", followerID, err)
	}

	base := n2.Handler()
	counting := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/raft/append" {
			atomic.AddInt64(&appendRPCCount, 1)
		}
		base.ServeHTTP(w, r)
	})

	restarted := &clusterNode{
		Node: n2,
		ln:   newLn,
		srv:  &http.Server{Handler: counting},
	}
	defer restarted.Stop()

	go restarted.srv.Serve(newLn) //nolint:errcheck
	go n2.Run()

	// Wait for the restarted follower to apply all entries.
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if n2.Status().LastApplied >= numEntries {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	got := atomic.LoadInt64(&appendRPCCount)
	if n2.Status().LastApplied < numEntries {
		t.Fatalf("restarted follower lastApplied=%d, want >=%d (appendRPCs=%d)",
			n2.Status().LastApplied, numEntries, got)
	}

	if got > int64(maxRPCs) {
		t.Fatalf("catchup took %d AppendEntries RPCs, want ≤%d (numEntries=%d, maxEntriesPerRPC=%d)",
			got, maxRPCs, numEntries, maxEntriesPerRPC)
	}
	t.Logf("catchup: %d entries in %d AppendEntries RPCs (≤%d allowed)", numEntries, got, maxRPCs)
}

// TestBatchAppendEntriesCatchup verifies the basic batch behavior:
// 20 entries fit in one batch so catchup takes ≤ ceil(20/64)+1 = 2 RPCs.
func TestBatchAppendEntriesCatchup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	const numEntries = 20
	maxRPCs := (numEntries+maxEntriesPerRPC-1)/maxEntriesPerRPC + 1 // ceil(20/64)+1 = 2
	runBatchCatchupTest(t, numEntries, maxRPCs)
}

// TestBatchAppendEntriesSplit verifies the acceptance criterion from upgrade-plan.md:
// a fresh follower with 100 committed entries behind converges in ≤ 3 AppendEntries
// RPCs. This exercises the batch-splitting path (100 > maxEntriesPerRPC=64) so the
// leader must send at least two non-empty batches.
func TestBatchAppendEntriesSplit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	const numEntries = 100
	// ceil(100/64)+1 = 2+1 = 3: up to 1 rejection round-trip + 2 payload batches.
	runBatchCatchupTest(t, numEntries, 3)
}
