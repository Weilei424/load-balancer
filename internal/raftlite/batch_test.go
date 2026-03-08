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

// TestBatchAppendEntriesCatchup verifies that a lagging follower catches up in
// ≤ ceil(numEntries/maxEntriesPerRPC)+1 AppendEntries RPCs rather than one per entry.
func TestBatchAppendEntriesCatchup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	const numEntries = 20

	// Phase 1: allocate listeners so we know all addresses up front.
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

	// Phase 2: start all 3 nodes so a leader can be elected.
	dataDirs := make([]string, 3)
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
		dir := t.TempDir()
		dataDirs[i] = dir
		n, err := NewNode(Config{
			ID:        id,
			Peers:     peers,
			HTTPPeers: make(map[string]string),
			DataDir:   dir,
		}, applyCh, proposeCh, zerolog.Nop())
		if err != nil {
			t.Fatalf("NewNode %s: %v", id, err)
		}
		srv := &http.Server{Handler: n.Handler()}
		go srv.Serve(lns[i]) //nolint:errcheck
		go n.Run()
		nodes = append(nodes, &clusterNode{Node: n, ln: lns[i], srv: srv})
	}

	// Wait for a leader to be elected.
	leader := waitForLeader(t, nodes, 3*time.Second)

	// Find a follower (not the leader) to stop.
	var followerIdx int
	var followerNode *clusterNode
	followerAddr := ""
	for i, cn := range nodes {
		if !cn.IsLeader() {
			followerIdx = i
			followerNode = cn
			followerAddr = lns[i].Addr().String()
			break
		}
	}

	// Determine the peer map for the follower's replacement node.
	followerID := ids[followerIdx]
	followerPeers := make(map[string]string)
	for pid, pa := range addrs {
		if pid != followerID {
			followerPeers[pid] = pa
		}
	}

	// Stop the follower (closes its Run loop, HTTP server, and listener).
	followerNode.Stop()

	// Phase 3: stop the other non-leader + leader cleanup on test exit.
	defer func() {
		for i, cn := range nodes {
			if i != followerIdx {
				cn.Stop()
			}
		}
	}()

	// Phase 4: propose numEntries on the leader. The remaining two nodes
	// (leader + one follower) constitute a majority (2/3) and can commit.
	cmd, _ := json.Marshal(map[string]string{"op": "batch-test"})
	for i := 0; i < numEntries; i++ {
		if err := leader.Propose(cmd); err != nil {
			t.Fatalf("Propose %d: %v", i, err)
		}
	}

	// Verify the leader has committed all entries.
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

	// Phase 5: restart the follower at the same address with a fresh log.
	// Wrap its handler to count how many AppendEntries RPCs it receives.
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
		DataDir:   t.TempDir(), // fresh data dir — follower starts with empty log
	}, applyCh2, proposeCh2, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewNode (restart) %s: %v", followerID, err)
	}

	// Wrap the raft handler to count /raft/append calls.
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

	// Phase 6: wait for the restarted follower to catch up.
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if n2.Status().LastApplied >= numEntries {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if n2.Status().LastApplied < numEntries {
		t.Fatalf("restarted follower lastApplied=%d, want >=%d (appendRPCs=%d)",
			n2.Status().LastApplied, numEntries, atomic.LoadInt64(&appendRPCCount))
	}

	// Assert that catchup required ≤ ceil(numEntries/maxEntriesPerRPC)+1 RPCs.
	// With numEntries=20 and maxEntriesPerRPC=64: ceil(20/64)+1 = 2.
	maxExpected := int64((numEntries+maxEntriesPerRPC-1)/maxEntriesPerRPC) + 1
	got := atomic.LoadInt64(&appendRPCCount)
	if got > maxExpected {
		t.Fatalf("catchup took %d AppendEntries RPCs, want ≤%d (maxEntriesPerRPC=%d)",
			got, maxExpected, maxEntriesPerRPC)
	}
	t.Logf("catchup complete: %d AppendEntries RPCs for %d entries (max allowed: %d)",
		got, numEntries, maxExpected)
}
