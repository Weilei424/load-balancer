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

// runBatchCatchupTest verifies that when a follower connects for the first time
// to a leader that already has numEntries in its log, it catches up in
// ≤ maxRPCs AppendEntries RPCs. This is the "late first connect" scenario from
// upgrade-plan.md Step 1.
//
// Cluster: 2 nodes. n1 is pre-loaded with numEntries log entries and set to
// Leader before n2 starts. n2 then connects and receives batched replication.
func runBatchCatchupTest(t *testing.T, numEntries, maxRPCs int) {
	t.Helper()

	// Allocate both listeners before creating nodes so each peer knows the
	// other's address from the start.
	n1Ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen n1: %v", err)
	}
	n2Ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen n2: %v", err)
	}
	n1Addr := "http://" + n1Ln.Addr().String()
	n2Addr := "http://" + n2Ln.Addr().String()

	// ── n1: leader pre-loaded with entries ─────────────────────────────────

	// Use a buffer larger than numEntries so the applier goroutine never
	// blocks on a full channel during the test.
	applyCh1 := make(chan LogEntry, numEntries+16)
	proposeCh1 := make(chan ProposeReq, 4)
	n1, err := NewNode(Config{
		ID:        "n1",
		Peers:     map[string]string{"n2": n2Addr},
		HTTPPeers: make(map[string]string),
		DataDir:   t.TempDir(),
	}, applyCh1, proposeCh1, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewNode n1: %v", err)
	}

	// Directly write numEntries into n1's log and crown it leader, all before
	// n2 has started. This is the "entries exist before follower connects" path.
	cmd, _ := json.Marshal(map[string]string{"op": "test"})
	n1.mu.Lock()
	n1.ps.CurrentTerm = 1
	_ = saveMeta(n1.dataDir, 1, "n1")
	for i := 1; i <= numEntries; i++ {
		entry := LogEntry{Index: i, Term: 1, Command: cmd}
		n1.ps.Log = append(n1.ps.Log, entry)
		if err := appendLogEntryDisk(n1.dataDir, entry); err != nil {
			n1.mu.Unlock()
			t.Fatalf("appendLogEntryDisk %d: %v", i, err)
		}
	}
	// Set leader state manually (bypasses becomeLeader's sendHeartbeats call
	// so no premature RPC goroutines race against the not-yet-started n2).
	n1.role = Leader
	n1.leaderID = "n1"
	// n2 has no entries yet; replication must start from index 1.
	n1.vs.NextIndex["n2"] = 1
	n1.vs.MatchIndex["n2"] = 0
	n1.mu.Unlock()

	srv1 := &http.Server{Handler: n1.Handler()}
	go srv1.Serve(n1Ln) //nolint:errcheck
	go n1.Run()
	defer func() {
		n1.Stop()
		srv1.Close()
		n1Ln.Close()
	}()
	// Drain n1's apply channel so the applier goroutine never blocks.
	go func() {
		for range applyCh1 {
		}
	}()

	// ── n2: follower that connects late ────────────────────────────────────

	var appendRPCCount int64

	applyCh2 := make(chan LogEntry, numEntries+16)
	proposeCh2 := make(chan ProposeReq, 4)
	n2, err := NewNode(Config{
		ID:        "n2",
		Peers:     map[string]string{"n1": n1Addr},
		HTTPPeers: make(map[string]string),
		DataDir:   t.TempDir(),
	}, applyCh2, proposeCh2, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewNode n2: %v", err)
	}
	// Drain n2's apply channel so the applier goroutine never blocks.
	go func() {
		for range applyCh2 {
		}
	}()

	// Wrap n2's handler to count every /raft/append call.
	base := n2.Handler()
	counting := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/raft/append" {
			atomic.AddInt64(&appendRPCCount, 1)
		}
		base.ServeHTTP(w, r)
	})

	srv2 := &http.Server{Handler: counting}
	go srv2.Serve(n2Ln) //nolint:errcheck
	go n2.Run()
	defer func() {
		n2.Stop()
		srv2.Close()
		n2Ln.Close()
	}()

	// Wait for n2 to apply all numEntries. n1's heartbeat (50 ms cadence)
	// carries batches of up to maxEntriesPerRPC entries; n2's election timer
	// (150–300 ms) won't fire before the first heartbeat arrives.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if n2.Status().LastApplied >= numEntries {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	got := atomic.LoadInt64(&appendRPCCount)
	if n2.Status().LastApplied < numEntries {
		t.Fatalf("n2 lastApplied=%d, want >=%d (appendRPCs=%d)",
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
// RPCs. This exercises the batch-splitting path (100 > maxEntriesPerRPC=64) so at
// least two non-empty payload batches are required.
func TestBatchAppendEntriesSplit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	const numEntries = 100
	// ceil(100/64)+1 = 2+1 = 3: two payload batches plus one commit-advance heartbeat.
	runBatchCatchupTest(t, numEntries, 3)
}
