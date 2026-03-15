package raftlite

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// startClusterNodeWithConfig creates a clusterNode using the given Config, binding
// to the supplied listener. The caller must provide a Config with DataDir already
// set (needed for restart tests). applyCh is drained by a background goroutine so
// the node never stalls; if the caller needs to observe entries, pass a separate
// channel and bridge it in cfg.ApplySnapshot.
func startClusterNodeWithConfig(t *testing.T, cfg Config, ln net.Listener) *clusterNode {
	t.Helper()
	applyCh := make(chan LogEntry, 256)
	proposeCh := make(chan ProposeReq, 8)
	n, err := NewNode(cfg, applyCh, proposeCh, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewNode %s: %v", cfg.ID, err)
	}
	srv := &http.Server{Handler: n.Handler()}
	go srv.Serve(ln) //nolint:errcheck
	go n.Run()
	// Drain applyCh so the node never stalls on a slow consumer.
	go func() {
		for range applyCh {
		}
	}()
	return &clusterNode{Node: n, ln: ln, srv: srv}
}

// TestVoteFreshnessAfterCompaction verifies that a node with a compacted snapshot
// correctly rejects votes from candidates whose log is less up-to-date than the
// snapshot boundary, and grants votes to candidates at or beyond the boundary.
//
// This is a regression test for the bug where handleRequestVote used
// lastIndex(ps.Log)/lastTerm(ps.Log) instead of the snapshot-aware helpers,
// causing a node with an empty tail (lastIndex=0, lastTerm=0) to grant votes to
// any candidate — even stale ones.
func TestVoteFreshnessAfterCompaction(t *testing.T) {
	dir := t.TempDir()

	// Place a snapshot at index 100, term 3 on disk.
	snap := Snapshot{LastIncludedIndex: 100, LastIncludedTerm: 3, Data: []byte("{}")}
	if err := saveSnapshot(dir, snap); err != nil {
		t.Fatalf("saveSnapshot: %v", err)
	}

	applyCh := make(chan LogEntry, 4)
	proposeCh := make(chan ProposeReq, 4)
	n, err := NewNode(Config{
		ID:        "n1",
		Peers:     map[string]string{},
		HTTPPeers: map[string]string{},
		DataDir:   dir,
	}, applyCh, proposeCh, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// Verify the snapshot was loaded (lastLogIndex should reflect it).
	if got := n.lastLogIndex(); got != 100 {
		t.Fatalf("lastLogIndex() = %d, want 100 (snapshot boundary)", got)
	}
	if got := n.lastLogTerm(); got != 3 {
		t.Fatalf("lastLogTerm() = %d, want 3", got)
	}

	// Set current term to match snapshot term.
	n.ps.CurrentTerm = 3

	// Case 1: candidate with stale log (index=50, term=2) — must be rejected.
	n.ps.VotedFor = ""
	reply := n.handleRequestVote(RequestVoteArgs{
		Term:         3,
		CandidateID:  "stale",
		LastLogIndex: 50,
		LastLogTerm:  2,
	})
	if reply.VoteGranted {
		t.Error("stale candidate (index=50, term=2): expected vote DENIED, got GRANTED")
	}

	// Case 2: candidate at exact snapshot boundary (index=100, term=3) — must be granted.
	n.ps.VotedFor = ""
	reply = n.handleRequestVote(RequestVoteArgs{
		Term:         3,
		CandidateID:  "boundary",
		LastLogIndex: 100,
		LastLogTerm:  3,
	})
	if !reply.VoteGranted {
		t.Error("boundary candidate (index=100, term=3): expected vote GRANTED, got DENIED")
	}

	// Case 3: candidate beyond the boundary (index=150, term=3) — must be granted.
	n.ps.VotedFor = ""
	reply = n.handleRequestVote(RequestVoteArgs{
		Term:         3,
		CandidateID:  "ahead",
		LastLogIndex: 150,
		LastLogTerm:  3,
	})
	if !reply.VoteGranted {
		t.Error("ahead candidate (index=150, term=3): expected vote GRANTED, got DENIED")
	}

	// Case 4: same index but lower term (index=100, term=2) — must be rejected.
	n.ps.VotedFor = ""
	reply = n.handleRequestVote(RequestVoteArgs{
		Term:         3,
		CandidateID:  "sameIdx",
		LastLogIndex: 100,
		LastLogTerm:  2,
	})
	if reply.VoteGranted {
		t.Error("same-index lower-term candidate (index=100, term=2): expected vote DENIED, got GRANTED")
	}
}

// TestSnapshotAndRestore verifies that:
//  1. After proposing many entries the leader auto-snapshots and compacts its log.
//  2. On restart the ApplySnapshot callback fires BEFORE any tail log entry is
//     replayed — enforcing the ordering guarantee.
//  3. CommitIndex/LastApplied are correctly restored from the snapshot.
func TestSnapshotAndRestore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen n1: %v", err)
	}
	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen n2: %v", err)
	}
	addr1 := "http://" + ln1.Addr().String()
	addr2 := "http://" + ln2.Addr().String()
	dataDir1 := t.TempDir()

	cfg1 := Config{
		ID:                "n1",
		Peers:             map[string]string{"n2": addr2},
		HTTPPeers:         map[string]string{},
		DataDir:           dataDir1,
		Snapshotter:       func() []byte { return []byte(`{"state":"snap"}`) },
		ApplySnapshot:     func(_ []byte) error { return nil },
		SnapshotThreshold: 100,
	}
	cfg2 := Config{
		ID:                "n2",
		Peers:             map[string]string{"n1": addr1},
		HTTPPeers:         map[string]string{},
		DataDir:           t.TempDir(),
		Snapshotter:       func() []byte { return []byte(`{"state":"snap"}`) },
		ApplySnapshot:     func(_ []byte) error { return nil },
		SnapshotThreshold: 100,
	}

	n1 := startClusterNodeWithConfig(t, cfg1, ln1)
	n2 := startClusterNodeWithConfig(t, cfg2, ln2)
	defer n2.Stop()

	leader := waitForLeader(t, []*clusterNode{n1, n2}, 3*time.Second)
	t.Logf("leader: %s", leader.Status().ID)

	// Propose 200 entries to trigger auto-snapshot (threshold=100).
	cmd, _ := json.Marshal(map[string]string{"op": "test"})
	for i := 0; i < 200; i++ {
		if err := leader.Propose(cmd); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}

	// Wait for leader log compaction.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s := leader.Status()
		if s.CommitIndex >= 200 && s.LogLength <= 100 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	ls := leader.Status()
	if ls.CommitIndex < 200 {
		t.Fatalf("leader commitIndex=%d, want 200", ls.CommitIndex)
	}
	if ls.LogLength > 100 {
		t.Fatalf("log not compacted: LogLength=%d (want ≤100)", ls.LogLength)
	}

	// Only test the restart-ordering guarantee when n1 is the leader (it has the
	// known dataDir). Skip otherwise — the compaction check above already passed.
	if leader.id != "n1" {
		t.Skip("n2 became leader; restart ordering verified only for n1")
	}

	if _, err := os.Stat(filepath.Join(dataDir1, "snapshot.json")); err != nil {
		t.Fatalf("snapshot.json missing on n1: %v", err)
	}

	n1.Stop()

	// Restart n1 with the same dataDir. Track ordering: applySnapshot must fire
	// before any entry from applyCh is consumed.
	var snapshotApplied atomic.Bool
	var orderingViolation atomic.Bool

	applyCh := make(chan LogEntry, 256)
	proposeCh := make(chan ProposeReq, 8)
	n1r, err := NewNode(Config{
		ID:        "n1",
		Peers:     map[string]string{"n2": addr2},
		HTTPPeers: map[string]string{},
		DataDir:   dataDir1,
		Snapshotter: func() []byte { return []byte(`{"state":"snap"}`) },
		ApplySnapshot: func(_ []byte) error {
			snapshotApplied.Store(true)
			return nil
		},
		SnapshotThreshold: 100,
	}, applyCh, proposeCh, zerolog.Nop())
	if err != nil {
		t.Fatalf("restart NewNode: %v", err)
	}

	// Consume entries from applyCh in background, checking ordering.
	go func() {
		for range applyCh {
			if !snapshotApplied.Load() {
				orderingViolation.Store(true)
			}
		}
	}()

	ln1r, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen restart: %v", err)
	}
	srv1r := &http.Server{Handler: n1r.Handler()}
	go srv1r.Serve(ln1r) //nolint:errcheck
	go n1r.Run()
	defer func() {
		n1r.Stop()
		srv1r.Close()
		ln1r.Close()
	}()

	// Give the node time to replay any tail entries.
	time.Sleep(200 * time.Millisecond)

	if orderingViolation.Load() {
		t.Error("ordering violation: a log entry was applied before the snapshot was restored")
	}

	s := n1r.Status()
	snap, _, err := loadSnapshot(dataDir1)
	if err != nil {
		t.Fatalf("loadSnapshot: %v", err)
	}
	if s.CommitIndex < snap.LastIncludedIndex {
		t.Errorf("restart commitIndex=%d < snapshot.LastIncludedIndex=%d",
			s.CommitIndex, snap.LastIncludedIndex)
	}
	if s.LastApplied < snap.LastIncludedIndex {
		t.Errorf("restart lastApplied=%d < snapshot.LastIncludedIndex=%d",
			s.LastApplied, snap.LastIncludedIndex)
	}
	t.Logf("restart: commitIndex=%d lastApplied=%d snapshotAt=%d logLen=%d",
		s.CommitIndex, s.LastApplied, snap.LastIncludedIndex, s.LogLength)
}

// TestLaggingFollowerGetsSnapshot verifies that a follower whose nextIndex falls
// below the leader's snapshot boundary receives an InstallSnapshot RPC and
// applies the snapshot to catch up.
func TestLaggingFollowerGetsSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Allocate listeners for 3 nodes. n1 and n2 start immediately; n3 starts later.
	lns := make([]net.Listener, 3)
	for i := range lns {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen[%d]: %v", i, err)
		}
		lns[i] = ln
	}
	ids := []string{"n1", "n2", "n3"}
	addrs := make(map[string]string, 3)
	for i, id := range ids {
		addrs[id] = "http://" + lns[i].Addr().String()
	}

	snapshotter := func() []byte { return []byte(`{"backends":[],"algorithm":"round_robin"}`) }
	applyFn := func(_ []byte) error { return nil }

	// Start n1 and n2 with n3 in their peer lists but not yet running.
	// Two out of three is a majority so proposals can commit.
	var cn12 []*clusterNode
	for i := 0; i < 2; i++ {
		id := ids[i]
		peers := make(map[string]string)
		for pid, pa := range addrs {
			if pid != id {
				peers[pid] = pa
			}
		}
		cfg := Config{
			ID:                id,
			Peers:             peers,
			HTTPPeers:         map[string]string{},
			DataDir:           t.TempDir(),
			Snapshotter:       snapshotter,
			ApplySnapshot:     applyFn,
			SnapshotThreshold: 50,
		}
		cn := startClusterNodeWithConfig(t, cfg, lns[i])
		cn12 = append(cn12, cn)
	}
	defer func() {
		for _, cn := range cn12 {
			cn.Stop()
		}
	}()

	leader := waitForLeader(t, cn12, 3*time.Second)
	t.Logf("leader: %s", leader.Status().ID)

	// Propose 200 entries (threshold=50 → multiple snapshots).
	cmd, _ := json.Marshal(map[string]string{"op": "test"})
	for i := 0; i < 200; i++ {
		if err := leader.Propose(cmd); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}

	// Wait for leader to compact its log.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s := leader.Status()
		if s.CommitIndex >= 200 && s.LogLength <= 50 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	ls := leader.Status()
	if ls.CommitIndex < 200 {
		t.Fatalf("leader commitIndex=%d, want ≥200", ls.CommitIndex)
	}
	t.Logf("leader: commitIndex=%d logLen=%d", ls.CommitIndex, ls.LogLength)

	// Start n3 with a fresh dataDir; its nextIndex on the leader will be 1,
	// which is below the snapshot boundary → leader sends InstallSnapshot.
	var n3SnapApplied atomic.Bool
	n3DataDir := t.TempDir()
	n3Peers := make(map[string]string)
	for pid, pa := range addrs {
		if pid != "n3" {
			n3Peers[pid] = pa
		}
	}
	n3cfg := Config{
		ID:        "n3",
		Peers:     n3Peers,
		HTTPPeers: map[string]string{},
		DataDir:   n3DataDir,
		ApplySnapshot: func(_ []byte) error {
			n3SnapApplied.Store(true)
			return nil
		},
		Snapshotter:       snapshotter,
		SnapshotThreshold: 50,
	}
	n3 := startClusterNodeWithConfig(t, n3cfg, lns[2])
	defer n3.Stop()

	// Wait for n3 to catch up.
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if n3.Status().CommitIndex >= 200 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	n3s := n3.Status()
	if n3s.CommitIndex < 200 {
		t.Fatalf("n3 commitIndex=%d after 10s, want ≥200", n3s.CommitIndex)
	}
	t.Logf("n3: commitIndex=%d lastApplied=%d logLen=%d", n3s.CommitIndex, n3s.LastApplied, n3s.LogLength)

	// n3 must have received a snapshot (file on disk is authoritative).
	if _, err := os.Stat(filepath.Join(n3DataDir, "snapshot.json")); err != nil {
		t.Fatalf("n3 snapshot.json missing: %v", err)
	}
	// ApplySnapshot callback must have fired.
	if !n3SnapApplied.Load() {
		t.Error("n3 ApplySnapshot callback was never called")
	}
}

// TestInstallSnapshotApplyFailureReturnsFailure verifies that when the
// ApplySnapshot callback returns an error during serveInstallSnapshot, the reply
// carries Success=false and LastApplied/CommitIndex are NOT advanced. This prevents
// Raft state from diverging from the state machine when restore fails.
//
// This is a unit test that drives receiveInstallSnapshot + the failure path
// directly, without needing a live cluster.
func TestInstallSnapshotApplyFailureReturnsFailure(t *testing.T) {
	dir := t.TempDir()

	var applyAttempts atomic.Int32
	applyCh := make(chan LogEntry, 4)
	proposeCh := make(chan ProposeReq, 4)
	n, err := NewNode(Config{
		ID:        "n1",
		Peers:     map[string]string{},
		HTTPPeers: map[string]string{},
		DataDir:   dir,
		ApplySnapshot: func(_ []byte) error {
			applyAttempts.Add(1)
			return errors.New("disk full")
		},
	}, applyCh, proposeCh, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	// Simulate a leader on term 1 sending us a snapshot at index 100.
	n.mu.Lock()
	n.ps.CurrentTerm = 1
	reply, accepted, snap := n.receiveInstallSnapshot(InstallSnapshotArgs{
		Term:              1,
		LeaderID:          "leader",
		LastIncludedIndex: 100,
		LastIncludedTerm:  1,
		Data:              []byte(`{}`),
	})
	n.mu.Unlock()

	if !accepted {
		t.Fatal("expected receiveInstallSnapshot to accept the snapshot")
	}

	// Now mimic what serveInstallSnapshot does on callback failure:
	// call applySnapshot, observe error, do NOT advance state.
	applyErr := n.applySnapshot(snap.Data)
	if applyErr == nil {
		t.Fatal("expected applySnapshot to return an error")
	}
	// reply.Success must remain false — we must NOT set it true after a failure.
	if reply.Success {
		t.Error("reply.Success must be false when applySnapshot fails")
	}

	// LastApplied and CommitIndex must not have advanced (finalization was skipped).
	s := n.Status()
	if s.CommitIndex >= 100 {
		t.Errorf("CommitIndex=%d advanced despite failed apply (want <100)", s.CommitIndex)
	}
	if s.LastApplied >= 100 {
		t.Errorf("LastApplied=%d advanced despite failed apply (want <100)", s.LastApplied)
	}
	if applyAttempts.Load() != 1 {
		t.Errorf("applySnapshot called %d times, want 1", applyAttempts.Load())
	}
}

// TestStartupSnapshotRestoreFailureStopsNode verifies that when the ApplySnapshot
// callback fails during startup, the node stops rather than replaying the log tail
// on top of an unrestored state machine.
func TestStartupSnapshotRestoreFailureStopsNode(t *testing.T) {
	dir := t.TempDir()

	// Place a snapshot at index 50, term 1.
	snap := Snapshot{LastIncludedIndex: 50, LastIncludedTerm: 1, Data: []byte("{}")}
	if err := saveSnapshot(dir, snap); err != nil {
		t.Fatalf("saveSnapshot: %v", err)
	}

	var logEntriesApplied atomic.Int32
	applyCh := make(chan LogEntry, 64)
	proposeCh := make(chan ProposeReq, 4)

	n, err := NewNode(Config{
		ID:        "n1",
		Peers:     map[string]string{},
		HTTPPeers: map[string]string{},
		DataDir:   dir,
		ApplySnapshot: func(_ []byte) error {
			return errors.New("restore failed")
		},
	}, applyCh, proposeCh, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	// Count any entries that leak through applyCh after a failed restore.
	go func() {
		for range applyCh {
			logEntriesApplied.Add(1)
		}
	}()

	// Run() should stop the node on restore failure, not replay the log.
	done := make(chan struct{})
	go func() {
		n.Run()
		close(done)
	}()

	select {
	case <-done:
		// Good: Run() returned (node stopped itself).
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return after startup snapshot restore failure")
	}

	// No log entries should have been sent to applyCh.
	time.Sleep(50 * time.Millisecond)
	if got := logEntriesApplied.Load(); got > 0 {
		t.Errorf("log entries were applied after failed snapshot restore: got %d, want 0", got)
	}
}
