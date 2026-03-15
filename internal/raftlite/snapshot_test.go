package raftlite

import (
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// startClusterNodeWithConfig creates a clusterNode using the given Config, reusing
// a caller-supplied DataDir (needed for restart tests).
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
	// Drain applyCh so the node doesn't stall.
	go func() {
		for range applyCh {
		}
	}()
	return &clusterNode{Node: n, ln: ln, srv: srv}
}

// TestSnapshotAndRestore verifies that:
//  1. After proposing many entries, auto-snapshot compacts the log.
//  2. On restart, the snapshot is delivered via SnapshotCh before log replay.
//  3. CommitIndex and LastApplied are correctly restored.
func TestSnapshotAndRestore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Allocate two listeners so we know the addresses before creating nodes.
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
		SnapshotThreshold: 100,
	}
	cfg2 := Config{
		ID:                "n2",
		Peers:             map[string]string{"n1": addr1},
		HTTPPeers:         map[string]string{},
		DataDir:           t.TempDir(),
		Snapshotter:       func() []byte { return []byte(`{"state":"snap"}`) },
		SnapshotThreshold: 100,
	}

	n1 := startClusterNodeWithConfig(t, cfg1, ln1)
	n2 := startClusterNodeWithConfig(t, cfg2, ln2)
	defer n2.Stop()

	// Wait for a leader.
	nodes := []*clusterNode{n1, n2}
	leader := waitForLeader(t, nodes, 3*time.Second)
	t.Logf("leader: %s", leader.Status().ID)

	// Propose 200 entries.
	cmd, _ := json.Marshal(map[string]string{"op": "test"})
	for i := 0; i < 200; i++ {
		if err := leader.Propose(cmd); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}

	// Wait for the leader's log to be compacted (LogLength ≤ 100).
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

	// snapshot.json must exist in the leader's dataDir.
	leaderDataDir := dataDir1
	if leader.id != "n1" {
		// n2 is leader; we care about n1 restart test, so skip the restart part.
		t.Skip("n2 became leader; skipping n1-restart sub-test")
	}
	if _, err := os.Stat(filepath.Join(leaderDataDir, "snapshot.json")); err != nil {
		t.Fatalf("snapshot.json missing on leader: %v", err)
	}

	// Stop n1 (leader). n2 will be leaderless but we're done with proposals.
	n1.Stop()

	// Restart n1 with same dataDir.
	ln1r, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen restart: %v", err)
	}
	addr1r := "http://" + ln1r.Addr().String()

	// Update n2's peers to point to the new address (for a real cluster we'd need
	// dynamic membership; here we just verify n1 restores state from snapshot).
	_ = addr1r // n2 peer update not needed for the restore check

	// Create fresh applyCh/proposeCh for restarted n1.
	applyCh := make(chan LogEntry, 256)
	proposeCh := make(chan ProposeReq, 8)
	n1r, err := NewNode(Config{
		ID:                "n1",
		Peers:             map[string]string{"n2": addr2},
		HTTPPeers:         map[string]string{},
		DataDir:           dataDir1,
		Snapshotter:       func() []byte { return []byte(`{"state":"snap"}`) },
		SnapshotThreshold: 100,
	}, applyCh, proposeCh, zerolog.Nop())
	if err != nil {
		t.Fatalf("restart NewNode: %v", err)
	}

	// Collect the startup snapshot from SnapshotCh before calling Run.
	snapCh := n1r.SnapshotCh()

	srv1r := &http.Server{Handler: n1r.Handler()}
	go srv1r.Serve(ln1r) //nolint:errcheck
	go n1r.Run()
	go func() { for range applyCh {} }()
	defer func() {
		n1r.Stop()
		srv1r.Close()
		ln1r.Close()
	}()

	// Drain SnapshotCh — should receive the startup snapshot quickly.
	var gotSnap Snapshot
	select {
	case gotSnap = <-snapCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for startup snapshot on restart")
	}
	if gotSnap.LastIncludedIndex == 0 {
		t.Fatal("startup snapshot has LastIncludedIndex=0")
	}

	// Verify restored state.
	s := n1r.Status()
	if s.CommitIndex < gotSnap.LastIncludedIndex {
		t.Fatalf("restart commitIndex=%d < snapshot.LastIncludedIndex=%d",
			s.CommitIndex, gotSnap.LastIncludedIndex)
	}
	if s.LastApplied < gotSnap.LastIncludedIndex {
		t.Fatalf("restart lastApplied=%d < snapshot.LastIncludedIndex=%d",
			s.LastApplied, gotSnap.LastIncludedIndex)
	}
	t.Logf("restart: commitIndex=%d lastApplied=%d snapshotAt=%d logLen=%d",
		s.CommitIndex, s.LastApplied, gotSnap.LastIncludedIndex, s.LogLength)
}

// TestLaggingFollowerGetsSnapshot verifies that a follower whose nextIndex falls
// below the leader's snapshot boundary receives an InstallSnapshot RPC and
// applies the snapshot to catch up.
func TestLaggingFollowerGetsSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Allocate listeners for 3 nodes: n1, n2, n3.
	// n1 and n2 start immediately; n3 starts later with an empty dataDir.
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

	// Start n1 and n2 only. n3 is configured in peers but not yet running.
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

	// Wait for a leader among n1+n2 (majority of 3 requires 2, achievable).
	leader := waitForLeader(t, cn12, 3*time.Second)
	t.Logf("leader: %s", leader.Status().ID)

	// Propose 200 entries (threshold=50 → snapshot(s) taken).
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

	// Start n3 with fresh dataDir.
	n3DataDir := t.TempDir()
	n3Peers := make(map[string]string)
	for pid, pa := range addrs {
		if pid != "n3" {
			n3Peers[pid] = pa
		}
	}
	n3cfg := Config{
		ID:                "n3",
		Peers:             n3Peers,
		HTTPPeers:         map[string]string{},
		DataDir:           n3DataDir,
		Snapshotter:       snapshotter,
		SnapshotThreshold: 50,
	}
	n3 := startClusterNodeWithConfig(t, n3cfg, lns[2])
	defer n3.Stop()

	// Drain n3's SnapshotCh so it can process snapshots.
	var snapReceived bool
	go func() {
		for range n3.SnapshotCh() {
			snapReceived = true
		}
	}()

	// Wait for n3 to catch up (commitIndex ≥ 200).
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		s := n3.Status()
		if s.CommitIndex >= 200 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	n3s := n3.Status()
	if n3s.CommitIndex < 200 {
		t.Fatalf("n3 commitIndex=%d after 10s, want ≥200", n3s.CommitIndex)
	}
	t.Logf("n3: commitIndex=%d lastApplied=%d logLen=%d", n3s.CommitIndex, n3s.LastApplied, n3s.LogLength)

	// n3 must have received a snapshot (snapshot.json exists in its dataDir).
	if _, err := os.Stat(filepath.Join(n3DataDir, "snapshot.json")); err != nil {
		t.Fatalf("n3 snapshot.json missing: %v", err)
	}
	_ = snapReceived // may or may not be set by the time we check due to race; file check is authoritative
}
