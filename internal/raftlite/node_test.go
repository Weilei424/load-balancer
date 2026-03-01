package raftlite

import (
	"math/rand"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func newTestNode(t *testing.T, id string, peers map[string]string) *Node {
	t.Helper()
	dir := t.TempDir()
	applyCh := make(chan LogEntry, 64)
	proposeCh := make(chan ProposeReq, 4)
	n, err := NewNode(Config{
		ID:        id,
		Peers:     peers,
		HTTPPeers: make(map[string]string),
		DataDir:   dir,
	}, applyCh, proposeCh, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return n
}

func TestElectionTimeoutBounds(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	for i := 0; i < 1000; i++ {
		d := randomElectionTimeout(r)
		if d < electionTimeoutMin || d >= electionTimeoutMax {
			t.Fatalf("timeout %v out of bounds [%v, %v)", d, electionTimeoutMin, electionTimeoutMax)
		}
	}
}

func TestVoteGrantedFreshNode(t *testing.T) {
	n := newTestNode(t, "n1", map[string]string{"n2": "http://unused"})
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := n.handleRequestVote(RequestVoteArgs{
		Term:         1,
		CandidateID:  "n2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if !reply.VoteGranted {
		t.Fatal("expected vote to be granted to first requester")
	}
	if reply.Term != 1 {
		t.Fatalf("expected term=1, got %d", reply.Term)
	}
}

func TestVoteDeniedStaleTerm(t *testing.T) {
	n := newTestNode(t, "n1", nil)
	n.mu.Lock()
	n.ps.CurrentTerm = 5
	n.mu.Unlock()

	n.mu.Lock()
	reply := n.handleRequestVote(RequestVoteArgs{
		Term:        3, // stale
		CandidateID: "n2",
	})
	n.mu.Unlock()

	if reply.VoteGranted {
		t.Fatal("vote must not be granted for stale term")
	}
}

func TestVoteDeniedAlreadyVoted(t *testing.T) {
	n := newTestNode(t, "n1", nil)
	n.mu.Lock()
	n.ps.CurrentTerm = 1
	n.ps.VotedFor = "n2"
	n.mu.Unlock()

	n.mu.Lock()
	reply := n.handleRequestVote(RequestVoteArgs{
		Term:        1,
		CandidateID: "n3", // different candidate
	})
	n.mu.Unlock()

	if reply.VoteGranted {
		t.Fatal("vote must not be granted if already voted for someone else")
	}
}

func TestVoteDeniedStaleLog(t *testing.T) {
	n := newTestNode(t, "n1", nil)
	n.mu.Lock()
	// Our log has term=2 at index 5; candidate only has term=1.
	n.ps.Log = []LogEntry{{Index: 5, Term: 2}}
	n.mu.Unlock()

	n.mu.Lock()
	reply := n.handleRequestVote(RequestVoteArgs{
		Term:         2,
		CandidateID:  "n2",
		LastLogIndex: 5,
		LastLogTerm:  1, // candidate log is stale
	})
	n.mu.Unlock()

	if reply.VoteGranted {
		t.Fatal("vote must not be granted when candidate log is stale")
	}
}

func TestStepDownOnHigherTerm(t *testing.T) {
	n := newTestNode(t, "n1", nil)
	n.mu.Lock()
	n.ps.CurrentTerm = 3
	n.role = Leader
	n.mu.Unlock()

	n.mu.Lock()
	n.handleAppendEntries(AppendEntriesArgs{
		Term:     5,
		LeaderID: "n2",
	})
	role := n.role
	term := n.ps.CurrentTerm
	n.mu.Unlock()

	if role != Follower {
		t.Fatalf("expected Follower after step-down, got %s", role)
	}
	if term != 5 {
		t.Fatalf("expected term=5, got %d", term)
	}
}

func TestAppendEntriesAdvancesCommit(t *testing.T) {
	n := newTestNode(t, "n1", nil)
	n.mu.Lock()
	n.ps.Log = []LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
	}
	n.mu.Unlock()

	n.mu.Lock()
	reply := n.handleAppendEntries(AppendEntriesArgs{
		Term:         1,
		LeaderID:     "leader",
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries:      nil,
		LeaderCommit: 2,
	})
	ci := n.vs.CommitIndex
	n.mu.Unlock()

	if !reply.Success {
		t.Fatal("expected success")
	}
	if ci != 2 {
		t.Fatalf("expected commitIndex=2, got %d", ci)
	}
}

func TestBecomeLeaderInitialisesNextIndex(t *testing.T) {
	n := newTestNode(t, "n1", map[string]string{"n2": "http://unused", "n3": "http://unused2"})
	n.mu.Lock()
	n.ps.Log = []LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}
	n.becomeLeader()
	for peer, ni := range n.vs.NextIndex {
		if ni != 3 {
			t.Errorf("peer %s nextIndex=%d, want 3", peer, ni)
		}
	}
	n.mu.Unlock()
}

func TestHeartbeatIntervalInRange(t *testing.T) {
	if heartbeatInterval >= electionTimeoutMin/2 {
		t.Fatalf("heartbeat (%v) must be well below election timeout min (%v)", heartbeatInterval, electionTimeoutMin)
	}
}

func TestLogTruncation(t *testing.T) {
	log := []LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
	}
	trimmed := truncateFrom(log, 3)
	if len(trimmed) != 2 {
		t.Fatalf("expected 2 entries after truncation from 3, got %d", len(trimmed))
	}
	if trimmed[1].Index != 2 {
		t.Fatalf("expected last index=2, got %d", trimmed[1].Index)
	}
}

func TestPersistenceRoundTrip(t *testing.T) {
	dir := t.TempDir()

	// Write.
	if err := saveMeta(dir, 7, "n3"); err != nil {
		t.Fatalf("saveMeta: %v", err)
	}
	entries := []LogEntry{{Index: 1, Term: 7}, {Index: 2, Term: 7}}
	for _, e := range entries {
		if err := appendLogEntryDisk(dir, e); err != nil {
			t.Fatalf("appendLogEntry: %v", err)
		}
	}

	// Read back.
	term, voted, err := loadMeta(dir)
	if err != nil || term != 7 || voted != "n3" {
		t.Fatalf("loadMeta: term=%d voted=%s err=%v", term, voted, err)
	}
	loaded, err := loadLogDisk(dir)
	if err != nil || len(loaded) != 2 {
		t.Fatalf("loadLog: len=%d err=%v", len(loaded), err)
	}
}

func TestNodeStopsCleanly(t *testing.T) {
	n := newTestNode(t, "n1", nil)
	done := make(chan struct{})
	go func() {
		n.Run()
		close(done)
	}()
	n.Stop()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("node did not stop within 2s")
	}
}
