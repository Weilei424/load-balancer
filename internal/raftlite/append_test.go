package raftlite

import (
	"os"
	"path/filepath"
	"testing"
)

// TestHandleAppendEntriesDiskAppendFailure verifies that when the disk write
// for a new log entry fails, handleAppendEntries returns Success=false and
// rolls back the in-memory append so memory and disk remain consistent.
func TestHandleAppendEntriesDiskAppendFailure(t *testing.T) {
	n := newTestNode(t, "n1", nil)

	// Seed the log with one entry both in memory and on disk.
	seed := LogEntry{Index: 1, Term: 1}
	if err := appendLogEntryDisk(n.dataDir, seed); err != nil {
		t.Fatalf("setup appendLogEntryDisk: %v", err)
	}
	n.mu.Lock()
	n.ps.Log = []LogEntry{seed}
	n.ps.CurrentTerm = 1
	n.mu.Unlock()

	// Make log.jsonl read-only so the next append fails.
	logPath := filepath.Join(n.dataDir, "log.jsonl")
	if err := os.Chmod(logPath, 0o444); err != nil {
		t.Fatalf("chmod log.jsonl: %v", err)
	}
	defer os.Chmod(logPath, 0o644) //nolint:errcheck

	n.mu.Lock()
	before := len(n.ps.Log)
	reply := n.handleAppendEntries(AppendEntriesArgs{
		Term:         1,
		LeaderID:     "leader",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []LogEntry{{Index: 2, Term: 1}},
	})
	after := len(n.ps.Log)
	n.mu.Unlock()

	if reply.Success {
		t.Fatal("expected Success=false when disk append fails")
	}
	if after != before {
		t.Fatalf("in-memory log should be unchanged: was %d, now %d entries", before, after)
	}
}

// TestHandleAppendEntriesDiskRewriteFailure verifies that when the disk
// rewrite triggered by a conflicting entry fails, handleAppendEntries returns
// Success=false and leaves the in-memory log untouched.
func TestHandleAppendEntriesDiskRewriteFailure(t *testing.T) {
	n := newTestNode(t, "n1", nil)

	// Seed two entries with term=1 in memory and on disk.
	entries := []LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}
	for _, e := range entries {
		if err := appendLogEntryDisk(n.dataDir, e); err != nil {
			t.Fatalf("setup appendLogEntryDisk: %v", err)
		}
	}
	n.mu.Lock()
	n.ps.Log = entries
	n.ps.CurrentTerm = 2
	n.mu.Unlock()

	// Make the data directory read-only so rewriteLogDisk cannot create its
	// temporary file.
	if err := os.Chmod(n.dataDir, 0o555); err != nil {
		t.Fatalf("chmod dataDir: %v", err)
	}
	defer os.Chmod(n.dataDir, 0o755) //nolint:errcheck

	// Send an entry at index 2 with term=2 — this conflicts with term=1 at
	// index 2 and would normally trigger a truncation + rewrite.
	n.mu.Lock()
	before := len(n.ps.Log)
	reply := n.handleAppendEntries(AppendEntriesArgs{
		Term:         2,
		LeaderID:     "leader",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []LogEntry{{Index: 2, Term: 2}},
	})
	after := len(n.ps.Log)
	n.mu.Unlock()

	if reply.Success {
		t.Fatal("expected Success=false when disk rewrite fails")
	}
	if after != before {
		t.Fatalf("in-memory log should be unchanged: was %d, now %d entries", before, after)
	}
}
