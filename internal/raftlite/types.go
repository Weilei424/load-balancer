// Package raftlite is an in-repo minimal Raft consensus implementation.
// It covers leader election, heartbeats, and log replication for config commands.
package raftlite

import "encoding/json"

// Role represents a Raft node role.
type Role int8

const (
	Follower  Role = iota
	Candidate Role = iota
	Leader    Role = iota
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// LogEntry is a single Raft log entry.
type LogEntry struct {
	Index   int             `json:"index"`
	Term    int             `json:"term"`
	Command json.RawMessage `json:"command"`
}

// PersistentState fields are durably stored before responding to RPCs.
// The Log is stored separately in log.jsonl; Meta (term/vote) in state.json.
type PersistentState struct {
	CurrentTerm int
	VotedFor    string
	Log         []LogEntry
}

// VolatileState is kept in memory only (reset on restart).
type VolatileState struct {
	CommitIndex int
	LastApplied int
	// Leader-only: per-peer replication state.
	NextIndex  map[string]int
	MatchIndex map[string]int
}

// Snapshot holds a point-in-time compaction of the log.
type Snapshot struct {
	LastIncludedIndex int    `json:"last_included_index"`
	LastIncludedTerm  int    `json:"last_included_term"`
	Data              []byte `json:"data"` // JSON-marshalled state machine
}

// InstallSnapshotArgs is the argument struct for InstallSnapshot RPC.
type InstallSnapshotArgs struct {
	Term              int    `json:"term"`
	LeaderID          string `json:"leader_id"`
	LastIncludedIndex int    `json:"last_included_index"`
	LastIncludedTerm  int    `json:"last_included_term"`
	Data              []byte `json:"data"`
}

// InstallSnapshotReply is the response for InstallSnapshot RPC.
type InstallSnapshotReply struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}

// Config holds configuration for creating a raftlite Node.
type Config struct {
	ID        string
	Peers     map[string]string // peerID → raft base URL (e.g. "http://127.0.0.1:10001")
	HTTPPeers map[string]string // peerID → HTTP base URL (e.g. "http://127.0.0.1:9001")
	DataDir   string
	// Snapshotter is called to serialize the state machine when a snapshot is triggered.
	// Nil disables auto-snapshotting.
	Snapshotter func() []byte
	// SnapshotThreshold triggers a snapshot when len(log) exceeds this value. Default: 1000.
	SnapshotThreshold int
}

// RequestVoteArgs is the argument struct for RequestVote RPC.
type RequestVoteArgs struct {
	Term         int    `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex int    `json:"last_log_index"`
	LastLogTerm  int    `json:"last_log_term"`
}

// RequestVoteReply is the response for RequestVote RPC.
type RequestVoteReply struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

// AppendEntriesArgs is the argument struct for AppendEntries RPC.
type AppendEntriesArgs struct {
	Term         int        `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex int        `json:"prev_log_index"`
	PrevLogTerm  int        `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leader_commit"`
}

// AppendEntriesReply is the response for AppendEntries RPC.
type AppendEntriesReply struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
	// ConflictIndex/ConflictTerm implement the fast-rollback optimization from Raft §5.3.
	ConflictIndex int `json:"conflict_index,omitempty"`
	ConflictTerm  int `json:"conflict_term,omitempty"`
}

// StatusReply is returned by GET /raft/status.
type StatusReply struct {
	ID          string `json:"id"`
	Role        string `json:"role"`
	Term        int    `json:"term"`
	LeaderID    string `json:"leader_id"`
	CommitIndex int    `json:"commit_index"`
	LastApplied int    `json:"last_applied"`
	LogLength   int    `json:"log_length"`
}

// ProposeReq is sent from external callers to the raft node via proposeCh.
type ProposeReq struct {
	Command json.RawMessage
	ReplyCh chan error
}
