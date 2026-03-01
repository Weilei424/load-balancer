package raftlite

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// Node is a raftlite consensus node.
type Node struct {
	id        string
	dataDir   string
	peers     map[string]string // peerID → raft base URL
	httpPeers map[string]string // peerID → HTTP base URL (for admin redirect)

	mu       sync.Mutex
	ps       PersistentState
	vs       VolatileState
	role     Role
	leaderID string

	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker

	applyCh   chan LogEntry   // committed entries → state machine consumer
	proposeCh chan ProposeReq // external Propose() callers → Run() loop

	// notifyCh signals the applier goroutine that commitIndex advanced.
	// Buffered(1) so maybeApply callers never block.
	notifyCh chan struct{}

	// pending maps log index → reply channel for blocked Propose() callers.
	pending map[int]chan error

	stopCh chan struct{}
	log    zerolog.Logger
}

// NewNode creates and initialises a raftlite Node (does not start it).
// applyCh and proposeCh are owned by the caller.
func NewNode(cfg Config, applyCh chan LogEntry, proposeCh chan ProposeReq, logger zerolog.Logger) (*Node, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir dataDir: %w", err)
	}

	term, votedFor, err := loadMeta(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("load meta: %w", err)
	}
	entries, err := loadLogDisk(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("load log: %w", err)
	}
	commitIdx, err := loadCommit(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("load commit: %w", err)
	}
	// Clamp to the actual log length in case the log was truncated after the commit was persisted.
	if commitIdx > lastIndex(entries) {
		commitIdx = lastIndex(entries)
	}

	n := &Node{
		id:        cfg.ID,
		dataDir:   cfg.DataDir,
		peers:     cfg.Peers,
		httpPeers: cfg.HTTPPeers,
		ps: PersistentState{
			CurrentTerm: term,
			VotedFor:    votedFor,
			Log:         entries,
		},
		vs: VolatileState{
			CommitIndex: commitIdx,
			NextIndex:   make(map[string]int),
			MatchIndex:  make(map[string]int),
		},
		role:      Follower,
		applyCh:   applyCh,
		proposeCh: proposeCh,
		notifyCh:  make(chan struct{}, 1),
		pending:   make(map[int]chan error),
		stopCh:    make(chan struct{}),
		log:       logger,
	}
	n.heartbeatTicker = time.NewTicker(heartbeatInterval)
	n.electionTimer = time.NewTimer(randomElectionTimeout())
	return n, nil
}

// Run starts the Raft main event loop and the applier goroutine.
// Blocks until Stop() is called.
func (n *Node) Run() {
	n.log.Info().Str("role", n.role.String()).Msg("raft node starting")
	go n.runApplier()
	// Replay any committed-but-not-yet-applied entries that survived restart.
	n.signalApplier()

	for {
		select {
		case <-n.stopCh:
			n.log.Info().Msg("raft node stopping")
			return

		case <-n.electionTimer.C:
			n.mu.Lock()
			if n.role != Leader {
				n.startElection()
			}
			n.mu.Unlock()

		case <-n.heartbeatTicker.C:
			n.mu.Lock()
			if n.role == Leader {
				n.sendHeartbeats()
			}
			n.mu.Unlock()

		case req := <-n.proposeCh:
			n.mu.Lock()
			err := n.appendProposal(req.Command)
			if err != nil {
				n.mu.Unlock()
				req.ReplyCh <- err
				continue
			}
			idx := lastIndex(n.ps.Log)
			n.pending[idx] = req.ReplyCh
			// Kick off replication immediately.
			n.sendHeartbeats()
			n.mu.Unlock()
		}
	}
}

// Stop signals the Raft node to stop and fails any in-flight Propose callers.
func (n *Node) Stop() {
	n.mu.Lock()
	n.drainPending(fmt.Errorf("node stopped"))
	n.mu.Unlock()
	close(n.stopCh)
}

// Propose submits a command for Raft replication. Blocks until committed or error.
func (n *Node) Propose(cmd json.RawMessage) error {
	replyCh := make(chan error, 1)
	select {
	case n.proposeCh <- ProposeReq{Command: cmd, ReplyCh: replyCh}:
	case <-n.stopCh:
		return fmt.Errorf("node stopped")
	}
	// Also escape if the node stops while we wait for the commit reply.
	select {
	case err := <-replyCh:
		return err
	case <-n.stopCh:
		return fmt.Errorf("node stopped")
	}
}

// Status returns a snapshot of the node's current Raft state.
func (n *Node) Status() StatusReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	return StatusReply{
		ID:          n.id,
		Role:        n.role.String(),
		Term:        n.ps.CurrentTerm,
		LeaderID:    n.leaderID,
		CommitIndex: n.vs.CommitIndex,
		LastApplied: n.vs.LastApplied,
		LogLength:   len(n.ps.Log),
	}
}

// IsLeader reports whether this node is currently the Raft leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == Leader
}

// LeaderID returns the known leader ID (may be empty if unknown).
func (n *Node) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// LeaderHTTPAddr returns the HTTP base URL of the current leader (for redirect).
func (n *Node) LeaderHTTPAddr() string {
	n.mu.Lock()
	lid := n.leaderID
	n.mu.Unlock()
	if lid == "" {
		return ""
	}
	if lid == n.id {
		return "" // we are the leader; no redirect needed
	}
	return n.httpPeers[lid]
}

// ---- Internal methods ----

// startElection starts a new election. Called with n.mu held.
func (n *Node) startElection() {
	n.ps.CurrentTerm++
	n.ps.VotedFor = n.id
	_ = saveMeta(n.dataDir, n.ps.CurrentTerm, n.ps.VotedFor)
	n.role = Candidate
	n.leaderID = ""
	n.resetElectionTimer()

	term := n.ps.CurrentTerm
	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: lastIndex(n.ps.Log),
		LastLogTerm:  lastTerm(n.ps.Log),
	}
	total := 1 + len(n.peers)
	votes := 1 // voted for self
	var voteMu sync.Mutex

	n.log.Info().Int("term", term).Msg("starting election")

	for peerID, peerAddr := range n.peers {
		go func(pid, addr string) {
			reply, err := sendVote(addr, args)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > n.ps.CurrentTerm {
				n.stepDown(reply.Term)
				return
			}
			if n.role != Candidate || n.ps.CurrentTerm != term {
				return
			}
			if reply.VoteGranted {
				voteMu.Lock()
				votes++
				won := votes*2 > total
				voteMu.Unlock()
				if won {
					n.becomeLeader()
				}
			}
		}(peerID, peerAddr)
	}
}

// becomeLeader transitions this node to Leader. Called with n.mu held.
func (n *Node) becomeLeader() {
	n.role = Leader
	n.leaderID = n.id
	li := lastIndex(n.ps.Log)
	for id := range n.peers {
		n.vs.NextIndex[id] = li + 1
		n.vs.MatchIndex[id] = 0
	}
	n.log.Info().Int("term", n.ps.CurrentTerm).Msg("became leader")
	n.sendHeartbeats()
}

// drainPending fails all in-flight Propose callers with err. Called with n.mu held.
func (n *Node) drainPending(err error) {
	for idx, ch := range n.pending {
		ch <- err
		delete(n.pending, idx)
	}
}

// stepDown transitions to Follower. Called with n.mu held.
func (n *Node) stepDown(newTerm int) {
	if n.role == Leader {
		n.log.Info().Int("new_term", newTerm).Msg("leader stepped down")
		// Fail any Propose callers blocked waiting for a commit that will never arrive.
		n.drainPending(fmt.Errorf("leader stepped down"))
	}
	n.ps.CurrentTerm = newTerm
	n.ps.VotedFor = ""
	_ = saveMeta(n.dataDir, n.ps.CurrentTerm, n.ps.VotedFor)
	n.role = Follower
	n.leaderID = ""
	n.resetElectionTimer()
}

// resetElectionTimer restarts the election timer with a fresh random duration.
// Called with n.mu held.
func (n *Node) resetElectionTimer() {
	if !n.electionTimer.Stop() {
		select {
		case <-n.electionTimer.C:
		default:
		}
	}
	n.electionTimer.Reset(randomElectionTimeout())
}

// sendHeartbeats replicates to all peers in parallel goroutines.
// Called with n.mu held; goroutines acquire their own locks.
func (n *Node) sendHeartbeats() {
	for peerID, peerAddr := range n.peers {
		go n.replicateToPeer(peerID, peerAddr)
	}
}

// appendProposal adds a new entry to the leader's local log. Called with n.mu held.
func (n *Node) appendProposal(cmd json.RawMessage) error {
	if n.role != Leader {
		return fmt.Errorf("not leader (leader=%s)", n.leaderID)
	}
	entry := LogEntry{
		Index:   lastIndex(n.ps.Log) + 1,
		Term:    n.ps.CurrentTerm,
		Command: cmd,
	}
	n.ps.Log = append(n.ps.Log, entry)
	if err := appendLogEntryDisk(n.dataDir, entry); err != nil {
		// Roll back the in-memory append so memory and disk stay consistent.
		n.ps.Log = n.ps.Log[:len(n.ps.Log)-1]
		return fmt.Errorf("append log disk: %w", err)
	}
	return nil
}

// signalApplier sends a non-blocking signal to the applier goroutine.
// Called with n.mu held (but does not block).
func (n *Node) signalApplier() {
	select {
	case n.notifyCh <- struct{}{}:
	default:
	}
}

// notifyPending resolves any pending Propose callers whose entry is now committed.
// Called with n.mu held.
func (n *Node) notifyPending() {
	for idx, ch := range n.pending {
		if idx <= n.vs.CommitIndex {
			ch <- nil
			delete(n.pending, idx)
		}
	}
}

// runApplier is a dedicated goroutine that applies committed log entries to applyCh.
func (n *Node) runApplier() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.notifyCh:
			n.applyCommitted()
		}
	}
}

// applyCommitted sends newly committed entries to applyCh, in index order.
func (n *Node) applyCommitted() {
	for {
		n.mu.Lock()
		if n.vs.LastApplied >= n.vs.CommitIndex {
			n.mu.Unlock()
			return
		}
		n.vs.LastApplied++
		target := n.vs.LastApplied
		entry, ok := entryAtIndex(n.ps.Log, target)
		// Notify pending proposals that reached commit.
		if ch, found := n.pending[target]; found {
			ch <- nil
			delete(n.pending, target)
		}
		n.mu.Unlock()

		if ok {
			// Send without holding the lock (blocks if consumer is slow).
			select {
			case n.applyCh <- entry:
			case <-n.stopCh:
				return
			}
		}
	}
}
