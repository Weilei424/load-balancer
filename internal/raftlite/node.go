package raftlite

import (
	"encoding/json"
	"fmt"
	"math/rand"
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

	// snapshot is the current compacted snapshot (LastIncludedIndex=0 → none).
	snapshot      Snapshot
	snapThreshold int           // effective threshold; triggers auto-snapshot
	snapshotter   func() []byte // serializes state machine; nil disables auto-snapshotting
	applySnapshot func([]byte) error // restores state machine; called synchronously, no lock held

	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker

	applyCh   chan LogEntry   // committed entries → state machine consumer
	proposeCh chan ProposeReq // external Propose() callers → Run() loop

	// rng is a per-node random source so each node gets independent election
	// timeouts even when processes start at the same wall-clock instant.
	rng *rand.Rand

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

	snap, _, err := loadSnapshot(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("load snapshot: %w", err)
	}

	term, votedFor, err := loadMeta(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("load meta: %w", err)
	}
	entries, err := loadLogDisk(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("load log: %w", err)
	}
	// Filter out any log entries already covered by the snapshot (defensive: clean
	// shutdown always trims them, but a mid-snapshot crash might leave them).
	if snap.LastIncludedIndex > 0 {
		entries = logSlice(entries, snap.LastIncludedIndex)
	}

	commitIdx, err := loadCommit(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("load commit: %w", err)
	}
	// commitIdx must be at least the snapshot boundary.
	if commitIdx < snap.LastIncludedIndex {
		commitIdx = snap.LastIncludedIndex
	}
	// Clamp to the actual log extent (log + snapshot boundary).
	maxIdx := lastIndex(entries)
	if maxIdx < snap.LastIncludedIndex {
		maxIdx = snap.LastIncludedIndex
	}
	if commitIdx > maxIdx {
		commitIdx = maxIdx
	}

	snapThreshold := cfg.SnapshotThreshold
	if snapThreshold <= 0 {
		snapThreshold = 1000
	}

	// Seed per-node RNG: mix wall-clock nanoseconds with a hash of the node ID so
	// processes started at the same instant still get independent timeouts.
	seed := time.Now().UnixNano()
	for _, ch := range cfg.ID {
		seed ^= int64(ch)
		seed *= 1099511628211 // FNV-prime
	}
	rng := rand.New(rand.NewSource(seed))

	n := &Node{
		id:            cfg.ID,
		dataDir:       cfg.DataDir,
		peers:         cfg.Peers,
		httpPeers:     cfg.HTTPPeers,
		snapshot:      snap,
		snapThreshold: snapThreshold,
		snapshotter:   cfg.Snapshotter,
		applySnapshot: cfg.ApplySnapshot,
		ps: PersistentState{
			CurrentTerm: term,
			VotedFor:    votedFor,
			Log:         entries,
		},
		vs: VolatileState{
			CommitIndex: commitIdx,
			LastApplied: snap.LastIncludedIndex, // entries up to snapshot already applied
			NextIndex:   make(map[string]int),
			MatchIndex:  make(map[string]int),
		},
		role:      Follower,
		rng:       rng,
		applyCh:   applyCh,
		proposeCh: proposeCh,
		notifyCh:  make(chan struct{}, 1),
		pending:   make(map[int]chan error),
		stopCh:    make(chan struct{}),
		log:       logger,
	}
	n.heartbeatTicker = time.NewTicker(heartbeatInterval)
	n.electionTimer = time.NewTimer(n.randomElectionTimeout())
	return n, nil
}

// Run starts the Raft main event loop and the applier goroutine.
// Blocks until Stop() is called.
func (n *Node) Run() {
	n.mu.Lock()
	n.withStateLocked(n.log.Info()).Msg("raft node starting")
	n.mu.Unlock()
	// Apply the startup snapshot synchronously, before the applier goroutine starts,
	// so the state machine is fully restored before any log-tail entries are replayed.
	// A restore failure is unrecoverable: replaying the log on top of an unrestored
	// state machine would silently corrupt state, so we stop the node instead.
	if n.snapshot.LastIncludedIndex > 0 && n.applySnapshot != nil {
		if err := n.applySnapshot(n.snapshot.Data); err != nil {
			n.log.Error().Err(err).Msg("startup snapshot apply failed; stopping node to prevent state corruption")
			n.Stop()
			return
		}
	}
	go n.runApplier()
	// Replay any committed-but-not-yet-applied entries that survived restart.
	n.signalApplier()

	for {
		select {
		case <-n.stopCh:
			n.mu.Lock()
			n.withStateLocked(n.log.Info()).Msg("raft node stopping")
			n.mu.Unlock()
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

// withStateLocked attaches term, role, and leader_id to e.
// Caller must hold n.mu.
func (n *Node) withStateLocked(e *zerolog.Event) *zerolog.Event {
	return e.Int("term", n.ps.CurrentTerm).Str("role", n.role.String()).Str("leader_id", n.leaderID)
}

// lastLogIndex returns the index of the last log entry, accounting for snapshots.
// Called with n.mu held.
func (n *Node) lastLogIndex() int {
	li := lastIndex(n.ps.Log)
	if li < n.snapshot.LastIncludedIndex {
		return n.snapshot.LastIncludedIndex
	}
	return li
}

// lastLogTerm returns the term of the last log entry, accounting for snapshots.
// Called with n.mu held.
func (n *Node) lastLogTerm() int {
	if len(n.ps.Log) == 0 {
		return n.snapshot.LastIncludedTerm
	}
	return lastTerm(n.ps.Log)
}

// termAtWithSnapshot returns the term at idx, checking the snapshot boundary first.
// Called with n.mu held.
func (n *Node) termAtWithSnapshot(idx int) int {
	if idx == n.snapshot.LastIncludedIndex {
		return n.snapshot.LastIncludedTerm
	}
	return termAt(n.ps.Log, idx)
}

// TakeSnapshot compacts the log up to CommitIndex and saves a snapshot.
// Safe to call from the applier goroutine (acquires n.mu internally).
// Holds n.mu during disk writes to prevent concurrent log appends from racing
// with the atomic log rewrite.
func (n *Node) TakeSnapshot(data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	commitIdx := n.vs.CommitIndex
	term := n.termAtWithSnapshot(commitIdx)
	if term == 0 {
		return fmt.Errorf("takeSnapshot: term unknown for index %d", commitIdx)
	}
	newLog := logSlice(n.ps.Log, commitIdx)
	snap := Snapshot{LastIncludedIndex: commitIdx, LastIncludedTerm: term, Data: data}

	if err := saveSnapshot(n.dataDir, snap); err != nil {
		return err
	}
	if err := rewriteLogDisk(n.dataDir, newLog); err != nil {
		return err
	}
	n.ps.Log = newLog
	n.snapshot = snap
	return nil
}

// receiveInstallSnapshot validates the snapshot RPC, writes it to disk, and updates
// in-memory log state. It does NOT advance LastApplied/CommitIndex — the caller
// (serveInstallSnapshot) does that after the state machine callback returns.
//
// Returns (reply, accepted, snap):
//   - accepted=false: reply is already final (stale term or already current).
//   - accepted=true:  caller should invoke applySnapshot(snap.Data) then finalize.
//
// Must be called with n.mu held.
func (n *Node) receiveInstallSnapshot(args InstallSnapshotArgs) (InstallSnapshotReply, bool, Snapshot) {
	reply := InstallSnapshotReply{Term: n.ps.CurrentTerm}
	if args.Term < n.ps.CurrentTerm {
		return reply, false, Snapshot{}
	}
	if args.Term > n.ps.CurrentTerm {
		n.stepDown(args.Term)
	} else if n.role == Candidate {
		n.role = Follower
	}
	n.leaderID = args.LeaderID
	n.resetElectionTimer()
	reply.Term = n.ps.CurrentTerm

	if args.LastIncludedIndex <= n.snapshot.LastIncludedIndex {
		reply.Success = true // already up-to-date; nothing to apply
		return reply, false, Snapshot{}
	}

	snap := Snapshot{
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Data:              args.Data,
	}
	newLog := logSlice(n.ps.Log, args.LastIncludedIndex)
	if err := saveSnapshot(n.dataDir, snap); err != nil {
		n.log.Error().Err(err).Msg("install-snapshot: save failed")
		return reply, false, Snapshot{}
	}
	if err := rewriteLogDisk(n.dataDir, newLog); err != nil {
		n.log.Error().Err(err).Msg("install-snapshot: rewrite log failed")
		return reply, false, Snapshot{}
	}
	n.ps.Log = newLog
	n.snapshot = snap
	// LastApplied/CommitIndex are NOT updated here; they are set after
	// the state machine callback confirms the restore completed.
	return reply, true, snap
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
		LastLogIndex: n.lastLogIndex(),
		LastLogTerm:  n.lastLogTerm(),
	}
	total := 1 + len(n.peers)
	votes := 1 // voted for self
	var voteMu sync.Mutex

	n.withStateLocked(n.log.Info()).Msg("starting election")

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
	li := n.lastLogIndex()
	for id := range n.peers {
		n.vs.NextIndex[id] = li + 1
		n.vs.MatchIndex[id] = 0
	}
	n.withStateLocked(n.log.Info()).Msg("became leader")
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
		n.log.Info().Int("term", newTerm).Str("role", "follower").Str("leader_id", "").Msg("leader stepped down")
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

// randomElectionTimeout returns a node-specific random duration in [min, max).
func (n *Node) randomElectionTimeout() time.Duration {
	return randomElectionTimeout(n.rng)
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
	n.electionTimer.Reset(n.randomElectionTimeout())
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
		Index:   n.lastLogIndex() + 1,
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
// After all pending entries are applied, triggers an auto-snapshot if the log
// has grown past the threshold.
func (n *Node) applyCommitted() {
	for {
		n.mu.Lock()
		if n.vs.LastApplied >= n.vs.CommitIndex {
			shouldSnap := n.snapshotter != nil && len(n.ps.Log) > n.snapThreshold
			n.mu.Unlock()
			if shouldSnap {
				data := n.snapshotter()
				if err := n.TakeSnapshot(data); err != nil {
					n.log.Error().Err(err).Msg("auto-snapshot failed")
				}
			}
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
