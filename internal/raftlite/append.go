package raftlite

// handleAppendEntries processes an AppendEntries RPC (heartbeat or replication).
// Must be called with n.mu held.
func (n *Node) handleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	reply := AppendEntriesReply{Term: n.ps.CurrentTerm}

	if args.Term < n.ps.CurrentTerm {
		return reply // stale — reject
	}

	if args.Term > n.ps.CurrentTerm {
		n.stepDown(args.Term)
	} else if n.role == Candidate {
		// Another node won this term's election.
		n.role = Follower
	}

	n.leaderID = args.LeaderID
	n.resetElectionTimer()
	reply.Term = n.ps.CurrentTerm

	// Log consistency check at PrevLogIndex.
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex <= n.snapshot.LastIncludedIndex {
			// Entry was compacted into the snapshot.
			if args.PrevLogIndex == n.snapshot.LastIncludedIndex &&
				args.PrevLogTerm != n.snapshot.LastIncludedTerm {
				// Snapshot boundary term mismatch — rare but possible after split-brain.
				reply.ConflictIndex = n.snapshot.LastIncludedIndex
				reply.ConflictTerm = n.snapshot.LastIncludedTerm
				return reply
			}
			// PrevLogIndex < snapshotLastIndex: implicitly consistent (already applied).
		} else {
			ourTerm := termAt(n.ps.Log, args.PrevLogIndex)
			if ourTerm == 0 {
				// We don't have the entry at PrevLogIndex.
				reply.ConflictIndex = n.lastLogIndex() + 1
				reply.ConflictTerm = 0
				return reply
			}
			if ourTerm != args.PrevLogTerm {
				// Term mismatch — send fast-rollback hint.
				reply.ConflictTerm = ourTerm
				reply.ConflictIndex = firstIndexOfTerm(n.ps.Log, ourTerm)
				return reply
			}
		}
	}

	// Append new entries, handling conflicts.
	//
	// Disk errors are treated as hard failures: we return Success=false so the
	// leader retries rather than accepting entries with memory/disk divergence.
	// The trade-off is that a node with a broken disk will keep rejecting
	// AppendEntries until the operator intervenes, which is far safer than
	// silently losing log entries that later disappear on restart.
	for _, entry := range args.Entries {
		if entry.Index <= n.snapshot.LastIncludedIndex {
			continue // already committed in snapshot
		}
		existing := termAt(n.ps.Log, entry.Index)
		if existing != 0 {
			if existing != entry.Term {
				// Conflicting entry — truncate to just before this index and
				// rewrite the log file atomically. If the disk write fails,
				// do NOT update n.ps.Log so memory and disk remain consistent.
				truncated := truncateFrom(n.ps.Log, entry.Index)
				if err := rewriteLogDisk(n.dataDir, truncated); err != nil {
					n.withStateLocked(n.log.Error().Err(err).Int("index", entry.Index)).
						Msg("log truncation disk write failed; rejecting AppendEntries")
					return reply // Success=false; leader will retry
				}
				n.ps.Log = truncated
			} else {
				continue // already have this entry
			}
		}
		n.ps.Log = append(n.ps.Log, entry)
		if err := appendLogEntryDisk(n.dataDir, entry); err != nil {
			// Roll back the in-memory append so memory and disk stay in sync.
			n.withStateLocked(n.log.Error().Err(err).Int("index", entry.Index)).
				Msg("log append disk write failed; rejecting AppendEntries")
			n.ps.Log = n.ps.Log[:len(n.ps.Log)-1]
			return reply // Success=false; leader will retry
		}
	}

	// Advance commit index.
	if args.LeaderCommit > n.vs.CommitIndex {
		li := n.lastLogIndex()
		if args.LeaderCommit < li {
			n.vs.CommitIndex = args.LeaderCommit
		} else {
			n.vs.CommitIndex = li
		}
		// saveCommit is best-effort: the commit index is re-derived from the log
		// on restart, so a failure here does not corrupt state — log it and move on.
		if err := saveCommit(n.dataDir, n.vs.CommitIndex); err != nil {
			n.withStateLocked(n.log.Error().Err(err).Int("commit_index", n.vs.CommitIndex)).
				Msg("failed to persist commit index")
		}
		n.signalApplier()
	}

	reply.Success = true
	return reply
}

// maxEntriesPerRPC limits how many log entries are sent in a single AppendEntries
// RPC. Batching improves catchup throughput without unbounded payload sizes.
const maxEntriesPerRPC = 64

// replicateToPeer sends AppendEntries (or InstallSnapshot) to a single peer.
// Runs in its own goroutine.
func (n *Node) replicateToPeer(peerID, peerAddr string) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	nextIdx := n.vs.NextIndex[peerID]

	// If the peer has fallen behind the snapshot boundary, send the snapshot instead.
	if nextIdx <= n.snapshot.LastIncludedIndex {
		snap := n.snapshot
		term := n.ps.CurrentTerm
		n.mu.Unlock()

		args := InstallSnapshotArgs{
			Term:              term,
			LeaderID:          n.id,
			LastIncludedIndex: snap.LastIncludedIndex,
			LastIncludedTerm:  snap.LastIncludedTerm,
			Data:              snap.Data,
		}
		reply, err := sendInstallSnapshot(peerAddr, args)
		if err != nil {
			return
		}

		n.mu.Lock()
		defer n.mu.Unlock()
		if reply.Term > n.ps.CurrentTerm {
			n.stepDown(reply.Term)
			return
		}
		if n.role != Leader {
			return
		}
		if reply.Success {
			if snap.LastIncludedIndex > n.vs.MatchIndex[peerID] {
				n.vs.MatchIndex[peerID] = snap.LastIncludedIndex
			}
			n.vs.NextIndex[peerID] = n.vs.MatchIndex[peerID] + 1
		}
		return
	}

	// Normal AppendEntries path.
	prevIdx := nextIdx - 1
	prevTerm := n.termAtWithSnapshot(prevIdx)
	rawEntries := logSlice(n.ps.Log, prevIdx)
	if len(rawEntries) > maxEntriesPerRPC {
		rawEntries = rawEntries[:maxEntriesPerRPC]
	}
	entries := make([]LogEntry, len(rawEntries))
	copy(entries, rawEntries)
	args := AppendEntriesArgs{
		Term:         n.ps.CurrentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.vs.CommitIndex,
	}
	n.mu.Unlock()

	reply, err := sendAppend(peerAddr, args)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.ps.CurrentTerm {
		n.stepDown(reply.Term)
		return
	}
	if n.role != Leader {
		return
	}

	if reply.Success {
		newMatch := prevIdx + len(entries)
		if newMatch > n.vs.MatchIndex[peerID] {
			n.vs.MatchIndex[peerID] = newMatch
		}
		n.vs.NextIndex[peerID] = n.vs.MatchIndex[peerID] + 1
		n.tryAdvanceCommitIndex()
	} else {
		// Fast rollback using ConflictIndex/ConflictTerm (Raft §5.3).
		if reply.ConflictTerm == 0 {
			n.vs.NextIndex[peerID] = reply.ConflictIndex
		} else {
			idx := lastIndexOfTerm(n.ps.Log, reply.ConflictTerm)
			if idx > 0 {
				n.vs.NextIndex[peerID] = idx + 1
			} else {
				n.vs.NextIndex[peerID] = reply.ConflictIndex
			}
		}
		if n.vs.NextIndex[peerID] < 1 {
			n.vs.NextIndex[peerID] = 1
		}
	}
}

// tryAdvanceCommitIndex advances commitIndex if a majority has replicated a new entry.
// Must be called with n.mu held.
func (n *Node) tryAdvanceCommitIndex() {
	total := 1 + len(n.peers) // self + peers
	for idx := lastIndex(n.ps.Log); idx > n.vs.CommitIndex; idx-- {
		if termAt(n.ps.Log, idx) != n.ps.CurrentTerm {
			// Only commit entries from the current term (Raft §5.4.2).
			continue
		}
		count := 1 // leader counts itself
		for _, mi := range n.vs.MatchIndex {
			if mi >= idx {
				count++
			}
		}
		if count*2 > total { // majority
			n.vs.CommitIndex = idx
			if err := saveCommit(n.dataDir, idx); err != nil {
				n.withStateLocked(n.log.Error().Err(err).Int("commit_index", idx)).
					Msg("failed to persist commit index")
			}
			n.signalApplier()
			n.notifyPending()
			break
		}
	}
}
