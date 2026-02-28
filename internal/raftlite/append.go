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
		ourTerm := termAt(n.ps.Log, args.PrevLogIndex)
		if ourTerm == 0 {
			// We don't have the entry at PrevLogIndex.
			reply.ConflictIndex = lastIndex(n.ps.Log) + 1
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

	// Append new entries, handling conflicts.
	for _, entry := range args.Entries {
		existing := termAt(n.ps.Log, entry.Index)
		if existing != 0 {
			if existing != entry.Term {
				// Conflicting entry — truncate and re-append.
				n.ps.Log = truncateFrom(n.ps.Log, entry.Index)
				_ = rewriteLogDisk(n.dataDir, n.ps.Log)
			} else {
				continue // already have this entry
			}
		}
		n.ps.Log = append(n.ps.Log, entry)
		_ = appendLogEntryDisk(n.dataDir, entry)
	}

	// Advance commit index.
	if args.LeaderCommit > n.vs.CommitIndex {
		li := lastIndex(n.ps.Log)
		if args.LeaderCommit < li {
			n.vs.CommitIndex = args.LeaderCommit
		} else {
			n.vs.CommitIndex = li
		}
		n.signalApplier()
	}

	reply.Success = true
	return reply
}

// replicateToPeer sends AppendEntries to a single peer. Runs in its own goroutine.
func (n *Node) replicateToPeer(peerID, peerAddr string) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	nextIdx := n.vs.NextIndex[peerID]
	prevIdx := nextIdx - 1
	prevTerm := termAt(n.ps.Log, prevIdx)
	rawEntries := logSlice(n.ps.Log, prevIdx)
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
			n.signalApplier()
			n.notifyPending()
			break
		}
	}
}
