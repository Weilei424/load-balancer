package raftlite

// handleRequestVote processes a RequestVote RPC.
// Must be called with n.mu held.
func (n *Node) handleRequestVote(args RequestVoteArgs) RequestVoteReply {
	reply := RequestVoteReply{Term: n.ps.CurrentTerm}

	if args.Term < n.ps.CurrentTerm {
		return reply // stale term — reject
	}

	if args.Term > n.ps.CurrentTerm {
		n.stepDown(args.Term)
	}

	reply.Term = n.ps.CurrentTerm

	// Reject if we've already voted for someone else this term.
	alreadyVoted := n.ps.VotedFor != "" && n.ps.VotedFor != args.CandidateID
	if alreadyVoted {
		return reply
	}

	// Candidate log must be at least as up-to-date as ours (Raft §5.4.1).
	ourLastIndex := lastIndex(n.ps.Log)
	ourLastTerm := lastTerm(n.ps.Log)
	logOK := args.LastLogTerm > ourLastTerm ||
		(args.LastLogTerm == ourLastTerm && args.LastLogIndex >= ourLastIndex)
	if !logOK {
		return reply
	}

	n.ps.VotedFor = args.CandidateID
	_ = saveMeta(n.dataDir, n.ps.CurrentTerm, n.ps.VotedFor)
	n.resetElectionTimer()
	reply.VoteGranted = true
	return reply
}
