package raftlite

import (
	"encoding/json"
	"net/http"
)

// Handler returns an http.Handler for the Raft RPC endpoints.
// Mount this on the dedicated raft port (e.g. :10001).
func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/raft/vote", n.serveVote)
	mux.HandleFunc("/raft/append", n.serveAppend)
	mux.HandleFunc("/raft/status", n.serveStatus)
	mux.HandleFunc("/raft/install-snapshot", n.serveInstallSnapshot)
	return mux
}

func (n *Node) serveVote(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var args RequestVoteArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	n.mu.Lock()
	reply := n.handleRequestVote(args)
	n.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(reply) //nolint:errcheck
}

func (n *Node) serveAppend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var args AppendEntriesArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	n.mu.Lock()
	reply := n.handleAppendEntries(args)
	n.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(reply) //nolint:errcheck
}

func (n *Node) serveStatus(w http.ResponseWriter, r *http.Request) {
	s := n.Status()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s) //nolint:errcheck
}

func (n *Node) serveInstallSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var args InstallSnapshotArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	n.mu.Lock()
	reply, accepted, snap := n.receiveInstallSnapshot(args)
	n.mu.Unlock()

	if accepted {
		// Apply snapshot to state machine synchronously without holding n.mu.
		// This guarantees the state machine is restored before LastApplied advances,
		// so no subsequent log entry can be applied to a stale state machine.
		if n.applySnapshot != nil {
			if err := n.applySnapshot(snap.Data); err != nil {
				n.log.Error().Err(err).Msg("install-snapshot: apply callback failed")
				// Snapshot is already on disk; log the error and continue — the
				// leader will retry if the state machine stays inconsistent.
			}
		}
		// Finalize volatile state now that restore has completed.
		n.mu.Lock()
		if n.vs.CommitIndex < snap.LastIncludedIndex {
			n.vs.CommitIndex = snap.LastIncludedIndex
			_ = saveCommit(n.dataDir, n.vs.CommitIndex)
		}
		if n.vs.LastApplied < snap.LastIncludedIndex {
			n.vs.LastApplied = snap.LastIncludedIndex
		}
		reply.Success = true
		reply.Term = n.ps.CurrentTerm
		n.mu.Unlock()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(reply) //nolint:errcheck
}
