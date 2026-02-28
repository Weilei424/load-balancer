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
