package lb

import (
	"encoding/json"
	"net/http"

	"github.com/rs/zerolog"

	"github.com/Weilei424/load-balancer/internal/raftlite"
)

// RaftNode is the subset of raftlite.Node the admin handler needs.
type RaftNode interface {
	IsLeader() bool
	LeaderHTTPAddr() string
	Propose(cmd json.RawMessage) error
}

// AdminHandler handles /admin/* requests.
// Leader-only: followers respond with HTTP 307 pointing to the leader.
type AdminHandler struct {
	node RaftNode
	log  zerolog.Logger
}

// NewAdminHandler creates an AdminHandler backed by the given raftlite node.
func NewAdminHandler(node *raftlite.Node, log zerolog.Logger) *AdminHandler {
	return &AdminHandler{node: node, log: log}
}

func (ah *AdminHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !ah.node.IsLeader() {
		leaderAddr := ah.node.LeaderHTTPAddr()
		if leaderAddr == "" {
			http.Error(w, `{"error":"no leader known"}`, http.StatusServiceUnavailable)
			return
		}
		// Redirect client to leader.
		http.Redirect(w, r, leaderAddr+r.RequestURI, http.StatusTemporaryRedirect)
		return
	}

	switch r.URL.Path {
	case "/admin/backends":
		switch r.Method {
		case http.MethodPost:
			ah.handleAddBackend(w, r)
		case http.MethodDelete:
			ah.handleRemoveBackend(w, r)
		case http.MethodPatch:
			ah.handleSetWeight(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case "/admin/algorithm":
		if r.Method == http.MethodPut {
			ah.handleSetAlgorithm(w, r)
		} else {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	default:
		http.NotFound(w, r)
	}
}

func (ah *AdminHandler) handleAddBackend(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL    string `json:"url"`
		Weight int    `json:"weight"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.URL == "" {
		http.Error(w, `{"error":"url required"}`, http.StatusBadRequest)
		return
	}
	if req.Weight <= 0 {
		req.Weight = 1
	}
	cmd := Command{Op: OpAddBackend, URL: req.URL, Weight: req.Weight}
	ah.propose(w, cmd)
}

func (ah *AdminHandler) handleRemoveBackend(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.URL == "" {
		http.Error(w, `{"error":"url required"}`, http.StatusBadRequest)
		return
	}
	ah.propose(w, Command{Op: OpRemoveBackend, URL: req.URL})
}

func (ah *AdminHandler) handleSetWeight(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL    string `json:"url"`
		Weight int    `json:"weight"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.URL == "" {
		http.Error(w, `{"error":"url required"}`, http.StatusBadRequest)
		return
	}
	if req.Weight <= 0 {
		http.Error(w, `{"error":"weight must be positive"}`, http.StatusBadRequest)
		return
	}
	ah.propose(w, Command{Op: OpSetWeight, URL: req.URL, Weight: req.Weight})
}

func (ah *AdminHandler) handleSetAlgorithm(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Algorithm string `json:"algorithm"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.Algorithm != AlgoRoundRobin && req.Algorithm != AlgoLeastConn && req.Algorithm != AlgoConsistentHash {
		http.Error(w, `{"error":"algorithm must be \"round_robin\", \"least_conn\", or \"consistent_hash\""}`, http.StatusBadRequest)
		return
	}
	ah.propose(w, Command{Op: OpSetAlgorithm, Algorithm: req.Algorithm})
}

func (ah *AdminHandler) propose(w http.ResponseWriter, cmd Command) {
	raw, err := cmd.Encode()
	if err != nil {
		http.Error(w, "encode error", http.StatusInternalServerError)
		return
	}
	if err := ah.node.Propose(raw); err != nil {
		ah.log.Error().Err(err).Msg("propose failed")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()}) //nolint:errcheck
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "committed"}) //nolint:errcheck
}
