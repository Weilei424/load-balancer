// Package lb implements the load-balancer proxy, routing, health-checking,
// config state machine, and admin HTTP handlers.
package lb

import "encoding/json"

// OpType identifies the configuration operation to replicate through Raft.
type OpType string

const (
	OpAddBackend    OpType = "add_backend"
	OpRemoveBackend OpType = "remove_backend"
	OpSetWeight     OpType = "set_weight"
	OpSetAlgorithm  OpType = "set_algorithm"
)

// Command is a configuration command replicated through the Raft log.
type Command struct {
	Op        OpType `json:"op"`
	URL       string `json:"url,omitempty"`
	Weight    int    `json:"weight,omitempty"`
	Algorithm string `json:"algorithm,omitempty"`
}

// Encode serialises a Command to JSON for inclusion in a log entry.
func (c Command) Encode() (json.RawMessage, error) {
	return json.Marshal(c)
}
