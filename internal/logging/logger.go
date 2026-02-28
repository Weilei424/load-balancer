// Package logging provides zerolog-based structured logging.
// zerolog is used for zero-allocation structured logging as required by AGENTS.md.
package logging

import (
	"os"

	"github.com/rs/zerolog"
)

// New creates a structured logger tagged with the given node ID.
func New(nodeID string) zerolog.Logger {
	return zerolog.New(os.Stdout).With().
		Timestamp().
		Str("node_id", nodeID).
		Logger()
}
