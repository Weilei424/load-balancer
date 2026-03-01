package raftlite

import (
	"math/rand"
	"time"
)

const (
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond
)

// randomElectionTimeout returns a random duration in [150ms, 300ms) using r.
// Accepting an explicit *rand.Rand keeps this testable and avoids the shared
// global source that causes nodes started at the same instant to pick identical
// timeouts and loop through synchronized elections indefinitely.
func randomElectionTimeout(r *rand.Rand) time.Duration {
	spread := int64(electionTimeoutMax - electionTimeoutMin)
	return electionTimeoutMin + time.Duration(r.Int63n(spread))
}
