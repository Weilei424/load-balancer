// Package dashboard provides the SSE-powered web dashboard.
package dashboard

import "time"

// BackendSnapshot is a read-only snapshot of a single backend for the dashboard.
type BackendSnapshot struct {
	URL       string `json:"url"`
	Weight    int    `json:"weight"`
	Healthy   bool   `json:"healthy"`
	ConnCount int64  `json:"conn_count"`
}

// NodeSnapshot is a full dashboard state snapshot sent over SSE.
type NodeSnapshot struct {
	ID          string            `json:"id"`
	Role        string            `json:"role"`
	Term        int               `json:"term"`
	LeaderID    string            `json:"leader_id"`
	CommitIndex int               `json:"commit_index"`
	LastApplied int               `json:"last_applied"`
	Algorithm   string            `json:"algorithm"`
	Backends    []BackendSnapshot `json:"backends"`
	// Requests proxied since last snapshot
	RequestsTotal int64 `json:"requests_total"`
	ErrorsTotal   int64 `json:"errors_total"`
	Timestamp     int64 `json:"timestamp"` // unix millis
}

// LeaderChangeEvent is recorded in the timeline when leadership changes.
type LeaderChangeEvent struct {
	Term     int       `json:"term"`
	LeaderID string    `json:"leader_id"`
	At       time.Time `json:"at"`
}
