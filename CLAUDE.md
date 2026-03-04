# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

A load balancer written in Go. Module: `github.com/Weilei424/load-balancer` (Go 1.24.3).

## Commands

```bash
# Build
go build ./...

# Run tests
go test ./...

# Run a single test
go test ./path/to/package -run TestName

# Run with race detector
go test -race ./...

# Format code
gofmt -w .

# Vet
go vet ./...
```

# CLAUDE.md — Claude Code Instructions (Strict)

You are Claude Code working inside this repository.

## Primary Rule
Follow **AGENTS.md** as the single source of truth. Do not deviate.

## Deliverables
Implement the full project per AGENTS.md including:
- active-active proxy LB
- raftlite election + heartbeats + minimal log replication
- admin commands replicated through raft log
- web dashboard with **SSE** live updates
- metrics endpoint `/metrics`
- structured logging (zap or zerolog)
- load test script
- chaos script
- scoping notes in README
- Makefile targets (build/test/demo/chaos)

## Work Style Rules
- Make small, correct commits (or logically grouped changes) with clear messages.
- Prefer correctness + clarity over cleverness.
- Keep dependencies minimal. Every new dependency must be justified in a comment in code and in README.

## Implementation Rules
- Raft is not optional and must be implemented in-repo (`/internal/raftlite`).
- Real inter-node communication required (HTTP+JSON or gRPC is fine; prefer HTTP+JSON).
- Election + heartbeats must function across separate processes.
- Log replication must commit on majority and apply commands deterministically.

## Safety / Non-goals
- Do not implement snapshots, joint consensus, or dynamic membership for v1.
- Do not add unrelated features (TLS, auth, Kubernetes) unless required by AGENTS.md.
- Avoid heavy frameworks.

## Testing Rules
- Add tests as you build features.
- Provide at least one integration test that boots multiple nodes.
- All tests must be runnable with `go test ./...` and `make test`.

## Output Expectations
When you finish:
- `make demo` works on a clean machine with Go installed.
- Dashboard shows leader, followers, term, backends, req counts.
- Leader failover is visible and traffic continues.

If anything is ambiguous, choose the simplest approach consistent with AGENTS.md and document the choice in README “Tradeoffs”.
