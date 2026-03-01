# Raft Load Balancer

`load-balancer` is a local-first distributed systems project written in Go. It combines an active-active HTTP reverse proxy with an in-repo Raft implementation (`raftlite`) so multiple load balancer nodes can accept traffic while cluster-wide configuration changes are coordinated through leader election, heartbeats, and replicated log entries.

The system is built for demonstration and learning rather than production hardening. It runs as a small set of plain Go processes on localhost: a few backend servers, a few LB nodes, and a browser-accessible dashboard. The result is a compact project that shows consensus, failover, routing, observability, and operational scripting in one repo.

## Overview

Each LB node exposes:
- a reverse-proxy HTTP listener for client traffic
- an admin API for backend and routing changes
- a live SSE dashboard
- a `/metrics` endpoint
- a dedicated Raft RPC listener

Backends are simple HTTP servers with a `/health` endpoint. Once backends are added through the admin API, all nodes eventually apply the same configuration and can proxy requests using their local state. Reads are local and fast; writes are leader-only and replicated through the Raft log.

Current routing behavior:
- `round_robin`
- `least_conn`

Current replicated config commands:
- `AddBackend(url)`
- `RemoveBackend(url)`
- `SetWeight(url, w)`
- `SetAlgorithm(round_robin | least_conn)`

## Highlights

- Active-active L7 reverse proxy across multiple nodes
- In-repo Raft leader election, heartbeats, and minimal log replication
- Leader-only config writes with follower redirect behavior
- Health-aware backend selection
- Live dashboard over Server-Sent Events
- Prometheus-style metrics and structured zerolog output
- Demo, load, and chaos scripts for local operation

## Quick Start

```bash
make build
make test
make demo
```

Useful endpoints after `make demo`:
- Dashboard: `http://localhost:9001/`
- Dashboard: `http://localhost:9002/`
- Dashboard: `http://localhost:9003/`
- Metrics: `http://localhost:9001/metrics`
- Raft status: `http://localhost:10001/raft/status`

## Repo Layout

```text
cmd/lb/                CLI entrypoint (`lb node`, `lb backend`, `lb admin`)
internal/raftlite/     Raft election, heartbeats, replication, persistence
internal/lb/           Proxy, routing, health checks, admin handlers, state machine
internal/dashboard/    Dashboard HTML and SSE broadcasting
internal/metrics/      Prometheus-style metrics rendering
internal/logging/      zerolog setup
scripts/               demo, load test, and chaos helpers
```

## Documentation

Detailed runbooks live in:
- [Usage Guide](docs/usage.md)
- [Testing Guide](docs/testing.md)
- [Deployment And Operations](docs/deployment.md)

## Scope Notes

Implemented:
- leader election with randomized election timeouts
- heartbeat-based liveness detection
- minimal log replication with majority commit
- persistence of term, vote, and log
- backend health checks
- local demo and chaos workflows

Not implemented in v1:
- snapshots / log compaction
- dynamic membership / joint consensus
- TLS or auth
- production packaging or orchestration
