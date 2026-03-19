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
- `round_robin` — smooth weighted round-robin (SWRR), proportional to backend weight
- `least_conn` — weighted least connections; lowest `active_conns / weight` wins
- `consistent_hash` — CRC32 virtual-node ring; same client IP always reaches the same backend; ring-walks past circuit-open backends

Current replicated config commands:
- `AddBackend(url)`
- `RemoveBackend(url)`
- `SetWeight(url, w)`
- `SetAlgorithm(round_robin | least_conn | consistent_hash)`

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

## Tradeoffs

**Follower redirect vs. forward**
Admin write requests land on a follower are answered with an HTTP 307 redirect pointing to the current leader. The alternative — having the follower transparently forward the request body to the leader — was not chosen because it would require buffering a potentially large request body, duplicating the response, and handling mid-stream errors. A redirect keeps follower logic minimal and lets the client own the retry.

**Snapshot compaction threshold**
The Raft log is compacted once it exceeds 1000 entries (configurable via `Config.SnapshotThreshold`). The snapshot is written atomically to `snapshot.json` via a tmp→rename, and the log tail is rewritten to contain only entries after the snapshot boundary. The `ApplySnapshot` callback is called synchronously before any log replay on restart, so the state machine is always restored before new entries are applied. Snapshot transfer to lagging followers uses the `POST /raft/install-snapshot` RPC; `LastApplied`/`CommitIndex` are not advanced until the restore callback returns without error.

**Weighted round-robin implementation**
Routing uses Nginx's smooth weighted round-robin (SWRR) rather than expanding each backend into `weight` virtual slots. SWRR uses O(n) memory regardless of total weight and produces a well-interleaved sequence with no bursts, while the slot-expansion approach uses O(Σweight) memory and can produce runs of the same backend.

**Bootstrap behaviour**
The proxy returns 503 until at least one backend is committed through the Raft log and applied to the local config state. Waiting indefinitely was not chosen because it would obscure startup errors. Operators can observe the dashboard or `/metrics` to confirm when backends become available.

**Election timeout range**
150–300 ms was chosen to be fast enough for a visible demo but is not tuned for production. The values are constants in `internal/raftlite/ticker.go` and can be changed without touching the rest of the implementation.

## Scope Notes

Implemented:
- leader election with randomized election timeouts
- heartbeat-based liveness detection
- log replication with majority commit and batched AppendEntries (up to 64 entries/RPC)
- log snapshot and compaction with InstallSnapshot RPC for lagging followers
- persistence of term, vote, log, commit index, and snapshot
- backend health checks
- local demo and chaos workflows

Not implemented:
- dynamic membership / joint consensus
- TLS or auth
- production packaging or orchestration
