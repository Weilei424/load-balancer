# Testing Guide

This guide covers the automated test suite and the included demo/load/chaos validation workflows.

## Test Commands

Run the standard test suite:

```bash
make test
```

Equivalent direct command:

```bash
go test -timeout 60s ./...
```

Run with the race detector:

```bash
make race
```

Format and vet:

```bash
make fmt
make vet
```

## Focused Test Runs

Run only fast unit tests:

```bash
go test -short ./...
```

Run a single package:

```bash
go test ./internal/raftlite -v
```

Run one integration test:

```bash
go test ./internal/raftlite -v -run TestThreeNodeElection
```

## What Is Covered Today

`internal/raftlite` tests cover:
- election timeout bounds
- vote granting and rejection rules
- leader step-down on higher term
- append and commit advancement behavior
- three-node election
- log replication across three nodes
- leader failover
- persistence round-trips

`internal/lb` tests cover:
- round-robin selection
- least-connection preference
- concurrent `Pick()` behavior
- config apply logic for add/remove/set operations

## Demo Validation

Run:

```bash
make demo
```

What to verify manually:
- one node becomes leader within a few seconds
- dashboards load on ports `9001`, `9002`, and `9003`
- requests to any node return backend responses
- `/metrics` increases as traffic is sent

Quick checks:

```bash
curl http://localhost:9001/raft/status
curl http://localhost:9001/hello
curl http://localhost:9002/hello
curl http://localhost:9003/hello
```

## Load Testing

Run the included load script:

```bash
bash scripts/loadtest.sh
```

Useful environment variables:

```bash
DURATION=15s CONCURRENCY=20 bash scripts/loadtest.sh
```

Defaults:
- duration: `30s`
- concurrency: `10`
- targets: `http://localhost:9001`, `:9002`, `:9003`

Behavior:
- uses `hey` if installed
- otherwise falls back to a `curl` loop

## Chaos Testing

Run the chaos workflow:

```bash
make chaos
```

Equivalent:

```bash
bash scripts/chaos.sh
```

Useful override:

```bash
CHAOS_ROUNDS=5 bash scripts/chaos.sh
```

What the script does:
- starts 3 backends
- starts 3 LB nodes
- adds backends through the admin CLI
- starts the load test in the background
- randomly kills and restarts LB nodes every 2 to 5 seconds

What to watch during chaos:
- a new leader should be elected after a leader kill
- dashboards should continue updating
- most proxied requests should continue to succeed

## Practical Notes

- The Raft integration tests open real localhost listeners; environments that restrict socket binding can block them.
- The scripts assume the standard localhost ports are free.
- `make clean` removes `./bin` and `./data` if you need a fresh local run.
