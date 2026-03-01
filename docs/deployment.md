# Deployment And Operations

This project is designed for local development and demo use. "Deploy" here means running a small cluster of processes on a single machine or simple VM without external dependencies.

## Local Deployment Model

A minimal cluster uses:
- 3 LB nodes
- 3 backend processes
- one shared binary: `./bin/lb`

Recommended port layout:

```text
LB HTTP:   9001, 9002, 9003
LB Raft:   10001, 10002, 10003
Backends:  9101, 9102, 9103
```

## Recommended Bring-Up Order

1. Build the binary.
2. Start the backend servers.
3. Start all LB nodes.
4. Wait for leader election.
5. Add backends through the admin CLI.
6. Verify traffic, dashboard, and metrics.

## Manual Local Bring-Up

Build:

```bash
make build
```

Start backends:

```bash
./bin/lb backend --id b1 --listen :9101
./bin/lb backend --id b2 --listen :9102
./bin/lb backend --id b3 --listen :9103
```

Start LB nodes:

```bash
PEERS="n1=:10001,n2=:10002,n3=:10003"
HTTP_PEERS="n1=:9001,n2=:9002,n3=:9003"

./bin/lb node --id n1 --http :9001 --raft :10001 --peers "$PEERS" --http-peers "$HTTP_PEERS" --data ./data/n1
./bin/lb node --id n2 --http :9002 --raft :10002 --peers "$PEERS" --http-peers "$HTTP_PEERS" --data ./data/n2
./bin/lb node --id n3 --http :9003 --raft :10003 --peers "$PEERS" --http-peers "$HTTP_PEERS" --data ./data/n3
```

Register backends:

```bash
./bin/lb admin add-backend --url http://localhost:9101
./bin/lb admin add-backend --url http://localhost:9102
./bin/lb admin add-backend --url http://localhost:9103
```

## Scripted Operation

For normal demo use:

```bash
make demo
```

For resilience demonstration:

```bash
make chaos
```

These scripts are the easiest way to run the project end-to-end.

## Data And Persistence

Each node stores local Raft state under its `--data` directory:

- `state.json`: current term and vote
- `log.jsonl`: append-only replicated log

Example layout:

```text
data/
  n1/
    state.json
    log.jsonl
  n2/
    state.json
    log.jsonl
  n3/
    state.json
    log.jsonl
```

Operational notes:
- use a separate data directory per node
- keep node IDs stable if you want to reuse stored state
- remove `./data` for a fully fresh cluster start

## Monitoring

Per node, check:

```bash
curl http://localhost:9001/metrics
curl http://localhost:10001/raft/status
```

Useful signals:
- current role and term
- known leader ID
- total request and error counts
- backend request distribution

The dashboard provides the same information in a browser with live SSE updates.

## Failure Handling

Expected local behavior:
- if a follower stops, the cluster should continue serving
- if the leader stops, another node should elect itself leader
- if a backend becomes unhealthy, the health checker should stop routing to it

Operational workflow after failure:

1. Check `/raft/status` on each node.
2. Confirm which node is leader.
3. Restart failed nodes with the same `--id`, ports, and `--data` path.
4. Re-run admin commands if you intentionally started from a clean data directory.

## Environment Assumptions

This repo assumes:
- a Unix-like shell for the provided scripts
- free localhost ports matching the examples
- permission to bind local TCP listeners
- Go installed locally for builds and tests

## Non-Goals For This Deployment Model

This repository does not currently provide:
- systemd service files
- Docker Compose manifests
- Kubernetes manifests
- TLS certificates or secret management
- rolling upgrade orchestration

If you want to package it further, treat the current shell scripts as the baseline reference behavior and add production controls around process supervision, security, and recovery.
