# Usage Guide

This guide covers how to build the binary, run the load balancer manually, and use the admin and observability endpoints.

## Build

From the repository root:

```bash
make build
```

This produces:

```bash
./bin/lb
```

## CLI Modes

The project uses one binary with three subcommands:

```bash
lb node
lb backend
lb admin
```

## Run Backend Servers

Start one or more test backends:

```bash
./bin/lb backend --id b1 --listen :9101
./bin/lb backend --id b2 --listen :9102
./bin/lb backend --id b3 --listen :9103
```

Behavior:
- `GET /health` returns a simple JSON health payload
- all other paths return a JSON response with backend ID, method, and path

## Run Load Balancer Nodes

Run three LB nodes in separate terminals.

Node 1:

```bash
./bin/lb node \
  --id n1 \
  --http :9001 \
  --raft :10001 \
  --peers "n1=:10001,n2=:10002,n3=:10003" \
  --http-peers "n1=:9001,n2=:9002,n3=:9003" \
  --data ./data/n1
```

Node 2:

```bash
./bin/lb node \
  --id n2 \
  --http :9002 \
  --raft :10002 \
  --peers "n1=:10001,n2=:10002,n3=:10003" \
  --http-peers "n1=:9001,n2=:9002,n3=:9003" \
  --data ./data/n2
```

Node 3:

```bash
./bin/lb node \
  --id n3 \
  --http :9003 \
  --raft :10003 \
  --peers "n1=:10001,n2=:10002,n3=:10003" \
  --http-peers "n1=:9001,n2=:9002,n3=:9003" \
  --data ./data/n3
```

Notes:
- `--peers` is the Raft RPC address list
- `--http-peers` is used for leader redirect behavior on admin requests
- the local node is automatically removed from each peer map internally
- `--data` stores `state.json` and `log.jsonl`
- `--health-interval` defaults to `5s`

## Add Backends

After the cluster elects a leader, add backends through the admin CLI:

```bash
./bin/lb admin add-backend --url http://localhost:9101
./bin/lb admin add-backend --url http://localhost:9102
./bin/lb admin add-backend --url http://localhost:9103
```

The admin client tries a list of node HTTP addresses and follows follower redirects to the leader.

To target a specific set of nodes:

```bash
./bin/lb admin add-backend \
  --url http://localhost:9101 \
  --nodes "http://localhost:9001,http://localhost:9002,http://localhost:9003"
```

## Change Configuration

Remove a backend:

```bash
./bin/lb admin remove-backend --url http://localhost:9103
```

Set a backend weight:

```bash
./bin/lb admin set-weight --url http://localhost:9101 --weight 2
```

Switch routing algorithm:

```bash
./bin/lb admin set-algorithm --algorithm least_conn
./bin/lb admin set-algorithm --algorithm round_robin
```

## Send Traffic

Once at least one healthy backend exists, requests sent to any LB node are proxied:

```bash
curl http://localhost:9001/hello
curl http://localhost:9002/api/test
curl http://localhost:9003/bench
```

If no healthy backends are configured, the proxy returns:

```json
{"error":"no healthy backends configured"}
```

## Dashboard And Metrics

Per-node HTTP endpoints:

- `/` or `/dashboard`: dashboard HTML
- `/events`: live SSE stream
- `/metrics`: Prometheus-style metrics text
- `/admin/backends`: backend management
- `/admin/algorithm`: algorithm management

Per-node Raft endpoint:

- `/raft/status`: current node role, term, leader, and log status

Examples:

```bash
curl http://localhost:9001/metrics
curl http://localhost:10001/raft/status
```

## Fastest Way To See It Running

Use the scripted demo:

```bash
make demo
```

This starts:
- 3 backend processes
- 3 LB node processes
- initial backend registration
- a short burst of traffic

Then it leaves the system running until you stop it with `Ctrl+C`.
