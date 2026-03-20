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

### Using config files (recommended)

The repository ships with ready-made config files for the three demo nodes. Run each in a separate terminal:

```bash
./bin/lb node --config configs/n1.yaml
./bin/lb node --config configs/n2.yaml
./bin/lb node --config configs/n3.yaml
```

Config files must supply `id`, `http`, and `raft` — missing required fields produce a startup error. CLI flags override any value from the file; for example:

```bash
./bin/lb node --config configs/n1.yaml --health-interval 10s
```

### Using flags directly

All settings can be provided as flags without a config file:

```bash
./bin/lb node \
  --id n1 \
  --http :9001 \
  --raft :10001 \
  --peers "n2=:10002,n3=:10003" \
  --http-peers "n2=:9002,n3=:9003" \
  --data ./data/n1
```

Notes:
- `--peers` is the Raft RPC address list (omit self)
- `--http-peers` is used for leader redirect behavior on admin requests (omit self)
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

To send directly to a known leader address (skipping node discovery):

```bash
./bin/lb admin add-backend \
  --url http://localhost:9101 \
  --leader http://localhost:9001
```

If `--leader` points at a follower, the 307 redirect is still followed automatically.

## Change Configuration

Remove a backend:

```bash
./bin/lb admin remove-backend --url http://localhost:9103
```

Set a backend weight (higher weight means proportionally more traffic under weighted round-robin):

```bash
./bin/lb admin set-weight --url http://localhost:9101 --weight 2
./bin/lb admin set-weight --url http://localhost:9102 --weight 1
# Result: ~67% of requests go to 9101, ~33% to 9102
```

Use `--leader` to send directly:

```bash
./bin/lb admin set-weight --url http://localhost:9101 --weight 3 --leader http://localhost:9001
```

Switch routing algorithm:

```bash
./bin/lb admin set-algorithm --algorithm least_conn
./bin/lb admin set-algorithm --algorithm round_robin
./bin/lb admin set-algorithm --algorithm consistent_hash
```

With `consistent_hash`, each client IP is mapped to a consistent backend via a CRC32 virtual-node ring. The same source IP always reaches the same backend as long as it is healthy. When a backend goes down the ring re-routes only the affected keys; all other clients stay pinned to their original backends.

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
