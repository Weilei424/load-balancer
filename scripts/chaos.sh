#!/usr/bin/env bash
# chaos.sh — Kill and restart LB nodes randomly while load-testing.
set -e

BIN="./bin/lb"
DATA="./data"

cleanup() {
  echo "==> chaos: stopping all…"
  kill $(jobs -p) 2>/dev/null || true
}
trap cleanup EXIT INT TERM

make build
mkdir -p "$DATA"/{n1,n2,n3}

# Start backends.
$BIN backend --id b1 --listen :9101 &
$BIN backend --id b2 --listen :9102 &
$BIN backend --id b3 --listen :9103 &
sleep 0.5

# Start LB nodes and record PIDs.
declare -A NODE_PIDS
start_node() {
  local id=$1
  $BIN node --config "configs/${id}.yaml" &
  NODE_PIDS[$id]=$!
}

start_node n1
start_node n2
start_node n3

echo "==> Waiting for leader election (4s)…"
sleep 4

# Add backends.
$BIN admin add-backend --url http://localhost:9101 \
  --nodes "http://localhost:9001,http://localhost:9002,http://localhost:9003" 2>/dev/null || true
$BIN admin add-backend --url http://localhost:9102 \
  --nodes "http://localhost:9001,http://localhost:9002,http://localhost:9003" 2>/dev/null || true
$BIN admin add-backend --url http://localhost:9103 \
  --nodes "http://localhost:9001,http://localhost:9002,http://localhost:9003" 2>/dev/null || true

echo "==> Starting load test in background…"
bash scripts/loadtest.sh &

echo "==> Starting chaos loop (kill/restart nodes every 2–5s)…"
ROUNDS="${CHAOS_ROUNDS:-10}"
for round in $(seq 1 "$ROUNDS"); do
  sleep $(( RANDOM % 4 + 2 ))
  target="n$(( (RANDOM % 3) + 1 ))"
  pid=${NODE_PIDS[$target]}
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    echo "==> [chaos round $round] Killing $target (pid $pid)"
    kill "$pid" 2>/dev/null || true
    sleep 1
    echo "==> [chaos round $round] Restarting $target"
    start_node "$target"
    echo "==> $target restarted (pid ${NODE_PIDS[$target]})"
  fi
done

echo "==> Chaos done. Letting load test finish…"
wait
echo "==> Chaos run complete."
