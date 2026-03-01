#!/usr/bin/env bash
# demo.sh — Start 3 LB nodes + 3 backends, open the dashboard, run brief traffic.
set -e

BIN="./bin/lb"
DATA="./data"

cleanup() {
  echo "==> Stopping all processes…"
  kill $(jobs -p) 2>/dev/null || true
  rm -rf "$DATA"
}
trap cleanup EXIT INT TERM

# Build if needed.
[ -f "$BIN" ] || make build

mkdir -p "$DATA"/{n1,n2,n3}

echo "==> Starting 3 backend servers…"
$BIN backend --id b1 --listen :9101 &
$BIN backend --id b2 --listen :9102 &
$BIN backend --id b3 --listen :9103 &
sleep 0.5

PEERS="n1=:10001,n2=:10002,n3=:10003"
HTTP_PEERS="n1=:9001,n2=:9002,n3=:9003"

echo "==> Starting 3 LB nodes…"
$BIN node --id n1 --http :9001 --raft :10001 \
  --peers "$PEERS" --http-peers "$HTTP_PEERS" --data "$DATA/n1" &
$BIN node --id n2 --http :9002 --raft :10002 \
  --peers "$PEERS" --http-peers "$HTTP_PEERS" --data "$DATA/n2" &
$BIN node --id n3 --http :9003 --raft :10003 \
  --peers "$PEERS" --http-peers "$HTTP_PEERS" --data "$DATA/n3" &

echo "==> Waiting for leader election (3s)…"
sleep 3

echo "==> Adding backends via admin API…"
$BIN admin add-backend --url http://localhost:9101 \
  --nodes "http://localhost:9001,http://localhost:9002,http://localhost:9003"
$BIN admin add-backend --url http://localhost:9102 \
  --nodes "http://localhost:9001,http://localhost:9002,http://localhost:9003"
$BIN admin add-backend --url http://localhost:9103 \
  --nodes "http://localhost:9001,http://localhost:9002,http://localhost:9003"

echo ""
echo "==> Dashboard: http://localhost:9001/"
echo "==> Dashboard: http://localhost:9002/"
echo "==> Dashboard: http://localhost:9003/"
echo ""

# Try to open browser.
if command -v xdg-open &>/dev/null; then
  xdg-open http://localhost:9001/ &
elif command -v open &>/dev/null; then
  open http://localhost:9001/ &
fi

echo "==> Sending 100 requests to demonstrate round-robin…"
for i in $(seq 1 100); do
  curl -s http://localhost:9001/hello >/dev/null &
done
wait

echo ""
echo "==> System running. Press Ctrl+C to stop."
wait
