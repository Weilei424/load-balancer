#!/usr/bin/env bash
# loadtest.sh — Send sustained traffic against all 3 LB nodes.
# Uses 'hey' if available, otherwise falls back to a simple Go-based generator.
set -e

DURATION="${DURATION:-30s}"
CONCURRENCY="${CONCURRENCY:-10}"
NODES="${NODES:-http://localhost:9001 http://localhost:9002 http://localhost:9003}"

if command -v hey &>/dev/null; then
  echo "==> Running hey load test (${DURATION} @ concurrency ${CONCURRENCY})…"
  for node in $NODES; do
    hey -z "$DURATION" -c "$CONCURRENCY" "$node/bench" &
  done
  wait
  echo "==> Load test complete."
else
  echo "==> 'hey' not found; running built-in curl loop for ${DURATION}…"
  END=$(( $(date +%s) + ${DURATION%s} ))
  i=0
  while [ "$(date +%s)" -lt "$END" ]; do
    for node in $NODES; do
      curl -s "$node/bench" >/dev/null &
    done
    (( i++ )) || true
    if (( i % 50 == 0 )); then
      wait
    fi
  done
  wait
  echo "==> Load test complete."
fi
