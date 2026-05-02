#!/usr/bin/env bash
# gen_mock_log.sh — append synthetic log events to mock.log indefinitely.
#
# Usage:
#   ./gen_mock_log.sh              # append one event per second forever
#   ./gen_mock_log.sh 20           # stop after 20 events
#   ./gen_mock_log.sh 0 200        # forever, 200 ms between events

set -euo pipefail

LOGFILE="$(dirname "$0")/mock.log"
MAX="${1:-0}"          # 0 = run forever
INTERVAL_MS="${2:-1000}"
INTERVAL=$(echo "scale=3; $INTERVAL_MS / 1000" | bc)

LEVELS=(INFO INFO INFO DEBUG WARN ERROR DEBUG INFO)
MESSAGES=(
  "Request received method=GET path=/api/v1/events"
  "Cache hit key=session:99 store=redis"
  "Batch flushed size=50 duration_ms=12"
  "Heartbeat ok latency_ms=3"
  "Disk usage high percent=91 mount=/"
  "DB query slow duration_ms=340 query=SELECT"
  "Connection pool exhausted pool=db-primary"
  "Config reloaded source=outputs.conf"
)

count=0
echo "Appending to $LOGFILE  (Ctrl-C to stop)"
while true; do
  idx=$(( RANDOM % ${#LEVELS[@]} ))
  ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  echo "${ts} ${LEVELS[$idx]}  ${MESSAGES[$idx]}" >> "$LOGFILE"
  (( count++ )) || true
  if [[ "$MAX" -gt 0 && "$count" -ge "$MAX" ]]; then
    echo "Done — appended $count events."
    break
  fi
  sleep "$INTERVAL"
done
