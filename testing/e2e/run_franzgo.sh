#!/usr/bin/env bash
# franz-go 档 e2e：起 broker → 跑 testing/franzgo → 收尾。
set -u
PORT="${1:-9092}"
DATA=$(mktemp -d /tmp/basalt-franzgo-XXXX)
LOG=$(mktemp /tmp/basalt-franzgo-log-XXXX)

OLD=$(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
[ -n "$OLD" ] && kill -9 "$OLD" 2>/dev/null && sleep 0.5

BASALT_DATA_DIR="$DATA" BASALT_PORT="$PORT" BASALT_NUM_PARTITIONS=2 BASALT_METRICS_PORT=0 \
  BASALT_LOG_LEVEL="${BASALT_LOG_LEVEL:-info}" \
  ./target/debug/basalt-server > "$LOG" 2>&1 &
PID=$!
trap 'kill -9 $PID 2>/dev/null; wait $PID 2>/dev/null' EXIT
sleep 1

(cd testing/franzgo && go run .)
RC=$?
if [ $RC -ne 0 ]; then
  echo "--- server log tail ---"
  sed 's/\x1b\[[0-9;]*m//g' "$LOG" | grep -E "WARN|ERROR|panick" | tail -8
fi
rm -rf "$DATA" "$LOG"
exit $RC
