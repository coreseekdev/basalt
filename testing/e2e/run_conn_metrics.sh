#!/usr/bin/env bash
# e2e：连接级指标（A4）——活跃连接 gauge + 每连接字节累计 + 关闭清理。
set -u
PORT="${PORT:-9092}"
DATA=$(mktemp -d /tmp/basalt-conngauge-XXXX)
LOG=$(mktemp /tmp/basalt-conngauge-log-XXXX)

for P in $(ss -tlnp 2>/dev/null | grep -E ":$PORT |:9094 " | grep -oP 'pid=\K[0-9]+' | sort -u); do
  kill -9 "$P" 2>/dev/null
done
sleep 0.5

BASALT_DATA_DIR="$DATA" BASALT_PORT="$PORT" BASALT_NUM_PARTITIONS=1 \
  BASALT_METRICS_PORT="${BASALT_METRICS_PORT:-9094}" BASALT_LOG_LEVEL="${BASALT_LOG_LEVEL:-info}" \
  ./target/debug/basalt-server > "$LOG" 2>&1 &
PID=$!
trap 'kill -9 $PID 2>/dev/null; wait $PID 2>/dev/null; rm -rf "$DATA" "$LOG"' EXIT
sleep 1

if python3 testing/e2e/conn_metrics.py; then
  echo "PASS: connection-level metrics"
else
  echo "FAIL: connection-level metrics"
  sed 's/\x1b\[[0-9;]*m//g' "$LOG" | grep -E "WARN|ERROR|panick" | tail -8
  exit 1
fi
