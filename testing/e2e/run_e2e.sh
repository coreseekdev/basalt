#!/usr/bin/env bash
# e2e 跑批：起 broker → 跑测试 → 收尾。用法: run_e2e.sh <script.py> [port]
set -u
SCRIPT="${1:-basic_produce_consume.py}"
PORT="${2:-9092}"
DATA=$(mktemp -d /tmp/basalt-e2e-XXXX)
LOG=$(mktemp /tmp/basalt-e2e-log-XXXX)

# 清理占用端口的遗留 broker
OLD=$(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
[ -n "$OLD" ] && kill -9 "$OLD" 2>/dev/null && sleep 0.5

BASALT_DATA_DIR="$DATA" BASALT_PORT="$PORT" BASALT_NUM_PARTITIONS="${BASALT_NUM_PARTITIONS:-2}" \
  BASALT_LOG_LEVEL="${BASALT_LOG_LEVEL:-info}" \
  ./target/debug/basalt-server > "$LOG" 2>&1 &
PID=$!
trap 'kill -9 $PID 2>/dev/null; wait $PID 2>/dev/null' EXIT
sleep 1

python3 "testing/e2e/$SCRIPT"
RC=$?
if [ $RC -ne 0 ]; then
  echo "--- server log tail ---"
  sed 's/\x1b\[[0-9;]*m//g' "$LOG" | grep -E "WARN|ERROR|panick" | tail -8
fi
rm -rf "$DATA" "$LOG"
exit $RC
