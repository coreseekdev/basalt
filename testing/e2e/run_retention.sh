#!/usr/bin/env bash
# e2e：本地日志 time-based retention（A3）—— BASALT_LOG_RETENTION_MS=1500、
# sweep 1s、4KB 段上限（保证滚出 sealed 段）。
# 时序：produce(100 条) → sleep 3.5s（跨 retention+sweep 窗口）→
#       fresh(10 条) → sleep 1.5s（等 sweep 跑到删除点）→ verify。
set -u
PORT="${PORT:-9092}"
DATA=$(mktemp -d /tmp/basalt-retention-XXXX)
LOG=$(mktemp /tmp/basalt-retention-log-XXXX)

# 清理占用端口的遗留 broker
OLD=$(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
[ -n "$OLD" ] && kill -9 "$OLD" 2>/dev/null && sleep 0.5

BASALT_DATA_DIR="$DATA" BASALT_PORT="$PORT" BASALT_NUM_PARTITIONS=1 \
  BASALT_METRICS_PORT=0 BASALT_SEGMENT_MAX_BYTES=4096 \
  BASALT_LOG_RETENTION_MS=1500 BASALT_RETENTION_SWEEP_MS=1000 \
  BASALT_LOG_LEVEL="${BASALT_LOG_LEVEL:-info}" \
  ./target/debug/basalt-server > "$LOG" 2>&1 &
PID=$!
trap 'kill -9 $PID 2>/dev/null; wait $PID 2>/dev/null; rm -rf "$DATA" "$LOG"' EXIT
sleep 1

RC=1
{
  python3 testing/e2e/retention_sweep.py produce &&
  echo "=== sleeping 3.5s (retention window) ===" && sleep 3.5 &&
  python3 testing/e2e/retention_sweep.py fresh &&
  echo "=== sleeping 1.5s (sweep tick) ===" && sleep 1.5 &&
  python3 testing/e2e/retention_sweep.py verify
} && RC=0

if [ $RC -eq 0 ]; then
  echo "PASS: time-based retention sweep"
else
  echo "FAIL: time-based retention"
  echo "--- server log tail ---"
  sed 's/\x1b\[[0-9;]*m//g' "$LOG" | grep -E "WARN|ERROR|panick|retention" | tail -10
fi
exit $RC
