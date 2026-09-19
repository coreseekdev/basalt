#!/usr/bin/env bash
# e2e：per-topic retention 透传——CreateTopics configs（retention.ms）→
# ClusterRecord → actor LogOptions；未配置题不受影响（per-topic 优先）。
set -u
PORT="${PORT:-9092}"
DATA=$(mktemp -d /tmp/basalt-pt-ret-XXXX)
LOG=$(mktemp /tmp/basalt-pt-ret-log-XXXX)

for P in $(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | sort -u); do
  kill -9 "$P" 2>/dev/null
done
sleep 0.5

BASALT_DATA_DIR="$DATA" BASALT_PORT="$PORT" BASALT_NUM_PARTITIONS=1 \
  BASALT_METRICS_PORT=0 BASALT_SEGMENT_MAX_BYTES=4096 \
  BASALT_RETENTION_SWEEP_MS=1000 \
  BASALT_LOG_LEVEL="${BASALT_LOG_LEVEL:-info}" \
  ./target/debug/basalt-server > "$LOG" 2>&1 &
PID=$!
trap 'kill -9 $PID 2>/dev/null; wait $PID 2>/dev/null; rm -rf "$DATA" "$LOG"' EXIT
sleep 1

RC=1
{
  python3 testing/e2e/retention_pertopic.py setup &&
  echo "=== sleeping 4s（retention 1.5s 窗口）===" &&
  sleep 4 &&
  python3 testing/e2e/retention_pertopic.py seal &&
  echo "=== sleeping 2s（sweep 执行）===" &&
  sleep 2 &&
  python3 testing/e2e/retention_pertopic.py verify
} && RC=0

if [ $RC -eq 0 ]; then
  echo "PASS: per-topic retention passthrough"
else
  echo "FAIL: per-topic retention"
  sed 's/\x1b\[[0-9;]*m//g' "$LOG" | grep -E "WARN|ERROR|panick|retention" | tail -8
fi
exit $RC
