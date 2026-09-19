#!/usr/bin/env bash
# e2e：fsync=always 档 durability——produce → kill -9 → 同数据目录重启 → 全量校验。
# 显式固定 BASALT_FSYNC=always（与 A1 之后的生产默认一致，不依赖默认值）。
set -u
PORT="${PORT:-9092}"
DATA=$(mktemp -d /tmp/basalt-durability-XXXX)
LOG=$(mktemp /tmp/basalt-durability-log-XXXX)

# 清理占用端口的遗留 broker
OLD=$(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
[ -n "$OLD" ] && kill -9 "$OLD" 2>/dev/null && sleep 0.5

start_broker() {
  BASALT_DATA_DIR="$DATA" BASALT_PORT="$PORT" BASALT_NUM_PARTITIONS="${BASALT_NUM_PARTITIONS:-2}" \
    BASALT_METRICS_PORT=0 BASALT_FSYNC=always BASALT_LOG_LEVEL="${BASALT_LOG_LEVEL:-info}" \
    ./target/debug/basalt-server >> "$LOG" 2>&1 &
  BROKER_PID=$!
}

stop_broker() {
  [ -n "${BROKER_PID:-}" ] && kill -9 "$BROKER_PID" 2>/dev/null
  wait "${BROKER_PID:-0}" 2>/dev/null
  BROKER_PID=""
}
trap 'stop_broker; rm -rf "$DATA" "$LOG"' EXIT

start_broker
sleep 1

echo "=== phase 1: produce (fsync=always) ==="
python3 testing/e2e/durability_kill9.py produce || {
  echo "--- server log tail ---"
  sed 's/\x1b\[[0-9;]*m//g' "$LOG" | grep -E "WARN|ERROR|panick" | tail -8
  exit 1
}

echo "=== kill -9 broker ==="
kill -9 "$BROKER_PID"
wait "$BROKER_PID" 2>/dev/null
BROKER_PID=""
sleep 1

echo "=== phase 2: restart on same data dir, verify ==="
start_broker
sleep 1
if python3 testing/e2e/durability_kill9.py verify; then
  echo "PASS: durability (kill -9 → restart → zero loss)"
  RC=0
else
  echo "FAIL: durability"
  echo "--- server log tail ---"
  sed 's/\x1b\[[0-9;]*m//g' "$LOG" | grep -E "WARN|ERROR|panick" | tail -8
  RC=1
fi
exit $RC
