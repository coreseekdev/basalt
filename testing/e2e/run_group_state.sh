#!/usr/bin/env bash
# e2e：组状态权威存储迁移（B2）——commit 落 __basalt_group_state（produce
# 路径，acks=all），本地 __consumer_offsets.log 不再是权威；kill -9 重启后
# 位点从内部 topic 重放恢复。
set -u
PORT="${PORT:-9092}"
DATA=$(mktemp -d /tmp/basalt-gs-XXXX)
LOG=$(mktemp /tmp/basalt-gs-log-XXXX)

for P in $(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | sort -u); do
  kill -9 "$P" 2>/dev/null
done
sleep 0.5

start_broker() {
  BASALT_DATA_DIR="$DATA" BASALT_PORT="$PORT" BASALT_NUM_PARTITIONS=1 \
    BASALT_METRICS_PORT=0 BASALT_LOG_LEVEL="${BASALT_LOG_LEVEL:-info}" \
    ./target/debug/basalt-server >> "$LOG" 2>&1 &
  PID=$!
}
stop_broker() {
  kill -9 "$PID" 2>/dev/null; wait "$PID" 2>/dev/null
}
trap 'stop_broker; rm -rf "$DATA" "$LOG"' EXIT

start_broker
sleep 1

RC=1
{
  python3 testing/e2e/group_state_e2e.py setup &&
  ls "$DATA/__consumer_offsets.log" 2>/dev/null && {
    echo "FAIL: 本地 offset 日志不应再被创建（权威存储已迁移）"
    exit 1
  }
  echo "[x] 本地 __consumer_offsets.log 不存在（权威存储 = 内部 topic）✓"
  echo "=== kill -9 → 同数据目录重启 ==="
  stop_broker
  sleep 1
  start_broker
  sleep 1
  python3 testing/e2e/group_state_e2e.py verify
} && RC=0

if [ $RC -eq 0 ]; then
  echo "PASS: group state authoritative store (internal topic)"
else
  echo "FAIL: group state"
  sed 's/\x1b\[[0-9;]*m//g' "$LOG" | grep -E "WARN|ERROR|panick|group state" | tail -10
fi
exit $RC
