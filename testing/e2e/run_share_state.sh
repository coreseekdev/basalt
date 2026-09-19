#!/usr/bin/env bash
# e2e：share 组状态持久化迁移（B4）——ACK 事件落 __share_group_state
# （内部 topic produce 路径），kill -9 重启后从分区重放恢复交付游标，
# accepted 记录不重投；本地 share-state/*.json 不再创建。
set -u
PORT="${SHARE_PORT:-9092}"
DATA=$(mktemp -d /tmp/basalt-share-b4-XXXX)
LOG=$(mktemp /tmp/basalt-share-b4-log-XXXX)

for P in $(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | sort -u); do
  kill -9 "$P" 2>/dev/null
done
sleep 0.5

start_broker() {
  BASALT_DATA_DIR="$DATA" BASALT_PORT="$PORT" BASALT_NUM_PARTITIONS=1 BASALT_METRICS_PORT=0 \
    BASALT_SHARE_LOCK_MS=30000 \
    BASALT_LOG_LEVEL="${BASALT_LOG_LEVEL:-warn}" \
    ./target/debug/basalt-server >> "$LOG" 2>&1 &
  PID=$!
}
stop_broker() {
  kill -9 "$PID" 2>/dev/null; wait "$PID" 2>/dev/null
}
trap 'stop_broker; rm -rf "$DATA" "$LOG"' EXIT

start_broker
for _ in $(seq 1 40); do
  (exec 3<>/dev/tcp/localhost/$PORT) 2>/dev/null && exec 3>&- && break
  sleep 0.25
done
sleep 1.5

RC=1
{
  echo "=== phase 1: full share flow（accept/release/reject 全链）==="
  (cd testing/franzgo && go run ./share "$PORT")
  echo "=== kill -9 → 同数据目录重启 ==="
  stop_broker
  sleep 1
  start_broker
  sleep 2
  [ -d "$DATA/share-state" ] && { echo "FAIL: share-state JSON 目录不应再创建"; exit 1; }
  echo "[x] 本地 share-state/*.json 不存在（权威存储 = 内部 topic）✓"
  echo "=== phase 2: verify-persist（重启后 accepted 不重投）==="
  (cd testing/franzgo && go run ./share "$PORT" verify-persist)
} && RC=0

if [ $RC -eq 0 ]; then
  echo "PASS: share state authoritative store (internal topic)"
else
  echo "FAIL: share state"
  sed 's/\x1b\[[0-9;]*m//g' "$LOG" | grep -E "WARN|ERROR|panic|share" | tail -8
fi
exit $RC
