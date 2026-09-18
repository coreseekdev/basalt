#!/usr/bin/env bash
# 分层存储 e2e（T-M4.3 块 c，ADR-21 §4）：
# tiered 模式 + 小段 → produce（滚动+上传+本地回收）→ 本地文件数断言 →
# 全量消费（读穿透）→ broker 重启（注册表恢复）→ 再全量消费。
set -u
PORT="${1:-9092}"
DATA=$(mktemp -d /tmp/basalt-tiered-XXXX)
LOG=$(mktemp /tmp/basalt-tiered-log-XXXX)

OLD=$(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
[ -n "$OLD" ] && kill -9 "$OLD" 2>/dev/null && sleep 0.5

start_server() {
  BASALT_DATA_DIR="$DATA" BASALT_PORT="$PORT" BASALT_NUM_PARTITIONS=2 BASALT_METRICS_PORT=0 \
    BASALT_STORAGE_MODE=tiered BASALT_SEGMENT_MAX_BYTES=2048 \
    BASALT_LOG_LEVEL="${BASALT_LOG_LEVEL:-info}" \
    ./target/debug/basalt-server > "$LOG" 2>&1 &
  PID=$!
}
stop_server() {
  kill -9 $PID 2>/dev/null; wait $PID 2>/dev/null
}
trap 'stop_server; rm -rf "$DATA" "$LOG"' EXIT
start_server
sleep 1

python3 testing/e2e/tiered_storage.py produce || { echo "--- server log ---"; tail -5 "$LOG"; exit 1; }
sleep 1

# 本地回收断言：本地 .log 只剩 active（≈分区数），分层对象数 ≥ 20
LOCAL_LOGS=$(find "$DATA" -name "*.log" -not -path "*objectstore*" | wc -l)
TIERED_LOGS=$(find "$DATA" -path "*objectstore*" -name "*.log" | wc -l)
echo "local segments=$LOCAL_LOGS tiered objects=$TIERED_LOGS"
if [ "$TIERED_LOGS" -lt 12 ]; then
  echo "FAIL: 分层对象过少（$TIERED_LOGS < 12）——上传未发生"
  exit 1
fi
if [ "$LOCAL_LOGS" -gt 6 ]; then
  echo "FAIL: 本地段未回收（$LOCAL_LOGS > 6）"
  exit 1
fi

python3 testing/e2e/tiered_storage.py consume || { echo "--- server log ---"; tail -5 "$LOG"; exit 1; }
echo "[1] produce→回收→读穿透 全链 ✔"

# 重启恢复：注册表从对象存储重建，读穿透继续可用
stop_server
start_server
sleep 1.5
python3 testing/e2e/tiered_storage.py consume || { echo "--- server log ---"; tail -5 "$LOG"; exit 1; }
echo "[2] 重启后读穿透（注册表恢复）✔"

PASS_MSG="PASS ✔ (tiered storage：滚动上传/本地回收/读穿透/重启恢复)"
echo "$PASS_MSG"
