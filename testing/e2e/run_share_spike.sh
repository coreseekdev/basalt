#!/usr/bin/env bash
# T-M3.6 spike：share groups（KIP-932）franz-go 真客户端全链——
# heartbeat/fetch/acknowledge + accept 不重投 + release 重投递 + DeliveryCount。
set -u
PORT="${SHARE_PORT:-9092}"
DATA=$(mktemp -d /tmp/basalt-share-XXXX)
LOG=$(mktemp /tmp/basalt-share-log-XXXX)

OLD=$(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
[ -n "$OLD" ] && kill -9 "$OLD" 2>/dev/null && sleep 0.5

BASALT_DATA_DIR="$DATA" BASALT_PORT="$PORT" BASALT_NUM_PARTITIONS=1 BASALT_METRICS_PORT=0 \
  BASALT_SHARE_LOCK_MS=30000 \
  BASALT_LOG_LEVEL="${BASALT_LOG_LEVEL:-warn}" \
  ./target/debug/basalt-server > "$LOG" 2>&1 &
PID=$!
trap 'kill -9 $PID 2>/dev/null; wait $PID 2>/dev/null; rm -rf "$DATA" "$LOG"' EXIT
for _ in $(seq 1 40); do
  (exec 3<>/dev/tcp/localhost/$PORT) 2>/dev/null && exec 3>&- && break
  sleep 0.25
done
sleep 0.5

cd testing/franzgo && go run ./share "$PORT"
RC=${PIPESTATUS[0]}
cd ../..
if [ $RC -ne 0 ]; then
  echo "--- server log ---"
  sed 's/\x1b\[[0-9;]*m//g' "$LOG" | grep -E "WARN|ERROR|panic" | tail -6
  exit $RC
fi

# [5] 持久化验收：同 DATA 目录重启（state 在 DATA/share-state），已 accepted
# 的 40 条不得重投
kill -9 $PID 2>/dev/null; wait $PID 2>/dev/null
BASALT_DATA_DIR="$DATA" BASALT_PORT="$PORT" BASALT_NUM_PARTITIONS=1 BASALT_METRICS_PORT=0 \
  BASALT_SHARE_LOCK_MS=30000 \
  BASALT_LOG_LEVEL="${BASALT_LOG_LEVEL:-warn}" \
  ./target/debug/basalt-server > "$LOG" 2>&1 &
PID=$!
for _ in $(seq 1 40); do
  (exec 3<>/dev/tcp/localhost/$PORT) 2>/dev/null && exec 3>&- && break
  sleep 0.25
done
sleep 1.5
cd testing/franzgo && go run ./share "$PORT" verify-persist
RC2=${PIPESTATUS[0]}
cd ../..
[ $RC2 -ne 0 ] && { echo "--- server log ---"; sed 's/\x1b\[[0-9;]*m//g' "$LOG" | grep -E "WARN|ERROR|panic" | tail -6; exit $RC2; }
exit 0
