#!/usr/bin/env bash
# 吞吐基准（无配额基线）：franz-go 直连面 produce/consume（默认 200MB）。
# 产出 MB/s 与 msgs/s 基线——配额改动（T-S5）与存储路径改动的回归对照。
# 默认 release 二进制（基线反映优化后吞吐；BASALT_BIN 可覆盖）。
set -u
PORT="${1:-9092}"
MSGS="${2:-200000}"
BIN="${BASALT_BIN:-./target/release/basalt-server}"
[ -x "$BIN" ] || BIN=./target/debug/basalt-server
DATA=$(mktemp -d /tmp/basalt-bench-XXXX)
LOG=$(mktemp /tmp/basalt-bench-log-XXXX)

OLD=$(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
[ -n "$OLD" ] && kill -9 "$OLD" 2>/dev/null && sleep 0.5

# 无配额、无鉴权：纯基线
BASALT_DATA_DIR="$DATA" BASALT_PORT="$PORT" BASALT_NUM_PARTITIONS=4 BASALT_METRICS_PORT=0 \
  BASALT_LOG_LEVEL="${BASALT_LOG_LEVEL:-warn}" \
  "$BIN" > "$LOG" 2>&1 &
PID=$!
trap 'kill -9 $PID 2>/dev/null; wait $PID 2>/dev/null; rm -rf "$DATA" "$LOG"' EXIT
for _ in $(seq 1 40); do
  (exec 3<>/dev/tcp/localhost/$PORT) 2>/dev/null && exec 3>&- && break
  sleep 0.25
done
sleep 0.5

echo "bench binary: $BIN"
cd testing/franzgo && go run ./bench "$PORT" "$MSGS"
RC=${PIPESTATUS[0]}
cd ../..
exit $RC
