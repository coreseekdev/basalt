#!/usr/bin/env bash
# KIP-848 新消费组协议 e2e（T-M3.3 块 c，ADR-19 §6c）：
# 起 broker（宣吿 68 v1）→ 跑 testing/franzgo/kip848 → 收尾。
# 验收面：混布收敛 / 增量 rebalance 无停等 / 双隔离级消费。
set -u
PORT="${1:-9092}"
DATA=$(mktemp -d /tmp/basalt-kip848-XXXX)
LOG=$(mktemp /tmp/basalt-kip848-log-XXXX)

OLD=$(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
[ -n "$OLD" ] && kill -9 "$OLD" 2>/dev/null && sleep 0.5

BASALT_DATA_DIR="$DATA" BASALT_PORT="$PORT" BASALT_NUM_PARTITIONS=2 BASALT_METRICS_PORT=0 \
  BASALT_LOG_LEVEL="${BASALT_LOG_LEVEL:-info}" \
  ./target/debug/basalt-server > "$LOG" 2>&1 &
PID=$!
trap 'kill -9 $PID 2>/dev/null; wait $PID 2>/dev/null' EXIT
sleep 1

(cd testing/franzgo && go run ./kip848)
RC=$?
if [ $RC -ne 0 ]; then
  echo "--- server log tail ---"
  sed 's/\x1b\[[0-9;]*m//g' "$LOG" | grep -E "WARN|ERROR|panick" | tail -8
fi
rm -rf "$DATA" "$LOG"
exit $RC
