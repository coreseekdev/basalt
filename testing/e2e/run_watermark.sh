#!/usr/bin/env bash
# e2e：磁盘前置水位（A2）。
# 阶段 1：BASALT_DISK_WATERMARK_PCT=1（任何真实磁盘必越过）+ sweep 500ms
#         → produce 必须被拒（read_only 前置降级）。
# 阶段 2：默认水位 95%（健康磁盘）→ produce 正常放行（默认档不误伤）。
set -u
PORT="${PORT:-9092}"
LOG=$(mktemp /tmp/basalt-watermark-log-XXXX)

# 清理占用端口的遗留 broker
OLD=$(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
[ -n "$OLD" ] && kill -9 "$OLD" 2>/dev/null && sleep 0.5

start_broker() { # $1 = watermark pct
  BASALT_DATA_DIR=$(mktemp -d /tmp/basalt-wm-XXXX) BASALT_PORT="$PORT" BASALT_NUM_PARTITIONS=1 \
    BASALT_METRICS_PORT=0 BASALT_RETENTION_SWEEP_MS=500 BASALT_DISK_WATERMARK_PCT="$1" \
    BASALT_LOG_LEVEL="${BASALT_LOG_LEVEL:-info}" \
    ./target/debug/basalt-server > "$LOG" 2>&1 &
  PID=$!
}
stop_broker() {
  kill -9 "$PID" 2>/dev/null; wait "$PID" 2>/dev/null
  rm -rf /tmp/basalt-wm-*
}
trap 'stop_broker; rm -f "$LOG"' EXIT

RC=1
{
  echo "=== phase 1: watermark=1% → produce must be rejected ==="
  start_broker 1
  sleep 1.5
  python3 testing/e2e/watermark_reject.py reject &&
  grep -q "disk over watermark" "$LOG" &&
  echo "[log] watermark gate confirmed" &&
  echo "=== phase 2: default watermark=95% → produce accepted ==="
  stop_broker
  start_broker 95
  sleep 1.5
  python3 testing/e2e/watermark_reject.py accept
} && RC=0

if [ $RC -ne 0 ]; then
  echo "--- server log tail ---"
  sed 's/\x1b\[[0-9;]*m//g' "$LOG" | grep -E "WARN|ERROR|panick" | tail -8
else
  echo "PASS: disk watermark proactive read-only"
fi
exit $RC
