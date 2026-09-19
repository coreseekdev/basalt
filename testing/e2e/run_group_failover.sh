#!/usr/bin/env bash
# e2e：组协调器 failover（B3，方案 B 块 b3）——3 节点 RF=3。
# 组协调器 = __basalt_group_state p0 leader：kill -9 该节点（兼控制器）→
# raft 引擎选举新控制器 → 新 leader 监督循环重放副本数据 → 位点恢复 +
# 新协调器可提交。默认 RAFTRS=raftrs（引擎模式）：静态控制器模式下
# 控制器节点不可替代，无 failover 语义（见 run_multinode.sh RAFTRS）。
set -u
BASE_PORT=9092
PID_DIR=$(mktemp -d /tmp/basalt-gf-XXXX)
DATA_DIR=$(mktemp -d /tmp/basalt-gf-data-XXXX)

for PORT in 9092 9102 9112; do
  OLD=$(fuser $PORT/tcp 2>/dev/null)
  [ -n "$OLD" ] && kill -9 $OLD 2>/dev/null
done
sleep 0.5

cleanup() {
  for f in "$PID_DIR"/pid-*; do
    [ -f "$f" ] && kill -9 "$(cat "$f")" 2>/dev/null
  done
  cp "$PID_DIR"/node*.log /tmp/ 2>/dev/null || true
  rm -rf "$PID_DIR" "$DATA_DIR"
}
trap cleanup EXIT

for i in 0 1 2; do
  PORT=$((BASE_PORT + i * 10))
  NODE_DIR="$DATA_DIR/node$i"
  BASALT_NODE_ID=$i BASALT_PORT=$PORT BASALT_DATA_DIR="$NODE_DIR" \
  BASALT_HOST=localhost BASALT_NUM_PARTITIONS=2 BASALT_RF=3 \
  BASALT_NODES="0=localhost:9092,1=localhost:9102,2=localhost:9112" BASALT_METRICS_PORT=0 \
  BASALT_CTRL_RAFT_ENGINE="${RAFTRS:-raftrs}" \
  BASALT_ISR_LAG_MS=500 BASALT_LOG_LEVEL="${BASALT_LOG_LEVEL:-info}" \
  ./target/debug/basalt-server > "$PID_DIR/node$i.log" 2>&1 &
  echo $! > "$PID_DIR/pid-$i"
  echo $! > "$PID_DIR/port-$PORT"
done
sleep 2

RC=1
{
  python3 testing/e2e/group_failover_scenario.py setup &&
  echo "=== kill -9 node0（组协调器所在）===" &&
  kill -9 "$(cat "$PID_DIR/pid-0")" &&
  sleep 1 &&
  python3 testing/e2e/group_failover_scenario.py verify
} && RC=0

if [ $RC -eq 0 ]; then
  echo "PASS: group coordinator failover (replay on promoted leader)"
else
  echo "FAIL: group coordinator failover"
  for i in 1 2; do
    echo "--- node$i ---"
    sed 's/\x1b\[[0-9;]*m//g' "$PID_DIR/node$i.log" 2>/dev/null | grep -E "WARN|ERROR|panic|bound|replay" | tail -6
  done
fi
exit $RC
