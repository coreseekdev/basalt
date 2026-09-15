#!/usr/bin/env bash
# 3 节点集群编排：拉起 → e2e → 收尾（含 kill -9 failover 注入）
set -u
BASE_PORT=9092
PID_DIR=$(mktemp -d /tmp/basalt-mn-XXXX)
DATA_DIR=$(mktemp -d /tmp/basalt-mn-data-XXXX)
export PID_DIR DATA_DIR

# 清理遗留 broker（三端口 + 内部端口）
for PORT in 9092 9102 9112 9093 9103 9113; do
  OLD=$(fuser $PORT/tcp 2>/dev/null)
  [ -n "$OLD" ] && kill -9 $OLD 2>/dev/null
done
sleep 0.5

cleanup() {
  for f in "$PID_DIR"/*; do
    [ -f "$f" ] && kill -9 "$(cat "$f")" 2>/dev/null
  done
  cp "$PID_DIR"/node*.log /tmp/ 2>/dev/null || true
  # 调试：保留 node 日志
cp "$PID_DIR"/node*.log /tmp/ 2>/dev/null || true
rm -rf "$PID_DIR" "$DATA_DIR"
}
trap cleanup EXIT

# 起控制器候选（node0 即控制器）
for i in 0 1 2; do
  PORT=$((BASE_PORT + i * 10))
  NODE_DIR="$DATA_DIR/node$i"
  BASALT_NODE_ID=$i BASALT_PORT=$PORT BASALT_DATA_DIR="$NODE_DIR" \
  BASALT_HOST=localhost BASALT_NUM_PARTITIONS=2 BASALT_RF=3 \
  BASALT_NODES="0=localhost:9092,1=localhost:9102,2=localhost:9112" BASALT_METRICS_PORT=0 \
  BASALT_CTRL_RAFT_ENGINE="${RAFTRS:-}" BASALT_LOG_LEVEL="${BASALT_LOG_LEVEL:-info}" \
  ./target/debug/basalt-server > "$PID_DIR/node$i.log" 2>&1 &
  echo $! > "$PID_DIR/pid-$i"
  echo $! > "$PID_DIR/port-$PORT"
done
sleep 2

SCENARIO="${1:-multinode_failover.py}"
# RAFTRS=1 时全节点启用 raft 引擎 runtime
if [ "${RAFTRS:-0}" = "1" ]; then export RAFTRS=1; fi
KILL_BY_PORT=1 BROKERS="localhost:9092,localhost:9102,localhost:9112" \
  python3 "testing/e2e/$SCENARIO"
RC=$?

echo "--- node logs (WARN/ERROR/FAILOVER) ---"
for i in 0 1 2; do
  echo "node$i:"
  sed 's/\x1b\[[0-9;]*m//g' "$PID_DIR/node$i.log" | grep -E "WARN|ERROR|FAILOVER|panic" | head -6
done
exit $RC
