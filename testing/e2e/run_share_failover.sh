#!/usr/bin/env bash
# e2e：KIP-932 share 组多节点 failover——3 节点 RF=3，raft 引擎模式。
# setup：node0 上 share 组 poll + accept 前缀 → kill -9 node0 →
# verify：node1 上同组新成员续读后缀（游标经内部 topic 重放随协调器迁移恢复）。
# 前提：静态控制器模式无控制器 failover（见 run_group_failover.sh 注）。
set -u
BASE_PORT=9092
PID_DIR=$(mktemp -d /tmp/basalt-sf-XXXX)
DATA_DIR=$(mktemp -d /tmp/basalt-sf-data-XXXX)

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
  [ "${KEEP_DATA:-0}" = "1" ] || rm -rf "$PID_DIR" "$DATA_DIR"
}
trap cleanup EXIT

for i in 0 1 2; do
  PORT=$((BASE_PORT + i * 10))
  NODE_DIR="$DATA_DIR/node$i"
  BASALT_NODE_ID=$i BASALT_PORT=$PORT BASALT_DATA_DIR="$NODE_DIR" \
  BASALT_HOST=localhost BASALT_NUM_PARTITIONS=1 BASALT_RF=3 \
  BASALT_NODES="0=localhost:9092,1=localhost:9102,2=localhost:9112" BASALT_METRICS_PORT=0 \
  BASALT_ISR_LAG_MS=500 BASALT_CTRL_RAFT_ENGINE="${RAFTRS:-raftrs}" \
  BASALT_LOG_LEVEL="${BASALT_LOG_LEVEL:-info}" \
  ./target/debug/basalt-server > "$PID_DIR/node$i.log" 2>&1 &
  echo $! > "$PID_DIR/pid-$i"
done
sleep 2

RC=1
{
  echo "=== phase 1: node0 上 share 组 poll+accept 前缀 ==="
  (cd testing/franzgo && SHARE_DEBUG=1 go run ./share localhost:9092 failover-setup sf-grp sf-e2e 30,10) 2> /tmp/sf-setup-debug.log
  echo "=== 等待副本追平（2s）===" && sleep 2 &&
  echo "=== kill -9 node0（share 协调器所在）===" &&
  kill -9 "$(cat "$PID_DIR/pid-0")" &&
  # 等 promotion 完成（选举 + 分区升主 + 重放绑定 ≈ 3-8s）：franz-go share
  # 源在首个 metadata 视图钉死 fetch broker（preview 限制，不 re-seed）——
  # 客户端启动过早会钉到死节点
  sleep 10 &&
  echo "=== phase 2: node1 上同组续读（游标随协调器迁移恢复）===" &&
  (cd testing/franzgo && SHARE_DEBUG=1 go run ./share localhost:9102 failover-verify sf-grp sf-e2e 10,30) 2> /tmp/sf-verify-debug.log
} && RC=0

if [ $RC -eq 0 ]; then
  echo "PASS: share group coordinator failover (multi-node)"
else
  echo "FAIL: share failover"
  for i in 1 2; do
    echo "--- node$i ---"
    sed 's/\x1b\[[0-9;]*m//g' "$PID_DIR/node$i.log" 2>/dev/null | grep -E "WARN|ERROR|panic|bound" | tail -5
  done
fi
exit $RC
