#!/usr/bin/env bash
# e2e：内部口 HMAC 质询-应答（B1）——两节点同配 BASALT_INTERNAL_TOKEN：
# [1] python v2 握手（HMAC）+ heartbeat；[2] 明文握手 fail-closed；
# [3] 跨节点 CreateTopics（node1 → node0 控制器走 v2 内部链路）成功。
set -u
PID_DIR=$(mktemp -d /tmp/basalt-ia-XXXX)
DATA_DIR=$(mktemp -d /tmp/basalt-ia-data-XXXX)
export TOKEN_VALUE="test-secret-123"

for PORT in 9092 9102; do
  OLD=$(fuser $PORT/tcp 2>/dev/null)
  [ -n "$OLD" ] && kill -9 $OLD 2>/dev/null
done
sleep 0.5

cleanup() {
  for f in "$PID_DIR"/pid-*; do
    [ -f "$f" ] && kill -9 "$(cat "$f")" 2>/dev/null
  done
  rm -rf "$PID_DIR" "$DATA_DIR"
}
trap cleanup EXIT

for i in 0 1; do
  PORT=$((9092 + i * 10))
  BASALT_NODE_ID=$i BASALT_PORT=$PORT BASALT_DATA_DIR="$DATA_DIR/node$i" \
  BASALT_HOST=localhost BASALT_NUM_PARTITIONS=1 BASALT_RF=2 \
  BASALT_NODES="0=localhost:9092,1=localhost:9102" BASALT_METRICS_PORT=0 \
  BASALT_INTERNAL_TOKEN="$TOKEN_VALUE" BASALT_LOG_LEVEL="${BASALT_LOG_LEVEL:-info}" \
  ./target/debug/basalt-server > "$PID_DIR/node$i.log" 2>&1 &
  echo $! > "$PID_DIR/pid-$i"
done
sleep 2

RC=1
{
  python3 testing/e2e/internal_auth_probe.py positive &&
  python3 testing/e2e/internal_auth_probe.py negative &&
  echo "[3] 跨节点建题（node1 → node0 内部链路 v2 握手）" &&
  python3 - <<'EOF'
from kafka.admin import KafkaAdminClient, NewTopic
import time
admin = KafkaAdminClient(bootstrap_servers="localhost:9102", client_id="ia-e2e")
ok = False
for _ in range(10):
    try:
        admin.create_topics([NewTopic("ia-e2e", num_partitions=1, replication_factor=2)])
        ok = True
        break
    except Exception as e:
        time.sleep(1)
admin.close()
assert ok, "跨节点建题失败——内部 v2 握手链路异常"
print("create via node1 → node0 ✓")
EOF
} && RC=0

if [ $RC -eq 0 ]; then
  echo "PASS: internal plane HMAC auth (B1)"
else
  echo "FAIL: internal auth"
  sed 's/\x1b\[[0-9;]*m//g' "$PID_DIR"/node0.log | grep -E "WARN|ERROR|panic" | tail -6
fi
exit $RC
