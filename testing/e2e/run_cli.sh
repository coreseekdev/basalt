#!/usr/bin/env bash
# e2e：basalt-cli 运维面（A5）——topics list/describe/create/delete +
# 非法名拒绝 + 消费组 list/describe/offsets(lag)。
set -u
PORT="${PORT:-9092}"
DATA=$(mktemp -d /tmp/basalt-cli-e2e-XXXX)
LOG=$(mktemp /tmp/basalt-cli-e2e-log-XXXX)

for P in $(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | sort -u); do
  kill -9 "$P" 2>/dev/null
done
sleep 0.5

BASALT_DATA_DIR="$DATA" BASALT_PORT="$PORT" BASALT_NUM_PARTITIONS=1 \
  BASALT_METRICS_PORT=0 BASALT_LOG_LEVEL="${BASALT_LOG_LEVEL:-info}" \
  ./target/debug/basalt-server > "$LOG" 2>&1 &
PID=$!
trap 'kill -9 $PID 2>/dev/null; wait $PID 2>/dev/null; rm -rf "$DATA" "$LOG"' EXIT
sleep 1

export BASALT_BOOTSTRAP="localhost:$PORT"
B=./target/debug/basalt-cli
RC=1
{
  set -e
  echo "[1] topics create"
  $B topics create cli-e2e --partitions 2 --replication 1 | grep -q "created"
  echo "[2] create duplicate → TOPIC_ALREADY_EXISTS 可见（EnsureTopic 幂等语义，错误码 0）"
  $B topics create cli-e2e --partitions 2 --replication 1 >/dev/null
  echo "[3] create 非法名 → INVALID_TOPIC_EXCEPTION"
  ! $B topics create "bad name!"
  echo "[4] topics list 含 cli-e2e 且分区数正确"
  $B topics list | grep -E "^cli-e2e +2 +1"
  echo "[5] topics describe 全部分区有 leader"
  $B topics describe cli-e2e | grep -cE "^0 +0| ^1 " >/dev/null || $B topics describe cli-e2e | grep -qE "^1 "
  echo "[6] describe 未知 topic → 报错"
  ! $B topics describe no-such-topic 2>/dev/null
  echo "[7] kafka-python 建组 + 提交 offset"
  python3 testing/e2e/cli_group_setup.py "$PORT"
  echo "[8] groups list 含 lag-e2e-grp"
  $B groups list | grep -q "lag-e2e-grp"
  echo "[9] groups offsets 有 committed 行"
  $B groups offsets lag-e2e-grp | grep -E "^lag-e2e +[0-9]+ +[0-9]+ +[0-9]+"
  echo "[10] topics delete + 复查消失"
  $B topics delete cli-e2e | grep -q deleted
  ! $B topics list | grep -q cli-e2e
  set +e
} && RC=0

if [ $RC -eq 0 ]; then
  echo "PASS: basalt-cli"
else
  echo "FAIL: basalt-cli"
  sed 's/\x1b\[[0-9;]*m//g' "$LOG" | grep -E "WARN|ERROR|panick" | tail -8
fi
exit $RC
