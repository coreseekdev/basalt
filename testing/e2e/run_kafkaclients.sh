#!/usr/bin/env bash
# kafka-clients（Apache Java 官方栈）档 e2e：起 broker → mvn exec → 收尾。
# 账本 C11 第四客户端档（kafka-python / librdkafka / franz-go / kafka-clients）。
set -u
PORT="${1:-9092}"
DATA=$(mktemp -d /tmp/basalt-jclient-XXXX)
LOG=$(mktemp /tmp/basalt-jclient-log-XXXX)

OLD=$(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
[ -n "$OLD" ] && kill -9 "$OLD" 2>/dev/null && sleep 0.5

BASALT_DATA_DIR="$DATA" BASALT_PORT="$PORT" BASALT_NUM_PARTITIONS=2 BASALT_METRICS_PORT=0 \
  BASALT_LOG_LEVEL="${BASALT_LOG_LEVEL:-warn}" \
  ./target/debug/basalt-server > "$LOG" 2>&1 &
PID=$!
SKIP="${SKIP_CLEANUP:-0}"
trap '[ "$SKIP" = "1" ] || kill -9 $PID 2>/dev/null; wait $PID 2>/dev/null' EXIT
# 就绪等待：端口可连（负载下 listen 可能晚于 1s，预建题在此之前连接即失败）
for _ in $(seq 1 40); do
  (exec 3<>/dev/tcp/localhost/$PORT) 2>/dev/null && exec 3>&- && break
  sleep 0.25
done
sleep 0.5

# 预建题：kafka-clients 的 metadata auto-create 解析缺口（C11 已知差异，
# 待抓 v12 wire 分诊）——其余三档客户端均覆盖 auto-create 路径
python3 - <<'PY'
from kafka.admin import KafkaAdminClient, NewTopic
import time
a = KafkaAdminClient(bootstrap_servers="localhost:9092")
a.create_topics([NewTopic("kafkaclients-e2e", 2, 1)])
time.sleep(1)
PY

cd testing/kafkaclients
mvn -q compile exec:java 2>&1 | grep -vE "^\[WARNING|Unsafe"
RC=${PIPESTATUS[0]}
cd ../..
if [ $RC -ne 0 ]; then
  echo "--- server log tail ---"
  sed 's/\x1b\[[0-9;]*m//g' "$LOG" | grep -E "WARN|ERROR|panick" | tail -8
fi
rm -rf "$DATA" "$LOG"
if [ "$SKIP" = "1" ]; then sleep 30; fi   # 调试窗口：失败现场存活供同命令检查
exit $RC
