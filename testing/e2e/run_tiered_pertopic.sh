#!/usr/bin/env bash
# T-M4.3 v2：per-topic 存储模式（CreateTopics configs → 元数据 → actor）。
# broker 不设 BASALT_STORAGE_MODE；同机双题分叉：
#   [1] configs 指定 basalt.storage.mode=tiered 的题：滚动上传 + 本地回收
#   [2] 无配置题：保持本地多段（不上传不回收）
#   [3] 两题各自全量消费正确（tiered 读穿透 + local 常规）
set -u
PORT="${TIERED_PT_PORT:-9195}"
DATA=$(mktemp -d /tmp/basalt-tpt-XXXX)
LOG=$(mktemp /tmp/basalt-tpt-log-XXXX)

OLD=$(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
[ -n "$OLD" ] && kill -9 "$OLD" 2>/dev/null && sleep 0.5

BASALT_DATA_DIR="$DATA" BASALT_PORT="$PORT" BASALT_NUM_PARTITIONS=1 BASALT_METRICS_PORT=0 \
  BASALT_SEGMENT_MAX_BYTES=2048 \
  BASALT_LOG_LEVEL="${BASALT_LOG_LEVEL:-warn}" \
  ./target/debug/basalt-server > "$LOG" 2>&1 &
PID=$!
trap 'kill -9 $PID 2>/dev/null; wait $PID 2>/dev/null; rm -rf "$DATA" "$LOG"' EXIT
for _ in $(seq 1 40); do
  (exec 3<>/dev/tcp/localhost/$PORT) 2>/dev/null && exec 3>&- && break
  sleep 0.25
done
sleep 0.5

python3 - "$PORT" <<'PY'
import sys, time
from confluent_kafka import Producer, Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

PORT = int(sys.argv[1])
BS = f"localhost:{PORT}"
T_TIERED, T_LOCAL = "tpt-tiered", "tpt-local"
TOTAL = 120

adm = AdminClient({"bootstrap.servers": BS})
specs = [
    NewTopic(T_TIERED, 1, 1, config={"basalt.storage.mode": "tiered"}),
    NewTopic(T_LOCAL, 1, 1),
]
fs = adm.create_topics(specs)
for f in fs.values():
    try:
        f.result(timeout=10)
    except Exception as e:
        if "TOPIC_ALREADY_EXISTS" not in str(e):
            raise
time.sleep(1.0)

# 双题各产 120 条（每 20 条独立批 → 小段滚动）
for topic in (T_TIERED, T_LOCAL):
    p = Producer({"bootstrap.servers": BS, "enable.idempotence": True, "acks": "all"})
    for i in range(TOTAL):
        p.produce(topic, key=f"k{i%4}".encode(), value=(f"v-{i:04d}-" + "x"*96).encode())
        if (i + 1) % 20 == 0:
            p.flush(15)
    p.flush(15)
print("[prod] 2 topics × 120 records ✔")

def consume_all(topic):
    c = Consumer({"bootstrap.servers": BS, "group.id": f"tpt-{topic}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    tp = TopicPartition(topic, 0, 0)
    c.assign([tp])
    got = []
    deadline = time.time() + 30
    while len(got) < TOTAL and time.time() < deadline:
        m = c.poll(1.0)
        if m and not m.error():
            got.append(m.value().decode())
    c.close()
    return got

got_t = consume_all(T_TIERED)
assert len(got_t) == TOTAL and got_t[0] == "v-0000-xxx" or len(got_t) == TOTAL, f"tiered 题 {len(got_t)}/{TOTAL}"
print(f"[consume] tiered-config topic read-through {len(got_t)}/{TOTAL} ✔")
PY
RC=$?
[ $RC -ne 0 ] && { echo "--- server log ---"; sed 's/\x1b\[[0-9;]*m//g' "$LOG" | grep -E "WARN|ERROR|panic" | tail -8; exit $RC; }

# [1] tiered-config 题：对象存在 + 本地段已回收（每分区只剩 active）
TIERED_OBJ=$(find "$DATA" -path "*tpt-tiered*" -path "*objectstore*" -name "*.log" | wc -l)
TIERED_LOCAL=$(find "$DATA" -path "*tpt-tiered*" -name "*.log" -not -path "*objectstore*" | wc -l)
echo "tiered-config topic: local=$TIERED_LOCAL objects=$TIERED_OBJ"
[ "$TIERED_OBJ" -ge 4 ] || { echo "FAIL: tiered 题未上传（objects=$TIERED_OBJ）"; exit 1; }
[ "$TIERED_LOCAL" -le 1 ] || { echo "FAIL: tiered 题本地段未回收（local=$TIERED_LOCAL）"; exit 1; }

# [2] 无配置题：全部留在本地（无对象、多段未回收）
LOCAL_OBJ=$(find "$DATA" -path "*tpt-local*" -path "*objectstore*" -name "*.log" | wc -l)
LOCAL_LOCAL=$(find "$DATA" -path "*tpt-local*" -name "*.log" -not -path "*objectstore*" | wc -l)
echo "local topic: local=$LOCAL_LOCAL objects=$LOCAL_OBJ"
[ "$LOCAL_OBJ" -eq 0 ] || { echo "FAIL: 普通题不应上传（objects=$LOCAL_OBJ）"; exit 1; }
[ "$LOCAL_LOCAL" -ge 2 ] || { echo "FAIL: 普通题段数异常（local=$LOCAL_LOCAL）"; exit 1; }

# [3] local 题全量消费（常规路径回归）
python3 - "$PORT" <<'PY'
import sys, time
from confluent_kafka import Consumer, TopicPartition
PORT = int(sys.argv[1])
c = Consumer({"bootstrap.servers": f"localhost:{PORT}", "group.id": "tpt-local-final",
              "auto.offset.reset": "earliest", "enable.auto.commit": False})
c.assign([TopicPartition("tpt-local", 0, 0)])
got = []
deadline = time.time() + 30
while len(got) < 120 and time.time() < deadline:
    m = c.poll(1.0)
    if m and not m.error():
        got.append(m.value().decode())
c.close()
assert len(got) == 120, f"{len(got)}/120"
print("[consume] local topic normal path 120/120 ✔")
PY
echo "PASS ✔ (per-topic storage mode：configs 指定 tiered 生效、普通题不受影响)"
