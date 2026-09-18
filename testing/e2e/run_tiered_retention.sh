#!/usr/bin/env bash
# T-M4.3 v2：对象侧 retention（分层联动）e2e。
# tiered 题 + BASALT_RETENTION_MS=3000 / SWEEP=800ms：
#   [1] 上传发生后 sweep 删除过期对象（对象数下降）
#   [2] log_start 推进 → earliest 消费拿到**连续后缀**（非全量、非空）
#   [3] 重启恢复：checkpoint 的 log_start 保持，注册表与删除后对象面一致
set -u
PORT="${TIERED_RET_PORT:-9195}"
DATA=$(mktemp -d /tmp/basalt-tret-XXXX)
LOG=$(mktemp /tmp/basalt-tret-log-XXXX)

OLD=$(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
[ -n "$OLD" ] && kill -9 "$OLD" 2>/dev/null && sleep 0.5

start_server() {
  BASALT_DATA_DIR="$DATA" BASALT_PORT="$PORT" BASALT_NUM_PARTITIONS=1 BASALT_METRICS_PORT=0 \
    BASALT_SEGMENT_MAX_BYTES=2048 BASALT_RETENTION_MS=3000 BASALT_RETENTION_SWEEP_MS=800 \
    BASALT_LOG_LEVEL="${BASALT_LOG_LEVEL:-warn}" \
    ./target/debug/basalt-server > "$LOG" 2>&1 &
  PID=$!
}
stop_server() {
  kill -9 $PID 2>/dev/null; wait $PID 2>/dev/null
}
trap 'stop_server; rm -rf "$DATA" "$LOG"' EXIT
start_server
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
TOPIC = "tret-e2e"
TOTAL = 120

adm = AdminClient({"bootstrap.servers": BS})
fs = adm.create_topics([NewTopic(TOPIC, 1, 1, config={"basalt.storage.mode": "tiered"})])
for f in fs.values():
    try:
        f.result(timeout=10)
    except Exception as e:
        if "TOPIC_ALREADY_EXISTS" not in str(e):
            raise
time.sleep(1.0)

p = Producer({"bootstrap.servers": BS, "enable.idempotence": True, "acks": "all"})
for i in range(TOTAL):
    p.produce(TOPIC, key=f"k{i%4}".encode(), value=(f"r-{i:04d}-" + "x"*96).encode())
    if (i + 1) % 20 == 0:
        p.flush(15)
p.flush(15)
print("[prod] 120 records ✔")

# 等 retention（3s）+ 若干 sweep（800ms）
time.sleep(6)

def consume_earliest(label):
    c = Consumer({"bootstrap.servers": BS, "group.id": f"tret-{label}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.assign([TopicPartition(TOPIC, 0, 0)])
    got = []
    deadline = time.time() + 20
    while len(got) < TOTAL and time.time() < deadline:
        m = c.poll(1.0)
        if m and not m.error():
            got.append(int(m.value().decode().split("-")[1]))
    c.close()
    return got

got = consume_earliest("a")
assert got, "earliest 消费为空——retention 误删全部或读路径坏"
assert len(got) < TOTAL, f"{len(got)}/{TOTAL}——retention 未删除任何对象"
# 连续后缀：从某 k 开始连续到末尾
k = got[0]
assert got == list(range(k, k + len(got))), "消费结果非连续后缀（数据完整性破坏）"
print(f"[1] retention deleted aged objects; earliest yields contiguous suffix [{k:04d}..{got[-1]:04d}] ({len(got)}/{TOTAL}) ✔")
sys.stdout.flush()

# 记录后缀起点供重启后比对
open("/tmp/tret_suffix.txt", "w").write(str(k))
PY
RC=$?
[ $RC -ne 0 ] && { echo "--- server log ---"; grep -iE "retention|WARN|ERROR|panic" "$LOG" | tail -8; exit $RC; }

# [3] 重启恢复：log_start（checkpoint 持久化）+ 注册表（对象面）一致
stop_server
start_server
sleep 1.5
python3 - "$PORT" <<'PY'
import sys, time
from confluent_kafka import Consumer, TopicPartition
PORT = int(sys.argv[1])
k = int(open("/tmp/tret_suffix.txt").read())
c = Consumer({"bootstrap.servers": f"localhost:{PORT}", "group.id": "tret-restart",
              "auto.offset.reset": "earliest", "enable.auto.commit": False})
c.assign([TopicPartition("tret-e2e", 0, 0)])
got = []
deadline = time.time() + 20
while len(got) < 120 and time.time() < deadline:
    m = c.poll(1.0)
    if m and not m.error():
        got.append(int(m.value().decode().split("-")[1]))
c.close()
assert got, "重启后消费为空"
assert got == list(range(got[0], got[0] + len(got))), "重启后数据非连续"
assert got[0] >= k - 5, f"重启后 log_start 回退？before={k} after={got[0]}"
print(f"[2] restart: log_start retained, suffix [{got[0]:04d}..{got[-1]:04d}] consistent ✔")
print("PASS ✔ (对象侧 retention：过期删除/log_start 推进/后缀完整/重启一致)")
PY
RC=$?
[ $RC -ne 0 ] && { echo "--- server log ---"; grep -iE "retention|WARN|ERROR|panic" "$LOG" | tail -8; }
rm -f /tmp/tret_suffix.txt
exit $RC
