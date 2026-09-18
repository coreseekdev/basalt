#!/usr/bin/env bash
# T-S5 配额/限流 e2e：produce/fetch 字节率 token bucket（BASALT_QUOTA_*_BYTES）。
#   [1]  produce 0.4MB（突发额度内）——快速完成
#   [2]  再 produce 3MB @1MB/s 配额——耗时 ≥2s（节流生效）
#   [3]  consume 全量 3.4MB @1MB/s fetch 配额——耗时 ≥2s 且数据完整
# 对照（无配额同量级毫秒级完成）由 token bucket 单测 + run_bench 基线交叉证明。
set -u
PORT="${QUOTA_PORT:-9194}"
DATA=$(mktemp -d /tmp/basalt-quota-XXXX)
LOG=$(mktemp /tmp/basalt-quota-log-XXXX)

OLD=$(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
[ -n "$OLD" ] && kill -9 "$OLD" 2>/dev/null && sleep 0.5

BASALT_DATA_DIR="$DATA" BASALT_PORT="$PORT" BASALT_NUM_PARTITIONS=1 BASALT_METRICS_PORT=0 \
  BASALT_QUOTA_PRODUCER_BYTES=1000000 BASALT_QUOTA_FETCH_BYTES=1000000 \
  BASALT_LOG_LEVEL="${BASALT_LOG_LEVEL:-warn}" \
  ./target/debug/basalt-server > "$LOG" 2>&1 &
PID=$!
trap 'kill -9 $PID 2>/dev/null; wait $PID 2>/dev/null; rm -rf "$DATA" "$LOG"' EXIT
for _ in $(seq 1 40); do
  (exec 3<>/dev/tcp/localhost/$PORT) 2>/dev/null && exec 3>&- && break
  sleep 0.25
done
sleep 0.3

python3 - "$PORT" <<'PY'
import sys, time
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer, KafkaConsumer

PORT = int(sys.argv[1])
BS = f"localhost:{PORT}"
TOPIC = "quota-e2e"
PAYLOAD = b"x" * 1000   # 1KB/条

adm = KafkaAdminClient(bootstrap_servers=BS)
adm.create_topics([NewTopic(TOPIC, 1, 1)])
time.sleep(0.8)
adm.close()

prod = KafkaProducer(bootstrap_servers=BS, acks=1)

# [1] 突发额度内：0.4MB 快速完成
t0 = time.time()
for i in range(400):
    prod.send(TOPIC, value=PAYLOAD)
prod.flush()
burst = time.time() - t0
assert burst < 2.0, f"突发额度内 0.4MB 耗时 {burst:.2f}s（应为亚秒级）"
print(f"[1] burst phase: 0.4MB in {burst:.2f}s ✔")

# [2] 配额节流：+3MB @1MB/s（初始桶已耗尽）→ ≥2s
t0 = time.time()
for i in range(3000):
    prod.send(TOPIC, value=PAYLOAD)
prod.flush()
throttled = time.time() - t0
assert throttled >= 1.8, f"3MB 仅耗时 {throttled:.2f}s——配额未生效？"
assert throttled < 30, f"3MB 耗时 {throttled:.2f}s——节流过度"
print(f"[2] throttled phase: 3.0MB in {throttled:.2f}s (≥1MB/s quota enforced) ✔")
prod.close()

# [3] fetch 配额：消费全量 3.4MB @1MB/s → ≥2s 且不丢
cons = KafkaConsumer(TOPIC, bootstrap_servers=BS, auto_offset_reset="earliest",
                     consumer_timeout_ms=15000)
t0 = time.time()
n = 0
for m in cons:
    assert m.value == PAYLOAD
    n += 1
fetch_t = time.time() - t0
cons.close()
assert n == 3400, f"消费 {n}/3400"
assert fetch_t >= 1.8, f"fetch 3.4MB 仅耗时 {fetch_t:.2f}s——fetch 配额未生效？"
print(f"[3] fetch phase: 3.4MB/{n} records in {fetch_t:.2f}s (fetch quota enforced, no loss) ✔")
print("PASS ✔ (produce/fetch 字节率配额)")
PY
RC=$?
[ $RC -ne 0 ] && { echo "--- server log tail ---"; sed 's/\x1b\[[0-9;]*m//g' "$LOG" | grep -E "WARN|ERROR|panic" | tail -10; exit $RC; }

# ============ 阶段二：per-user 配额覆盖（T-M4.2 尾项） ============
# 全局默认 200KB/s；per-user "fast" 覆盖为 2MB/s。同样投递 600KB：
#   [4] fast（覆盖生效）亚秒完成
#   [5] slowpoke（无条目 → 回落全局默认）≥2.5s
kill -9 $PID 2>/dev/null; wait $PID 2>/dev/null
DATA2=$(mktemp -d /tmp/basalt-quota2-XXXX)
BASALT_DATA_DIR="$DATA2" BASALT_PORT="$PORT" BASALT_NUM_PARTITIONS=1 BASALT_METRICS_PORT=0 \
  BASALT_AUTH=scram BASALT_SASL_USERS="fast:fp,slowpoke:sp" \
  BASALT_QUOTA_PRODUCER_BYTES=200000 BASALT_QUOTA_USER_BYTES="fast:p=2000000" \
  BASALT_LOG_LEVEL="${BASALT_LOG_LEVEL:-warn}" \
  ./target/debug/basalt-server > "$LOG" 2>&1 &
PID=$!
for _ in $(seq 1 40); do
  (exec 3<>/dev/tcp/localhost/$PORT) 2>/dev/null && exec 3>&- && break
  sleep 0.25
done
sleep 0.3

python3 - "$PORT" <<'PY'
import sys, time
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

PORT = int(sys.argv[1])
BS = f"localhost:{PORT}"
TOPIC = "quota-user-e2e"
conf = {"bootstrap.servers": BS, "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanisms": "SCRAM-SHA-256"}
adm = AdminClient({**conf, "sasl.username": "fast", "sasl.password": "fp"})
fs = adm.create_topics([NewTopic(TOPIC, 1, 1)])
for f in fs.values():
    try:
        f.result(timeout=10)
    except Exception as e:
        if "TOPIC_ALREADY_EXISTS" not in str(e):
            raise
time.sleep(0.8)

def produce_timed(user, pw, n=600):
    p = Producer({**conf, "sasl.username": user, "sasl.password": pw})
    t0 = time.time()
    for i in range(n):
        p.produce(TOPIC, value=b"x" * 1000, partition=0)
    left = p.flush(60)
    dt = time.time() - t0
    assert left == 0, f"{user}: flush 残留 {left}"
    return dt

fast = produce_timed("fast", "fp")
slow = produce_timed("slowpoke", "sp")
print(f"[4] per-user override 'fast' (2MB/s): 600KB in {fast:.2f}s ✔")
print(f"[5] fallback 'slowpoke' (global 200KB/s): 600KB in {slow:.2f}s ✔")
assert fast < 1.5, f"per-user 覆盖未生效？fast={fast:.2f}s"
assert slow >= 1.8, f"回落全局默认未生效？slow={slow:.2f}s"  # 600KB-200KB 突发=400KB@200KB/s → ≥2.0s
assert slow > fast * 3, "两用户速率无差异"
print("PASS ✔ (per-user 配额覆盖 + 全局回落)")
PY
RC=$?
if [ $RC -ne 0 ]; then
  echo "--- server log tail ---"
  sed 's/\x1b\[[0-9;]*m//g' "$LOG" | grep -E "WARN|ERROR|panic" | tail -10
fi
kill -9 $PID 2>/dev/null; wait $PID 2>/dev/null; rm -rf "$DATA2"
exit $RC
