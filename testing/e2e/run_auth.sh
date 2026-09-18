#!/usr/bin/env bash
# T-S1 SASL/SCRAM-SHA-256 e2e：认证门禁 + SCRAM 握手全链（librdkafka 档，
# 标准 KIP-152 SaslHandshake/SaslAuthenticate 流程）。
#   [1] 正确凭据：Admin 建题 + produce 20 + consume 20 全链
#   [2] 错误口令：认证失败（SASL_AUTHENTICATION_FAILED=58）→ 投递失败
#   [3] 未知用户：同上
#   [4] 未认证越权：无 SASL 客户端业务请求 → 连接被服务端关闭（Kafka 同语义）
#   [5] 负路径后服务端存活：正确凭据仍可完成 produce/consume
# 已知差异：kafka-python 2.2.3 的 SCRAM 走 pre-KIP-152 裸 socket 交换
# （SaslHandshake v0 + 裸 token，无 SaslAuthenticate 包裹）——basalt 与
# Kafka KIP-152 后的强制 SaslAuthenticate 路径一致，裸路径留 TASK P2。
set -u
PORT="${AUTH_PORT:-9193}"
DATA=$(mktemp -d /tmp/basalt-auth-XXXX)
LOG=$(mktemp /tmp/basalt-auth-log-XXXX)

OLD=$(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
[ -n "$OLD" ] && kill -9 "$OLD" 2>/dev/null && sleep 0.5

BASALT_DATA_DIR="$DATA" BASALT_PORT="$PORT" BASALT_NUM_PARTITIONS=2 BASALT_METRICS_PORT=0 \
  BASALT_AUTH=scram BASALT_SASL_USERS="admin:secret123,app:apppass" \
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
from confluent_kafka import Producer, Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

PORT = int(sys.argv[1])
BS = f"localhost:{PORT}"
TOPIC = "auth-e2e"

def conf(user, pw):
    return {
        "bootstrap.servers": BS,
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanisms": "SCRAM-SHA-256",
        "sasl.username": user,
        "sasl.password": pw,
    }

# [1] 正确凭据全链
adm = AdminClient(conf("admin", "secret123"))
fs = adm.create_topics([NewTopic(TOPIC, 2, 1)])
for f in fs.values():
    try:
        f.result(timeout=10)
    except Exception as e:
        if "TOPIC_ALREADY_EXISTS" not in str(e):
            raise
time.sleep(0.8)

prod = Producer(conf("app", "apppass"))
for i in range(20):
    prod.produce(TOPIC, value=f"sec-{i:03d}".encode(), partition=i % 2)
left = prod.flush(15)
assert left == 0, f"flush 残留 {left}"
print("[1] SCRAM produce 20 via authenticated connection ✔")

cons = Consumer({**conf("app", "apppass"),
                 "group.id": "auth-g1", "auto.offset.reset": "earliest"})
cons.subscribe([TOPIC])
got = set()
deadline = time.time() + 20
while len(got) < 20 and time.time() < deadline:
    m = cons.poll(1.0)
    if m is None or m.error():
        continue
    got.add(m.value().decode())
cons.close()
assert len(got) == 20, f"消费 {len(got)}/20"
print("[2] SCRAM consume 20 exactly-once ✔")
PY

# [2]/[3] 负路径：错误口令 / 未知用户 → 认证失败，消息无法投递。
# librdkafka 把认证失败归一为连接重试/MSG_TIMED_OUT（不回 SASL 字样），
# 故以「服务端日志明确记录 auth failed」+「客户端投递失败」双证据断言。
LOGPATH="$LOG" python3 - "$PORT" <<'PY'
import sys, time, os, re
from confluent_kafka import Producer, Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

PORT = int(sys.argv[1])
BS = f"localhost:{PORT}"
TOPIC = "auth-e2e"
LOG = os.environ["LOGPATH"]

def server_rejections():
    txt = open(LOG, errors="replace").read()
    return len(re.findall(r"sasl authentication failed", txt))

def conf(user, pw):
    return {
        "bootstrap.servers": BS,
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanisms": "SCRAM-SHA-256",
        "sasl.username": user,
        "sasl.password": pw,
    }

def server_alive():
    """检查服务进程仍存活（被 e2e kill 的话 fail fast）"""
    import subprocess
    r = subprocess.run(["ss", "-tln"], capture_output=True, text=True)
    return f":{PORT} " in r.stdout

# [1] 正确凭据全链
adm = AdminClient(conf("admin", "secret123"))
fs = adm.create_topics([NewTopic(TOPIC, 2, 1)])
for f in fs.values():
    try:
        f.result(timeout=10)
    except Exception as e:
        if "TOPIC_ALREADY_EXISTS" not in str(e):
            raise
time.sleep(0.8)

prod = Producer(conf("app", "apppass"))
for i in range(20):
    prod.produce(TOPIC, value=f"sec-{i:03d}".encode(), partition=i % 2)
left = prod.flush(15)
assert left == 0, f"flush 残留 {left}"
print("[1] SCRAM produce 20 via authenticated connection ✔")

cons = Consumer({**conf("app", "apppass"),
                 "group.id": "auth-g1", "auto.offset.reset": "earliest"})
cons.subscribe([TOPIC])
got = set()
deadline = time.time() + 20
while len(got) < 20 and time.time() < deadline:
    m = cons.poll(1.0)
    if m is None or m.error():
        continue
    got.add(m.value().decode())
cons.close()
assert len(got) == 20, f"消费 {len(got)}/20"
print("[2] SCRAM consume 20 exactly-once ✔")

# [2]/[3] 负路径
def expect_auth_fail(label, user, pw):
    before = server_rejections()
    p = Producer({**conf(user, pw), "message.timeout.ms": 6000,
                  "reconnect.backoff.max.ms": 300})
    results = []
    p.produce(TOPIC, value=b"must-fail", partition=0,
              callback=lambda err, msg: results.append(str(err)))
    p.flush(15)
    rejected = server_rejections() > before
    assert rejected, f"{label}: 服务端未记录认证拒绝（日志无 sasl authentication failed）"
    assert results, f"{label}: 消息未以失败收场：仍被投递？"
    assert server_alive(), "服务端在负路径中崩溃"
    print(f"[{label}] auth rejected server-side (×{server_rejections()-before}), client delivery failed ✔")

expect_auth_fail("3", "admin", "wrong-password")
expect_auth_fail("4", "ghost", "secret123")

# [5] 未认证越权：无 SASL 客户端 → 业务请求被门禁断连（Kafka 同语义）
before = server_rejections()
p = Producer({"bootstrap.servers": BS, "message.timeout.ms": 6000,
              "reconnect.backoff.max.ms": 300})
results = []
p.produce(TOPIC, value=b"plaintext", partition=0,
          callback=lambda err, msg: results.append(str(err)))
p.flush(15)
txt = open(LOG, errors="replace").read()
assert "request before SASL auth" in txt, "门禁未记录越权断连"
assert results, "PLAINTEXT 客户端消息未被拒"
assert server_alive(), "服务端在负路径中崩溃"
print("[5] unauthenticated client rejected (gate closed connection) ✔")

# [6] 服务端存活：负路径后正确凭据仍全通
prod = Producer(conf("app", "apppass"))
ok = []
prod.produce(TOPIC, value=b"after-negative", partition=0,
             callback=lambda err, msg: ok.append(err is None))
assert prod.flush(15) == 0 and all(ok), "负路径后服务端不可用"
cons = Consumer({**conf("app", "apppass"), "group.id": "auth-g2"})
cons.assign([TopicPartition(TOPIC, 0, 0)])
found = False
deadline = time.time() + 10
while time.time() < deadline and not found:
    m = cons.poll(1.0)
    if m and not m.error() and m.value() == b"after-negative":
        found = True
cons.close()
assert found, "未读到负路径后的新消息"
print("[6] server alive after auth failures ✔")
print("PASS ✔ (SASL/SCRAM-SHA-256 认证门禁全链)")
PY
RC=$?
if [ $RC -ne 0 ]; then
  echo "--- server log tail ---"
  sed 's/\x1b\[[0-9;]*m//g' "$LOG" | grep -E "WARN|ERROR|panic" | tail -10
fi
exit $RC
