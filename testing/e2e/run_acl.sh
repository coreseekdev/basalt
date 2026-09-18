#!/usr/bin/env bash
# T-M4.1 ACL 骨架 e2e：authorizer 开启（SCRAM 身份 + 超级用户）+ 三 Admin API。
#   [1] 超级用户 admin：建题（CLUSTER/TOPIC:CREATE 旁路）
#   [2] app 无授权：produce 拒绝（TOPIC_AUTHORIZATION_FAILED=29）
#   [3] 授予 TOPIC:WRITE → produce 成功
#   [4] 无 READ：consume 拿不到数据
#   [5] 授予 TOPIC:READ → consume 读到全量
#   [6] describe_acls 往返 + delete_acls 删除 WRITE → produce 再拒
set -u
PORT="${ACL_PORT:-9196}"
DATA=$(mktemp -d /tmp/basalt-acl-XXXX)
LOG=$(mktemp /tmp/basalt-acl-log-XXXX)

OLD=$(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
[ -n "$OLD" ] && kill -9 "$OLD" 2>/dev/null && sleep 0.5

BASALT_DATA_DIR="$DATA" BASALT_PORT="$PORT" BASALT_NUM_PARTITIONS=1 BASALT_METRICS_PORT=0 \
  BASALT_AUTH=scram BASALT_SASL_USERS="admin:secret123,app:apppass" \
  BASALT_AUTHORIZER_ENABLED=true BASALT_SUPER_USERS="admin" \
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
from confluent_kafka import Producer, Consumer, TopicPartition, KafkaError
from kafka.admin import KafkaAdminClient
from kafka.admin.acl_resource import (
    ACL, ResourcePattern, ACLOperation, ACLPermissionType, ResourceType,
    ACLResourcePatternType,
)

PORT = int(sys.argv[1])
BS = f"localhost:{PORT}"
TOPIC = "acl-e2e"

def scram(user, pw):
    return {"bootstrap.servers": BS, "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanisms": "SCRAM-SHA-256", "sasl.username": user,
            "sasl.password": pw}

# confluent_kafka Admin（ACL 面；SCRAM 可用——kafka-python SCRAM 为
# pre-KIP-152 裸交换，见 run_auth.sh 头注）
from confluent_kafka.admin import AclBinding, AclBindingFilter, AclOperation, AclPermissionType, ResourceType, ResourcePatternType

def grant(user, op):
    binding = AclBinding(
        ResourceType.TOPIC, TOPIC, ResourcePatternType.LITERAL,
        f"User:{user}", "*", AclOperation(op), AclPermissionType.ALLOW)
    fs = adm_ck.create_acls([binding])
    for f in fs.values():
        f.result(timeout=10)

def describe_all(user="User:app"):
    flt = AclBindingFilter(ResourceType.ANY, None, ResourcePatternType.ANY,
                           user, None, AclOperation.ANY, AclPermissionType.ANY)
    return list(adm_ck.describe_acls(flt).result(timeout=10))

# [1] 超级用户建题
from confluent_kafka.admin import AdminClient as CkAdmin, NewTopic
adm = CkAdmin(scram("admin", "secret123"))
adm_ck = CkAdmin(scram("admin", "secret123"))
fs = adm.create_topics([NewTopic(TOPIC, 1, 1)])
for f in fs.values():
    try:
        f.result(timeout=10)
    except Exception as e:
        if "TOPIC_ALREADY_EXISTS" not in str(e):
            raise
time.sleep(0.8)
assert len(describe_all()) == 0, "起始 ACL 应为空"
print("[1] superuser create topic + empty ACL table ✔")

# [2] app 无授权 produce → 拒绝
p = Producer({**scram("app", "apppass"), "message.timeout.ms": 8000,
              "reconnect.backoff.max.ms": 300})
results = []
p.produce(TOPIC, value=b"denied", partition=0,
          callback=lambda err, msg: results.append(str(err)))
p.flush(15)
assert any("authorization" in r.lower() for r in results), \
    f"未得到授权拒绝：{results}"
print("[2] unauthorized produce rejected (TOPIC_AUTHORIZATION_FAILED) ✔")

# [3] 授予 WRITE → 成功
grant("app", ACLOperation.WRITE)
time.sleep(0.5)
ok = []
p2 = Producer(scram("app", "apppass"))
for i in range(10):
    p2.produce(TOPIC, value=f"acl-{i:02d}".encode(), partition=0,
               callback=lambda err, msg: ok.append(err is None))
assert p2.flush(15) == 0 and all(ok), "授予 WRITE 后投递失败"
print("[3] granted WRITE → produce 10 ✔")

# [4] 无 READ → 消费拿不到数据
c = Consumer({**scram("app", "apppass"), "group.id": "acl-g1",
              "auto.offset.reset": "earliest"})
c.assign([TopicPartition(TOPIC, 0, 0)])
n = 0
deadline = time.time() + 6
while time.time() < deadline:
    m = c.poll(1.0)
    if m is None:
        continue
    if m.error():
        assert "authorization" in str(m.error()).lower(), str(m.error())
        continue
    n += 1
c.close()
assert n == 0, f"无 READ 竟消费 {n} 条"
print("[4] unauthorized consume gets nothing ✔")

# [5] 授予 READ → 全量
grant("app", ACLOperation.READ)
time.sleep(0.5)
c = Consumer({**scram("app", "apppass"), "group.id": "acl-g2",
              "auto.offset.reset": "earliest"})
c.assign([TopicPartition(TOPIC, 0, 0)])
got = []
deadline = time.time() + 15
while len(got) < 10 and time.time() < deadline:
    m = c.poll(1.0)
    if m and not m.error():
        got.append(m.value().decode())
c.close()
assert len(got) == 10, f"{len(got)}/10"
print("[5] granted READ → consume 10/10 ✔")

# [6] describe 往返 + 删除 WRITE → 再拒
seen = describe_all()
assert len(seen) == 2, f"describe 应见 2 条绑定，得 {len(seen)}"
flt = AclBindingFilter(ResourceType.TOPIC, TOPIC, ResourcePatternType.LITERAL,
                       "User:app", None, AclOperation.WRITE, AclPermissionType.ANY)
deleted = []
for f in adm_ck.delete_acls([flt]).values():
    deleted += list(f.result(timeout=10))
assert len(deleted) == 1, f"删除 WRITE 绑定失败（{len(deleted)}）"
time.sleep(0.5)
p3 = Producer({**scram("app", "apppass"), "message.timeout.ms": 8000,
               "reconnect.backoff.max.ms": 300})
results = []
p3.produce(TOPIC, value=b"denied-again", partition=0,
           callback=lambda err, msg: results.append(str(err)))
p3.flush(15)
assert any("authorization" in r.lower() for r in results), f"删除后未拒绝：{results}"
print("[6] describe roundtrip + delete WRITE → produce denied again ✔")
print("PASS ✔ (ACL 骨架：授权门禁 + 三 Admin API)")
PY
RC=$?
if [ $RC -ne 0 ]; then
  echo "--- server log tail ---"
  sed 's/\x1b\[[0-9;]*m//g' "$LOG" | grep -E "WARN|ERROR|panic" | tail -8
fi
exit $RC
