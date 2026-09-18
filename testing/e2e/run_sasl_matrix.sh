#!/usr/bin/env bash
# T-M4.1 SASL/TLS 客户端矩阵 e2e：SCRAM-SHA-256 × {SASL_PLAINTEXT, SASL_SSL}
# 三客户端档（验收矩阵，ADR-22 §4）。broker 同开鉴权 + TLS：
#   [1] librdkafka   SASL_SSL   produce/consume
#   [2] franz-go     SASL_PLAINTEXT（SCRAM）
#   [3] franz-go     SASL_SSL（SCRAM + TLS CA 校验）
#   [4] franz-go     错误口令 → 认证拒绝
#   [5] kafka-clients SASL_SSL（Java，PEM truststore）
#   [6] 负路径：无 SASL 客户端打 TLS 口 → 被拒（门禁+加密双面）
set -u
PORT="${SASL_MATRIX_PORT:-9196}"
TLS_PORT_N=$((PORT + 2))
DATA=$(mktemp -d /tmp/basalt-matrix-XXXX)
LOG=$(mktemp /tmp/basalt-matrix-log-XXXX)
CERTS=$(mktemp -d /tmp/basalt-matrix-cert-XXXX)

openssl req -x509 -newkey rsa:2048 -keyout "$CERTS/key.pem" -out "$CERTS/cert.pem" \
  -days 2 -nodes -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost" 2>/dev/null
[ -s "$CERTS/cert.pem" ] && [ -s "$CERTS/key.pem" ] || { echo "FAIL: openssl 证书生成"; exit 1; }

OLD=$(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
[ -n "$OLD" ] && kill -9 "$OLD" 2>/dev/null && sleep 0.5

BASALT_DATA_DIR="$DATA" BASALT_PORT="$PORT" BASALT_NUM_PARTITIONS=4 BASALT_METRICS_PORT=0 \
  BASALT_AUTH=scram BASALT_SASL_USERS="admin:secret123,app:apppass" \
  BASALT_TLS_CERT="$CERTS/cert.pem" BASALT_TLS_KEY="$CERTS/key.pem" \
  BASALT_LOG_LEVEL="${BASALT_LOG_LEVEL:-warn}" \
  ./target/debug/basalt-server > "$LOG" 2>&1 &
PID=$!
trap 'kill -9 $PID 2>/dev/null; wait $PID 2>/dev/null; rm -rf "$DATA" "$LOG" "$CERTS"' EXIT
for _ in $(seq 1 40); do
  (exec 3<>/dev/tcp/localhost/$PORT) 2>/dev/null && exec 3>&- && break
  sleep 0.25
done
(exec 3<>/dev/tcp/localhost/$TLS_PORT_N) 2>/dev/null && exec 3>&- || { echo "FAIL: TLS 口未监听"; exit 1; }
sleep 0.3

echo "== [1] librdkafka SASL_SSL =="
python3 - "$PORT" "$TLS_PORT_N" "$CERTS/cert.pem" <<'PY' || exit 1
import sys, time
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic

TLS_PORT, CA = int(sys.argv[2]), sys.argv[3]
TOPIC = "sasl-matrix-rdk"
conf = {"bootstrap.servers": f"localhost:{TLS_PORT}", "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "SCRAM-SHA-256", "sasl.username": "app",
        "sasl.password": "apppass", "ssl.ca.location": CA}
adm = AdminClient(conf)
fs = adm.create_topics([NewTopic(TOPIC, 2, 1)])
for f in fs.values():
    try:
        f.result(timeout=10)
    except Exception as e:
        if "TOPIC_ALREADY_EXISTS" not in str(e):
            raise
time.sleep(0.8)
prod = Producer(conf)
for i in range(20):
    prod.produce(TOPIC, value=f"rd-{i:02d}".encode(), partition=i % 2)
assert prod.flush(15) == 0
cons = Consumer({**conf, "group.id": "matrix-rdk", "auto.offset.reset": "earliest"})
cons.subscribe([TOPIC])
got = set()
deadline = time.time() + 20
while len(got) < 20 and time.time() < deadline:
    m = cons.poll(1.0)
    if m is None or m.error():
        continue
    if m.value().decode().startswith("rd-"):
        got.add(m.value().decode())
cons.close()
assert len(got) == 20, f"{len(got)}/20"
print("[1] librdkafka SASL_SSL 20/20 ✔")
PY

echo "== [2][3][4] franz-go SCRAM (plain / ssl / negpw) =="
cd testing/franzgo
BASALT_TLS_CA="$CERTS/cert.pem" go run ./sasl "$PORT" plain || exit 1
BASALT_TLS_CA="$CERTS/cert.pem" go run ./sasl "$TLS_PORT_N" ssl || exit 1
BASALT_TLS_CA="$CERTS/cert.pem" go run ./sasl "$TLS_PORT_N" negpw || exit 1
cd ../..

echo "== [5] kafka-clients SASL_SSL (Java) =="
cd testing/kafkaclients
mvn -q compile 2>&1 | grep -viE "^\[WARNING" | head -3
JAVA_LOG=$(mktemp /tmp/matrix-java-XXXX)
mvn -q exec:java -Dexec.mainClass=io.basalt.e2e.SaslSslE2E \
  -Dexec.args="$TLS_PORT_N $CERTS/cert.pem" > "$JAVA_LOG" 2>&1
RC_J=$?
grep -E "PASS|FAIL|Exception" "$JAVA_LOG" | head -4
grep -q "PASS ✔" "$JAVA_LOG" || { echo "FAIL: kafka-clients SASL_SSL（详见 $JAVA_LOG）"; exit 1; }
rm -f "$JAVA_LOG"
cd ../..

echo "== [6] 负路径：无 SASL 打 TLS 口 =="
python3 - "$PORT" "$TLS_PORT_N" <<'PY' || exit 1
import sys
from confluent_kafka import Producer
PLAIN, TLSP = int(sys.argv[1]), int(sys.argv[2])
p = Producer({"bootstrap.servers": f"localhost:{TLSP}", "message.timeout.ms": 6000,
              "reconnect.backoff.max.ms": 300})
results = []
p.produce("sasl-matrix-rdk", value=b"no-auth", partition=0,
          callback=lambda err, msg: results.append(str(err)))
p.flush(12)
assert results and not any("Message delivered" in r for r in results), \
    "无 SASL 客户端竟在 TLS 口投递成功"
print("[6] unauthenticated on TLS port rejected ✔")
PY

echo "PASS ✔ (SASL/TLS 客户端矩阵：librdkafka + franz-go + kafka-clients)"
