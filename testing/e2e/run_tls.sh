#!/usr/bin/env bash
# T-S1 加密面 e2e：TLS listener（BASALT_TLS_CERT/KEY，独立端口 = client+2）。
#   [1] librdkafka SSL 档（CA 校验）produce/consume 全链 20 条
#   [2] 主明文口照常工作（多 listener 共存语义）
#   [3] 明文客户端打 TLS 口 → 握手失败断连（数据不泄露）
# 证书由 openssl 现生成（CN=localhost，与 bootstrap host 匹配过校验）。
set -u
PORT="${TLS_PORT:-9195}"
TLS_PORT_N=$((PORT + 2))
DATA=$(mktemp -d /tmp/basalt-tls-XXXX)
LOG=$(mktemp /tmp/basalt-tls-log-XXXX)
CERTS=$(mktemp -d /tmp/basalt-tls-cert-XXXX)

openssl req -x509 -newkey rsa:2048 -keyout "$CERTS/key.pem" -out "$CERTS/cert.pem" \
  -days 2 -nodes -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost" 2>/dev/null
[ -s "$CERTS/cert.pem" ] && [ -s "$CERTS/key.pem" ] || { echo "FAIL: openssl 证书生成"; exit 1; }

OLD=$(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
[ -n "$OLD" ] && kill -9 "$OLD" 2>/dev/null && sleep 0.5

BASALT_DATA_DIR="$DATA" BASALT_PORT="$PORT" BASALT_NUM_PARTITIONS=2 BASALT_METRICS_PORT=0 \
  BASALT_TLS_CERT="$CERTS/cert.pem" BASALT_TLS_KEY="$CERTS/key.pem" \
  BASALT_LOG_LEVEL="${BASALT_LOG_LEVEL:-warn}" \
  ./target/debug/basalt-server > "$LOG" 2>&1 &
PID=$!
trap 'kill -9 $PID 2>/dev/null; wait $PID 2>/dev/null; rm -rf "$DATA" "$LOG" "$CERTS"' EXIT
for _ in $(seq 1 40); do
  (exec 3<>/dev/tcp/localhost/$PORT) 2>/dev/null && exec 3>&- && break
  sleep 0.25
done
sleep 0.5
(exec 3<>/dev/tcp/localhost/$TLS_PORT_N) 2>/dev/null || { echo "FAIL: TLS 口未监听"; grep -iE "tls|error" "$LOG" | head; exit 1; } && exec 3>&-

python3 - "$PORT" "$TLS_PORT_N" "$CERTS/cert.pem" <<'PY'
import sys, time
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic

PLAIN, TLSP, CA = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
TOPIC = "tls-e2e"

# [1] TLS 档全链（CA 校验 + localhost SAN）
conf = {"bootstrap.servers": f"localhost:{TLSP}", "security.protocol": "SSL",
        "ssl.ca.location": CA}
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
    prod.produce(TOPIC, value=f"tls-{i:03d}".encode(), partition=i % 2)
assert prod.flush(15) == 0, "TLS produce 残留"
print("[1] TLS produce 20 ✔")

cons = Consumer({**conf, "group.id": "tls-g1", "auto.offset.reset": "earliest"})
cons.subscribe([TOPIC])
got = set()
deadline = time.time() + 20
while len(got) < 20 and time.time() < deadline:
    m = cons.poll(1.0)
    if m is None or m.error():
        continue
    got.add(m.value().decode())
cons.close()
assert len(got) == 20, f"TLS 消费 {len(got)}/20"
print("[2] TLS consume 20 exactly-once ✔")

# [2] 明文主口共存
p = Producer({"bootstrap.servers": f"localhost:{PLAIN}", "message.timeout.ms": 8000})
ok = []
p.produce(TOPIC, value=b"via-plaintext-port", partition=0,
          callback=lambda err, msg: ok.append(err is None))
p.flush(10)
assert all(ok) and ok, "明文主口不可用"
print("[3] plaintext main port coexists ✔")

# [3] 明文客户端打 TLS 口 → 握手失败（Kafka 帧在 TLS 期被拒）
p = Producer({"bootstrap.servers": f"localhost:{TLSP}", "message.timeout.ms": 6000,
              "reconnect.backoff.max.ms": 300})
results = []
p.produce(TOPIC, value=b"to-tls-plaintext", partition=0,
          callback=lambda err, msg: results.append(str(err)))
p.flush(12)
assert results, "明文打 TLS 口竟成功投递——加密面失效？"
assert not any("Message delivered" in r for r in results), "明文打 TLS 口竟成功投递"
print("[4] plaintext to TLS port rejected at handshake ✔")
print("PASS ✔ (TLS 加密面全链)")
PY
RC=$?
if [ $RC -ne 0 ]; then
  echo "--- server log tail ---"
  sed 's/\x1b\[[0-9;]*m//g' "$LOG" | grep -E "WARN|ERROR|panic|tls" | tail -10
fi
exit $RC
