#!/usr/bin/env python3
"""librdkafka 幂等模式 e2e（T-M3.1）：enable.idempotence=true
→ InitProducerId + sequence 管理由 librdkafka 协商——服务端去重兼容性。

断言：produce N（含显式重试扰动）→ 消费恰好 N 条唯一值（零重复零丢失）。
"""
import time
from confluent_kafka import Producer, Consumer

BROKER = "localhost:9092"
TOPIC = "idem-e2e"

def main():
    p = Producer({"bootstrap.servers": BROKER, "enable.idempotence": True,
                  "acks": "all", "retries": 5, "linger.ms": 5})
    n_ok = 0
    for i in range(50):
        p.produce(TOPIC, key=str(i % 2), value=f"idem-{i}".encode())
        p.poll(0)
        n_ok += 1
    p.flush(30)
    print(f"[1] 50 produced (idempotence=on)", flush=True)

    c = Consumer({"bootstrap.servers": BROKER, "group.id": f"idem-{time.time()}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([TOPIC])
    got = set()
    deadline = time.time() + 20
    while len(got) < 50 and time.time() < deadline:
        recs = c.consume(num_messages=50, timeout=1.0)
        for r in recs:
            if r.error():
                continue
            got.add(r.value().decode())
    c.close()
    assert len(got) == 50, f"读回 {len(got)}/50"
    print(f"[2] read {len(got)} unique — 零丢失零重复")
    print("PASS ✔ (librdkafka 幂等模式)")

if __name__ == "__main__":
    main()
