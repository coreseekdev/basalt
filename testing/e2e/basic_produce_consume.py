#!/usr/bin/env python3
"""basalt e2e：produce → fetch（consume）往返，验证不丢不重、offset 语义。"""
import sys, time
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.errors import TopicAlreadyExistsError
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.protocol.admin import CreateTopicsRequest

BROKER = "localhost:9092"
TOPIC = "e2e-basic"

def wait_for_broker(timeout=15):
    from kafka.client_async import KafkaClient
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            c = KafkaClient(bootstrap_servers=BROKER)
            c.close()
            return True
        except Exception:
            time.sleep(0.5)
    return False

def main():
    assert wait_for_broker(), "broker not reachable"
    print("[1] broker reachable")

    # 建题（走 CreateTopics；broker 未实现则用 metadata auto-create 兜底）
    try:
        admin = KafkaAdminClient(bootstrap_servers=BROKER, client_id="e2e-admin")
        try:
            admin.create_topics([NewTopic(TOPIC, num_partitions=2, replication_factor=1)])
            print(f"[2] topic {TOPIC} created (admin)")
        except TopicAlreadyExistsError:
            print(f"[2] topic {TOPIC} already exists")
        except Exception as e:
            print(f"[2] admin create not supported ({type(e).__name__}); fallback auto-create")
            # 触发 auto-create：发 metadata
            from kafka.client_async import KafkaClient
            from kafka.protocol.metadata import MetadataRequest
            c = KafkaClient(bootstrap_servers=BROKER)
            fut = c.send(3, MetadataRequest[1]([TOPIC]))
            c.poll(future=fut, timeout_ms=3000)
            c.close()
    except Exception as e:
        print(f"[2] admin failed entirely: {e}")
    time.sleep(1)

    # produce
    producer = KafkaProducer(bootstrap_servers=BROKER, client_id="e2e-producer",
                             acks=-1, retries=3, max_block_ms=10000)
    n = 100
    futures = []
    for i in range(n):
        futures.append(producer.send(TOPIC, key=f"key{i%7}".encode(), value=f"msg-{i:05d}".encode()))
    for f in futures:
        meta = f.get(timeout=10)
        assert meta.offset >= 0
    producer.flush()
    producer.close()
    print(f"[3] produced {n} messages")

    # consume（手动 assign，读全量）
    consumer = KafkaConsumer(bootstrap_servers=BROKER, client_id="e2e-consumer",
                             auto_offset_reset="earliest", enable_auto_commit=False,
                             consumer_timeout_ms=15000)
    parts = [TopicPartition(TOPIC, 0), TopicPartition(TOPIC, 1)]
    consumer.assign(parts)
    got = []
    deadline = time.time() + 15
    while len(got) < n and time.time() < deadline:
        for m in consumer.poll(timeout_ms=1000, max_records=50).values():
            for rec in m:
                got.append((rec.partition, rec.offset, rec.value.decode()))
    consumer.close()
    print(f"[4] consumed {len(got)} messages")
    assert len(got) == n, f"expected {n}, got {len(got)}"

    # 不重：全局唯一
    vals = [v for _, _, v in got]
    assert len(set(vals)) == n, f"duplicates detected: {n - len(set(vals))}"
    # 顺序：分区内 offset 单调
    by_part = {}
    for p, o, _ in got:
        by_part.setdefault(p, []).append(o)
    for p, offs in by_part.items():
        assert offs == sorted(offs), f"partition {p} out of order"
    print("[5] invariants: no loss / no dup / per-partition order OK")

    # offsets_for_times（时间查询）
    tp0 = TopicPartition(TOPIC, 0)
    consumer2 = KafkaConsumer(bootstrap_servers=BROKER)
    consumer2.assign([tp0])
    result = consumer2.offsets_for_times({tp0: 0})
    print(f"[6] offsets_for_times(ts=0) p0 = {result[tp0]}")
    beginning = consumer2.beginning_offsets([tp0])
    end = consumer2.end_offsets([tp0])
    print(f"[7] beginning={beginning[tp0]} end={end[tp0]}")
    consumer2.close()
    print("PASS ✔")

if __name__ == "__main__":
    main()
    sys.exit(0)
