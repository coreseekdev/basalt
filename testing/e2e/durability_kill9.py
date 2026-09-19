#!/usr/bin/env python3
"""basalt e2e：fsync=always 档下 kill -9 → 重启 → 数据不丢。

用法: durability_kill9.py produce|verify
两阶段由 run_durability.sh 在 broker 重启前后分别调用；
分区数据用裸 assign 消费（不走消费组），与组状态恢复解耦。

注意语义：kill -9 验证的是 WAL/恢复逻辑（page cache 在进程死亡后仍由内核
保留，kill -9 本身不丢 page cache）；真正的断电防护由 fsync=always 档提供。
本 e2e 固定 always 档，保证被测路径与生产默认一致。
"""
import sys
import time

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BROKER = "localhost:9092"
TOPIC = "e2e-durability"
N = 200


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


def produce():
    assert wait_for_broker(), "broker not reachable"
    print("[1] broker reachable (pre-kill)")
    admin = KafkaAdminClient(bootstrap_servers=BROKER, client_id="e2e-durability-admin")
    try:
        admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1)])
        print(f"[2] topic {TOPIC} created")
    except TopicAlreadyExistsError:
        print(f"[2] topic {TOPIC} already exists")
    admin.close()
    time.sleep(1)

    producer = KafkaProducer(bootstrap_servers=BROKER, client_id="e2e-durability-producer",
                             acks=-1, retries=3, max_block_ms=10000)
    for i in range(N):
        meta = producer.send(TOPIC, key=f"key{i}".encode(),
                             value=f"durable-{i:05d}".encode()).get(timeout=10)
        assert meta.offset == i, f"offset {meta.offset} != {i}"
    producer.flush()
    producer.close()
    print(f"[3] produced {N} durable messages (acks=all)")


def verify():
    assert wait_for_broker(), "broker not reachable after restart"
    print("[4] broker reachable (post-restart)")
    consumer = KafkaConsumer(bootstrap_servers=BROKER, client_id="e2e-durability-consumer",
                             auto_offset_reset="earliest", enable_auto_commit=False)
    tp = TopicPartition(TOPIC, 0)
    consumer.assign([tp])
    consumer.seek_to_beginning(tp)

    got = []
    deadline = time.time() + 15
    while len(got) < N and time.time() < deadline:
        batch = consumer.poll(timeout_ms=500, max_records=N - len(got))
        for records in batch.values():
            got.extend(records)
    consumer.close()

    assert len(got) == N, f"expected {N} messages after restart, got {len(got)}"
    for i, r in enumerate(got):
        assert r.value == f"durable-{i:05d}".encode(), \
            f"message {i} corrupted: {r.value!r}"
    print(f"[5] all {N} messages survived kill -9 + restart, order intact")


if __name__ == "__main__":
    phase = sys.argv[1] if len(sys.argv) > 1 else ""
    if phase == "produce":
        produce()
    elif phase == "verify":
        verify()
    else:
        print("usage: durability_kill9.py produce|verify")
        sys.exit(2)
