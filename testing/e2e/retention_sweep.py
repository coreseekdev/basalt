#!/usr/bin/env python3
"""basalt e2e：本地日志 time-based retention（A3）——过期 sealed 段删除、
active 保留、beginning offset 前移、未到期数据可读。

用法: retention_sweep.py produce|fresh|verify
三阶段由 run_retention.sh 按时序调用。

段布局设计（linger_ms=1000 + 大 batch_size → 各整批单段）：
  old 90 条 → 单个 oversized sealed 段 [0-89]
  fresh 10 条 → 整批前置滚动 → 独立段 [90-99]（active，永不删除）
retention 删除 [0-89] 后：beginning=90、end=100、fresh 全量可读。
"""
import sys
import time

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BROKER = "localhost:9092"
TOPIC = "e2e-retention"
N_OLD = 90
N_FRESH = 10


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


def make_producer():
    # linger_ms + 大 batch_size → 每阶段恰好一个 producer 批；broker 端
    # 批不可分裂 → 段边界即批边界，新旧数据不混段
    return KafkaProducer(bootstrap_servers=BROKER, client_id="e2e-retention-producer",
                         acks=-1, retries=3, max_block_ms=10000,
                         linger_ms=1000, batch_size=1_000_000)


def produce_old():
    assert wait_for_broker(), "broker not reachable"
    print("[1] broker reachable")
    admin = KafkaAdminClient(bootstrap_servers=BROKER, client_id="e2e-retention-admin")
    try:
        admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1)])
        print(f"[2] topic {TOPIC} created")
    except TopicAlreadyExistsError:
        print(f"[2] topic {TOPIC} already exists")
    admin.close()
    time.sleep(1)
    producer = make_producer()
    for i in range(N_OLD):
        producer.send(TOPIC, value=f"old-{i:05d}".encode().ljust(200, b"x"))
    producer.flush()
    producer.close()
    print(f"[3] produced {N_OLD} old messages (one batch, pre-retention)")


def produce_fresh():
    producer = make_producer()
    for i in range(N_FRESH):
        producer.send(TOPIC, value=f"fresh-{i:05d}".encode().ljust(200, b"x"))
    producer.flush()
    producer.close()
    print(f"[4] produced {N_FRESH} fresh messages (post-retention-window)")


def verify():
    consumer = KafkaConsumer(bootstrap_servers=BROKER, client_id="e2e-retention-consumer",
                             auto_offset_reset="earliest", enable_auto_commit=False)
    tp = TopicPartition(TOPIC, 0)
    beginning = consumer.beginning_offsets([tp])[tp]
    end = consumer.end_offsets([tp])[tp]
    assert beginning == N_OLD, f"beginning offset {beginning} != {N_OLD}（过期 sealed 段未删除）"
    assert end == N_OLD + N_FRESH, f"end offset {end} != {N_OLD + N_FRESH}"

    consumer.assign([tp])
    consumer.seek(tp, beginning)
    got = []
    deadline = time.time() + 10
    while len(got) < N_FRESH and time.time() < deadline:
        batch = consumer.poll(timeout_ms=500, max_records=N_FRESH - len(got))
        for records in batch.values():
            got.extend(records)
    consumer.close()

    assert len(got) == N_FRESH, f"expected {N_FRESH} fresh messages, got {len(got)}"
    for i, r in enumerate(got):
        assert r.value.startswith(f"fresh-{i:05d}".encode()), \
            f"message {i} wrong: {r.value[:12]!r}"
    print(f"[5] beginning={beginning} end={end}；{N_FRESH} 条新数据可读，过期段已删除")


if __name__ == "__main__":
    phase = sys.argv[1] if len(sys.argv) > 1 else ""
    if phase == "produce":
        produce_old()
    elif phase == "fresh":
        produce_fresh()
    elif phase == "verify":
        verify()
    else:
        print("usage: retention_sweep.py produce|fresh|verify")
        sys.exit(2)
