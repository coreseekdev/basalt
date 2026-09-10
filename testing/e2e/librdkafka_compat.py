#!/usr/bin/env python3
"""librdkafka（confluent-kafka）兼容性 e2e——客户端多样性档。

锁定缺陷：OffsetFetch v8+ 布局（Groups[] 取代顶层 GroupId/Topics）。
librdkafka 协商 OffsetFetch v9，旧实现回 v0-7 形状（Topics 被版本守卫丢弃，
Groups 编码为 null）→ 客户端 read buffer underflow、消费 0 条。

场景（与 consumer_group.py 同构）：
  1. producer 20 条 → 订阅组消费者 A 拉全量（走 JoinGroup/SyncGroup/Heartbeat）
  2. 显式 commit → committed() 回读校验（OffsetCommit/OffsetFetch v9 往返）
  3. 追加 10 条 → 同组消费者 B 从提交位置续读，不重不漏
用法：testing/e2e/run_e2e.sh librdkafka_compat.py"""
import time
from confluent_kafka import Producer, Consumer, TopicPartition

BROKER = "localhost:9092"
TOPIC = "librdkafka-e2e"
GROUP = "librdkafka-e2e-group"


def wait_topic(p, timeout=10):
    dl = time.monotonic() + timeout
    while time.monotonic() < dl:
        if TOPIC in p.list_topics(timeout=5).topics:
            return
        time.sleep(0.1)
    raise AssertionError(f"topic {TOPIC} 未出现")


def group_read(stop_after, label):
    c = Consumer({"bootstrap.servers": BROKER, "group.id": GROUP,
                  "auto.offset.reset": "earliest", "enable.auto.commit": False,
                  "session.timeout.ms": 6000})
    c.subscribe([TOPIC])
    got, processed = [], {}
    dl = time.monotonic() + 30
    while len(got) < stop_after and time.monotonic() < dl:
        msgs = c.consume(num_messages=10, timeout=1.0)
        for m in msgs:
            if m.error():
                continue
            got.append(m.value().decode())
            processed[m.partition()] = m.offset() + 1
            if len(got) % 5 == 0 and processed:
                c.commit(offsets=[TopicPartition(TOPIC, p, o)
                                  for p, o in processed.items()],
                         asynchronous=False)
    if processed:
        c.commit(offsets=[TopicPartition(TOPIC, p, o)
                          for p, o in processed.items()],
                 asynchronous=False)
    committed = {tp.partition: tp.offset
                 for tp in c.committed([TopicPartition(TOPIC, p) for p in range(2)], timeout=10)
                 if tp.offset >= 0}
    c.close()
    print(f"  {label}: read {len(got)}, committed {committed}")
    return got, committed


def main():
    producer = Producer({"bootstrap.servers": BROKER})
    for i in range(20):
        producer.produce(TOPIC, value=f"g-{i:04d}".encode(), partition=i % 2)
    producer.flush(30)
    wait_topic(producer)
    print("[1] produced 20")

    got1, committed1 = group_read(20, "consumer A")
    assert len(set(got1)) == 20, f"A: no-dup, got {len(got1)}"
    assert set(got1) == {f"g-{i:04d}" for i in range(20)}, "A: full coverage"
    assert committed1 == {0: 10, 1: 10}, f"committed must be 10/10, got {committed1}"
    print("[2] consumer A: full read + committed 10/10 ✔ (OffsetFetch v9 往返)")

    for i in range(20, 30):
        producer.produce(TOPIC, value=f"g-{i:04d}".encode(), partition=i % 2)
    producer.flush(30)
    time.sleep(1)
    got2, _ = group_read(10, "consumer B (resume)")
    assert sorted(got2) == [f"g-{i:04d}" for i in range(20, 30)], \
        f"B must read exactly the new 10, got {sorted(got2)}"
    print("[3] consumer B: resumed exactly at committed boundary, no dup/loss ✔")
    print("PASS ✔")


if __name__ == "__main__":
    main()
