#!/usr/bin/env python3
"""消费组 e2e（确定性版）：
  1. 组消费者 A 消费全量，处理位置显式提交
  2. committed() 校验
  3. 新增数据后，同组消费者 B 从提交位置续读——不重不漏
多消费者并发 rebalance 的混沌场景在 chaos 阶段覆盖。"""
import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.structs import TopicPartition, OffsetAndMetadata

BROKER = "localhost:9092"
TOPIC = "group-e2e"
GROUP = "e2e-group"

def group_read(stop_after, quiet_ms=6000, label=""):
    c = KafkaConsumer(TOPIC, bootstrap_servers=BROKER, group_id=GROUP,
                      auto_offset_reset="earliest",
                      session_timeout_ms=6000,
                      enable_auto_commit=False,
                      consumer_timeout_ms=quiet_ms)
    got = []
    processed = {}
    for m in c:
        got.append((m.partition, m.offset, m.value.decode()))
        processed[m.partition] = m.offset + 1
        if len(got) % 5 == 0:
            c.commit(offsets={TopicPartition(TOPIC, p): OffsetAndMetadata(o, "")
                              for p, o in processed.items()})
        if len(got) >= stop_after:
            break
    if processed:
        c.commit(offsets={TopicPartition(TOPIC, p): OffsetAndMetadata(o, "")
                          for p, o in processed.items()})
    c.close()
    print(f"  {label}: read {len(got)}, committed {processed}")
    return got

def main():
    producer = KafkaProducer(bootstrap_servers=BROKER)
    producer.partitions_for(TOPIC)
    time.sleep(0.5)

    # 第一批
    for i in range(20):
        producer.send(TOPIC, value=f"g-{i:04d}".encode(), partition=i % 2)
    producer.flush()
    print("[1] produced 20")

    got1 = group_read(20, label="consumer A")
    vals1 = [v for _, _, v in got1]
    assert len(set(vals1)) == 20, "consumer A: no-dup"
    assert set(vals1) == {f"g-{i:04d}" for i in range(20)}, "consumer A: full coverage"
    print("[2] consumer A: exactly-once full read ✔")

    # 提交位置校验
    c = KafkaConsumer(bootstrap_servers=BROKER, group_id=GROUP)
    tps = [TopicPartition(TOPIC, 0), TopicPartition(TOPIC, 1)]
    def _off(v):
        return v.offset if hasattr(v, "offset") else v
    committed = {tp.partition: _off(c.committed(tp)) for tp in tps}
    c.close()
    assert committed == {0: 10, 1: 10}, f"committed must be 10/10, got {committed}"
    print("[3] committed offsets = 10/10 ✔")

    # 第二批：新消费者同组续读
    for i in range(20, 30):
        producer.send(TOPIC, value=f"g-{i:04d}".encode(), partition=i % 2)
    producer.flush()
    producer.close()
    time.sleep(1)
    got2 = group_read(30, label="consumer B (resume)")
    vals2 = [v for _, _, v in got2]
    expected2 = {f"g-{i:04d}" for i in range(20, 30)}
    assert set(vals2) == expected2, f"B must read exactly the new 10, got {sorted(vals2)}"
    assert len(set(vals2)) == len(vals2) == 10, "B: no-dup"
    print("[4] consumer B: resumed exactly at committed boundary, no dup/loss ✔")
    print("PASS ✔")

if __name__ == "__main__":
    main()
