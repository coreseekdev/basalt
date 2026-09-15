#!/usr/bin/env python3
"""M2.1 引擎 runtime 三进程联调（BASALT_CTRL_RAFT_ENGINE=raftrs）。

验证：三节点 raft 控制器组选举 → CreateTopic 经 raft propose 复制到
全部节点 → produce/consume 全链路不丢。
前置：run_multinode.sh 已以 raftrs 模式拉起三节点。"""
import time, sys
from kafka import KafkaProducer, KafkaConsumer
from kafka.structs import TopicPartition

B = ["localhost:9092", "localhost:9102", "localhost:9112"]
TOPIC = "raftrs-e2e"


def read_all(total):
    consumer = KafkaConsumer(bootstrap_servers=B, auto_offset_reset="earliest")
    consumer.assign([TopicPartition(TOPIC, p) for p in range(2)])
    got = set()
    deadline = time.time() + 25
    while time.time() < deadline and len(got) < total:
        batch = consumer.poll(timeout_ms=1000)
        for recs in batch.values():
            for m in recs:
                got.add(m.value.decode())
    consumer.close()
    return got


def main():
    producer = KafkaProducer(bootstrap_servers=B, acks=-1,
                             request_timeout_ms=5000, max_block_ms=5000)
    producer.partitions_for(TOPIC)
    time.sleep(1)
    for i in range(30):
        producer.send(TOPIC, value=f"r-{i}".encode(), partition=i % 2).get(timeout=5)
    producer.close()

    got = read_all(30)
    lost = {f"r-{i}" for i in range(30)} - got
    print(f"read {len(got)}/30 lost={len(lost)}", flush=True)
    assert not lost, f"丢失 {len(lost)}"
    print("PASS ✔ (raftrs 引擎 runtime 三进程联调)", flush=True)


if __name__ == "__main__":
    main()
