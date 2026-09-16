#!/usr/bin/env python3
"""Basalt 三副本 acks=all 吞吐基准（T-M2.4 验收度量）。

与 throughput_confluent.py 同构，但面向 **3 节点多副本集群**：
produce acks=all 的每条消息经 leader→follower pull 复制到提交面才放行
（冻结提交面语义），吞吐反映的是复制协议的真实成本，而非单节点上限。

用法（multinode 集群拉起后）：
  SKIP_CLEANUP=1 RAFTRS=1 bash testing/e2e/run_multinode.sh multinode_raftrs.py \
    &  # 或直接由 run_multinode 拉起后手动跑
  BROKER="localhost:9092,localhost:9102,localhost:9112" \
    python3 benches/throughput_replicated.py

环境变量：NUM_MSGS（默认 20000）、MSG_SIZE（默认 1024）。
"""
import os, time
from confluent_kafka import Producer, Consumer, TopicPartition

BROKER = os.environ.get("BROKER", "localhost:9092,localhost:9102,localhost:9112")
TOPIC = f"bench-repl-{int(time.time())}"
NUM_MSGS = int(os.environ.get("NUM_MSGS", "20000"))
MSG_SIZE = int(os.environ.get("MSG_SIZE", "1024"))


def pct(data, p):
    s = sorted(data)
    return s[min(len(s) - 1, int(len(s) * p / 100))]


def main():
    payload = b"x" * MSG_SIZE

    print(f"=== 3-replica Pipelined: {NUM_MSGS} × {MSG_SIZE}B (acks=all) ===")
    delivered = [0, 0]
    producer = Producer({"bootstrap.servers": BROKER, "acks": "all",
                         "linger.ms": 5, "batch.size": 256 * 1024,
                         "request.timeout.ms": 60000})

    def on_delivery(err, msg):
        if err:
            delivered[1] += 1
        else:
            delivered[0] += 1

    for _ in range(100):
        md = producer.list_topics(TOPIC, timeout=1)
        if TOPIC in md.topics:
            break
        time.sleep(0.1)
    time.sleep(1)

    t0 = time.monotonic()
    for i in range(NUM_MSGS):
        producer.produce(TOPIC, value=payload, partition=i % 2,
                         on_delivery=on_delivery)
    remaining = producer.flush(120)
    elapsed = time.monotonic() - t0
    assert remaining == 0, f"{remaining} 条未回执"
    assert delivered[1] == 0, f"{delivered[1]} 条投递失败"
    mb = NUM_MSGS * MSG_SIZE / 1048576
    print(f"  produce: {NUM_MSGS/elapsed:.0f} msg/s  {mb/elapsed:.1f} MB/s  ({elapsed:.2f}s)")

    # === Consume（三副本集群读） ===
    consumer = Consumer({"bootstrap.servers": BROKER, "group.id": f"bench-{time.time()}",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False,
                         "fetch.max.bytes": 16 * 1024 * 1024})
    consumer.subscribe([TOPIC])
    got = 0
    c0 = time.monotonic()
    deadline = c0 + 120
    while got < NUM_MSGS and time.monotonic() < deadline:
        recs = consumer.poll(1.0)
        if recs is not None:
            got += len(recs)
    c1 = time.monotonic()
    consumer.close()
    if got < NUM_MSGS:
        print(f"  consume: {got}/{NUM_MSGS} (不完整)")
        raise SystemExit(1)
    print(f"  consume: {got/(c1-c0):.0f} msg/s  ({c1-c0:.2f}s)")

    # === Individual 延迟（acks=all 往返，含复制面） ===
    lat = []
    producer2 = Producer({"bootstrap.servers": BROKER, "acks": "all"})
    for i in range(200):
        t = time.monotonic()
        producer2.produce(TOPIC, value=payload, partition=i % 2)
        producer2.flush(30)
        lat.append((time.monotonic() - t) * 1000)
    print(f"  latency acks=all: p50={pct(lat,50):.1f}ms p90={pct(lat,90):.1f}ms p99={pct(lat,99):.1f}ms")
    print(f"  TOTAL delivered: {NUM_MSGS + 200} → PASS ✔")


if __name__ == "__main__":
    main()
