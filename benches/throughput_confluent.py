#!/usr/bin/env python3
"""Basalt 吞吐基准——confluent-kafka (librdkafka) 档。

与 throughput.py（kafka-python 档）场景一一对应，保证横向可比：
  A) pipelined：全部 produce 后一次 flush（kafka 标准吞吐测量法）
  B) consume：assign 双分区拉满
  C) individual：逐条 produce+等回执（延迟上界 = RTT）
用法：由 run_bench.sh 拉起 broker 后运行。"""
import os, time
from confluent_kafka import Producer, Consumer, TopicPartition

BROKER = os.environ.get("BROKER", "localhost:9092")
TOPIC = f"bench-{int(time.time())}"
NUM_MSGS = int(os.environ.get("NUM_MSGS", "50000"))
MSG_SIZE = int(os.environ.get("MSG_SIZE", "1024"))

def pct(data, p):
    s = sorted(data)
    return s[min(len(s)-1, int(len(s)*p/100))]

def main():
    payload = b"x" * MSG_SIZE

    # === A. Pipelined 吞吐 ===
    print(f"=== Pipelined: {NUM_MSGS} × {MSG_SIZE}B (acks=all) ===")
    delivered = [0, 0]  # [ok, err]
    producer = Producer({"bootstrap.servers": BROKER, "acks": "all",
                         "linger.ms": 5, "batch.size": 256*1024,
                         "request.timeout.ms": 60000})

    def on_delivery(err, msg):
        if err:
            delivered[1] += 1
        else:
            delivered[0] += 1

    # metadata 轮询让客户端拿到分区拓扑
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
    remaining = producer.flush(60)
    elapsed = time.monotonic() - t0
    assert remaining == 0, f"{remaining} 条未回执"
    assert delivered[1] == 0, f"{delivered[1]} 条投递失败"
    mb = NUM_MSGS * MSG_SIZE / 1048576
    print(f"  produce: {NUM_MSGS/elapsed:.0f} msg/s  {mb/elapsed:.1f} MB/s  ({elapsed:.2f}s)")
    producer.flush(30)

    # === B. Consume 吞吐 ===
    consumer = Consumer({"bootstrap.servers": BROKER, "group.id": f"bench-{time.time()}",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False,
                         "fetch.max.bytes": 16*1024*1024})
    consumer.assign([TopicPartition(TOPIC, p) for p in range(2)])
    got = 0
    t1 = time.monotonic()
    while got < NUM_MSGS and time.monotonic() - t1 < 60:
        msgs = consumer.consume(num_messages=2000, timeout=1.0)
        for m in msgs:
            if not m.error():
                got += 1
    ct = time.monotonic() - t1
    consumer.close()
    if ct > 0:
        print(f"  consume: {got/ct:.0f} msg/s  {got*MSG_SIZE/1048576/ct:.1f} MB/s")
    assert got == NUM_MSGS, f"consume 不足：{got}/{NUM_MSGS}"

    # === C. Individual 延迟（RTT 上界） ===
    print("=== Individual: 500 msgs (produce+回执 往返) ===")
    producer = Producer({"bootstrap.servers": BROKER, "acks": "all",
                         "request.timeout.ms": 30000})
    lat = []
    for i in range(500):
        st = time.monotonic()
        producer.produce(TOPIC, value=payload, partition=i % 2)
        assert producer.flush(30) == 0
        lat.append((time.monotonic() - st) * 1000)
    print(f"  latency p50={pct(lat,50):.1f}ms p90={pct(lat,90):.1f}ms p99={pct(lat,99):.1f}ms")

    total_msgs = NUM_MSGS + 500
    print(f"\n  TOTAL delivered: {total_msgs} → PASS ✔")

if __name__ == "__main__":
    main()
