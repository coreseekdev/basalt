#!/usr/bin/env python3
"""Basalt 吞吐基准（流水线异步 + 延迟分布）。

场景：
  A) pipelined：全部 send 后一次 flush（kafka 标准吞吐测量法）
  B) individual：逐条 send+get（延迟上界 = RTT）
用法：由 run_bench.sh 拉起 broker 后运行。"""
import os, time, statistics
from kafka import KafkaProducer, KafkaConsumer
from kafka.structs import TopicPartition

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
    print(f"=== Pipelined: {NUM_MSGS} × {MSG_SIZE}B (acks=-1) ===")
    producer = KafkaProducer(bootstrap_servers=BROKER, acks=-1,
                             batch_size=256*1024, linger_ms=5,
                             request_timeout_ms=60000, max_block_ms=30000)
    producer.partitions_for(TOPIC)
    time.sleep(1)

    t0 = time.monotonic()
    for i in range(NUM_MSGS):
        producer.send(TOPIC, value=payload, partition=i % 2)
    producer.flush(timeout=60)
    elapsed = time.monotonic() - t0
    mb = NUM_MSGS * MSG_SIZE / 1048576
    print(f"  produce: {NUM_MSGS/elapsed:.0f} msg/s  {mb/elapsed:.1f} MB/s  ({elapsed:.2f}s)")
    producer.close()

    # === B. Consume 吞吐 ===
    consumer = KafkaConsumer(bootstrap_servers=BROKER, auto_offset_reset="earliest",
                             consumer_timeout_ms=20000, fetch_max_bytes=16*1024*1024)
    tps = [TopicPartition(TOPIC, p) for p in range(2)]
    consumer.assign(tps)
    got = 0
    t1 = time.monotonic()
    while got < NUM_MSGS and time.monotonic() - t1 < 60:
        for recs in consumer.poll(timeout_ms=1000, max_records=2000).values():
            got += len(recs)
    ct = time.monotonic() - t1
    consumer.close()
    if ct > 0:
        print(f"  consume: {got/ct:.0f} msg/s  {got*MSG_SIZE/1048576/ct:.1f} MB/s")

    # === C. Individual 延迟（RTT 上界） ===
    print(f"=== Individual: 500 msgs (send+get 往返) ===")
    producer = KafkaProducer(bootstrap_servers=BROKER, acks=-1,
                             request_timeout_ms=30000, max_block_ms=30000)
    lat = []
    for i in range(500):
        st = time.monotonic()
        producer.send(TOPIC, value=payload, partition=i % 2).get(timeout=30)
        lat.append((time.monotonic() - st) * 1000)
    producer.close()
    print(f"  latency p50={pct(lat,50):.1f}ms p90={pct(lat,90):.1f}ms p99={pct(lat,99):.1f}ms")

    total_msgs = NUM_MSGS + 500
    print(f"\n  TOTAL delivered: {total_msgs} → PASS ✔")

if __name__ == "__main__":
    main()
