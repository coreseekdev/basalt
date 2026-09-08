#!/usr/bin/env python3
"""Basalt 吞吐/延迟基准。
用法：先由 run_bench.sh 拉起单节点 broker，然后运行本脚本。
输出：produce/fetch 吞吐 (msg/s, MB/s) 与 P50/P99 延迟。
"""
import os, sys, time, statistics
from kafka import KafkaProducer, KafkaConsumer
from kafka.structs import TopicPartition

BROKER = os.environ.get("BROKER", "localhost:9092")
TOPIC = f"bench-{int(time.time())}"
NUM_MSGS = int(os.environ.get("NUM_MSGS", "10000"))
MSG_SIZE = int(os.environ.get("MSG_SIZE", "1024"))
PARTITIONS = 2

def percentile(data, p):
    s = sorted(data)
    k = max(0, min(len(s) - 1, int(len(s) * p / 100)))
    return s[k]

def main():
    payload = b"x" * MSG_SIZE

    # produce
    producer = KafkaProducer(bootstrap_servers=BROKER, acks=-1,
                             request_timeout_ms=30000, max_block_ms=30000)
    producer.partitions_for(TOPIC)
    time.sleep(0.5)

    print(f"=== Produce: {NUM_MSGS} msgs × {MSG_SIZE}B ===")
    latencies = []
    t0 = time.monotonic()
    for i in range(NUM_MSGS):
        st = time.monotonic()
        f = producer.send(TOPIC, value=payload, partition=i % PARTITIONS)
        f.get(timeout=30)
        latencies.append((time.monotonic() - st) * 1000)
    elapsed = time.monotonic() - t0
    producer.flush(timeout=30)
    producer.close()

    total_mb = NUM_MSGS * MSG_SIZE / 1024 / 1024
    print(f"  throughput: {NUM_MSGS / elapsed:.0f} msg/s, {total_mb / elapsed:.1f} MB/s")
    print(f"  latency p50={percentile(latencies,50):.1f}ms p99={percentile(latencies,99):.1f}ms max={max(latencies):.1f}ms")

    # consume
    consumer = KafkaConsumer(bootstrap_servers=BROKER, auto_offset_reset="earliest",
                             consumer_timeout_ms=15000)
    tps = [TopicPartition(TOPIC, p) for p in range(PARTITIONS)]
    consumer.assign(tps)
    got = []
    ct0 = time.monotonic()
    while len(got) < NUM_MSGS and time.monotonic() - ct0 < 60:
        for recs in consumer.poll(timeout_ms=1000).values():
            got.extend(m.value for m in recs)
    ct = time.monotonic() - ct0
    consumer.close()

    assert len(got) == NUM_MSGS, f"expected {NUM_MSGS}, got {len(got)}"
    consume_mb = len(got) * MSG_SIZE / 1024 / 1024
    print(f"=== Consume: {NUM_MSGS} msgs ===")
    print(f"  throughput: {NUM_MSGS / ct:.0f} msg/s, {consume_mb / ct:.1f} MB/s")
    print("PASS ✔")

if __name__ == "__main__":
    main()
