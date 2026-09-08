#!/usr/bin/env python3
"""3 节点 failover e2e：kill -9 leader → FAILOVER → 零丢失验证。
前置条件：run_multinode.sh 已拉起 3 个 broker。"""
import os, time, sys
from kafka import KafkaProducer, KafkaConsumer
from kafka.structs import TopicPartition

B = ["localhost:9092", "localhost:9102", "localhost:9112"]
TOPIC = "fo"
PID_DIR = os.environ.get("PID_DIR", "/tmp/basalt-multinode-pids")

def main():
    # 阶段 1: auto-create + metadata 传播等待 + 40 条 acks=all
    producer = KafkaProducer(bootstrap_servers=B, acks=-1,
                             request_timeout_ms=15000, max_block_ms=15000)
    producer.partitions_for(TOPIC)
    time.sleep(1)  # 等 metadata 传播到全 broker
    for i in range(40):
        producer.send(TOPIC, value=f"k-{i}".encode(), partition=i % 2).get(timeout=15)
    producer.close()
    print("[1] 40 pre produced (acks=all)", flush=True)

    # 阶段 2: kill -9 p1 leader（node1 = port 9102）
    pid_file = os.path.join(PID_DIR, "port-9102")
    if os.path.exists(pid_file):
        pid = int(open(pid_file).read().strip())
        os.kill(pid, 9)
        print(f"[2] killed node1 (pid={pid})", flush=True)
    time.sleep(8)  # 等 heartbeat timeout(4s) + failover + metadata 收敛

    # 阶段 3: 只用存活节点 post-failover 生产 + 全量读回
    B_alive = ["localhost:9092", "localhost:9112"]
    producer = KafkaProducer(bootstrap_servers=B_alive, acks=-1,
                             request_timeout_ms=15000, max_block_ms=15000)
    for i in range(40, 60):
        producer.send(TOPIC, value=f"k-{i}".encode(), partition=i % 2).get(timeout=15)
    producer.close()
    print("[3] 20 post produced (acks=all)", flush=True)

    # 阶段 4: 全量读回验证
    consumer = KafkaConsumer(bootstrap_servers=B_alive, auto_offset_reset="earliest")
    tps = [TopicPartition(TOPIC, 0), TopicPartition(TOPIC, 1)]
    consumer.assign(tps)
    got = set()
    deadline = time.time() + 20
    while time.time() < deadline:
        batch = consumer.poll(timeout_ms=1000)
        n = sum(len(v) for v in batch.values())
        for recs in batch.values():
            for m in recs:
                got.add(m.value.decode())
        if n == 0 and len(got) >= 55:
            break
    consumer.close()

    lost = {f"k-{i}" for i in range(60)} - got
    print(f"[4] read: {len(got)}/60; lost: {sorted(lost)[:5] if lost else 'NONE'}", flush=True)
    if lost:
        print(f"FAIL: {len(lost)} messages lost after failover", flush=True)
        sys.exit(1)
    print("PASS ✔ (kill -9 leader → 60/60 zero loss)", flush=True)

if __name__ == "__main__":
    main()
