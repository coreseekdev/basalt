#!/usr/bin/env python3
"""M2.1 控制器 kill 场景（引擎模式）：kill -9 node0（静态控制器）→
raft 重选 → 存活节点 controller 接管 → 生产/消费不丢。
前置：run_multinode.sh 已以 RAFTRS=1 拉起三节点。"""
import os, time, sys
from kafka import KafkaProducer, KafkaConsumer
from kafka.structs import TopicPartition

B_ALIVE = ["localhost:9102", "localhost:9112"]
B_ALL = ["localhost:9092", "localhost:9102", "localhost:9112"]
TOPIC = "ctrl-kill"
PID_DIR = os.environ.get("PID_DIR", "/tmp/basalt-multinode-pids")


def main():
    producer = KafkaProducer(bootstrap_servers=B_ALL, acks=-1,
                             request_timeout_ms=5000, max_block_ms=5000)
    producer.partitions_for(TOPIC)
    time.sleep(1)
    for i in range(20):
        producer.send(TOPIC, value=f"pre-{i}".encode(), partition=i % 2).get(timeout=5)
    producer.close()
    print("[1] 20 pre produced", flush=True)

    # kill -9 node0（静态控制器 + leader）
    pid = int(open(os.path.join(PID_DIR, "port-9092")).read().strip())
    t0 = time.monotonic()
    os.kill(pid, 9)
    print(f"[2] killed node0 (controller) at T0", flush=True)

    # 存活节点继续生产（raft 重选 + controller 接管）
    producer = KafkaProducer(bootstrap_servers=B_ALIVE, acks=-1,
                             request_timeout_ms=1000, max_block_ms=1000, retries=0)
    deadline = t0 + 15
    i = 100
    ok_at = None
    while time.monotonic() < deadline:
        try:
            producer.send(TOPIC, value=f"post-{i}".encode(), partition=0).get(timeout=1)
            ok_at = time.monotonic()
            break
        except Exception:
            time.sleep(0.1)
            i += 1
    assert ok_at, "15s 内无 acks=all 成功——controller kill 后未恢复"
    for j in range(i + 1, i + 16):
        try:
            producer.send(TOPIC, value=f"post-{j}".encode(), partition=0).get(timeout=2)
        except Exception:
            pass
    producer.close()
    elapsed = ok_at - t0
    print(f"[3] first acks=all at +{elapsed*1000:.0f}ms; total post: {i+15-100}", flush=True)

    consumer = KafkaConsumer(bootstrap_servers=B_ALIVE, auto_offset_reset="earliest")
    consumer.assign([TopicPartition(TOPIC, p) for p in range(2)])
    got = set()
    deadline = time.time() + 25
    while time.time() < deadline and len(got) < 36:
        batch = consumer.poll(timeout_ms=1000)
        for recs in batch.values():
            for m in recs:
                got.add(m.value.decode())
    consumer.close()
    lost = {f"pre-{k}" for k in range(20)} - got
    print(f"[4] read {len(got)}, lost={len(lost)}", flush=True)
    assert not lost, f"丢失 {len(lost)}"
    print("PASS ✔ (controller kill：存活节点接管，不丢)", flush=True)


if __name__ == "__main__":
    main()
