#!/usr/bin/env python3
"""M2 L1 门禁：崩溃 failover <2s（kill -9 leader → 新主可服务，acks=all）。

度量口径：kill 时刻 T0 → 第一次 acks=all 生产成功时刻 T1（存活 broker 上）。
链路 = 心跳检出(≤1.0s) + FAILOVER 指派 + 元数据收敛(≤0.2s) + 客户端重试。
用法：run_multinode.sh 场景 2。"""
import os, time, sys
from kafka import KafkaProducer

B_ALL = ["localhost:9092", "localhost:9102", "localhost:9112"]
B_ALIVE = ["localhost:9092", "localhost:9112"]
TOPIC = "l1"
PID_DIR = os.environ.get("PID_DIR", "/tmp/basalt-multinode-pids")


def main():
    producer = KafkaProducer(bootstrap_servers=B_ALL, acks=-1,
                             request_timeout_ms=5000, max_block_ms=5000)
    producer.partitions_for(TOPIC)
    time.sleep(1)
    for i in range(10):
        producer.send(TOPIC, value=f"pre-{i}".encode(), partition=i % 2).get(timeout=5)
    print("[1] 10 pre produced", flush=True)

    # kill -9 当前 leader（node1=9102 持有写流量——l1 主题两分区轮询路由）
    pid = int(open(os.path.join(PID_DIR, "port-9102")).read().strip())
    t0 = time.monotonic()
    os.kill(pid, 9)
    print(f"[2] killed node1 at T0", flush=True)

    # 重试循环：第一次 acks=all 成功即新主可服务
    deadline = t0 + 10
    ok_at = None
    i = 100
    producer.close()
    producer = KafkaProducer(bootstrap_servers=B_ALIVE, acks=-1,
                             request_timeout_ms=1000, max_block_ms=1000,
                             retries=0)
    while time.monotonic() < deadline:
        try:
            producer.send(TOPIC, value=f"post-{i}".encode(), partition=0).get(timeout=1)
            ok_at = time.monotonic()
            break
        except Exception:
            time.sleep(0.05)
            i += 1
    assert ok_at, "10s 内无任何 acks=all 成功——failover 失败"
    elapsed = ok_at - t0
    print(f"[3] first acks=all success after {elapsed*1000:.0f} ms", flush=True)
    assert elapsed < 2.0, f"L1 门禁 2s 超标：{elapsed*1000:.0f}ms"
    producer.close()
    print("PASS ✔ (failover <2s)", flush=True)


if __name__ == "__main__":
    main()
