#!/usr/bin/env python3
"""诊断探针：分别查询三个 broker 的 metadata，对比同一 topic 各分区
的 leader 报告是否一致（引擎模式重启后僵尸 leader 检测）。
前置：run_multinode.sh 已以 RAFTRS=1 拉起三节点。"""
import sys, time
from kafka.client_async import KafkaClient
from kafka.protocol.metadata import MetadataRequest_v1 as MetadataRequest

B = [("localhost", 9092), ("localhost", 9102), ("localhost", 9112)]
TOPIC = sys.argv[1] if len(sys.argv) > 1 else "probe-split"


def query(bootstrap, topic):
    c = KafkaClient(bootstrap_servers=[f"{bootstrap[0]}:{bootstrap[1]}"],
                    request_timeout_ms=3000, api_version=(0, 10))
    t0 = time.time()
    while time.time() - t0 < 4:
        fut = c.cluster.request_update()
        c.poll(timeout_ms=500, future=fut)
        if fut.is_done:
            break
    from kafka.structs import TopicPartition
    out = {}
    for part in (c.cluster.partitions_for_topic(topic) or set()):
        tp = TopicPartition(topic, part)
        leader = c.cluster.leader_for_partition(tp)
        out[part] = (leader, None)
    c.close()
    return out


def main():
    results = {}
    for b in B:
        results[b[1]] = query(b, TOPIC)
    ok = True
    ports = sorted(results)
    for p in ports:
        print(f"broker {p}: {results[p]}", flush=True)
    base = results[ports[0]]
    for p in ports[1:]:
        if results[p] != base:
            ok = False
    # 等 5s 再查一遍（收敛检查）
    time.sleep(5)
    print("--- after 5s ---", flush=True)
    results2 = {}
    for b in B:
        results2[b[1]] = query(b, TOPIC)
    for p in sorted(results2):
        print(f"broker {p}: {results2[p]}", flush=True)
    base = results2[ports[0]]
    ok2 = all(results2[p] == base for p in ports[1:])
    print(f"t0-consistent={ok} t5-consistent={ok2}", flush=True)
    sys.exit(0 if (ok and ok2) else 1)


if __name__ == "__main__":
    main()
