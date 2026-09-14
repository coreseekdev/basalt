#!/usr/bin/env python3
"""M2 ducktape 式 bounce 矩阵 v1（T-M2.6）。

轮次矩阵：{hard kill -9, clean SIGTERM} × {node1, node2}——谁在主位谁被
弹（failover 路径），谁在从位谁被追（chase 路径），两种路径都覆盖。
每轮：杀节点 → 重启 → acks=all 连续生产 → 下一轮。收尾全量读回断言
不丢不重。
注：SIGTERM 未接优雅停机 handler，语义上等同 crash（v1 边界，如实标注）。
"""
import os, signal, subprocess, time, sys
from kafka import KafkaProducer, KafkaConsumer
from kafka.structs import TopicPartition

B = ["localhost:9092", "localhost:9102", "localhost:9112"]
TOPIC = "bounce"
PID_DIR = os.environ["PID_DIR"]
DATA_DIR = os.environ["DATA_DIR"]

NODES = {1: {"BASALT_NODE_ID": "1", "BASALT_PORT": "9102"},
         2: {"BASALT_NODE_ID": "2", "BASALT_PORT": "9112"}}
COMMON = {"BASALT_HOST": "localhost", "BASALT_NUM_PARTITIONS": "2",
          "BASALT_RF": "3", "BASALT_METRICS_PORT": "0",
          "BASALT_NODES": "0=localhost:9092,1=localhost:9102,2=localhost:9112",
          "BASALT_LOG_LEVEL": "warn"}

ROUNDS = [(1, "hard"), (2, "hard"), (1, "clean"), (2, "clean"),
          (2, "hard"), (1, "clean")]


def kill_node(node: int, mode: str):
    pf = os.path.join(PID_DIR, f"port-{9092 + node * 10}")
    if not os.path.exists(pf):
        return
    pid = int(open(pf).read().strip())
    try:
        os.kill(pid, signal.SIGKILL if mode == "hard" else signal.SIGTERM)
    except ProcessLookupError:
        pass
    time.sleep(0.5)


def start_node(node: int):
    env = {**os.environ, **COMMON, **NODES[node],
           "BASALT_DATA_DIR": os.path.join(DATA_DIR, f"node{node}")}
    log = open(os.path.join(PID_DIR, f"node{node}-bounce.log"), "ab")
    p = subprocess.Popen(["./target/debug/basalt-server"], env=env,
                         stdout=log, stderr=subprocess.STDOUT)
    open(os.path.join(PID_DIR, f"port-{9092 + node * 10}"), "w").write(str(p.pid))


def produce_round(count, tag):
    """返回本轮**实际 ack 到**的值集合（不丢不变式的断言面；
    超时重试在故障期可能造成 at-least-once 重复——幂等去重属 T-M3.1）。"""
    producer = KafkaProducer(bootstrap_servers=B, acks=-1,
                             request_timeout_ms=3000, max_block_ms=3000)
    producer.partitions_for(TOPIC)
    time.sleep(0.8)
    acked = set()
    deadline = time.monotonic() + 30
    k = 0
    while k < count and time.monotonic() < deadline:
        try:
            v = f"b-{tag}-{k}"
            producer.send(TOPIC, value=v.encode(), partition=k % 2).get(timeout=3)
            acked.add(v)
            k += 1
        except Exception:
            time.sleep(0.1)
    producer.close()
    assert len(acked) >= count * 0.9, f"轮 {tag}：仅 {len(acked)}/{count} 条被 ack"
    return acked


def main():
    producer = KafkaProducer(bootstrap_servers=B, acks=-1,
                             request_timeout_ms=3000, max_block_ms=3000)
    producer.partitions_for(TOPIC)
    time.sleep(1)
    producer.close()

    expected = set()
    for rnd, (node, mode) in enumerate(ROUNDS):
        expected |= produce_round(15, f"r{rnd}a")
        kill_node(node, mode)
        # 杀后先在存活多数派上生产 5 条（failover/chase 期间的提交面 = 多数派）
        expected |= produce_round(5, f"r{rnd}b")
        start_node(node)
        time.sleep(2.5)  # 重启注册 + 追赶
        expected |= produce_round(10, f"r{rnd}c")
        total = len(expected)
        print(f"[round {rnd}] node{node} {mode}: acked total {total}", flush=True)

    # 收尾全量读回：不丢（全部 acked 值可读）；重复仅报告（at-least-once）
    consumer = KafkaConsumer(bootstrap_servers=B, auto_offset_reset="earliest")
    consumer.assign([TopicPartition(TOPIC, p) for p in range(2)])
    got = set()
    dup = 0
    deadline = time.time() + 40
    read_n = 0
    while time.time() < deadline and len(got) < total:
        batch = consumer.poll(timeout_ms=1000)
        for recs in batch.values():
            for m in recs:
                read_n += 1
                v = m.value.decode()
                if v in got:
                    dup += 1
                got.add(v)
    consumer.close()
    lost = expected - got
    print(f"[done] read {read_n}, unique {len(got)}, acked {total}, dup={dup}, lost={len(lost)}", flush=True)
    assert not lost, f"丢失 {len(lost)}: {sorted(lost)[:4]}"
    print(f"PASS ✔ (bounce 矩阵 v1：6 轮 churn 零丢失；dup={dup} 为故障期重试的 at-least-once 重复)", flush=True)


if __name__ == "__main__":
    main()
