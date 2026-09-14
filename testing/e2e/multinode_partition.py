#!/usr/bin/env python3
"""M2 分区注入 + 追赶场景 e2e。

阶段：
  [1] 基线 20 条 acks=all
  [2] node2 重启并携带 BASALT_BLOCK_PEERS=1（与 leader node1 断边）——
      follower pull 停摆，分区期间 node2 落后
  [3] 分区期间生产 20 条（acks=all：fresh follower 仅 node0，
      fresh+1=2 ≥ 多数派 2 → 提交面仍为多数派，C1 成立）
  [4] 治愈：node2 无 env 重启 → FollowerPull 从本地 LEO 拉齐
  [5] 全量读回：40/40 不丢不重
前置：run_multinode.sh 已拉起 3 节点（需导出 DATA_DIR）。"""
import os, time, signal, subprocess, sys
from kafka import KafkaProducer, KafkaConsumer
from kafka.structs import TopicPartition

B = ["localhost:9092", "localhost:9102", "localhost:9112"]
TOPIC = "chase"
PID_DIR = os.environ["PID_DIR"]
DATA_DIR = os.environ["DATA_DIR"]
NODE_ENV = {
    "BASALT_NODE_ID": "2", "BASALT_PORT": "9112", "BASALT_HOST": "localhost",
    "BASALT_NUM_PARTITIONS": "2", "BASALT_RF": "3", "BASALT_METRICS_PORT": "0",
    "BASALT_NODES": "0=localhost:9092,1=localhost:9102,2=localhost:9112",
    "BASALT_LOG_LEVEL": "warn",
}


def kill_node2():
    pf = os.path.join(PID_DIR, "port-9112")
    if os.path.exists(pf):
        pid = int(open(pf).read().strip())
        os.kill(pid, signal.SIGKILL)
        time.sleep(0.5)


def start_node2(blocked: bool):
    env = {**os.environ, **NODE_ENV}
    if blocked:
        env["BASALT_BLOCK_PEERS"] = "1"
    log = open(os.path.join(PID_DIR, "node2-scenario.log"), "ab")
    p = subprocess.Popen(["./target/debug/basalt-server"], env=env,
                         stdout=log, stderr=subprocess.STDOUT)
    open(os.path.join(PID_DIR, "port-9112"), "w").write(str(p.pid))
    time.sleep(1.5)  # 启动 + 注册 + 首轮元数据
    return p


def produce(count, start):
    producer = KafkaProducer(bootstrap_servers=B, acks=-1,
                             request_timeout_ms=3000, max_block_ms=3000)
    producer.partitions_for(TOPIC)
    time.sleep(0.8)
    pending = list(range(start, start + count))
    deadline = time.monotonic() + 30
    while pending and time.monotonic() < deadline:
        i = pending[0]
        try:
            producer.send(TOPIC, value=f"c-{i}".encode(), partition=i % 2).get(timeout=3)
            pending.pop(0)
        except Exception:
            time.sleep(0.1)
    producer.close()
    assert not pending, f"{len(pending)} 条未完成（start={start}）"


def read_back(total):
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
    producer.close()

    print("[1] baseline 20", flush=True)
    produce(20, 0)

    print("[2] node2 restart with BLOCK_PEERS=1 (partitioned from leader)", flush=True)
    kill_node2()
    start_node2(blocked=True)
    time.sleep(2.5)

    print("[3] produce 20 while partitioned", flush=True)
    produce(20, 20)

    print("[4] heal: node2 restart without block (chase from local LEO)", flush=True)
    kill_node2()
    start_node2(blocked=False)
    time.sleep(3)  # 追赶 + 复制

    got = read_back(40)
    lost = {f"c-{i}" for i in range(40)} - got
    dup = 40 - len(got)
    print(f"[5] read {len(got)}/40, lost={sorted(lost)[:4] if lost else 'NONE'}", flush=True)
    assert not lost, f"丢失 {len(lost)}"
    assert dup == 0, f"重复 {dup}"
    print("PASS ✔ (分区→追赶→治愈：不丢不重)", flush=True)


if __name__ == "__main__":
    main()
