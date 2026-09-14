#!/usr/bin/env python3
"""M2 in-flight 重复投递场景 e2e（T-M2.5）。

node2 携带 BASALT_PULL_REPLAY=1 重启：follower 对每一切片做**重放**
（同数据二次 apply）。绝对偏移策略下重放被 base 校验拒绝（replica
gap 路径）或零新记录——offset 纪律即幂等。乱序/hold 场景同理由该
纪律吸收（乱序到达 = base 不匹配拒绝 + 重试）。
验证：分区式生产 30 条 → 集群读回恰 30 个唯一值。
前置：run_multinode.sh 已拉起 3 节点（需导出 DATA_DIR）。"""
import os, time, signal, subprocess
from kafka import KafkaProducer, KafkaConsumer
from kafka.structs import TopicPartition

B = ["localhost:9092", "localhost:9102", "localhost:9112"]
TOPIC = "replay"
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
        try:
            os.kill(int(open(pf).read().strip()), signal.SIGKILL)
        except ProcessLookupError:
            pass
        time.sleep(0.5)


def start_node2(replay: bool):
    env = {**os.environ, **NODE_ENV}
    if replay:
        env["BASALT_PULL_REPLAY"] = "1"
    log = open(os.path.join(PID_DIR, "node2-replay.log"), "ab")
    p = subprocess.Popen(["./target/debug/basalt-server"], env=env,
                         stdout=log, stderr=subprocess.STDOUT)
    open(os.path.join(PID_DIR, "port-9112"), "w").write(str(p.pid))
    time.sleep(2)


def main():
    producer = KafkaProducer(bootstrap_servers=B, acks=-1,
                             request_timeout_ms=3000, max_block_ms=3000)
    producer.partitions_for(TOPIC)
    time.sleep(1)
    producer.close()

    print("[1] node2 restart with PULL_REPLAY=1", flush=True)
    kill_node2()
    start_node2(replay=True)

    print("[2] produce 30 under duplicate delivery", flush=True)
    producer = KafkaProducer(bootstrap_servers=B, acks=-1,
                             request_timeout_ms=3000, max_block_ms=3000)
    producer.partitions_for(TOPIC)
    time.sleep(0.8)
    for i in range(30):
        producer.send(TOPIC, value=f"r-{i}".encode(), partition=i % 2).get(timeout=3)
    producer.close()
    time.sleep(2)  # 等复制收敛（含重放）

    consumer = KafkaConsumer(bootstrap_servers=B, auto_offset_reset="earliest")
    consumer.assign([TopicPartition(TOPIC, p) for p in range(2)])
    got = set()
    read_n = 0
    deadline = time.time() + 25
    while time.time() < deadline and len(got) < 30:
        batch = consumer.poll(timeout_ms=1000)
        for recs in batch.values():
            for m in recs:
                read_n += 1
                got.add(m.value.decode())
    consumer.close()
    lost = {f"r-{i}" for i in range(30)} - got
    print(f"[3] read {read_n}, unique {len(got)}/30, lost={len(lost)}", flush=True)
    assert not lost, f"丢失 {len(lost)}"
    assert read_n == 30, f"读流出现重复（{read_n} > 30）——重放未被 offset 纪律吸收"
    print("PASS ✔ (in-flight 重复投递：offset 纪律幂等)", flush=True)


if __name__ == "__main__":
    main()
