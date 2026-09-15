#!/usr/bin/env python3
"""M2.2 ISR 收缩/重回 e2e（账本 ㉟ 专属场景，T-M2.2）。

机制：SIGSTOP 冻结 follower（node2）→ 其 LEO 上报停 → leader 在 isr_lag
(500ms) 内显式收缩（ISR shrink 日志）→ 提交面 = 2/3，acks=all 继续成功
（收缩只损失可用性冗余，不丢可用性）→ SIGCONT 解冻 → 追赶 → 完全追平
重回 ISR（ISR expand 日志）→ 后续 acks 提交面恢复 3/3。

断言：
  [2] 收缩期间 acks=all 连续成功（提交面收缩语义）
  [4] 解冻追赶后 acks 恢复
  [5] 全量读回零丢失
  [6] 节点日志含 ISR shrink 与 ISR expand（收缩/重回均为显式可观测动作）
前置：run_multinode.sh 已拉起三节点（BASALT_ISR_LAG_MS=500 由 runner 注入）。
"""
import os, signal, time, sys
from kafka import KafkaProducer, KafkaConsumer
from kafka.structs import TopicPartition

B = ["localhost:9092", "localhost:9102", "localhost:9112"]
TOPIC = "isr"
PID_DIR = os.environ["PID_DIR"]


def produce_batch(producer, start, count, tag, timeout=10):
    ok = 0
    for i in range(start, start + count):
        producer.send(TOPIC, value=f"{tag}-{i}".encode(), partition=i % 2).get(timeout=timeout)
        ok += 1
    return ok


def main():
    producer = KafkaProducer(bootstrap_servers=B, acks=-1, retries=5,
                             request_timeout_ms=15000, max_block_ms=15000)
    producer.partitions_for(TOPIC)
    time.sleep(1)
    n = produce_batch(producer, 0, 20, "a")
    assert n == 20
    producer.close()
    print(f"[1] 20 pre produced (acks=all)", flush=True)

    # 冻结 node2：pull/hb 全停 → leader 在 isr_lag 内收缩
    pid2 = int(open(os.path.join(PID_DIR, "port-9112")).read().strip())
    os.kill(pid2, signal.SIGSTOP)
    print(f"[2] SIGSTOP node2 (pid={pid2})", flush=True)
    time.sleep(1.5)  # > isr_lag(500ms)：收缩完成

    producer = KafkaProducer(bootstrap_servers=B[:2], acks=-1, retries=5,
                             request_timeout_ms=15000, max_block_ms=15000)
    n = produce_batch(producer, 100, 20, "b")
    assert n == 20, f"收缩期间 acks=all 失败：{n}/20"
    producer.close()
    print(f"[3] 20 produced during shrink (提交面 2/3)", flush=True)

    # 解冻后立刻把两分区交接给尚未追平的 node2——逼出就任拉齐
    # （reconciliation）：node2 须从存活副本拉回 b-* 尾部后才能服务，
    # 否则 acks=b-* 的消息会被未拉齐的主截掉（㉟ 残余窗口的 e2e 演练）
    os.kill(pid2, signal.SIGCONT)
    print(f"[4] SIGCONT node2 + transfer both partitions -> node2", flush=True)
    import socket, struct

    def transfer(topic: str, partition: int, to: int):
        sock = socket.create_connection(("localhost", 9093), timeout=5)
        name = topic.encode()
        payload = struct.pack(">h", len(name)) + name + struct.pack(">ii", partition, to)
        sock.sendall(struct.pack(">I", len(payload) + 1) + bytes([6]) + payload)
        head = b""
        while len(head) < 4:
            chunk = sock.recv(4 - len(head))
            assert chunk
            head += chunk
        (length,) = struct.unpack(">I", head)
        body = b""
        while len(body) < length:
            chunk = sock.recv(length - len(body))
            assert chunk
            body += chunk
        sock.close()
        return struct.unpack(">h", body)[0]

    for part in (0, 1):
        deadline = time.monotonic() + 3
        code = -1
        while time.monotonic() < deadline:
            code = transfer(TOPIC, part, 2)
            if code == 0:
                break
            time.sleep(0.1)  # node2 心跳恢复（alive 门控）需要一拍
        assert code == 0, f"transfer p{part} -> node2 失败 code={code}"

    producer = KafkaProducer(bootstrap_servers=B, acks=-1, retries=5,
                             request_timeout_ms=15000, max_block_ms=15000)
    n = produce_batch(producer, 200, 20, "c")
    assert n == 20, f"重回+拉齐后 acks=all 失败：{n}/20"
    producer.close()
    print(f"[5] 20 post-reconcile produced (node2 leadership, 提交面 3/3)", flush=True)

    consumer = KafkaConsumer(bootstrap_servers=B, auto_offset_reset="earliest")
    consumer.assign([TopicPartition(TOPIC, p) for p in range(2)])
    got = set()
    expected = {f"a-{i}" for i in range(20)} | {f"b-{i}" for i in range(100, 120)} | {f"c-{i}" for i in range(200, 220)}
    deadline = time.time() + 25
    while time.time() < deadline and len(got) < len(expected):
        batch = consumer.poll(timeout_ms=1000)
        for recs in batch.values():
            for m in recs:
                got.add(m.value.decode())
    consumer.close()
    lost = expected - got
    assert not lost, f"丢失 {len(lost)}: {sorted(lost)[:5]}"
    print(f"[6] read {len(got)}/{len(expected)} zero loss", flush=True)

    # 收缩/重回必须留下显式观测痕迹
    shrink = expand = 0
    for f in os.listdir(PID_DIR):
        if f.startswith("node") and f.endswith(".log"):
            try:
                data = open(os.path.join(PID_DIR, f), "rb").read().decode("utf-8", "ignore")
                shrink += data.count("ISR shrink")
                expand += data.count("ISR expand")
            except OSError:
                pass
    assert shrink >= 1, f"未见 ISR shrink 日志（收缩应显式可观测）shrink={shrink} expand={expand}"
    assert expand >= 1, f"未见 ISR expand 日志（重回应显式可观测）shrink={shrink} expand={expand}"
    print(f"[7] ISR shrink={shrink} expand={expand} (explicit observability)", flush=True)
    print("PASS ✔ (ISR 收缩→跳过提交面 acks→重回→拉齐，零丢失)", flush=True)


if __name__ == "__main__":
    main()
