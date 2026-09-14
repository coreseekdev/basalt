#!/usr/bin/env python3
"""M2 计划内交接 e2e：TransferLeader（epoch+1 计划内指派）。

场景：acks=all 连续流 → 内部 RPC 触发 p0 交接（node0 → node2）→ 测量
ack 间隙（服务端窗口 = 元数据广播周期）→ 交接后再交回 → 全量读回
不丢不重。前置：run_multinode.sh 已拉起 3 节点。"""
import os, socket, struct, time, sys
from kafka import KafkaProducer, KafkaConsumer
from kafka.structs import TopicPartition

B = ["localhost:9092", "localhost:9102", "localhost:9112"]
TOPIC = "xfer"


def transfer(topic: str, partition: int, to: int, timeout=5.0):
    """内部 RPC MSG_TRANSFER（发往控制器 node0 的内部端口）。"""
    sock = socket.create_connection(("localhost", 9093), timeout=timeout)
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
    code = struct.unpack(">h", body)[0]
    assert code == 0, f"transfer 失败：code={code}"
    print(f"    transfer(topic={topic}, p={partition}, to=node{to}) ok", flush=True)


def main():
    producer = KafkaProducer(bootstrap_servers=B, acks=-1,
                             request_timeout_ms=5000, max_block_ms=5000)
    producer.partitions_for(TOPIC)
    time.sleep(1)

    # 连续 acks=all 流（10ms 节拍），记录每条 ack 的墙钟
    acks = []
    errors = 0
    t_end = time.monotonic() + 4
    i = 0
    while time.monotonic() < t_end:
        try:
            t = time.monotonic()
            producer.send(TOPIC, value=f"x-{i}".encode(), partition=0).get(timeout=2)
            acks.append((t, i))
            i += 1
        except Exception:
            errors += 1
            time.sleep(0.02)
        time.sleep(0.005)
    n_pre = i
    assert len(acks) >= 15, f"流太短：{len(acks)}（acks=all 停等周期受限属正常）"

    # 触发交接 p0: node0 → node2（流保持），测最大 ack 间隙
    transfer(TOPIC, 0, 2)
    t_transfer = time.monotonic()
    t_end = t_transfer + 4
    while time.monotonic() < t_end:
        try:
            t = time.monotonic()
            producer.send(TOPIC, value=f"x-{i}".encode(), partition=0).get(timeout=2)
            acks.append((t, i))
            i += 1
        except Exception:
            errors += 1
            time.sleep(0.02)
        time.sleep(0.005)

    # 交接窗口 = 交接命令后最大的相邻 ack 间隙（服务端窗口 + 客户端刷新）
    post = [t for (t, _) in acks if t >= t_transfer - 0.1]
    gaps = [b - a for a, b in zip(post, post[1:])]
    window = max(gaps) if gaps else 0.0
    print(f"[1] transfer done: {n_pre}+ acks, errors={errors}, max ack gap={window*1000:.0f} ms", flush=True)
    assert window < 1.5, f"交接窗口超标：{window*1000:.0f}ms"

    # 交回 node0；收尾 20 条容忍元数据刷新期的 NotLeader（重试直至 ack）
    transfer(TOPIC, 0, 0)
    time.sleep(0.5)
    pending = list(range(i, i + 20))
    deadline = time.monotonic() + 20
    while pending and time.monotonic() < deadline:
        j = pending[0]
        try:
            producer.send(TOPIC, value=f"x-{j}".encode(), partition=0).get(timeout=2)
            pending.pop(0)
        except Exception:
            time.sleep(0.1)
    producer.close()
    assert not pending, f"{len(pending)} 条未完成收尾生产"
    total = i + 20

    # 全量读回：不丢不重
    consumer = KafkaConsumer(bootstrap_servers=B, auto_offset_reset="earliest")
    consumer.assign([TopicPartition(TOPIC, 0)])
    got = []
    deadline = time.time() + 20
    while time.time() < deadline:
        batch = consumer.poll(timeout_ms=1000)
        for recs in batch.values():
            got.extend(m.value.decode() for m in recs)
        if len(got) >= total:
            break
    consumer.close()
    dup = len(got) - len(set(got))
    lost = total - len(set(got))
    print(f"[2] read {len(got)} unique={len(set(got))}/{total} dup={dup} lost={lost}", flush=True)
    assert lost == 0 and dup == 0, f"丢失 {lost} / 重复 {dup}"
    print("PASS ✔ (计划内交接：流不中断，不丢不重)", flush=True)


if __name__ == "__main__":
    main()
