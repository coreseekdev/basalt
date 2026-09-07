#!/usr/bin/env python3
"""多节点 failover e2e：
  3 broker 集群；acks=all 生产；kill -9 leader；验证 failover 后不丢已确认数据。
用法：先由 run_multinode.sh 拉起 3 个 broker，再运行本脚本（BROKERS 已注入环境）。"""
import os, sys, time
from kafka import KafkaProducer, KafkaConsumer
from kafka.structs import TopicPartition

BROKERS = os.environ.get("BROKERS", "localhost:9092,localhost:9102,localhost:9112")
TOPIC = "mn-e2e"
N1 = 50

def main():
    producer = KafkaProducer(bootstrap_servers=BROKERS, acks=-1, retries=5,
                             request_timeout_ms=15000, max_block_ms=15000)
    producer.partitions_for(TOPIC)
    time.sleep(1)
    assert set(producer.partitions_for(TOPIC)) == {0, 1}, "2 partitions expected"

    # 阶段 1：failover 前生产
    print("[0.5] sending...", flush=True)
    for i in range(N1):
        producer.send(TOPIC, value=f"pre-{i:04d}".encode(), partition=i % 2)
    print("[0.6] flushing...", flush=True)
    producer.flush(timeout=30)
    print(f"[1] produced {N1} (acks=all)", flush=True)

    def read_all(label, expect_min):
        c = KafkaConsumer(bootstrap_servers=BROKERS, auto_offset_reset="earliest",
                          enable_auto_commit=False, consumer_timeout_ms=8000)
        tps = [TopicPartition(TOPIC, 0), TopicPartition(TOPIC, 1)]
        c.assign(tps)
        got = []
        deadline = time.time() + 15
        while len(got) < expect_min and time.time() < deadline:
            for recs in c.poll(timeout_ms=1000).values():
                for m in recs:
                    got.append(m.value.decode())
        c.close()
        print(f"  {label}: {len(got)}")
        return got

    got1 = read_all("phase1", N1)
    assert len(set(got1)) == N1, f"phase1 lost: {N1 - len(set(got1))}"
    print("[2] pre-failover data complete")

    # 阶段 2：找到 p0 的 leader 并 kill -9 该 broker 进程
    import socket, struct
    def raw_metadata(bootstrap, topic):
        cid = b"failover-probe"
        name = topic.encode()
        body = b"\x00\x03" + (1).to_bytes(2, "big") + (7).to_bytes(4, "big") + len(cid).to_bytes(2, "big") + cid
        body += b"\x00\x00\x00\x01" + len(name).to_bytes(2, "big") + name
        s = socket.create_connection(bootstrap, timeout=3)
        s.sendall(len(body).to_bytes(4, "big") + body)
        buf = b""
        while len(buf) < 4:
            buf += s.recv(4 - len(buf))
        n = int.from_bytes(buf, "big")
        while len(buf) < 4 + n:
            buf += s.recv(4 + n - len(buf))
        s.close()
        return buf[4 + 8:]  # 去帧长 + 响应头 corr(4)+brokers 段起点未知——整体解析见下

    # 简化解析：从响应中抓 leader_id=0 的分区与其 host:port 关联——
    # 直接复用 kafka-python 的解析器
    from kafka.protocol.metadata import MetadataResponse
    from io import BytesIO
    resp_bytes = raw_metadata(("localhost", 9092), TOPIC)
    # 重新完整取（上面截了头）
    def full_metadata(bootstrap, topic):
        cid = 7
        name = topic.encode()
        cid_s = b"failover-probe"
        body = b"\x00\x03" + (1).to_bytes(2, "big") + cid.to_bytes(4, "big") + len(cid_s).to_bytes(2, "big") + cid_s
        body += b"\x00\x00\x00\x01" + len(name).to_bytes(2, "big") + name
        s = socket.create_connection(bootstrap, timeout=3)
        s.sendall(len(body).to_bytes(4, "big") + body)
        buf = b""
        while len(buf) < 4:
            buf += s.recv(4 - len(buf))
        n = int.from_bytes(buf[:4], "big")
        while len(buf) < 4 + n:
            buf += s.recv(4 + n - len(buf))
        s.close()
        return MetadataResponse[1].decode(BytesIO(buf[4+4:]))
    md = full_metadata(("localhost", 9092), TOPIC)
    # kafka-python 解码为位置元组：brokers=[(node_id, host, port, rack)]
    brokers = {b[0]: (b[1], b[2]) for b in md.brokers}
    # topics=[(error, topic, internal, partitions=[...])]；partitions=[(err, idx, leader, replicas, isr)]
    p0_topic = [t for t in md.topics if t[1] == TOPIC][0]
    leader_id = p0_topic[3][0][2]
    leader_host_port = brokers[leader_id]
    print(f"[3] p0 leader node={leader_id} at {leader_host_port}")
    assert leader_host_port, "leader not found"

    pid_dir = os.environ.get("PID_DIR", "/tmp/basalt-mn")
    pid_file = os.path.join(pid_dir, f"port-{leader_host_port[1]}")
    pid = int(open(pid_file).read().strip())
    os.kill(pid, 9)
    print(f"[4] killed leader broker pid={pid} (SIGKILL)")

    # 阶段 3：failover 后继续生产（客户端自动切新 leader）
    time.sleep(5)  # 等 heartbeat timeout(4s) + failover + 客户端 metadata 刷新
    sent = 0
    try:
        for i in range(N1, N1 + 30):
            producer.send(TOPIC, value=f"post-{i:04d}".encode(), partition=i % 2)
            sent += 1
        producer.flush()
    except Exception as e:
        print(f"  post-failover partial send: {e}")
    print(f"[5] post-failover sent {sent} (acks=all)")

    got2 = read_all("phase2", N1 + sent)
    uniq = set(got2)
    pre = {f"pre-{i:04d}" for i in range(N1)}
    post = {f"post-{i:04d}" for i in range(N1, N1 + sent)}
    lost_pre = pre - uniq
    lost_post = post - uniq
    assert not lost_pre, f"LOST acked pre-failover: {sorted(lost_pre)[:5]}"
    assert not lost_post, f"LOST post-failover: {sorted(lost_post)[:5]}"
    print("[6] invariants: acked data survived failover ✔")
    print("PASS ✔")

if __name__ == "__main__":
    main()
