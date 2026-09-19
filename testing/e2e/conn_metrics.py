#!/usr/bin/env python3
"""basalt e2e：连接级指标（A4）——活跃连接 gauge（非累计）+ 每连接字节累计。

流量后保持一条消费者连接不断开，抓取 /metrics：
  basalt_connections_active >= 1
  basalt_connection_bytes_in_total{peer=...} > 0
  basalt_connection_bytes_out_total{peer=...} > 0
"""
import re
import time
import urllib.request

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

BROKER = "localhost:9092"
METRICS = "http://localhost:9094/metrics"
TOPIC = "e2e-conn-metrics"
N = 50


def wait_for_broker(timeout=15):
    from kafka.client_async import KafkaClient
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            c = KafkaClient(bootstrap_servers=BROKER)
            c.close()
            return True
        except Exception:
            time.sleep(0.5)
    return False


def scrape():
    with urllib.request.urlopen(METRICS, timeout=5) as r:
        return r.read().decode()


def main():
    assert wait_for_broker(), "broker not reachable"
    print("[1] broker reachable")
    admin = KafkaAdminClient(bootstrap_servers=BROKER, client_id="e2e-conn-admin")
    try:
        admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1)])
    except Exception:
        pass
    admin.close()
    time.sleep(1)

    producer = KafkaProducer(bootstrap_servers=BROKER, client_id="e2e-conn-producer",
                             acks=-1, retries=3, max_block_ms=10000)
    for i in range(N):
        producer.send(TOPIC, value=f"cm-{i:04d}".encode().ljust(100, b"x")).get(timeout=10)
    producer.flush()
    producer.close()
    print(f"[2] produced {N}")

    # 保持连接存活：手动 assign + 不 close
    consumer = KafkaConsumer(bootstrap_servers=BROKER, client_id="e2e-conn-consumer",
                             auto_offset_reset="earliest", enable_auto_commit=False)
    tp = TopicPartition(TOPIC, 0)
    consumer.assign([tp])
    consumer.seek_to_beginning(tp)
    got = []
    deadline = time.time() + 10
    while len(got) < N and time.time() < deadline:
        for records in consumer.poll(timeout_ms=500, max_records=N - len(got)).values():
            got.extend(records)
    assert len(got) == N, f"consumed {len(got)} != {N}"
    print(f"[3] consumed {N}（连接保持打开）")
    time.sleep(0.5)

    body = scrape()
    m_active = re.search(r"^basalt_connections_active (\d+)$", body, re.M)
    assert m_active, "metrics 缺 basalt_connections_active"
    active = int(m_active.group(1))
    assert active >= 1, f"活跃连接 gauge = {active}，应 ≥ 1"

    in_lines = {m.group(1): int(m.group(2)) for m in
                re.finditer(r'^basalt_connection_bytes_in_total\{peer="([^"]+)"\} (\d+)$', body, re.M)}
    out_lines = {m.group(1): int(m.group(2)) for m in
                 re.finditer(r'^basalt_connection_bytes_out_total\{peer="([^"]+)"\} (\d+)$', body, re.M)}
    assert in_lines, "无每连接入向字节指标"
    assert out_lines, "无每连接出向字节指标"
    live_in = sum(v for v in in_lines.values())
    live_out = sum(v for v in out_lines.values())
    assert live_in > 0 and live_out > 0, f"存活连接字节为 0: in={in_lines} out={out_lines}"

    consumer.close()
    time.sleep(0.5)
    body2 = scrape()
    m_active2 = re.search(r"^basalt_connections_active (\d+)$", body2, re.M)
    active2 = int(m_active2.group(1))
    assert active2 < active or active2 == 0, f"close 后活跃连接未回落: {active} → {active2}"
    in_after = len(re.findall(r"^basalt_connection_bytes_in_total", body2, re.M))
    assert in_after == 0, f"close 后每连接条目未清理（残留 {in_after}）"
    total = re.search(r"^basalt_connections_total (\d+)$", body2, re.M)
    assert total and int(total.group(1)) > 0, "累计计数器不应随连接清理"
    print(f"[4] active {active}→{active2}，每连接条目已清理，累计计数保留")
    print("PASS")


if __name__ == "__main__":
    main()
