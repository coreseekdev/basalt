#!/usr/bin/env python3
"""basalt e2e：per-topic retention 配置透传。

broker 无 BASALT_LOG_RETENTION_MS（默认 7 天）：
  A 题 pt-ret   带 retention.ms=1500 → 过期删除，beginning 前移
  B 题 pt-keep  无配置              → 保持（per-topic 优先、默认不误伤）
  DescribeConfigs 回显 A 的 retention.ms=1500
"""
import sys
import time

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType, NewTopic

BROKER = "localhost:9092"
TOPIC_A = "pt-ret"
TOPIC_B = "pt-keep"
N = 50


def wait_for_broker(timeout=20):
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


def produce(topic, n):
    # linger + 大 batch → 整批单段（老段整段过期）
    p = KafkaProducer(bootstrap_servers=BROKER, acks=-1, linger_ms=1000,
                      batch_size=1_000_000)
    for i in range(n):
        p.send(topic, value=f"{topic}-{i:03d}".encode().ljust(200, b"x"))
    p.flush()
    p.close()


def setup():
    assert wait_for_broker(), "broker not reachable"
    print("[1] broker up")
    admin = KafkaAdminClient(bootstrap_servers=BROKER, client_id="pt-admin")
    admin.create_topics([
        NewTopic(TOPIC_A, num_partitions=1, replication_factor=1,
                 topic_configs={"retention.ms": "1500"}),
        NewTopic(TOPIC_B, num_partitions=1, replication_factor=1),
    ])
    admin.close()
    print("[2] topics created (A 带 retention.ms=1500)")
    time.sleep(1)

    produce(TOPIC_A, N)
    produce(TOPIC_B, N)
    print(f"[3] produced {N}+{N}（active 段）")


def seal():
    # active 段永不删除（retention 只作用于 sealed）——各补 1 条 fresh
    # 触发超限滚动，把老数据封存为 sealed 段
    produce(TOPIC_A, 1)
    produce(TOPIC_B, 1)
    print("[4] sealed old segments（fresh 触发滚动）")


def verify():
    consumer = KafkaConsumer(bootstrap_servers=BROKER, auto_offset_reset="earliest")
    tps = [TopicPartition(TOPIC_A, 0), TopicPartition(TOPIC_B, 0)]
    beginning = consumer.beginning_offsets(tps)
    end = consumer.end_offsets(tps)
    ba, bb = beginning[tps[0]], beginning[tps[1]]
    ea, eb = end[tps[0]], end[tps[1]]
    consumer.close()
    print(f"[5] A: beginning={ba} end={ea}；B: beginning={bb} end={eb}")
    assert ba == N, f"A 老段未过期删除（beginning={ba}，应 = {N}）"
    assert ea == N + 1, f"A end 异常：{ea}"
    assert bb == 0, f"B 无配置却被删（beginning={bb}）——默认档被误伤"
    assert eb == N + 1, f"B end 异常：{eb}"

    admin = KafkaAdminClient(bootstrap_servers=BROKER, client_id="pt-admin2")
    resp = admin.describe_configs(
        [ConfigResource(ConfigResourceType.TOPIC, TOPIC_A)])[0]
    admin.close()
    val = None
    for (_ec, _em, _rt, _rn, cfgs) in resp.resources:
        for entry in cfgs:
            if isinstance(entry, tuple) and entry[0] == "retention.ms":
                val = entry[1] if len(entry) > 1 else None
    assert val == "1500", f"DescribeConfigs retention.ms = {val!r}，应 = '1500'"
    print(f"[6] DescribeConfigs retention.ms = {val} ✓")
    print("PASS")


if __name__ == "__main__":
    phase = sys.argv[1] if len(sys.argv) > 1 else ""
    if phase == "setup":
        setup()
    elif phase == "seal":
        seal()
    elif phase == "verify":
        verify()
    else:
        print("usage: retention_pertopic.py setup|verify")
        sys.exit(2)
