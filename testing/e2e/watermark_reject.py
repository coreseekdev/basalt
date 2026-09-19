#!/usr/bin/env python3
"""basalt e2e：磁盘前置水位（A2）。

用法: watermark_reject.py reject|accept
  reject — 水位 1%（任何真实磁盘必越过）+ sweep 500ms：produce 必须被拒
  accept — 默认水位 95%（健康磁盘）：produce 正常放行（默认档不误伤）
"""
import sys
import time

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

BROKER = "localhost:9092"
TOPIC = "e2e-watermark"


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


def run(expect_reject: bool):
    assert wait_for_broker(), "broker not reachable"
    tag = "reject" if expect_reject else "accept"
    print(f"[1] broker reachable (expect {tag})")

    admin = KafkaAdminClient(bootstrap_servers=BROKER, client_id="e2e-watermark-admin")
    try:
        admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1)])
    except Exception:
        pass
    admin.close()
    time.sleep(1)

    producer = KafkaProducer(bootstrap_servers=BROKER, client_id="e2e-watermark-producer",
                             acks=1, retries=0, max_block_ms=10000)
    err = None
    try:
        producer.send(TOPIC, value=b"wm").get(timeout=10)
    except Exception as e:
        err = e
    producer.close()

    if expect_reject:
        assert err is not None, "produce 越过水位后仍成功——水位闸未生效"
        print(f"[2] produce rejected as expected: {type(err).__name__}: {err}")
    else:
        assert err is None, f"健康磁盘 produce 失败（水位误伤）: {err}"
        print("[2] produce accepted as expected (default watermark 95)")
    print("PASS")


if __name__ == "__main__":
    phase = sys.argv[1] if len(sys.argv) > 1 else ""
    if phase == "reject":
        run(expect_reject=True)
    elif phase == "accept":
        run(expect_reject=False)
    else:
        print("usage: watermark_reject.py reject|accept")
        sys.exit(2)
