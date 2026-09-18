# 分层存储 e2e（T-M4.3 块 c，ADR-21 §4）——tiered 模式数据面。
# 用法（由 run_tiered.sh 驱动，两阶段夹一次 broker 重启）：
#   python3 tiered_storage.py produce   # 产 120 条（小段触发滚动+分层回收）
#   python3 tiered_storage.py consume   # 从 0 全量消费（读穿透 + 本地尾段）
import sys
import tempfile

from confluent_kafka import Consumer, Producer, TopicPartition

BROKER = "localhost:9092"
TOPIC = "ts-e2e"
TOTAL = 400


def fail(msg):
    print(f"FAIL: {msg}")
    sys.exit(1)


def conf(extra=None):
    c = {"bootstrap.servers": BROKER, "socket.timeout.ms": 10000}
    if extra:
        c.update(extra)
    return c


def wait_leader(timeout=30):
    import time
    from confluent_kafka.admin import AdminClient
    adm = AdminClient(conf())
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            md = adm.list_topics(topic=TOPIC, timeout=5)
            if md.topics[TOPIC].partitions:
                return
        except Exception:
            pass
        time.sleep(0.5)
    fail("broker/topic 不可用")


def produce():
    wait_leader()
    p = Producer(conf({"enable.idempotence": True, "acks": "all"}))
    for i in range(TOTAL):
        p.produce(TOPIC, key=f"k{i % 4}".encode(), value=(f"t-{i:04d}-" + "x" * 96).encode())
        # 每 20 条强制一个独立批次——segment_max_bytes 的滚动按批粒度判定，
        # 单批超限只会落在空 active（首批允许超限）而永不滚动
        if (i + 1) % 20 == 0:
            p.flush(30)
        else:
            p.poll(0)
    p.flush(30)
    if len(p) != 0:
        fail(f"produce 未清零：{len(p)} 条在途")
    print(f"[tiered] produced {TOTAL}")


def consume():
    wait_leader()
    c = Consumer(conf({
        "group.id": "tiered-e2e",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }))
    md = c.list_topics(topic=TOPIC, timeout=10)
    parts = [TopicPartition(TOPIC, pid, 0) for pid in md.topics[TOPIC].partitions]
    c.assign(parts)
    got = {}
    import time
    deadline = time.time() + 60
    while len(got) < TOTAL and time.time() < deadline:
        msgs = c.consume(num_messages=50, timeout=1.0)
        for m in msgs:
            if m.error():
                continue
            got[m.value().decode()] = (m.partition(), m.offset())
    c.close()
    if len(got) != TOTAL:
        fail(f"读到 {len(got)}/{TOTAL}")
    for i in range(TOTAL):
        v = f"t-{i:04d}-" + "x" * 96
        if v not in got:
            fail(f"缺 {v}")
    parts_seen = {p for p, _ in got.values()}
    print(f"consume {len(got)}/{TOTAL} unique, partitions={sorted(parts_seen)}")


if __name__ == "__main__":
    if tempfile.gettempdir() == "":
        fail("tempdir")
    phase = sys.argv[1] if len(sys.argv) > 1 else ""
    if phase == "produce":
        produce()
    elif phase == "consume":
        consume()
    else:
        fail("用法：tiered_storage.py produce|consume")
