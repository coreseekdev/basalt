#!/usr/bin/env python3
"""Vector 式 drain-then-commit 模板 e2e（T-M3.5）。

Vector sink 语义映射：enable.auto.commit=false，worker 循环
「poll 一批 → 处理（sink flush，本地文件为落点）→ 之后才手动提交 offset」
——先落盘后提交。两段会话模拟崩溃恢复：
  [1] 产 40 条 → 会话 1 逐批 drain-commit 读到 20 → 崩溃：再 poll 一批
      sink 已写、offset 未提交即弃世（不 commit 不 close——优雅关闭会
      触发最终提交，模拟不了 kill -9）。
  [2] 会话 2 同组从提交位续读 → sink 以记录值本身做幂等键去重后
      40/40 零丢失；raw 写入数 > 去重数证明 at-least-once 崩溃路径
      真被触发（允许会话边界重复，由幂等键折叠）。
  [3] 事务子场景：drain-commit 与产出端事务联动——
      send_offsets_to_transaction 把消费位点并入产出事务
      （TxnOffsetCommit 面），read_committed 只读 commit 流。

兼容性预期注释：send_offsets_to_transaction 携带 consumer_group_metadata()
的不透明字节，broker 的 TxnOffsetCommit 需解析该 ConsumerGroupMetadata
结构。broker 已支持 TxnOffsetCommit（T-M3.2）；若联调红优先核该结构解析，
不应为此简化断言。
用法：testing/e2e/run_e2e.sh vector_drain_commit.py
"""
import os
import shutil
import tempfile
import time

from confluent_kafka import Consumer, Producer, TopicPartition

BROKER = os.environ.get("BASALT_BROKER", "localhost:9092")
RUN = int(time.time())
TOPIC = f"vector-drain-e2e-{RUN}"
TXN_TOPIC = f"vector-drain-txne2e-{RUN}"  # 事务面独立题面（防主面 40 条混入隔离断言）
GROUP = f"vector-drain-e2e-{RUN}"  # 两会话必须同组才能从提交位续读
TXN_ID = f"vector-drain-txn-{RUN}"
TOTAL, SPLIT = 40, 20

SINK_DIR = tempfile.mkdtemp(prefix="vector-sink-")  # sink 落点（tempfile，不硬编码路径）
SINK = os.path.join(SINK_DIR, "sink.jsonl")


def wait_broker(timeout=30):
    dl, last = time.monotonic() + timeout, None
    while time.monotonic() < dl:
        try:
            if Producer({"bootstrap.servers": BROKER}).list_topics(timeout=3).brokers:
                return
        except Exception as e:
            last = e
        time.sleep(0.5)
    raise AssertionError(f"broker {BROKER} 不可达: {last}")


def wait_topic(p, topic=TOPIC, timeout=15):
    dl = time.monotonic() + timeout
    while time.monotonic() < dl:
        if topic in p.list_topics(timeout=5).topics:
            return
        time.sleep(0.1)
    raise AssertionError(f"topic {topic} 未出现")


# ---- sink（幂等键 = 记录值本身） ----

def sink_write(values):
    with open(SINK, "a") as f:
        for v in values:
            f.write(v + "\n")


def sink_unique():
    if not os.path.exists(SINK):
        return set()
    with open(SINK) as f:
        return {ln.strip() for ln in f if ln.strip()}


def sink_raw():
    if not os.path.exists(SINK):
        return []
    with open(SINK) as f:
        return sum(1 for ln in f if ln.strip())


# ---- Vector 式 worker ----

def make_consumer(group=GROUP, isolation=None):
    conf = {"bootstrap.servers": BROKER, "group.id": group,
            "auto.offset.reset": "earliest", "enable.auto.commit": False,
            "enable.auto.offset.store": False, "session.timeout.ms": 6000}
    if isolation:
        conf["isolation.level"] = isolation
    return Consumer(conf)


def drain_round(c, num=10):
    """poll 一批 → sink flush（① 处理）→ 返回批次；提交由调用方决定（② 后提交）。"""
    msgs = [m for m in c.consume(num_messages=num, timeout=1.0) if not m.error()]
    if msgs:
        sink_write([m.value().decode() for m in msgs])
    return msgs


def commit_batch(c, msgs):
    c.commit(offsets=[TopicPartition(TOPIC, m.partition(), m.offset() + 1) for m in msgs],
             asynchronous=False)


def session1():
    """读到 SPLIT 条即止（逐批 drain-commit），随后一批 sink 已写不提交即崩溃。"""
    c = make_consumer()
    c.subscribe([TOPIC])
    dl = time.monotonic() + 40
    while len(sink_unique()) < SPLIT and time.monotonic() < dl:
        msgs = drain_round(c)
        if msgs:
            commit_batch(c, msgs)
    tps = [TopicPartition(TOPIC, p) for p in range(2)]
    committed = {tp.partition: tp.offset for tp in c.committed(tps, timeout=10) if tp.offset >= 0}
    crash_batch = []
    cdl = time.monotonic() + 10
    while not crash_batch and time.monotonic() < cdl:
        crash_batch = drain_round(c)  # ① 已处理（sink 已写）
    assert crash_batch, "崩溃批未取到数据（会话 1 提前读空？）"
    # ② 未提交——kill -9：不 commit、不 close，句柄随进程退出回收
    print(f"  会话 1: drain-commit {committed}，崩溃批 sink 已写 {len(crash_batch)} 条未提交")
    return committed


def session2(timeout=40):
    """同组续读：从提交位重放崩溃批 + 读剩余，逐批 drain-commit 至全量收敛。"""
    c = make_consumer()
    c.subscribe([TOPIC])
    tps = [TopicPartition(TOPIC, p) for p in range(2)]
    pre = {tp.partition: tp.offset for tp in c.committed(tps, timeout=10) if tp.offset >= 0}
    print(f"  会话 2: 从提交位续读 {pre}")
    dl, stable = time.monotonic() + timeout, 0
    while time.monotonic() < dl:
        msgs = drain_round(c)
        if msgs:
            commit_batch(c, msgs)
            stable = 0
        elif len(sink_unique()) >= TOTAL:
            stable += 1
            if stable >= 3:
                break
    final = {tp.partition: tp.offset for tp in c.committed(tps, timeout=10) if tp.offset >= 0}
    c.close()
    return final


def main():
    wait_broker()
    p = Producer({"bootstrap.servers": BROKER})
    errs = []
    for i in range(TOTAL):
        p.produce(TOPIC, value=f"v-{i:04d}".encode(), partition=i % 2,
                  on_delivery=lambda e, _m: errs.append(e) if e else None)
    assert p.flush(30) == 0 and not errs, f"produce 未全达: {errs}"
    wait_topic(p)
    print(f"[1] produced {TOTAL}")

    boundary = session1()
    assert sum(boundary.values()) == SPLIT, f"会话 1 提交位应 {SPLIT}，得 {boundary}"
    uniq_crash = len(sink_unique())
    assert uniq_crash == SPLIT + 10, f"崩溃批应 sink 已写 10 条（共 {SPLIT + 10}），得 {uniq_crash}"
    print(f"[1] 会话 1：提交 {SPLIT} / sink 已写 {uniq_crash}（差值即未提交在途——崩溃面就位）")

    final = session2()
    uniq, raw = sink_unique(), sink_raw()
    assert uniq == {f"v-{i:04d}" for i in range(TOTAL)}, \
        f"sink 去重后必须 40/40 精确（零丢失零幻影），得 {len(uniq)}"
    assert raw > len(uniq), f"raw={raw} 应 > 去重 {len(uniq)}——崩溃批重放必须产生真实重复"
    assert sum(final.values()) == TOTAL, f"终态提交位应 {TOTAL}，得 {final}"
    print(f"[2] 会话 2 续读收敛：sink 去重 {len(uniq)}/{TOTAL} 零丢失，raw={raw}（at-least-once 折叠）✔")
    shutil.rmtree(SINK_DIR, ignore_errors=True)

    # ---- [3] 事务联动：消费位点并入产出事务 ----
    tp = Producer({"bootstrap.servers": BROKER, "transactional.id": TXN_ID,
                   "transaction.timeout.ms": 30000})
    tp.init_transactions(30)
    terrs = []

    def _on_delivery(e, _m):
        if e:
            terrs.append(e)

    tp.begin_transaction()
    for i in range(10):
        tp.produce(TXN_TOPIC, value=f"c-{i:04d}".encode(), partition=i % 2, on_delivery=_on_delivery)
    assert tp.flush(30) == 0
    tp.commit_transaction(30)
    tp.begin_transaction()
    for i in range(10):
        tp.produce(TXN_TOPIC, value=f"x-{i:04d}".encode(), partition=i % 2, on_delivery=_on_delivery)
    assert tp.flush(30) == 0
    tp.abort_transaction(30)
    assert not terrs, f"事务流 delivery errors: {terrs}"
    wait_topic(p, TXN_TOPIC)

    c3 = make_consumer(group=f"vector-txn-{RUN}", isolation="read_committed")
    c3.subscribe([TXN_TOPIC])
    got, pos = [], {}
    dl = time.monotonic() + 35
    while len(set(got)) < 10 and time.monotonic() < dl:
        for m in c3.consume(num_messages=10, timeout=1.0):
            if m.error():
                continue
            got.append(m.value().decode())
            pos[m.partition()] = m.offset() + 1
    assert set(got) == {f"c-{i:04d}" for i in range(10)}, \
        f"read_committed 只可见 commit 流，读到 {sorted(set(got))}"
    assert len(pos) == 2, f"双分区位点齐备，得 {pos}"
    tp.begin_transaction()
    tp.send_offsets_to_transaction(  # drain-commit 与事务提交联动（TxnOffsetCommit）
        [TopicPartition(TXN_TOPIC, p, o) for p, o in sorted(pos.items())],
        c3.consumer_group_metadata(), 30)
    tp.commit_transaction(30)
    tps = [TopicPartition(TXN_TOPIC, p) for p in range(2)]
    committed = {t.partition: t.offset for t in c3.committed(tps, timeout=10) if t.offset >= 0}
    c3.close()
    assert sum(committed.values()) == 10, f"事务化提交位点应合计 10，得 {committed}"
    print(f"[3] send_offsets_to_transaction → commit：位点 {committed} 事务性生效 ✔")
    print("PASS ✔ (Vector 式 drain-then-commit + 事务联动)")


if __name__ == "__main__":
    main()
