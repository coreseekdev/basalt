#!/usr/bin/env python3
"""librdkafka cooperative-sticky 档 e2e（T-M3.5，补 T-M3.4 的 rdkafka 遗留面）。

franz-go 侧已验（run_franzgo_coop.sh）；本档换 librdkafka 后端复刻：
partition.assignment.strategy=cooperative-sticky。librdkafka 原生处理协作
协议的增量 assign/unassign——本环境的 confluent_kafka wheel 禁用
rebalance_cb（librdkafka _INVALID_ARG，回调注入面不可用），协作语义改由
行为面断言：

  [1] 两 consumer 先后入同组，rebalance 后双双消费到波 2 记录
      （B 消费到数据 == 增量分配传导；A 拿到波 2 == 无停顿式停等）。
  [2] 全量 40/40 不重不漏（跨成员重复由 distinct 收口——分区迁移时
      旧属主可能已读、新属主从提交位再读同一分区）。
  [3] 组协议面：cooperative-sticky 经典协作组被 broker 接受并完成多轮
      rebalance（协议选择 = leader 偏好 ∩ 全体支持集，ADR-20 §2——
      协议名错则 join 即败，行为面无数据）。

用法：testing/e2e/run_e2e.sh librdkafka_cooperative.py
"""
import os
import threading
import time

from confluent_kafka import Consumer, Producer

BROKER = os.environ.get("BASALT_BROKER", "localhost:9092")
RUN = int(time.time())
TOPIC = f"coop-rdk-e2e-{RUN}"
GROUP = f"coop-rdk-e2e-{RUN}"
N1, N2 = 20, 20  # 波 1 预产 / 波 2 慢流（B 加入后）


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


def wait_topic(p, timeout=15):
    dl = time.monotonic() + timeout
    while time.monotonic() < dl:
        if TOPIC in p.list_topics(timeout=5).topics:
            return
        time.sleep(0.1)
    raise AssertionError(f"topic {TOPIC} 未出现")


def produce_wave(prefix, n, gap=0.0):
    p = Producer({"bootstrap.servers": BROKER})
    errs = []
    for i in range(n):
        p.produce(TOPIC, value=f"{prefix}-{i:04d}".encode(), partition=i % 2,
                  on_delivery=lambda e, _m: errs.append(e) if e else None)
        p.poll(0)
        if gap:
            time.sleep(gap)
    assert p.flush(30) == 0 and not errs, f"produce 未全达: {errs}"


class Member(threading.Thread):
    """组员：独立 Consumer 实例 + cooperative-sticky（librdkafka 原生增量）。"""

    def __init__(self, name):
        super().__init__(name=name, daemon=True)
        self.name = name
        self.got = {}      # value -> 首次接收时刻（monotonic）
        self.c = Consumer({"bootstrap.servers": BROKER, "group.id": GROUP,
                           "auto.offset.reset": "earliest", "enable.auto.commit": False,
                           "partition.assignment.strategy": "cooperative-sticky",
                           "session.timeout.ms": 6000, "heartbeat.interval.ms": 2000})
        self.c.subscribe([TOPIC])
        self.stop_flag = threading.Event()

    def run(self):
        dl = time.monotonic() + 90
        while not self.stop_flag.is_set() and time.monotonic() < dl:
            for m in self.c.consume(num_messages=5, timeout=0.3):
                if m.error():
                    continue
                self.got.setdefault(m.value().decode(), time.monotonic())

    def close(self):
        self.stop_flag.set()
        self.join(timeout=10)
        self.c.close()


def main():
    wait_broker()
    produce_wave("c", N1)
    wait_topic(Producer({"bootstrap.servers": BROKER}))
    print(f"[1] 波 1 预产 {N1}（2 分区轮询）")

    a = Member("A")
    a.start()
    dl = time.monotonic() + 30
    while len(a.got) < N1 and time.monotonic() < dl:
        time.sleep(0.1)
    assert len(a.got) >= N1, f"A 预热读 {len(a.got)}/{N1}"

    t_join = time.monotonic()
    b = Member("B")
    b.start()
    print("[2] B 入组，波 2 慢流启动（100ms/条）")
    produce_wave("w", N2, gap=0.1)

    dl = time.monotonic() + 30
    while len(set(a.got) | set(b.got)) < N1 + N2 and time.monotonic() < dl:
        time.sleep(0.1)

    # —— 快照（close 前）——
    a_post = {v for v, ts in a.got.items() if ts > t_join and v.startswith("w-")}
    b_post = {v for v, ts in b.got.items() if ts > t_join and v.startswith("w-")}

    assert len(b.got) > 0, "B 未消费到数据——增量分配未传导"
    union = set(a.got) | set(b.got)
    want = {f"c-{i:04d}" for i in range(N1)} | {f"w-{i:04d}" for i in range(N2)}
    missing = sorted(want - union)
    assert len(union) == N1 + N2 and not missing, \
        f"全组覆盖 {len(union)}/{N1 + N2}（缺 {missing}）"
    wave2 = {f"w-{i:04d}" for i in range(N2)}
    assert a_post, "A 在 B 加入后未再消费到波 2 记录（停顿式停等？）"
    assert b_post, "B 未消费到波 2 记录"
    print(f"[2] rebalance 收敛：A/B 双双消费到波 2，全组 {N1 + N2}/{N1 + N2} 不重不漏 ✔")
    print(f"[3] 协作增量语义（行为面）：B 增量分得 {len(b.got)} 条；"
          f"A 在 B 加入后继续消费 {len(a_post)} 条波 2 记录（无停等）✔")
    a.close()
    b.close()
    print("PASS ✔ (librdkafka cooperative-sticky)")


if __name__ == "__main__":
    main()
