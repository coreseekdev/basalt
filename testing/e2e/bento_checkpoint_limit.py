#!/usr/bin/env python3
"""Bento 式 checkpoint_limit 背压模板 e2e（T-M3.5）。

Bento processor 语义映射：source 维护「未 checkpoint 记录数上限」
（checkpoint_limit）：达到上限即停止拉取输入（背压），仅当 checkpoint
完成才放行下一窗口。本 e2e 复刻：enable.auto.commit=false + 未提交窗口
上限 LIMIT=5——窗口满 → consumer.pause() 暂停全部分区（停止拉取）→
同步 commit（checkpoint）→ resume()。生产端突发 60 条。

断言：
  [1] 全程观察到的「未提交窗口」≤ 5。±在途容差说明：consume 的
      num_messages 参数按剩余窗口截流（LIMIT-window），窗口计数只含
      已交付到应用的记录；librdkafka 本地预取队列中的在途消息不进入
      观察窗口（与 Bento 的 processor 边界一致），故窗口上限是精确断言
      而非近似。为压小在途面，conf 收紧 queued.min.messages。
  [2] 60/60 全消费，值集合精确匹配——无重无漏；resume 放行若失效即
      停等至超时红。
  [3] checkpoint 单调：每分区提交位点严格递增；终态提交位 == 末位
      （= 30/30 per 分区）。
用法：testing/e2e/run_e2e.sh bento_checkpoint_limit.py
"""
import os
import time

from confluent_kafka import Consumer, Producer, TopicPartition

BROKER = os.environ.get("BASALT_BROKER", "localhost:9092")
RUN = int(time.time())
TOPIC = f"bento-cl-e2e-{RUN}"
LIMIT, TOTAL = 5, 60


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


def main():
    wait_broker()
    # 生产端突发 60 条（2 分区轮询）
    p = Producer({"bootstrap.servers": BROKER})
    errs = []
    for i in range(TOTAL):
        p.produce(TOPIC, value=f"bento-{i:04d}".encode(), partition=i % 2,
                  on_delivery=lambda e, _m: errs.append(e) if e else None)
        p.poll(0)
    assert p.flush(30) == 0 and not errs, f"produce 未全达: {errs}"
    wait_topic(p)
    print(f"[1] burst produced {TOTAL}")

    c = Consumer({"bootstrap.servers": BROKER, "group.id": f"bento-cl-{RUN}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False,
                  "session.timeout.ms": 6000,
                  # 收紧预取，让 pause 的「停止拉取」语义可见（压小在途面）
                  "queued.min.messages": 1, "fetch.wait.max.ms": 10})
    c.subscribe([TOPIC])

    window = 0            # 未提交窗口（已交付未 checkpoint）
    max_window = 0
    pending = {}          # p -> next offset（本次窗口待提交位点）
    seen = set()
    total = 0
    pauses = 0
    history = {0: [], 1: []}

    def checkpoint():
        """barrier：同步提交全窗口位点（Bento 的 checkpoint）。"""
        nonlocal window
        offs = [TopicPartition(TOPIC, part, o) for part, o in sorted(pending.items())]
        c.commit(offsets=offs, asynchronous=False)
        for part, o in pending.items():
            history[part].append(o)
        pending.clear()
        window = 0

    dl = time.monotonic() + 60
    while total < TOTAL and time.monotonic() < dl:
        room = max(1, LIMIT - window)  # 拉取量按剩余窗口截流
        msgs = c.consume(num_messages=room, timeout=1.0)
        for m in msgs:
            if m.error():
                continue
            seen.add(m.value().decode())
            pending[m.partition()] = m.offset() + 1
            window += 1
            total += 1
            max_window = max(max_window, window)
        if window >= LIMIT:
            asg = c.assignment()
            if asg:  # 背压：窗口满即停止拉取（有交付必有分配，asg 必非空）
                c.pause(asg)
                pauses += 1
            checkpoint()  # checkpoint 完成才放行
            if asg:
                c.resume(asg)

    if window > 0:
        checkpoint()
    assert total == TOTAL, f"应全消费 {TOTAL}，得 {total}（resume 放行失效？）"
    assert len(seen) == TOTAL, f"无重：distinct {len(seen)}/{TOTAL}"
    assert seen == {f"bento-{i:04d}" for i in range(TOTAL)}, "值集合精确覆盖"
    assert max_window <= LIMIT, f"未提交窗口峰值 {max_window} 必须 ≤ {LIMIT}"
    assert pauses >= TOTAL // LIMIT, f"背压事件应 ≥ {TOTAL // LIMIT} 次，得 {pauses}"
    for part, hs in history.items():
        assert hs == sorted(hs) and len(set(hs)) == len(hs), f"p{part} 提交位点必须严格递增"
    committed = {tp.partition: tp.offset
                 for tp in c.committed([TopicPartition(TOPIC, p) for p in range(2)], timeout=10)
                 if tp.offset >= 0}
    c.close()
    assert committed == {0: TOTAL // 2, 1: TOTAL // 2}, \
        f"终态提交位应 30/30，得 {committed}"
    print(f"[2] 窗口峰值 {max_window}/{LIMIT}（{pauses} 次 pause→checkpoint→resume），60/60 不重不漏 ✔")
    print(f"[3] 提交位点严格递增，终态 {committed} ✔")
    print("PASS ✔ (Bento 式 checkpoint_limit 背压)")


if __name__ == "__main__":
    main()
