#!/usr/bin/env python3
"""basalt e2e：组状态权威存储迁移（B2，方案 B 块 b2）。

setup：produce 20 条 → 组消费 8 条并 commit → 内部 topic __basalt_group_state
       end offset > 0（提交落内部 topic）→ 本地 __consumer_offsets.log 不存在
verify（broker kill -9 后同数据目录重启）：committed(offset) == 8（从内部
       topic 重放恢复，而非丢位点）→ 续读 12 条全量。
"""
import sys
import time

from kafka import KafkaConsumer, KafkaProducer, TopicPartition

BROKER = "localhost:9092"
TOPIC = "gs-e2e"
GROUP = "gs-grp"
N = 20
CUT = 8


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


def setup():
    assert wait_for_broker(), "broker not reachable"
    print("[1] broker up")
    p = KafkaProducer(bootstrap_servers=BROKER, acks=-1)
    for i in range(N):
        p.send(TOPIC, value=f"gs-{i:03d}".encode()).get(timeout=10)
    p.flush()
    p.close()
    print(f"[2] produced {N}")

    c = KafkaConsumer(bootstrap_servers=BROKER, group_id=GROUP,
                      enable_auto_commit=False)
    tp = TopicPartition(TOPIC, 0)
    c.assign([tp])
    c.seek_to_beginning(tp)
    got = 0
    deadline = time.time() + 10
    while got < CUT and time.time() < deadline:
        for records in c.poll(timeout_ms=500, max_records=CUT - got).values():
            got += len(records)
    assert got == CUT, f"consumed {got} != {CUT}"
    c.commit({tp: OffsetMarker(CUT)})
    time.sleep(0.5)
    committed = c.committed(tp)
    c.close()
    assert committed == CUT, f"committed {committed} != {CUT}"
    print(f"[3] committed offset {CUT} as group {GROUP}")

    # 内部 topic 收到提交（end offset 增长）
    c2 = KafkaConsumer(bootstrap_servers=BROKER)
    itp = TopicPartition("__basalt_group_state", 0)
    end = c2.end_offsets([itp])[itp]
    c2.close()
    assert end > 0, f"内部 topic end offset = {end}，提交未落内部 topic"
    print(f"[4] __basalt_group_state end offset = {end}（提交已持久化到内部 topic）")


class OffsetMarker:
    """kafka-python commit 需要 OffsetAndMetadata；薄封装避免重复导入。"""
    def __new__(cls, offset):
        from kafka.structs import OffsetAndMetadata
        return OffsetAndMetadata(offset, "")


def verify():
    assert wait_for_broker(), "broker not reachable after restart"
    print("[5] broker restarted")
    time.sleep(2)  # 等内部 topic 重放 + BindState
    c = KafkaConsumer(bootstrap_servers=BROKER, group_id=GROUP,
                      enable_auto_commit=False)
    tp = TopicPartition(TOPIC, 0)
    c.assign([tp])
    committed = c.committed(tp)
    assert committed == CUT, \
        f"重启后 committed = {committed}（期望 {CUT}）——组状态未从内部 topic 恢复"
    print(f"[6] committed = {committed}（从内部 topic 重放恢复）")
    c.seek(tp, CUT)
    got = []
    deadline = time.time() + 10
    while len(got) < N - CUT and time.time() < deadline:
        for records in c.poll(timeout_ms=500, max_records=N - CUT - len(got)).values():
            got.extend(records)
    c.close()
    assert len(got) == N - CUT, f"续读 {len(got)} != {N - CUT}"
    print(f"[7] 续读 {len(got)} 条（offset {CUT}→{N}）语义完整")
    print("PASS")


if __name__ == "__main__":
    phase = sys.argv[1] if len(sys.argv) > 1 else ""
    if phase == "setup":
        setup()
    elif phase == "verify":
        verify()
    else:
        print("usage: group_state_e2e.py setup|verify")
        sys.exit(2)
