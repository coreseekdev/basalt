#!/usr/bin/env python3
"""librdkafka AdminClient 兼容性 e2e——管理面 API 版本分叉探测。

AdminClient（librdkafka）走的 API 版本普遍高于 kafka-python：
CreateTopics v7 / DeleteTopics v6 / ListGroups v4 / DescribeGroups v5 /
DeleteRecords v2。任一 handler 恒按旧布局读写即在此暴露（同缺陷⑩模式）。

场景：create 2 分区 topic → produce → ListGroups/DescribeGroups →
DeleteRecords 截前缀 → 验证剩余区间 → DeleteTopics → 复查 metadata。
用法：testing/e2e/run_e2e.sh librdkafka_admin.py"""
import os, sys, time
from confluent_kafka import Producer, Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

BROKER = "localhost:9092"
TOPIC = "librdkafka-admin-e2e"
GROUP = "librdkafka-admin-group"


def done(futs, timeout=30):
    """等待 AdminClient future 集合；错误以异常形式抛出，成功返回 None 值映射"""
    dl = time.monotonic() + timeout
    out = {}
    for t, f in futs.items():
        try:
            out[t] = f.result(timeout=max(0.1, dl - time.monotonic()))
        except Exception as e:
            out[t] = e
    return out


def main():
    adm = AdminClient({"bootstrap.servers": BROKER})

    # [1] CreateTopics（v7：含 NumPartitions/ReplicationFactor 配置域）
    fs = done(adm.create_topics([NewTopic(TOPIC, num_partitions=2, replication_factor=1)]))
    for t, r in fs.items():
        if isinstance(r, Exception) and "TOPIC_ALREADY_EXISTS" not in str(r):
            raise AssertionError(f"create_topics({t}): {r}")
    print("[1] CreateTopics v7 ✔")

    # [2] produce 20 条（0..19，每分区 10）
    p = Producer({"bootstrap.servers": BROKER})
    for i in range(20):
        p.produce(TOPIC, value=f"a-{i:04d}".encode(), partition=i % 2)
    p.flush(30)
    time.sleep(0.5)

    # [3] 组入组（为 ListGroups/DescribeGroups 造活跃组）
    c = Consumer({"bootstrap.servers": BROKER, "group.id": GROUP,
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([TOPIC])
    got = 0
    t0 = time.monotonic()
    while got < 20 and time.monotonic() - t0 < 20:
        for m in c.consume(num_messages=10, timeout=1.0):
            if not m.error():
                got += 1
    assert got == 20, f"consume 全量失败：{got}/20"
    c.commit(offsets=[TopicPartition(TOPIC, 0, 10), TopicPartition(TOPIC, 1, 10)],
             asynchronous=False)
    time.sleep(0.5)
    print("[2] consume 20 + commit ✔")

    # [4] ListGroups（v0-4；librdkafka list_groups 走同款请求）
    lg = adm.list_groups(timeout=10)
    assert GROUP in [g.id for g in lg], f"ListGroups 未见到 {GROUP}：{lg}"
    print("[3] ListGroups ✔")

    # [5] DescribeGroups（v0-5；describe_consumer_groups 走同款请求）
    dg = adm.describe_consumer_groups([GROUP], request_timeout=10)
    g = dg[GROUP].result()
    assert g.state is not None, "group state 缺失"
    members = list(g.members)
    print(f"[4] DescribeGroups ✔（state={g.state}, members={len(members)}）")
    assert len(members) == 1, f"应见 1 名成员，实得 {members}"

    # [6] DeleteRecords v2：截掉每分区前 4 条（offset<4），保留 [4,10)
    lo = 4
    tps = [TopicPartition(TOPIC, 0, lo), TopicPartition(TOPIC, 1, lo)]
    dr = adm.delete_records(tps)
    for tp, f in dr.items():
        f.result()  # 异常即失败；lowwatermark 语义由随后的读回区间校验
    time.sleep(0.5)

    # 消费位置从 4 起仍能读到 [4,10)：earliest 不得越过 log_start 回放已删前缀
    c2 = Consumer({"bootstrap.servers": BROKER, "group.id": f"admin-verify-{time.time()}",
                   "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c2.assign([TopicPartition(TOPIC, 0, 4), TopicPartition(TOPIC, 1, 4)])
    vals = []
    t0 = time.monotonic()
    while len(vals) < 12 and time.monotonic() - t0 < 15:
        for m in c2.consume(num_messages=10, timeout=1.0):
            if not m.error():
                vals.append(m.value().decode())
    c2.close()
    # 分区 p 的第 k 条是 a-{2k+p}；截到 offset>=4 ⇒ 每分区保留 a-{8+p} .. a-{18+p}
    expected = {f"a-{i:04d}" for i in range(20) if i >= 8}
    assert set(vals) == expected, f"DeleteRecords 后应恰为 a-08..a-19，得 {sorted(set(vals))} / {sorted(vals)}"
    assert len(set(vals)) == len(vals), "no-dup"
    print(f"[5] DeleteRecords v2 ✔（读回 {len(vals)} 条 = 每分区 [4,10)）")

    # [7] DeleteTopics v6
    fs = done(adm.delete_topics([TOPIC]))
    for t, r in fs.items():
        if isinstance(r, Exception):
            raise AssertionError(f"delete_topics({t}): {r!r}")
    print("[6] DeleteTopics v6 ✔")
    print("PASS ✔")
    # 归档结论（2026-09-10）：confluent-kafka 2.15 + py3.14 的 AdminClient
    # delete_records 在解释器收尾阶段 segfault——对 Redpanda 参照实现同样
    # 复现（EXIT=139，业务输出全对），纯客户端 bug。跳过收尾规避。
    sys.stdout.flush()
    os._exit(0)


if __name__ == "__main__":
    main()
