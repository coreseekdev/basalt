#!/usr/bin/env python3
"""basalt e2e：组协调器 failover（B3，方案 B 块 b3）。

phase1 setup：3 节点集群，produce 30 → node0 上组消费 10 条 commit(10)。
（shell 随后 kill -9 node0——组协调器 = 内部 topic p0 leader 随之消失）
phase2 verify：换 node1/2 重连——
  [a] committed == 10（新 leader 从副本重放组状态，位点不丢）
  [b] 续读 20 条语义完整
  [c] 新协调器上再 commit(30) 成功（下沉通道随 leader 重绑）
"""
import sys
import time

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

BROKERS = ["localhost:9092", "localhost:9102", "localhost:9112"]
TOPIC = "gf-e2e"
GROUP = "gf-grp"
N = 30
CUT = 10


def wait_broker(addr, timeout=30):
    from kafka.client_async import KafkaClient
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            c = KafkaClient(bootstrap_servers=addr)
            c.close()
            return True
        except Exception:
            time.sleep(0.5)
    return False


def mk(offset):
    from kafka.structs import OffsetAndMetadata
    return OffsetAndMetadata(offset, "")


def setup():
    for b in BROKERS:
        assert wait_broker(b), f"{b} not reachable"
    print("[1] 3 brokers up")
    admin = KafkaAdminClient(bootstrap_servers=BROKERS[0], client_id="gf-admin")
    try:
        admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=3)])
    except Exception:
        pass
    admin.close()
    time.sleep(1.5)

    p = KafkaProducer(bootstrap_servers=BROKERS[0], acks=-1)
    for i in range(N):
        p.send(TOPIC, value=f"gf-{i:03d}".encode()).get(timeout=10)
    p.flush()
    p.close()
    print(f"[2] produced {N}")

    c = KafkaConsumer(bootstrap_servers=BROKERS[0], group_id=GROUP,
                      enable_auto_commit=False)
    tp = TopicPartition(TOPIC, 0)
    c.assign([tp])
    c.seek_to_beginning(tp)
    got = []
    deadline = time.time() + 15
    while len(got) < CUT and time.time() < deadline:
        for records in c.poll(timeout_ms=500, max_records=CUT - len(got)).values():
            got.extend(records)
    assert len(got) == CUT, f"consumed {len(got)}"
    c.commit({tp: mk(CUT)})
    time.sleep(0.5)
    assert c.committed(tp) == CUT, "commit before failover failed"
    c.close()
    print(f"[3] committed {CUT} on node0 组协调器")


def verify():
    survivor = BROKERS[1]
    assert wait_broker(survivor), "no survivor reachable"
    print("[4] node0 killed; reconnect to", survivor)
    tp = TopicPartition(TOPIC, 0)
    committed = None
    deadline = time.time() + 60
    while committed != CUT and time.time() < deadline:
        try:
            c = KafkaConsumer(bootstrap_servers=survivor, group_id=GROUP,
                              enable_auto_commit=False)
            c.assign([tp])
            committed = c.committed(tp)
            if committed != CUT:
                print(f"  attempt: committed={committed}")
                c.close()
                time.sleep(2)
                continue
            # [b] 续读 20 条
            c.seek(tp, CUT)
            got = []
            deadline2 = time.time() + 15
            while len(got) < N - CUT and time.time() < deadline2:
                for records in c.poll(timeout_ms=500, max_records=N - CUT - len(got)).values():
                    got.extend(records)
            c.close()
            assert len(got) == N - CUT, f"续读 {len(got)} != {N - CUT}"
            for i, r in enumerate(got):
                assert r.value == f"gf-{CUT + i:03d}".encode(), f"msg {i} corrupt"
            print(f"[5] committed = {committed}（新协调器重放恢复）；续读 {len(got)} 条完整")
            # [c] 新协调器可提交
            c = KafkaConsumer(bootstrap_servers=survivor, group_id=GROUP,
                              enable_auto_commit=False)
            c.assign([tp])
            c.commit({tp: mk(N)})
            time.sleep(0.5)
            got2 = c.committed(tp)
            c.close()
            assert got2 == N, f"failover 后 commit 失败：{got2}"
            print(f"[6] failover 后 commit({N}) 成功（下沉通道随 leader 重绑）")
            print("PASS")
            return
        except AssertionError:
            raise
        except Exception as e:
            print(f"  retry: {type(e).__name__}: {e}")
            time.sleep(2)
    raise AssertionError(f"committed 未恢复（期望 {CUT}，最后值 {committed}）——组状态未随 failover 迁移")


if __name__ == "__main__":
    phase = sys.argv[1] if len(sys.argv) > 1 else ""
    if phase == "setup":
        setup()
    elif phase == "verify":
        verify()
    else:
        print("usage: group_failover_scenario.py setup|verify")
        sys.exit(2)
