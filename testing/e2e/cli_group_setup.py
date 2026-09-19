#!/usr/bin/env python3
"""basalt-cli e2e 辅助：建 topic + 组消费提交 offset，供 CLI groups 面断言。"""
import sys
import time

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

port = sys.argv[1] if len(sys.argv) > 1 else "9092"
boot = f"localhost:{port}"

admin = KafkaAdminClient(bootstrap_servers=boot, client_id="cli-e2e-setup")
try:
    admin.create_topics([NewTopic("lag-e2e", num_partitions=2, replication_factor=1)])
except Exception:
    pass
admin.close()
time.sleep(1)

p = KafkaProducer(bootstrap_servers=boot, acks=-1)
for i in range(20):
    p.send("lag-e2e", value=f"m{i}".encode())
p.flush()
p.close()

c = KafkaConsumer("lag-e2e", bootstrap_servers=boot, group_id="lag-e2e-grp",
                  auto_offset_reset="earliest", enable_auto_commit=True,
                  consumer_timeout_ms=5000)
for _ in c:
    pass
c.close()
print("group lag-e2e-grp committed offsets on lag-e2e")
