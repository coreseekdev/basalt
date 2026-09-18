# T-M3.5 生态真实负载 e2e——模板 ↔ 真实系统行为对照

三个负载模板（RisingWave / Vector / Bento）各对应一个真实流系统的接入模式，
加一个 librdkafka cooperative 档（补 T-M3.4 的 rdkafka 遗留面）。全部走
confluent_kafka（librdkafka 后端，2.15.0 实证）作验收客户端。

## 对照表

| 真实系统 | 模式语义 | e2e 映射 | 断言要点 |
|---|---|---|---|
| **RisingWave source**（`librdkafka_assign.py`） | source executor 不走组协调器，`assign()` 全分区 earliest 从头扫，位点引擎内部自管，不依赖 group offset 面 | [1] 产 30（2 分区轮询）→ assign() 双分区全量扫 | 30/30 不重不漏；全程无 JoinGroup/SyncGroup/Heartbeat 面 |
| **RW × 上游事务** | 产出端事务流 × 扫描读隔离 | [2] transactional.id=librdkafka-assign-txn：commit 流 10 + abort 流 10 | read_committed 只见 commit 流；read_uncommitted 双流全见（LSO 对照）；`enable.partition.eof` 界定扫尾 |
| **Vector sink**（`vector_drain_commit.py`） | drain-then-commit：poll 一批 → 处理（sink flush）→ 才手动提交 offset；崩溃恢复后从提交位续读，sink 以记录值做幂等键去重 | [1][2] 两段会话：会话 1 读 20 逐批 drain-commit 后「崩溃」（最后一批 sink 已写、未提交、不 close）；会话 2 同组续读 | 去重后 40/40 零丢失；raw 写入 > 去重数（at-least-once 崩溃路径真被触发，幂等键折叠）；终态提交位 == 40 |
| **Vector × 事务** | 消费位点并入产出事务：`send_offsets_to_transaction`（TxnOffsetCommit 面） | [3] read_committed 读 commit 流 10 → 位点随产出事务提交 | abort 流不可见；事务提交后位点生效（合计 10） |
| **Bento processor**（`bento_checkpoint_limit.py`） | checkpoint_limit：未 checkpoint 记录数达上限即背压停拉，checkpoint 完成才放行下一窗口 | 窗口上限 5：窗口满 → `pause()` 全分区 → 同步 commit → `resume()`；生产端突发 60 条 | 全程窗口 ≤ 5（consume 按 `LIMIT-window` 截流，窗口只计已交付记录——在途预取不入窗口，断言是精确的）；60/60 不重不漏；提交位点严格递增、终态 30/30 |
| **librdkafka cooperative**（`librdkafka_cooperative.py`） | cooperative-sticky（KIP-429）增量 assign/revoke；rebalance_cb 内强制 `incremental_assign/incremental_unassign` | 同组双 consumer 先后加入：波 1 预产 20 → A 独占双分区 → B 加入 + 波 2 慢流 20 | B 收到分配并消费到数据（增量路径打通）；全组 40/40 不重不漏（跨成员重复由 distinct 收口）；A 的 B-加入后撤销集 < 全量（保留 ≥ 一半——非 eager 全量撤销）；A/B 都消费到波 2（无停顿式停等） |

## 运行方式

依赖：`python3` + `confluent_kafka`（2.15.0 实证），broker 需
`BASALT_NUM_PARTITIONS=2`（模板用 `partition=i%2` 显式分区）。

```bash
# 单模板本地跑（run_e2e.sh 自起本地 broker，tempdir 数据，跑完即收）
testing/e2e/run_e2e.sh librdkafka_assign.py
testing/e2e/run_e2e.sh vector_drain_commit.py
testing/e2e/run_e2e.sh bento_checkpoint_limit.py
testing/e2e/run_e2e.sh librdkafka_cooperative.py

# docker-compose 一键拉起 + 全量 4 模板（首次构建含 cargo release，需数分钟）
testing/e2e/run_e2e_docker.sh

# 手动 compose（宿主需 python3 + confluent_kafka）
docker compose up -d --build
python3 testing/e2e/librdkafka_assign.py   # 其余三个同理（对 localhost:9092）
docker compose down -v
```

## docker 面

- `deploy/docker/Dockerfile.e2e`：两阶段自举构建（rust:1-bookworm →
  debian:bookworm-slim，同 codename 防 glibc 漂移），暴露 9092/9093/9094，
  `BASALT_*` 环境变量与本地 `run_e2e.sh` 档位一致。与仓库根 `Dockerfile`
  分工：根镜像装载主机构建的 release 二进制（部署路径），本镜像容器内构建。
- `docker-compose.yml`（仓库根）：单 broker，`BASALT_HOST=localhost` +
  端口直映射，宿主客户端 advertised 回指即闭环。`9093` 为预留面
  （多节点内部面；单节点未监听，与根 Dockerfile 暴露口径对齐）。
- `run_e2e_docker.sh`：`docker compose` v2 优先、v1 `docker-compose` 回退；
  起 broker → 等 9092 → 依次跑 4 模板 → 失败时打 broker 日志尾 → `down -v` 收尾。

## 已知风险点（联调观察项）

- `librdkafka_assign.py` [2]：librdkafka 对 fetch 应答的 aborted transactions
  列表（KIP-98 编码）校验较 franz-go 严，若红优先核 broker aborted 编码。
- `vector_drain_commit.py` [3]：`send_offsets_to_transaction` 携带
  `consumer_group_metadata()` 不透明字节，TxnOffsetCommit 需解析
  ConsumerGroupMetadata 结构——若红核该结构解析，勿简化断言。
- `librdkafka_cooperative.py`：broker classic 组协议选择需接受
  `cooperative-sticky` 协议名（T-M3.4 已做 leader 偏好序 ∩ 支持集，
  franz-go 同名协议已过）；撤销集大小依赖 librdkafka sticky 分配器
  的保留语义（2 分区场景应为撤销 1 保留 1）。
