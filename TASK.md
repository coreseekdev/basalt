# TASK.md — Basalt 任务分解树（WBS 入口）

> **项目**：Basalt —— 用 Rust 复刻 Kafka 协议兼容的消息流平台
> **架构依据**：[../docs/10-rust-blueprint.md](../docs/10-rust-blueprint.md)（架构蓝图）
> **测试依据**：[../docs/11-testing-strategy.md](../docs/11-testing-strategy.md)（测试机制设计与验收不变式）
> **知识库**：[../docs/README.md](../docs/README.md)（12 个参考项目技术要点）

## 使用规则

- **状态图例**：⬜ 未开始 ｜ 🟨 进行中 ｜ ✅ 完成 ｜ 🚫 取消
- 每个任务包含：**产出物 / 验收标准 / 依赖 / 参考**（参考指向知识库对应章节）。
- **测试与功能同步交付**：每个里程碑内的测试基建任务（标注 🔬）与功能任务同批验收，不允许"先功能后补测试"。
- 提交信息引用任务 ID，如 `feat(protocol): T-M0.2 支持 Produce v3-11 编解码`。
- 通用 DoD（所有任务）：`cargo clippy -- -D warnings` 零告警；新代码有单元测试；涉及持久化/分布式的改动必须能跑在仿真 shim 上。

## 总览树

```
Basalt：Rust 版 Kafka 兼容消息流平台
│
├── T-0  工程基建（贯穿）···················· CI 门禁 / 协议 JSON 供应 / 依赖与可观测性底座
│
├── M0  协议与单机 ························· RecordBatch → 协议栈 → 存储引擎 → 网络 → 四 API → 🔬仿真基建
├── M1  单机语义完整 ························ 消费组 Classic / offset / ListOffsets / topic 生命周期 / 🔬report card v1
├── M2  集群与复制 ·························· openraft 控制器 / 多 broker / ISR 形态复制（ADR-10） / leader epoch / 🔬集群仿真+混沌
├── M3  高级语义 ··························· 幂等 producer / 事务 / KIP-848 新消费组 / cooperative / 🔬生态 e2e
├── M4  生产化 ····························· SASL/TLS/ACL / quota / tiered storage / 性能工程（io_uring、thread-per-core 评估）
│
└── T-Q  质量与形式化（贯穿）················· 🔬种子扫描 / 🔬opfuzz / TLA+ 规约 / 基准报表
```

里程碑退出条件（对应蓝图 §10）：

| 里程碑 | 退出条件 |
|---|---|
| M0 | rdkafka/franz-go 冒烟通过（Produce/Fetch/acks=1）；turmoil 单机崩溃持久性种子扫描绿 |
| M1 | librdkafka + franz-go + kafka-clients report card ≥ 90%；仿真种子扫描无丢重 |
| M2 | 仿真切主/分区/追赶场景不丢不重；ducktape 式 bounce 测试通过；failover L1：崩溃 <2s、计划内交接毫秒级（ADR-10） |
|   | **验收进度（2026-09-16）**：M2 数据面九场景 + 引擎模式九场景 e2e 全绿（双模式）；T-M2.1 ✅（双引擎控制器 + WAL + ISR/reconciliation + 规格化五门禁）；T-M2.3 ✅ 核心闭合（ISR 状态机 + 冻结提交面 + 长轮询调优 249K msg/s/延迟 p50 1.0ms）；T-M2.2 🟨（全量广播/任意节点 bootstrap ✅，增量 delta 待做）；C11 ✅ 四档客户端全闭合（kafka-python/librdkafka/franz-go/kafka-clients，各档均有战果）。ReplicationCommit.tla 规格化五门禁入 CI（安全名义 + 判别力阴性对照 + 名义活性 + 恢复活性）；账本 ㉒-㊺ 二十四缺陷全闭环 |
| M3 | RisingWave/Vector/Bento 式真实负载 e2e 通过；事务 marker 语义专项通过 |
| M4 | 基准对标报表产出；混沌长跑 24h 无不变式违反 |

**版本分界（2026-09-08 决策）**：

- **v1 = M0–M2 + T-M3.1（幂等 producer）**：3 节点集群、acks=all、Classic 消费组、ELR 选举、failover L1。对外发布的第一个可用版本。
- **v2（依序）**：T-M3.2 事务（直接 TV2）→ T-M3.3/3.4 KIP-848 + cooperative → T-M4.3 分层存储 + 存储模式插件（ADR-11）→ share groups 评估（触发条件：librdkafka 支持落地）。**T-M3.2 与 T-M4.3 启动设计前先读 [Arroyo §11](../docs/12-arroyo.md)**（对象存储 manifest/fencing/两段提交协议专项精读，ADR-11 路线的现成参照实现；2026-09-16 增补对照：[Morax §6](../docs/16-morax.md)——RDS 权威 + 不可变对象的另一端点路线，T-M4.3 启动时两路并排评审）。

---

## T-0 工程基建（贯穿）

| ID | 状态 | 任务 | 产出物 | 验收标准 | 依赖 | 参考 |
|---|---|---|---|---|---|---|
| T-0.1 | ✅ | 仓库与 workspace 初始化 | 本 repo：workspace + 8 crates 骨架 + TASK.md | `cargo check` 通过；lints 生效 | — | [蓝图 §1](../docs/10-rust-blueprint.md) |
| T-0.2 | ⬜ | 依赖集中管理（workspace.dependencies） | tokio/bytes/tracing/thiserror/crc32c/memmap2 集中管理 | 版本统一、零重复声明 | T-0.1 | [Iggy §1](../docs/03-iggy.md) |
| T-0.3 | ⬜ | 协议 JSON 供应商化 | 上游 `clients/src/main/resources/common/message/*.json` 导入 `protocol/definition/` + 版本锁定脚本 | `xtask protocol-check` 可 diff 上游新版本并列出增量 API | T-0.1 | [Kafka §7](../docs/01-apache-kafka.md) |
| T-0.4 | ⬜ | CI 流水线（PR 门禁 + 夜间） | GitHub Actions/Gitea Actions：fmt+clippy+test+固定 10 种子仿真；夜间种子扫描 1000+ | 门禁全绿才算合入；失败输出种子与复现命令 | T-0.2 | [测试 §11](../docs/11-testing-strategy.md) |
| T-0.5 | ⬜ | 可观测性与配置底座 | tracing+OTLP 接入；模块化配置文件 + 类型化 env 覆盖；统一 `BasaltError` | server 启动即输出结构化日志；env 覆盖有单测 | T-0.2 | [Iggy §8](../docs/03-iggy.md) |

## M0 协议与单机

| ID | 状态 | 任务 | 产出物 | 验收标准 | 依赖 | 参考 |
|---|---|---|---|---|---|---|
| T-M0.1 | ⬜ | RecordBatch v2 编解码（record/） | batch/record 头、varint/zigzag delta 编码、压缩（none+lz4+zstd 起步，gzip/snappy 由 M1 report card 缺口驱动补齐——ADR-9）、CRC32C | proptest 往返无损；能解码 Kafka 产出的 golden 文件 | T-0.2 | [Kafka §7](../docs/01-apache-kafka.md) [生态 §A](../docs/08-ecosystem.md) |
| T-M0.2 | ⬜ | 协议代码生成（protocol/） | build.rs 读 JSON → 生成 request/response 类型 + ApiVersions 协商；sans-I/O 编解码 | ApiVersions+Metadata+Produce+Fetch 全版本往返单测；tagged fields 支持 | T-0.3 | [Nisshi §2](../docs/05-nisshi.md) |
| T-M0.3 | ⬜ | 存储引擎核心（storage/） | DiskIo trait 定稿；segment+稀疏索引+timeindex；recovery checkpoint；producer snapshot；FsyncSchedule | opfuzz 随机操作序列后 recover() 恒合法（🔬T-Q.1）；崩溃矩阵测试通过 | T-0.2 | [Kafka §1](../docs/01-apache-kafka.md) [Walrus §2](../docs/07-walrus.md) |
| T-M0.4 | ⬜ | 网络层与请求流水线（server/） | tokio accept/连接管理、有界请求队列、per-connection 响应通道、优雅停机 | 连接 churn 压测无泄漏；背压可观测 | T-0.2 | [Kafka §2](../docs/01-apache-kafka.md) |
| T-M0.5 | ⬜ | 四 API 打通（acks=1 单机） | ApiVersions/Metadata/Produce/Fetch 处理器；min_bytes 长轮询（timer wheel purgatory） | rdkafka-rs 冒烟：produce→consume 回读一致 | T-M0.1..0.4 | [StoneMQ §3](../docs/06-stonemq.md) |
| T-M0.6 🔬 | ⬜ | 仿真测试基建（testing/） | 网络/磁盘/时钟/随机源四边界 shim；Cluster harness（事件队列+step 循环）；不丢不重断言器；verifiable producer/consumer | 单机"写→sync→crash→重启→读回"种子扫描（≥100 seeds）绿；失败一行复现 | T-0.4 | [测试 §3](../docs/11-testing-strategy.md) [turmoil 深研](../docs/README.md) |

## M1 单机语义完整

| ID | 状态 | 任务 | 产出物 | 验收标准 | 依赖 | 参考 |
|---|---|---|---|---|---|---|
| T-M1.1 | ⬜ | 消费组 Classic 协议 | FindCoordinator/JoinGroup/SyncGroup/Heartbeat/LeaveGroup + 组状态机（record-based 存储） | rebalance 收敛单测；组成员崩溃后组恢复 | T-M0.5, T-M0.6 | [Kafka §5](../docs/01-apache-kafka.md) [StoneMQ §3](../docs/06-stonemq.md) |
| T-M1.2 | ⬜ | offset 管理 | OffsetCommit/OffsetFetch；内部日志（`__consumer_offsets` 等价物）雏形；崩溃恢复=重放 | offset 提交崩溃后不回退已消费进度（仿真断言） | T-M1.1 | [Kafka §5](../docs/01-apache-kafka.md) |
| T-M1.3 | ⬜ | offset 查询与 fetch 语义 | ListOffsets（earliest/latest/by timestamp）、partition EOF、`offsets_for_times`、watermark 排他语义 | 与 Kafka 产出逐字节对拍的语义单测；rdkafka `offsets_for_times` 跑通 | T-M0.5 | [生态 §A](../docs/08-ecosystem.md) |
| T-M1.4 | ⬜ | topic 生命周期 | CreateTopics/DeleteTopics、AllowAutoTopicCreation、topic id 生成与刷新 | franz-go #676 场景专项：删重建后长连接 consumer 自动恢复 | T-M0.5 | [生态 §C](../docs/08-ecosystem.md) |
| T-M1.5 🔬 | ⬜ | report card 管线 v1 | librdkafka + franz-go 官方套件对拍；白名单 + FINDINGS.md；xtask compat-report | CI 出双客户端兼容卡；每个 gap 有记录与解锁条件 | T-M0.6, T-M1.1..1.4 | [Nisshi §6](../docs/05-nisshi.md) |
| T-M1.6 🔬 | ⬜ | 仿真场景扩展：消费组 churn | 场景：随机成员 crash/bounce → rebalance 收敛、无双消费（fencing） | ≥500 seeds 绿 | T-M0.6, T-M1.1 | [测试 §3.2](../docs/11-testing-strategy.md) |

## M2 集群与复制

| ID | 状态 | 任务 | 产出物 | 验收标准 | 依赖 | 参考 |
|---|---|---|---|---|---|---|
| T-M2.1 | ✅ | openraft + raft-rs 双引擎控制器（ADR-15/16） | 双引擎 + MSG_RAFT wire + 控制器 WAL 写穿（ADR-17：dendro SPEC 02 移植子集，快照降级为状态机种子）+ propose 转发/职权门控 + 就任 reconciliation；**验收（2026-09-16）**：引擎模式九场景 e2e 全绿（含 ctrl_kill +1.8s 恢复、bounce 6 轮零丢失、ISR 收缩/重回/拉齐），CI 门禁覆盖 | multinode_raftrs/isr/ctrl_kill e2e；restore 回归单测（快照+WAL 联合恢复、无快照仅 WAL 重建）；ReplicationCommit.tla 规格化（判别力阴性对照入 CI） | T-M1 全部 | [Kafka §4](../docs/01-apache-kafka.md) |
| T-M2.2 | 🟨 | 多 broker 拓扑与元数据广播 | Metadata 全量广播（200ms Sync 轮询，任意节点 bootstrap ✅）+ broker 侧路由表/分区归属（routes ✅）+ 请求转发（FetchSlice 内部协议 ✅）；**增量 delta 广播未做**（对照调研：dendro manifest delta 同构，全量在当前规模无瓶颈） | 3 节点起停/扩容（multinode runner ✅；docker 化未做）；客户端任意节点可 bootstrap ✅（四档客户端验证） | T-M2.1 | [Redpanda §1](../docs/02-redpanda.md) |
| T-M2.3 | 🟨 | ISR 形态复制协议 + leader epoch（ADR-10） | 控制器指派 leader + epoch fencing + follower-pull 多数派 append + HW 水位 + leader-epoch checkpoint；**规约义务已履行（缺陷⑯/C14①）**：租约随进程死亡失效（actor 重启默认 Follower）+ acks=all 下限钉死 max(min.insync, 多数派)（回归测试锁定）。**L1 已度量（2026-09-14）**：崩溃 failover 1063ms（<2s 门禁）、计划内交接 TransferLeader 153-279ms（epoch+1 走 LeaderChange）。**ISR 状态机 + reconciliation（2026-09-16，账本 ㉟ 完整修复）**：显式 ISR 收缩/追平重回 + HW=min(ISR LEO) + 冻结提交面 + 就任拉齐——failover 对副本数据状态不再敏感；规格化 ReplicationCommit.tla（判别力阴性对照入 CI）| 切主/分区/追赶/bounce/ISR/ctrl_kill 场景 e2e 全绿（multinode_*.py 九场景）；LSO/事务位未建模（M3） | T-M2.1 | [Kafka §3](../docs/01-apache-kafka.md)（KIP-966/951） |
| T-M2.4 | 🟨 | 复制调优 | 按 follower 聚批、落后副本批量追赶+全局限流、共用心跳 RPC。**基线已实测 + 首项调优落地（2026-09-16）**：FetchSlice 长轮询（事件唤醒替代 150ms 轮询节拍）→ 3 副本 acks=all produce **249K msg/s（243.6 MB/s，较基线 2.1×，达单节点 53%）**、单条延迟 **p50=1.0ms（较基线 152ms 提升 150×）**；追赶 P99 影响待测 | 基线 ✅ + 调优首项 ✅；按 follower 聚批/限流待做 | T-M2.3 | [Redpanda §3](../docs/02-redpanda.md) |
| T-M2.5 🔬 | ✅ | 集群仿真场景 | 五场景接入 harness：切主（multinode_failover 60/60）、分区+追赶（multinode_partition 40/40，BASALT_BLOCK_PEERS 双向断边）、bounce（multinode_bounce 六轮零丢失）、in-flight 重复投递（multinode_replay 30/30，offset 纪律幂等）、hold 重排（顺序拉取模型下由同一纪律吸收——乱序=base 不匹配拒绝+重试，注释标注）。全部进 CI | 每场景 ≥500 seeds（v1 为 1 组合多轮，扩量随 T-Q.4） | T-M2.3 | [测试 §3.2](../docs/11-testing-strategy.md) |
| T-M2.6 🔬 | 🟨 | ducktape 式混沌 v1 | bounce 矩阵 v1 已建（multinode_bounce.py：hard/clean × node1/node2 六轮，零丢失；dup=at-least-once 重试属 T-M3.1 幂等）。分区注入 BASALT_BLOCK_PEERS。1h 档与随机操作序列 ⬜ | 24 种注入组合下不变式全绿（当前 6 轮 v1） | T-M2.5 | [测试 §7](../docs/11-testing-strategy.md) |
| T-Q.2 | ✅ | TLA+ 规约 v0.3（数据面+消费组+活性） | 数据面：ADR-10 协议 + synced 崩溃边界（C14①）+ 控制器租约（缺陷⑯）+ 副本读消费；消费组：v0.3 generation fencing + 审计位监控（MUT-C/E 判别力实证）；活性：数据面稳定环境三性质绿 + 消费组二分负结果。全矩阵 TLC（含 1.27 亿状态全空间）+ 5 组突变对照（账本 C1-C14） | ✅ TLC 全绿/阴性对照红并入库（设计变更时重跑）；六场景门禁在 scripts/verify.sh | T-M2.1 | [Walrus §6](../docs/07-walrus.md) |

## M3 高级语义

| ID | 状态 | 任务 | 产出物 | 验收标准 | 依赖 | 参考 |
|---|---|---|---|---|---|---|
| T-M3.1 | 🟨 | 幂等 producer | **服务端去重已落地（2026-09-16）**：InitProducerId（进程内 PID 计数，跨 broker 唯一性 POC 边界）+ partition actor 幂等状态机（PID → epoch/last_seq/最近 5 批缓存：重复回放缓存偏移、乱序回 OutOfOrderSequence(45)、epoch 升级重置会话）+ 错误码 45/74/75 撞号修复；franz-go（默认幂等）e2e ✅、确定性单测 ×2 ✅ **librdkafka enable.idempotence=true e2e ✅（librdkafka_idempotent.py）**、**跨 broker PID 唯一性 ✅（PID = node_id 高位 | 进程内计数，构造性唯一零协调）**、**重试风暴仿真 ✅ retry_storm_tests（5 轮 × 2 PID × 3 批交错风暴：offset 恒定/LEO 不漂移/epoch 升级旧会话拒收——T-M3.1 全项闭合）** | T-M2.3 | [Kafka §6](../docs/01-apache-kafka.md) |
| T-M3.2 | 🟨 | 事务 | **设计 ✅ [ADR-18](docs/adr-18-transactions.md) · 块 a 数据面 ✅ · 块 b 协调面 ✅（TxnLog/两段/接管纯函数/sweep）· 块 c API 面 ✅（五 handler/FindCoordinator/跨节点 RPC）· 块 d java 验收 ✅（txnPhase ×4：commit 可见/abort 不可见/offset 消耗/sendOffsets 提升——KIP-447 全链）· TLA+ 三门禁 ✅（TransactionCommit.tla 名义 1,088 态绿 + 两突变体红 142/170 态入 CI）**；块 d franz-go 事务档 ✅（第二真客户端 v6/v13+ 协商路径三验收面）**；待做：Jepsen 三场景仿真；其余：txn coordinator + TxnLog；事务版本直接 TV2（KIP-890，ADR-9）不做 TV1 兼容；LSO 推进；control record 占 offset 不投递；read_committed 过滤；KIP-447 验证；接管恢复判定纯函数化（Ready/ReplayCommit/Orphaned 三态） | read_uncommitted/read_committed 对比专项；abort 后数据不可见但 offset 已消耗 | T-M3.1 ✅ | [Kafka §5/§6](../docs/01-apache-kafka.md) [Arroyo §11](../docs/12-arroyo.md) [ADR-18](docs/adr-18-transactions.md) |
| T-M3.3 | ✅ | KIP-848 新消费组协议 | **[ADR-19](docs/adr-19-kip848-consumer-groups.md) 四块全量落地（2026-09-18 闭合）**：ConsumerGroupHeartbeat 单循环（无栅栏）、服务端 Range 分配、member-epoch fence、组类型分叉；块 a 状态机+分配器 ✅（consumer_group_tests ×6）；块 b 协议面 ✅（68/69 handler+宣告+probe_consumer_group_layouts 字节级回归；fenced 三态映射 82/25 + describe 未知组 69；TopicId 反查 + 分区数快照经 Lookup）；块 c 验收面 ✅（franz-go v1.21.6 三验收面全过——混布收敛/增量 rebalance 无停等（存量成员间隙 201ms）/双隔离级；宣告升 0-1（should848 硬性 v1）+ codec 补 nullable-struct 原语（账本 51）+ 订阅 keepalive null 语义）；**块 d 规格化 ✅（ConsumerGroup848.tla 三门禁：名义 82,945 态绿 + 两突变体红——HeartbeatFence/CommitFence 正交捕获；C9 良构与 fencing 按 member-epoch 重述）**。客户端门槛定案：franz-go 先行，java 4.x 单列。遗留（P2 档）：sticky 分配器归 T-M3.4；POC 边界——Leave 不校验请求方身份、无 session timeout（ADR-19 §7） | T-M1.1 | [Kafka §5](../docs/01-apache-kafka.md) |
| T-M3.4 | ✅ | cooperative-sticky rebalance | **[ADR-20](docs/adr-20-cooperative-rebalance.md) 全量落地（2026-09-18 闭合）**：848 侧 ServerAssignor 协商（range/uniform first-wins；未知值 112）+ uniform 分配器 movement-minimizing（保留段按 topic 订阅者数升序 + 补派订阅约束优先——金样单测 ×2）；classic 侧组协议选择 = leader 偏好序 ∩ 全体支持集（去硬编码 range）+ 协调器 pending join/sync 悬挂修复（组清空回可重试 27，账本 52）；**e2e ✅（testing/franzgo/coop：franz-go cooperative-sticky B 中流加入 A 不断流（间隙 501ms）、全组 60/60 不重不漏——无停顿式双全量 rebalance 验收达成）**。遗留（P2）：rdkafka cooperative 档 | T-M3.3 ✅ | [生态 §C](../docs/08-ecosystem.md) |
| T-M3.5 🔬 | ✅ | 生态真实负载 e2e | **[ADR-20/11 验收，2026-09-18 闭合]**：三模板全过——rdkafka 手动 assign（RW 式 + 事务隔离，librdkafka_assign.py）/ Vector 式 drain-then-commit（崩溃批重放 at-least-once + 位点事务联动，vector_drain_commit.py）/ Bento 式 checkpoint_limit 背压（窗口峰值≤5 + 位点递增，bento_checkpoint_limit.py）；**docker-compose 一键拉起 ✅**（docker-compose.yml + run_e2e_docker.sh 4/4 PASS；Dockerfile.e2e 宿主二进制装载式，离线 override 用宿主 glibc loader 直启）；rdkafka cooperative 档 ✅（librdkafka_cooperative.py——T-M3.4 遗留闭合） | T-M3.2 ✅ | [生态 §A/B/C](../docs/08-ecosystem.md) |
| T-M3.6 🔬 | 🟨 | 事务仿真与兼容专项 | 事务场景进仿真 harness（**Jepsen 三场景 ✅ jepsen_sim_tests 500 seeds 全绿 + 50/commit 档入 CI**；错误码分流语义表 ✅ error_semantics_tests（官方数值+retriable 分类双重锁定，统一映射函数三处收口） | 事务场景 ≥500 seeds；语义表 100% 覆盖 | T-M3.2, T-M2.5 | [测试 §3.2/§5](../docs/11-testing-strategy.md) |

## M4 生产化

| ID | 状态 | 任务 | 产出物 | 验收标准 | 依赖 | 参考 |
|---|---|---|---|---|---|---|
| T-M4.1 | ⬜ | 安全 | SASL PLAIN/SCRAM-256/512（rsasl）、TLS（rustls+aws-lc-rs，ADR-13）、ACL（含 IDEMPOTENT_WRITE/CLUSTER） | 三客户端 SASL+TLS 矩阵全绿 | T-M1.5 | [生态 清单](../docs/08-ecosystem.md) |
| T-M4.2 | ⬜ | 限流与会话 | quota（produce/fetch 带宽）、fetch session、fetch 大小 PID 控制器 | quota 生效有指标；session 复用降低 metadata 压力（基准） | T-M2.4 | [Redpanda §7](../docs/02-redpanda.md) |
| T-M4.3 | 🟨 | 分层存储 + 存储模式插件 | **[ADR-21](docs/adr-21-tiered-storage.md) v1 落地（2026-09-18）**：前置阅读 ✅（Arroyo §11 + Morax §6 两路并评——取 CAS+权威记录路线，弃 RDS 权威）；块 a ObjectStore 四原语（create CAS 冲突回读）+ LocalFs/Memory 双实现 ✅；块 b 分层注册表（写序=段对象→段记录→本地回收，孤儿无害+启动期 GC）+ Log::release_sealed（回收不动 log_start）+ 读穿透/FetchSlice 分流/重启恢复 ✅；块 c e2e ✅（tiered 模式 400 条：滚动上传→本地回收→读穿透→重启恢复全链 PASS）。**联调副产修复：账本 53（幂等 last_seq 语义）/54（终态 fence 语义修订，ADR-18 §14）**。待做（v2 边界）：S3 适配器（契约已收窄）/per-topic storage-mode（穿控制器元数据）/对象侧 retention/GC 运行期化 | T-M3.2 | [Arroyo §11](../docs/12-arroyo.md) [Morax §6](../docs/16-morax.md) [ADR-21](docs/adr-21-tiered-storage.md) |
| T-M4.4 | ⬜ | 性能工程 | criterion 微基准；端到端基准管线（对标 Kafka/Redpanda）；io_uring 写路径（O_DIRECT+批量提交）与 thread-per-core（compio）评估报告 | 吞吐/延迟基线报表；演进建议（动/不动执行器） | T-M2.4 | [蓝图 §7](../docs/10-rust-blueprint.md) [Iggy §3](../docs/03-iggy.md) |
| T-M4.5 | ⬜ | 运维工具 | log_parser 离线段检查工具；集群 bootstrap/topic/消费组管理 CLI（对标 rpk 最小集） | 损坏段可检出并定位；CLI 覆盖常用运维动作 | T-M0.3, T-M2.2 | [StoneMQ §5](../docs/06-stonemq.md) |
| T-M4.6 | ⬜ | 湖仓导出（v2 正式特性，ADR-12） | Kafka batch → Arrow → iceberg-rust 写表管道（REST catalog） | 消息落 Iceberg 表可被 DataFusion 查询 | T-M3.2 | [Nisshi §4](../docs/05-nisshi.md) [iceberg-rust](../docs/08-ecosystem.md) |

## T-Q 质量与形式化（贯穿）

| ID | 状态 | 任务 | 产出物 | 验收标准 | 依赖 | 参考 |
|---|---|---|---|---|---|---|
| T-Q.1 🔬 | 🟨 | 存储 opfuzz | DiskIo 层随机操作序列 fuzzer（append/flush/roll/truncate/recover 交错）——**已建**：四档（clean/chaos × 标准/batch_io）80 种子全绿，累计抓出 7 个真实缺陷（账本 C7'）；SimDisk 故障注入含确定性开关 | 任意序列后 recover 恒合法 ✅；CI 每次 PR 跑 10^4 序列 ⬜（CI 流水线未建，T-0.4） | T-M0.3 | [Redpanda §8](../docs/02-redpanda.md) |
| T-Q.2 | ✅ | TLA+ 规约（条目列于 M2 表） | 现状见 M2 表 T-Q.2 行：数据面/消费组/活性全闭合，缺陷⑩-⑯ 均有机器证据 | 全绿 + 阴性对照红（verify.sh 门禁） | T-M2.1 | [测试 §9](../docs/11-testing-strategy.md) |
| T-Q.3 🔬 | 🟨 | 基准报表 | **已建（手工档）**：BENCHMARKS.md——vs Redpanda 同机对比 + 三客户端档（kafka-python/librdkafka/franz-go）；CI 报表与阈值告警 ⬜ | 报表趋势可查；回归>15% 触发告警 | T-M4.4 | [测试 §8](../docs/11-testing-strategy.md) |
| T-Q.4 🔬 | ⬜ | 混沌长跑 | 每周 24h 随机节点操作+分区+磁盘故障长跑 | 不变式零违反；失败自动产出种子/操作序列 | T-M2.6 | [测试 §7](../docs/11-testing-strategy.md) |

---

## 当前状态

- **里程碑**：T-0 → M0（未开始功能开发）
- **已完成**：T-0.1（仓库与 workspace 初始化，2026-09-07）
- **下一步建议**：T-0.2（依赖血液）与 T-0.3（协议 JSON 供应商化）可并行，随后 T-M0.1 / T-M0.6 并行开工（功能线与测试线同时起跑）。
- **2026-09-08**：定位与架构决策定稿（生产级开源替代 + 商业雏形；ADR-8~13）；v1 范围 = M0–M2 + T-M3.1。
- **2026-09-08（实施）**：T-0.2/0.3、M0 全部、M1 主体、M2 POC 完成——
  - ✅ 单机：kafka-python 真实客户端 e2e PASS（produce/fetch/listoffsets/自动建题，不丢不重）
  - ✅ 消费组：Classic 协议 + offset 持久化（append-only 重放）+ 确定性两段式 e2e PASS
  - ✅ 多节点 POC：控制器（record 日志 + failover watch）+ 内部 RPC + follower-pull 复制 +
    HW 停等 acks=all；kill -9 leader 实测 FAILOVER 迁移、已确认数据完好
  - ⏳ 已知问题：pod 重建竞态下的客户端重试风暴、CreateTopic 在 broker 注册完成前建题会钳制 RF、
    failover 自动化 e2e 的 read 窗口需放宽（机制已实测，脚本待硬化）
  - ✅ F2 迭代（R1 三路 Code Review → Fix）：
    - 协议：长度上限防御、tag varint、UTF-8 安全、slice_ref
    - 存储：两阶段 append（checkpoint 回滚）、TimeIndex 稀疏化、index truncate+write、
      BatchHeader magic 内建校验、truncate_to、sync_dir、retention 删除
    - 分布式：HW 纪律（append 不自抬 HW）、FetchSlice fencing、ISR 新鲜窗口、
      min.insync.replicas、pull 生命周期绑定 leader 变更、分叉尾巴截断自愈、
      控制器自心跳豁免、conn 队头阻塞消除
    - API：OffsetForLeaderEpoch(23) + DescribeGroups(15) + DeleteRecords 预留
    - 基准：produce 91045 msg/s（acks=all 流水线 1KB 消息）、consume 153891 msg/s、p50=0.2ms p99=0.5ms
  - ⏳ 已知问题：pod 重建竞态、failover 自动化 e2e 硬化、CreateTopic 注册竞态 RF 钳制
  - 📁 k8s：deploy/k8s（3 节点 Deployment + hostPath + chaos.sh）；microk8s 实测 Running

## 关键决策记录（ADR 索引）

| # | 决策 | 依据 |
|---|---|---|
| ADR-1 | 协议层走官方 JSON → build.rs 代码生成，sans-I/O | Nisshi/Kafka 双重验证；StoneMQ 手写为反例 |
| ADR-2 | 元数据采用 KRaft 式单写者控制器，共识用 openraft | Kafka QuorumController 范式；Walrus/Octopii 已验证 openraft 可行 |
| ADR-3 | ~~数据面 per-partition Raft group + offset translator~~（已被 ADR-10 修订为后评估项） | Redpanda 模型；Rust 无 Kafka 量级先例，M2 关键路径风险过高 |
| ADR-4 | RecordBatch 只支持 magic v2 | Kafka 自身已弃 v0/v1 下转换 |
| ADR-5 | 运行时 M0 用 tokio 多线程，partition 归属表抽象先行 | 保留 compio/thread-per-core 演进位 |
| ADR-6 | 存储写路径先"缓冲写+显式 FsyncSchedule"，O_DIRECT/io_uring 为 M4 评估项 | Kafka 形态简单正确；性能工程后置但接口先留 |
| ADR-7 | 测试边界四件套（网络 shim/DiskIo/tokio::time/统一随机源）为 M0 硬性架构约束 | 测试 §3.1；仿真不可后补 |
| ADR-8 | 选举策略：ISR → ELR → unclean 三段状态机；unclean.leader.election 默认 false | KIP-966 Part 1（Kafka 4.1 起新集群默认启用）；Confluent DR 最佳实践 |
| ADR-9 | 协议基线锁 Kafka 4.x feature levels；事务直接 TV2（KIP-890）不做 TV1；压缩四 codec（none/lz4/zstd 先行，gzip/snappy 由 report card 缺口驱动补齐） | KIP-896/724（版本面收敛、消息格式 v2-only）；KIP-890；生态验收清单要求四 codec |
| ADR-10 | 数据面复制走 ISR 形态：openraft 仅元数据 + 控制器指派 leader + epoch fencing + follower-pull 多数派 append。failover 预算：v1=L1（崩溃 <2s / 计划内交接毫秒级），心跳预留 L2 健康位图与 stall 上报，短租约 L3 为 SLA 驱动项；per-partition raft 降为后评估项 | 与 Kafka 语义零翻译（无 offset translator、无 retention 删头与 raft 不变式冲突）；Walrus/dendro 先例；Rust 无 per-partition raft 生产先例。翻案触发：openraft 0.10 GA + 千级 group 先例 / 分区目标 >5 万每 broker / 商业客户拒绝秒级切换 |
| ADR-11 | 存储模式插件化：SegmentStore trait 于 M2 定型，manifest 走对象存储条件写 CAS（put_if_not_exists）；local/tiered/cloud-direct 为同一内核的 per-topic storage-mode；写路径不做每分区一对象。**v1 落地（2026-09-18，ADR-21）：ObjectStore 四原语 + tiered 模式（broker 级开关）+ 读穿透/恢复/GC；cloud-direct 与 per-topic 开关为 v2 边界** | 2025-26 业界共识（Confluent Freight / Redpanda Cloud Topics / WarpStream Lightning）；dendro/SlateDB/Delta 的 CAS 先例；避免 copy_if_not_exists（S3 非原子） |
| ADR-12 | 内嵌 WASM 算子为非目标；produce/fetch 流水线预留声明式算子钩子（drop/DLQ 路由/header 改写/脱敏）；湖仓导出升格为正式特性 | Kafka 生态仅 Redpanda 一家 GA 且未成 table stakes；每消息变换主流在外置 pipeline（Vector/VRL、Bento）；Tableflow/AutoMQ/Redpanda Iceberg Topics 为 2025-26 产品化主流 |
| ADR-13 | 工程选型锁定：openraft 0.9.x（0.10 alpha 不作基线）；TLS=rustls+aws-lc-rs（明文路径 sendfile 零拷贝、TLS 路径用户态拷贝）；SCRAM=rsasl；客户端传输 TCP+Kafka 线协议（不做 QUIC） | openraft 生产用户均在 0.9；rustls 官方基准优于 OpenSSL 10-19%；QUIC 无消息 broker 生产先例 |
| ADR-14 | batch_io 持久化点：drain 窗口末先 flush 再统一应答（**ack ⇒ 已写入段文件**）；`sync()` 先排空 staging 再 fsync（**sync 即持久**）。契约经 DiskIo 抽象定义、与 I/O 实现无关——禁止代码依赖缓冲 I/O 特有行为；O_DIRECT（M4）在同契约下以设备写+FLUSH CACHE 实现 | opfuzz batch_io 档 + 性能 review #8 + code review 二轮 P0-2 实证"ack 而未写文件" |
