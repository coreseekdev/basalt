# ADR-19：KIP-848 新消费组协议（T-M3.3）

- 状态：落地中（设计 2026-09-17 草案；块 a ✅ 2026-09-17；块 b ✅ 2026-09-18）
- 依据：TASK.md T-M3.3/T-M3.4；Kafka §5（01-apache-kafka.md）；spec/ConsumerGroup.tla
  （C9 经典协议规格化：generation 令牌 + InvStableWellFormed）与
  CONSUMERGROUP-LIVENESS.md（四级公平性均不可满足的负结果）；KIP-848 官方语义。

## 1. 目标与非目标

**目标**：`group.protocol=consumer` 类型组——单一 RPC `ConsumerGroupHeartbeat`
（key 68，v0）承载全部成员管理；**服务端分配**（Range 起步）；增量 rebalance
（无 JoinGroup/SyncGroup 栅栏、无全员停等）；`ConsumerGroupDescribe`（key 69）
管理面。验收：新旧协议混布 rebalance 收敛；kafka-clients 4.x 客户端跑通。

**非目标（v2 边界）**：SubscribedTopicRegex（v1 字段，正则订阅后置）；
Share groups（KIP-932/controller-share groups，独立评估项）；classic 组的
存量改造（classic 路径原样保留，见 §5 共存）；持久化组成员关系（与 classic
同边界：成员驻内存，组重进即语义等价 session 过期——ADR 前例）。

## 2. 核心语义（与 classic 的对照）

| 维度 | classic（现有） | KIP-848（本设计） |
|---|---|---|
| RPC | JoinGroup/SyncGroup/Heartbeat/LeaveGroup 四件套 | ConsumerGroupHeartbeat 单循环 |
| rebalance 栅栏 | 全员停等（PreparingRebalance→CompletingSync） | 无栅栏：服务端逐成员推送 target assignment 增量 |
| 分配 | 客户端 leader 分配（SyncGroup 上交） | **服务端分配器**（Range 起步，ServerAssignor 字段协商） |
| fence 令牌 | generation（组级） | **member epoch（成员级）**——与 C9 的 generation-token fencing 同构下沉 |
| 离开 | LeaveGroup / session 超时 | 心跳超时 / 成员主动置 MemberEpoch=-1 |

**心跳循环**（ConsumerGroupHeartbeatRequest → Response）：
1. 成员携带 `MemberId`（首个心跳为空串 → 服务端分配返回）、`MemberEpoch`、
   `TopicPartitions`（**当前 owned 集**——增量确认面）；
2. 服务端：成员注册/续租 → 若订阅集或成员集变化则**重算 target assignment**
   → 响应携带新 `MemberEpoch` + `Assignment`（target 与 owned 的差异由
   客户端本地收敛：先撤销 revoke 集、再获取 assign 集）；
3. `HeartbeatIntervalMs` 由服务端下发（POC 固定 5s）。

**member epoch 语义**：每次 target assignment 变化 epoch+1；服务端拒绝
epoch 落后的心跳（fence 面，C9 的 InvCommitFencedMon 同构下沉为
「stale-epoch 心跳被拒」）。

## 3. 服务端状态机与分配器

组状态（简化，无栅栏相位）：`Empty → Stable`（含成员级 target/owned 追踪）；
成员集或订阅集变化 → assignment dirty → 下一个心跳重算。

- **分配器（Range 起步）**：订阅并集的 topic 列表 × 成员列表（按 MemberId
  稳定排序），topic 的分区逐个轮派到成员（range per topic）——与 java
  RangeAssignor 结果对齐，便于对拍；
- **增量性**：target 变化时保留未受影响成员的现有分配（sticky 性质由
  「只在必须移动时移动」的差分实现——POC 先全量重算 + 差分下发，sticky
  分配器列 T-M3.4）；
- 成员离开（超时/epoch=-1）：其分区回池，其余成员下一心跳收到增量。

## 4. 协议面与接线

- key 68（v0）/ 69（v0）宣告 + dispatch（schema 已在 protocol/definition）；
- FindCoordinator：组路径不变（Type=0 回自身——组协调器 POC 全节点）；
- **组类型分叉**：`GroupManager` 增加 protocol-type 维度——classic 组沿用
  现有 GroupCoordinator actor；consumer 组新建 ConsumerGroup actor（同
  actor 纪律：单任务独占、无锁）。OffsetCommit/OffsetFetch 双协议共用
  offset 存储（classic 路径直接写；consumer 组的心跳面提交走 KIP-447
  事务路径——T-M3.2 已就位）；
- TxnOffsetCommit 的组侧校验放宽边界不变（ADR-18 §10）。

## 5. 与 classic 的共存

组按创建时首个成员的 `group.protocol` 分型；两型互不可见（同名单独存在）。
混布 rebalance 收敛验收 = **同一 topic 的两个组**（一 classic 一 consumer）
各自收敛 + 消费不重不漏，而非同组混布（Kafka 4.x 亦不允许同组混协议）。

## 6. 测试与交付切分

- **a. 状态机+分配器 ✅（2026-09-17）**：consumer_group.rs（纯同步）+
  多组管理 actor（CGCmd::Heartbeat，分区数快照随命令携带）；consumer_group
  tests ×4 全绿（java 对拍/幂等续租/fence 三态/退订收回）。**块 b 已知
  接线要点**：①Assignment 响应的 TopicPartitions 需要 TopicId（uuid）——
  请求侧只带 TopicId，需路由表 by_id 反查 topic 名（RoutingTable 需补
  name_for(tid)）；②分区数快照经 MetaCmd::Lookup 携带（TopicMeta 有
  name/topic_id/partitions）；③fenced 映射 FencedMemberEpoch(82，需加
  ErrorCode+语义表行)、未知成员 UNKNOWN_MEMBER_ID(25)。ConsumerGroup actor（心跳循环/成员注册/
  Range 分配/member-epoch fence/差分下发）+ 确定性单测（分配对拍 java
  RangeAssignor 金样）；
- **b. 协议面 ✅（2026-09-18）**：68/69 v0 handler（server/src/handlers_consumer.rs）
  + 宣告（supported_versions 两行，v0 单档——v1 需 SubscribedTopicRegex/KIP-1082
  后置）+ conn.rs dispatch + Ctx.cg_tx（main.rs 每节点 spawn ConsumerGroups，
  与 classic 同拓扑）+ probe_consumer_group_layouts 字节级回归（注册/续租/
  fence 重同步/僵尸/离开接管/describe 全链，resp_decode 尾字节证明）。
  **三条接线要点的落地方式**：①`RoutingTable::name_for(tid)` 由名字哈希
  派生反查（topic_id_from 同函数，零新增状态面免三表同步失联；心跳 5s 档
  线性扫描可接受）；②分区数快照经 `MetaCmd::Lookup` 携带（TopicMeta 的
  name/topic_id/partitions 一次拿全——counts 进组状态机、topic_id 供
  assignment 回填；⚠ Lookup 传 `Some(vec![])` 恒定，None 是全量语义会把
  全集群 topic 灌进退订成员的组）；③fenced 三态映射：已知成员 epoch 不符
  → FENCED_MEMBER_EPOCH(82)/未知成员僵尸 → UNKNOWN_MEMBER_ID(25)
  （HeartbeatResult.unknown_member 显式判别位）/describe 未知组 →
  GROUP_ID_NOT_FOUND(69，新增枚举+语义表双锁行)。actor 失联（send/oneshot
  断）→ 15 可重试兜底。
- **c. 验收面**：e2e（python/franz-go 任一支持 KIP-848 的客户端）——
  混布收敛 + 增量 rebalance 无停等 + 双隔离级消费；
- **d. 规格化**：ConsumerGroup.tla 扩展 consumer 型状态机（无栅栏相位后
  C9 的 Stable 良构与 generation fencing 需按 member-epoch 重述）+
  阴性对照（stale-epoch 心跳被拒必须可检出）。

## 7. 已知边界与开放问题

- **客户端版本门槛（开放问题，块 c 前定案）**：KIP-848 客户端 GA =
  kafka-clients 4.0+（`group.protocol=consumer`）；仓库现有 java 档为
  3.7——需升级 maven 依赖或改用 franz-go（v1.21+ 已有早期支持）作验收
  客户端。倾向：franz-go 先行（工具链已就位），java 4.x 升级单列；
- SubscribedTopicRegex/Share groups 后置；成员关系驻内存（classic 同边界）；
- 服务端分配器只有 Range（Uniform/Sticky 随 T-M3.4）。
