# Share Groups（KIP-932）评估纪要

日期：2026-09-18　状态：评估完成 → **建议立项**（v2 序列，spike 先行）
触发条件核查：HANDOFF 记「触发条件 = librdkafka 支持落地」——**已达成**（见 §2）。

## 0. 结论

**建议立项**，规模 ≈ T-M3.3（4-5 会话），验收面 = franz-go `ShareGroup` +
`Record.Ack()` e2e（1 会话内可出 spike 结论）。理由：

1. **客户端面全部就绪**（原条件触发项）——三档均可用作验收与判别面；
2. **与 848 基建复用度高**：ShareGroupHeartbeat(76) 与 ConsumerGroupHeartbeat(68)
   同构（服务端分配 + member epoch fencing + session timeout），CG848 actor
   状态机可直接改造复用；
3. **语义风险集中在单点**：per-record ack 状态机（§3.2），其余均为已知形。

## 1. KIP-932 协议面（vendored schema 精读，Kafka 4.5 definition）

### 1.1 客户端 API（对客户端宣告面）

| API | Key | 版本 | 要点 |
|---|---|---|---|
| ShareGroupHeartbeat | 76 | v1 | GroupId/MemberId/MemberEpoch/RackId/SubscribedTopicNames——与 68 逐字段同构；差分下发、epoch fence、session timeout 全部同语义 |
| ShareFetch | 78 | v1-2 | Fetch 变体：以 (GroupId, MemberId, ShareSessionEpoch) 寻址；TopicId 寻址；delivery state 由服务端在取数时推进 |
| ShareAcknowledge | 79 | v1-2 | per-(topicId, partition) → AcknowledgementBatches{FirstOffset, LastOffset, AcknowledgeTypes[]int8}——**区间化 per-record 确认**；ShareSessionEpoch 同 fetch session 语义 |

确认类型三值（librdkafka 符号表双源核对）：`ACCEPT(1) / RELEASE(2) / REJECT(3)`。

### 1.2 broker↔share-coordinator 内部 RPC（不对外宣告）

InitializeShareGroupState / ReadShareGroupState / ReadShareGroupStateSummary /
WriteShareGroupState / DeleteShareGroupState——share coordinator 是**独立于
组协调器**的服务，状态持久化于内部 topic（Kafka 为 `__share_group_state`）。
basalt 落地可折衷：状态伴生在分区 actor + append-only share-state log（§3.2），
无需独立协调器服务（单机/多节点 POC 拓扑下与事务协调器同款职权面）。

### 1.3 核心语义（与经典消费组的本质差异）

- **无 offset commit**：消费位置 = 最小未终结记录（ack 状态推导），位点管理消失；
- **per-record 交付状态机**：Available → Acquired（交付即获取，带锁）→
  Acknowledged（Accept）/ Available（Release，重投递）/ Archived（Reject，
  丢弃）；**delivery attempt limit**（默认 5）内 Release 才重投，超限不再交付；
- **record lock**：Acquired 有锁期限（librdkafka 文档内嵌双源：
  `group.share.record.lock.duration.ms` 默认 30s），到期未确认自动 Release——
  崩溃消费者的在途记录自愈，无需 session 踢除兜底；
- **事务面**：share fetch 仅 read_uncommitted（KIP-932 明确不支持事务读取）。

## 2. 客户端支持度矩阵（本环境实测）

| 栈 | 版本 | 支持证据 | 接入面 |
|---|---|---|---|
| librdkafka | 2.15.0（confluent_kafka 2.15） | `SHARE_ACKNOWLEDGE_TYPE_{ACCEPT,RELEASE,REJECT}` + Share consumer 生命周期符号（so 字符串） | share consumer 模式（lock duration 为 broker 侧配置，客户端零配置） |
| kafka-clients | 3.9.1 | `ShareConsumer`/`ShareConsumeRequestManager`/`MockShareConsumer`（早鸟版；GA 于 4.0） | `KafkaShareConsumer`，group.protocol=share |
| franz-go | v1.21.6 | `consumer_share.go` + share_test.go | `kgo.ShareGroup(group)` + 逐条 `Record.Ack()`（显式确认 API，验收面最强） |

三档 opt-in 门槛面（对照 848 的 should848 教训）：franz-go ShareGroup 是显式
opt-in；kafka-clients 4.0 的 share 消费者需 `group.protocol=share`——**服务端
宣告 76/78/79 前客户端不会误入**，无 848 式「静默回退」陷阱，但宣告完备性
仍是 e2e 前置。

## 3. basalt 落地面

### 3.1 复用 848 基建（低成本项）

- ShareGroupHeartbeat(76) handler：字段面与 68 同构——差分下发、三值 fence
  （82/25）、session timeout sweep、静态成员、assignment interval 节流全部
  平移；uniform/range 分配器复用（无 offset 感知，纯分区分配）；
- 宣告面：76/78/79 加入 supported_versions（契约同款：只宣告已实现）；
- 字节级回归：probe_share_layouts（73-79 全族，nullable struct 语义已备——
  账本 51 的原语已在 codec 层）。

### 3.2 新增：分区侧 ack 状态机（核心成本项）

状态载体按 (group, topic, partition)：区间化存储（AcknowledgementBatches 天然
区间语义；Morax §6 的 INT8RANGE[] 对照在此收敛——区间数组而非逐记录位图，
压缩率与 Acknowledge 请求同构）：

```
struct ShareState {
  acquired: Vec<(first, last, member, lock_deadline)>,  // 在途锁
  delivered_counts: Vec<(first, last, count)>,          // 交付计数
  finished_upto: i64,                                    // 连续 Acknowledged 水位
  archived: Vec<(first, last)>,                          // Reject（不再交付）
}
```

- ShareFetch：Available 区间内交付 → 写 Acquired（锁=now+30s 配置）；
  超交付上限的区间跳过；
- ShareAcknowledge：Accept→并入 finished_upto 推进；Release→回 Available
  且 count+1；Reject→Archived；
- 锁到期 sweep：Acquired 过期自动 Release（复用 actor deadline 框架——
  与 parked_acks/txn_open 同款纪律）；
- 持久化：append-only share-state log（每分区目录旁路，同 classic 组位点
  日志形态），启动重放 + finished_upto 前截断 GC；崩溃恢复 = log 重放 +
  在途 Acquired 按 deadline 过期自愈（无脏状态）。

### 3.3 与既有面的共存边界

- 组类型命名空间隔离：classic / consumer(848) / share 三类型互不可见
  （DescribeGroups/ListGroups/ConsumerGroupDescribe 按 type 过滤；share 组
  走 ShareGroupDescribe(77)？——schema 在位，宣告面随 v1 一并定）；
- member epoch / generation 空间按组隔离，无跨类型交互；
- txn 交互：share fetch 恒 read_uncommitted（LSO 锚对 share 消费者不生效，
  需在 cap_for 显式短路）——与 ADR-18 §4.2 无冲突，需测试锁定；
- 配额（T-M4.2）：share fetch 计入 fetch 字节配额（同一 token bucket）。

## 4. 规模估算与分期

| 块 | 内容 | 规模 |
|---|---|---|
| a | 76 handler + 组 actor 改造 + 宣告 + 布局回归 | 1 会话 |
| b | ShareFetch/Acknowledge + ack 状态机 + 锁 sweep | 1-2 会话 |
| c | share-state log 持久化 + 恢复 + GC | 1 会话 |
| d | 三客户端 e2e（franz-go 验收主面）+ 矩阵 | 1 会话 |
| e | TLA+（ShareAck 状态机：无丢失/无重复交付/锁自愈）——按 TransactionCommit 纪律 | 0.5-1 会话 |

**spike 建议（先行，1 会话）**：块 a 最小面 + franz-go ShareGroup 连真
broker 拉通一轮 heartbeat/fetch/ack——验证协议面认知（尤其 ShareFetch 的
session epoch 与增量语义），再决定 b/c 细节。若 spike 揭示 schema 外的隐含
约束（如 ack 顺序要求），回到本纪要修订。

## 5. 风险

1. **ShareFetch session 语义**（增量 vs 全量、ShareSessionEpoch 边界）——
   schema 注释稀疏，spike 面向真客户端是唯一权威（848 账本 51 同教训：
   自 round-trip ≠ 互操作）；
2. ack 状态机的区间合并/分裂边界（Accept 部分区间覆盖 Acquired 区间）——
   单测矩阵 + TLA+ 块 e 兜底；
3. 多节点 failover 时 share-state log 与分区主从切换的一致性——v1 先
   单节点面（与 tiered v1 同策略），failover 面留 v2。
