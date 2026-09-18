# ADR-20：cooperative-sticky rebalance（T-M3.4）

- 状态：✅ 全部落地（2026-09-18，`d6d6dd6`；T-M3.4 闭合）
- 依据：TASK.md T-M3.4（验收：franz-go/rdkafka cooperative 模式跑通且无
  停顿式双全量 rebalance）；KIP-429（classic 增量协作）；KIP-848 §服务端
  分配器（uniform/range）；生态 §C。
- 前置：T-M3.3 ✅（ADR-19）。

## 1. 目标与非目标

**目标**：增量 partition 交接——成员变更只移动必须移动的分区（"只在必须
移动时移动"），协作路径无停顿式双全量 rebalance。两条面：
- **classic 路径（KIP-429）**：cooperative-sticky 协议组，客户端 leader
  分配、增量撤销/获取；服务端只负责**正确的协议选择与中继**；
- **848 路径**：服务端分配器扩展——ServerAssignor 协商（range/uniform）
  + uniform 分配器的 movement-minimizing 实现（franz-go stickyBalancer
  请求 "uniform"）。

**非目标（v2 边界）**：rdkafka（librdkafka）cooperative 档（工具链支持
面另列，franz-go 先行为 M3.2/M3.3 既定模式）；848 sticky 独立命名（Kafka
4.x 服务端亦只有 range/uniform，movement-minimizing 在 uniform 内实现）；
跨代混布（同组 range 与 cooperative-sticky 混协议——Kafka 不允许）。

## 2. classic 侧：协议选择面（现状缺陷与修法）

现状：`complete_rebalance` 硬编码 `protocol = "range"`（lib.rs）。任何非
range 协议组（cooperative-sticky 直接中招）的 JoinGroup 应答都回错误的
协议名——客户端校验即败。

修法（Kafka 语义）：
- `Member` 增记 `protocol_names: Vec<String>`（请求的协议名序，保偏好序）；
- 选协议 = **leader 偏好序 ∩ 全体成员支持集** 的第一个；空交集 = 当前
  兜底（InconsistentGroupProtocol=23 语义，POC 保持 range 兜底 + 注释）；
- 其余流程零改动：多轮 JoinGroup/SyncGroup 中继、generation fencing 均
  协议无关（cooperative 的两轮 join/sync 波在服务端即普通的两次 rebalance）。

## 3. 848 侧：ServerAssignor 协商 + uniform 分配器

- **协商**：心跳请求 `ServerAssignor`（nullable = 未变）。组创建时首个
  非空请求定组分配器（first-wins，后续不同请求忽略——POC 边界，注释）；
  请求值 ∉ {range, uniform} → `UNSUPPORTED_ASSIGNOR(57)` 直接拒绝
  （错误码入枚举 + 语义表双锁）。
- **range 分配器**：原样保留（per-topic 连续切块）。
- **uniform 分配器（movement-minimizing）**：
  1. 全局分区清单 = 订阅成员的 topic 并集 × 分区（按 (topic, partition)
     字典序——BTreeMap 稳定序）；
  2. 公平份额：n_i = total/M，前 r 位成员（按 id 稳定序）+1；
  3. **保留段**：按成员序，各成员在份额内保留现有 assignment 中仍订阅
     的分区（不受影响成员零移动——增量交接的核心性质）；
  4. **补派段**：余下分区按序轮派给未满份额成员。
  与经典 spec 的关系：ADR-19 §3「sticky 性质由只在必须移动时移动的差分
  实现兑现」。

## 4. 验收面（全部 ✅ 2026-09-18）

- **a. 848 分配器 ✅**：确定性单测金样 ×2（3 分区 [2,1] → C 加入：A/B
  保留、C 补派——与 range 的行为分叉点即金样断言；C 离开回归原分配；
  多 topic 场景订阅约束优先于份额）+ 布局回归（uniform 组协商 /
  describe AssignorName 同源 / 未知 assignor 112 且不建组）。
  **实现要点（评审中补强）**：保留段按「topic 订阅者数升序」排序——否则
  唯一订阅 topic 会被泛订阅 topic 挤出而饿死唯一订阅者；补派段带订阅
  过滤 + 全满时任一订阅者兜底（订阅约束优先于份额）。错误码 112 经
  kerr 双源核对（**非 57**——凭记忆错号被语义表纪律拦下）。
- **b. classic 协议选择 ✅**：单测探针（cooperative-sticky 组选中该协议
  名；第二成员偏好序不同不漂移；leader 保持；generation 推进）。
  **过程中抓到协调器缺陷（账本 52）**：rebalance 中途组清空时
  pending_joins/pending_syncs 悬挂（oneshot 永不答复）——协作探针挂死
  实证。修复：组→Empty 双路径回可重试 27 + maybe_complete 只数在组
  pending + complete_rebalance 对 stale pending 回 27 + 探针 deadline
  分支回归锁定。
- **c. e2e ✅**：testing/franzgo/coop（run_franzgo_coop.sh）——franz-go
  cooperative-sticky 经典路径（无 848 opt-in）：B 中流加入，A 不断流
  （窗口内最大间隙 501ms）、B 分得增量（15 条）、全组 60/60 不重不漏；
  四套 e2e（kafkaclients/franzgo/848/coop）全 PASS 零回归。
- d. TASK 验收面即 e2e，规格化不另设（848 分配器确定性单测即行为锁定）；
  rdkafka（librdkafka）cooperative 档列后续（§1 非目标）。

## 5. 已知边界

- 组分配器 first-wins：后加入成员请求不同 assignor 被忽略（Kafka 同为
  组级固定，差异在拒绝语义——POC 不做协商冲突报错）；
- uniform 的成员序 = MemberId 字典序（BTreeMap），与实现一致；
- classic cooperative 的撤销超时/rejoin 超时信任客户端 session/rebalance
  timeout（既有机制）。
