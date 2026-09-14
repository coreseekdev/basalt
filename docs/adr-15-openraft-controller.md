# ADR-15：openraft 控制器多副本（T-M2.1 最后闭合项）设计

> 状态：设计定稿，排期待执行。M2 其余全部退出条件已达成（TASK.md 里程碑表）。
> 现役控制器：单写者（node0 进程内 actor）+ `__controller.log` 记录日志重放。
> 缺口：控制器进程故障 = 元数据不可变更（数据面多数派副本完好）。

## 1. 目标与非目标

目标：
- 控制器状态（ClusterState：brokers + assignments）经 Raft 复制到 3 节点；
  控制器进程故障后新 leader 在 ≤1 个心跳周期内选出，元数据读写恢复。
- 命令语义不变：Register/Heartbeat/CreateTopic/TransferLeader 仍为
  ControllerCmd 接口，内部 RPC 协议不变（broker 侧零改动）。

非目标：
- 数据面日志（partition log）不经 Raft——ADR-10 ISR 复制不变；
- 心跳/FetchSlice 等高频面不经 Raft（仅元数据变更经 Raft 一致化）；
- 观察者/learner 节点。

## 2. 结构映射

| openraft 概念 | basalt 落点 |
|---|---|
| `NodeId` | `i32`（broker id，type alias `CtrlNodeId = i32`）|
| `D::Entry` | `EntryPayload::Normal(Vec<ClusterRecord>)`——现有记录类型原样复用 |
| `D::Resonder`/StateMachine | `Controller` 现有 `apply_and_persist` 拆为 apply（纯内存）+ raft 接管持久化；`__controller.log` 退役，快照 = ClusterState serde |
| LogStorage | 新 `CtrlLogStore`：目录 `data/ctrl-raft/`（log + hard_state + 快照三件套，`openraft::storage::RaftLogStorage` + `RaftStateMachine` v0.9 trait 拆分实现）|
| Network | 内部 RPC 新消息 `MSG_RAFT`（透传 openraft `RaftNetwork` 帧：AppendEntries/Snapshot/Vote），走现有内部端口；目标地址查 `cfg.nodes` |
| 客户端入口 | `ControllerCmd` 全部改为 raft propose：leader 节点本地 propose；非 leader 经 MSG_RAFT 转发（client_forward）或返回 NotLeader + leader 提示（内部 RPC 客户端重试一次）|
| membership | 初始 3 节点 static（与 `cfg.nodes` 同源）；成员变更（broker 扩缩容）不进 raft membership——broker 存活信息保留在状态机数据里，raft membership 仅含 3 个控制器候选 |

## 3. 关键决策

1. **读写线性化点**：Heartbeat 高频写不进 raft——心跳维持现状（各自直达
   控制器 leader 的内存表）。代价：leader 切换时心跳表丢失，最长
   heartbeat_timeout 内旧 leader 可能被判活——由数据面 epoch fencing 兜底
   （租约语义，缺陷⑯已证其必要性），raft 只保证**元数据变更**的一致性。
2. **CreateTopic/TransferLeader 线性化**：propose 后等待本地 apply 返回
   （openraft `raft.client_write`），响应延迟 = raft commit RTT（局域网
   ~1-5ms，满足毫秒级交接）。
3. **failover_check 迁移**：stale 判定保留在 leader 节点本地（读内存心跳
   表），产生的 LeaderChange 记录改走 raft propose——多控制器并存时的
   双主指派由 raft 唯一提交点消除。
4. **迁移路径**：单控制器 → openraft 的升级接受**全量重启**（v1 静态集群；
   旧 `__controller.log` 一次性转成 raft snapshot 导入）。
5. **测试策略**：MockRaftClient 式单测（T-M2.1 验收）= 对 `CtrlLogStore`
   的崩溃重放一致性与 3 节点内嵌 raft 集群的选举/提交/切主单测；多节点
   e2e 增加"杀控制器节点 → 元数据服务恢复 <2s"场景（复用 L1 度量）。

## 4. 工作量与排期（建议三段）

1. **raft 骨架（~1 天）**：openraft 0.9 依赖 + type config + CtrlLogStore +
   三节点内嵌集群单测（选举/日志复制/重启恢复）。
2. **控制器改造（~1 天）**：ControllerCmd → propose；MSG_RAFT 网络；
   快照导入迁移工具；Controller 单测迁到 raft 语义。
3. **场景回归（半天）**：multinode 全场景（failover/L1/transfer/partition/
   bounce/replay）× 控制器 kill 注入新场景；账本 C12/CI 收口。

## 5. 风险

- openraft API 版本漂移快（0.9 → 0.13 破坏性变更多）：锁定 0.9.x 并在
  README 记录（同 Verus 版本纪律）。
- 心跳表不进 raft（决策 1）带来的"死区"依赖 fencing 兜底——缺陷⑯的
  规约模型（租约）是该项正确性的规约依据，实现须对齐。
