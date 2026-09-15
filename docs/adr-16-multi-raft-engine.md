# ADR-16：控制器 Raft 多引擎支持（openraft / raft-rs 可插拔）

> 状态：已实施（openraft 骨架 + raft-rs 引擎 + 双测试 + 三进程 runtime 联调；2026-09-14）。
> **runtime 联调**：`BASALT_CTRL_RAFT_ENGINE=raftrs` 下三节点引擎组选举、
> CreateTopic 经 raft propose 复制到全部节点（multinode_raftrs.py 30/30），
> 默认模式五场景回归不受影响。
> **raft-rs 实施关键发现**：0.7 的出站消息分三类暴露——非 leader 走
> `ready.take_messages()`（先发，不依赖落盘）、leader 走
> `ready.persisted_messages()`（持久化后发）、`advance(ready)` 返回的
> LightReady 也带增量。只读 `ready.messages()` 对非 leader 恒为空
> （is_persisted_msg 门控）——这就是首轮集成选举全挂的根因。
> 关联：ADR-15（openraft 控制器设计）。
> 动机：raft 引擎是长周期基础设施选型——openraft（社区活跃、API 演进快）
> 与 raft-rs（TiKV 生产验证、驱动式 API、演进缓慢）各有成熟用户。
> 单一绑定 = 把控制器 HA 的正确性押在单一外部项目的演进上。

## 1. 引擎中立抽象

basalt 侧定义 `CtrlRaftEngine` trait，控制器 actor 只面向该接口：

```rust
#[async_trait?]  // 不用 async_trait——返回 impl/通道化避免动态分发问题
pub trait CtrlRaftEngine: Send {
    fn id(&self) -> i32;
    /// 当前是否有可用 leader（含自身）
    fn leader_known(&self) -> Option<i32>;
    /// 提交一条元数据变更（等待多数派 commit + apply 完成）
    async fn propose(&mut self, rec: ClusterRecord) -> Result<(), EngineError>;
    /// 失败检测触发的选举（watchdog 模式，两引擎语义对齐）
    async fn trigger_elect(&mut self);
    /// 测试/断言：已应用的日志 index
    fn applied_index(&self) -> u64;
    async fn shutdown(self);
}
```

状态机与持久化各引擎自持，但**状态语义共享**：
- apply 单元都是 `ClusterRecord`（复用 `ClusterState::apply`）；
- 快照 = `ClusterState` 的 serde 序列化；
- 两引擎的存储目录分离（`ctrl-openraft/`、`ctrl-raftrs/`），跨引擎迁移 =
  快照导入（一次性全量重启，同 ADR-15 §3.4）。

## 2. 引擎对照

| 维度 | openraft 0.9 | raft-rs 0.7（TiKV） |
|---|---|---|
| 模式 | 自驱事件循环（Raft 句柄 + propose 等待 commit） | **驱动式**：集成方 tick() + 处理 Ready + advance() |
| 存储 | v2 trait（0.9.25 封印，走经典 RaftStorage + Adaptor）| 同步 `Storage` trait（raft-rs 内置 MemStorage 可起步）|
| 网络 | RaftNetwork trait（async）| Ready.messages 取出后自行发送（同步消息类型）|
| 演进 | 活跃、0.9→0.10 破坏性大 | 平稳（TiKV 生产锚定）|
| 已知问题 | ~~tick 驱动选举未生效~~ ✅ 已解决：根因是消息流 bug（非 tick），修复后 tick 选举完美工作 | tick 节奏由集成方负责（时钟漂移/批次处理需自查）|
| 选型建议 | 默认引擎 | 备选引擎；TiKV 生态一致性偏好者 |

## 3. 选择与共存策略

- 运行时选择：`BASALT_CTRL_RAFT_ENGINE=openraft|raftrs`（默认 openraft，
  与 ADR-15 第一段一致）。
- 两引擎各自目录、各自测试；不承诺跨引擎迁移（raft 日志格式引擎私有，
  迁移走快照导入）。
- 引擎新增（如自研）：实现 trait + 测试即可，控制器 actor 不改。

## 4. 测试对齐

两引擎共享同一验收测试形态（内嵌三节点、通道网络）：
选举 → propose ×N → 多数派收敛 → 杀 leader → 选举/续写 → 存储重放/快照恢复。
断言口径一致，使引擎可横向比较。
