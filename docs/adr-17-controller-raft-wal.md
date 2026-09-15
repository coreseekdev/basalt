# ADR-17：控制器 raft WAL（写穿持久化）

- 状态：已实现（raftrs 引擎；2026-09-15）
- 依据：`docs/research/dendro-WAL与TiDBX对照调研.md`（dendro SPEC 02 移植子集）

## 1. 背景

raftrs 引擎的 raft 日志原本是纯内存（MemStorage）：控制器 propose 的
ack 点落在内存，多数派同停机会丢最近 2s 快照窗口内已 ack 的元数据变更；
重启节点依赖 leader 重放追赶（引擎缺陷簇 ㉗㉘㉙㉛㉜ 的共同根源）。

对照 TiDB X"Raft log 先落本地盘才 ack"的最低线与 dendro WAL 的成熟设计，
本轮落地写穿持久化。

## 2. 设计

`server/src/ctrl_raft/wal_log.rs`：

- **WalStorage = MemStorage（服务层）+ 写穿 WAL（持久层）**，实现
  raft-rs `Storage` trait（委托），driver 经 `ReadyPersist` trait 统一
  persist 调用（MemStorage/WalStorage 双实现，泛型路径共享）；
- **帧头 32B**（LE）：magic("BRWL") + version + ftype(HARDSTATE|ENTRY) +
  term + index + len + crc32c（只覆盖 payload，dendro 同款）；
- **段**：`{data_dir}/ctrl-raft/wal/{seg:020}.wal`，append + sync_data 每
  批一次，64MB 轮转；控制器元数据低频，不需要组提交；
- **恢复**：段序升序重放进 MemStorageCore——`append` 的冲突裁剪语义使
  跨生命周期混段重放安全；末段撕裂截到最后合法帧边界后续写（SQLite/PG
  同语义）；非末段腐坏 = 放弃重放、内存起步靠 leader 重发（不拒绝启动，
  文件保留取证）；
- **毒化**：IO 失败置位后 persist 跳过 WAL（内存降级 + ERROR 一次），恢复
  = 进程重启。POC 取舍：可用性优先；生产答案 = dendro 式拒绝写（40003
  completion_unknown 同构语义）；
- **快照文件降级为状态机种子**：state.json/meta 仍加载（shared + committed
  交付游标 cfg.applied），但正确性不再依赖——快照缺失时 committed 条目
  从 WAL 全量重投递，状态机从空重建（`restore_rebuilds_state_from_wal_
  without_snapshot` 测试锁定）；
- **campaign 门控升级**：`restore_meta.is_none() && !store.has_history()`
  ——有 raft 历史（WAL 或快照）的重 join 节点不主动 campaign。

## 3. 与 C1/账本义务的关系

修复 ㉗ 家族的根：无 WAL 重启的 commit 越界 panic、追赶窗口、僵尸防护的
触发面。双 catch_unwind 安全网保留为纵深防御。

## 4. 已知边界

- WAL 无 GC（快照截断基点留待后续；控制器元数据量级低频，暂不增长）；
- 毒化降级而非拒绝写（见上）；
- CONF 不持久化（静态成员每 boot 从 cfg 重设）。

## 5. 验证

- wal_log 单测 ×3：roundtrip 重放 / 撕裂截尾续写 / 跨生命周期冲突重放；
- restore_tests ×2：快照+WAL 联合恢复；无快照仅 WAL 重建状态机；
- 引擎 e2e：raftrs / ctrl_kill / bounce 全绿；failover 见账本 ㉟
  （acks-all × failover 竞态，非本 ADR 引入，时序漂移显形）。

## 6. 后记（同日）

ISR 收缩状态机 + leader reconciliation 已落地（账本 ㉟ 完整修复）：控制器
WAL 的时序抖动曾是 ㉟ 竞态的显形条件，reconciliation 就任拉齐使 failover
轮转对副本数据状态不再敏感——两机制互补闭环。
