# 12 · 形式化验证体系（basalt）

> 本文是 basalt 形式化验证的总纲：**验证什么、用什么验、怎么与代码同步、怎么作为对用户的信任依据**。
> 它落实 TASK.md T-Q 的 L8 层，与 [测试策略 §2](../../docs/11-testing-strategy.md) 的测试金字塔衔接——
> 测试回答"跑过的没坏"，验证回答"**没跑到的也不坏**"。vibe coding（AI 生成代码）状态下，
> 用户信任的对象不是某次 code review，而是本仓库中**机器检查过的规约与证明**。

---

## 0. 结论先行

1. **核心全功能正确性证明 + 外壳分层证据**。可证明核心 ≈ 4.5k 行（record / storage / coordinator / metadata / partition 复制逻辑），逐条规约证明；外壳（tokio、conn.rs、真实 fs、协议 codegen）永久停留在"仿真 + 崩溃矩阵 + 客户端对拍"证据层，在账本（§7）中如实标注。
2. **代码层主用 Verus，按任务切换 Creusot**（§4 切换协议）；**设计层 PlusCal/TLC，规模化用 Apalache**，Quint 在 M2 精化阶段再评估。
3. **故障模型单一事实源**：`storage/src/sim_disk.rs` 的语义（pending/sync/crash/torn write/ENOSPC）同时喂给 TLA+ 环境动作、opfuzz、turmoil 仿真和 Verus 的 recover 前置条件。改故障模型 = 设计变更。
4. **proof-gated contributions**：AI/人改核心代码，PR 由证明把关；人只审规约与不变式。

## 1. 核心 / 外壳边界（TCB 清单）

| 组件 | 归属 | 信任来源 |
|---|---|---|
| `record/` 编解码（zigzag/varint、批头） | **证明核心** | Verus/Creusot 全功能证明（本次已起步） |
| `storage/`（segment/index/checkpoint/recover） | **证明核心** | Verus 崩溃一致性证明（Sisyphus 方法论）+ T-Q.1 opfuzz |
| `coordinator/`（消费组状态机、offset 日志） | **证明核心** | PlusCal 模型 + Verus 状态机证明 |
| partition actor（ISR 复制、epoch fencing、三水位） | **证明核心** | PlusCal（ADR-10）+ Verus ghost 精化 |
| `metadata/`（image/Delta 重放） | **证明核心** | 重放确定性证明（快照+日志重放 = 全量重放） |
| 共识（元数据层 openraft） | **外包** | ADR-2：形式化负担由上游承担，本地只验适配层 |
| `server/src/conn.rs`、tokio 运行时 | 外壳 | turmoil 仿真（L2 测试）+ 混沌（L6） |
| `storage/src/disk.rs` 真实 fs 后端 | 外壳 | DiskIo trait 规约 + 崩溃矩阵测试（测试 §4） |
| `protocol/build.rs` 生成代码 | 外壳 | 验证生成器构造性质 + 客户端对拍（测试 §5） |
| 兼容语义（与 librdkafka/franz-go 行为一致） | 外壳 | report card 对拍——正确性由外部定义的部分不可证明，只能对拍收敛 |

## 2. 信任阶梯（每层独立交付，顶层是全功能证明）

| 层 | 对象 | 工具 | 证据形态 | CI 位置 |
|---|---|---|---|---|
| L0 语言安全 | 全部 | `unsafe_code=deny`、clippy disallowed-types（已有） | 类型系统健全性 | 每次提交 |
| L1 无 panic / 无 UB | 核心全部 | Kani、Miri | 有界穷举：任意输入不 panic | PR（触达 crate）/ nightly 全量 |
| L2 设计正确 | 复制 / 选举 / rebalance 协议 | PlusCal + TLC（Apalache 扫大参数） | 模型检查通过 | 设计变更时 + nightly 回归 |
| L3 组件全功能正确 | record → storage → 水位/epoch → actor → coordinator | **Verus（主）/ Creusot（切换）** | 机器检查的函数级规约+证明 | PR（触达 crate） |
| L4 系统精化 | 抽象协议 ↔ 实现状态机 | ghost 层重表达 + 对应表 | 精化对应表（v2 机器检查） | 随 L3 |
| L5 语义层对外发布（远期可选） | 协议语义本身 | Lean/Rocq（Cedar 模式） | 独立可审计的形式化语义 | 不设 |

**唯一残余的诚实声明**：tokio 异步、网络、真实 fs 不在任何证明工具表达范围内——由外壳证据层兜住，账本里逐行标注。

## 3. 故障模型（单一事实源）

权威定义 = `storage/src/sim_disk.rs` + 本文，四处消费方必须同步：

```
写路径:   write(f, bytes)  → 进入 pending（未持久化）
持久化:   sync(f)          → pending 原子落为 committed（无 torn 时）
崩溃:     crash()          → 全部 pending 丢弃，committed 保留
torn write: sync 时按概率只落前半块（block 粒度截断）
资源:     第 N 次写起返回 ENOSPC；操作可按概率失败
静默损坏: 读回可与写入不一致，CRC 必须检出（对齐 Kafka recoverSegment）
```

| 消费方 | 用法 |
|---|---|
| TLA+ 环境动作（`spec/`） | `Crash`/`SyncBeforeCrash` 建模为"已 sync 数据存活，未 sync 数据消失" |
| T-Q.1 opfuzz | 随机交错 append/flush/roll/truncate/recover，`recover()` 恒合法 |
| turmoil-fs（L2 仿真） | `block_size`（torn write）、`crash()`、ENOSPC —— 语义同上 |
| Verus storage 证明 | `recover` 的前置条件 = "输入是任意符合本模型的操作序列后的盘面" |
**持久化点契约（ADR-14，I/O 实现无关）**：
- `DiskIo::append` = 写边界（StdDisk：page cache；未来 DirectDisk：设备写）；
- `DiskIo::sync_file` = 持久边界（fsync / FLUSH CACHE）；
- Log 保证"无 ack 而未写文件"：batch_io 窗口应答在 flush 后统一发放（deferred_produce）；
- `sync()` 先排空 staging 再 fsync——**sync 即持久**，禁止调用顺序陷阱；
- 该契约对缓冲写与 O_DIRECT 同构（SimDisk pending/committed ≡ 设备易失/非易失），direct IO 不得破坏（M4）。
| log.rs truncate_to/delete_records | kept_end 批对齐、log_start 推进（§12 ②③⑤） |

**变更纪律**：改 `sim_disk.rs` 语义 → 必须同 PR 更新本节、TLA+ 环境动作、opfuzz 参数，并重跑 TLC（设计变更门禁）。

## 4. 工具链与切换协议（2026-09 选型，随生态演进复审）

### 4.1 代码层：Verus 主、Creusot 切换

| | Verus | Creusot |
|---|---|---|
| 后端 | Z3（SMT），自动化强，vstd 规约库厚 | Why3→Coma 多求解器 + **Coq 逃生舱** |
| 规约 | Rust 内嵌（ghost/tracked/权限），partition actor 的所有权推理需要它 | Pearlite（对工程师最自然的 DSL），trait laws |
| 先例 | **Sisyphus（日志结构存储引擎，与 storage/ 同构）**、AWS 用它验证 Rust std、AI 辅助证明生态（KVerus 等） | 中小颗粒案例，POPL 2026 tutorial |
| 工具链 | 自带 rustc fork + verus-analyzer | cargo 集成更好（`cargo creusot prove`） |

**默认 Verus**；**切到 Creusot 的触发条件**（满足其一）：
1. 目标代码在不重构的前提下需要原地验证（Creusot 直接跑 cargo crate，Verus 通常要求改写为 owned/ghost 风格）；
2. Z3 在非线性目标上卡死且 goal 适合 Coq 手工证（如 delta 编码的取模/整除推理）；
3. record 切片 bake-off 数据显示某类任务 Creusot 迭代显著更快。

**共同纪律**：工具版本锁进账本；证明不允许 delete-to-pass；规约变更必须在 PR 描述中声明。

### 4.2 设计层：PlusCal/TLC 为主

- v1 用 PlusCal/TLC（T-Q.2）：活性（rebalance 收敛）目前只有 TLC 的公平性机制完整支持；工业语料大，AI 辅助写 spec 效率高；同仓库 walrus `DistributedWalrus.tla` 先例。
- 状态空间爆掉时用 **Apalache** 做符号化归纳不变式检查与参数扫描。
- **Quint** 在 M2 精化阶段评估：它的类型化代数数据类型与 Verus ghost struct 映射最自然，且可编译为测试预言；若届时对应表方案（§5）够用则跳过。

## 5. 精化桥（L4）协议

抽象规约和实现证明必须是同一个形式化对象，否则就是两个各说各话的模型：

1. **协议定稿**：PlusCal 模型 TLC 全绿后冻结（git tag），此后改动按设计变更走。
2. **ghost 重表达**：把冻结的抽象状态机用 Verus ghost 类型重写为实现证明的抽象层（TLA+ 降级为探索工具）。
3. **对应表**：过渡期维护三方链接表（TLA+ 动作/不变式 ↔ Verus 规约 ↔ 代码位置），格式见 `spec/README.md`；任何一侧改动必须同步另两侧，PR 模板勾选项。
4. **规约即类型**：不变式是编译进代码的，改实现必须同步过规约——这是 proof-gated 的机制本体。

## 6. CI 分层

| 触发 | 内容 |
|---|---|
| PR | 现状（clippy+test+proptest）+ 触达 crate 的 Verus/Creusot 证明 |
| nightly | Kani 全量核心 crate + TLC 固定小模型回归 |
| 设计变更 | TLC 全参数扫描 + Apalache |
| 每周 | turmoil 种子扫描（T-Q.4）、混沌长跑 |

## 7. 验证账本（对用户的信任面）

> 用户信任这张表，而不是某次 code review。每行：承诺 → 层级 → 工具版本 → 状态 → 复验命令。

| # | Claim（对外承诺） | 层 | 工具/版本 | 状态 | 复验 |
|---|---|---|---|---|---|
| C1 | 不丢：ack 数据存在于任一多数派（多数派持久性），控制器现任主一旦可服务必持有全部 acked 数据 | L2 | PlusCal/TLC (TLC2 2026.09.09.014814)，`spec/BasaltDataPlane.tla` | ✅ v0.1 全空间通过（3 节点×日志长 2×2 值，8890 万状态）；**SplitBrain 场景同样通过**（控制器 fencing 失效下 follower 侧 epoch fencing + 继任规则自足）。**适用条款**（不变式评审 P0-1）：结论以 acks=all 提交 = ack ≥ 多数派且 `unclean.leader.election=false`（ADR-8）为前提；ISR 收缩/扩张与事务未建模（见 VERIFICATION-GUIDE 未建模边界清单），M2 实现必须钉死该提交条件 | `make -C spec check` |
| C2 | 单写者：每 epoch 至多一个有效 leader；日志匹配（同 index 同 epoch 同值）；view epoch 不超前。注：InvOneLeaderPerEpoch/InvViewEpochSane 为机制锁性质（结构性，MUT-A 可红），承载结论的是 InvLogMatching + C1a/C1b | L2 | 同上 | ✅ v0.1 通过（v0.2 注释标注） | `make -C spec check` |
| C3 | 继任规则（(lastEpoch, len) 字典序最大者接管）是 C1 的**必要设计**：跳过它（EagerLeader）TLC 检出"新主缺 acked 数据"反例 | L2 | 同上 | ✅ 反例已检出 | `make -C spec demo-eager` |
| C4 | 游标有界：消费者只读已提交前缀，且每条可从任一多数派恢复。注：v0.2 前 InvConsumedBounded 曾结构性恒真（consume 直读 committed 变量，评审 MUT-B 判别实证）；v0.2 改为向现任主日志 fetch——不变式现依赖 C1b+日志匹配，MUT-B 形态（脏主）可红；InvConsumedOnLeader 为推理闭包（P1-2，降级标注） | L2 | 同上 | ✅ v0.2 通过（全空间 1200 万状态，2026-09-10） | `make -C spec check` |
| C4' | 提交时多数派视图校验（CommitChecksEpoch）对安全性**非必需**（可作纵深防御保留） | L2 | 同上 | ✅ 实验确认（splitbrain / splitbrain-cepoch 双绿） | `make -C spec splitbrain` |
| C5 | zigzag 双射：crate 位运算原型与算术规约逐点相等；`roundtrip_crate(v)==v` 对全部 i64 | L3 | Verus 0.2026.09.09.f42e59f，`verification/verus/record_core.rs` | ✅ 18 verified, 0 errors | 见 verification/verus/README.md |
| C6a | Compression::from_bits/bits 全函数正确性与往返（全 i16 / 全枚举值） | L3 | 同上 | ✅ | 同上 |
| C6 | varint（LEB128）无损：`put_varint`/`get_varint` 往返 == Some(z)（全 u64）、≤10 字节上界、规范形式（续传位/终止位） | L3 | Verus 0.2026.09.09.f42e59f | ✅ 38 verified, 0 errors（2026-09-09） | `verus --crate-type=lib verification/verus/record_core.rs` |
| C7' | **opfuzz 累计发现并修复 7 个真实缺陷**：① SimDisk::len 只返回 pending（crash 后簿记失真）；② truncate_to 盲设 next_offset 与批边界错位（重启 offset 重排）；③ truncate_to_front 保留/删除颠倒（delete_records 销毁 ≥ offset 全部数据）；④ roll() 空段封存创建同路径双 Segment（删一炸二）；⑤ segment_for 忽略 active 段（多段日志 fetch active 尾部返回旧数据/空——P0 服务端读路径缺陷）。opfuzz 80 种子全绿（四档含 batch_io）、解除 ignore 转正；⑥ segment_for 忽略 active 段（多段日志 fetch active 尾部返回旧数据/空——P0 读路径，opfuzz 跨段循环读抓出）；⑦ truncate_to 扫描起点错用 locate(offset) 跳过应保留批（opfuzz batch_io 档抓出，已修为段首扫描）| L1 | cargo test opfuzz（clean 40 + chaos 20 + batch_io 20 种子） | ✅ 收官（2026-09-10，7 缺陷全修，T-Q.1 ✅，batch_io 档转正）
| C7'a | **恢复安全性模型 Verus 15/15 全绿**：设备 D1-D4 + scan_valid/scan_len_bounded + synced_survive + intact_prefix_scanned | L3 | Verus 0.2026.09.09 | ✅ 2026-09-10 | `verus --crate-type=lib verification/verus/log_recovery_model.rs` | | `cargo test -p basalt-storage --test opfuzz` |
| C7 | recover() 后日志合法（offset 连续、索引可重建）、已 sync 数据存活 | L1+L3 | opfuzz（T-Q.1）+ Verus storage（未开始） | ⬜ 规划中（M2 前） | — |
| C8 | 水位正确：HW ≤ LEO、HW 单调、fetch 不越过 HW/LSO | L2+L3 | TLA+（注：数据面模型无 HW/LSO 变量——"fetch 不越过 HW"仅以 C4 的 committed 长度界形态存在，L2 半边实际为空）+ Verus partition actor（未开始） | ⬜ L2 空缺如实标注；L3 未开始 | — |
| C9 | 消费组安全性：Stable 良构（全员就绪/每分区恰一 owner/owner ∈ 成员集）、同代分配唯一；阴性对照（部分就绪 Sync）反例已检出——CompletingSync 入口"全员完成加入"守卫为必要设计。收敛性归仿真层/T-Q.4 | L2 | PlusCal/TLC `spec/ConsumerGroup.tla` | ✅ v0.3（2026-09-10 不变式评审落地）：commit 带 generation 令牌 + 审计位监控 InvCommitFencedMon（MUT-C 成员校验移除→红、MUT-E 代令牌移除→红——判别力经突变实证，修复了 v0.2 守卫复述型不变式零判别力的 P0）；v0.2 新不变式已接入全部 3 个 cfg（P0-1）；活性二分闭合（四级公平性均红，根因=无限成员 churn，见 CONSUMERGROUP-LIVENESS.md）；残留边界：Stable 态成员失联踢除与 Leave 在模型中同效（注释已标注），memberGen 级僵尸追踪留 v0.4 | `make -C spec consumer-group` / `-demo` |
| C10 | record 批编解码（批头/CRC 域） | L1+L3 | proptest（已有）+ Kani parse harness（`verification/kani/batch_header.rs`，5/5 ✅：parse/append 守卫任意输入无 panic + 守卫完备性 + 畸形 total 域）→ Verus 契约（规划） | 🚧 L1 已闭合（2026-09-10） | `kani verification/kani/batch_header.rs` |
| C11 | 兼容语义 = 真实客户端行为 | 外壳 | librdkafka 档已建（2026-09-10）：`testing/e2e/librdkafka_compat.py`（全组协议 e2e）+ `benches/throughput_confluent.py`（基准档，produce 437-605K msg/s、consume 254K msg/s）；曾据此抓出缺陷⑩⑪（OffsetFetch v8+ 布局；管理面版本分叉 ×4——DeleteTopics/DescribeGroups/ListGroups/未宣告 API）。franz-go / kafka-clients report card 仍未建 | 🚧 librdkafka 数据面+管理面 e2e 闭合；多语言 report card 待建 | `bash testing/e2e/run_e2e.sh librdkafka_compat.py` |
| C14 | **规约扩展**（不变式评审清单）：① crash 模型加 synced 边界（P1-4：当前 Crash 全量持久与 §3 故障模型不符——M2 前必须闭合）；② 消费组 CommitOffsets fencing——✅ v0.3 已落地（generation 令牌 + 审计位监控，MUT-C/E 实证判别力）；③ 超时踢除路径——✅ Leave/崩溃动作已覆盖（Stable 态失联踢除与其同效，注释标注；memberGen 级追踪留 v0.4）；④ 活性公平性实验（✅ 已闭合为二分负结果）；⑤ view 回退方向实验（⬜）；⑥ 不变式标注（✅ 结构性/推理闭包标注落地 .tla+指南） | L2 | TLC | 🚧 ①⑤ 未闭合 | — |
| C13 | **已修复缺陷**：validate_crc / batch_len_at 对 crafted 报文（合法 magic + batch_length=0）曾 panic（`&buf[21..12]`，网络可达 DoS）——由 C6 之外的边界推理发现，回归测试锁定；教训已固化为 Verus 定理（C13'：`c13_short_batch_invalid` 等 4 条，45 verified） | L1+L3 | cargo test + Verus | ✅ 已修复（2026-09-09） | `cargo test -p basalt-record` + `verus --crate-type=lib verification/verus/record_core.rs` |
| C12 | 网络路径/tokio/真实 fs 行为 | 外壳 | turmoil 仿真 + 混沌（T-Q.4） | ⬜ | — |

## 8. 本次落地与下一步

**已落地（2026-09-09）**：
- `spec/BasaltDataPlane.tla` —— ADR-10 数据面 v0.1：单写者控制器、epoch fencing、follower-pull 全量同步（含截断）、多数派 commit、继任者 (lastEpoch, len) 字典序规则、消费者游标。TLC 3 节点 / 日志长 3 / 2 值全空间通过；`EagerLeader=TRUE`（跳过继任规则）检出 acked 数据丢失反例——证明该规则是必要设计，也证明检查器本身有判别力。
- `verification/verus/record_core.rs` —— zigzag 双射全称定理（含 crate 位运算原型与算术规约的 bit_vector 桥接）+ Compression 全函数正确性，Verus 18 verified / 0 errors。varint 循环机器为下一步。

**下一步（按 ROI）**：
1. storage 崩溃一致性 Verus 证明（C7，Sisyphus 方法论：围绕不变式分层抽象；故障前置条件 = §3）；
2. coordinator 消费组 PlusCal 模型（C9 上半）；
3. Kani nightly（L1 全量无 panic）+ Creusot bake-off 补齐切换协议数据；
4. M2 实现 ISR 复制时启动 L4 ghost 重表达与对应表。

## 12. 缺陷 → 检测机制矩阵（2026-09-10，评审驱动新增）

> 原则：**每个已发生的缺陷必须落一个永久检测机制**，并指明该机制覆盖的错误类。
> 机制分层：L1 Kani（不可信输入边界）→ opfuzz（状态机交互采样）→ 一致性/边界表/
> 属性测试（确定性边界与表示不变式）→ L2 TLC（协议语义）→ L3 Verus（算术与
> 包含性，C7 深水区）。code review agent 作为跨层兜底。

| # | 缺陷 | 错误类别 | 抓住它的机制 | 新增/补强的永久机制 |
|---|---|---|---|---|
| ① | SimDisk::len 只返回 pending | 环境模型语义不一致 | opfuzz（间接） | DiskIo len≡read 一致性属性测试（conformance.rs）+ C7 DiskIo 规约翻译 |
| ② | truncate_to 盲设 next_offset | 派生状态与内容不一致 | opfuzz crash 比对 | truncate 批对齐回归 + LEO==base+next_rel 结构断言（conformance）|
| ③ | truncate_to_front 保留/删除颠倒 | 边界循环方向错误 | 边界表测试（新增）| delete/truncate 边界表（0..=10 每值）+ 规格层"前缀物理删除不可行"论证（Kafka 语义采纳）|
| ④ | roll 空段封存同路径双 Segment | 表示不变式违反（base 唯一性）| opfuzz crash-reopen 分歧 | roll no-op 守卫 + 段链严格递增断言（conformance）|
| ⑤ | segment_for 忽略 active | 情形分析不完备 | opfuzz 跨段循环读（新增）| 读包含性属性测试（任意 offset 读回覆盖）+ segment_for active 守卫 |
| ⑥ | 空截断后 append 静默失效 | 空状态与判定条件交互 | 边界表测试（新增）| 🚧 WIP——needs_roll 判定修复后解除 ignore |
| C13/7 | crafted 批 panic（validate_crc/append 双入口）| 不可信输入边界错误 | crafted 回归测试 + **Kani harness**（新增）| Verus total_len 算术定理（已有）+ Kani parse 扩展至 append 校验路径（待做）|
| 8 | batch_io roll staging 丢失（review P0-2）| 多缓冲生命周期交互 | **opfuzz batch_io 档**（新增）| ✅ 随 ADR-14 settle 模式闭合（4 档 80 种子全绿）+ 远期两级水位 TLA+ 小模型 |
| 9 | checkpoint stale 窗口（truncate 不重写）| 派生持久化遗漏 | code review agent | truncate_to/retention 重写 checkpoint（已修）+ fsync（已修）|
| 10 | OffsetFetch v8+ 双侧布局缺失（恒按 v0-7 读写）| 协议版本分叉语义遗漏 | **confluent-kafka（librdkafka）档**（新增，客户端多样性兜底）| librdkafka_compat.py e2e（JoinGroup/SyncGroup/OffsetFetch v9/OffsetCommit 全组协议 + 提交位续读）——kafka-python 停在 v7 探测不到，librdkafka 协商 v9 即断（消费 0 条）|
| 11 | 管理面版本分叉 ×4：DeleteTopics 只读 v6+ 域（v4 TopicNames 丢失→客户端超时）；DescribeGroups 请求域错读（GroupIds≠Groups，恒空响应）；ListGroups 恒空列表+缺顶层 ErrorCode；四个已实现 API 未宣告（librdkafka 视为不支持拒绝发送）| 同上（宣告/实现漂移 + 请求域版本分叉）| 同上（AdminClient 档，一次探针全数暴露）| librdkafka_admin.py e2e（CreateTopics v7/ListGroups/DescribeGroups 成员明细/DeleteRecords v2 读回区间/DeleteTopics）+ api.rs 宣告纪律回归 |
| ⑫ | internal RPC server 未接线（7d5f11d 重构事故：serve 行被删，多节点内部 RPC 全部静默挂死；单节点不经过该路径，既有测试全绿）| 重构事故（接线丢失）| 四轮 code review agent 运行时探针（对照法：客户端端口应答/内部端口超时）| internal_smoke.py e2e——内部端口 MSG_HEARTBEAT/MSG_CREATE_TOPIC 限时应答（"连接成功≠服务在"）|
| ⑬ | read_ex 首批越窗补读后无条件 break——小 max_bytes 消费者拉大批分区永久空响应活锁（三轮 P1-1 契约被 582b071 窗口化击穿）| 契约击穿（性能改造回归）| 四轮 review agent 差分探针（场景 8 红 / 场景 1-7,9,10 绿自证分辨力）| log_test::read_ex_large_batch_small_max_bytes_returns_batch |
| ⑭ | batch_io staging 生命周期三处：fast path 直写绕窗、fast path SyncEach 失败零回滚（僵尸滞留）、收口失败 staging 残留（错误结算数据随下窗落盘）| 多缓冲生命周期交互 | 四轮 review agent actor 级探针 | log_test ×2（append 失败按 cp.staged_len 截除、收口失败整体丢弃）+ SimDisk::set_fail_writes 确定性注入机制 |
| ⑮ | truncate_to 窗口内无条件 clear staged——低于截断点的批未写盘却被按保留结算（ack 成功但数据从未落盘；FollowerPull 严格 await 使其今日不可达，潜伏）| 窗口语义边界（storage/actor 未闭合缺口）| 四轮 review agent log 级探针（含对照组/现状快照防恒真）| log_test::truncate_to_flushes_window_first_keeps_staged_below_offset |


## 9. 参考

- Verus：<https://github.com/verus-lang/verus>（Sisyphus：OOPSLA'24，围绕不变式的存储证明方法论）
- Creusot：<https://creusot.rs/>
- TLA+ / TLC / Apalache：<https://lamport.azurewebsites.net/tla/tla.html>、<https://apalache.informal.systems/>
- 仓库内先例：`../walrus/distributed-walrus/spec/DistributedWalrus.tla`（walrus 的 TLA+ 规约）
- 本工作区知识库：`../../docs/11-testing-strategy.md`（测试金字塔与不变式表）、`../../docs/07-walrus.md` §6
