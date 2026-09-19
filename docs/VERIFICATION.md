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

> **版本切换记录（2026-09-14）**：Verus 由 0.2026.09.09.f42e59f（rolling 本地构建，
> 发布页已不可得）切换为发布版 **0.2026.09.06.8dea4a2** + 显式 Z3 **4.16.0**
> （09.06 发行包不再捆绑 z3，需 `VERUS_Z3_PATH` 指向 z3 4.16.0；rustup toolchain
> 1.98.0）。三个证明文件全量重跑绿（45+10+15）。CI 同步锁定该版本
> （.github/workflows/verification.yml verus job）。

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
| C1 | 不丢：ack 数据存在于任一多数派（多数派持久性），控制器现任主一旦可服务必持有全部 acked 数据 | L2 | PlusCal/TLC (TLC2 2026.09.09.014814)，`spec/BasaltDataPlane.tla` | ✅ v0.1 全空间通过（3 节点×日志长 2×2 值，8890 万状态）；**SplitBrain 场景同样通过**（控制器 fencing 失效下 follower 侧 epoch fencing + 继任规则自足）。**适用条款**（不变式评审 P0-1）：结论以 acks=all 提交 = ack ≥ 多数派且 `unclean.leader.election=false`（ADR-8）为前提；ISR 收缩/扩张与事务未建模（见 VERIFICATION-GUIDE 未建模边界清单）。**实现侧已钉死（2026-09-14，M2 义务履行）**：partition actor 的 acks=all 前置校验下限 = max(min.insync 配置, 多数派)（RF=3 时 min.insync=1 亦须 ≥2 副本新鲜），回归测试 acks_all_pinned_to_majority_floor + 多节点 failover e2e（kill -9 → 60/60 零丢失）复核 | `make -C spec check` |
| C2 | 单写者：每 epoch 至多一个有效 leader；日志匹配（同 index 同 epoch 同值）；view epoch 不超前。注：InvOneLeaderPerEpoch/InvViewEpochSane 为机制锁性质（结构性，MUT-A 可红），承载结论的是 InvLogMatching + C1a/C1b | L2 | 同上 | ✅ v0.1 通过（v0.3 注释标注 + 租约语义；全空间 1.27 亿状态绿，2026-09-10） | `make -C spec check` |
| C3 | 继任规则（(lastEpoch, len) 字典序最大者接管）是 C1 的**必要设计**：跳过它（EagerLeader）TLC 检出"新主缺 acked 数据"反例 | L2 | 同上 | ✅ 反例已检出 | `make -C spec demo-eager` |
| C4 | 游标有界：消费者只读已提交前缀，且每条可从任一多数派恢复。注：v0.2 前 InvConsumedBounded 曾结构性恒真（consume 直读 committed 变量，评审 MUT-B 判别实证）；v0.2 改为向现任主日志 fetch——不变式现依赖 C1b+日志匹配，MUT-B 形态（脏主）可红；InvConsumedOnLeader 为推理闭包（P1-2，降级标注） | L2 | 同上 | ✅ v0.2 通过（全空间 1200 万状态，2026-09-10） | `make -C spec check` |
| C4' | 提交时多数派视图校验（CommitChecksEpoch）对安全性**非必需**（可作纵深防御保留） | L2 | 同上 | ✅ 实验确认（splitbrain / splitbrain-cepoch 双绿） | `make -C spec splitbrain` |
| C5 | zigzag 双射：crate 位运算原型与算术规约逐点相等；`roundtrip_crate(v)==v` 对全部 i64 | L3 | Verus 0.2026.09.06.8dea4a2（release），`verification/verus/record_core.rs` | ✅ 45 verified, 0 errors | 见 verification/verus/README.md |
| C6a | Compression::from_bits/bits 全函数正确性与往返（全 i16 / 全枚举值） | L3 | 同上 | ✅ | 同上 |
| C6 | varint（LEB128）无损：`put_varint`/`get_varint` 往返 == Some(z)（全 u64）、≤10 字节上界、规范形式（续传位/终止位） | L3 | Verus 0.2026.09.06.8dea4a2 | ✅ 合并入 record_core 45 verified（2026-09-14 全量重跑） | `verus --crate-type=lib verification/verus/record_core.rs` |
| C7' | **opfuzz 累计发现并修复 7 个真实缺陷**：① SimDisk::len 只返回 pending（crash 后簿记失真）；② truncate_to 盲设 next_offset 与批边界错位（重启 offset 重排）；③ truncate_to_front 保留/删除颠倒（delete_records 销毁 ≥ offset 全部数据）；④ roll() 空段封存创建同路径双 Segment（删一炸二）；⑤ segment_for 忽略 active 段（多段日志 fetch active 尾部返回旧数据/空——P0 服务端读路径缺陷）。opfuzz 80 种子全绿（四档含 batch_io）、解除 ignore 转正；扩量扫描（2000 种子）另捕获缺陷⑱⑲⑳（均已修，扫描转绿）；⑥ segment_for 忽略 active 段（多段日志 fetch active 尾部返回旧数据/空——P0 读路径，opfuzz 跨段循环读抓出）；⑦ truncate_to 扫描起点错用 locate(offset) 跳过应保留批（opfuzz batch_io 档抓出，已修为段首扫描）| L1 | cargo test opfuzz（clean 40 + chaos 20 + batch_io 20 种子） | ✅ 收官（2026-09-10，7 缺陷全修，T-Q.1 ✅，batch_io 档转正）
| C7'a | **恢复安全性模型 Verus 15/15 全绿**：设备 D1-D4 + scan_valid/scan_len_bounded + synced_survive + intact_prefix_scanned | L3 | Verus 0.2026.09.09 | ✅ 2026-09-10 | `verus --crate-type=lib verification/verus/log_recovery_model.rs` | | `cargo test -p basalt-storage --test opfuzz` |
| C7 | recover() 后日志合法（offset 连续、索引可重建）、已 sync 数据存活 | L1+L3 | opfuzz（T-Q.1）+ Verus storage（未开始） | ⬜ 规划中（M2 前） | — |
| C8 | 水位正确：HW ≤ LEO、HW 单调、fetch 不越过 HW/LSO；活性（稳定环境）：终有主/写入终提交/提交终消费 | L2+L3 | TLA+（HW/LSO 变量未建模，"fetch 不越过 HW"以 C4 committed 长度界形态存在；活性最小参数实验已闭合——`BasaltDataPlaneLiveness.tla`：稳定环境三性质绿、无限 churn+epoch horizon 耗尽下负结果归仿真）+ Verus partition actor（未开始） | 🚧 L2 安全半边以 C4 形态存在、活性半边已闭合；L3 未开始 | `java -cp spec/tools/tla2tools.jar tlc2.TLC -deadlock -config spec/dp-liveness-stable.cfg spec/BasaltDataPlaneLiveness.tla` |
| C9 | 消费组安全性：Stable 良构（全员就绪/每分区恰一 owner/owner ∈ 成员集）、同代分配唯一；阴性对照（部分就绪 Sync）反例已检出——CompletingSync 入口"全员完成加入"守卫为必要设计。收敛性归仿真层/T-Q.4 | L2 | PlusCal/TLC `spec/ConsumerGroup.tla` | ✅ v0.3（2026-09-10 不变式评审落地）：commit 带 generation 令牌 + 审计位监控 InvCommitFencedMon（MUT-C 成员校验移除→红、MUT-E 代令牌移除→红——判别力经突变实证，修复了 v0.2 守卫复述型不变式零判别力的 P0）；v0.2 新不变式已接入全部 3 个 cfg（P0-1）；活性二分闭合（四级公平性均红，根因=无限成员 churn，见 CONSUMERGROUP-LIVENESS.md）；残留边界：Stable 态成员失联踢除与 Leave 在模型中同效（注释已标注），memberGen 级僵尸追踪留 v0.4 | `make -C spec consumer-group` / `-demo` |
| C10 | record 批编解码（批头/CRC 域） | L1+L3 | proptest（已有）+ Kani parse harness（`verification/kani/batch_header.rs`，5/5 ✅：parse/append 守卫任意输入无 panic + 守卫完备性 + 畸形 total 域）→ Verus 契约（规划） | 🚧 L1 已闭合（2026-09-10） | `kani verification/kani/batch_header.rs` |
| C11 | 兼容语义 = 真实客户端行为 | 外壳 | librdkafka 档已建（2026-09-10）：`testing/e2e/librdkafka_compat.py`（全组协议 e2e）+ `benches/throughput_confluent.py`（基准档，produce 437-605K msg/s、consume 254K msg/s）；曾据此抓出缺陷⑩⑪（OffsetFetch v8+ 布局；管理面版本分叉 ×4——DeleteTopics/DescribeGroups/ListGroups/未宣告 API）。franz-go 档已建（produce/组消费/committed 续读）。**librdkafka 幂等模式档已建（2026-09-16，librdkafka_idempotent.py：enable.idempotence=true 全链）**；**kafka-clients（Java 官方栈）档已建（2026-09-16）**：`testing/kafkaclients/` + `run_kafkaclients.sh`——**据它抓出三缺陷**：㊷ conn.rs 并发派发响应乱序（Kafka 线协议要求同连接按请求序返回；Java 严格按序匹配 → correlation 失配；修复 = 写任务按请求序重排缓冲 + 错误分支序号占位 + off-by-one）+ ㊸ rf 守卫误伤单节点 auto-create（无自注册路径 brokers 恒空 → 全部建题被拒；守卫收窄至 rf>1）+ ㊹ 单节点无自注册（metadata Brokers=[]，kafka-clients 严格校验 leader 可映射 → "Topic not present"；补 self-register）| ✅ 四档客户端全闭合（kafka-python / librdkafka / franz-go / kafka-clients）| `bash testing/e2e/run_kafkaclients.sh` |
| C14 | **规约扩展**（不变式评审清单）：① crash 模型加 synced 边界——✅ v0.3 已落地（终局全空间 1.27 亿状态绿，约 45 分钟 8 worker）（synced 水位+Persist+Crash 截断+CommitAdvance 持久前缀校验+InvCommittedDurable；顺带暴露并修复规约缺陷⑯：无租约自恢复同 epoch 重写破坏日志匹配，引入控制器租约 lease 编码 ADR-10 自我指派禁令）；② 消费组 CommitOffsets fencing——✅ v0.3 已落地（generation 令牌 + 审计位监控，MUT-C/E 实证判别力）；③ 超时踢除路径——✅ Leave/崩溃动作已覆盖（Stable 态失联踢除与其同效，注释标注；memberGen 级追踪留 v0.4）；④ 活性公平性实验（✅ 已闭合为二分负结果）；⑤ view 回退方向实验——✅ 已闭合（安全性不敏感正结果：租约语义下 1650 万状态全空间绿，Push fencing+强制重新接管+继任规则兜底）；⑥ 不变式标注（✅ 结构性/推理闭包标注落地 .tla+指南） | L2 | TLC | ✅ 全部闭合（2026-09-10）；M2 实现义务：租约随进程死亡失效 + acks=all 提交条件钉死 | — |
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
| 17 | OffsetFetch null-topics（取组全部）回路由全集（含零提交 topic）——Kafka 语义为只回有已提交 offset 的 topic（未知组回空）| 语义偏离（客户端可见）| **五轮 review 探针**（handler 布局字节级验证沉淀为永久测试）| handlers_layout_tests.rs（6 测试：FindCoordinator/OffsetFetch/DeleteTopics/DescribeGroups/ListGroups/InitProducerId 逐版本字节级布局）|
| 18 | 重开 LEO 塌缩：delete/truncate 删光全部段文件 + crash 重开 → next_offset 从 0 重建（checkpoint 只存 start 不存 LEO）→ 后续 append 从 0 重新分配（offset 流回卷，⑮同族）| 恢复拓扑与持久化水位不一致 | **opfuzz 扩量扫描**（种子数 env 参数化，夜间档 2000 种子）| 修复：重开 LEO = max(扫描值, 持久化 start)；完全位于 start 之下的段移除（拓扑一致）；log_test 确定性复现待补（clean seed=207 ops 已捕获）|
| 19 | 恢复重开段间不连续：前段 torn 截尾后，后段经 checkpoint 大小匹配的轻量路径被无条件信任 → 读流出现空洞（0,1→6,7 缺 4,5）| 轻量路径缺链连续性校验 | 同上（chaos seed=879009 实证）| 修复：轻量路径加 `base == next_offset` 前置，不连续落回全扫（空洞段截齐后移除）|
| 20 | truncate_to 截活动段后继续 append：段文件内容与段名脱钩（base=4 段内数据从 6 起）——恢复扫描不校验批 base 连续性，重开读流出现 4,5 空洞 | 段复用与恢复校验缺口 | 同上（chaos seed=879009，时间线全捕获 docs/diagnostics/chaos-879009-timeline.txt）| 修复：scan_and_truncate 批 base 连续性校验（run_off==h.base_offset，自此截断）|
| 21 | 扩量扫描收口：2000 种子全绿（⑱⑲⑳ 三缺陷修复后）；夜间 CI 转阻塞门禁 | — | — | verification.yml opfuzz-nightly continue-on-error 移除 |
| ⑫ | internal RPC server 未接线（7d5f11d 重构事故：serve 行被删，多节点内部 RPC 全部静默挂死；单节点不经过该路径，既有测试全绿）| 重构事故（接线丢失）| 四轮 code review agent 运行时探针（对照法：客户端端口应答/内部端口超时）| internal_smoke.py e2e——内部端口 MSG_HEARTBEAT/MSG_CREATE_TOPIC 限时应答（"连接成功≠服务在"）|
| ⑬ | read_ex 首批越窗补读后无条件 break——小 max_bytes 消费者拉大批分区永久空响应活锁（三轮 P1-1 契约被 582b071 窗口化击穿）| 契约击穿（性能改造回归）| 四轮 review agent 差分探针（场景 8 红 / 场景 1-7,9,10 绿自证分辨力）| log_test::read_ex_large_batch_small_max_bytes_returns_batch |
| ⑭ | batch_io staging 生命周期三处：fast path 直写绕窗、fast path SyncEach 失败零回滚（僵尸滞留）、收口失败 staging 残留（错误结算数据随下窗落盘）| 多缓冲生命周期交互 | 四轮 review agent actor 级探针 | log_test ×2（append 失败按 cp.staged_len 截除、收口失败整体丢弃）+ SimDisk::set_fail_writes 确定性注入机制 |
| ⑮ | truncate_to 窗口内无条件 clear staged——低于截断点的批未写盘却被按保留结算（ack 成功但数据从未落盘；FollowerPull 严格 await 使其今日不可达，潜伏）| 窗口语义边界（storage/actor 未闭合缺口）| 四轮 review agent log 级探针（含对照组/现状快照防恒真）| log_test::truncate_to_flushes_window_first_keeps_staged_below_offset |
| ㉒ | Controller::open 构造体硬编码 `engine: None`——引擎句柄参数被丢弃，引擎模式全部 controller 退化为传统单写者（propose 复制/职权门控全失效）| 重构事故（参数丢弃）| e2e 探针（CTRL-LOOP engine=false 一眼定位）| multinode_raftrs.py：引擎模式建题经 raft 复制 + 全节点可见断言 |
| ㉓ | CreateTopic 在本地 propose 不路由 raft leader——非 leader 节点提案永挂；ProposeRemote 无职权门控时 7.5s 级联阻塞转发方 | 架构接线缺口（ proposer 位置错误）| FWD 探针链（FWD-BEGIN/FWD-ARRIVE）| MSG_CTRL_PROPOSE 转发（encode_record + 1B 应答）+ ProposeRemote 仅 raft leader 受理（非 leader 立即拒绝防级联）|
| ㉔ | 双重 id 错位：① follower 存 leader_id 残留 raft id（未减 broker+1 偏移）→ 转发指错节点；② leader_id=0 当"未知"哨兵而 broker 0 合法 → "leader 是 node0"被误判未知、propose 永不转发 | 边界值语义错误（哨兵与合法值冲突）| 三节点单测 + HB-LOOP/FWD 探针不对称性 | 哨兵改 -1 + driver 统一减偏移；raftrs 三节点测试断言收敛 |
| ㉕ | 注册 fire-and-forget 化之前：心跳/同步 daemon 循环串行等待注册完成——转发重试期间（选举窗口 + 转发目标 7.5s 重试）daemon 全停 | 生命周期耦合（关键路径被可重试路径阻塞）| HB-LOOP 探针缺失不对称性 | 注册 spawn 化（state.brokers 经 MetaSync 最终一致收敛）|
| ㉖ | CreateTopic 非幂等：重复提案 push 重复 assignment（metadata 重试打到不同节点即触发，LeaderChange 只改第一份，failover 后路由仍指向死节点）；且 broker 未注册齐即建题 → rf 被 min 成 1、单副本无 failover 能力 | 状态机幂等缺失 + 初始化时序 | failover e2e 15s 超时 + assignments=4（应 2）异常信号 | cluster.rs apply(CreateTopic) 同名 no-op（早退不 bump version）+ controller rf 守卫（brokers < rf 拒绝，客户端重试）|
| ㉗ | 无 WAL 重启：MemStorage 空、leader 心跳 commit 越过空日志 → raft-rs commit_to fatal! 杀死驱动线程（节点永不追赶、静默死亡）| 持久化模型与库契约不一致 | 节点日志 panic 行 + TICK-DIAG 停摆 | 快照恢复三元组：state.json + state.meta.json（applied/term，先 state 后 meta 的写序保证）→ apply_snapshot + **cfg.applied 契约**（commit_since_index 初值）+ step/ready 双 catch_unwind 安全网 |
| ㉘ | hs/entries 持久化块重复执行 → MemStorage 无重叠检查 append → 存储日志重复索引 → slice 定位错乱、committed 条目跳条交付 | 重复副作用（幂等性破坏）| restore 单测逐条 APPLY 序列比对 | 确定性回归锁：**restore_tests::restore_catches_up_missed_entries**（杀节点→多数派续写→快照恢复→逐 assignment 内容比对，3×连跑通过）|
| ㉙ | **raft-rs 0.7 LightReady 契约漏读**：committed entries 分两批交付（Ready + advance 的 LightReady），驱动只应用 Ready 批——LightReady 批被丢弃（游标照常推进、状态机永久落后，快照恢复节点必现）| 库契约误读（交付面不完整）| PR 探针（ce=0 而 committed/persisted 全 6）+ applied 水位跳变 | process_ready 对 ready 与 light 两批走同一 apply_entries；㉘ 单测锁定 |
| ㉚ | 跨进程 RaftWire 与进程内 msg_rx 步进路径分裂：RESYNC 预检/catch_unwind/水位跟踪只写在进程内通道——生产路径（TCP wire）三层防护全为死代码；其后 process_ready 重构又丢失 leader_id 原子更新（转发恒 -1）| 传输抽象不一致 + 重构事故（第二次）| FWD-BEGIN=0 不对称性一锤定音 | step_incoming 统一入口（wire 解析后与进程内同路径）+ 引擎 8 场景 e2e 全绿门禁 |
| ㉛ | leader 侧 Progress.matched 单调不下调（update_committed 契约）——无 WAL 重启节点永远"已被追平"、缺失日志永不重发 | 库状态机与持久化模型不匹配 | probe_meta_split.py 三 broker 元数据比对（分裂且 5s 不收敛）| 心跳响应预检：commit < matched → 下调 matched + become_probe（重发接管）；probe_meta_split.py 转正为永久诊断工具 |
| ㉜ | 追平门控缺失：重启节点以陈旧快照服务 metadata（p1 leader=自己，真 leader 已 failover）——僵尸 leader 吸收 produce 致 acks=all 停摆 | 新旧权威共存（fencing 缺失）| 同 ㉛ 探针（node 报 leader=1 其余报 2）| engine_state_ready 门控（applied >= leader_commit 水位，无水位=未就绪）：Sync/TransferLeader/CreateTopic 幂等检查统一走门控 |
| ㉝ | pre-vote 未启用 + 无条件 startup campaign：重启节点以同等日志参选打断在位 leader（disruption 活锁，bounce r0c/r1c 实证）| 选举扰动（thesis §9.6 缺失）| bounce 轮次失败模式聚类 | cfg.pre_vote=true + startup campaign 仅冷启动（重 join 靠 tick election timeout 兜底活性）|
| ㉞ | bounce 场景重启节点缺 BASALT_CTRL_RAFT_ENGINE（runner inline 传参不 export）——重启节点无 raft 运行时，集群多数派永久缺失 | 测试 harness 环境漂移 | HB-LOOP 探针单侧缺失 | 场景内按 RAFTRS 显式补齐引擎 env；重启节点引擎一致性纳入场景前置检查 |
| ㉟ | acks=all 放行竞态：advance_hw 的 min 只算 fresh（isr_lag 窗口内上报）follower——laggard 过期即被静默跳过，HW 越过其 LEO；failover 轮转恰好选中该副本为新 leader 时，divergent-tail healing 截掉多数派已 ack 的尾部消息（failover e2e k-39 实证；控制器 WAL 的 fsync 抖动使 pull 节拍更易跨过 isr_lag 窗口，A/B 实证 pre-WAL 3/3 vs post-WAL 1/3）| 提交面检查与放行条件不一致（C1 只钉了提交时检查，放行侧漏防）| LEO-REPORT/HW-ADV/ACK-RELEASE 探针链（BASALT_LEO_PROBE 门控）+ A/B 二分 | **🟡 缓解已落地（2026-09-15）**：① 冻结提交面——parked ack 记录 append 时的 fresh follower 集合，放行要求面内全员最后已知 LEO 追平（frozen_face_tests 单测锁定，k-39 类越权放行消除）；② runner 固化 BASALT_ISR_LAG_MS=500（死成员 500ms 后退出 fresh 集 → face 收缩恢复活性；l1 <2s 门禁 3/3、failover 零丢失 3/3）。**✅ 完整修复落地（2026-09-15，T-M2.2 首项）**：① ISR 收缩状态机（partition actor 显式 ISR 集合：isr_lag 无上报显式收缩并日志、完全追平（offset ≥ next_offset）重回；HW = min(ISR LEO)；提交面 = leader + ISR；冻结面 = ISR 快照）；② **leader reconciliation**——升主前从存活副本拉回缺失尾部（副本间 FetchSlice 互信放宽 + ReconcileAppend 原偏移落盘 + 有界 800ms/副本），「豁免掉队成员后来当 leader 丢已 ack 消息」的残余窗口由此关闭，failover 轮转选谁都安全。验证：failover 3/3、l1 2/2、引擎 8 场景 + 默认 6 场景全绿。规格化 ✅ 已落地（2026-09-16）：`spec/ReplicationCommit.tla`（HW/ISR/冻结面/reconciliation 四者一致性，3 节点 2 条目全空间 10,284 状态绿）；判别力阴性对照 `replication-commit-norecon`（Reconciliation=FALSE 突变体被 InvAckedSurvivable 检出反例）入 CI 门禁。模型化过程中修正两处实现同源语义认知：HW 是 per-leader 状态（换主即重算）、拉取服务以 serving 为准（换主后以新主为准）。**专属场景 ✅ `multinode_isr.py`（2026-09-16）**：SIGSTOP 冻结 follower → 显式收缩 → 2/3 提交面继续 acks → CONT 追赶重回 → TransferLeader 交给未追平副本逼出就任拉齐 → 零丢失；首轮演练即抓出 reconciliation 的 actor 角色门缺口（拉齐请求落在源副本角色翻转后被拒 → 未拉齐就任丢 10 条），修复（副本集内互信下沉到 actor 角色门）后 4/4 稳定——ISR 机制的永久回归面。**规格化状态（2026-09-16）**：安全门禁 ✅（ReplicationCommit.tla 名义全空间绿）；活性扩展 🚧 WIP——恢复活性（crash→选举→reconcile→serving，WF(Elect)+SF(Reconcile)）✅ 全空间绿；名义活性（EventuallyAllAcked）反例待解（fairness 与 shrink/rejoin 循环交错）；**阴性对照 norecon 判别力暂失**——模型演化（elected 门控/Pull 重回语义）后状态空间收敛至 28 态、变异体路径不可达，CI/Makefile 门禁暂撤（cfg 保留），恢复判别力需 fresh/ISR 双阈值分离建模（忠实表达 ㉟ 原始缺陷：fresh-only HW 与显式 ISR 收缩是两个机制）。**判别力恢复（2026-09-16 深夜）**：活性攻坚中发现模型存在系统性 UNCHANGED 污染（审计脚本曾把 primed 变量错补进各动作的 UNCHANGED——Produce 自相矛盾不可执行、HWAdvance 的 hw 自锁致 hw 恒 0、CrashL 不可执行致崩溃路径从未被探索——此前的「全空间绿」部分无效）。全量重写九个动作的 UNCHANGED（= 全变量集 − 各自 primed 集）后：状态空间 28 → 39,802 态（崩溃/选举/拉齐全部可达），**无 reconciliation 单突变体被 InvLeaderServingHasAcked 检出**（1,283 态），CI 判别力门禁恢复；「无冻结面」单突变被 reconciliation 掩盖（冻结面为第二层防线，符合设计）；MaxEntries=3 下 liveness/recovery 全绿（152/39,802 态）。遗留：名义活性 EventuallyAllAcked 的 stutter 反例（follower 从不 pull 的沉默簇行为合法不 ack——性质需条件化重述）|
| ㊷ | conn.rs 请求并发派发后响应乱序写出——Kafka 线协议要求同连接响应按请求序返回（correlation id 匹配前提）；Java kafka-clients 严格按序匹配 → "Correlation id mismatch"，python/franz-go/librdkafka 按各自策略容忍故长期未暴露 | 并发化重构破坏线协议顺序契约 | **kafka-clients（Java 官方栈）档**（C11 第四客户端）| 写任务按请求序重排缓冲（BTreeMap + write_seq）+ 错误分支（UnknownApi/Protocol/UnsupportedVersion 不可合成）序号占位推进 + off-by-one（write_seq 0 vs req_seq 1）修复 |
| ㊸ | rf 守卫误伤单节点：守卫判 `brokers.len() < rf` 即拒建题——单节点传统模式无自注册路径，brokers 恒空 → rf=1 的 auto-create/admin CreateTopics 全部被拒 | 守卫条件与拓扑认知不一致 | kafka-clients 档 python admin 预建题 error 17 | 守卫收窄至 `rf > 1`（rf=1 无 failover 可言；多节点 rf>1 的保护原样保留）|
| ㊹ | 单节点（BASALT_NODES 未配置）无 broker 自注册：sync task 对 controller_peer=None 提前 return，cluster.brokers 恒空 → metadata 响应 Brokers=[]；kafka-clients 严格要求分区 leader 可映射到 Brokers 列表 → "Topic not present in metadata"（其余三档客户端宽容故未暴露）| 拓扑初始化缺口 | 同上（kafka-clients 档）| async_main 补单节点 self-register（controller state.brokers 恒含自身）|
| ㊻ | protocol registry 错误码撞号：UnknownLeaderEpoch=45 与 OutOfOrderSequence=45 撞号（Kafka 官方：45=OutOfOrderSequence、74=FencedLeaderEpoch、75=UnknownLeaderEpoch）——幂等 producer 落地时发现 | 常量表笔误（判别值重复，E0081 编译期才暴露）| cargo build E0081（判别值重复编译期检查）| 45=OutOfOrderSequence、74=FencedLeaderEpoch、75=UnknownLeaderEpoch 对齐 Kafka 官方 |
| ㊺ | DeleteTopics 标记语义：handler 回 None 但从不删除（ClusterRecord 无 DeleteTopic 变体）——Java AdminClient delete 后 describe 仍可见（其余档未断言"删除后消失"故未暴露）；删除落地后 meta 本地簇视图缺刷新 → post-lookup 误报 UNKNOWN（第二跳）| 语义未实现 + 缓存失同步 | kafka-clients 档 adminPhase（create→describe→list→delete→describe-after 全链断言）| ClusterRecord::DeleteTopic（tag 4 编解码 + apply retain 幂等）+ ControllerCmd/MetaCmd::DeleteTopic 全链 + 删除后 refresh_cluster_snapshot + 路由失效清理（actor 孤儿为 POC 边界，数据由 retention 清理）|
| ㊱ | 控制器 raft 日志纯内存（ack 点在内存，多数派同停机丢快照窗口内已 ack 元数据；重启依赖 leader 重放追赶）| 持久化模型缺失（TiDB X"Raft log 落本地盘才 ack"最低线不满足）| 引擎缺陷簇 ㉗㉘㉙㉛㉜ 全部为"无 WAL"补丁税的结构性信号 | **✅ ADR-17 delta-A 落地**：WalStorage 写穿（dendro SPEC 02 移植子集：帧头 crc-payload-only/段轮转/末段撕裂容忍/毒化降级），快照降级为状态机种子（restore_rebuilds_state_from_wal_without_snapshot 锁定）；controller ack 点 = 本地盘 durable |
| ㊼ | 阴性对照 cfg 失真：a434850 重做 norecon cfg 时把 `Reconciliation` 写回 TRUE——留下的唯一突变 FrozenFace=FALSE 正是提交说明自载「被 reconciliation 掩盖（符合设计）」的恒绿突变体，阴性对照退化为结构性必绿（406,978 态全绿）；判别力门禁（交接文档 §5 基线）名存实亡 | 门禁配置回归（阴性对照恒真——「检查器自身要活着」纪律的 cfg 层违例）| 接手基线重跑（§5 预期一绿一红，实测四绿）+ 探针实证含对照组：现 cfg 恒绿（406,978 态）；恢复 Reconciliation=FALSE 单突变即被 InvLeaderServingHasAcked 检出（1,524 态 / 双突变 1,641 态）| cfg 修复为账本 ㉟ 记载的单突变体（Reconciliation=FALSE + FrozenFace=TRUE，MaxEntries 与名义对齐为 2——review 探针实证 3 非判别力所需、2 下 1,437 态检出；与名义 cfg 逐参数 diff 只剩突变项 + InvHWBounded 省略），重跑四门禁一绿一红两绿（39,802 / 1,437 检出 / 152 / 39,802）；**连带勘误**：㉟ 与 a434850 提交说明的「MaxEntries=3 下 liveness/recovery 全绿（152/39,802 态）」归因失真——盘面 liveness/recovery cfg 恒为 MaxEntries=2，152/39,802 即 2 的实测值；CI 反转断言（`if make norecon; then error`）本应在线上拦截——a434850 推送后未核查 Actions 运行即为漏网窗口，cfg 类改动推送后必须回看 CI；突变 cfg 三方一致纪律：Makefile 注释 ↔ 账本 ㉟ ↔ cfg 突变参数逐项核对（含与名义 cfg 的 delta 纯化） |
| ㊽ | partition actor run() 循环定时器失联：deadline 感知等待只挂在循环体尾部一次，超时分支处理完命令后回到循环顶部的裸 recv().await——此后任何挂起项（fetch 截止/ack 停等/副本长轮询）在安静 actor 上永不超时（无命令驱动 on_deadline 不运行）；T-M3.2 事务 deadline 自 abort 依赖同一计时面，committed 泄露探针测试实证（挂起 committed fetch 2s 无回包）| 定时器生命周期与等待点脱节（重构潜伏缺陷，非本轮引入）| txn_tests::committed_pending_fetch_timeout_does_not_leak（挂起→超时→回包断言，容忍旧实现直接挂死）| run() 重构为唯一等待点：循环顶计算五类 deadline 最小值 → 带超时 recv / 到点 on_deadline / 无 deadline 裸 recv 三分支统一；超时与命令处理共享同一组结算序列（settle/on_deadline/serve_pending/serve_replica_pends）|
| ㊾ | txn deadline sweep 过期条目不保证清除：sweep 失败路径（append IO 失败）与 follower 角色门路径都让过期 deadline 原样留存——主循环 deadline 已过 → on_deadline → continue 无让渡热旋（98% CPU、rx 饿死全部命令）；follower 变体由 follower_does_not_self_abort 测试首轮实证（测试挂死暴露）| 过期源与清除责任不闭合（同一「deadline 已过必须收敛」契约只覆盖了部分路径）| 同上测试族（首轮挂死）+ run 循环唯一等待点使风暴可见化 | 失败退避：sweep Err / follower 早退两路都把过期 deadline 前推 1s；升任（SetRole leader）刷新全部开事务 deadline（新任期 = 新超时，退避残留被接管）；follower_does_not_self_abort 锁定 follower 不落 marker + 升任收敛 |
| ㊿ | 协议 int8 字段用 `as_i32()` 解析——Value::I8 落入兜底分支静默返 0：①Fetch IsolationLevel=1（read_committed）被静默降级 read_uncommitted（abort 后数据对"rc 消费者"可见，Java 事务 e2e 实证）；②FindCoordinator KeyType=1（事务查找）恒判 0 走组路径（多节点事务查找自旋）| 类型化解析器的变体窄匹配 + 跨变体调用静默返零（协议语义字段必须精确变体）| Java kafka-clients 事务 e2e（txn abort 不可见性断言红 → TXN-NOT-ABORTED 探针证伪 fetch 分支）+ probe_txn_api_layouts 非自身 controller 断言（KeyType=1 → NodeId=1 BrokerAddr 查址，打破 controller==self 恒真）| int8 字段统一 as_i8（conn.rs parse_fetch / handlers_groups find_coordinator）；e2e txnPhase 转正（commit/abort/offset 消耗/sendOffsets 四验收面）|
| 51 | protocol codec 缺 nullable-struct 线上原语：null 侧编码为空结构体（缺 int8 -1 标记）、存在侧无 int8 1 前缀——自写 codec 双向 round-trip 自洽（布局探针/字节级回归全绿），与 Kafka 真实语义分叉；KIP-848 Assignment 为全定义域首个真实用户（其余仅未实现的 DescribeTopicPartitions），franz-go 解码越界（kbin ErrNotEnoughData "response did not contain enough data"）| 原语缺失 + 自 round-trip ≠ 互操作（㊿ 同族：自洽面检不出跨实现分叉）| 真客户端 e2e（块 c franz-go 848 档）——跨实现（kmsg/Java codegen）对拍是唯一权威面，探针不可替代 | codec encode/decode 补 int8 presence 标记（与 flexible 无关）；修复面回归由全仓 18 套件 + 两套既有 e2e 锁定 |
| 52 | classic GroupManager：rebalance 中途组清空（session 全灭 / rebalance 截止全踢）时 pending_joins/pending_syncs 悬挂——JoinGroup/SyncGroup oneshot 永不答复，客户端等满自身超时（cooperative 探针挂死实证：10s session TTL 与 rebalance deadline 同点到期，session sweep 先清组）| 协调器状态收尾缺失：组→Empty 路径漏 pending 冲刷；maybe_complete 还把已过期成员的挂起请求计入完成条件 | probe_cooperative_protocol_selection（协作流探针；成员不重入组即触发）| 组→Empty 双路径（session sweep / rebalance 截止）回可重试 27；maybe_complete 只数在组 pending；complete_rebalance 对 stale pending 回 27；探针 deadline 分支锁定（session 3s < 截止 10s 的确定性路径）|
| 53 | 幂等生产者 last_seq 语义错位：idem_record 把会话末序号记成批次 base_sequence——多记录批次的下一批（base = base+count）必判 gap 回 45。librdkafka 分批 flush e2e（T-M4.3 tiered 档）首曝；既有幂等 e2e 恰好全部单批会话故未暴露 | 序号推进语义错位（Kafka：seq 按记录数推进，非按批）| tiered e2e 分批 flush（多批次同 PID 会话）| idem_record 记 base+count-1；语义表不受影响（45 判别值/重试分类不变）|
| 54 | 事务终态数据 fence 过强：同 (pid,epoch) 终态 marker 后拒绝同 epoch 新事务批/终态 marker——TV2「init-per-txn（epoch 必 bump）」假设下的强化，对 epoch 复用的真实客户端（librdkafka 一 epoch 跑多事务）是协议破坏：abort 流数据被拒 + abort marker 被拒 → EndTxn marker re-drive 死循环，客户端 abort 超时（librdkafka 模板联调实证）| fence 语义与真实客户端事务模型冲突（Kafka 本尊：数据 fence = epoch 单调；中止可见性由 aborted 区间承担）| T-M3.5 librdkafka_assign/vector 模板（多事务同 epoch 会话）；franz-go/java（init-per-txn 流）无法暴露 | 数据 fence = epoch 单调（仅更晚 epoch 后旧 epoch 续写为僵尸）；marker fence = 同 epoch 异 outcome 且 txn_open 重锚才放行；terminal_fence 测试重写锁新语义；ADR-18 §4.1 语义修订注记 |
| 55 | 配额节流被 pipeline 并发稀释：节流以「请求任务内 sleep」实现，同连接 pipeline 的并发请求各自看到空桶各自 sleep——sleep 并行叠加，吞吐 = 配额 × 并发度（1MB/s 配额实测 ~2.3MB/s）；且 token 额度在休眠期照常回补、睡醒请求把回补量瞬时消耗——双计进一步翻倍 | 节流无串行化点 + token bucket 回补与睡眠重叠（ slept-time 被当作新鲜额度）| run_quota.sh 断言红（3MB 应 ≥1.8s 实测 0.6s）→ trace 延迟分布（同毫秒并发 5×80ms）| ConnState 锁 std→tokio（节流持锁休眠=请求在锁上排队按速率串行化）+ token 允许负债（先扣减可为负，负值即欠账时长——休眠不再产生可消费额度）；单测负债账本三段锁定 |
| 56 | TLS listener 接入后的启动即停机：优雅停机 select! 新增的 TLS 臂在未配置 TLS 时为 None→async 块瞬间就绪，select 随机选中即走 drain→进程存活 ~2s 后退出——run_e2e.sh「broker not reachable」；早前 auth/quota e2e 系 TLS 改造前二进制所跑，未踩中 | select 臂恒就绪（None 分支无 pending 兜底——「多臂 select 的每个分支都要么挂起要么真的该触发」）| 基础 e2e 偶发红复跑仍红 → 手动起服观察 2s 自杀 → 日志无 SIGTERM 行 → 二分到新臂 | None 分支 std::future::pending()() 挂起；「select 臂增删必须问一遍：未命中时它挂不挂起」入交接陷阱清单 |
| 57 | TLS 客户端被重定向明文口：metadata 通告地址取自集群缓存（主口），SSL 客户端 bootstrap 后按通告地址连明文口发 TLS——握手必败，TLS 面形同虚设（AdminClient 建题卡死实证）。Kafka 语义：metadata 按请求所连 listener 通告对应 listener 地址 | 通告地址单一来源（缓存）+ 逐连接 listener 上下文缺失 | run_tls.sh [1] AdminClient create_topics 超时 → rdkafka 日志 ssl://localhost:9195（明文口）| 通告跟随所连 listener：tls_accept_loop 取 local_addr 端口 + broker_array 对本节点条目强制用连接级 host/port（缓存其余节点不动）
| 58 | produce 处理序乱序：conn.rs per-request 并发派发下，同一连接 pipeline 的两个 produce 请求在分区 actor 邮箱上竞速、后发先至——幂等 seq 检查（正确地）回 OOOSN，但正确回错给了乱序面：kgo 收 45 → 整 PID reload（本地 epoch bump + 回卷重发，已落盘区间在新 epoch 下按新会话重写 = 真重复）+ 元数据刷新（metadataMinAge=5s）停等 → produceTimeout(10s) 静默作废缓冲批（seq 消耗）→ 更多 OOOSN 的自激级联。表现=双重：吞吐假象（13.3MB/s，5s 成簇请求）+ 200k 基准 118 条重复（此前误判为「PID 未持久化」——非事务 PID per-session 本就是 Kafka 语义，方向作废）| 响应序有重排保证而处理序没有（「Kafka 线协议要求同连接按请求序返回」㊷ 只修了写侧；幂等 seq 的前提是 produce 按**连接序应用**——处理序也必须有序）| 双探针对拍：服务端 idem 插桩（gap base=4815 last=3851，缺口恰一批）× kgo debug 日志（produced 3852→4815 成功后 5778 才 OOOSN——成功与缺口矛盾 → 锁定乱序而非丢批）| PRODUCE 请求读循环内联处理（处理序=读序，零竞态；其余 API 保持并发——长轮询 fetch 不阻塞消费连接的 commit/heartbeat 的原始动机不受影响）。修复后 bench produce 13.3→1703MB/s（93×）、重复归零、八套 e2e 全绿；「吞吐优化」类的改进诉求应先查此类处理序缺陷——慢的根源常是正确性缺陷的级联
| 59 | CreateTopics 请求的 NumPartitions/ReplicationFactor 被静默忽略——handler 走 Lookup{allow_create:true} 自动建题路径，恒用 broker 默认值（cfg.num_partitions/default_rf），请求值只回显不生效（BASALT_NUM_PARTITIONS=4 下请求 2 分区实得 4；既有 e2e 通过纯属 env 默认值恰好相等）| 建题参数与查询路径混用（「缺则建」复用了 auto-create，语义=默认值而非请求值）| SASL 矩阵档消费 0/20（生产落 p3、消费订 0/1）→ actor 数对不上请求数 | 新增 MetaCmd::EnsureTopic（缺则按请求值建，存则查回），create_topics 改走之；auto-create 路径（metadata/produce 隐式建题）语义不变仍用 broker 默认 |
| 60 | 空日志 read_ex 返回 Io NotFound（未写过数据的活动段文件未落盘，StdDisk read_at 打开不存在文件）→ storage_error_code 兜底 15（COORDINATOR_NOT_AVAILABLE，可重试）→ franz-go 消费端对该分区无限 strip+元数据刷新循环，空分区消费永久饥饿 | 读路径缺「空 = 合法态」守卫 + 未分类存储错误兜底到强语义错误码（15 意为协调器不可用，客户端据此重构元数据）| SASL 矩阵档消费 0/20 → 服务端 read_for 探针（off=0 cap=0 next=0 => NotFound）| read_ex 加 from_offset >= next_offset 空读守卫（合法空结果=长轮询超时空回的语义基础）；教训：兜底错误码的选择就是客户端行为的编程——未知错误慎映射到触发客户端协议动作的码 |
| 61 | SCRAM server nonce 字符集非法：rand_server_nonce 手写坏 base64 算术（(byte>>2)+b'A' 溢出产生 \、]、非 ASCII），server-first 带垃圾字符——宽松解析器（librdkafka/franz-go/kafka-python）容忍、Java kafka-clients 严格解析整轮认证失败（"Invalid SCRAM server first message format"）| 字符集不合规 + 判别面缺失（既有 SCRAM e2e 恰好全是宽松解析器档）| T-M4.1 矩阵补 kafka-clients SASL_SSL 档即红——「最严苛解析器」是互操作验收的必要面 | nonce 重写为纯字母数字 24 位（LCG）+ 字符集单测 ×50 轮；连带升级 testing/kafkaclients kafka-clients 3.7.0→3.9.1（3.7.0 与 JDK 24+ 不兼容 KAFKA-16443，Subject.getSubject 移除）
| 62 | 单节点重启后 MetaService 簇视图为空：MetaService::spawn 不做启动期刷新，多节点的同步循环分支在单节点拓扑不运行——重启后既有题 metadata 恒 UNKNOWN，直至某次 create/allow_create 请求侥幸触发 refresh_cluster_snapshot。librdkafka 恒发 allow_create=false → 重启后消费永久失败；既有 e2e 的重启面（tiered/多节点）全部依赖 kafka-python 系客户端的版本探测（allow_create=true）或消费组提交位点续读，掩盖此缺陷 | 恢复路径缺口：控制器 WAL 重放后无人把快照推给本进程 MetaService（「重启后的第一份元数据从哪来」没有答案）| 对象侧 retention e2e [2]（librdkafka earliest 消费重启后 EMPTY）→ PROBE-LOOKUP assignments=0 锁定；分层插桩对拍（水位于 100/120 正确而 fetch 不到 actor）排除存储面 | MetaService::run 起始 refresh_cluster_snapshot 一次（多节点同步循环语义对齐）；retention e2e [2] 重启一致 + 全量 143 测试/十二套 e2e 回归绿。教训：「重启后第一个客户端请求」与「重启前最后一个客户端请求」必须同构测试——客户端组合变化即缺陷暴露面变化 |
| 63 | Fetch v13+ 分区可仅带 TopicId 不带名字（librdkafka 实测 name 空）——ACL 授权在空名上决策（ authorize("") → 默认拒绝），授予 READ 后消费仍全拒；同构隐患面 = 一切「按 topic 名决策」的路径（quota per-topic、未来租约面）| 请求形状漂移（v7-12 双字段 → v13 id-only）+ 决策面假设名字非空 | run_acl.sh [5]（授予 READ 后 0/10）→ PROBE-AUTHZ-DENY topic= 空名实锤 | fetch 闸门先 routes.name_for(topic_id) 反查名字（848 块 b 已有零状态面助手）再决策；新按名决策面一律问「id-only 请求形状下名字从哪来」 |

| 64 | 本地日志 time-based retention 首次接线即暴露误删：段级时间索引是稀疏的（push_batch 仅在 INDEX_INTERVAL_BYTES 边界落项，Kafka 同型设计）→ 末项 ≠ 段内最大时间戳；按「索引末项 + retention 过期」判删会把索引陈旧但段尾仍有新写入的段整段删除——retention e2e 实证：段 [90-104] 含 5 条 1 秒前生产的 fresh 消息被按旧批时间判过期（beginning 跳到 105，客户端视角 = 未过期数据丢失）| 稀疏索引的末项语义 ≠ 覆盖到段尾（「索引说什么」与「段里有什么」之间有 encoding gap；仅凭单一信号的时间判定不完备）| 本轮新增的 retention e2e（首轮红：beginning=105 ≠ 100）——t=13.9s 产出的消息在 t=14.9s sweep 被删；诊断日志（段 base/last_ts/cutoff）+ 单测 stale_index_fresh_mtime_kept 锁定 | 过期判定 = 索引末项 ∧ 段 mtime **双条件**（mtime = 段最后一次写入的服务器时钟）：对索引稀疏、客户端时钟偏斜（过去/未来任一方向）、崩溃恢复段（索引无 roll 时末项）三种情形保守正确；真过期段两者必然都老；删除粒度 = 整段（同段未过期数据随之消失 = Kafka 同型语义，e2e 按此设计段布局）|
| 65 | CreateTopics 空名 topic 放行：请求侧编码按计划字段名查值、查不到写默认空串（basalt-cli 首轮把请求字段名写成 "TopicName"——官方 JSON 全版本统一为 "Name"——实证）→ handler 裸收空名照单建出名为 "" 的 topic，metadata 全量列举出现空名条目（kafka-python 复核实锤，同名空串条目混入簇状态）| 入口零校验 + 编码器静默默认值 = 伪造请求的通道（「上游发来的名字总有效」假设不成立）| CLI list 出现空名行 → kafka-python topics() 复核同样空名 → 排除 CLI 解码侧，锁定服务端入口 | 空名 / >249 字节 / "." / ".." / 非法字符（非 [a-zA-Z0-9._-]）拒绝，回 INVALID_TOPIC_EXCEPTION + 原因消息（Kafka 同型校验）；CLI 侧字段名修正 + e2e 锁定非法名拒绝面。教训：字段名以官方协议 JSON 为唯一权威（registry 计划名即线名），「按 v0-6 旧文档记忆写请求」是双向坑——查不到计划字段名时编码器不报错而是写默认值 |
| 66 | CreateTopics 的 ReplicationFactor 静默变 0：线上类型为 **int16**，handler 用 as_i32() 解析——Value::I16 落入窄匹配兜底静默返 0（㊿ as_i8 家族第二例）。后果链：rf=0 → 无副本分区（多节点集群题面只剩 leader 单点）→ 控制器 failover 后无从提主（B3 failover e2e 首曝：位点恢复失败表面下埋着副本面缺失）| 变体窄匹配 + 跨变体调用静默返零（与 ㊿ 同根；不同 API 字段逐一复发）| B3 组协调器 failover e2e（位点恢复失败 → node 日志无候选升主 → APPLY 记录 rf:0 实锤）→ 官方 JSON 类型比对审计（scripts/audit_field_types.py 全量扫描 as_iXX vs 声明类型，当前 0 错配）| as_i16() 修正；审计脚本入 scripts/ 作回归门禁——同族缺陷的系统性面不再是人工逐字段 review |
| 67 | share ack 状态机终态遮蔽：①部分 REJECT 后同锁超集 ACCEPT 把已归档 offset 并进 accepted（accepted ∩ archived ≠ ∅，Reject 永久性被破坏，游标跳过归档记录）；②对称面：ACCEPT 后 REJECT 把已消费记录归档（冗余归档面）。ShareAck.tla 规约首跑即抓（评估纪要风险#2「区间合并/分裂边界」坐实——块 e 立项的原始动机）| 双向终态 ack 无优先级裁决（区间合并/分裂边界）；既有 share e2e 全部单向序列故未暴露 | ShareAck.tla 名义 cfg 首跑红（depth 5/6 即检出）→ TTrace 锁定动作序列 | 终态优先语义（双向对称）：ACCEPT 剔除已归档 offset（归档优先）；REJECT 剔除已 accepted offset（首个终态 ack 定局）——accepted ∩ archived = ∅ 成为真实不变式。规约名义 cfg 绿（49,901 态 depth 15）+ phantom 阴性对照红（InvCursorDelivered 判别力）+ apply_ack 单测锁定 |

## 9. 参考

- Verus：<https://github.com/verus-lang/verus>（Sisyphus：OOPSLA'24，围绕不变式的存储证明方法论）
- Creusot：<https://creusot.rs/>
- TLA+ / TLC / Apalache：<https://lamport.azurewebsites.net/tla/tla.html>、<https://apalache.informal.systems/>
- 仓库内先例：`../walrus/distributed-walrus/spec/DistributedWalrus.tla`（walrus 的 TLA+ 规约）
- 本工作区知识库：`../../docs/11-testing-strategy.md`（测试金字塔与不变式表）、`../../docs/07-walrus.md` §6
