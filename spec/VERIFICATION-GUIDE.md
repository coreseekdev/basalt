# Basalt 规约验证指南（spec/）

> 面向第一次接触本目录的工程师。回答五个问题：这两个 TLA+ 规约**验了什么**、每个不变式违反**对应什么真实事故**、三个实验开关**各自证明了什么**、TLC 反例轨迹**怎么读**、怎么**复跑**；最后给出 v0.1 审查确认的**未建模边界**清单。
> 前置阅读：[docs/VERIFICATION.md](../docs/VERIFICATION.md) §7 验证账本（C1-C4/C9 是本目录的产出）、[测试策略 §1](../../docs/11-testing-strategy.md)（七条验收不变式——规约层只是其中一层的断言来源）。

---

## 0. 一页结论

| 规约 | 一句话业务承诺 | 复跑命令 | 期望结果 |
|---|---|---|---|
| `BasaltDataPlane.tla` | 任意崩溃/断网/切主序列之后：**已 ack 的消息永远不丢，消费者永远读不到没被承诺的数据，每个 epoch 只有一个有权写的主** | `make check` | 全绿（8890 万状态，TLC2 2026.09.09.014814） |
| 同上（阴性对照） | 跳过继任规则 = 必然丢数据，检查器能抓到 | `make demo-eager` | **必须红**（实测先爆 InvCurrentLeaderHasCommitted，5,587 状态；单留 InvLeaderHasCommitted 亦可检出，19,877 状态——C3 实质成立） |
| 同上（实验） | 控制器 fencing 全失效时，follower 侧 fencing + 继任规则自足 | `make splitbrain` / `splitbrain-cepoch` | 双绿 |
| `ConsumerGroup.tla` | 成员任意来去之后，只要组进入 Stable，**每个分区恰好有一个负责人，且是活着的成员** | `make consumer-group` | 全绿 |
| 同上（阴性对照） | "部分就绪即 Sync"= Stable 中出现没报到的 owner，检查器能抓到 | `make consumer-group-demo` | **必须红**（InvStableWellFormed 反例） |

两条纪律：

1. **设计变更门禁**：改复制协议、rebalance 状态机、或 `coordinator/src/lib.rs` / 未来 partition actor 的对应逻辑，必须重跑全部 6 个目标（4 数据面 + 2 消费组）。阴性对照变绿 = 检查器失去判别力，比阳性变红更严重。
2. **规约层以安全性为主**；活性仅一处且为**已证明的负结果**——`ConsumerGroupLiveness.tla` 四级公平性二分（2026-09-10）实证 `RebalanceCompletes` 在任何 coordinator 侧公平性下均不可满足（成员无限 churn 使收口永非持续可用），`consumer-group-liveness.cfg` 保留为已知红阴性对照。现实收敛性归 L2 仿真 + 混沌长跑（T-Q.4）。

---

## 1. 两个规约各验证什么（业务语言）

### 1.1 BasaltDataPlane —— 复制协议（ADR-10 数据面）

模型对象与实现对象的对应：

| 模型里的东西 | 业务上是什么 |
|---|---|
| `Brokers` 3 个节点 + `up/parted` | 3 台 broker，进程崩溃 / 与外界失联 |
| `log[n]`（<<写入 epoch, 值>> 序列） | 每个副本上的分区日志（`storage/src/log.rs` 的逻辑形态） |
| `committed` | **已向生产者 ack 的消息序列**（提交点 = ack 点） |
| `consumed` | 消费者已读的消息序列（游标） |
| `curEpoch / curLeader / leaderHist` | 控制器（单写者）的纪元与指派历史 |
| `view[n]`（epoch, leader） | 节点 n 持久的 fencing 凭据 = Kafka 的 leader-epoch checkpoint |
| `AssignLeader / Crash / Restart / Partition / Heal` | 控制器切主 / 进程崩溃重启 / 网络分区与恢复 |
| `CatchUp / Produce / Push / CommitAdvance / Consume` | 继任接管（截断对齐）/ 生产写入 / follower-pull 复制 / 多数派提交推进 / 消费 |

它验证的命题（都是"任意可达状态必须成立"的断言，TLC 穷举了 3 节点 × 日志长 2 × 2 值 × epoch 2 的**全部**交错）：

- **不丢（C1）**：已 ack 的消息在每一个多数派中都存在完整副本，且现任主一旦可服务必然持有全部 acked 数据。=> 任意少数派同时崩溃，数据仍可从存活的多数派恢复。
- **单写者（C2）**：每个 epoch 至多一个被指派的主；同一位置同一写入 epoch 的日志条目值必相同（截断对齐的前提）；节点不会自以为处在"未来"的 epoch。
- **游标有界（C4）**：消费者只读已 ack 前缀，且读到的每一条都能从任一多数派恢复——主随时崩掉，消费进度之后的数据也不会蒸发。

### 1.2 ConsumerGroup —— rebalance 状态机

镜像 `coordinator/src/lib.rs` 的四态机（Empty → PreparingRebalance → CompletingSync → Stable）。

它验证的命题：

- **Stable 良构（C9）**：组只要到达 Stable，全员已报到、每个分区恰有一个 owner、owner 都在当前成员集里。=> 不存在"没人消费的分区"（业务停摆且无告警），也不存在"两个人消费同一分区"（同一代内的双消费）。
- **簿记**：ready ⊆ members、generation 历史 ≤ 轮数上界。

它**不**验：rebalance 多快收敛（活性）、跨代的僵尸成员双消费（需要 offset 提交 fencing 模型，见 §6）、session timeout 时序。

---

## 2. 每个不变式违反 = 什么真实事故

### 2.1 数据面

| 不变式 | 白话 | 违反时的事故 | 分量 |
|---|---|---|---|
| `InvLeaderHasCommitted` | 每个多数派都保有全部已 ack 消息 | **生产者收到"写成功"，事后消息消失**。少数派崩溃 + 切主后，ack 过的订单/扣款事件查无此条，且无法审计丢在哪个环节。这是 C1 的主体 | 承载结论 |
| `InvCurrentLeaderHasCommitted` | 现任主必持有全部 acked 数据 | 新主缺数据还继续服务：新写入盖在空洞上，消费者读到 offset 跳变；"不丢"退化成"运气好" | 承载结论 |
| `InvLogMatching` | 同位置同 epoch 的条目值相同 | 截断对齐失效：failover 后两个副本"同 offset 不同内容"，fetch 返回错数据，下游主键数据错乱且难以定位 | 承载结论 |
| `InvOneLeaderPerEpoch` | 每 epoch 至多一主 | 脑裂：两个主各自接受写入并都自认权威，同一 offset 出现两条不同消息 | 结构性（控制器每指派一次 epoch+1，构造上保证；价值在于锁死设计意图） |
| `InvViewEpochSane` | 节点不自以为在未来 epoch | fencing 凭据失真：拒绝合法主的复制流，或接受非法主 | 半承载 |
| `InvConsumedBounded` | 消费者只读已 ack 前缀 | **幻读**：下游处理了一条系统从未承诺过的消息——无法重放、无法解释、对账不平 | 承载结论 |
| `InvConsumedOnLeader` | 读到的每条可从任一多数派恢复 | 上一条的端到端版：主一崩，消费位置之后的数据全部蒸发，offset 已提交但数据没了 | 承载结论 |

### 2.2 消费组

| 不变式 | 白话 | 违反时的事故 | 分量 |
|---|---|---|---|
| `InvStableWellFormed` | Stable 态每分区恰一个活着的 owner | 双 owner：消息被两个消费者同时处理（重复扣款/重复发货）；零 owner：分区无人消费且没有任何告警——比双消费更隐蔽 | 承载结论 |
| `InvGenAssignmentUnique` | 同代分配历史唯一 | （若可违反）两个成员拿到互相矛盾的分配表 | **结构性**：Sync 每次必然 gen+1，构造上恒真，见 §6 P2-7 |
| `InvReadySubset` / `InvGenHistoryBounded` | 簿记自洽 | 状态机写坏 | 结构性 |

---

## 3. 三个实验开关各自证明了什么

名义全绿只能证明"协议与检查器自洽"，**不能证明检查器有判别力**（一个把不变式写错或模型写得too弱的检查器也会全绿）。阴性对照就是给检查器做的"已知有病、必须确诊"的测试。

| 开关 | 改了什么 | 结果 | 证明了什么 |
|---|---|---|---|
| `EagerLeader=TRUE`（`make demo-eager`） | 新主跳过继任接管（CatchUp）直接服务 | **红**：实测先爆 InvCurrentLeaderHasCommitted（5,587 状态——新主持有分叉短日志即违 C1b）；单验 InvLeaderHasCommitted 也红（19,877 状态——新主拿空/短日志 Push，把持有 acked 数据的 follower 截断掉） | 继任规则（取多数派中 (lastEpoch, len) 字典序最大者）是 C1 的**必要**设计。TASK.md T-M2.3 的"继任者预计算"不可省、不可简化 |
| `SplitBrain=TRUE`（`make splitbrain`） | 控制器 fencing 失效：旧主永远不知道自己被替换，与新旧主并发服务（且提交不校验视图） | 绿 | follower 侧 epoch fencing（只接受不晚于自身 view 的 epoch）+ 继任规则的 epoch 支配，**在没有控制器兜底时依然保住 C1/C2/C4**。旧主无限期僵尸化不破坏安全 |
| `CommitChecksEpoch=TRUE`（`make splitbrain-cepoch`） | 在上一场景基础上，commit 的多数派还需视图与主一致 | 绿 | `CommitChecksEpoch` 对安全性**非必需**（C4'）：它不是 C1 成立的隐藏前提，可作纵深防御保留。注意 `check.cfg` 名义场景默认开它——若未来改动使 splitbrain 变红，说明改动隐式依赖了这个防线 |

两处容易误读的地方：

- **splitbrain(F) 绿可以推出名义+CommitChecksEpoch(F) 也绿**：SplitBrain=TRUE 只放宽 ThinksLeader 守卫，行为集是真超集。所以 C4' 不需要再跑第四个组合。
- **Makefile 里 `-deadlock` 是关闭死锁检查**（TLC 的该 flag 语义是"不查死锁"）。这是故意的：epoch 耗尽（`MaxEpochs`）、日志写满、全员 parted 都是合法停机态，死锁不是本模型的缺陷信号。

---

## 4. 怎么读懂 TLC 反例轨迹

TLC 检出反例时输出一串编号状态 `State 1: <Initial predicate>` → `State 2: <Action ...>` → … → 最后一行 `Error: Invariant InvXxx is violated.`。读法固定四步：

1. **先看报的是哪个不变式**，对照 §2 的表翻译成事故（比如 InvLeaderHasCommitted = "已 ack 数据从多数派消失"）。
2. **在末状态定位违反点**：对 InvLeaderHasCommitted，找一个多数派 q，确认 q 里每个 r 的 `log[r]` 都不是 `committed` 的值前缀；重点看 `committed` 的长度和各副本 `log` 的差异。
3. **倒着找关键动作**：从后往前找第一个让局面不可逆的动作——通常是某次 `Push`（follower 日志被整条覆盖 = 截断发生）、`AssignLeader`（换主 + `caughtUp` 被置位）、`CommitAdvance`（ack 点推进）或 `Crash`。
4. **核对三个信号**：(a) `committed` 长度 vs 各 `log` 长度——谁缺了已 ack 的尾巴；(b) 条目里的写入 epoch 与各节点 `view`——旧主/新主的凭据各是多少；(c) `up/parted/caughtUp`——崩溃与接管的状态对不对得上。

以 `make demo-eager` 的反例为例，轨迹形状必然是：AssignLeader 指派新主且 `caughtUp := TRUE`（跳过接管）→ 新主 Produce（空日志上追加）→ 新主 Push 短日志给持有 acked 数据的 follower（follower view epoch 更小，无法拒绝）→ 多数派中不再有人持有完整 `committed` 前缀 → 不变式爆。这就是"继任规则不可省"的机器检查版证明。

实用技巧：

- 加 `-dumpTrace trace.bin`（或 TLC GUI）可以逐状态回放；`-cleanup` 会清掉状态缓存文件，调试时去掉它。
- 想人为构造反例场景，最省力的是复制一个 `.cfg` 改开关（如 `demo-eager.cfg` 的做法），而不是改 .tla。
- 反例里的每一步都是**合法**动作——读懂"为什么这一步是允许的"比读结局更重要，修法通常是收紧守卫而不是删动作。

---

## 5. 怎么复跑

环境：JDK 11+；`tla2tools.jar` 放本目录（README 顶部有 wget 命令），或 `make TLATOOLS=/path/to/tla2tools.jar …`。

```sh
cd spec
make check               # 名义协议：8 个不变式应全绿（约 8890 万状态，-Xmx8g / 8 workers）
make demo-eager          # 阴性对照：必须检出 InvLeaderHasCommitted 反例
make splitbrain          # SplitBrain=TRUE + CommitChecksEpoch=FALSE：应绿
make splitbrain-cepoch   # SplitBrain=TRUE + CommitChecksEpoch=TRUE：应绿
make consumer-group      # 消费组名义：4 个不变式应全绿
make consumer-group-demo # 阴性对照（SyncRequiresFull=FALSE）：必须检出 InvStableWellFormed 反例
```

| 何时必须重跑 | 范围 |
|---|---|
| 改 `BasaltDataPlane.tla` / 复制协议设计（ADR-10 相关 ADR 修订） | 全部 4 个数据面目标 |
| 改 `ConsumerGroup.tla` / `coordinator/src/lib.rs` 状态机 | 全部 2 个消费组目标 |
| 改 `storage/src/sim_disk.rs` 故障模型语义 | 全部 6 个（VERIFICATION.md §3 变更纪律） |
| nightly 回归（CI） | `make check` + `make consumer-group` |

数据面 4 个场景全跑一遍在 8c/8G 上是小时级（check 一项即 8890 万状态）；消费组两个是分钟级。CI 里只放小模型回归，全量留在设计变更门禁。

---

## 6. 已知的未建模边界（v0.1 审查结论，2026-09-10）

> 这是"信任这张表之前必须读的部分"。按 P0（安全盲区，必须在 M2 实现前闭合）/ P1（建模缺口，v0.2 补）/ P2（改进或明确标注）分级。

| # | 边界 | 影响 | 去向 / 修法 |
|---|---|---|---|
| **P0-1** | **模型是"多数派提交"，实现是"ISR 形态 + HW ack 计数"**：`CommitAdvance` 要求多数派持有相同前缀才 ack；spec/README.md 精化桥把 `CommitAdvance` 映射到"HW 推进的 ack 计数"，但没写"计数 ≥ 多数派"。ISR 可收缩（Kafka 语义）到少数派，unclean election（ADR-8 第三段）更是设计内数据丢失 | 若实现允许 ISR 收缩后仍以 acks=all 提交，**已验证的 C1 不覆盖该形态**——"不丢"从定理退化为配置纪律 | 二选一并在账本 C1 行标注适用条件：(a) 实现侧把 acks=all 的提交条件钉死为 ack 数 ≥ ⌈N/2⌉+1，ISR 收缩到多数派以下只允许损失可用性；(b) 模型侧加 ISR/min-insync 参数与收缩/扩张动作，验证提交集合恒为多数派。同时把 `unclean.leader.election=false` 写进 C1 的承诺条款——EagerLeader 反例已演示跳过继任规则必丢数据，unclean 选举即"从少数派跳过继任规则" |
| P1-2 | **Crash 不丢未持久化尾巴** | 实现 ack 前不 fsync 的 bug 规约层抓不到 | ✅ 已闭合（2026-09-10，C14①）：synced 水位 + Persist（fsync 边界）+ Crash 截断到 synced + CommitAdvance 校验持久前缀 + `InvCommittedDurable`（commit ⇒ 任一多数派含 fsync 副本）。**副作用即价值**：诚实的 crash 暴露了规约缺陷⑯——SplitBrain 下崩溃旧主以原 epoch 自恢复重写分叉日志（InvLogMatching 红），修复为控制器租约语义（lease 随 crash 失效，编码 ADR-10 自我指派禁令）。M2 实现义务：租约随进程死亡失效 | |
| P1-3 | **消费组缺 Stable 成员消失路径**：模型只有显式 Leave 和 rebalance 中崩溃；Stable 成员静默死亡/心跳超时（实现里的 `rebalance_deadline`、session timeout）未建模。死成员在模型里永远持有分区 | `InvStableWellFormed` 是在"成员只会体面离开"的世界里证的；超时踢除路径未经检验 | 加动作：`m ∈ members` 超时被移除，Stable/CompletingSync → PreparingRebalance（-members）；`InvStableWellFormed` 应仍绿 |
| P1-4 | **"无双消费（fencing）"完全未建模**：模型没有消费动作和 offset 提交；跨代僵尸（旧代成员继续消费/提交）是验收不变式 5 的另一半 | 账本 C9 只覆盖安全性上半；generation fencing 的客户端语义（`IllegalGeneration`，实现已有）无规约依据 | v0.2 加：offset 提交动作带 (generation, member_id)，coordinator 拒绝旧代；不变式 = 已提交 offset 只被当代 owner 推进且单调不减。可选：僵尸继续消费的动作 + "消费位置合并不回退" |
| P1-5 | **活性零验证** | 收敛性 bug（活锁、饥饿）设计层不设防 | ✅ 已闭合（2026-09-10）：**数据面** `BasaltDataPlaneLiveness.tla` 最小参数实验——稳定环境（无崩溃/分区）三性质全绿（终有主/写入终提交/提交终消费）；无限 churn 下负结果（epoch horizon 耗尽+主崩溃=永久不可服务，租约语义的设计内代价），归 T-Q.4。**消费组**活性二分负结果（CONSUMERGROUP-LIVENESS.md：任何 coordinator 侧公平性均不可满足，需 churn 有界环境假设）。时限性指标（<2s）永远归仿真/混沌 | |
| P2-6 | **view 持久化是理想化** | 方向性分析表明 view 回退大概率无害，但未机器检查 | ✅ 已闭合（2026-09-10，C14⑤）：`ViewRollback` 开关 + `demo-viewrollback.cfg`（Restart 载入 epoch-1 过期 checkpoint），约 600 万状态全空间绿——view 回退安全性不敏感；危险方向是"崩溃后原 epoch 自恢复"（缺陷⑯，由租约封死） | |
| P2-7 | **结构性不变式应如实标注**：`InvOneLeaderPerEpoch`、`InvGenAssignmentUnique`、`InvGenHistoryBounded`、`InvReadySubset` 在当前动作定义下构造恒真（epoch/gen 每次严格 +1、守卫直接保证子集关系），防回归有价值但不承载结论 | 新人可能高估绿的数量 | 已落地（2026-09-10）：.tla 注释标注完成；另据不变式评审 MUT-B/C 判别——InvConsumedBounded 曾为结构性恒真（consume 直读 committed 变量），v0.2 已改为主日志 fetch（MUT-B 变红有判别力）；InvCommitHasOwner 曾零 fencing 内容（MUT-C 全绿），v0.3 以审计位监控 InvCommitFencedMon 取代（MUT-C/E 变红） |
| P2-8 | **ThinksLeader 读全局 `curLeader`**：broker 守卫里出现全局变量（全知性）。由于指派原子广播给全部未分区节点，该合据可证冗余 | 模型比现实强：现实中 broker 只知本地 view | 删掉 `curLeader = n` 合取重跑 4 个场景；绿则模型更贴近实现 |
| P2-9 | **ConsumerGroup 两个动作语义存疑**："JoinGroup 完成"（从 ready 移除，m 仍在 members）在现实协议中无对应物——若想表达 rebalance 超时踢除，应同时移出 members；Leave 不重置其余成员的 ready，意味着"离开触发的 rebalance 无需他人重新报到"，与 Kafka Classic 语义相左 | 规约与 `coordinator/src/lib.rs` 可能各说各话（精化桥断裂） | 逐动作与实现对表：给出精确现实对应或删除；Leave 语义与 lib.rs 对齐后二选一并写进注释 |
| P2-10 | **网络模型粗粒度**：Push 原子化（fetch+截断+append+ack 一步）；`parted` 是节点级单 bit（无单向分区、无消息丢失/乱序/延迟）；控制器永不崩溃（其 HA 靠 openraft，ADR-2 外包）、指派原子广播 | 时序窗口类 bug（半复制状态、延迟到达的旧请求）不设防；安全方向这些抽象是保守的 | 保持现状 + 明示；链路级故障归 turmoil 仿真（§3 标准场景"网络分区""in-flight 重排"） |
| P2-11 | **探索深度**：MaxEpochs=2 意味着只穷举"初次指派 + 一次切主"后的世界；MaxLogLen=2 | 三次以上切主的复合场景未穷举 | 账本已列 Apalache 符号化扫大参数（nightly） |
| P2-12 | **事务/LSO 未建模**：无 LSO 变量，read_committed、abort 后 offset 已消耗等验收不变式 4 无规约。当前实现 `LastStableOffset = HW`（server/src/handlers.rs），暂无偏差 | 落地事务（M3+）时设计层空白 | 实现事务前先扩模型：加 LSO 变量 + "fetch 不越过 LSO" 不变式，把验收不变式 4 纳入 C 账本 |
| P2-13 | **消息级语义构造恒真**：offset 编号、幂等/重试去重、同 key 顺序、retention 删头都未建模——"不重/单调 offset/顺序"在模型里不可能违反，也就没被检验 | 验收不变式 2/3/6 在设计层无覆盖 | 有意取舍：这些是机制层性质，归 proptest / 仿真断言器 / 客户端对拍（测试策略 §1 的"主要验证层"本就如此分配）。勿在账本里把它们记成 TLA+ 已覆盖 |

---

## 7. 文件地图

| 文件 | 内容 |
|---|---|
| `BasaltDataPlane.tla` | 数据面协议 v0.1（PlusCal + 翻译），头部注释是开关与边界的权威声明 |
| `ConsumerGroup.tla` | 消费组四态机 v0.1 |
| `check.cfg` / `demo-eager.cfg` / `splitbrain.cfg` / `splitbrain-cepoch.cfg` | 数据面 4 个场景（仅开关组合不同） |
| `consumer-group.cfg` / `consumer-group-demo.cfg` | 消费组名义 / 阴性对照（SyncRequiresFull=FALSE） |
| `Makefile` | 全部复跑入口 |
| `README.md` | 精化桥 v0（TLA+ 动作 ↔ 实现 ↔ Verus 规约对应表，随 M2 充实） |
| [docs/VERIFICATION.md](../docs/VERIFICATION.md) | 验证账本（对外信任面）；本指南 §6 的分级结论应随修复合入其 C1/C9 行 |
