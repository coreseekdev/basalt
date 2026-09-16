# ADR-18：事务（T-M3.2，TV2-only）

- 状态：设计草案 v2（2026-09-16，review 探针后修订，待评审）
- 依据：TASK.md T-M3.2/T-M3.6；ADR-9（事务直接 TV2/KIP-890，不做 TV1 兼容）；
  Arroyo §11 专项精读（12-arroyo.md）；Kafka §5/§6（01-apache-kafka.md）；
  KIP-890 官方语义（每事务 epoch bump / 单次合并 AddPartitionsToTxn / broker 侧
  僵尸防御 / CONCURRENT_TRANSACTIONS）。
- 评审记录：2026-09-16 review 代理 1×P0 + 7×P1 全部吸收（控制批 wire 格式
  key/value 纠正、marker 提交面落定、fence 错误码工作量、pending 持久化、
  信任发散两洞、引擎模式波及面、阴性对照判别力重设计、挂起 fetch 三接触点）。

## 1. 目标与非目标

**目标**：跨多分区原子写（commit 全可见 / abort 全不可见，数据占 offset）；
事务性消费位提交（sendOffsetsToTransaction）；read_committed 隔离级；
接管恢复不丢已 ack 的提交决定。验收面 = T-M3.6：Jepsen Bufstream 三场景
（aborted reads / torn transactions / lost writes）+ 仿真 ≥500 seeds。

**非目标（v2 边界）**：TV1 兼容（ADR-9）；exactly-once 流处理（consume-transform-
produce 循环里的事务 API 属客户端编排，服务端原语同套）；txn coordinator 多活
（单实例，见 §9 拓扑）；跨 controller failover 的 TxnLog 复制（引擎模式边界，§9）。

## 2. 总体架构

三个职责面，各自独占状态（actor 纪律不变）：

- **txn coordinator actor**（新增，`server/src/txn/`）：事务元数据状态机 +
  `__transaction_state` 的 basalt 对应物 = **TxnLog**（append-only 文件，恢复=重放；
  OffsetLog/ADR-17 WAL 同族，`coordinator/src/log.rs` 范式）。记录族：开事务
  （pid/epoch 分配）、AddPartitionsToTxn 分区登记、**TxnOffsetCommit 的 pending
  offsets**（§7）、Prepare{outcome,parts}、Complete{outcome}。PID/epoch 独占分配、
  EndTxn 两段、marker 编排、超时 abort。
- **partition actor 扩展**（`server/src/partition.rs`，T-M3.1 幂等状态机的自然延伸）：
  开事务表（per-pid first_offset = LSO 锚点）、LSO 推进、控制记录 append
  （**过提交面落定**，§4.1）、marker 幂等、终态 fence、aborted 区间过滤、
  恢复扫描收割。
- **group coordinator 扩展**（`coordinator/`）：TxnOffsetCommit pending 生效/
  丢弃（KIP-447 的 essential 部分）。

**不引入新持久化文件到数据分区**：事务对分区日志的全部痕迹 = 数据批（原有格式，
transactional 位置位）+ 提交/放弃 marker（控制批）——两者都走既有 append 路径；
重启后事务状态从恢复扫描收割（§4.4）。对照 Kafka per-segment `.txnindex`：
basalt 打开日志本就全量扫描（⑱⑲⑳ 的恢复校验已付费），派生免费，少一类文件。

## 3. PID / epoch 所有权上移（TV2 核心）

T-M3.1 现状：PID = node_id 高位 | 进程内计数（构造性唯一），epoch 恒 0。
TV2 语义（KIP-890）：**每开一个新事务 epoch +1**（隔离上一事务的僵尸副本；
Kafka v2 的 bump 落点在 EndTxn，basalt 取开事务（AddPartitionsToTxn）落点——
单 TxnLog 所有者下等价，且让「新事务被拒（CONCURRENT_TRANSACTIONS）」
先于「新 epoch 已放号」发生，僵尸窗口更窄）。

- 带事务 ID 的 InitProducerId → 路由到 txn coordinator：分配 PID（沿用
  node_id|counter 构造性唯一公式），epoch = TxnLog 中该 txn_id 记录值 + 1，
  持久化后应答。僵尸 fence = 旧 epoch 在所有 broker 的 `idem_check` 处
  自然被拒。**工作量和现状差距（review P1）**：fence 判定在（partition.rs
  idem_check epoch 倒退分支），但 produce 错误映射兜底是 UnknownServer——
  Java 客户端对 UnknownServer 无限重试，fence 对客户端等于没生效。须新增
  `StorageError::InvalidProducerEpoch` 变体并映射错误码 47（api.rs 已对齐官方）；
  §7 TxnOffsetCommit 的 stale-epoch 回 47 复用同一映射。
- 无事务 ID 的 InitProducerId 保持现路径（普通幂等，不受影响）。
- 客户端语义衔接：每事务 epoch bump ⇒ 客户端每事务 sequence 从 0 重开
  ——`idem_check` 的「epoch 升级重置会话」分支已覆盖。

## 4. 数据面（partition actor）

### 4.1 开事务表、LSO 与 marker 落定

```
txn_open: HashMap<pid, (epoch, first_offset)>   // 首个事务批 append 时登记
last_marker: HashMap<pid, (epoch, outcome)>     // 终态 fence + 重放幂等（§4.3）
lso = min(HW, min(first_offset of txn_open))    // 无开事务 = HW
```

事务性批（transactional 位 1）**produce 路径**（Assign）校验链：role/提交面
（原有）→ `idem_check`（原有）→ **终态 fence**：`last_marker` 命中同 (pid, epoch) ⇒ 拒绝
（InvalidTxnState/47 族）——封「marker 落地后僵尸同 epoch 续写复活开事务」
（review 实洞：无此规则 idem 放行 seq+1，txn_open 复活且无人补 marker →
LSO 永久停滞）。**复制路径（Absolute，follower pull）**不重查 idem/fence（信任 leader 已
fencing——marker seq 恰为数据 last+1，重查幂等会楔死多批切片复制），
改为切片内逐批收割式推进事务视图（控制批 → 终态簿记；事务数据批 →
开/续事务）——follower 的 txn_open/last_marker/aborted 与 leader 一致，
升任继承不腐化（review P1×2 实证后定案）。

分区侧另挂 **txn_open deadline 自 abort**：开事务超时走与
coordinator 同一 marker 内部通道自弃（「未登记漂移批」的兜底不在 coordinator
的 TxnLog 分区清单内，只能靠分区侧——§10 发散边界的封口前提）。纪律：
follower 不自 abort（绕过复制层写日志 = 副本分叉，TruncateTo fencing 同款
纪律），过期 deadline 前推退避（否则主循环 deadline 风暴热旋），升任刷新
新任期超时；sweep 失败同款退避。

**marker 纪律（块 a 落地补强，review 实证）**：① acks=all 前置门与 produce
同款（新鲜 ISR 低于 max(min.insync, 多数派) 不受理——否则空冻结面「停等」
全称真即放行，coordinator 拿到成功而零副本持字节）；② 在途互斥——同 pid
已有 marker 在停等/窗口内未结算时拒绝新 marker（可重试错误），保证簿记
顺序与字节顺序一致；③ produce 终态 fence 用序判定（last_marker.epoch ≥
批 epoch 即拒）且同样查在途——封「marker 飞行中僵尸续写逃出 aborted
区间」；④ Commit 落账清除同会话 aborted 残留（末写胜出，防竞态）；
⑤ aborted 区间落账取 txn_open 现值（append 与 release 之间被扩大的区间
不漏）。**WriteTxnMarker 落定语义（review P1，§13 一致性）**：marker 是数据——
acks 全部生效前不得应答 coordinator，落定判据 = 冻结提交面放行（ParkedAck
同型：面内全员 LEO 追平；单副本 = flush 即放行）。ReconcileAppend 只引为
「内部命令管道」先例（跨节点 meta actor 落到本地 PartitionCmd），
**不是应答语义先例**（它是 append 即应答的拉齐特例）。否则 marker 未达
多数派时 coordinator 即可写 Complete，崩溃 + failover 丢 marker = 已提交
事务数据成孤儿 = lost writes。

### 4.2 LSO 推进与消费隔离

- marker append（占一个 offset，控制批）→ 从 txn_open 移除 + 写 last_marker
  → LSO 跳到下一锚或 HW；abort 同时把 (pid, epoch, first, last) 追加进内存
  aborted 集。
- Fetch（现有 handler 响应占位转正）：`LastStableOffset` = lso；
  isolation_level=read_committed → 读上限从 HW 换 LSO + 按 aborted 集丢批；
  read_uncommitted → HW 上限（现状）。
  **投递模型（块 a 定案，review P0 实证后修正）**：服务端预过滤 aborted 批 +
  `AbortedTransactions` 恒发空数组——java 客户端的 aborted 区间跟踪按
  (pid, FirstOffset) 入集、**只靠收到 ABORT 控制批终结**，而 Kafka 的
  控制批剥离在客户端库层（wire 上投递）——「服务端剥离控制批 + 发非空
  条目」组合会让同 pid 后续已提交批被客户端误丢（aborted reads 反向：
  丢真数据）；「服务端滤数据 + 空列表」是唯一自洽组合，对客户端能力
  无假设（四档解析层兼容；read_committed 专项 e2e 在块 d 落地后转实测）。若未来改走 Kafka 忠实模型（wire 投递控制批 +
  客户端过滤），必须两者同改。
- **挂起 fetch 三接触点全改（review P1）**：PendingFetch 增隔离级字段；
  唤醒谓词（serve_pending 的 `offset < high_watermark`）与**超时空回路径
  （on_deadline 的 read_ex(…, ReadCap::HighWatermark)——现状硬编码）**均按
  请求隔离级取对应上限；漏掉超时路径 = read_committed 长轮询超时回包
  泄露未提交/已 abort 批。
- wire 格式从官方（review P0 纠正）：marker = 单记录控制批（control+
  transactional 位，携带事务 pid/epoch），record **key = [version:i16=0]
  [type:i16]**（ABORT=0/COMMIT=1；key 不得为 null——客户端正是以
  「非空 key 可解析出 ControlRecordType」判别控制记录，java 事务消费者
  从 key 解析），value 为空/不透明。

### 4.3 marker 幂等（重放安全）

append 前查（以无在途 marker 为前提——在途互斥见 §4.1 纪律②，回可重试
错误而非终态冲突）：内存 txn_open/last_marker 命中同 (pid, epoch) 且
outcome 相同 → no-op ack（不占新 offset）；**outcome 相异（COMMIT vs ABORT）→ 拒绝后到
marker、应答错误令 coordinator 转入 abort 重对齐——COMMIT 永不覆盖 ABORT
终态（abort 是安全方向；可达场景：分区侧自 abort 已落地后 coordinator 接管
重发 COMMIT，若无此规则已 abort 数据将转可见 = aborted reads）**；恢复后由
4.4 收割的 last_marker 同样可判 no-op。⇒ coordinator 崩溃重发 marker /
ReplayCommit 补发都安全（对照 Kafka ProducerStateManager 的同型去重）。

### 4.4 恢复收割（零新增持久化）

Log::open 的全量扫描顺带逐批 parse 批头（log.rs 两条扫描路均逐批
BatchHeader::parse，producer_id/epoch/attributes 全在头部，无需读 payload）：
- per-pid 最后 epoch（喂 idem 状态机）；
- 尾部未关事务（transactional 批后无同 pid/epoch 的 marker）→ 重建 txn_open
  的 first_offset ⇒ 重启瞬间 LSO 语义连续，**开事务数据在 abort 落地前
  不因重启泄露**；
- 已关事务 → last_marker 重建；abort 的区间 → aborted 集重建
  （read_committed 过滤跨重启有效，torn transactions 场景的根基）。

## 5. 协调面（txn coordinator 状态机与 EndTxn 两段）

```
Empty ──AddPartitionsToTxn──▶ Ongoing ──EndTxn──▶ Prepare{Commit|Abort} ──markers 全 ack──▶ Complete{Commit|Abort}
  ▲                 │超时/孤儿                     │                                        │
  └─────────────── 完结 ◀─────────────────────────┘── 崩溃恢复 ──▶ §6 接管判定 ◀──────────────┘
```

- **InitProducerId（事务路径）每调用 bump epoch**（TV2 每事务 bump 落点，
  §3）；AddPartitionsToTxn 携带该 epoch，协调器校验 txn_id↔pid↔epoch →
  Empty/Complete 则落 TxnLog Begin 记录（携带已 bump 的 epoch——client 用
  init 应答的 epoch 产数据，Begin 再 bump 会被 broker 幂等面 fence 掉合法
  流）→ Ongoing，登记分区清单（marker fan-out 范围）；同 epoch 重复 add =
  幂等扩分区。前事务 Prepare 残留 → CONCURRENT_TRANSACTIONS（可重试）。
  re-init 遇 Ongoing 先强制 abort（init 即 fence）；遇 Prepare 残留 bump
  照常（旧事务由分区侧自 abort 收敛 + §6 接管重驱收口）。
- **EndTxn**（key 26）两段：
  1. Ongoing→Prepare：**TxnLog 先写 Prepare 记录（含分区清单、outcome）并
     fsync**——arroyo §11 的所有权语义：「占有即授权外部提交/放弃」，
     这一步是 lost-writes 防线的落盘点；
  2. 并发向各分区 leader 下发 marker（内部 RPC，`internal.rs` 新增消息类型，
     各节点 meta actor 落到本地 `PartitionCmd::WriteTxnMarker`；应答语义 =
     §4.1 冻结提交面落定）；全 ack → commit 时先让 group coordinator 生效
     pending offsets（§7，证据已在 TxnLog）→ TxnLog 写 Complete → 应答
     客户端。
- **超时**：双保险——coordinator 对 Ongoing 挂 deadline（transaction.timeout.ms，
  broker 上限 15min 同 Kafka 默认），到期走 EndTxn(abort) 同路径；分区侧
  txn_open 自 abort（§4.1）兜住 TxnLog 清单外的漂移批与 coordinator 失联。
- **Torn transactions 封口**：Prepare 落盘后、Complete 前任意点崩溃，恢复时
  §6 判定补发 marker ⇒ 任一分区都不会出现「部分提交可见」。

## 6. 接管恢复判定纯函数（TASK.md 点名）

无 I/O，输入全部来自 TxnLog 重放结果，可单测、可进 TLA+（arroyo §11
`derive_checkpoint_state`/`resolve_candidate` 的同构物）：

```
resolve_takeover(rec) -> Takeover
  rec: Empty | Complete{..}            => Ready
  rec: Prepare{outcome, parts}         => ReplayCommit{outcome, parts}
                                          // marker 补发（§4.3 幂等）；
                                          // outcome=Commit 时附带重跑 pending offsets
                                          // 生效（§7 证据在 TxnLog，重放幂等）
  rec: Ongoing{pid, epoch, parts}      => Orphaned{pid, epoch, parts}
                                          // 恢复到的 Ongoing 一律转强制 abort
                                          // （安全方向，落地后转 Ready）——epoch 被
                                          // 超越/ deadline 已过只是其成因分类
```

三态映射（arroyo 对照）：`Committing → committed.json → ReplayCommit` ↔
`Prepare → Complete → ReplayCommit`。命名注（review P2）：arroyo 的
Orphaned 是终态停机（StopOrphaned = leader 自杀退休），basalt 用作
「强制 abort」行动态——同名异义，实现注释须防混淆。

## 7. 消费组交互（TxnOffsetCommit / KIP-447 essential）

- TxnOffsetCommit（key 28）：**pending offsets 随请求追加落 TxnLog**
  （review P1：内存桶 + 「丢失=未提交」论证只覆盖未提交事务；markers-ack
  窗口内 group coordinator/进程崩溃 = 事务已 ack 而位移静默丢失，恰是
  lost-writes 类——Kafka 在 TxnOffsetCommit 即写 `__consumer_offsets`）。
  带 producer epoch 校验——stale epoch 回 InvalidProducerEpoch(47)（§3 同一
  映射面，broker 侧验证 = KIP-447 的防御本体）。
- EndTxn(commit) 成功路径上，coordinator 通知 group coordinator 把 TxnLog
  中的 pending 记录冲入 OffsetLog（既有持久化点）；abort → 丢弃（回放时
  未转正 pending 跳过）。§6 ReplayCommit{Commit} 重跑同一生效动作（幂等）。
- 崩溃窗口残量：markers 全 ack 后、group 生效与 TxnLog Complete 落盘之间
  进程级崩溃 ⇒ 恢复路径由 ReplayCommit 覆盖（pending 证据在 TxnLog）；
  不再有「已 ack 提交而位移消失」的静默窗口。
- 完整 VerificationStateEntry（group generation 联动验证）= 已知边界（§10）。

## 8. API 面与宣告

新增 key（编号对官方 protocol 表核实过）：AddPartitionsToTxn=24（v0-4）、
EndTxn=26（v0-3）、WriteTxnMarkers=27（v0-1，**内部 RPC 复用此编号形态，
不对客户端宣告**）、TxnOffsetCommit=28（v0-3）、DescribeTransactions=65（v0）、
ListTransactions=66（v0-1）。FindCoordinator v4+ `KeyType=1(Transaction)` →
回 controller 节点地址（现 handler 对一切回 self，需分支）。宣告纪律不变：
只宣告 dispatch 真正实现的（api.rs 白名单）。错误码工作量：新增
`StorageError::InvalidProducerEpoch` → 47 映射（§3，现 produce 兜底
UnknownServer 会让 java 客户端无限重试，fence 形同虚设）。

## 9. 拓扑与可用性边界

- txn coordinator 单实例驻 controller 节点；TxnLog 为节点本地文件。
- controller 不可用 ⇒ 事务暂不可用（FindCoordinator 无应答/超时转移）；
  **普通 produce/read_uncommitted consume 不受影响，但受影响分区的
  read_committed 会因孤儿 txn_open 锚点停滞**（review P1：raftrs 是 ADR-16
  修订后的默认引擎，failover 后新主 TxnLog 空 → §6 判定全 Ready → 无从
  得知分区清单）。**前置缓解 = §4.1 分区侧 txn_open deadline 自 abort**
  （锚点超时自弃不依赖 coordinator）；TxnLog 经控制器 raft 复制列为后续项，
  与 ADR-16 openraft 补齐同批评估。v2 第一迭代事务验收面 = 传统单写者模式。
- 分区级：marker 下发只到当时 leader；中途 failover 由 §6 ReplayCommit
  重发兜底（幂等保证重发无害）。

## 10. 已知边界（诚实清单）

- KIP-447 完整形态（EndTxn 阻塞于消费组 generation 验证）不做——pending
  落盘 + epoch 校验已封 aborted-reads/lost-writes 主路径；
- DeleteRecords/retention 与开事务 LSO 锚点的交互：POC 约定删除点之下的
  开事务锚点前移到段边界（记日志），不追求 Kafka 的精确等价；
- 分区 actor 不强制「先 AddPartitionsToTxn 登记」（登记面在 coordinator）；
  发散的安全兜底 = §4.1 两条分区侧规则（终态 fence + deadline 自 abort）：
  同 epoch 僵尸续写被 last_marker 拒、漂移批/孤儿锚点被自 abort 收敛，
  可用性与 LSO 停滞风险均已封口；与 Kafka 严格注册校验（InvalidTxnState）
  的差异是少一层前置拒绝、多一层事后收敛，POC 取舍；
- TxnLog 无紧凑化（Ongoing 量级低，同 OffsetLog 先例）；无 GC 边界同 ADR-17。

## 11. 形式化验证计划（㊼ 纪律前置）

`spec/TransactionCommit.tla`：coordinator 状态机 × partition marker 应用 ×
消费可见性三变量群。不变式：
- InvNoAbortedVisible（abort 决定后其数据永不可见于 read_committed —— aborted reads）；
- InvMarkerOnce（同 (pid,epoch) 至多一个 marker 生效 —— torn transactions）；
- InvFenceClosed（**终态 marker 落地后同 (pid,epoch) 事务批不得 append、
  txn_open 不得复活** —— 与 §4.1 分区侧规则同构，zombie fence 的直述性质）；
- InvLsoBounded（LSO ≤ HW ∧ 单调）；
- InvCommitEffectDurable（**提交效果已现（任一分区 commit marker 已落/
  read_committed 可见）⇒ TxnLog 含持久 Prepare/Complete** —— lost writes；
  前件必须是「效果已现」而非「Complete 已落」，否则无 Prepare 突变体平凡绿）。

**判别力阴性对照（两只，防平凡绿/恒绿 cfg——账本 ㊼ 教训；review 预判
修正后的可达窗口）**：
① 无 Prepare 持久化突变体：崩溃窗口 = **Prepare 落盘后至 Complete 前**
   （含部分 marker 已落/效果已现的时点）——必须被 InvCommitEffectDurable
   检出（「Prepare 前崩溃」无可见效果，不算数）；
② 无 epoch/终态 fence 突变体：僵尸同 epoch 续写放行 ⇒ txn_open 复活 /
   终态后 append 放行——必须被 InvFenceClosed 检出（不能拿
   InvNoAbortedVisible 充数：LSO 锚住僵尸数据反而使其恒绿）。
名义全空间绿 + 两突变体红 = 三门禁；cfg 突变参数与本文档/账本三方一致。

## 12. 测试与落地切分（每块独立 review/收口）

- **a. 数据面**：LSO/控制批/过滤/收割 + marker 幂等 + 终态 fence + deadline
  自 abort 单测（partition.rs 测试模式照 idempotence_tests）；内部 marker
  命令先于 coordinator 可注入；挂起 fetch 三接触点的隔离级回归
  （含 on_deadline 超时回包泄露探针）。
- **b. 协调面 ✅（2026-09-16）**：TxnLog（5 类记录 + fsync Prepare 落盘
  点）+ 状态机 + EndTxn 两段 + §6 纯函数（takeover_tests ×4 表驱动）+
  超时 sweep abort + 接管恢复驱动（ReplayCommit/Orphaned）+ TxnOffsetCommit
  pending 落盘/提升钩子 + epoch fence；coordinator_tests ×8 全绿（两段
  闭环/重放/孤儿/超时/并发/concurrent fence/pending 提升）。
- **c. 协议面 ✅（2026-09-16）**：五个 API handler（24 v0-3 客户端形状 /
  26 / 28 / 65 / 66）+ api.rs 宣告 + FindCoordinator Type=Transaction 回
  controller（v1-3 KeyType）+ InitProducerId 事务分支（非事务路径原样）；
  TxnCoordinator 单实例驻 controller（txn_tx 双路：直发 / NotCoordinator
  16 协议自愈重路由）；TxnOffsetCommit 非 controller 节点内部 RPC 代理
  （MSG_TXN_OFFSET_COMMIT）；marker 路由器（本地直发 / 远端
  MSG_WRITE_TXN_MARKER + MetaCmd::BrokerAddr 查址）；
  probe_txn_api_layouts 全链字节级回归（⑰ 先例；review 实证后扩充 28/
  FindCoordinator v1+v4/NotCoordinator 面）。review 两个 P0 修复入档：
  ① Ctx.all_brokers 生产恒空（仅 metadata 响应缓存）——controller 路由/
  TxnOffsetCommit 代理改经 MetaCmd::BrokerAddr 查运行期地址 + Ctx 增加
  controller_id 推举值；② Java 3.x/franz-go 的事务 FindCoordinator 按
  broker 宣告版本发 v4+（KeyType 字段 v1+ 恒在）——查找无版本门。
  P1：storage_err_to_code 兜底改 15（83 常量实为 EligibleLeadersNot
  Available，事务面误用会给出错误重试语义）。
- **d. 验收面 🟨（2026-09-16 java 档通过）**：java kafka-clients 事务 e2e ✅
  （txnPhase ×4 验收：commit 流 read_committed 可见 / abort 流 rc 不可见 +
  uncommitted 可见 / aborted offset 消耗不复用 / sendOffsetsToTransaction
  提升到组——KIP-447 全链）；暴露并修复协议 int8 字段 as_i32 静默返零缺陷
  （㊿：IsolationLevel/KeyType 两处）；另有 FindCoordinator v1-3 扁平响应
  位即协调器地址的形状缺陷（探针实证后修复——Coordinators[] 是 v4+ 字段，
  librdkafka/kafka-python 事务查找读扁平位）。**待做**：franz-go 事务档；
  TLA+ 三门禁（§11）；T-M3.6 Jepsen 三场景进仿真 harness（≥500 seeds）；
  Describe/List 相位串已对齐官方命名（PrepareCommit 等）。

## 13. 与既有账本/机制的衔接

- T-M3.1 幂等状态机是本设计的直接地基（§3/§4.1），epoch 语义零改动复用；
- ㉟ 冻结提交面/ISR 语义不受影响——事务 marker 走同一 acks=all 提交面
  （marker 也是数据，须过提交面才算落定，否则 torn transactions 复现；
  §4.1 与本条一字对齐——review P1 抓过此处自相矛盾）；
- ㊼ 教训前置到 §11：阴性对照 cfg 的突变参数在门禁落地当天与文档三方核对。
