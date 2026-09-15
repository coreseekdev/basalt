# dendro WAL 处理方式 × TiDB X 对照调研（映射 basalt）

- 日期：2026-09-15
- 参考：`/home/nzinfo/src.db/dendro/docs/research/TiDB_X_SharedSST调研.md`、
  dendro `spec/02-wal.md`（定稿 v1）、`crates/dendro-core/src/wal.rs`（1007 行）、
  `recovery.rs`、`docs/design/提交管线重构.md`
- 结论先行：**basalt 与 dendro 的 WAL 在数据面已经同构**（组提交窗口、毒化
  语义、撕裂容忍分级、段不可变+退休安全界——各自的缺陷史甚至互为镜像），
  物理载体不同（本地盘 vs 对象存储）但那是谱系两端的有意取舍，不需要改。
  **真正的缺口在控制面**：raftrs 引擎的 raft 日志是纯内存（MemStorage），
  ack 点落在内存里——对照 TiDB X"Raft log 先落本地盘才 ack"，这是 basalt
  唯一必须补的洞。本会话引擎缺陷簇 ㉗㉘㉙㉛㉜ 五个全是"无 WAL"的补丁税
  （catch_unwind 安全网、cfg.applied、追平门控、resync、僵尸防护）——
  控制器 WAL（delta-A）落地后这层补丁大部分可拆。dendro 的 WAL 设计
  （段/帧格式/恢复分级/毒化）可直接移植，不需要发明。

## 1. dendro 对 WAL 的处理方式（机制清单）

从 SPEC 02 定稿 + wal.rs 提炼，按机制分组：

### 1.1 物理模型：段 = 唯一写入单元

| 机制 | 内容 | 来源 |
|------|------|------|
| 段即原子单位 | 对象存储无 append → 段 = 一个不可变对象，事务原子性 = "段对象整体存在或不存在"（slatedb 同款契约）| SPEC 02 §1 |
| 本地追加模式 | `supports_append` 存储：帧就地续写当前段、按 segment_bytes 封段（追加 32B trailer）；fallocate 预分配 + 偏移写 + fdatasync → durable 延迟 742→326µs p50 | P2-6e/f |
| 单调编号命名 | `{branch}/e{epoch}/{seg}.wal`，无 LIST 可寻址；恢复只靠 HEAD 探测 | SPEC 02 §1 |
| 无 rewrite | 修正 = 新对象；删除只有 GC | SPEC 02 §1 |

### 1.2 帧与段格式

- 帧头 24B（LE）：magic("DRNO") + version + frame_type(TXN/CHECKPOINT/
  FENCE/SEAL) + seq u64 + len u32 + **crc32c 只覆盖 payload**。
- 段尾 32B trailer：magic("ESAL") + frame_count + min/max seq + crc。
- **魔数区分帧与 trailer**（账本 #19 的 Kani H2 教训：旧实现按"剩余 ≤32B
  即停"会把小帧当 trailer 静默丢弃——封段回放丢已确认提交）。迭代器不做
  trailer CRC 校验——防符号执行成本爆炸（28B CRC 链让 Kani H2 状态爆炸）。
- FENCE 帧 = `{epoch u64}`，接管写权时的首帧。

### 1.3 组提交与 durability 三档

```
append(ty, seq, payload, durability)
  ├─ NoWait : 入队即返回，不唤醒刷盘（搭车：下组/段满/空闲节拍兜底）
  ├─ Group  : await_durable(seq)——事件驱动（入队即唤醒 flush 线程，
  │           延迟 ≈ PUT RTT 与 flush_interval 无关）
  └─ Always : flush_now()（单飞互斥）+ await_durable
```

- **durable 等待必须在提交串行锁之外**（P2-6 两段式）：等待持锁使缓冲永远
  只有 1 帧，组提交退化（8 并发 20 commits/s → 解耦后 160/s）。配套四件：
  in-flight 写集仍参与裁决、memtx 按 ts 有序安装、watermark = min(installed_max,
  min(in-flight)−1) 无间隙前沿、段退休安全界。
- **单飞互斥（P0-A）**：同一时刻至多一个 flush 在途——此前同段号并发 PUT
  双写同路径，最后写者胜，已 ack 帧被静默覆盖。

### 1.4 毒化语义（P0-D，SQLSTATE 40003 completion_unknown）

- 任何 flush PUT 失败即**毒化写者**：append 一律拒绝（不得在结果未知状态上
  叠加写）；flush 停止上传——**确定性失败的帧绝不持久化，失败 = 未提交**；
- **Uncertain 边界**：失败若为超时/断连，PUT 可能已成功——reopen 后回放可
  见，事务结果未知，客户端对账（按段为不确定域）；
- 唯一恢复路径 = reopen（新 writer + 恢复回放裁决真实状态）；
- 毒化同时停租约保活——"诚实"：不可用写者不假装活着。

### 1.5 恢复：epoch 升序回放 + 撕裂容忍分级

- 段路径嵌 epoch：`wal/{branch}/e{epoch}/{seg}.wal` → 恢复按 epoch 升序回放，
  **复合 ts = epoch<<32 | seq**，高 epoch 写自然覆盖低 epoch（脑裂安全）；
  陈旧写抑制：同 key 已有更高 ts 跳过；
- **撕裂容忍分级**：epoch **最后一段**的帧错误一律按 torn tail 容忍（SQLite/
  PG 同语义——追加+fsync 中途崩溃、掉电乱序持久化都无法与位腐区分）；
  非最后一段其后的段存在而本段帧损坏 = 真实腐坏，严格报错；
- `covered_seq` 已物化跳过；`probe_tail` HEAD 探测恢复起点；GC 后旧 epoch
  目录可能整体回收（探测 0 段 = 零帧，不报错）。

### 1.6 段退休安全界（retire_bound）

段退休（GC 推进 first_seg）必须以**"段内全部帧已安装"**为界：per-段
`seg_max_seq` 表，max seq 的 ts ≤ covered 才可退休。原因：两段式提交下
durable-but-in-flight 的帧可落在 checkpoint 帧之前的段里——无界退休会在
reopen 回放时跳过含在途帧的段 = **已 ack 提交丢失**（审计 R3-P0 实证）。

### 1.7 工程细节（防坑清单）

- 后台 flush 线程**持 Weak**（Arc 自环 → Drop 永不触发 → 遗弃分支的线程
  永生，GC 删分支后"复活"已删分支的 fence 对象）；
- 租约保活回调挂 flush 节拍（惰性续期只挂 commit 路径 → 无流量分支 TTL
  到期 → 提交全被拒且无自愈——"空闲写者自毒化"）；
- 失败重试复用同段号（不留空洞——空洞使 probe_tail 丢失其后所有段）；
  失败时数据放回缓冲头部（不丢帧）；
- 追加模式 will_close 时锁内同步推进段号（失败已毒化、成功已封口，窗口期
  新帧自然落下一一一段）。

## 2. TiDB X 对 basalt 有效的结论（从 dendro 调研转译）

| TiDB X 机制 | 对 basalt 的转译 |
|------|------|
| 持久层 = 对象存储唯一事实源；**本地盘只承载 Raft log（ack 点）与缓存** | basalt 倒置：数据面日志在本地盘（Kafka 传统形态），控制器 raft 日志却在**内存**——TiDB X 最不看重的"本地盘 Raft log"恰是 basalt 缺的 |
| 写路径：Raft log 落盘 → apply → ack；SST/发布全后台 | basalt 数据面已同构：produce → 窗口/多数派 → ack；retention/checkpoint 后台 |
| 复制货币 = 日志 + 文件版本变更（三副本共享不可变 SST） | 映射 basalt 分区 = tiered storage（段对象共享 + 只复制"日志 + 段清单变更"）——远期记录，当前不摇摆 |
| ack 点契约谱系：本地盘 Raft log+副本 HA（TiDB X）vs objstore durability 单副本（dendro） | basalt 数据面已声明契约（acks=all = 多数派 LEO；Os 策略靠副本、SyncEach 靠本地盘）——**控制器没有对应契约**：propose ack 点 = 内存复制 + 2s 快照 |
| compaction 出进程 seam | basalt 的对应重活：retention/恢复扫描——当前进程内可接受（数据量小），记录 seam 纪律 |

## 3. basalt 现状对照

### 3.1 数据面（storage/log.rs）——已同构，确认不摇摆

| dendro 机制 | basalt 对应物 | 结论 |
|------|------|------|
| durability 三档 NoWait/Group/Always | FsyncSchedule Os/SyncEach/OnRoll（Os = 不主动 fsync 靠副本，Kafka `flush.messages=MAX` 同型）| 同谱系两端，不改 |
| 组提交（事件驱动聚批摊薄 PUT RTT）| batch_io 窗口（ADR-14：drain→end_batch_window→settle）| 同构；dendro"等待在串行锁外"的教训对应 basalt 的 deferred produce/settle 收口——已同构实现 |
| 毒化（失败=未提交 + Uncertain 对账）| 窗口收口失败 = 窗口内 produce 全部错误应答（⑭ 修复）| basalt 更强：本地盘 SyncEach 失败 = 已写盘，checkpoint 回滚 + 截盘可判定（⑭ 的语义）；无需 Uncertain 档 |
| 撕裂容忍分级（末段容忍/非末段严格）| opfuzz torn write 注入 + scan_and_truncate 批 base 连续性校验（⑳）+ 轻量恢复路径 base==next_offset 前置（⑲）| 已同构，opfuzz 四档 80 种子+2000 种子扩量已验证 |
| 段不可变 + 退休安全界 retire_bound | 段滚动后只读 ✓；retention/checkpoint 重写（⑨ 教训：truncate/retention 同步重写 checkpoint）| 语义等价，但缺**显式不变式**："段可删 ⇔ 段内最大批 LEO ≤ 保留前沿"——建议升格为属性测试（delta-C）|
| 帧魔数区分 trailer（#19 教训）| 批头 magic/length/crc 三重校验 + crafted 报文 Kani（C13）| 已同构 |

### 3.2 控制面（ctrl_raft/raftrs_engine.rs）——唯一真实缺口

现状：raft-rs `MemStorage`（纯内存），重启后靠 state.json 快照（2s 周期）+
leader 日志重放追赶。本会话为此付出的**补丁税**（全部是"无 WAL"的一阶和
二阶后果）：

| 补丁 | 对应缺陷 | 有 WAL 后 |
|------|------|------|
| catch_unwind 双安全网（step + ready 周期）| ㉗ commit_to fatal!（心跳 commit 越过空日志）| 恢复后的 last_index/term 来自 WAL，commit 越界窗口消失；安全网可保留为纵深但不再有已知触发面 |
| cfg.applied + apply_snapshot 恢复 + meta 写序纪律 | 同上 | 快照仍是优化（WAL 截断基点），但**正确性不再依赖快照文件**——冷恢复 = WAL 全重放 |
| engine_state_ready 追平门控（applied >= leader_commit）| ㉜ 僵尸 leader（陈旧快照服务 metadata）| 重启即从 WAL 恢复到崩溃前 committed，追赶窗口≈0；门控退化为普通水位检查 |
| 心跳响应 resync（matched 下调 + become_probe）| ㉛ matched 残留 | 同上，触发面消失 |
| RestoreTests 全套 + probe_meta_split.py | 回归锁 | 保留（机制不变，触发面缩小）|

另两个对照结论：

- **ack 点契约缺口**：控制器 propose 的 ack 点 = "多数派内存复制 + 本节点
  apply"——TiDB X 最基础的"Raft log 落本地盘才 ack"不满足。Controller
  kill 场景（ctrl_kill）能过，靠的是存活节点重放；若**多数派同时重启**
  （全停机升级），内存日志全灭，只剩 2s 前的快照——**已 ack 的元数据变更
  （建题/转移）可丢**。这是数据正确性缺口，不只是可用性优化。
- **epoch 路径安全性**：dendro 把 epoch 嵌段路径获得脑裂安全恢复。basalt
  控制器的等价物天然存在：raft term + 日志索引。WAL 段路径嵌
  `{term}/{seg}` 或帧头带 (term, index) 即获得同构保证（raft-rs 的条目
  本就带 term，恢复校验日志匹配即可）。

## 4. 可落地 delta（按性价比排序）

### A. 控制器 raft WAL（近期，中等工作量——ADR-17 候选）

实现 `RaftWalLog`：实现 raft-rs `Storage` trait（MemStorage 同接口：
initial_state / entries / term / first_index / last_index / snapshot），
物理载体直接移植 dendro 帧格式：

- 帧：24B 头（magic "BRWL" + version + type + (term,index) u64×2 + len +
  crc32c-payload-only）；type = ENTRY | HARDSTATE | SNAPSHOT_MARKER；
- 段：`{data_dir}/ctrl-raft/{seg:020}.wal`，segment_bytes 封段 + 32B trailer
  （frame_count/min/max index/crc）；控制器元数据低频（~10 条/天量级），
  **不需要组提交**——SyncEach 语义（每批 append + fdatasync）延迟无感；
- 恢复：顺序重放 [first..tail]，末段撕裂容忍（dendro 分级语义），HARDSTATE
  取最大 term/voted_for，SNAPSHOT_MARKER 之前的段可 GC；
- 写路径：RawNode::ready() 的 hs/entries 既有调用点不变（driver ② 段落），
  MemStorage → RaftWalLog 替换，`advance` 语义不变；
- 收益：① 控制器 ack 点从内存升格为本地盘 durable（对齐 TiDB X 最低线）；
  ② 多数派同停机后元数据零丢失（当前会丢 2s 快照窗口）；③ ㉗ 家族补丁税
  大部分可拆；④ 快照线程从"正确性依赖"降级为"WAL 截断优化"。

### B. ack 点契约文档化（纯文档，立即可做）

写进 ADR-17 或 README 的契约表：数据面 acks=all = 多数派 LEO（副本数/2+1
floor）、acks=1 = leader 本地（FsyncSchedule 决定 fsync 点）；控制器
propose ack = raft commit（A 落地后 = 本地 WAL durable + 多数派复制）。
对照 TiDB X/dendro 的谱系声明（防性能对比口径混淆——dendro 调研 §3-E 同款）。

### C. 段退休安全界不变式化（小）

dendro retire_bound 的 basalt 版：**"段可被 retention 删除 ⇔ 段内最大批
LEO ≤ log_start（或 checkpoint covered）"**。当前靠 ⑨⑱⑲⑳ 回归测试隐式
覆盖，升格为 storage 属性测试（conformance 风格：任意删段决策后，重开
LEO ≥ 删除前 acked LEO − 删除段外字节），防未来 retention 改动重蹈。

### D. 复制货币映射（远期记录，不启动）

TiDB X"复制日志 + 文件版本变更、数据文件共享"映射到 basalt 分区 tiered
storage：段对象（不可变）共享于副本/对象层，复制面只传"追加日志 + 段清单
变更"。当前 RF=3 全量副本是 Kafka 传统形态（副本即数据），无 3× 放大痛点
（分区天然分片），**不摇摆**。触发条件：对象存储形态需求 / 冷数据分层需求
真实出现。

## 5. 不照抄的部分

- **对象存储持久层**：dendro WAL 的"段 = 不可变对象、PUT 一次成型"是
  OSS 无 append 的妥协设计；basalt 本地盘有真 append + fsync，StdDisk
  直写 + 段滚动已经是最简正确形态。可搬的是**语义与不变式**（撕裂容忍
  分级、退休安全界、毒化错误语义），不是物理载体。
- **组提交全套 plumbing**：dendro 为摊薄 OSS RTT 而生（PUT RTT 主导）。
  basalt 数据面 batch_io 已覆盖该需求；控制器元数据低频到组提交无意义。
- **shared cache 层 / 三副本共享 SST 动机**：basalt 分区日志按 partition
  天然分片、副本即数据，无 dendro/TiDB X 的存储放大问题维度。
- **Uncertain 对账档**：dendro 的 OSS PUT"可能已成功"不可判定；basalt
  本地盘可截盘判定（⑭），更强的语义不需要降级对齐。

## 6. 行动建议

| 时点 | 动作 |
|------|------|
| 现在 | ① 本文档入 docs/research（已完成）；② delta-B 契约表写进 ADR-17 开篇 |
| 下一会话 | delta-A：RaftWalLog 实现 raft-rs Storage trait（帧格式/恢复分级按 §4-A），raftrs 引擎 MemStorage 替换，restore 测试升级为"进程重启"语义（快照+WAL 联合恢复）；完成后拆 ㉗ 家族补丁（保留纵深）|
| 触发后 | delta-C（retention 改动时）；delta-D（对象存储/tiered 需求出现）|
| 不做 | OSS 持久层、组提交 plumbing 移植控制器、Uncertain 档、三副本共享形态 |

## 参考

- dendro `spec/02-wal.md`（WAL 定稿 v1：段模型/帧格式/组提交/毒化/恢复）
- dendro `crates/dendro-core/src/wal.rs`（实现：单飞互斥/追加模式/毒化/Weak 线程）
- dendro `crates/dendro-core/src/recovery.rs`（epoch 升序回放/撕裂容忍分级）
- dendro `docs/research/TiDB_X_SharedSST调研.md`（TiDB X 机制与谱系分析）
- dendro `docs/design/提交管线重构.md`（Adjudicator/Journal 分离，Journal 可替换为 Raft）
- basalt ADR-14（batch_io 窗口）、ADR-16（多引擎）、账本 §12 ㉒-㉞（引擎缺陷簇）
