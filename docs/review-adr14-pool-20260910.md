# ADR-14 窗口收口 / BufferPool Arc 贯通 / 性能路径 评审（2026-09-10，四轮 code review 探针实证）

> 评审范围：commit 3282f17 之后合入的实质性变更——9fe3f0a（end_batch_window）、
> ffc7b9c + 7d5f11d（BufferPool Arc 贯通）、582b071（read_ex 单次大读 + 响应编码零拷贝）。
>
> **结论：1 个 P0（7d5f11d 重构事故——internal RPC server 未接线，多节点全挂）、
> 3 个 P1（read_ex 大批补读被 break 击穿=消费活锁；append 失败后 staging 残留=
> 僵尸数据+offset 重复；TruncateTo 丢弃窗口内低于截断点的 staged 批+假成功结算=潜伏）、
> 6 个 P2。** 全部 P0/P1 结论经临时探针跑出红/绿证据（探针已删，工作区干净）。
> 除 P0-1 外，opfuzz 四档 80 种子与 workspace 全测保持全绿——即现有验证体系对
> 上述缺陷均无覆盖（详见 §5 缺口）。

## P0-1 internal RPC server 未接线（7d5f11d 重构事故，多节点部署整体失效）
`server/src/main.rs:77-107`。3282f17 版本的 main.rs 在 `tokio::spawn` 内调用
`internal::serve(internal_listener, ctx).await;`；7d5f11d 改造该块时把这一行删了，
spawn 块只剩构造 InternalCtx（且 :100 又 new 了第二个未共享的 `Arc::new(BufferPool::new())`）
后即结束。`internal_listener` bind 后无人 accept，`internal::serve`（internal.rs:381）
成为死代码。

- **证据（静态）**：`cargo build -p basalt-server` 11 条警告，含
  `function serve is never used`、`unused variable: internal_listener`、
  `variant FetchSlice is never constructed`（分区 actor 的 FetchSlice 指令
  唯一发送方在 serve 下游——整条调用图死亡）。
- **证据（运行时探针，已删）**：启动真实二进制，对内部端口发 MSG_CREATE_TOPIC
  帧 → TCP 连接成功（backlog）但 2s 无响应；对照客户端端口 ApiVersions 正常应答
  （证明探针方法有效）。输出：
  `CLIENT-PORT ApiVersions -> 有响应；INTERNAL-PORT CREATE_TOPIC -> 超时无响应`。
- **后果**：多节点 POC 全部内部 RPC（Register/Heartbeat/MetaSync/CreateTopic/
  FetchSlice）静默挂死——`InternalClient::call` 无读超时，follower 拉取永久阻塞、
  非控制器节点注册/心跳永久阻塞 → 控制器判死 → failover 风暴。单节点
  （controller_tx 直连）不受影响，故现有测试全绿。
- **修法**：恢复 spawn 块内 `internal::serve(internal_listener, ctx).await;`，
  删除块内第二个池、改用共享 pool；补最小冒烟测试（内部端口 CREATE_TOPIC
  应答断言），并把"多节点内部端口应答"纳入启动自检。

## P1-1 read_ex 大批补读后 break——三轮 P1-1 反活锁修复被 perf 改造击穿
`storage/src/log.rs:565-576`。want 上界 = max_bytes + 4KB + 61（封顶段剩余）。
首批批体越窗时进入补读分支：`raw.resize` + `read_at` 把完整批体读入 raw——
**随后无条件 `break`，补读结果从不参与解析**。循环带 `out.is_empty()` 退出，
ReadResult.data 为空、HW 照常返回。

- **后果**：消费者以小 PartitionMaxBytes（< 批长 − 4KB）拉取含大批的分区时，
  每次拉取空响应（HW 前探）→ 永久活锁。这正是三轮 review P1-1 修复的契约
  （"首批即便超预算/超窗口也整批带上，防消费活锁"）。
- **回归溯源**：582b071^（重构前）实现直接 `read_at` 批体进 `out`，整批返回；
  582b071 引入窗口后丢该能力；3282f17 补回补读分支，但把 break 留在了补读之后，
  修复形同虚设。
- **探针证据（红）**：`probe4_tmp_read_ex::read_ex_semantics_differential` 场景 8：
  30KB 批 + `read_ex(0, 1024)` → `返回批数=0 data=0B hw=10`（断言 ≥1 批失败）。
  同测试场景 1-7、10（全量读、跳批、预算越线批、HW 批粒度封顶、跨段续读拼接、
  log_start 过滤/OOR、空读）全绿——探针分辨力自证。
- **修法**：补读成功后不 break，回到解析（重读批头）；为防短读死循环，
  记录"本 pos 已补读过"标志，二次不足再 break。

## P1-2 append 失败后 batch_staging 残留 → offset 回卷复用 → 僵尸数据落盘 + 成功批 offset 重复
`storage/src/log.rs:931-955`（rollback_to）+ `:442-449`（commit_staged）。
append 错误路径回卷 next_offset/HW 但**不清 batch_staging**（`clear_batch()`
:485 存在却无任何调用者）。窗口内失败批的字节留在 staging；下一批 produce
按回卷后的 next_offset 重新分配（与失败批同 offset）；end_batch_window 的
flush 把 [失败批][新批] 一并写盘。

- **后果**：①按错误应答的 produce 数据落盘可读（僵尸数据——错误应答的语义是
  数据不存在）；②同一 offset 区间两批并存，消费端读到 offset 回卷流。
- **探针证据（红，actor 级确定性）**：`server/src/probe_tmp.rs::flush_failure_
  leaves_stale_staging_and_reuses_offsets`（SyncEach+StdDisk 首窗口 sync 失败
  作为确定性注入点）：produce A 报错（NotFound）、produce B 以 base=0..4 成功
  （复用 A 的 offset）；最终文件 `batch bases=[0,0]`，失败批 payload "A-0" 与
  成功批 "B-0" 同时在文件中。断言 `!has_a` 失败=红。
- **覆盖缺口**：opfuzz 注释自认"ENOSPC/瞬时错误注入后续接入"——写失败 × batch_io
  交互无任何模型覆盖。
- **修法**：append 错误返回前（或 rollback_to 内）调用 `clear_batch()`；
  opfuzz ENOSPC 档落地时补 batch_io × 写失败矩阵。

## P1-3 TruncateTo 丢弃窗口内低于截断点的 staged 批，且按 success 结算（潜伏）
`storage/src/log.rs:825-828`（truncate_to 无条件 `batch_staging.clear()`，只按
文件内容批对齐）+ `server/src/partition.rs:443-469`（TruncateTo 结算：deferred
produce 中 `last_offset < offset` 的按 **success** 应答后照常截断）。

batch_io 窗口内 staging 持有未写盘字节时，`truncate_to(offset)` 会把**完整位于
offset 之下**的 staged 批一并 clear——它们从未落盘，也不在截断保留范围内。
partition.rs 却把这类 produce 按 success 应答（数据"已在截断点之下"），形成
"ack 成功但数据从未写盘"。

- **探针证据（红，log 级确定性）**：`probe4_tmp_truncate_window::
  truncate_to_must_not_drop_staged_batches_below_offset`：staging 持有 A(0..4)+
  B(5..9)，`truncate_to(7)` → `next_offset` 实得 0（契约要求 5，A 应存活），
  重开 LEO=0。**对照**：`control_truncate_noop_keeps_staged`（truncate ≥ LEO
  no-op）绿；`snapshot_current_behavior_for_discrimination`（固化现状
  next_offset==0）绿——证明探针测的是真实行为而非恒真。
- **可达性（重要收敛）**：仓库内 TruncateTo 唯一发送方是 FollowerPull
  （meta.rs:476），它严格 await 前一 produce 应答（settle 发生在窗口收口后）
  才可能发 truncate → **同组场景今日不可达**，故降为 P1 潜伏而非 P0。但这是
  ADR-14 窗口语义在 storage/actor 边界的未闭合缺口：任何新接线（admin 截断
  API、控制器 leader 侧截断、opfuzz actor 化）都会引爆。
- **修法（二选一）**：① TruncateTo handler 开头若 `batch_staging` 非空先
  `end_batch_window()`（flush 后截断，低于 offset 的数据真实保留，success
  结算转为正确）；② 更保守：`log.batch_io == true` 期间所有 deferred 一律按
  错误结算。建议 ①，并落 log_test 确定性用例（即本探针场景）。

## P2（一行/局部级）
1. **pool 读缓冲不归还，perf #2 "闭环"未成立**：read_ex 的窗口缓冲（log.rs:539）
   与输出缓冲（:544）冻结进 `ReadResult.data` 逃逸后无人 release；writer 归还的
   是 dispatch 新建的响应缓冲（conn.rs:225 `BytesMut::with_capacity(4+512)`，
   从未从池 acquire）→ 池只存在"响应→读窗口"单向流，读路径稳态每 fetch 净增
   窗口类+输出类两次分配。**探针**：`probe4_tmp_pool`（计数分配器）——对照组
   acquire→release 16 次循环新增 0B（计数器识别复用，探针非恒真）；read_ex
   稳态每次读新增 **32792B**（≈16KB 窗口类 + 8KB 输出类）。修法：ReadResult
   携带归还钩子，或 fetch 编码完成后 `Bytes::try_into_mut` → `pool.release`。
   正确性无影响（acquire `clear()` + resize 零填，无脏数据泄漏），纯复用失效。
2. **`Arc<BufferPool>` 违反本仓库 clippy.toml disallowed-types**（性能纪律
   "生产代码禁用 Rc/RefCell/Arc"）：conn.rs:22、handlers.rs:29、main.rs:73/100、
   meta.rs:70、partition.rs:121/144 共 8 处告警；clippy.toml 例外注释只覆盖
   SimDisk 测试基建。要么 clippy.toml 补 pool 例外并同步根 Cargo.toml 说明，
   要么改 channel 传递。
3. **StdDisk+SyncEach+batch_io 首窗口 append 必失败**：batch_io 下 append 未写盘
   即 `sync_file`（log.rs:422-427），StdDisk::sync_file（disk.rs:137）对不存在
   文件 `OpenOptions::append(true)` 无 create → NotFound。生产恒用 Os 故潜伏；
   opfuzz 用 SimDisk（缺文件 sync 为 no-op）故未暴露。探针：
   `stddisk_syneach_batch_io_first_window_fails` 红（作为 P1-2 的注入点复用）。
   修法：sync_file 打开句柄带 create(true)，或 batch_io 下 append 内跳过窗口 sync。
4. **truncate 后仍为 Leader 时 produce 继续接受并复用截断区 offset**：探针 B
   快照（持续 produce 压力 + TruncateTo(40)）：acked=150、终态 LEO=40、文件仅
   8 批——40 之上 110 批 success 应答后被截掉、offset 被后续 produce 复用
   （同 offset 两次 success、仅一批存活）。failover 丢 acks=1 数据本身是设计
   内（unclean 语义），但无 fencing 的 offset 复用会使消费端 offset 流回卷。
   修法：TruncateTo 处理中/后拒绝新 Assign produce（直至 epoch bump），或
   TruncateTo 前先 SetRole follower。
5. **窗口内同组 fetch 可读到 HW 已推进但数据未写盘的空窗**：同 drain 组内
   [Produce, Fetch] 时，fetch 走 `offset < HW` 立即读路径，读到的是 flush 前的
   文件 → 空响应 + HW 前探。窗口 µs 级、下次 poll 自愈，无活锁（客户端重试）。
   记录语义偏差；可选修：窗口内 fetch 延后到 end_batch_window 之后 serve。
6. **文档漂移**：disk.rs:3 "BufWriter 语义由 flush 控制"——StdDisk 实现无
   BufWriter（write_all 直写，符合 ADR-14 写边界定义）；应改注释而非行为。

## 已确认无问题（探针绿 / 读码确认）
- **read_ex 语义（perf #1 未破坏正确性的部分）**：全量读、from_offset 跳批
  （首批含 from）、预算越线批规则（Kafka 同型：返回 ceil(max/批长) 批、超幅
  ≤1 批）、HW 批粒度封顶（base<HW 的跨 HW 批整批可见）、跨段单段为界 + 续读
  拼接全量一致、log_start OOR 与跨线批整批返回、空日志/LEO 空读不报错、
  返回批流 CRC 与 offset 连续性——`probe4_tmp_read_ex::read_ex_log_start_
  filtering` + differential 场景 1-7、9、10 全绿。
- **窗口收口顺序**：run() 两条 drain 路径均为 process → end_batch_window
  （flush+SyncEach 补 sync）→ settle_deferred_produce → serve_pending/on_deadline；
  flush 失败时窗口内 produce 全部按错误应答（探针 A 的 o1 错误应答按原样送达）。
  acks=all 停等路径不经 deferred：parked ack 仅在 follower LEO（≤ 已 flush 数据）
  推进 HW 后放行，flush 失败不会造成"ack 而未写文件"。
- **pool 双重归还 / use-after-return / 脏数据**：writer 归还经
  `Bytes::try_into_mut` 唯一性门槛（conn.rs:37），无共享缓冲误归还；acquire
  `clear()` + 调用方 resize 零填，池内残留字节不可跨请求泄漏（pool.rs 单测 +
  对照组复用测试绿）；响应编码对 `Value::Bytes` 为 `extend_from_slice` 拷贝
  （codec.rs:268-279），池缓冲不会别名进入响应帧，长度前缀回填正确。
- **opfuzz 四档（clean/chaos × 标准/batch_io）80 种子全绿**；workspace 全测绿
  （除探针自身）。
- **ADR-14 缓冲 I/O 审计**：StdDisk 无 BufWriter（write_all 直写 page cache =
  定义的写边界，无"写后未 flush 可读"类依赖）；SimDisk 的 pending 可读是
  文档化的仿真 page-cache 语义；未发现以"未 sync 数据可持久"为依据的代码。
  唯一抽象旁路：`Log::persist_checkpoint`/open 用 `std::fs::write`+
  `sync_all` 直写 recovery.checkpoint（绕过 DiskIo，自带 fsync，语义安全——
  记录为旁路，非缺陷）。

## 执行顺序（下个会话）
1. P0-1：恢复 `internal::serve` 接线（一行）+ 删除块内第二池 + 内部端口冒烟
   测试（探针脚本逻辑转正为测试）。
2. P1-1：补读后 re-parse（防短读死循环守卫）+ 场景 8 转正为 log_test 确定性
   用例（红转绿）。
3. P1-2：rollback_to 清 batch_staging（或 append 错误路径调用 clear_batch）+
   opfuzz ENOSPC 档接入 batch_io × 写失败矩阵。
4. P1-3：TruncateTo 先 flush 再截断（或窗口内全错误结算）+ 探针场景转正。
5. P2 逐条：pool 归还钩子（配合基准验证 perf #2 真实闭环）、clippy 例外或去
   Arc、sync_file create、truncate fencing、注释漂移两处。
6. 账本（docs/VERIFICATION.md §12）登记本轮缺陷 → 机制映射。
