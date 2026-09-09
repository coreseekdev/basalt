# 存储 opfuzz 修复评审（2026-09-09，code review agent 探针实证）

> 结论：本轮三笔修复中 SimDisk::len 与 truncate_to active 区批对齐正确；
> **truncate_to_front 的反转修复仍不达标**（文件 truncate 只能删尾部，
> 无法删前缀），sealed 区分支与 log_start 持久化缺失均为 P0/P1。
> 全部结论经临时探针测试实证（探针已删，工作区干净）。

## P0-1 truncate_to_front 错误侧删除（丢已确认数据）
log.rs:633（sealed）/657（active）：循环推进 p 到"首个 end > offset 批"的
起点后 `truncate(p)`——保留的 [0,p) 恰是应删前缀，删除的 [p,EOF) 恰是应保留
数据。实证：delete_records(9) 后 crash+reopen，LEO 10→8，应保留 [8,10) 被销毁。
**修法（采纳 a）**：Kafka 语义——只整段删除 + 推进 log_start_offset
（读路径已按 log_start 过滤），跨线段不重写；log_start 持久化见 P1-1。

## P0-2 truncate_to 的 sealed 区分支（!target_seg）不删数据且重开复活
log.rs:795-801：只删 base ≥ offset 的段，包含 offset 的段原样保留却以
offset 重建空 active、next_offset=offset。实证：truncate_to(5) 后内存内
[5,6) 仍可读；crash+reopen 截断整体复活（next=6≠5）。
**修法**：复用 target_seg 批对齐逻辑——定位包含 offset 的 sealed 段，
截到批边界 kept_end，移出 sealed 作为新 active（base 不变），删其后所有段。

## P1-1 log_start_offset 不持久化
delete 后重开回退到首 sealed base，已删数据重新可读。
**修法**：checkpoint 增加 `start:<offset>` 行，open() 取 max(推导值, 持久值)，
delete/retention 后重写。

## P1-2 active 前缀 rescan 重置 next_rel 破坏 LEO 记账
采纳 P0-1 修法 (a) 后自动消失。

## P1-3（测试缺口）
- opfuzz.rs:178 early-return 放行"重开 LEO 停在 log_start"签名——clean 档
  应断言 tracked 非空 ⇒ next_offset > log_start，并断言重开 LEO ≥ 删除后下界；
- read(0, 1MB) 不跨段（read_ex 单段为界）——按 first_offset/批 last+1 循环 fetch 拼接；
- truncate 分支无条件 tracked.clear() 放弃核对——按 kept_end retain；
- log_test.rs 对 delete_records/truncate_to/log_start 零覆盖——先落
  <30 行确定性单测（P0 探针用例）再修。

## P2（一行级）
- truncate_to 批头短读/解析 break 前 `kept_end = base + rel_cur`（当前保持
  初值 offset，可能批内）；
- truncate 成功后 `self.batch_staging.clear()`（batch_io 下防旧 offset 流错位）；
- 按 kept_end 裁 `epoch_history` 尾部（end_offset_for_epoch 防虚报）。

## 已确认无问题
SimDisk::len 修复、truncate_to active 区批对齐、read_ex 的 log_start 过滤
语义、单写者/所有权模型。

## 执行顺序（下个会话）
1. 落 P0 探针为 log_test 确定性单测（红）；
2. P0-1 (a) + P1-1（整段删除 + log_start 持久化）；
3. P0-2 sealed 区批对齐 + 段提升；
4. P2 三项一行修；
5. opfuzz 按 P1-3 增强，解除 ignore，全绿收官 C7 前半。
