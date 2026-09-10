# 存储与服务器热路径性能审查（2026-09-10，性能 review agent）

> 总评：produce（+27% vs Redpanda）的主要余量在 append 的 staging 双拷贝链；
> consume 落后 29% 的根因是三个用户态叠加缺陷：read_ex 逐批双 pread +
> resize 零填充、BufferPool 只取不还（pooling 完全失效）、响应编码三重拷贝
> ——三者在 sendfile 之前就能拿回大部分差距。disk.rs Mutex 多分区争用
> 经核实不是问题（每 partition actor 独享 StdDisk）。

## Top 10（按 预期收益/实现风险 排序）

### 1. read_ex 逐批双 pread + resize 零填充（收益：高）
log.rs:520-559：每批 2 次 read_at（61B 头 + 体重读同页），resize 先 memset
再覆盖（~280MB/s 冗余 memset @143k msg/s）。修法：locate 后单次 pread
整块读入、内存顺批解析。验证：consume A/B + strace pread 计数 + perf memset。

### 2. BufferPool 只取不还（收益：高）
pool.rs:31-53：release 生产代码零调用；>1MB 时 resize 触发 realloc 整段
再拷贝，capacity 非 2 的幂进不了池。修法：writer 写完后
Bytes::try_into_mut 成功即还池（归还点 conn.rs:29-35）。
验证：dhat/jemalloc + page-faults + consume A/B。

### 3. 响应编码三重拷贝（收益：高，成本相对低）
conn.rs:216-225：out → framed（纯浪费）→ to_vec（纯浪费）。
修法：reserve(4) 占位长度前缀 + 回填 + freeze。~280MB/s 冗余 memcpy 消除。

### 4. produce staging 双拷贝链 + fast path 常态不命中（收益：中高）
log.rs:333-357/374/397-416：Assign 下客户端 base ≠ 分配值 → 稳态全走慢
路径；batch_io 下 raw→staging→batch_staging→write 三拷贝。修法：直写
batch_staging + 就地 patch，删中间 staging。验证：produce A/B + opfuzz
全量回归（触及回滚路径）。

### 5. 每请求 spawn + ctx 深克隆（收益：中）
conn.rs:52-74、handlers.rs:32-43：~153k req/s 下每请求 spawn + 3-4 次堆
分配（p99 1.5ms vs Redpanda 1.1ms 的主因）。修法：produce/metadata inline
dispatch，仅 fetch 保留 spawn。验证：延迟分位数 A/B + tokio-console。

### 6. StdDisk 每次 append/read_at try_clone dup syscall（收益：中低）
disk.rs:51-84。修法：File 留锁内直接操作。验证：strace -c dup 计数。

### 7. 写半：单响应单 write 无 vectored 合并（收益：中低）
conn.rs:29-35。验证：confluent-kafka 压测 + strace write 计数。

### 8. 【正确性】SyncEach × batch_io 顺序缺陷 + ack 先于落盘（收益：正确性）
log.rs:422-427（SyncEach fsync 的是未 flush 的旧数据）+
partition.rs:353（reply 早于 flush_batch）——batch_io 档存在"已 ack 不持久"
违反。与 opfuzz batch_io WIP（LEO 背离）同域，修法：batch_io 下 SyncEach
升级为组末 flush+fsync 后 ack；opfuzz 增加 SyncEach+batch_io+flush 前
crash 用例。

### 9. follower 空拉紧循环 + 复制热路径 eprintln（收益：低-中）
meta.rs:393-435/420/438。空拉退避 + 日志降频（多副本部署才有感）。

### 10. actor 杂项：advance_hw 双重 Vec 分配、10s 硬编码停等、Vec::remove O(n)（收益：低）
partition.rs:434-452/347/232-255/473-484。顺带清理。

## 维度结论
- 索引：二分 O(log n) 无问题；4KB 密度读放大被发现 1 吸收。
- 锁：per-partition StdDisk 无跨分区争用（非问题）。
- 与架构的根本冲突：sendfile/splice（M4，预期 consume +3x）要求 read 返回
  "文件+区间"，与 DiskIo 路径寻址（ADR-7）和 SimDisk committed+pending 语义
  冲突——需为 sendfile 增加真实盘专用逃生口并保持验证双轨。

## 验证方法
基准：benches/run_bench.sh + throughput.py（建议加 confluent-kafka 档——
当前 kafka-python 受 GIL 限制，会吃掉发现 1/3/5 的效果）；
syscall 画像 strace -c；分配画像 dhat/jemalloc；每项优化挂 config flag
做同规格 A/B；opfuzz + sim_disk 回归必跑（发现 4/8 触及持久化语义）。
