# ADR-21：分层存储与存储模式插件（T-M4.3）

- 状态：✅ v1 落地（2026-09-18，`5ae39cc`；实现提交含账本 53/54 联调修复）
- 依据：TASK.md T-M4.3 + ADR-11 注册表行；前置阅读（硬性）：
  Arroyo §11（arroyo-state-protocol 精读，CAS + 权威记录路线）与
  Morax §6（不可变对象 + RDS 权威路线）两路并排评审。

## 1. 两路并评结论（前置阅读的评审记录）

| 维度 | Arroyo（§11） | Morax（§6） | basalt 取舍 |
|---|---|---|---|
| 所有权 | 对象存储条件写 CAS + epoch record 为权威；fence 仅 advisory | RDS 单一权威，无 CAS | **取 Arroyo**：CAS 契约（create put_if_not_exists + 冲突回读）；RDS 权威违背单二进制立场（Morax 警示①） |
| 对象形态 | 5 类协议对象，immutable 为主 | 不可变对象 + 区间注册 | 取不可变段对象；**不做**每分区一对象（ADR-11 立场）——段粒度（20 位 base offset 命名与本地段对齐） |
| 孤儿对象 | GC 三纪律（分类先行/manifest 最后删/并发上限） | 孤儿无害性：先写对象后提交元数据 → 孤儿=垃圾，无丢失方向 | **两路合成**：写序 = 段对象 create → 段记录 create（权威）→ 本地回收；孤儿（有对象无记录）= 垃圾，GC 义务 sweep |
| 元数据 | 指针 ≠ 事实：manifest 指针只是候选，恢复前对照所有权记录 resolve | — | 分层注册表的权威 = 每段一条 **immutable 记录**（create CAS）；任何内存/指针视图只是缓存 |
| 测试 | 协议 crate 化 + 4 原语 + 纯函数 + 内存假体 → 50 测试全离线 | testcontainers 真依赖 | **取 Arroyo**：MemoryObjectStore 假体 + 纯决策函数，单测零外部依赖；S3 适配器 = 同契约换入（POC 边界） |
| 读路径 | — | 无范围读整对象读（警示④） | 段对象整读可接受（段 ≤ segment_max_bytes 有界）；索引复用本地段格式 |

## 2. 架构（v1 边界）

**原语层（storage/src/object_store.rs）**：`ObjectStore` trait 四原语
（get / put / **create**——CAS put_if_not_exists，冲突回读已有字节 /
delete + list）。v1 实现：`LocalFsObjectStore`（CAS = O_EXCL 原子创建，
同 fs 语义成立）+ `MemoryObjectStore`（测试假体）。S3 适配器同契约换入，
非 v1 目标。

**分层注册表（storage/src/tiered.rs）**：
- 段对象 key：`{topic}/p{partition}/seg/{base:020}.log`（与本地段命名
  对齐，20 位 base offset）；
- 段记录（权威，immutable create CAS）：`{topic}/p{partition}/rec/
  {base:020}.json` = {base, last_offset, bytes, key}；
- 写序（Morax 孤儿无害方向）：段对象 create → 段记录 create → 本地段
  回收。任何一步失败重试安全：对象已存在 = 幂等成功（回读比对）；
  记录已存在 = 已上传；记录成功前的孤儿对象 = GC 义务；
- 回收 = `Log::release_sealed(base)`（新 API）：从 sealed 移除 + 删本地
  文件，**不推进 log_start_offset**（分层数据仍可读，Kafka 分层语义：
  log start 由 retention 决定，不由分层回收决定）。

**读路径**：fetch 请求 offset < 本地首段 base 时走读穿透——段记录 →
get 段对象 → 原始批流直连（与本地 .log 同格式，零转换）。本地段区间照旧。

**恢复**：partition actor 启动（tiered 模式）list 段记录前缀 → 重建
内存注册表；本地 log 照常 open；有效本地起点 = max(log_start, 首个本地
sealed base)。

**GC（v1 sweep）**：list 段对象 vs 段记录集合 → 无记录的孤儿对象删除
（对象 mtime 老于阈值才删，防上传在途竞态）。

**存储模式**：v1 = broker 级 `BASALT_STORAGE_MODE=local|tiered`（默认
local）。per-topic storage-mode（CreateTopics configs → TopicMeta →
controller raft 复制）为下一块边界——元数据面穿越控制器，独立成块。

## 2.5 v1.1（2026-09-18，研究行动清单落地）

对照研究（docs/research/2026-09-18-reference-comparison.md §3/§4）三笔
结构项当日落地：①段 key 编入 leader epoch（记录追加式，同 base 漂移
不再覆盖——Redpanda term-in-key 同款）；②上传异步化（OffloadJob 通道 +
专用 OS 线程 offloader + 字节配额；嵌套 block_on panic 实证后弃
tokio::spawn 改 OS 线程——阻塞 IO 专用线程纪律同 Log::open）；③运行期
孤儿 GC（mtime 宽限窗，retention sweep 周期驱动）。读穿透 range 化
（记录带 crc32c + 稀疏批边界索引）。**duramen S3 适配器落地**（arrow-rs
0.14 AmazonS3 薄层，feature objstore-s3 默认开）：实机联调发现并推动
修复 duramen Last-Modified 头格式（ISO-8601 → RFC 2822，duramen
2f2a8ec）——produce→上传→读穿透→重启恢复 400/400 全链 PASS。

## 3. 已知边界（v1）

- cloud-direct（S3）适配器未接：契约面已按 Arroyo 四原语收窄，换入即用；
- per-topic 存储模式未做（broker 级开关）；分层的 retention 联动（对象
  侧过期删除）未做——对象保留全量，本地侧 retention 照旧；
- fetch 对已回收段的读穿透为整段对象读（段有界）；无跨段合并优化；
- 控制器/多节点：tiered 对象存储为节点本地配置（各副本各自上传各自的
  段对象，key 含 base offset 天然幂等——同 base 同内容，create CAS 去重）。

## 4. 验收面

- a. 原语 + 注册表单测（MemoryObjectStore 假体：CAS 冲突回读/幂等重试/
  孤儿 GC/写序崩溃点各态）；
- b. Log::release_sealed 单测（回收不动 log_start/读路径让位）；
- c. e2e：`BASALT_STORAGE_MODE=tiered` produce 120 条 ×小段 → 本地段
  回收（文件消失）→ fetch 读穿透返回全量 → broker 重启后再读全量
  （恢复重建注册表）→ local 模式回归不受影响。
