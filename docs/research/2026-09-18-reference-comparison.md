# 参考实现对照研究：KIP-848 消费组 / 分层存储 / 对象存储后端（2026-09-18）

> 任务来源：工作区项目清单报告 + 项目路径索引（2026-09-18）——按图索骥定位与
> basalt 当前三块新面（KIP-848 消费组、cooperative-sticky、分层存储 v1）直接
> 相关的参考项目，对照其实现研究差异。
> **克隆版本是本轮分析成立的关键前提**：kafka trunk（2026-09-07，KIP-848 GA +
> KIP-405 完整）、redpanda v26.3-dev（2026-08）、slatedb trunk（2026-09-05）、
> duramen（自研 S3 REST 子集）、iggy 0.9-edge、walrus v0.3——全部为 2026 新鲜
> 克隆，早期精读文档（stream-db/docs/01/02）的 4.5-SNAPSHOT 时代结论需要按本轮
> 增量修正。
>
> 方法：三个并行只读深潜（服务端协议面 / 分层存储面 / 对象存储后端面），
> basalt 侧基线以 2026-09-18 已落地代码为准（T-M3.3/T-M3.4/T-M4.3）。

## 0. basalt 侧基线（对照锚点）

| 面 | basalt 现状（2026-09-18） | 关键文件 |
|---|---|---|
| KIP-848 | ConsumerGroupHeartbeat 68 v0-1 宣告；单 member epoch + subsig 门重算；服务端 Range（per-topic 连续切块）+ uniform（movement-minimizing）；ServerAssignor first-wins；fence=epoch 单调（账本 54 修订）；全量 assignment 下发；HeartbeatIntervalMs 固定 5000；无 session timeout/static membership/rack | coordinator/src/consumer_group.rs、server/src/handlers_consumer.rs |
| cooperative | classic 组中继，协议选择 = leader 偏好 ∩ 全体支持集 | coordinator/src/lib.rs |
| 分层存储 | ObjectStore 四原语（create=CAS 回读）+ LocalFs/Memory；每段 immutable 记录=权威；写序=段对象→段记录→本地回收（release_sealed 不动 log_start）；读穿透整段读；启动期孤儿 GC；broker 级模式开关 | storage/src/{object_store,tiered}.rs、server/src/partition.rs |

## 1. 第一手验证（本报告撰写者直接读码，佐证后续 agent 结论）

- **Kafka 服务端 UniformAssignor 确为粘性**：`UniformHomogeneousAssignmentBuilder`
  读取 `memberAssignment(memberId).partitions()` 旧分配，仍均衡则
  `deepCopyAssignment(oldAssignment)` 原样保留（assignor/UniformHomogeneous-
  AssignmentBuilder.java:162-236）——basalt uniform 的 movement-minimizing 取向
  与上游一致；差异：Kafka 按 topic **Uuid** + `SubscribedTopicDescriber` 拉取
  分区数（元数据经协调器描述器），basalt 按名字 + Lookup 快照。
- **RangeAssignor（服务端 315 行）**：minQuota + extraPartitions 给前位成员的
  逐 topic 连续切块——basalt range_targets 核心一致；Kafka 额外有
  **co-partitioning**（同构组 + 等分区数时各 member 跨 topic 取相同分区号，
  服务 join 场景）与 rack 感知注记，basalt 未做。
- Kafka 服务端 assignor 体系是**策略插件接口**（ConsumerGroupPartitionAssignor
  + GroupSpec/SubscribedTopicDescriber），Range/Uniform/Simple 各自又分
  同构/异构 builder——basalt 的 if assignor == "uniform" 二分是可接受简化，
  但接口形状值得在 v2 对齐。

## 2. Agent A：KIP-848 服务端核心对照（kafka trunk）

> 缩写：GCS=GroupCoordinatorService、GMM=GroupMetadataManager、CAB=CurrentAssignmentBuilder、CGM=ConsumerGroupMember、TAH=TargetAssignmentBuilder（均在 group-coordinator/src/main/java/org/apache/kafka/coordinator/group/）

### 2.1 状态机与 epoch 体系

Kafka 成员四相：`STABLE / UNREVOKED_PARTITIONS（等 owned 确认撤销）/ UNRELEASED_PARTITIONS（等前 owner 释放）/ UNKNOWN（直接 fence）`（modern/MemberState.java:28-51，调和逻辑 CurrentAssignmentBuilder:208-243）。**三层 epoch**：groupEpoch（订阅/metadata hash 变化 +1）→ assignmentEpoch（=触发它的 groupEpoch）→ memberEpoch（调和到 assignmentEpoch）；仅当 groupEpoch > assignmentEpoch 才重算 target（GMM:4245-4250）。分区级 epoch 表（CGM:304）支撑 offset-commit fence，双 owner 直接 IllegalStateException。

basalt 对照：单一 member epoch + rebalances 计数，无相位。**丢掉两个正确性语义**：①owned 撤销确认闭环（basalt 的 owned 参数完全未用，旧 owner 未撤销前新 owner 即可拿分区——双 owner 窗口）；②target 与 current 分离的 reconcile 中间态（basalt 直接把 target 写进 member.assignment）。

### 2.2 分配器

内建仅 uniform + range（GCC:218-224），插件接口可扩展；协商是**组内多数派偏好**（computePreferredServerAssignor，成员请求变化触发 bump），非 basalt 的组级 first-wins 不可变。range 算法等价（minQuota+extra 前位、逐 topic 连续段），差异：Kafka 静态成员按 instanceId 优先排序、异构组共享游标接续、**订阅不存在 topic 抛异常**（basalt 容忍为 0 分区）；**内建 assignor 均不用 rack**（basalt 未做 rack 无碍）。uniform 同构 builder 同样以旧分配均衡性判定保留（与 basalt movement-minimizing 同向，builder 细节未逐条对齐，属独立实现可接受）。

**差分 vs 全量（重要）**：Kafka 线上下发仅三种情况带 Assignment——epoch==0 / full request / 分配已变（GMM:2715-2719）；keepalive 且未变时 **Assignment=null**。basalt 每拍全量下发，O(members×partitions) 带宽且下游无法用「Assignment=null ⇔ 无变化」这一 848 客户端普遍依赖的信号。

### 2.3 心跳会话面

- **Session timeout（P0 缺口）**：成员级 timer 心跳滑动续期（GMM:5242-5250）；超时=三张 tombstone（assignment/target/subscription）+ groupEpoch+1，回收由下一次任意成员心跳驱动（GMM:4931-4990）。**basalt 完全没有——成员崩溃即分区永久泄漏**，唯一阻断真实可用的缺口。
- MemberId：服务端统一 Uuid.randomUuid()（GMM:2595）；v1 起客户端必须自带（KIP-1082）。basalt `consumer-N` 形状可接受。
- HeartbeatIntervalMs：静态配置 per-group（默认 5000，GCC:203）——basalt 固定 5000 恰等于默认值，差距仅在不可按组调。
- Static membership：InstanceId 校验/顶替（GMM:3453-3471）；**epoch=-2 = 临时离开保留 assignment**（GMM:4698-4712）——basalt 只认 -1，-2 会被误当永久离开。

### 2.4 Fence 与错误面

- **FENCED_MEMBER_EPOCH(82) 精确条件**（GMM:1674-1700）：received > known → fence；received < known 且（≠previousMemberEpoch 或 owned ⊄ assigned）→ fence；**received==previous && owned⊆assigned → 放行**（应答丢失恢复）；已知成员 epoch==0 → 放行（fenced member recovery，GMM:1679-1683）。basalt 一律 fence，客户端只能重进。
- **fence 应答的 MemberEpoch = 请求原值**，非 basalt 的「回带服务端当前 epoch」（GMM:4691-4694）——basalt 的回带不是 Kafka 语义（功能收敛但每轮 join/leave 多一次 fence-重同步 RTT）。
- INVALID_REQUEST(42) 集中校验面比 basalt 宽（GCS:474-516：join 时 RebalanceTimeoutMs=-1、TopicPartitions 非空、names/regex 双 null、epoch<-2、静态离开无 InstanceId 等）。

### 2.5 协议面

- 68 v0/v1：v1 = SubscribedTopicRegex + KIP-1082；服务端 regex 全链（异步解析→解析成功才 bump epoch；订阅=names∪resolved）。basalt 的 INVALID_REQUEST 拒收是自设边界（能力缺失非裁剪）。
- 69 Describe：GroupState = `Empty/Assigning/Reconciling/Stable`（派生判定 ConsumerGroup.java:963-976）——**Assigning/Reconciling 是运维观测 rebalance 的主入口**；成员含 targetAssignment 与 assignment 双份 + MemberType。basalt 只回 Empty/Stable 且双份同值。
- classic↔consumer 同名分流/在线迁移（ConsumerGroupMigrationPolicy）：basalt 定位可保留，但需保证同名组不双栖。

### 2.6 建议定级

| 级 | 项 | 依据 |
|---|---|---|
| **P0** | session timeout（成员 timer+心跳续期+超时注销重算） | 成员崩溃分区永久泄漏，唯一阻断真实可用 |
| **P0** | 撤销确认闭环最小版（重算后被移出分区标 pending，owned 确认后才派新 owner） | 双 owner = 重复消费，正确性缺陷 |
| P1 | 差分下发（未变→Assignment=null + full-request 判定） | 成本极低，砍 keepalive 全量 payload，真实客户端行为假设 |
| P1 | previous-epoch 容错 + epoch-0 rejoin | 应答丢失不强制全员重 join |
| P2 | Describe 状态串三值 + assignment interval 节流（1s）+ group max size | 观测面/防风暴，各 ~20 行 |

有意 POC 边界（可保留）：固定 HeartbeatInterval=5000（=Kafka 默认）、first-wins 协商、无 rack（Kafka 内建也不用）、consumer-N 成员 id、leave 不验身份（与 Kafka 动态成员语义基本一致）、Describe 极简版。明确差距（非边界）：SubscribedTopicRegex 整链、static membership（-2 误处理）、rebalance timeout、按分区 assignment epoch 的 commit-fence 联动。

## 3. Agent B：分层存储对照（KIP-405 + redpanda）

### 3.1 Kafka KIP-405 要点（file:line 均在 kafka 仓库内）

- **元数据走内部 topic 事件流**（`__remote_log_metadata`，TopicBasedRemoteLog-
  MetadataManager），段元数据 `RemoteLogSegmentMetadata` 含 UUID segmentId、
  epoch→段内起始 offset 的 NavigableMap、四态状态机
  （COPY_STARTED/FINISHED → DELETE_STARTED/FINISHED）；
  **对象布局不下沉 broker**——RemoteStorageManager 是插件接口，无内置实现。
- **log_start 双计数**：全局 logStartOffset（remote+local 可读起点）与
  localLogStartOffset（本地保留起点）分离；remote retention 推前者、本地段
  删除推后者；`highestOffsetInRemoteStorage` 闸门防本地早删（UnifiedLog.java:
  162,226,384-397、RemoteLogManager.java:2104-2122）。
- **failover 断点不靠本地文件**：becomeLeader 从 leader-epoch 链逐级回退找
  remote 已有段最高 offset 作为恢复拷贝点（RemoteLogManager.java:905-916）。
- **删除两阶段**（DELETE_STARTED→删对象→FINISHED），先推进 start 再删对象；
  unclean 选举后按 epoch lineage 清孤儿段。
- 上传候选=非 active 段且 endOffset < LSO，带 lag 阈值（时间/大小）+ copier
  线程池 + copy quota 阻塞（:943-976,:1027-1050）。

### 3.2 Redpanda cloud storage 要点

- **manifest = 每分区单对象**，多版本（v1-v4）+ serde/json 双态 + 列存压缩
  （segment_meta_cstore DeltaFor 编码）；raft 侧 `archival_metadata_stm` 快照
  免重放（types.h:78-119、partition_manifest.h）。
- **段 key 编入 revision + term**：`{cluster}/kafka/{topic}/{part}_{revision}/
  {base}-{committed}-{size}-{term}-v1.log`——leader epoch 漂移 = 新 term = 新
  key，**天然不可变、永不覆盖**；替换走 replaced 列表 + active 引用保护
  （partition_manifest.cc:337-356、archival_metadata_stm.cc:1611-1630）。
- GC 三层：housekeeping 常驻周期 + S3 inventory 全局账本比对 + purger/
  lifecycle marker（防同名 topic 重建撞 key）。
- 读路径：chunk 化 hydrate（not_available/download_in_progress/hydrated 三态
  + 共享句柄防淘汰竞态）+ 段 offset 索引 + 异步 manifest 视图。

### 3.3 三方差异对照表

| 维度 | basalt v1 | Kafka KIP-405 | Redpanda |
|---|---|---|---|
| 元数据权威 | 每段一条 immutable 记录（CAS），store 为准 | 事件流进内部 topic + 四态状态机 | 每分区 manifest 对象（多版本）+ raft STM 快照 |
| 冲突消解 | CAS 回读；同 base 漂移 warn+**覆盖** | UUID 新内容=新对象，永不覆盖 | 新 term=新 key + replaced 列表 |
| failover/截断 | 单写者假设 | epoch 链回放断点；lineage 清孤儿 | STM truncate 严格对齐；重传段新 term |
| 读路径 | 整段 GET 进内存 + 批过滤 | 远端索引 LRU + 段级读 | chunk hydrate + 磁盘 cache + 索引 |
| log_start 推进 | 不动 + fetch 侧 tier_min 曝光 | 双计数分离 + remote 闸门 | manifest start_offset 截断即推进 |
| GC | 仅启动期 | 运行期 retention 兼重试器（两阶段删除） | 三层（housekeeping/inventory/purger） |
| 校验和 | 无（靠 CAS 字节比对） | sizeInBytes 元数据校验 | size 编入 key 名 |
| 上传触发 | produce 窗口收口同步 | 后台线程池 + quota + lag 阈值 | archiver fiber + backlog + run quota |
| epoch 维度 | 无 | 一等公民（段内 epoch 表） | 一等公民（key/manifest 双处） |

### 3.4 basalt v1 三风险点的各家解法

1. **同 base 段漂移（warn+覆盖）**：两家共同点 = **对象 key 与内容绑定**
   （Kafka UUID / Redpanda term），记录只增不改——覆盖会破坏 CAS 语义与
   旧读句柄一致性，v2 必须改 key 方案；
2. **上传在 produce 关键路径、无背压**：两家都完全异步化（后台线程/fiber +
   字节配额 + lag 阈值），触发按 lag 而非事件点；
3. **孤儿 GC 仅启动期**：两家都是运行期幂等后台任务 + 在途标记/宽限期区分
   在途与垃圾（Kafka 的 DELETE_STARTED 悬挂重删；Redpanda 的 replaced
   backlog）。

### 3.5 basalt v2 建议与可保留简化

**值得抄（性价比排序）**：①段 key 加内容维度（term/UUID），记录改追加式，
终结覆盖问题（改动集中 tiered.rs upload，性价比最高）；②上传异步化+配额+
lag 阈值（Kafka RemoteLogManager 最小子集）；③读路径 range read + 段稀疏
索引（S3 整段 GET 不可持续，抄 Redpanda chunk+index）；④孤儿 GC 泛化为
周期任务（在途集合/记录 mtime 宽限窗）；⑤段记录补 version+leader_epoch
字段（Redpanda segment_meta 最小子集）；⑥对象侧 retention +「先推 start
后删对象」安全序。

**可保留的 v1 简化**：CAS create+回读契约（与 put-if-absent 同形，正确且
面向云）；每段一条 immutable 记录 + list 前缀恢复（Redpanda 单 manifest 的
分布式退化版）；写序三步（崩溃安全方向一致，少一个 unreferenced 运行期
簿记）；release_sealed 不动 log_start + tier_min 曝光（Kafka 双计数的等价
最简实现）；broker 级开关 + 失败回退 local-only（渐进启用路径正确）。

## 4. Agent C：对象存储后端对照（slatedb / duramen / iggy / walrus）

### 4.1 slatedb：不自研 trait，复用 arrow-rs `object_store`（0.14）

方法面比 basalt 四原语宽：`put/put_opts/put_multipart/get/get_range/head/
delete/list/list_with_delimiter/copy/copy_if_not_exists/rename`，条件写统一
`PutOptions{ mode: PutMode::Create|Overwrite|With(Etag) }`（slatedb/src/
retrying_object_store.rs:13-17）。basalt 缺 `list_with_delimiter`（GC 省流量）、
`get_range/head`（大段部分读）、multipart——对 v1.1 段存储均非必需。

**S3 无原生 CAS 的三件套工程解法**：
1. 条件写按厂商抽象给 arrow-rs（S3/Azure=If-None-Match:*，GCP=
   ifGenerationMatch），业务只见 PutMode；
2. **重试 + ULID 自证**：条件写附 metadata `slatedbputid=ULID`
   （retrying_object_store.rs:23-27）；超时重试撞 AlreadyExists 时
   `verify_put_succeeded`（:130-150）回读比对 ULID——自己的写入视为成功。
   瞬态错误退避重试（100ms→1s），确定性错误（AlreadyExists/Precondition）
   不重试（:107-121）；
3. **Fencing = epoch + 零字节对象 CAS**（fence.rs:83-90、wal/store.rs:137-157：
   PutMode::Create 撞 AlreadyExists → Fenced）；manifest 按**单调 id 版本化**
   为独立对象（manifest/store.rs:166-173）——CAS 落在版本号上而非原地覆盖。

**契约等价性结论**：basalt 的 create 回读契约与 slatedb「PutMode::Create +
回读比对」等价；basalt 少 ULID 身份信息（同内容分不清自己的重放 vs 他人写
同内容），但段 immutable + 分区单写者下冲突即异常，语义足够；slatedb 的重试
包装可在适配层补。

### 4.2 duramen：**四原语全覆盖，v1.1 首选后端**

| basalt 操作 | duramen 支撑 | 证据 |
|---|---|---|
| get | GET Object + Range | duramen/src/s3.rs:362、gateway.rs:747 |
| put | PUT（ETag=内容 MD5） | s3.rs:354、gateway.rs:540 |
| create（CAS） | PUT + If-None-Match:* → 412 | s3.rs:453-458、gateway.rs:649-653（commit_version_cas/CasPrecond） |
| delete | DELETE（墓碑+异步释放，幂等） | gateway.rs:800、s3.rs:813-860 |
| list | ListObjectsV2（prefix/delimiter/continuation） | s3.rs:332 |

额外还有版本化、MPU、批量删、GetObjectAttributes。单二进制 `durd`
（0.0.0.0:9000），SigV4/presigned，可选 TLS——**basalt 适配器可直接用
arrow-rs 的 AmazonS3 构建器指向 duramen，不必手写 HTTP**。内容寻址在块级
（block_id=sha256），对象 key 仍用户命名：若 basalt 段 key 改为
`seg/{sha256}.log` 则 CAS 退化为纯保险栓（同 key 必同内容）；当前
`{base:020}.log` key 下冲突回读仍有单写者告警价值。

### 4.3 iggy / walrus 一句话

- iggy：segment 全本地（20 位定宽命名，与 basalt keys 同款），**无对象存储
  分层可抄**；
- walrus：Raft 流引擎的本地 mmap WAL（消费端），sealed 段只读复用思路可取，
  非对象存储面。

### 4.4 落地建议（basalt v1.1）

1. 先接 duramen：四原语全覆盖、同生态运维近端；真云 S3 复用同一适配器；
2. 适配层放 storage crate 内 `object_store_s3.rs`（feature 门控），直接依赖
   arrow-rs object_store 0.14（slatedb 同款），create = put_opts(Create) →
   412 时 GET 回读 → `Existed(bytes)`；
3. key 规范：S3 无根斜杠（strip 逻辑照搬 path_of）；建议集群/租户提为首段
   `{mount}/{tenant}/{cluster}/{topic}/p{n}/{seg|rec}/…`，保 list(prefix)
   语义且让 delimiter 服务端折叠；
4. 借鉴 slatedb：零字节 owner 对象 CAS 做 writer fencing；段对象加 crc32
   footer 端到端校验。

## 5. 结论与路线图建议

### 5.1 总体判断

1. **KIP-848 面设计方向正确，落后在生命周期管理**：basalt 的 epoch 单调 fence、
   服务端 Range/uniform、 Assignment=null 语义缺口与撤销确认闭环是三笔结构性
   差距；轮次量级小（合计约 300-500 行），建议列为 T-M3.3.1 收尾块。
2. **分层存储 v1 的「记录权威 + CAS + 写序」骨架与两家一致**，且比 Kafka 少一个
   unreferenced 运行期簿记、比 Redpanda 少 manifest 汇总对象——在 basalt 的
   规模下是合理退化。三笔 v2 结构项：段 key 加内容维度（终结覆盖）、上传异步化
   +配额、读路径 range+索引；两笔运维项：运行期孤儿 GC、对象侧 retention。
3. **duramen 是分层存储的天然后端**：S3 子集已覆盖四原语（含 If-None-Match:*
   条件写），arrow-rs 适配层即可对接；「dendro/duramen/basalt 自研三件套」的
   对象存储闭环在此合流。
4. **对象存储前缀规划**：`{mount}/{tenant}/{cluster}/{topic}/p{n}/{seg|rec}/…`
   （Agent C §4），段对象加 crc32 footer、writer 零字节对象 CAS fencing
   （slatedb 同款）随 v2 一并做。

### 5.2 行动清单（并入 TASK.md 的建议排序）

| 优先 | 项 | 来源 |
|---|---|---|
| P0 | 848 session timeout + 撤销确认闭环最小版 | Agent A §2.6 |
| P1 | 848 差分下发（Assignment=null）+ previous-epoch 容错 | Agent A §2.6 |
| P1 | tiered：段 key 加 term/UUID + 记录追加式 + 运行期孤儿 GC | Agent B §3.4①③ |
| P1 | tiered：上传异步化 + 配额/lag 阈值（移出 produce 关键路径） | Agent B §3.4② |
| P2 | duramen S3 适配器（arrow-rs 0.14 薄层）+ 段 crc32 footer | Agent C §4.4 |
| P2 | 848 Describe 状态串三值 / interval 节流 / group max size | Agent A §2.6 |
| P2 | 读路径 range read + 段稀疏索引 | Agent B §3.4③ |
| P2 | SubscribedTopicRegex 整链 / static membership（-2）/ rebalance timeout | Agent A §2.5-2.6 |

### 5.3 既有精读文档的增量修正

- stream-db/docs/01-apache-kafka.md 基于 4.5-SNAPSHOT：本轮补充 KIP-848 GA
  服务端（group-coordinator 模块 5 万行级，含 assignor 插件体系）与 KIP-405
  的 logStartOffset 双计数/epoch lineage 细节；
- 02-redpanda.md 补充 cloud_storage v4 manifest/列存 cstore/lifecycle marker；
- 新增对照面：slatedb（arrow-rs object_store 抽象 + ULID 重试自证 + 零字节
  fencing）与 duramen（S3 子集覆盖矩阵）为「对象存储后端」维度的首批结论。

### 5.4 方法备注

三路并行只读深潜（kafka trunk 209 个协调器 java 文件 / redpanda cloud_storage
C++ 树 / slatedb+duramen 全仓），全部论断带 file:line；basalt 侧基线由实现者
第一手复核（UniformAssignor 粘性、RangeAssignor 算法两处独立验证与 agent 结论
互洽）。本文档仅记录差异与建议，不改 basalt 行为；行动清单如采纳应走
TASK.md/ADR 流程。

