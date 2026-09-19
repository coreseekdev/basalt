# 生产就绪评估：可观测性 / 日志 / 管理面与数据面解耦（2026-09-19）

> 范围：四项评审——①距离产品化的缺失清单 ②可观测水平评估与框架提升设计
> ③日志使用合理性 ④管理面/数据面解耦（数据面过载不得阻塞管理指令）。
> 结论均已落为速赢修复或带编号的缺口项；share groups spike（已批准）的
> 设计约束见 §6。

## 0. 本轮速赢（已落地）

| 项 | 缺陷/缺口 | 修复 |
|---|---|---|
| 消费指标假值 | `messages_consumed`/`bytes_consumed` 标 `dead_code`——fetch 路径从未递增，**指标恒 0**（观测假象） | fetch 响应组装处按批头解析真实记录数/字节数递增 |
| METRICS_PORT=0 语义 | e2e 全部传 0 期望"关闭"，实际绑定**随机端口**起无人访问的 listener | 0 = 显式关闭（info 日志标注） |
| 健康面缺失 | 无 liveness/readiness 端点，K8s 探针无着落 | metrics HTTP 挂 `/health`（liveness，恒 ok）与 `/ready`（readiness = 路由非空，控制器快照已应用；空簇 503）|
| 41 处 eprintln | 绕过 tracing：不受 BASALT_LOG_LEVEL 管控、无级别/字段、与 tracing 输出交错 | 核心面（HB/SYNC/PULL/CTRL-CREATE/FC/LEO-REPORT/HW-ADV/ACK-DEADLINE 等 26 处）迁移 tracing（tag → target + 结构字段；高频降 debug、错误升 warn）；ctrl_raft 引擎诊断 13 处 + txn jepsen 测试 harness 3 处留存（测试/POC 面，§3 政策表中列为已知债）|

## 1. 距离产品化的缺失清单（分级）

### P0（无此不可上生产）

| # | 缺口 | 现状事实 | 方向 |
|---|---|---|---|
| 1 | **内部口（client port+1）无鉴权无加密** | Register/Heartbeat/MetaSync/**CreateTopic/DeleteTopic/FetchSlice** 全裸奔——可达内网的任何进程可删题/读数据/伪造 broker | 内部口复用 SASL/SCRAM（ creds 独立于客户端口）+ 可选 TLS；最少从"绑定内网地址 + 防火墙"降级为文档化的网络隔离前提（当前 RUNBOOK 未写） |
| 2 | **持久化默认不防断电** | `FsyncSchedule::Os`（page cache，进程崩溃不丢、**断电丢窗口**）且无 per-topic durable 开关暴露 | durable 配置档（acks=all + periodic/commit fsync）；与 ADR-14 batch_io 收口纪律合流 |
| 3 | **组/事务协调器 HA 面** | 组状态（classic/848/txn）单节点内存 + 本地文件；failover 后组协调不迁移（多节点 e2e 覆盖的是复制面，不是组面） | 组状态进控制器元数据或共享存储 + 组协调器选主迁移 |
| 4 | 可观测假值/缺口 | 本轮已修消费指标；**仍缺 per-topic/partition 维度、延迟直方图、队列深度、连接数**（§2） | 见 §2 框架 L1-L2 |
| 5 | 磁盘满/ inode 行为未定义 | append 失败 → produce error（兜底 15 可重试），但无主动水位告警/只读降级 | 水位检查 + 只读模式 |

### P1

- **配置热更**：BASALT_LOG_LEVEL 等启动静态；动态日志级别（EnvFilter reload）见 §2 L4。
- **优雅停机**：固定 2s drain + 无连接感知（活跃消费连接被硬切，客户端重连成本）；drain 时长可配 + in-flight 计数收敛判定。
- **ACL 骨架补全**（本轮已立骨架）：txn-id 闸门、host 匹配、PREFIXED 推广到 delete/describe、多节点 ACL 复制（现单机文件）。
- **quota 维度**：per-user 已做；(user, client-id) 二维与 broker 维带宽面留待。
- **消费组持久化 HA 与 offset 面的 __consumer_offsets 兼容**（外部工具依赖）。
- **多节点元数据快照兼容性测试**（账本 62 族：快照/记录线任何扩展必须带跨重启/跨节点回归——本轮已立惯例）。

### P2

- thread-per-core / io_uring（T-M4.4 既有项）；DescribeConfigs per-topic 覆盖动态化；kafka-python 裸 SCRAM 兼容（pre-KIP-152）；SCRAM-512/OAUTH；重试风暴夜间扩量（T-Q.4）。

## 2. 可观测水平评估与框架提升设计

### 2.1 现状

| 面 | 现有 | 缺口 |
|---|---|---|
| 指标 | 7 个 counter（produced/consumed × msg/bytes、produce_errors、fetch/produce requests、compressed），Prometheus 文本，独立端口 | **无 label 维度**（topic/partition/user 全部聚合成单值）；无 gauge/histogram（HW lag、actor 队列深度、请求延迟分布）；管理面零指标（rebalance、组数、meta version、controller 命令延迟）；fetch 侧计数此前恒 0（已修）；METRICS_PORT=0 语义（已修） |
| 健康 | 无（本轮加 /health、/ready） | ready 判定粒度（单节点恒可服务 vs 空簇）需按部署形态可配 |
| 日志 | tracing + EnvFilter（BASALT_LOG_LEVEL 静态） | 41 处 eprintln（26 已迁）；无运行时动态级别；无慢日志 |
| 追踪 | 无 request 关联 id；跨 actor 路径（produce→actor→replicate）不可追踪 | span 化成本高——v1 用 request 级 corr id 字段贯穿日志即可 |

### 2.2 框架设计（分层，每层独立可交付）

- **L1 指标基建统一（1 会话）**：抽 `server/src/metrics.rs`——Counter/Gauge/Histogram 三原语 + register 宏；label 维度第一批 = topic+partition（维度基数可控：题数 × 分区数）；指标与导出解耦（Prometheus 文本留现有端口）。
  第一批指标：分区 HW/LEO/log_start（gauge，actor 处直读）、produce/fetch 延迟 histogram（actor 处理耗时）、连接数/认证失败/授权拒绝计数（conn 层）、组数/rebalance 计数（组协调器回调）、meta version gauge。
- **L2 管理面观测（0.5 会话）**：ControllerCmd/GroupCmd/MetaCmd 队列深度与等待时间（gauge+histogram）——**直接服务 §4 解耦验证**（过载可见才能谈隔离）。
- **L3 慢日志（0.5 会话）**：produce/fetch 处理超阈值（默认 500ms，env 可调）warn 输出 topic/partition/bytes/耗时；fetch 长轮询天然 >max_wait 需排除（以 actor 处理耗时为准，不含 pending 等待）。
- **L4 动态日志（0.5 会话）**：tracing EnvFilter::reload——metrics HTTP 挂 `POST /log?level=debug`（复用现有端口，无新面）。
- **L5（按需）**：corr id 贯穿（conn 层生成 req_seq 已存在 → 日志字段注入）。

## 3. 日志使用评估

**好面**：tracing 结构化字段纪律在核心事件上成立（failover/ISR shrink/retention/SASL 认证/账本系列修复点均带 topic/partition/error 字段）；EnvFilter 分级生效；e2e 以日志为断言证据的实践（run_auth/run_acl 服务端日志双证据）反推日志可依赖性尚可。

**问题与政策**：

| 问题 | 事实 | 政策 |
|---|---|---|
| eprintln 旁路 | 41 处（26 已迁 tracing；ctrl_raft 引擎 13 + jepsen 测试 harness 3 留存） | 运行时代码禁止 eprintln（评审清单项）；ctrl_raft 引擎诊断随引擎 POC 收口迁移；测试 harness 内允许 |
| 级别语义混用 | 每分区 actor started/role updated = info（多分区高频刷屏）；LEO-REPORT/HW-ADV 这类高频循环事件曾是裸打印 | 生命周期/拓扑变化 = debug；故障与自愈动作（failover/收缩/retention/授权拒绝）= info~warn；逐请求/逐循环 = trace |
| 字段纪律 | 大体统一（topic/partition/error），eprintln 迁移后需保持 | 固定键集：topic/partition/principal/bytes/error/node |

## 4. 管理面/数据面解耦评估

### 4.1 现状拓扑（实测事实）

```
客户端口（明文 9092 + TLS 9094 档）：produce/fetch/组/管理 API 混布
内部口（client+1）：Register/Heartbeat/MetaSync/CreateTopic/Delete/FetchSlice —— ⚠ 无鉴权
metrics 口：/health /ready /metrics（本轮补健康面）

actor 拓扑：每分区独立 actor（mpsc 1024）｜GroupManager（512）｜CG848（256）
           ｜MetaService（256）｜Controller（256）｜TxnCoordinator（256）｜offloader（64）
```

**隔离良好**：分区独立 actor（分区间数据面互不拖死）；fetch 长轮询挂 pending 不占 actor 循环；管理指令通道与数据通道分离（produce 不占 GroupCmd）；TLS/明文监听分离。

### 4.2 风险点（管理指令被拖死的真实路径，按严重度）

| # | 路径 | 场景 | 缓解（现装） | 建议 |
|---|---|---|---|---|
| 1 | **连接级队头**（账本 58 决策的已知代价） | PRODUCE 内联处理——RF≥2 停等最长 10s 期间，同连接的 metadata/admin 请求全部排队 | 管理 API 走独立连接（AdminClient 独立连接）时不受伤；跨连接不受影响 | v2：内联仅覆盖幂等面，停等型 acks=all 移出内联（响应序由 writer 重排保证，处理序由分区 actor 序保证的另一方案：seq 分配内联、IO 并发） |
| 2 | **ControllerCmd 单 actor 256** | Heartbeat（300ms×N 节点）+ CreateTopic/DeleteTopic/Sync 串行——心跳洪峰拖慢建题；**Sync 是账本 62 类自愈的关键路径** | 心跳无 reply 不占等待，但占队列 | 心跳合并（同节点只留最新）或拆独立通道；Sync 优先 |
| 3 | **GroupCmd 单 actor 512** | offset commit 风暴串行化 rebalance 指令 | 组面与数据面本就分离 | commit 批量化；rebalance 指令优先级 |
| 4 | **MetaService 单 actor 256** | 周期性客户端 metadata 刷新排队；账本 62 的启动刷新也走它 | Lookup 只读——可无锁化（ArcSwap<ClusterState>） | 只读快照原子化，写路径（create/delete）留 actor |
| 5 | tokio runtime 共享 | CPU 饱和时全面同 degradation | 多 worker 天然缓冲 | thread-per-core（T-M4.4）；短期观察 |

### 4.3 结论

**解耦未"完全"，但数据面过载拖死管理面的主路径不存在**：数据面（produce/fetch）与管理面（元数据/组/ACL）actor 分离，跨连接的管理指令不受数据面排队影响。真实风险集中在**连接内混用**（#1，已知情决策）与**管理面内部三个单 actor 的心跳/commit 挤占**（#2/#3，多节点规模化后才显现）。§2 L2 的队列指标是这一切的前置——先可见再隔离。

## 5. 快速验证

```bash
cargo test --workspace                # 148 测试
curl localhost:9094/health            # ok（liveness）
curl localhost:9094/ready             # ready / empty-routing（readiness）
curl localhost:9094/ | grep basalt    # Prometheus 指标（含消费面，本轮起真实）
```

## 6. 对已批准 share groups spike 的约束回填

1. **share 协调器独立 actor + 独立 channel**（§4 #2/#3 教训前置：不与 classic/848 组协调器共 channel）；
2. ack 状态机 sweep 挂分区 actor deadline 链（与 retention sweep 同款，不新增定时线程）；
3. 指标进 §2 L1 第一批（share 组数/在途 acquired 数/redelivery 计数），避免再出"恒 0 假指标"；
4. 日志走 tracing 政策（§3），不留 eprintln。
