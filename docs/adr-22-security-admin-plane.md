# ADR-22：安全面与管理面对齐（T-M4.1/T-M4.2 第一期——鉴权/加密/配额/管理 API）

日期：2026-09-18　状态：已落地（一期）　关联：TASK T-M4.1/T-M4.2、HANDOFF-2026-09-18、
参考实现对照行动清单 S-1/S-2/S-5

## 1. 背景

生产就绪评估（2026-09-18）列出的四大缺口中的三项：

1. **无任何鉴权/加密**——任意 TCP 连接可读写全部 topic；
2. **管理面对齐**——kafka-clients AdminClient 的 DescribeCluster/DescribeConfigs
   无 handler（宣告都没有），客户端直接吃 UNSUPPORTED_VERSION；
3. **无配额/限流**——单连接可打满共享带宽，无公平性约束。

「有限兼容」边界（评估结论原话）：兼容端口提供 Kafka 客户端可用的最小
安全/管理语义，增强能力（ACL、per-topic 配置覆盖、多 listener 名称机制）
按需后置。

## 2. 决策

### 2.1 SASL/SCRAM-SHA-256（鉴权）

- **机制**：仅 `SCRAM-SHA-256`（RFC 5802 服务端流：client-first →
  server-first → client-final → server-final `v=<sig>`）。凭据
  `BASALT_SASL_USERS=user:pass,...` 启动期一次性派生（salt=HMAC(sha256(user),
  server)、4096 轮 PBKDF2），明文口令即弃；进程内只存 stored_key/server_key。
- **门禁**：`BASALT_AUTH=scram` 后，未认证连接仅放行 ApiVersions(18) /
  SaslHandshake(17) / SaslAuthenticate(36)，其余请求读侧直接断连（Kafka 同
  语义：SASL 端口上的明文业务请求不回错误、直接关闭）。
- **断连语义**：认证失败（proof 不匹配/未知用户）回
  SASL_AUTHENTICATION_FAILED=58 后断连；错误机制回 33 + 机制表后断连；
  无握手直接 Authenticate 回 ILLEGAL_SASL_STATE=34 后断连。断连经
  watch 通道通知读侧——错误响应先于断连写出（写任务冲刷队列后关闭）。
- **已知边界**：kafka-python 2.2.3 的 SCRAM 走 pre-KIP-152 裸 socket 交换
  （SaslHandshake v0 + 裸 token，不经 SaslAuthenticate）——basalt 与 Kafka
  KIP-152 后的强制 SaslAuthenticate 路径一致，裸路径留 TASK P2。
  librdkafka / franz-go / kafka-clients 均为标准路径。

### 2.2 TLS（加密）

- **独立端口**（Kafka 多 listener 语义）：`BASALT_TLS_CERT/BASALT_TLS_KEY`
  （PEM）配置后于 `BASALT_TLS_PORT`（默认 client+2）另起 rustls listener；
  主口保持明文兼容。证书由部署侧提供。
- **通告跟随 listener**（Kafka 语义）：metadata/DescribeCluster 通告地址 =
  客户端所连 listener 的端口（`broker_array` 对本节点条目强制用连接级
  host/port）——否则 SSL 客户端 bootstrap 后被重定向明文口（账本 57）。
- 实现面：`serve_connection` 泛型化（`AsyncRead+AsyncWrite+Unpin+Send`），
  TLS 握手在连接服务之外完成，握手失败仅断该连接。

### 2.3 配额（限流）

- **逐连接字节率 token bucket**：`BASALT_QUOTA_PRODUCER_BYTES` /
  `BASALT_QUOTA_FETCH_BYTES`（bytes/sec；0=不限）。produce 按请求批字节、
  fetch 按响应字节数节流；桶容量 2×rate（突发），初始 1×rate（首请求不受罚）。
- **两处结构性要点**（账本 55）：
  1. **持锁休眠**：ConnState 用 tokio Mutex，节流在锁内 sleep——后续请求
     在锁上排队，请求按配额速率**串行化**；否则 pipeline 并发请求各自
     sleep，吞吐 = 配额 × 并发度。
  2. **负债制**：token 先扣减、允许为负，负值即欠账时长；休眠期回补不再
     产生可消费额度（否则睡醒请求把回补量瞬时吃掉，节流率翻倍）。
- **per-user 覆盖**（T-M4.2 尾项，2026-09-18）：
  `BASALT_QUOTA_USER_BYTES="app:p=500000;f=1000000,admin:f=2000000"`（条目
  `,` 分隔、维度 `;` 分隔；0=不限）——SASL 认证完成后按键覆盖全局默认，
  无条目回落。e2e：run_quota.sh 阶段二（覆盖生效 vs 回落双断言）。
- 语义注记：节流以延迟响应实现（v0/v1 语义；Kafka v2+ 先响应后节流下一
  请求，效果等同、形态不同，客户端无感）。(user, client-id) 二维实体面
  属 ACL 一期。

### 2.4 管理面（DescribeCluster/DescribeConfigs + ClusterId）

- **DescribeCluster(60) v0-2**：broker 全集（含 v2 IsFenced=false）+
  controller + cluster id；EndpointType 恒 1（broker 端点）；
  AuthorizedOperations 恒 Int.MIN（无 ACL 面，客户端按「未上报」处理）。
- **DescribeConfigs(32) v1-4**（v0 已被 Kafka 4.0 删除，基线 v1）：topic
  （存在性校验经元数据，未知回 3）与 broker（=4，答自身节点）两资源型，
  返回启动期配置的**静态投影**（cleanup.policy/retention.ms/retention.bytes/
  segment.bytes/min.insync.replicas + broker 侧 num.partitions 等），
  `ConfigSource=5（DEFAULT_CONFIG）`、`ReadOnly=true`（无 Alter 面，如实
  标注）。`ConfigurationKeys` 过滤生效。per-topic 覆盖面等 AlterConfigs/per-
  topic storage-mode 一期后置。
- **ClusterId**：`BASALT_CLUSTER_ID` 优先，否则 `data_dir/cluster_id` 持久化
  （重启一致——客户端按 ClusterId 缓存校验，逐次漂移会导致重连风暴）。
  Metadata 与 DescribeCluster 一致回填（此前 Metadata 恒 Null）。

## 3. 后果

- 客户端矩阵：librdkafka（SCRAM+SSL 两面 e2e）、kafka-clients/franz-go
  （管理面 Admin API 可用）、kafka-python（明文路径不受影响）。
- 性能基线建立：`testing/bench/run_bench.sh`（release，franz-go 直连面）。
  2026-09-18 基线（账本 58 修复后）：produce **1703MB/s**、consume
  1058MB/s（页缓存读穿透）。**基准当轮暴露并修复**：produce 处理序乱序
  → OOOSN 自激级联（初测 13.3MB/s 假象 + 200k 条 118 条重复，双重表现
  同根；修复 = PRODUCE 读循环内联，见账本 58）。
- TLA+ 面：本轮无新增规约（配额/鉴权为接入面语义，不涉核心复制/事务
  不变式；ACL 一期若引入授权决策面再评估）。

## 4. 未做（后置清单）

- ACL 骨架（授权决策面 + DescribeCluster 的 AuthorizedOperations 真值）
- per-user 配额、fetch session（T-M4.2 后半）
- SCRAM-SHA-512、SASL/OAUTHBEARER
- kafka-python 裸 SCRAM 兼容路径（pre-KIP-152，TASK P2）
- per-topic 配置覆盖（DescribeConfigs 动态化 + AlterConfigs 实现）
