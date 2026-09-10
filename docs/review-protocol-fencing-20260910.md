# 协议修复 / fencing / pool 闭环评审（2026-09-10，五轮 code review）

> 评审范围：四轮（review-adr14-pool-20260910.md）之后合入的修复——缺陷⑩⑪
> （OffsetFetch v8+/管理面×4）、FindCoordinator key 回显、TruncateTo Leader
> fencing、pool 闭环补充、coordinator ListGroups/DescribeGroup 命令。
>
> **过程说明**：本轮评审 agent 在探针运行阶段异常终止（探针自身的驱动缺陷
> 导致挂死，见下），主线接管其探针、修复驱动后完成全部实证与处置。
> **结论：1 项服务端语义偏离（已修）+ 探针缺陷 2 处（已修）+ 全部布局回归
> 转正为永久机制（6/6 绿）。**

## P1-1 OffsetFetch null-topics 语义偏离 Kafka（探针实证，已修）
`server/src/handlers_groups.rs` offset_fetch：Topics=null（取该组全部）时
回**路由 topic 全集**（含零提交的 topic，Partitions 空数组）。Kafka 语义：
只回**有已提交 offset** 的 topic；未知组 → 空 Topics。
- **探针红**：v2/v8 null 请求 → `["t1","t2"]`（期望 `["t1"]`）；未知组 ghost
  → Topics 非空（期望空）。
- **修法**：null 分支过滤 `committed` 中不存在的 topic。显式 topic 请求不变
  （逐分区 -1 回填保持）。消费端三档 e2e（kafka-python/librdkafka/franz-go）
  复跑全绿。

## 探针自身缺陷 2 处（主线接管时修复）
1. **驱动死锁**：`futures_now::block_on` 用 noop-waker 忙等——单线程
   `#[tokio::test]` runtime 被阻塞，MetaService/GroupManager 后台任务永远
   得不到轮询 → 首个探针即挂死（评审 agent 异常终止的直接原因）。修法：
   探针内一律 `.await` 让出。
2. **漏发 ApplyCluster**：make_ctx 构建了 ClusterState 却未发送
   `MetaCmd::ApplyCluster` → MetaService 集群态为空 → delete_topics 的
   Lookup 恒空（误报 ErrorCode 3）。修法：补发 + await 让出。
3. （期望自洽性）同探针内"null Topics 展开全部"与"未知组回空 Topics"两条
   断言互相矛盾——按 Kafka 语义统一为后者口径。

## 已确认无问题（探针绿）
- **FindCoordinator**：v0-3 Key / v4+ CoordinatorKeys 双布局，key 逐条回显
  （缺陷修复的字节级验证，含多 key 批量）。
- **OffsetFetch**：v0-9 逐版本布局（Throttle/顶层 ErrorCode/LeaderEpoch/
  CommittedLeaderEpoch 出现版本）、分区过滤、未提交分区 -1 回填、Metadata
  null、v8+ Groups[] 回显与 MemberId/MemberEpoch 域。
- **DeleteTopics**：v0/v5（TopicNames→Responses，ErrorMessage 5+）、v6
  （Topics struct + TopicId 回显）、纯 TopicId → UNKNOWN_TOPIC_ID。
- **DescribeGroups / ListGroups**：v0-5 布局、成员明细（MemberMetadata/
  MemberAssignment 字节域）、StatesFilter、不存在组 → Dead。
- **InitProducerId**：v0-5 布局（OngoingTxn* 6+ 不出现）。

## 评审范围覆盖说明（诚实边界）
agent 异常终止前仅完成协议 handler 探针；**fencing 与 pool 闭环两区由主线
以既有确定性测试复核**：TruncateTo Leader fencing（truncate_fencing_tests
×2 + multinode failover e2e 60/60 零丢失）、pool 闭环（响应缓冲入池 +
read_ex 窗口归还，基准复查持平）——覆盖方式为确定性测试与基准，非本轮
新探针，特此注明。

## 永久机制
协议布局探针转正：`server/src/handlers_layout_tests.rs`（6 测试，字节级
独立解码验证）——缺陷⑩⑪类（版本分叉恒按单一布局读写）的永久回归锁定。
