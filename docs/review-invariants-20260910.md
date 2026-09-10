# 规约不变式评审（2026-09-10，不变式 review agent，突变判别实证）

> 评审范围：spec/BasaltDataPlane.tla、spec/ConsumerGroup.tla、7 个 cfg、账本与指南。
> 方法：TLC 判别实验 + 突变测试（MUT-A/B/C/D/E，/tmp 隔离副本，仓库零改动）。
> 一句话：C1/C2/C3 承载不变式经判别实验证实有真实约束力；但 v0.2 新不变式从未
> 接入 cfg、fencing 不变式零判别力、InvConsumedBounded 结构性恒真——全部已处置。

## 发现与处置

| # | 发现（agent 原判） | 突变/实验证据 | 处置（主线，2026-09-10） |
|---|---|---|---|
| P0-1 | v0.2 新不变式（InvCommitHasOwner/InvCommitTypeOK）未被任何 cfg 检查，"已闭合"无机器证据 | cfg 时间戳早于 .tla v0.2；代跑名义+全 7 不变式绿 | ✅ 三个 consumer-group cfg 全部接线；v0.3 后以 InvCommitFencedMon 替代 |
| P0-2 | fencing 不变式零内容：MUT-C（去 `m ∈ members` 守卫放行僵尸提交）全绿；commit 无 generation 维度；commitOwner 死变量 | MUT-C 5,308 状态全绿（检查器毫无反应） | ✅ v0.3：commit 带 (member, generation) 双令牌（`assignGen` 为分配代令牌）+ 审计位监控 `InvCommitFencedMon`；**MUT-C 复测红、新增 MUT-E（去代令牌）红**——判别力经突变实证；`commitGen` 入账审计 |
| P1-1 | InvConsumedBounded 结构性恒真（Consume 直读 committed 变量）；MUT-B（消费走副本日志）2,125 状态即红——多数派安全与消费幻读解耦，正是该不变式应有的内容 | MUT-B 红（旧主未提交条目被消费） | ✅ 采纳处置 (a)：Consume 改为向**现任主日志** fetch（值取自主日志、长度以已提交前缀为界）——不变式现依赖 C1b+日志匹配；全空间 1200 万状态绿 |
| P1-2 | InvConsumedOnLeader 为前两条不变式的推理闭包，永不会独立报红 | 逻辑论证 | ✅ 降级标注（"规约语义恒真·定理"，保留文档价值） |
| P1-3 | 结构性恒真共 6 项（InvOneLeaderPerEpoch/InvViewEpochSane/InvGenAssignmentUnique/InvReadySubset/InvGenHistoryBounded/InvCommitTypeOK 死支）；但 MUT-A 证明它们是**机制锁**非定义空洞 | MUT-A（epoch 回绕）34 状态红 | ✅ .tla 注释 + 账本 C2 行 + 指南 P2-7 三处标注落地；MUT-A/B/C/E 登记为期望红突变对照 |
| P1-4 | Crash 全量持久与 §3 故障模型（"已 sync 存活、未 sync 消失"）矛盾——"ack 前未 fsync"类缺陷在规约层不可表达 | 读码 | ⬜ C14① 维持 open，**M2 前必须闭合**（synced[n] ≤ Len(log[n]) 边界） |
| P2-1 | RebalanceCompletes 现状红；逐动作公平性救不了（MUT-D 删"完成加入"动作仍红：Join↔Leave 纯 churn 循环） | 复跑 + MUT-D | ✅ 已按负结果入账：四级公平性二分（CONSUMERGROUP-LIVENESS.md），liveness cfg 转已知红阴性对照 |
| P2-2 | 数据面活性零覆盖 | 清单 | ✅ 已闭合（2026-09-10，最小参数实验 `BasaltDataPlaneLiveness.tla` + dp-liveness*.cfg）：**稳定环境**（无崩溃/分区）三性质全绿——终有可服务主、写入终被提交、提交终被消费（机器已证）；**无限 churn 下负结果**——WF(AssignLeader) 尽快烧完 epoch horizon，horizon 耗尽后一次主崩溃（租约失效，缺陷⑯修复的设计内代价）⇒ 永久不可服务。真实系统 epoch 为 64 位计数器实际不可耗尽；可用性/时限归 T-Q.4 仿真 |
| P2-3 | 文档漂移 5 项（demo-eager 实际先爆 InvCurrentLeaderHasCommitted；纪律 2 与活性实验矛盾；Makefile 无 liveness 目标；C9 行旧口径；C8 行 L2 半边为空） | 实测 | ✅ 全部落地（指南/Makefile/账本） |
| P2-4 | MaxRounds 逃生分支：gen 达上界后组永久无法回 Stable | 读码 | ✅ .tla 注释明示（已知取舍） |

## 突变对照登记（期望红，防检查器判别力退化）

| MUT | 突变 | 期望 | 实测 |
|---|---|---|---|
| MUT-A | AssignLeader epoch 回绕（破坏单写者机制） | InvOneLeaderPerEpoch 红 | 红（34 状态） |
| MUT-B | Consume 改任意副本读（幻读形态） | InvConsumedBounded 红 | 红（2,125 状态）；v0.2 后主日志 fetch 同样可红 |
| MUT-C | 去成员 fencing（僵尸提交放行） | InvCommitFencedMon 红 | v0.2 全绿（缺陷实证）→ **v0.3 红** |
| MUT-D | 删"JoinGroup 完成"动作 | liveness 仍红（churn 循环） | 红（Join↔Leave 循环） |
| MUT-E | 去代令牌 fencing（旧代提交放行） | InvCommitFencedMon 红 | **红**（v0.3 新增判别力） |

## 后续处置（同日，C14①⑤ 落地时的规约层新发现）

### C14① synced 崩溃边界——已落地，并暴露规约缺陷⑯
按 P1-4 处方实现：`synced[n]` 持久水位 + Persist 动作（fsync 边界）+ Crash
截断到 synced + CommitAdvance 多数派校验改为持久前缀（`L <= synced[r]`）+
新不变式 `InvCommittedDurable`（commit ⇒ 任一多数派含 fsync 完整副本——
机器检查"commit ⇒ 多数派已持久"）。

**新发现（缺陷⑯，规约层）**：synced 边界使 SplitBrain 场景 InvLogMatching
变红——反例：旧主 push `<<1,v1>>` 给 n2（未持久）→ 旧主 crash 丢失该尾部 →
重启后凭持久 view 在**同一 epoch** 自恢复并重写 index 1（`<<1,v2>>`）→
n2/n3 同 epoch 同 index 不同值。根因：模型允许 broker 自我指派（违背
ADR-10"broker 不得自我指派"）；follower 侧 fencing 无法吸收同 epoch 重写。
**修复**：引入控制器租约 `lease[n]`——AssignLeader 授予、Crash 即失效、
SplitBrain（控制器不可达）下不可重授；SplitBrain 语义修正为"分区不撤销
租约的旧主继续服务"（其本意），自恢复路径被协议规则正确封死。
终局复核（2026-09-10）：check 全空间 **1.27 亿状态绿**（45 分钟 8 worker）、
splitbrain / splitbrain-cepoch 双绿、demo-eager 红（判别力保持）、
view 回退实验绿。
**教训**：环境模型过强（crash 全量持久）会掩盖真实协议缺口——这正是
评审 P1-4 称其为"唯一危险侧缺口"的原因；M2 实现必须实现"租约随进程
死亡失效"（failover 后旧主不得凭持久 view 复写）。

### C14⑤ view 回退方向实验——已闭合（安全性不敏感，正结果）
`ViewRollback=TRUE`（Restart 载入过期 view 检查点，epoch-1）：约 600 万
状态全空间绿。结论：view 回退不破坏安全性——Push 的 epoch 比较 fencing、
crash 后强制重新继任接管、继任规则三者兜底。与缺陷⑯对照：**危险方向不是
view 陈旧，而是"崩溃后以原 epoch 自恢复服务"**——租约语义封死的正是后者。

## 残留边界（如实入账）

1. C14① synced 崩溃边界未闭合（M2 前置门禁）。
2. memberGen 级僵尸追踪（逐成员记录所属代）留 v0.4——当前成员校验 + 代令牌
   已覆盖"已退成员/旧代成员"两类僵尸，更细粒度（组内代错位）不可表达。
3. Stable 态成员失联踢除与 Leave 动作在模型中同效（效果等价的转移不改变
   行为图）——以注释标注，不增设冗余动作。
4. code review 四轮 P2-4（truncate 后 Leader 继续接受 produce 复用截断区
   offset）已于同日修复：partition actor 的 TruncateTo 在 Leader 角色且
   offset < LEO 时拒绝（Err）——截断是 follower 侧分叉自愈动作，自愈前
   必须先经 SetRole Follower；对照测试锁定 Follower 角色截断仍可用。
