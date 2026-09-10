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
| P2-1 | RebalanceCompletes 现状红；逐动作公平性救不了（MUT-D 删"完成加入"动作仍红：Join↔Leave 纯 churn 循环） | 复跑 + MUT-D | ✅ 已按负结果入账：四级公平性二分（CONSUMERGROUP-LIVENESS-WIP.md），liveness cfg 转已知红阴性对照 |
| P2-2 | 数据面活性零覆盖 | 清单 | ⬜ 最小参数方案（日志长 1、单值）留 C14⑤ 同批 |
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

## 残留边界（如实入账）

1. C14① synced 崩溃边界未闭合（M2 前置门禁）。
2. memberGen 级僵尸追踪（逐成员记录所属代）留 v0.4——当前成员校验 + 代令牌
   已覆盖"已退成员/旧代成员"两类僵尸，更细粒度（组内代错位）不可表达。
3. Stable 态成员失联踢除与 Leave 动作在模型中同效（效果等价的转移不改变
   行为图）——以注释标注，不增设冗余动作。
