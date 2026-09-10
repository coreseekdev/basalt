# 消费组收敛性活性实验 —— 已闭合（结论：eager rebalance 在持续 churn 下无收敛保证）

## 实证结论（2026-09-10，TLC 反例确认）
`WF_vars(Coordinator)` 公平性下，TLC 给出真实反例循环：
`NewJoin m1（ready 重置）→ Rejoin m2 → …` 无限重复——
**eager rebalance 在成员持续 churn（离开-重入环）下不保证收敛**。
这是协议固有性质（Kafka 同款已知问题，KIP-429 cooperative rebalance
即为其缓解方案），不是实现缺陷。

> **v0.2 已闭合部分（2026-09-10）**：CommitOffset fencing 动作（仅当代 owner、
> offset 单调）与超时踢除路径已入模型，名义/阴性对照全绿（账本 C9 更新）。
> 本文档仅剩「收敛性活性」一项未闭合。

## 目标（不变式评审建议，账本 C9 收敛性 L2）
`(state = PreparingRebalance ∧ gen < MaxRounds) ⇒ ◇(state ∈ {Stable, Empty} ∨ gen = MaxRounds)`

## 已尝试（2026-09-10）
1. PlusCal `fair process`：翻译产物**不含 WF/SF 合取**（grep 零命中）——
   公平性未生效，反例为纯 Stuttering（Preparing + ready={} +
   members={m1,m2} 永久停留，类型上 Rejoin 明明持续可用）。
2. 显式 `FairSpec == Spec /\ WF_vars(Next)` + `SPECIFICATION FairSpec`：
   同一 stuttering 反例仍被 TLC 判为公平——疑点：`with` 语句对
   ENABLED ⟨Next⟩_vars 计算的折叠、Next 合取粒度、或 WF_vars 对
   合取动作的语义。

## 下一步
- 单动作 WF 实验：`WF_vars(Rejoin-action)` 而非 `WF_vars(Next)` 合取；
- 查 pcal 翻译的动作粒度（单标签 while + either 的动作展开方式）；
- 或放弃模型层收敛性，转为仿真层结论（turmoil/timed 长跑），在
  VERIFICATION-GUIDE「未建模边界」中如实标注——与账本 C9 行的
  "收敛性归仿真层/T-Q.4" 条款一致。

## 当前规约状态
`ConsumerGroup.tla` 含活性编辑（fair process + RebalanceCompletes +
FairSpec）——三个名义 cfg 全绿不受影响（FairSpec 未被其引用）；
`consumer-group-liveness.cfg` 指向 FairSpec，当前红（见上）。
