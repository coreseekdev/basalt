# 消费组收敛性活性实验 —— 已闭合（四级公平性二分，2026-09-10）

## 结论

`RebalanceCompletes` 在**任何 coordinator 侧公平性强度下都不可满足**——
不是公平性写得不够，而是模型允许成员无限「报到↔取消报到」churn，收口
动作永远无法**持续可用**（continuously enabled），WF 在语义上就无法强制
它。这是 eager rebalance 的固有性质（Kafka 同款，KIP-429 cooperative
rebalance 的动机），需环境假设（churn 有界/最终停止）才能闭合收敛性。

## 二分实验（spec/ConsumerGroupLiveness.tla + consumer-group-l{0,2,3}.cfg）

| 级别 | 公平性假设 | 结果 | 反例结构 |
|---|---|---|---|
| L0 | 无（Spec） | 红 | 纯 Stuttering（State 3: Stuttering） |
| L1 | `WF_vars(Coordinator)`（粗粒度合取动作，consumer-group-liveness.cfg） | 红 | **真实循环**（3→4→3），非 stuttering |
| L2 | WF(收口) + WF(Sync)（单动作粒度） | 红 | 收口仅在 `ready=members` 时可用，被 ready 缩放反复打断 |
| L3 | + WF(报到) + WF(完成加入)（rebalance 全部子动作） | 红 | **AddReady ↔ FinishJoin 乒乓**：`ready={} → {m2} → {}` 无限往复 |

L3 反例轨迹（TLC 实录，684 状态全空间）：

```
State 2: state=PreparingRebalance, members={m2}, ready={}   \* 收口 disabled
State 3: state=PreparingRebalance, members={m2}, ready={m2} \* 收口 enabled
Back to state 2                                             \* FinishJoin(m2) 再缩空
```

`WF_vars(EnterCompletingStep)` 要求该动作**持续**可用时最终发生；乒乓使它
间歇可用，公平性公式不成立也不违约——这是 TLA+ 公平性的正确语义，不是
工具问题。**更正**：本文件早先版本称 L1 反例为 "stuttering"，实为真实循环
（每步 vars 均变化，粗粒度 WF 被任意进展步满足）——「下一步」的单动作
WF 实验落地后厘清；`with` 折叠/翻译粒度疑点均排除。

## 处置

1. **模型层闭合（留待 v0.3）**：给模型加 churn 预算旋钮（如 `JoinBudget`
   常量：报到/完成总次数上界），「churn 耗尽后必收敛」即可在 TLC 全空间
   判定——届时 `RebalanceCompletes` 改写为预算耗尽后激活的条件性质。
2. **现实收敛性**：归 T-Q.4 仿真长跑（账本 C9 行既定条款）。实现层
   coordinator 已有 session timeout 踢除（v0.2 入模）；churn 上界由客户端
   max.poll/retry 策略决定，属环境参数。
3. `consumer-group-liveness.cfg`（指向粗粒度 FairSpec）保留为**已知红
   阴性对照**：任何使其变绿的规约改动都必须解释 churn 语义发生了什么
   变化。

## 实验产物

- `spec/ConsumerGroupLiveness.tla`——additive 扩展模块（四个子动作公式
  逐一镜像翻译产物析取支，含全变量 UNCHANGED；不动 ConsumerGroup.tla 本体）
- `spec/consumer-group-l0.cfg` / `-l2.cfg` / `-l3.cfg`——三级公平性配置
- 运行：`java -cp spec/tools/tla2tools.jar tlc2.TLC -config spec/consumer-group-l3.cfg spec/ConsumerGroupLiveness.tla`（秒级）
