------------------------------- MODULE ConsumerGroupLiveness -------------------------------
(***************************************************************************)
(* C9 收敛性活性二分实验（additive——不改动 ConsumerGroup.tla 本体）        *)
(*                                                                         *)
(* 问题：RebalanceCompletes 需要什么强度的公平性？逐级加码：               *)
(*   L0  Spec（无公平性）            —— 期望红（纯 Stuttering）            *)
(*   L1  FairSpec = Spec /\ WF_vars(Coordinator)（粗粒度，合取动作）        *)
(*                                   —— 期望红（真实循环，非 stuttering）  *)
(*   L2  + 仅推进动作 WF（收口/Sync）—— 期望红（ready 缩放循环不受约束）    *)
(*   L3  + rebalance 全部四个子动作 WF —— 检验 churn（报到/完成乒乓）是否   *)
(*         仍可无限循环                                                     *)
(*                                                                         *)
(* 子动作公式逐一镜像 ConsumerGroup 翻译产物的对应析取支（含全变量          *)
(* UNCHANGED），使 WF_vars(子动作) 的 ENABLED 计算落在单动作粒度上——       *)
(* 这正是 WIP 文档「单动作 WF 实验」的落地。                                *)
(***************************************************************************)
EXTENDS ConsumerGroup, Integers, FiniteSets

AllVars == << state, gen, members, ready, assignment, commits, commitOwner, hist >>

\* 报到：成员加入 PreparingRebalance（ready 增）
AddReadyStep ==
    /\ state = "PreparingRebalance"
    /\ \E m \in members :
        ready' = (ready \cup {m})
    /\ UNCHANGED <<state, gen, members, assignment, commits, commitOwner, hist>>

\* 完成加入：成员从 ready 移除（ready 缩）
FinishJoinStep ==
    /\ state = "PreparingRebalance"
    /\ \E m \in ready :
        ready' = (ready \ {m})
    /\ UNCHANGED <<state, gen, members, assignment, commits, commitOwner, hist>>

\* 全员就绪收口 → CompletingSync
EnterCompletingStep ==
    /\ state = "PreparingRebalance"
    /\ members # {}
    /\ (~SyncRequiresFull \/ ready = members)
    /\ state' = "CompletingSync"
    /\ UNCHANGED <<gen, members, ready, assignment, commits, commitOwner, hist>>

\* SyncGroup：generation+1 进 Stable
SyncStep ==
    /\ \E a \in [Parts -> members] :
        /\ state = "CompletingSync"
        /\ gen < MaxRounds
        /\ assignment' = a
        /\ hist' = (hist \cup {<<gen + 1, a>>})
        /\ gen' = gen + 1
        /\ state' = "Stable"
    /\ UNCHANGED <<members, ready, commits, commitOwner>>

\* L2：仅推进动作公平（收口与 Sync 最终发生——若持续可用）
FairAdvance == Spec /\ WF_vars(EnterCompletingStep) /\ WF_vars(SyncStep)

\* L3：rebalance 全部四个子动作公平
FairRebalance ==
    /\ FairAdvance
    /\ WF_vars(AddReadyStep)
    /\ WF_vars(FinishJoinStep)

=============================================================================
