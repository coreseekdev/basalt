------------------------------- MODULE BasaltDataPlaneLiveness -------------------------------
(* 数据面活性最小参数实验（P2-2 / 指南 P1-5 处方，additive 不动本体）        *)
(*                                                                           *)
(* 公平性：选择性 WF（逐子动作镜像翻译产物析取支）——Crash/Partition 刻意      *)
(* 不公平（环境恶意可无限发生）；SF(Restart ∨ Heal) = "每个崩溃/分区最终      *)
(* 恢复"的稳定性编码（C9 二分同款教训：无稳定性假设的收敛性不可证）。        *)
(*                                                                           *)
(* 性质（最小参数：MaxLogLen=1、Values={v1}、MaxEpochs=3）：                  *)
(*   EventuallyServable            <> 有可服务主                             *)
(*   EventuallyCommitted           <> 已产出被提交                           *)
(*   CommittedEventuallyConsumed   已提交终被消费                            *)
(* 预期（C9 同款边界）：P1 绿；P2/P3 在"epoch 耗尽 + 崩溃"循环下可能红——     *)
(* 红 = 可用性边界（控制器 horizon 耗尽 + 租约失效 = 永久不可服务），如实     *)
(* 归档为负结果/边界，不弱化规约。                                            *)
EXTENDS BasaltDataPlane, Integers, FiniteSets, Sequences

AllVars == << up, parted, curEpoch, curLeader, view, caughtUp, leaderHist, log, synced, lease, committed, consumed >>

(* ---------- Controller 子动作 ---------- *)
AssignLeaderStep ==
    /\ \E n \in Brokers:
         /\ curEpoch < MaxEpochs
         /\ curEpoch' = curEpoch + 1
         /\ curLeader' = n
         /\ lease' = [lease EXCEPT ![n] = TRUE]
         /\ leaderHist' = (leaderHist \cup {<<curEpoch', n>>})
         /\ caughtUp' = [caughtUp EXCEPT ![n] = EagerLeader]
         /\ view' = [n2 \in Brokers |->
                       IF parted[n2] THEN view[n2]
                       ELSE [epoch |-> curEpoch', leader |-> n]]
    /\ UNCHANGED <<up, parted, log, synced, committed, consumed>>

CrashStep ==
    /\ \E n \in Brokers:
         /\ up[n]
         /\ up' = [up EXCEPT ![n] = FALSE]
         /\ log' = [log EXCEPT ![n] = IF synced[n] >= Len(log[n]) THEN log[n] ELSE IF synced[n] = 0 THEN <<>> ELSE SubSeq(log[n], 1, synced[n])]
         /\ caughtUp' = [caughtUp EXCEPT ![n] = FALSE]
         /\ lease' = [lease EXCEPT ![n] = FALSE]
         /\ IF curLeader = n
              THEN curLeader' = NoLeader
              ELSE UNCHANGED curLeader
    /\ UNCHANGED <<parted, curEpoch, view, leaderHist, synced, committed, consumed>>

PersistStep(self) ==
    /\ self \in Brokers
    /\ up[self]
    /\ synced[self] < Len(log[self])
    /\ synced' = [synced EXCEPT ![self] = Len(log[self])]
    /\ UNCHANGED <<up, parted, curEpoch, curLeader, view, caughtUp, leaderHist, log, lease, committed, consumed>>

RestartStep(self) ==
    /\ self \in Brokers
    /\ ~up[self]
    /\ up' = [up EXCEPT ![self] = TRUE]
    /\ IF ViewRollback /\ view[self].epoch > 0
         THEN view' = [view EXCEPT ![self] = [epoch |-> view[self].epoch - 1, leader |-> view[self].leader]]
         ELSE view' = view
    /\ UNCHANGED <<parted, curEpoch, curLeader, caughtUp, leaderHist, log, synced, lease, committed, consumed>>

PartitionStep ==
    /\ \E n \in Brokers:
         /\ ~parted[n]
         /\ parted' = [parted EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<up, curEpoch, curLeader, view, caughtUp, leaderHist, log, synced, lease, committed, consumed>>

HealStep(self) ==
    /\ self \in Brokers
    /\ parted[self]
    /\ parted' = [parted EXCEPT ![self] = FALSE]
    /\ view' = [view EXCEPT ![self] = [epoch |-> curEpoch, leader |-> curLeader]]
    /\ UNCHANGED <<up, curEpoch, curLeader, caughtUp, leaderHist, log, synced, lease, committed, consumed>>

(* ---------- Broker 子动作（参数化） ---------- *)
CatchUpStep(self) ==
    /\ \E q \in Majors:
         \E j \in q:
           /\ ~EagerLeader
           /\ ThinksLeader(self)
           /\ ~caughtUp[self]
           /\ ~parted[self]
           /\ up[j]
           /\ ~parted[j]
           /\ SuccessorOf(q, j)
           /\ log' = [log EXCEPT ![self] = log[j]]
           /\ caughtUp' = [caughtUp EXCEPT ![self] = TRUE]
    /\ UNCHANGED <<view, committed, consumed, up, parted, curEpoch, curLeader, leaderHist, synced, lease>>

ProduceStep(self) ==
    /\ \E v \in Values:
         /\ Serving(self)
         /\ Len(log[self]) < MaxLogLen
         /\ log' = [log EXCEPT ![self] = Append(log[self], <<view[self].epoch, v>>)]
    /\ UNCHANGED <<view, caughtUp, committed, consumed, up, parted, curEpoch, curLeader, leaderHist, synced, lease>>

PushStep(self) ==
    /\ \E f \in Brokers:
         /\ Serving(self)
         /\ f # self
         /\ up[f]
         /\ ~parted[self]
         /\ ~parted[f]
         /\ log[f] # log[self]
         /\ (\/ view[f].epoch < view[self].epoch
             \/ /\ view[f].epoch = view[self].epoch
                /\ view[f].leader = self)
         /\ log' = [log EXCEPT ![f] = log[self]]
         /\ view' = [view EXCEPT ![f] = [epoch |-> view[self].epoch, leader |-> self]]
    /\ UNCHANGED <<caughtUp, committed, consumed, up, parted, curEpoch, curLeader, leaderHist, synced, lease>>

CommitAdvanceStep(self) ==
    /\ \E q \in Majors:
         \E L \in Len(committed)+1..Len(log[self]):
           /\ Serving(self)
           /\ IsValsPrefix(committed, log[self])
           /\ \A r \in q :
                /\ L <= synced[r]
                /\ SameVals(log[r], log[self], L)
                /\ (\/ ~CommitChecksEpoch
                    \/ /\ view[r].epoch = view[self].epoch
                       /\ view[r].leader = self)
           /\ committed' = [i \in 1..L |-> log[self][i][2]]
    /\ UNCHANGED <<view, caughtUp, log, consumed, up, parted, curEpoch, curLeader, leaderHist, synced, lease>>

ConsumeStep(self) ==
    /\ \E r \in Brokers:
         /\ curLeader = r
         /\ view[r].leader = r
         /\ view[r].epoch = curEpoch
         /\ caughtUp[r]
         /\ Len(consumed) < Len(log[r])
         /\ Len(consumed) < Len(committed)
         /\ consumed' = Append(consumed, log[r][Len(consumed)+1][2])
    /\ UNCHANGED <<view, caughtUp, log, committed, up, parted, curEpoch, curLeader, leaderHist, synced, lease>>

(* ---------- 公平性与性质 ---------- *)
FairDP ==
    /\ Spec
    /\ WF_vars(AssignLeaderStep)
    /\ \A n \in Brokers :
         /\ WF_vars(CatchUpStep(n))
         /\ WF_vars(ProduceStep(n))
         /\ WF_vars(PushStep(n))
         /\ WF_vars(CommitAdvanceStep(n))
         /\ WF_vars(ConsumeStep(n))
         /\ WF_vars(PersistStep(n))
         /\ SF_vars(RestartStep(n) \/ HealStep(n))

(* 稳定环境变体：无崩溃/分区（churn 排除）——"崩溃停止发生"的精确编码。
   该变体下活性应全绿：终有主、写入终被提交、提交终被消费。 *)
NoChurnNext == Next /\ ~CrashStep /\ ~PartitionStep
StableSpec == Init /\ [][NoChurnNext]_vars
FairStable ==
    /\ StableSpec
    /\ WF_vars(AssignLeaderStep)
    /\ \A n \in Brokers :
         /\ WF_vars(CatchUpStep(n))
         /\ WF_vars(ProduceStep(n))
         /\ WF_vars(PushStep(n))
         /\ WF_vars(CommitAdvanceStep(n))
         /\ WF_vars(ConsumeStep(n))
         /\ WF_vars(PersistStep(n))

EventuallyServable == <>(\E n \in Brokers : Serving(n))
EventuallyCommitted == <>(Len(committed) = 1)
CommittedEventuallyConsumed == [](Len(committed) = 1 => <>(Len(consumed) = 1))

=============================================================================
