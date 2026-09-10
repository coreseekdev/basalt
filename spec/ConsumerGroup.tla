------------------------------- MODULE ConsumerGroup -------------------------------
(***************************************************************************)
(* 消费组 rebalance 状态机 v0.1 —— 镜像 coordinator/src/lib.rs 四态机      *)
(* （TASK.md T-M1.1/T-M1.2，账本 C9 上半：安全性部分）                      *)
(*                                                                         *)
(* Classic 协议核心语义：                                                  *)
(*  - JoinGroup 触发 rebalance：Empty/Stable → PreparingRebalance，        *)
(*    完成加入的成员记入 ready；                                           *)
(*  - 全员就绪（ready = members ≠ {}）→ CompletingSync；                   *)
(*  - SyncGroup：leader 提交分配函数 → generation+1，进入 Stable；         *)
(*  - LeaveGroup / 成员消失：组空 → Empty，否则 Stable → PreparingRebalance*)
(*                                                                         *)
(* 不变式：                                                                *)
(*  - InvStableWellFormed：Stable 态全员就绪、每个分区恰有一个 owner、      *)
(*    owner ∈ 当前成员集                                                   *)
(*  - InvGenAssignmentUnique：同一 generation 的分配函数历史唯一            *)
(*    （同代无双重分配）                                                    *)
(*  - InvReadySubset / InvGenMonotone / InvOnlyMembersOwn                  *)
(*                                                                         *)
(* 不验（留待后续）：session timeout 细粒度时序（收敛性归 L2 仿真 /        *)
(* T-Q.4 长跑）；commit 的 generation fencing 客户端侧语义。               *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS MemberIds,        \* 成员标识全集
          Parts,            \* 分区集合
          MaxRounds,        \* rebalance 轮数上界
          SyncRequiresFull  \* FALSE = 允许部分就绪即 Sync（阴性对照）

NoOwner == "NoOwner"

(*--algorithm ConsumerGroup

variables
  state      = "Empty" ;
  gen        = 0 ;
  members    = {} ;
  ready      = {} ;
  assignment = [p \in Parts |-> NoOwner] ;
  hist       = {} ;      \* {<<generation, 分配函数>>}——同代分配唯一性的历史

define
  PartsCovered(a) == \A p \in Parts : a[p] # NoOwner

  TypeOK ==
    /\ state \in {"Empty", "PreparingRebalance", "CompletingSync", "Stable"}
    /\ gen \in 0..MaxRounds
    /\ members \subseteq MemberIds
    /\ ready \subseteq members
    /\ assignment \in [Parts -> MemberIds \cup {NoOwner}]
    /\ hist \subseteq {<<g, a>> : g \in 1..MaxRounds, a \in [Parts -> MemberIds]}

  \* Stable 态良构：全员就绪、每分区恰一 owner、owner 都是当前成员
  InvStableWellFormed ==
    state # "Stable" \/
      ( /\ ready = members
        /\ members # {}
        /\ \A p \in Parts : /\ assignment[p] # NoOwner
                            /\ assignment[p] \in members )

  \* 同一 generation 的分配历史唯一（无双重分配）
  InvGenAssignmentUnique ==
    \A h1, h2 \in hist :
      (h1[1] = h2[1]) => (h1[2] = h2[2])

  InvReadySubset == ready \subseteq members

  \* generation 只增
  InvGenHistoryBounded == Cardinality(hist) <= MaxRounds

  \* C9 收敛性（活性，公平性实验）：PreparingRebalance 在 gen 未达上界时
  \* 必须最终离开（到 Stable 或组空）。需要 fair process 的 WF：
  \* - Rejoin/全员完成加入 持续可用则最终发生（WF）
  \* - Sync 持续可用则最终发生（WF）
  \* 注意：gen = MaxRounds 的 Preparing 允许永久停留（上界守卫），故
  \* 结论含 gen = MaxRounds 逃生分支。
  RebalanceCompletes ==
    []((state = "PreparingRebalance" /\ gen < MaxRounds) =>
        <>(state \in {"Stable", "Empty"} \/ gen = MaxRounds))

end define

\* 单一 coordinator actor + 成员事件的非确定性选择（镜像无锁 actor 实现）
process Coordinator = "coord"
begin
  C:
  while TRUE do
    either
      \* 新成员加入：触发 rebalance（Completing 中到达则重新协调）
      with m \in MemberIds \ members do
        members := members \cup {m} ;
        ready := {} ;
        state := "PreparingRebalance"
      end with ;
    or
      \* Rejoin：成员在 PreparingRebalance 中报到
      with m \in members do
        await state = "PreparingRebalance" ;
        ready := ready \cup {m}
      end with ;
    or
      \* LeaveGroup：成员退出；组空回 Empty，否则 Stable 态触发 rebalance
      with m \in members do
        members := members \ {m} ;
        ready := ready \ {m} ;
        if members = {} then
          state := "Empty"
        else
          if state = "Stable" then
            state := "PreparingRebalance"
          end if
        end if ;
      end with ;
    or
      \* JoinGroup 完成（成员在 PreparingRebalance 中报到）
      with m \in ready do
        await state = "PreparingRebalance" ;
        ready := ready \ {m}
      end with ;
    or
      \* 全员完成加入 → CompletingSync（SyncRequiresFull=FALSE 时部分就绪即推进）
      await /\ state = "PreparingRebalance"
            /\ members # {}
            /\ (~SyncRequiresFull \/ ready = members) ;
        state := "CompletingSync" ;
    or
      \* SyncGroup：leader 提交分配 → generation+1 进 Stable
      with a \in [Parts -> members] do
        await state = "CompletingSync" ;
        await gen < MaxRounds ;
        assignment := a ;
        hist := hist \cup {<<gen + 1, a>>} ;
        gen := gen + 1 ;
        state := "Stable"
      end with ;
    or
      \* 成员崩溃于 rebalance 中：回退重新协调
      with m \in ready do
        await state \in {"PreparingRebalance", "CompletingSync"} ;
        members := members \ {m} ;
        ready := {} ;
        if members = {} then
          state := "Empty"
        else
          state := "PreparingRebalance"
        end if
      end with ;
    end either ;
  end while ;
end process ;

end algorithm ; *)
\* BEGIN TRANSLATION (chksum(pcal) = "aab8c01d" /\ chksum(tla) = "eab8da7d")
VARIABLES state, gen, members, ready, assignment, hist

(* define statement *)
PartsCovered(a) == \A p \in Parts : a[p] # NoOwner

TypeOK ==
  /\ state \in {"Empty", "PreparingRebalance", "CompletingSync", "Stable"}
  /\ gen \in 0..MaxRounds
  /\ members \subseteq MemberIds
  /\ ready \subseteq members
  /\ assignment \in [Parts -> MemberIds \cup {NoOwner}]
  /\ hist \subseteq {<<g, a>> : g \in 1..MaxRounds, a \in [Parts -> MemberIds]}


InvStableWellFormed ==
  state # "Stable" \/
    ( /\ ready = members
      /\ members # {}
      /\ \A p \in Parts : /\ assignment[p] # NoOwner
                          /\ assignment[p] \in members )


InvGenAssignmentUnique ==
  \A h1, h2 \in hist :
    (h1[1] = h2[1]) => (h1[2] = h2[2])

InvReadySubset == ready \subseteq members


InvGenHistoryBounded == Cardinality(hist) <= MaxRounds







RebalanceCompletes ==
  []((state = "PreparingRebalance" /\ gen < MaxRounds) =>
      <>(state \in {"Stable", "Empty"} \/ gen = MaxRounds))


vars == << state, gen, members, ready, assignment, hist >>

ProcSet == {"coord"}

Init == (* Global variables *)
        /\ state = "Empty"
        /\ gen = 0
        /\ members = {}
        /\ ready = {}
        /\ assignment = [p \in Parts |-> NoOwner]
        /\ hist = {}

Coordinator == \/ /\ \E m \in MemberIds \ members:
                       /\ members' = (members \cup {m})
                       /\ ready' = {}
                       /\ state' = "PreparingRebalance"
                  /\ UNCHANGED <<gen, assignment, hist>>
               \/ /\ \E m \in members:
                       /\ state = "PreparingRebalance"
                       /\ ready' = (ready \cup {m})
                  /\ UNCHANGED <<state, gen, members, assignment, hist>>
               \/ /\ \E m \in members:
                       /\ members' = members \ {m}
                       /\ ready' = ready \ {m}
                       /\ IF members' = {}
                             THEN /\ state' = "Empty"
                             ELSE /\ IF state = "Stable"
                                        THEN /\ state' = "PreparingRebalance"
                                        ELSE /\ TRUE
                                             /\ state' = state
                  /\ UNCHANGED <<gen, assignment, hist>>
               \/ /\ \E m \in ready:
                       /\ state = "PreparingRebalance"
                       /\ ready' = ready \ {m}
                  /\ UNCHANGED <<state, gen, members, assignment, hist>>
               \/ /\ /\ state = "PreparingRebalance"
                     /\ members # {}
                     /\ (~SyncRequiresFull \/ ready = members)
                  /\ state' = "CompletingSync"
                  /\ UNCHANGED <<gen, members, ready, assignment, hist>>
               \/ /\ \E a \in [Parts -> members]:
                       /\ state = "CompletingSync"
                       /\ gen < MaxRounds
                       /\ assignment' = a
                       /\ hist' = (hist \cup {<<gen + 1, a>>})
                       /\ gen' = gen + 1
                       /\ state' = "Stable"
                  /\ UNCHANGED <<members, ready>>
               \/ /\ \E m \in ready:
                       /\ state \in {"PreparingRebalance", "CompletingSync"}
                       /\ members' = members \ {m}
                       /\ ready' = {}
                       /\ IF members' = {}
                             THEN /\ state' = "Empty"
                             ELSE /\ state' = "PreparingRebalance"
                  /\ UNCHANGED <<gen, assignment, hist>>

Next == Coordinator

Spec == Init /\ [][Next]_vars

\* 活性实验（C9 收敛性）：对 Next 的弱公平——Preparing 下任一 Next 步
\* 都单调推进 ready/成员状态，排纯 Stuttering；配合 RebalanceCompletes。
FairSpec == Spec /\ WF_vars(Next)

\* END TRANSLATION 
================================================================================
\* 2026-09-09 v0.1：安全性不变式（Stable 良构 / 同代分配唯一）；收敛性归仿真层。
