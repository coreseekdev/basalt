------------------------------- MODULE ConsumerGroup -------------------------------
(***************************************************************************)
(* 消费组 rebalance 状态机 v0.3 —— 镜像 coordinator/src/lib.rs 四态机      *)
(* （TASK.md T-M1.1/T-M1.2，账本 C9 上半：安全性部分）                      *)
(* v0.2：CommitOffset fencing + 崩溃踢除；v0.3（不变式评审落地）：          *)
(*  commit 携带 generation 令牌（assignGen）+ 审计位监控（判别力经         *)
(*  MUT-C/MUT-E 突变实证，见 docs/review-invariants-20260910.md）。        *)
(* 已知取舍（P2-4）：gen 达 MaxRounds 后 SyncGroup 守卫使组无法回 Stable—— *)
(* 高轮数收敛不设防，属于模型上界参数的固有 escape hatch。                  *)
(* Stable 态成员失联踢除与 Leave 动作效果等价（同效转移不改变行为图），     *)
(* 不增设冗余动作；memberGen 级僵尸追踪留 v0.4。                            *)
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
  commits    = [p \in Parts |-> -1] ;   \* 每分区已提交 offset（-1 = 无）
  commitOwner = [p \in Parts |-> NoOwner] ;  \* 最后提交者（fencing 审计）
  assignGen  = [p \in Parts |-> 0] ;  \* fencing 令牌：分配创建时的 generation
  commitGen  = [p \in Parts |-> -1] ; \* 最后一次提交携带的 generation
  zombieApplied  = FALSE ;  \* 审计位：提交落到已退成员——守卫完好时恒 FALSE
  staleGenApplied = FALSE ; \* 审计位：旧代令牌提交被放行——同上
  hist       = {} ;      \* {<<generation, 分配函数>>}——同代分配唯一性的历史

define
  PartsCovered(a) == \A p \in Parts : a[p] # NoOwner

  TypeOK ==
    /\ state \in {"Empty", "PreparingRebalance", "CompletingSync", "Stable"}
    /\ gen \in 0..MaxRounds
    /\ members \subseteq MemberIds
    /\ ready \subseteq members
    /\ assignment \in [Parts -> MemberIds \cup {NoOwner}]
    /\ assignGen \in [Parts -> 0..MaxRounds]
    /\ commitGen \in [Parts -> -1..MaxRounds]
    /\ zombieApplied \in BOOLEAN
    /\ staleGenApplied \in BOOLEAN
    /\ hist \subseteq {<<g, a>> : g \in 1..MaxRounds, a \in [Parts -> MemberIds]}

  \* Stable 态良构：全员就绪、每分区恰一 owner、owner 都是当前成员
  InvStableWellFormed ==
    state # "Stable" \/
      ( /\ ready = members
        /\ members # {}
        /\ \A p \in Parts : /\ assignment[p] # NoOwner
                            /\ assignment[p] \in members )

  \* 同一 generation 的分配历史唯一（无双重分配）
  \* 【结构性恒真标注·不变式评审 P1-3】epoch/gen 由构造严格 +1 后才入 hist，
  \* 本性质在本模型内不可违反——它是"机制锁"（防回归 tripwire），非承载性
  \* 约束。判别实验 MUT-A（AssignLeader epoch 回绕）可使其变红。
  InvGenAssignmentUnique ==
    \A h1, h2 \in hist :
      (h1[1] = h2[1]) => (h1[2] = h2[2])

  \* 【结构性恒真标注·不变式评审 P1-3】守卫直接保证，机制锁性质。
  InvReadySubset == ready \subseteq members

  \* C9 fencing（v0.3）：审计位监控。守卫完好时 zombie/stale 恒 FALSE；
  \* 任何 fencing 守卫缺失（成员校验/代令牌）立刻使其变红——判别力经
  \* 突变实验验证（不变式评审 P0-2：纯守卫复述型不变式无判别力）。
  InvCommitFencedMon ==
    zombieApplied = FALSE /\ staleGenApplied = FALSE

  InvCommitTypeOK ==
    /\ commits \in [Parts -> -1..6]
    /\ commitOwner \in [Parts -> MemberIds \cup {NoOwner}]
    /\ \A p \in Parts : commits[p] >= 0 => commitGen[p] <= assignGen[p]



  \* generation 只增【结构性恒真标注·不变式评审 P1-3】机制锁性质。
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
      \* SyncGroup：leader 提交分配 → generation+1 进 Stable；
      \* assignGen 同步推进——成为下一轮提交的 fencing 令牌
      with a \in [Parts -> members] do
        await state = "CompletingSync" ;
        await gen < MaxRounds ;
        assignment := a ;
        assignGen := [p \in Parts |-> gen + 1] ;
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
    or
      \* OffsetCommit（C9 v0.3 fencing）：请求携带 (member, generation)。
      \* 双重令牌：成员必须在组（成员 fencing）+ 请求代 = 分配代（代 fencing）。
      \* 审计位在放行时按事实写入——守卫完好则恒 FALSE，cfg 断言之；
      \* 守卫被突变移除则对应位翻 TRUE（阴性对照自动变红）。
      with m \in members, p \in Parts, o \in 0..6, g \in 0..MaxRounds do
        await /\ assignment[p] = m
              /\ g = assignGen[p]
              /\ o > commits[p] ;
        commits := [commits EXCEPT ![p] = o] ;
        commitOwner := [commitOwner EXCEPT ![p] = m] ;
        commitGen := [commitGen EXCEPT ![p] = g] ;
        zombieApplied := zombieApplied \/ (m \notin members) ;
        staleGenApplied := staleGenApplied \/ (g # assignGen[p])
      end with ;
    end either ;
  end while ;
end process ;

end algorithm ; *)
\* BEGIN TRANSLATION (chksum(pcal) = "39197d9f" /\ chksum(tla) = "254aca15")
VARIABLES state, gen, members, ready, assignment, commits, commitOwner, 
          assignGen, commitGen, zombieApplied, staleGenApplied, hist

(* define statement *)
PartsCovered(a) == \A p \in Parts : a[p] # NoOwner

TypeOK ==
  /\ state \in {"Empty", "PreparingRebalance", "CompletingSync", "Stable"}
  /\ gen \in 0..MaxRounds
  /\ members \subseteq MemberIds
  /\ ready \subseteq members
  /\ assignment \in [Parts -> MemberIds \cup {NoOwner}]
  /\ assignGen \in [Parts -> 0..MaxRounds]
  /\ commitGen \in [Parts -> -1..MaxRounds]
  /\ zombieApplied \in BOOLEAN
  /\ staleGenApplied \in BOOLEAN
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




InvCommitFencedMon ==
  zombieApplied = FALSE /\ staleGenApplied = FALSE

InvCommitTypeOK ==
  /\ commits \in [Parts -> -1..6]
  /\ commitOwner \in [Parts -> MemberIds \cup {NoOwner}]
  /\ \A p \in Parts : commits[p] >= 0 => commitGen[p] <= assignGen[p]




InvGenHistoryBounded == Cardinality(hist) <= MaxRounds







RebalanceCompletes ==
  []((state = "PreparingRebalance" /\ gen < MaxRounds) =>
      <>(state \in {"Stable", "Empty"} \/ gen = MaxRounds))


vars == << state, gen, members, ready, assignment, commits, commitOwner, 
           assignGen, commitGen, zombieApplied, staleGenApplied, hist >>

ProcSet == {"coord"}

Init == (* Global variables *)
        /\ state = "Empty"
        /\ gen = 0
        /\ members = {}
        /\ ready = {}
        /\ assignment = [p \in Parts |-> NoOwner]
        /\ commits = [p \in Parts |-> -1]
        /\ commitOwner = [p \in Parts |-> NoOwner]
        /\ assignGen = [p \in Parts |-> 0]
        /\ commitGen = [p \in Parts |-> -1]
        /\ zombieApplied = FALSE
        /\ staleGenApplied = FALSE
        /\ hist = {}

Coordinator == \/ /\ \E m \in MemberIds \ members:
                       /\ members' = (members \cup {m})
                       /\ ready' = {}
                       /\ state' = "PreparingRebalance"
                  /\ UNCHANGED <<gen, assignment, commits, commitOwner, assignGen, commitGen, zombieApplied, staleGenApplied, hist>>
               \/ /\ \E m \in members:
                       /\ state = "PreparingRebalance"
                       /\ ready' = (ready \cup {m})
                  /\ UNCHANGED <<state, gen, members, assignment, commits, commitOwner, assignGen, commitGen, zombieApplied, staleGenApplied, hist>>
               \/ /\ \E m \in members:
                       /\ members' = members \ {m}
                       /\ ready' = ready \ {m}
                       /\ IF members' = {}
                             THEN /\ state' = "Empty"
                             ELSE /\ IF state = "Stable"
                                        THEN /\ state' = "PreparingRebalance"
                                        ELSE /\ TRUE
                                             /\ state' = state
                  /\ UNCHANGED <<gen, assignment, commits, commitOwner, assignGen, commitGen, zombieApplied, staleGenApplied, hist>>
               \/ /\ \E m \in ready:
                       /\ state = "PreparingRebalance"
                       /\ ready' = ready \ {m}
                  /\ UNCHANGED <<state, gen, members, assignment, commits, commitOwner, assignGen, commitGen, zombieApplied, staleGenApplied, hist>>
               \/ /\ /\ state = "PreparingRebalance"
                     /\ members # {}
                     /\ (~SyncRequiresFull \/ ready = members)
                  /\ state' = "CompletingSync"
                  /\ UNCHANGED <<gen, members, ready, assignment, commits, commitOwner, assignGen, commitGen, zombieApplied, staleGenApplied, hist>>
               \/ /\ \E a \in [Parts -> members]:
                       /\ state = "CompletingSync"
                       /\ gen < MaxRounds
                       /\ assignment' = a
                       /\ assignGen' = [p \in Parts |-> gen + 1]
                       /\ hist' = (hist \cup {<<gen + 1, a>>})
                       /\ gen' = gen + 1
                       /\ state' = "Stable"
                  /\ UNCHANGED <<members, ready, commits, commitOwner, commitGen, zombieApplied, staleGenApplied>>
               \/ /\ \E m \in ready:
                       /\ state \in {"PreparingRebalance", "CompletingSync"}
                       /\ members' = members \ {m}
                       /\ ready' = {}
                       /\ IF members' = {}
                             THEN /\ state' = "Empty"
                             ELSE /\ state' = "PreparingRebalance"
                  /\ UNCHANGED <<gen, assignment, commits, commitOwner, assignGen, commitGen, zombieApplied, staleGenApplied, hist>>
               \/ /\ \E m \in members:
                       \E p \in Parts:
                         \E o \in 0..6:
                           \E g \in 0..MaxRounds:
                             /\ /\ assignment[p] = m
                                /\ g = assignGen[p]
                                /\ o > commits[p]
                             /\ commits' = [commits EXCEPT ![p] = o]
                             /\ commitOwner' = [commitOwner EXCEPT ![p] = m]
                             /\ commitGen' = [commitGen EXCEPT ![p] = g]
                             /\ zombieApplied' = (zombieApplied \/ (m \notin members))
                             /\ staleGenApplied' = (staleGenApplied \/ (g # assignGen[p]))
                  /\ UNCHANGED <<state, gen, members, ready, assignment, assignGen, hist>>

Next == Coordinator

Spec == Init /\ [][Next]_vars

\* END TRANSLATION 


\* 活性性质（C9 收敛性）：RebalanceCompletes 定义见上方 hoisted define 区
FairSpec == Spec /\ WF_vars(Coordinator)
================================================================================
\* 2026-09-09 v0.1：安全性不变式（Stable 良构 / 同代分配唯一）；收敛性归仿真层。
