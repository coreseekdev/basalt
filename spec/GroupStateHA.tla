------------------------------- MODULE GroupStateHA -------------------------------
(***************************************************************************)
(* GroupStateHA —— 组状态持久化 HA（方案 B：内部 topic + 重放恢复）         *)
(* （T-M3.6 方案 B，TLA+ 先行）                                             *)
(*                                                                         *)
(* 核心不变式：                                                            *)
(*  InvNoLostAckedCommit : 已 ack 的 offset 必须 <= coordState             *)
(*  （即：ack 过的 commit 在任何时刻都不丢失）                              *)
(*                                                                         *)
(* 阴性对照：                                                              *)
(*  SyncSkip : durableLen 恒 0 → 重启重放空 → accepted 不在 coordState → 红 *)
(*                                                                         *)
(* ⚠ TLC 发现（2026-09-19）：InvNoLostAckedCommit 在当前模型中被 violation  *)
(*  — Sync 的 durable 回退（Sync(3) 后 Sync(1)）+ Compact 交互可导致        *)
(*  已 ack 的 offset 从 coordState 中消失。块 b1 实现时 sync 必须          *)
(*  保证单调递增（n > durableLen），Compact 须只删 coordPos 之前的条目。     *)
(*  修复方案已定：Sync 加 n > durableLen 守卫 + Compact 限制。              *)
(*  突变体验证：SyncSkip 下 InvNoLostAckedCommit 必红（判别力 ✓）           *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS MaxOffset
CONSTANTS MaxLogLen

VARIABLES
  log,
  durableLen,
  coordAlive,
  coordState,
  acked,
  pending

allVars == <<log, durableLen, coordAlive, coordState, acked, pending>>

G == "g1"

Init ==
  /\ log = <<>>
  /\ durableLen = 0
  /\ coordAlive = TRUE
  /\ coordState = [g \in {G} |-> 0]
  /\ acked = {}
  /\ pending = {}

(* client sends commit to coordinator *)
ClientCommit(g, o) ==
  /\ coordAlive
  /\ o > 0
  /\ o <= MaxOffset
  /\ pending' = pending \cup {[g |-> g, o |-> o]}
  /\ UNCHANGED <<log, durableLen, coordAlive, coordState, acked>>

(* coordinator appends commit to internal topic log *)
CoordAppend ==
  /\ coordAlive
  /\ pending # {}
  /\ Len(log) < MaxLogLen
  /\ \E c \in pending :
       /\ log' = Append(log, c)
       /\ pending' = pending \ {c}
       /\ coordState' = [coordState EXCEPT ![c.g] =
             IF c.o > coordState[c.g] THEN c.o ELSE coordState[c.g]]
       /\ UNCHANGED <<durableLen, coordAlive, acked>>

(* durability barrier: first n entries of log survive crash *)
Sync(n) ==
  /\ coordAlive
  /\ n > durableLen
  /\ n <= Len(log)
  /\ durableLen' = n
  /\ UNCHANGED <<log, coordAlive, coordState, acked, pending>>

(* coordinator acks client: commit is durable *)
CoordAck(g, o) ==
  /\ coordAlive
  /\ \E i \in 1..durableLen :
       /\ log[i].g = g
       /\ log[i].o = o
  /\ acked' = acked \cup {[g |-> g, o |-> o]}
  /\ UNCHANGED <<log, durableLen, coordAlive, coordState, pending>>

(* coordinator crashes *)
CoordCrash ==
  /\ coordAlive
  /\ coordAlive' = FALSE
  /\ UNCHANGED <<log, durableLen, coordState, acked, pending>>

(* helper: max committed offset for group g in log[1..n] *)
LastOffsetFor(l, n, g) ==
  LET matching == {i \in 1..n : l[i].g = g}
  IN IF matching = {}
     THEN 0
     ELSE LET vals == {l[i].o : i \in matching}
          IN CHOOSE o \in vals : \A p \in vals : p <= o

(* new coordinator replays durable log *)
CoordRestart ==
  /\ ~coordAlive
  /\ coordAlive' = TRUE
  /\ coordState' = [g \in {G} |-> LastOffsetFor(log, durableLen, g)]
  /\ UNCHANGED <<log, durableLen, acked, pending>>

(* ---------- invariants ---------- *)

(* core: acked commits survive crash-restart *)
InvNoLostAckedCommit ==
  \A c \in acked : coordState[c.g] >= c.o

InvTypeOK ==
  /\ durableLen \in 0..Len(log)

Next ==
  \/ \E o \in 1..MaxOffset : ClientCommit(G, o)
  \/ CoordAppend
  \/ \E n \in 1..MaxLogLen : Sync(n)
  \/ \E o \in 1..MaxOffset : CoordAck(G, o)
  \/ CoordCrash
  \/ CoordRestart

Spec == Init /\ [][Next]_allVars

=============================================================================
