------------------------------- MODULE GroupStateHA -------------------------------
(***************************************************************************)
(* GroupStateHA —— 组状态持久化 HA（方案 B 确认模型）                       *)
(*                                                                         *)
(* 设计决策（2026-09-19 确认）：内部 topic 复用 produce 路径——             *)
(* append 与 durable 是同一原子操作（acks=all = ISR 确认即持久化）。       *)
(* 无独立 Sync 步骤——TLA+ 模型 v1 的 violation 是建模 artifact。           *)
(*                                                                         *)
(* 核心不变式：                                                            *)
(*  InvNoLostAckedCommit : acked ⊆ coordState（ack 过的 commit 不丢失）    *)
(*                                                                         *)
(* Rust 映射：CoordAppend → GroupStateStore::append (produce 路径)          *)
(*           CoordAck    → Kafka 响应（append 成功即 ack）                  *)
(*           CoordRestart → GroupManager::replay(store)                     *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS MaxOffset
CONSTANTS MaxLogLen

VARIABLES
  log,          \* internal topic log
  coordAlive,   \* coordinator alive?
  coordState,   \* coordinator in-memory committed offset
  acked,        \* acked commits
  pending       \* received but not yet appended

allVars == <<log, coordAlive, coordState, acked, pending>>

G == "g1"

Init ==
  /\ log = <<>>
  /\ coordAlive = TRUE
  /\ coordState = [g \in {G} |-> 0]
  /\ acked = {}
  /\ pending = {}

ClientCommit(g, o) ==
  /\ coordAlive
  /\ o > 0
  /\ o <= MaxOffset
  /\ pending' = pending \cup {[g |-> g, o |-> o]}
  /\ UNCHANGED <<log, coordAlive, coordState, acked>>

(* append + durable 原子操作（produce acks=all = ISR 确认即持久化） *)
CoordAppend ==
  /\ coordAlive
  /\ pending # {}
  /\ Len(log) < MaxLogLen
  /\ \E c \in pending :
       /\ log' = Append(log, c)
       /\ pending' = pending \ {c}
       /\ coordState' = [coordState EXCEPT ![c.g] =
             IF c.o > coordState[c.g] THEN c.o ELSE coordState[c.g]]
       /\ UNCHANGED <<coordAlive, acked>>

CoordAck(g, o) ==
  /\ coordAlive
  /\ \E i \in 1..Len(log) :
       /\ log[i].g = g
       /\ log[i].o = o
  /\ acked' = acked \cup {[g |-> g, o |-> o]}
  /\ UNCHANGED <<log, coordAlive, coordState, pending>>

CoordCrash ==
  /\ coordAlive
  /\ coordAlive' = FALSE
  /\ UNCHANGED <<log, coordState, acked, pending>>

LastOffsetFor(l, g) ==
  LET matching == {i \in 1..Len(l) : l[i].g = g}
  IN IF matching = {}
     THEN 0
     ELSE LET vals == {l[i].o : i \in matching}
          IN CHOOSE o \in vals : \A p \in vals : p <= o

CoordRestart ==
  /\ ~coordAlive
  /\ coordAlive' = TRUE
  /\ coordState' = [g \in {G} |-> LastOffsetFor(log, g)]
  /\ UNCHANGED <<log, acked, pending>>

InvNoLostAckedCommit == \A c \in acked : coordState[c.g] >= c.o

InvTypeOK == TRUE

Next ==
  \/ \E o \in 1..MaxOffset : ClientCommit(G, o)
  \/ CoordAppend
  \/ \E o \in 1..MaxOffset : CoordAck(G, o)
  \/ CoordCrash
  \/ CoordRestart

Spec == Init /\ [][Next]_allVars

=============================================================================
