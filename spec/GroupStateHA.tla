------------------------------- MODULE GroupStateHA -------------------------------
(***************************************************************************)
(* GroupStateHA —— 组状态持久化 HA（方案 B：内部 topic + 重放恢复）         *)
(* （T-M3.6 方案 B，TLA+ 先行）                                             *)
(*                                                                         *)
(* 核心不变式：                                                            *)
(*  InvNoLostAckedCommit : 已 ack 的 offset 必须 <= coordState             *)
(*                                                                         *)
(* 重启安全守卫：CoordRestart 仅在 durable log 覆盖所有 acked 时才可发生    *)
(*  （生产语义 = 等 ISR 追平后再恢复服务）                                  *)
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

ClientCommit(g, o) ==
  /\ coordAlive
  /\ o > 0
  /\ o <= MaxOffset
  /\ pending' = pending \cup {[g |-> g, o |-> o]}
  /\ UNCHANGED <<log, durableLen, coordAlive, coordState, acked>>

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

Sync(n) ==
  /\ coordAlive
  /\ n > durableLen
  /\ n <= Len(log)
  /\ durableLen' = n
  /\ UNCHANGED <<log, coordAlive, coordState, acked, pending>>

CoordAck(g, o) ==
  /\ coordAlive
  /\ \E i \in 1..durableLen :
       /\ log[i].g = g
       /\ log[i].o = o
  /\ acked' = acked \cup {[g |-> g, o |-> o]}
  /\ UNCHANGED <<log, durableLen, coordAlive, coordState, pending>>

CoordCrash ==
  /\ coordAlive
  /\ coordAlive' = FALSE
  /\ UNCHANGED <<log, durableLen, coordState, acked, pending>>

LastOffsetFor(l, n, g) ==
  LET matching == {i \in 1..n : l[i].g = g}
  IN IF matching = {}
     THEN 0
     ELSE LET vals == {l[i].o : i \in matching}
          IN CHOOSE o \in vals : \A p \in vals : p <= o


ReplayState(l, n) == [g \in {G} |-> LastOffsetFor(l, n, g)]

(* restart only when durable log covers all acked commits *)
CoordRestart ==
  /\ ~coordAlive
  /\ coordAlive' = TRUE
  /\ coordState' = [g \in {G} |-> LastOffsetFor(log, durableLen, g)]
  /\ UNCHANGED <<log, durableLen, acked, pending>>

InvNoLostAckedCommit == \A c \in acked : coordState[c.g] >= c.o

InvTypeOK == durableLen \in 0..Len(log)

Next ==
  \/ \E o \in 1..MaxOffset : ClientCommit(G, o)
  \/ CoordAppend
  \/ \E n \in 1..MaxLogLen : Sync(n)
  \/ \E o \in 1..MaxOffset : CoordAck(G, o)
  \/ CoordCrash
  \/ CoordRestart

Spec == Init /\ [][Next]_allVars

=============================================================================
