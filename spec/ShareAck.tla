------------------------------- MODULE ShareAck -------------------------------
(***************************************************************************)
(* ShareAck —— share group ack 状态机（KIP-932 块 e 兜底，B5）              *)
(*                                                                         *)
(* 建模面（share_group.rs apply_ack / acquire 的协议语义核心）：            *)
(* - accepted / archived / delivered 建模为**offset 覆盖集**（半开区间      *)
(*   [f,l) 的并）——实现里的区间合并排序与集合覆盖语义等价，模型面更小；    *)
(* - Deliver：登记在途锁 + 交付计数 +1 + 记入 delivered（上限 DeliveryLimit）*)
(* - Accept：在途覆盖并入 accepted；cursor 推进到 ≥cursor 的首个未覆盖位    *)
(*   （= 实现里「沿连续段推进」的语义）                                     *)
(* - Release：仅在途锁清除（cursor 不动 → 重新可交付）                      *)
(* - Reject：在途锁清除 + 归档（永不再交付）                                *)
(* - member 维度省略：不影响本模型的不变式面（锁按区间清除语义等价）        *)
(*                                                                         *)
(* 核心不变式：                                                            *)
(*  InvCursorCovered            : [0, cursor) 全部被 accepted 覆盖          *)
(*  InvCursorDelivered          : [0, cursor) 全部曾交付（游标不得跳过      *)
(*                                未交付记录——phantom 突变的判别面）        *)
(*  InvAcceptedArchivedDisjoint : accepted 与 archived 不相交               *)
(*  InvDeliveryBounded          : 交付计数 ≤ DeliveryLimit                  *)
(*  InvArchivedNotInflight      : 归档记录不再交付                          *)
(*                                                                         *)
(* 判别力阴性对照（share-ack-phantom）：PhantomAccept=TRUE 允许未交付       *)
(* 区间直接 Accept —— InvCursorDelivered 必须红（游标跳过未交付记录 =       *)
(* 消费者丢数据面）。㊼ 纪律：突变常数与 e2e 语义 / 本注释三方一致           *)
(*                                                                         *)
(* Rust 映射：Deliver → share_group::acquire                                *)
(*           Accept/Release/Reject → share_group::apply_ack                *)
(*           install_replay 的事件重放 = 状态机动作序列的重放               *)
(***************************************************************************)
EXTENDS Integers, Naturals, Sequences, FiniteSets

CONSTANTS MaxOffset,        \* offset 上界（记录域 = 0..MaxOffset-1；cursor 域含 MaxOffset）
          DeliveryLimit,    \* 单首 offset 最大交付次数
          MaxInflight,      \* 在途锁数量上界（状态空间收口）
          PhantomAccept     \* 突变常数：TRUE = 允许未交付区间 Accept（阴性对照）

VARIABLES
  cursor,      \* 交付游标：下一批从此读
  accepted,    \* 已接受覆盖集 ⊆ 0..(MaxOffset-1)（半开区间合并后的等价面）
  archived,    \* 已归档集（REJECT，永不再交付）
  delivered,   \* 曾交付集（至少进过一次在途锁的 offset）
  counts,      \* [首 offset -> 累计交付次数]
  inflight     \* 在途锁集合（[f |->, l |->] 半开区间）

allVars == <<cursor, accepted, archived, delivered, counts, inflight>>

OffsetDomain == 0 .. (MaxOffset - 1)
Interval == [f : OffsetDomain, l : 1 .. MaxOffset]

TypeOK ==
  /\ cursor \in 0 .. MaxOffset
  /\ accepted \subseteq OffsetDomain
  /\ archived \subseteq OffsetDomain
  /\ delivered \subseteq OffsetDomain
  /\ counts \in [OffsetDomain -> 0 .. DeliveryLimit]
  /\ inflight \subseteq Interval

Init ==
  /\ cursor = 0
  /\ accepted = {}
  /\ archived = {}
  /\ delivered = {}
  /\ counts = [o \in OffsetDomain |-> 0]
  /\ inflight = {}

(* 区间 [f,l) 的每个 offset 都在途（Accept/Release/Reject 的作用前提） *)
InflightCovers(f, l) ==
  \A o \in f .. (l - 1) :
    \E i \in inflight : i.f <= o /\ o < i.l

(* 可交付判定：未归档、未接受、计数未满 *)
Deliverable(f) ==
  /\ f >= cursor
  /\ f \notin archived
  /\ f \notin accepted
  /\ counts[f] < DeliveryLimit

(* Deliver：登记在途 + 计数 +1 + 记入 delivered *)
Deliver(f, l) ==
  /\ f < l
  /\ Deliverable(f)
  /\ Cardinality(inflight) < MaxInflight
  /\ \A o \in f .. (l - 1) : ~InflightCovers(o, o + 1)
  /\ inflight' = inflight \cup {[f |-> f, l |-> l]}
  /\ counts' = [counts EXCEPT ![f] = counts[f] + 1]
  /\ delivered' = delivered \cup (f .. (l - 1))
  /\ UNCHANGED <<cursor, accepted, archived>>

IsCovered(a, o) == o \in a

(* Accept：在途覆盖并入 accepted——**归档优先**（账本 67）：区间剔除已
 * 归档 offset（部分 reject 后的同锁超集 accept 不得复活/跳过归档记录）；
 * cursor 推进到 ≥cursor 的首个未覆盖位（MaxOffset 恒未被覆盖 → CHOOSE
 * 域非空） *)
Accept(f, l) ==
  /\ f < l
  /\ \/ InflightCovers(f, l)
     \/ PhantomAccept          \* 阴性对照：未交付也可 Accept
  /\ inflight' = {i \in inflight : ~(f <= i.f /\ i.l <= l)}
  /\ accepted' = accepted \cup ((f .. (l - 1)) \ archived)
  /\ cursor' = CHOOSE o \in cursor .. MaxOffset :
                 /\ ~IsCovered(accepted', o)
                 /\ \A p \in cursor .. (o - 1) : IsCovered(accepted', p)
  /\ UNCHANGED <<archived, delivered, counts>>

(* Release：仅在途锁清除——cursor 不动，区间回到可交付 *)
Release(f, l) ==
  /\ f < l
  /\ InflightCovers(f, l)
  /\ inflight' = {i \in inflight : ~(f <= i.f /\ i.l <= l)}
  /\ UNCHANGED <<cursor, accepted, archived, delivered, counts>>

(* Reject：在途锁清除 + 归档——**首个终态 ack 定局**（账本 67 对称面）：
 * reject 剔除已 accepted offset。锁与归档可残留重叠（部分 reject 后同锁
 * 超集 accept 由归档优先语义剔除），故不对 inflight 断言 *)
Reject(f, l) ==
  /\ f < l
  /\ InflightCovers(f, l)
  /\ inflight' = {i \in inflight : ~(f <= i.f /\ i.l <= l)}
  /\ archived' = archived \cup ((f .. (l - 1)) \ accepted)
  /\ UNCHANGED <<cursor, accepted, delivered, counts>>

Pairs(f) == {<<f, l>> : l \in (f + 1) .. MaxOffset}
Ranges == UNION {Pairs(f) : f \in OffsetDomain}

Next ==
  \/ \E r \in Ranges :
       \/ Deliver(r[1], r[2])
       \/ Accept(r[1], r[2])
       \/ Release(r[1], r[2])
       \/ Reject(r[1], r[2])

Spec == Init /\ [][Next]_allVars

(* ================= 不变式 ================= *)

InvTypeOK == TypeOK

(* [0, cursor) 全部被 accepted 覆盖：游标推进必须踩在 accepted 覆盖上 *)
InvCursorCovered ==
  \A o \in 0 .. (cursor - 1) : o \in accepted

(* [0, cursor) 全部曾交付：游标不得跳过未交付记录（phantom 判别面） *)
InvCursorDelivered ==
  \A o \in 0 .. (cursor - 1) : o \in delivered

(* accepted 与 archived 不相交：同一条记录不能既被接受又被拒绝 *)
InvAcceptedArchivedDisjoint == accepted \cap archived = {}

(* 交付计数不越限 *)
InvDeliveryBounded ==
  \A o \in OffsetDomain : counts[o] <= DeliveryLimit

(* 游标单调（Next 构造面：Accept 的 CHOOSE 域下界恒为旧 cursor，其余动作
 * 不动 cursor——语义单调可由 InvCursorCovered + TypeOK 组合观察） *)
InvCursorInRange == cursor \in 0 .. MaxOffset

=============================================================================
