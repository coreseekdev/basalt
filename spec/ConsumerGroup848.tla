------------------------------- MODULE ConsumerGroup848 -------------------------------
(***************************************************************************)
(* ConsumerGroup848 —— KIP-848 新消费组协议状态机（T-M3.3 块 d，ADR-19 §6） *)
(* 镜像 coordinator/src/consumer_group.rs（纯同步组状态机 + 心跳单循环）。  *)
(*                                                                         *)
(* 与 C9 经典协议（ConsumerGroup.tla）的对照：                              *)
(*  - 无栅栏相位：Join/订阅变化/Leave 单步原子触发重算（actor 单步 = 模型   *)
(*    单步）——无 PreparingRebalance/CompletingSync，无全员停等；            *)
(*  - member-epoch fencing：成员级令牌取代组级 generation。心跳携带         *)
(*    (member, epoch)，epoch 与服务端不符 → 拒绝且零状态变化（C9 的         *)
(*    InvCommitFencedMon 令牌面下沉为「stale-epoch 心跳被拒」）；            *)
(*  - 服务端 Range 分配：确定性（java RangeAssignor 对齐，余数给前位），    *)
(*    非 classic 的 leader 任意函数上交；                                   *)
(*  - OffsetCommit 双令牌：成员在组 + epoch = 服务端 epoch。分区级不校验——  *)
(*    归属迁移期的合法陈旧提交窗口是 KIP-848 协议语义（at-least-once）。    *)
(*                                                                         *)
(* 不变式（C9 重述）：                                                     *)
(*  - InvRegisteredEpochBounded：在组成员 epoch ∈ 1..MaxEpoch（无栅栏：     *)
(*    注册即首算，成员从不驻留 epoch 0——848 设计面的直述性质）              *)
(*  - InvOwnerIsMember：分区 owner ∈ 当前成员集（leave 原子收回）           *)
(*  - InvOwnerSubscribed：owner 必在订阅（退订即收回）                      *)
(*  - InvCoverage：存在订阅成员 ⇒ 每分区有 owner（Range 全覆盖）            *)
(*    【机制锁标注·不变式评审 P1-3 同族】确定性分配器使然，防回归 tripwire *)
(*  - InvHeartbeatFencedMon / InvCommitFencedMon：fencing 审计位监控       *)
(*    （C9 v0.3 同款）：守卫完好恒 FALSE，任何 fencing 守卫缺失立刻变红     *)
(*                                                                         *)
(* 判别力阴性对照（账本 ㊼ 纪律：两只突变体必须红；cfg 突变常数与 ADR-19    *)
(* §6d / Makefile 三方一致）：                                             *)
(*  ① HeartbeatFence=FALSE：陈旧 epoch 心跳放行并改写服务端 epoch           *)
(*    → InvHeartbeatFencedMon 红（提交 fence 完好也拦不住心跳面——两面对     *)
(*    正交，各自的门只抓各自的洞）                                          *)
(*  ② CommitFence=FALSE：未在组/陈旧 epoch 的提交被放行                    *)
(*    → InvCommitFencedMon 红（心跳 fence 完好也拦不住提交面）              *)
(*                                                                         *)
(* 已知取舍（同 C9 v0.3 风格）：epoch 达 MaxEpoch 后重算类动作全部被守卫    *)
(* 阻断——模型上界参数的固有 escape hatch；commit 只前进（o > commits[p]，  *)
(* 经典 spec 同款，Kafka 本身允许回拨）；Leave 不校验请求方身份/epoch（与   *)
(* 实现及 classic LeaveGroup 同信任边界，POC 成员 id 可冒充已知边界，       *)
(* ADR-19 §7）。幂等续租心跳（e = epoch[m]，零状态变化）不建独立动作。      *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS MemberIds,      \* 成员标识全集（模型取 {1, 2}；Range 需全序故取整数）
          Parts,          \* 单 topic 分区集（模型取 {1, 2}）
          MaxEpoch,       \* member-epoch 上界（模型取 4）
          MaxOffsets,     \* 每分区提交 offset 上界（模型取 4）
          HeartbeatFence, \* TRUE=名义（陈旧心跳拒绝）；FALSE=突变体①
          CommitFence     \* TRUE=名义（提交成员级双令牌）；FALSE=突变体②

(* NoOwner 取 0：MemberIds = {1,2} 全为整数（Range 需全序），TLC 的 # 不做
   跨类型比较，占位符须与成员同型 *)
NoOwner == 0

MinOf(M) == CHOOSE m \in M : \A x \in M : m <= x
MaxOf(M) == CHOOSE m \in M : \A x \in M : m >= x

(* 服务端 Range 分配（2 分区 × ≤2 成员的有限化）：无订阅成员 → 全部回收；
   1 个订阅成员 → 全占；2 个 → 连续切块（p_min 给最小 id，余数给前位——
   与 java RangeAssignor 在该形状下结果一致） *)
RangeAssign(M) ==
  IF M = {} THEN [p \in Parts |-> NoOwner]
  ELSE IF Cardinality(M) = 1 THEN [p \in Parts |-> CHOOSE m \in M : TRUE]
  ELSE [p \in Parts |-> IF p = MinOf(Parts) THEN MinOf(M) ELSE MaxOf(M)]

VARIABLES
  members,             \* 在组成员集
  subscribed,          \* [m |-> BOOL]：订阅本 topic（POC 单 topic 有限化）
  epoch,               \* [m |-> 0..MaxEpoch]：服务端视角 member-epoch
  assignment,          \* [p |-> MemberIds/NoOwner]：当前 target assignment
  commits,             \* [p |-> -1..MaxOffsets]：组级已提交 offset（-1=无）
  commitEpoch,         \* [p |-> 0..MaxEpoch]：最后提交携带的 member-epoch
  staleHbApplied,      \* 审计位：陈旧 epoch 心跳被放行——守卫完好恒 FALSE
  zombieCommitApplied, \* 审计位：未在组成员的提交被放行——同上
  staleCommitApplied   \* 审计位：陈旧 epoch 的提交被放行——同上

Vars == <<members, subscribed, epoch, assignment, commits, commitEpoch,
          staleHbApplied, zombieCommitApplied, staleCommitApplied>>

Init ==
  /\ members = {}
  /\ subscribed = [m \in MemberIds |-> FALSE]
  /\ epoch = [m \in MemberIds |-> 0]
  /\ assignment = [p \in Parts |-> NoOwner]
  /\ commits = [p \in Parts |-> -1]
  /\ commitEpoch = [p \in Parts |-> 0]
  /\ staleHbApplied = FALSE
  /\ zombieCommitApplied = FALSE
  /\ staleCommitApplied = FALSE

(* 重算（原子，镜像 actor 单步）：target = Range(订阅成员集)；全体在组
   成员 epoch+1（注册成员 0→1）——幂等续租不 bump 的语义由「仅成员集/
   订阅集变化才触发本动作」承载（镜像 subsig 门） *)
Rebalance(mem, sub, ep) ==
  /\ \A m \in mem : ep[m] < MaxEpoch
  /\ assignment' = RangeAssign({m \in mem : sub[m]})
  /\ epoch' = [m \in MemberIds |-> IF m \in mem
                                   THEN (IF ep[m] = 0 THEN 1 ELSE ep[m] + 1)
                                   ELSE ep[m]]

(* 加入：注册即首算（epoch 0→1 并获得 target）——无栅栏设计面 *)
Join(m, s) ==
  /\ m \notin members
  /\ Rebalance(members \cup {m}, [subscribed EXCEPT ![m] = s], epoch)
  /\ members' = members \cup {m}
  /\ subscribed' = [subscribed EXCEPT ![m] = s]
  /\ UNCHANGED <<commits, commitEpoch, staleHbApplied,
                 zombieCommitApplied, staleCommitApplied>>

(* 订阅变化（含退订）：下一拍重算（镜像 subsig 变化路径） *)
FlipSub(m) ==
  /\ m \in members
  /\ Rebalance(members, [subscribed EXCEPT ![m] = ~subscribed[m]], epoch)
  /\ subscribed' = [subscribed EXCEPT ![m] = ~subscribed[m]]
  /\ UNCHANGED <<members, commits, commitEpoch, staleHbApplied,
                 zombieCommitApplied, staleCommitApplied>>

(* 离开（心跳 epoch=-1）：分区回池，剩余成员重算接管 *)
Leave(m) ==
  /\ m \in members
  /\ Rebalance(members \ {m}, subscribed, epoch)
  /\ members' = members \ {m}
  /\ UNCHANGED <<subscribed, commits, commitEpoch, staleHbApplied,
                 zombieCommitApplied, staleCommitApplied>>

(* 陈旧 epoch 心跳：名义被 fence 拒绝（零状态变化，动作禁用）；突变体①
   放行 epoch 回退并按客户端值改写服务端视角——审计位落 TRUE。（e 限定
   1..MaxEpoch：归零心跳在实现中同样被拒，且其破坏面是 epoch 界而非
   fencing 审计——突变体的捕获面必须落在预期不变式上） *)
StaleHeartbeat(m, e) ==
  /\ ~HeartbeatFence
  /\ m \in members
  /\ e # epoch[m]
  /\ e \in 1..MaxEpoch
  /\ epoch' = [epoch EXCEPT ![m] = e]
  /\ staleHbApplied' = TRUE
  /\ UNCHANGED <<members, subscribed, assignment, commits, commitEpoch,
                 zombieCommitApplied, staleCommitApplied>>

(* OffsetCommit（成员级双令牌）：在组 + epoch = 服务端 epoch。分区级不
   校验（归属迁移期合法窗口）；o 只前进（经典 spec 同款取舍）。突变体②
   摘除令牌守卫 → 审计位按事实落 TRUE。（审计位等式 RHS 加括号：TLA+
   中 `x' = a \/ b` 解析为 `(x' = a) \/ b`，不加括号时突变体下 unconstrained
   var 报错——经典 spec 同款写法） *)
OffsetCommit(m, e, p, o) ==
  /\ (CommitFence => (m \in members /\ e = epoch[m]))
  /\ o > commits[p]
  /\ commits' = [commits EXCEPT ![p] = o]
  /\ commitEpoch' = [commitEpoch EXCEPT ![p] = e]
  /\ zombieCommitApplied' = (zombieCommitApplied \/ (m \notin members))
  /\ staleCommitApplied' = (staleCommitApplied \/ (e # epoch[m]))
  /\ UNCHANGED <<members, subscribed, epoch, assignment, staleHbApplied>>

Next ==
  \/ \E m \in MemberIds, s \in BOOLEAN : Join(m, s)
  \/ \E m \in members : FlipSub(m)
  \/ \E m \in members : Leave(m)
  \/ \E m \in members, e \in 0..MaxEpoch : StaleHeartbeat(m, e)
  \/ \E m \in MemberIds, e \in 0..MaxEpoch, p \in Parts, o \in 0..MaxOffsets :
       OffsetCommit(m, e, p, o)

Spec == Init /\ [][Next]_Vars

(* ---------- 不变式（C9 重述，ADR-19 §6d） ---------- *)

TypeOK ==
  /\ members \subseteq MemberIds
  /\ subscribed \in [MemberIds -> BOOLEAN]
  /\ epoch \in [MemberIds -> 0..MaxEpoch]
  /\ assignment \in [Parts -> MemberIds \cup {NoOwner}]
  /\ commits \in [Parts -> -1..MaxOffsets]
  /\ commitEpoch \in [Parts -> 0..MaxEpoch]
  /\ staleHbApplied \in BOOLEAN
  /\ zombieCommitApplied \in BOOLEAN
  /\ staleCommitApplied \in BOOLEAN

(* 无栅栏：注册即首算——在组成员从不驻留 epoch 0 *)
InvRegisteredEpochBounded == \A m \in members : epoch[m] \in 1..MaxEpoch

(* 僵尸所有权：分区 owner 必在当前成员集（leave 原子收回） *)
InvOwnerIsMember == \A p \in Parts : assignment[p] # NoOwner => assignment[p] \in members

(* 幽灵订阅所有权：owner 必在订阅（退订即收回） *)
InvOwnerSubscribed == \A p \in Parts : assignment[p] # NoOwner => subscribed[assignment[p]]

(* 覆盖面：存在订阅成员 ⇒ 每分区有 owner（无消费空洞；机制锁性质） *)
InvCoverage == (\E m \in members : subscribed[m]) => \A p \in Parts : assignment[p] # NoOwner

(* fencing 审计位监控（C9 v0.3 同款）：守卫完好恒 FALSE；突变体①②各自变红 *)
InvHeartbeatFencedMon == ~staleHbApplied
InvCommitFencedMon == ~zombieCommitApplied /\ ~staleCommitApplied

================================================================================
