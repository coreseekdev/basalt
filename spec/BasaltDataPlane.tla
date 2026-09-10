------------------------------- MODULE BasaltDataPlane -------------------------------
(***************************************************************************)
(* Basalt 数据面复制协议 v0.1 —— ADR-10 / TASK.md T-Q.2                    *)
(*                                                                         *)
(* 模型对象：单写者控制器指派 leader + epoch fencing + follower-pull 全量  *)
(* 同步（含截断对齐）+ 多数派 commit + 消费者游标有界。                     *)
(*                                                                         *)
(* 关键抽象：                                                              *)
(*  - Push = follower-pull 的原子步：fetch + truncate-to-match + append +  *)
(*    元数据确认（leader-epoch checkpoint 更新）。                         *)
(*  - committed 是只追加的 oracle：仅当多数派持有待提交前缀时扩展。        *)
(*    “不丢”体现为不变式 InvLeaderHasCommitted（已 ack 数据恒在服务主上）  *)
(*    与 InvConsumedOnLeader（消费者读到的恒可从服务主读出）。             *)
(*  - view[n] = 节点 n 已听到的 (epoch, leader)，持久化（对齐 Kafka        *)
(*    leader-epoch checkpoint 落盘的假设）。                               *)
(*                                                                         *)
(* 三个实验开关（见 4 个 .cfg）：                                          *)
(*  - EagerLeader=TRUE ：新 leader 跳过继任规则直接服务 —— 用于演示继任    *)
(*    规则是 C1（不丢）的必要设计；TLC 应检出反例。                        *)
(*  - SplitBrain=TRUE ：控制器 fencing 失效，旧主在不知自身已被替换时继续  *)
(*    服务 —— 用于检验 follower 侧 epoch fencing + 继任规则是否自足。      *)
(*  - CommitChecksEpoch=TRUE：commit 多数派需确认各成员视图与 leader 一致  *)
(*    （内容校验之外的额外防线）。                                         *)
(*                                                                         *)
(* 不验（留待后续版本）：事务/LSO、ISR 收缩扩张、unclean 选举（ADR-8）、   *)
(* 活性（崩溃可无限发生；收敛性由 L2 仿真与 T-Q.4 长跑覆盖）。             *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS Brokers,            \* broker 节点集合
          Values,             \* 消息值字母表（小字母表以区分丢失/篡改）
          MaxEpochs,          \* 控制器纪元上界
          MaxLogLen,          \* 单副本日志长度上界
          EagerLeader,        \* TRUE = 新 leader 跳过继任规则（反例演示）
          SplitBrain,         \* TRUE = 控制器 fencing 失效（旧主继续服务）
          CommitChecksEpoch,  \* TRUE = commit 多数派校验成员视图
          ViewRollback        \* TRUE = Restart 载入过期 view 检查点（C14⑤ 实验）

NoLeader == "NoLeader"

(***************************************************************************)
(* 状态                                                                    *)
(*   up[n]        节点存活（crash = 进程死亡；磁盘数据保留）               *)
(*   parted[n]    n 与控制器失联（收不到 epoch 广播；Heal 时补听）         *)
(*   curEpoch     控制器纪元（只增）                                       *)
(*   curLeader    控制器当前指派（全局事实）                               *)
(*   view[n]      n 已听到的 [epoch, leader]（fencing 凭据，持久化）       *)
(*   caughtUp[n]  n 作为新 leader 是否已完成继任接管                       *)
(*   leaderHist   历史指派集 {<<epoch, node>>}（验证每 epoch 至多一主）    *)
(*   log[n]       副本日志：<<写入 epoch, 值>> 序列                        *)
(*   committed    已提交（已 ack）值序列 —— 只追加 oracle                  *)
(*   consumed     消费者已读值序列 —— 只追加                               *)
(***************************************************************************)
(***************************************************************************)
(* --algorithm BasaltDataPlane

variables
  up         = [n \in Brokers |-> TRUE] ;
  parted     = [n \in Brokers |-> FALSE] ;
  curEpoch   = 0 ;
  curLeader  = NoLeader ;
  view       = [n \in Brokers |-> [epoch |-> 0, leader |-> NoLeader]] ;
  caughtUp   = [n \in Brokers |-> TRUE] ;
  leaderHist = {} ;
  log        = [n \in Brokers |-> <<>>] ;
  committed  = <<>> ;
  consumed   = <<>> ;
  synced     = [n \in Brokers |-> 0] ;
  lease      = [n \in Brokers |-> FALSE] ;

define
  Majors == { q \in SUBSET Brokers :
                Cardinality(q) * 2 > Cardinality(Brokers) }

  IsPrefix(p, s) == /\ Len(p) <= Len(s)
                    /\ \A i \in 1..Len(p) : p[i] = s[i]

  Vals(lg) == [i \in 1..Len(lg) |-> lg[i][2]]

  IsValsPrefix(vs, lg) == IsPrefix(vs, Vals(lg))

  LastEpoch(lg) == IF Len(lg) = 0 THEN 0 ELSE lg[Len(lg)][1]

  SameVals(lg1, lg2, L) == /\ Len(lg1) >= L
                           /\ Len(lg2) >= L
                           /\ \A i \in 1..L : lg1[i][2] = lg2[i][2]

  \* 继任规则（Raft 选举限制的数据面形态）：j 是 q 中 (lastEpoch, len)
  \* 的字典序最大者。T-M2.3 的"继任者预计算"必须实现为该规则。
  SuccessorOf(q, j) == \A r \in q :
         \/ LastEpoch(log[j]) > LastEpoch(log[r])
         \/ /\ LastEpoch(log[j]) = LastEpoch(log[r])
            /\ Len(log[j]) >= Len(log[r])

  \* n 自认为主（SplitBrain=TRUE 时失联旧主凭**仍有效的租约**继续服务——
  \* 分区不撤销租约；crash 撤销租约且 SplitBrain 下控制器不可重授——
  \* v0.3 修复：无租约的崩溃旧主不得以原 epoch 自恢复（否则同 epoch 重写
  \* 分叉日志，InvLogMatching 红，见 docs/review-invariants-20260910.md））
  ThinksLeader(n) == /\ up[n]
                     /\ lease[n]
                     /\ view[n].leader = n
                     /\ (\/ SplitBrain
                         \/ /\ curLeader = n
                            /\ view[n].epoch = curEpoch)

  \* n 可实际服务（自认为主 且 已完成继任接管）
  Serving(n) == /\ ThinksLeader(n)
                /\ caughtUp[n]

  TypeOK ==
    /\ up \in [Brokers -> BOOLEAN]
    /\ parted \in [Brokers -> BOOLEAN]
    /\ curEpoch \in 0..MaxEpochs
    /\ curLeader \in Brokers \cup {NoLeader}
    /\ view \in [Brokers -> [epoch : 0..MaxEpochs,
                             leader : Brokers \cup {NoLeader}]]
    /\ caughtUp \in [Brokers -> BOOLEAN]
    /\ leaderHist \subseteq ((1..MaxEpochs) \X Brokers)
    /\ log \in [Brokers ->
          Seq({<<e, v>> : e \in 1..MaxEpochs, v \in Values})]
    /\ committed \in Seq(Values)
    /\ consumed \in Seq(Values)
    /\ synced \in [Brokers -> 0..MaxLogLen]
    /\ lease \in [Brokers -> BOOLEAN]

  \* C2a：每个 epoch 至多一个被指派的 leader
  \* 【结构性标注·不变式评审 P1-3】epoch 由构造严格 +1 入 hist——机制锁
  \* （防回归 tripwire）而非承载约束；MUT-A（epoch 回绕）可使其变红。
  InvOneLeaderPerEpoch ==
    \A e \in 1..MaxEpochs :
      Cardinality({n \in Brokers : <<e, n>> \in leaderHist}) <= 1

  \* 无人知晓"未来"的 epoch【结构性标注·不变式评审 P1-3】无动作写
  \* view[n].epoch > curEpoch——机制锁性质。
  InvViewEpochSane == \A n \in Brokers : view[n].epoch <= curEpoch

  \* 日志匹配：同 index 同写入 epoch 的条目值相同（Raft Log Matching 的
  \* 数据面形态；它是继任规则正确性的前提）
  InvLogMatching ==
    \A a, b \in Brokers :
      \A i \in (1..Len(log[a])) \cap (1..Len(log[b])) :
        (log[a][i][1] = log[b][i][1]) => (log[a][i][2] = log[b][i][2])

  \* C1a（不丢 = 多数派持久性）：已 ack 数据在每一个多数派中都存在完整副本。
  \* 这是继任规则能恢复全部 acked 数据的前提（任意两个多数派相交）。
  InvLeaderHasCommitted ==
    \A q \in Majors : \E r \in q : IsValsPrefix(committed, log[r])

  \* C1b：现任主一旦完成接管（可服务），必持有全部 acked 数据
  InvCurrentLeaderHasCommitted ==
    \A n \in Brokers :
      (curLeader = n /\ view[n].leader = n /\ view[n].epoch = curEpoch
       /\ caughtUp[n]) => IsValsPrefix(committed, log[n])

  \* C1d（commit ⇒ 多数派已持久，C14①）：已提交长度不超过任一多数派中
  \* 至少一个成员的持久化水位——多数派相交 ⇒ 提交数据在任意 Surviving
  \* 多数派中至少有一份 fsync 过的完整副本。这是 §3 故障模型（crash 截断
  \* 到 synced）下"不丢"的持久性支柱。
  InvCommittedDurable ==
    \A q \in Majors : \E r \in q : Len(committed) <= synced[r]

  \* C4（游标有界）：消费者只读已提交前缀。
  \* v0.2：Consume 改为向现任主 fetch（值取自主的日志，长度以已提交
  \* 前缀为界）——本不变式不再由构造保证，而依赖 C1b（主持有已提交
  \* 前缀）+ 日志匹配。幻读（脏主/分叉日志可被消费）在此显式变红。
  InvConsumedBounded == IsPrefix(consumed, committed)

  \* C4'（端到端形态）：消费者读到的每条都能从任一多数派中恢复。
  \* 【推理闭包标注·不变式评审 P1-2】由 InvConsumedBounded + 
  \* InvLeaderHasCommitted 传递可得——规约语义恒真（定理），保留其
  \* 文档价值，不作为独立防线。
  InvConsumedOnLeader ==
    \A q \in Majors : \E r \in q : IsPrefix(consumed, Vals(log[r]))

end define

\* ---------------------------- 控制器 ----------------------------
process Controller = "ctrl"
begin
  C:
  while TRUE do
    either
      \* AssignLeader：epoch+1 并指派；未失联节点立即听到
      with n \in Brokers do
        await curEpoch < MaxEpochs ;
        curEpoch := curEpoch + 1 ;
        curLeader := n ;
        lease := [lease EXCEPT ![n] = TRUE] ;
        leaderHist := leaderHist \cup {<<curEpoch, n>>} ;
        caughtUp := [caughtUp EXCEPT ![n] = EagerLeader] ;
        view := [n2 \in Brokers |->
                   IF parted[n2] THEN view[n2]
                   ELSE [epoch |-> curEpoch, leader |-> n]]
      end with ;
    or
      \* Crash：进程死亡；若为主，控制器标记主缺失
      with n \in Brokers do
        await up[n] ;
        up := [up EXCEPT ![n] = FALSE] ;
        \* crash 丢失未持久尾部（§3 故障模型：已 sync 存活、未 sync 消失）
        log := [log EXCEPT ![n] = IF synced[n] >= Len(log[n]) THEN log[n] ELSE IF synced[n] = 0 THEN <<>> ELSE SubSeq(log[n], 1, synced[n])] ;
        \* 重启后需重新继任接管（恢复扫描 + 与多数派比对）
        caughtUp := [caughtUp EXCEPT ![n] = FALSE] ;
        \* 租约随进程死亡失效（ADR-10：重授仅经控制器——SplitBrain 下
        \* 控制器不可达 ⇒ 崩溃旧主永久失去服务资格）
        lease := [lease EXCEPT ![n] = FALSE] ;
        if curLeader = n then
          curLeader := NoLeader
        end if
      end with ;
    or
      \* Persist：fsync 边界——把日志尾部推进持久化水位（ADR-14 写/持久边界）
      with n \in Brokers do
        await up[n] ;
        await synced[n] < Len(log[n]) ;
        synced := [synced EXCEPT ![n] = Len(log[n])]
      end with ;
    or
      \* Restart：磁盘数据（synced 前缀）与 view 持久保留；
      \* ViewRollback=TRUE 时载入过期 checkpoint（C14⑤ 实验：回退方向）
      with n \in Brokers do
        await ~up[n] ;
        up := [up EXCEPT ![n] = TRUE] ;
        if ViewRollback /\ view[n].epoch > 0 then
          view := [view EXCEPT ![n] =
                     [epoch |-> view[n].epoch - 1, leader |-> view[n].leader]]
        end if
      end with ;
    or
      \* Partition：与控制器失联
      with n \in Brokers do
        await ~parted[n] ;
        parted := [parted EXCEPT ![n] = TRUE]
      end with ;
    or
      \* Heal：恢复链路时补听最新元数据
      with n \in Brokers do
        await parted[n] ;
        parted := [parted EXCEPT ![n] = FALSE] ;
        view := [view EXCEPT ![n] =
                   [epoch |-> curEpoch, leader |-> curLeader]]
      end with ;
    end either ;
  end while ;
end process ;

\* ---------------------------- broker ----------------------------
process Broker \in Brokers
begin
  B:
  while TRUE do
    either
      \* CatchUp：继任接管——从某多数派中 (lastEpoch,len) 最大者取整日志。
      \* 网络语义：parted 节点既发不出也收不到。
      with q \in Majors, j \in q do
        await /\ ~EagerLeader
              /\ ThinksLeader(self)
              /\ ~caughtUp[self]
              /\ ~parted[self]
              /\ up[j]
              /\ ~parted[j]
              /\ SuccessorOf(q, j) ;
        log := [log EXCEPT ![self] = log[j]] ;
        caughtUp := [caughtUp EXCEPT ![self] = TRUE]
      end with ;
    or
      \* Produce：服务主在日志尾追加，条目带当前 epoch 标签
      with v \in Values do
        await /\ Serving(self)
              /\ Len(log[self]) < MaxLogLen ;
        log := [log EXCEPT ![self] =
                  Append(log[self], <<view[self].epoch, v>>)]
      end with ;
    or
      \* Push：follower-pull 的原子抽象（fetch + 截断对齐 + 追加 +
      \* epoch 确认）。follower 侧 fencing：只接受不晚于自身 view 的
      \* epoch，且同 epoch 必须来自同一 leader。
      with f \in Brokers do
        await /\ Serving(self)
              /\ f # self
              /\ up[f]
              /\ ~parted[self]
              /\ ~parted[f]
              /\ log[f] # log[self]
              /\ (\/ view[f].epoch < view[self].epoch
                  \/ /\ view[f].epoch = view[self].epoch
                     /\ view[f].leader = self) ;
        log := [log EXCEPT ![f] = log[self]] ;
        view := [view EXCEPT ![f] =
                   [epoch |-> view[self].epoch, leader |-> self]]
      end with ;
    or
      \* CommitAdvance：多数派持有待提交前缀（CommitChecksEpoch=TRUE 时
      \* 还需多数派视图与本主一致）才推进提交点 = 客户端 ack 点
      with q \in Majors, L \in Len(committed)+1..Len(log[self]) do
        await /\ Serving(self)
              /\ IsValsPrefix(committed, log[self])
              /\ \A r \in q :
                   /\ L <= synced[r]          \* 持久化边界（C14①）
                   /\ SameVals(log[r], log[self], L)
                   /\ (\/ ~CommitChecksEpoch
                       \/ /\ view[r].epoch = view[self].epoch
                          /\ view[r].leader = self) ;
        committed := [i \in 1..L |-> log[self][i][2]]
      end with ;
    or
      \* Consume：消费 fetch 打到现任主，读其日志第 i+1 项。
      \* 长度以已提交前缀为界（HW 过滤）；值取自主的日志而非 committed
      \* 变量——幻读检测点：该位置若主日志与已提交值分叉（EagerLeader/
      \* 脏主形态），InvConsumedBounded 变红（不变式评审 P1-1 处置 (a)）
      with r \in Brokers do
        await /\ curLeader = r
              /\ view[r].leader = r
              /\ view[r].epoch = curEpoch
              /\ caughtUp[r]
              /\ Len(consumed) < Len(log[r])
              /\ Len(consumed) < Len(committed) ;
        consumed := Append(consumed, log[r][Len(consumed)+1][2])
      end with
    end either ;
  end while ;
end process ;

end algorithm ; *)
\* BEGIN TRANSLATION (chksum(pcal) = "b7555bef" /\ chksum(tla) = "9ab6cb93")
VARIABLES up, parted, curEpoch, curLeader, view, caughtUp, leaderHist, log, 
          committed, consumed, synced, lease

(* define statement *)
Majors == { q \in SUBSET Brokers :
              Cardinality(q) * 2 > Cardinality(Brokers) }

IsPrefix(p, s) == /\ Len(p) <= Len(s)
                  /\ \A i \in 1..Len(p) : p[i] = s[i]

Vals(lg) == [i \in 1..Len(lg) |-> lg[i][2]]

IsValsPrefix(vs, lg) == IsPrefix(vs, Vals(lg))

LastEpoch(lg) == IF Len(lg) = 0 THEN 0 ELSE lg[Len(lg)][1]

SameVals(lg1, lg2, L) == /\ Len(lg1) >= L
                         /\ Len(lg2) >= L
                         /\ \A i \in 1..L : lg1[i][2] = lg2[i][2]



SuccessorOf(q, j) == \A r \in q :
       \/ LastEpoch(log[j]) > LastEpoch(log[r])
       \/ /\ LastEpoch(log[j]) = LastEpoch(log[r])
          /\ Len(log[j]) >= Len(log[r])





ThinksLeader(n) == /\ up[n]
                   /\ lease[n]
                   /\ view[n].leader = n
                   /\ (\/ SplitBrain
                       \/ /\ curLeader = n
                          /\ view[n].epoch = curEpoch)


Serving(n) == /\ ThinksLeader(n)
              /\ caughtUp[n]

TypeOK ==
  /\ up \in [Brokers -> BOOLEAN]
  /\ parted \in [Brokers -> BOOLEAN]
  /\ curEpoch \in 0..MaxEpochs
  /\ curLeader \in Brokers \cup {NoLeader}
  /\ view \in [Brokers -> [epoch : 0..MaxEpochs,
                           leader : Brokers \cup {NoLeader}]]
  /\ caughtUp \in [Brokers -> BOOLEAN]
  /\ leaderHist \subseteq ((1..MaxEpochs) \X Brokers)
  /\ log \in [Brokers ->
        Seq({<<e, v>> : e \in 1..MaxEpochs, v \in Values})]
  /\ committed \in Seq(Values)
  /\ consumed \in Seq(Values)
  /\ synced \in [Brokers -> 0..MaxLogLen]
  /\ lease \in [Brokers -> BOOLEAN]




InvOneLeaderPerEpoch ==
  \A e \in 1..MaxEpochs :
    Cardinality({n \in Brokers : <<e, n>> \in leaderHist}) <= 1



InvViewEpochSane == \A n \in Brokers : view[n].epoch <= curEpoch



InvLogMatching ==
  \A a, b \in Brokers :
    \A i \in (1..Len(log[a])) \cap (1..Len(log[b])) :
      (log[a][i][1] = log[b][i][1]) => (log[a][i][2] = log[b][i][2])



InvLeaderHasCommitted ==
  \A q \in Majors : \E r \in q : IsValsPrefix(committed, log[r])


InvCurrentLeaderHasCommitted ==
  \A n \in Brokers :
    (curLeader = n /\ view[n].leader = n /\ view[n].epoch = curEpoch
     /\ caughtUp[n]) => IsValsPrefix(committed, log[n])





InvCommittedDurable ==
  \A q \in Majors : \E r \in q : Len(committed) <= synced[r]





InvConsumedBounded == IsPrefix(consumed, committed)





InvConsumedOnLeader ==
  \A q \in Majors : \E r \in q : IsPrefix(consumed, Vals(log[r]))


vars == << up, parted, curEpoch, curLeader, view, caughtUp, leaderHist, log, 
           committed, consumed, synced, lease >>

ProcSet == {"ctrl"} \cup (Brokers)

Init == (* Global variables *)
        /\ up = [n \in Brokers |-> TRUE]
        /\ parted = [n \in Brokers |-> FALSE]
        /\ curEpoch = 0
        /\ curLeader = NoLeader
        /\ view = [n \in Brokers |-> [epoch |-> 0, leader |-> NoLeader]]
        /\ caughtUp = [n \in Brokers |-> TRUE]
        /\ leaderHist = {}
        /\ log = [n \in Brokers |-> <<>>]
        /\ committed = <<>>
        /\ consumed = <<>>
        /\ synced = [n \in Brokers |-> 0]
        /\ lease = [n \in Brokers |-> FALSE]

Controller == /\ \/ /\ \E n \in Brokers:
                         /\ curEpoch < MaxEpochs
                         /\ curEpoch' = curEpoch + 1
                         /\ curLeader' = n
                         /\ lease' = [lease EXCEPT ![n] = TRUE]
                         /\ leaderHist' = (leaderHist \cup {<<curEpoch', n>>})
                         /\ caughtUp' = [caughtUp EXCEPT ![n] = EagerLeader]
                         /\ view' = [n2 \in Brokers |->
                                       IF parted[n2] THEN view[n2]
                                       ELSE [epoch |-> curEpoch', leader |-> n]]
                    /\ UNCHANGED <<up, parted, log, synced>>
                 \/ /\ \E n \in Brokers:
                         /\ up[n]
                         /\ up' = [up EXCEPT ![n] = FALSE]
                         /\ log' = [log EXCEPT ![n] = IF synced[n] >= Len(log[n]) THEN log[n] ELSE IF synced[n] = 0 THEN <<>> ELSE SubSeq(log[n], 1, synced[n])]
                         /\ caughtUp' = [caughtUp EXCEPT ![n] = FALSE]
                         /\ lease' = [lease EXCEPT ![n] = FALSE]
                         /\ IF curLeader = n
                               THEN /\ curLeader' = NoLeader
                               ELSE /\ TRUE
                                    /\ UNCHANGED curLeader
                    /\ UNCHANGED <<parted, curEpoch, view, leaderHist, synced>>
                 \/ /\ \E n \in Brokers:
                         /\ up[n]
                         /\ synced[n] < Len(log[n])
                         /\ synced' = [synced EXCEPT ![n] = Len(log[n])]
                    /\ UNCHANGED <<up, parted, curEpoch, curLeader, view, caughtUp, leaderHist, log, lease>>
                 \/ /\ \E n \in Brokers:
                         /\ ~up[n]
                         /\ up' = [up EXCEPT ![n] = TRUE]
                         /\ IF ViewRollback /\ view[n].epoch > 0
                               THEN /\ view' = [view EXCEPT ![n] =
                                                  [epoch |-> view[n].epoch - 1, leader |-> view[n].leader]]
                               ELSE /\ TRUE
                                    /\ view' = view
                    /\ UNCHANGED <<parted, curEpoch, curLeader, caughtUp, leaderHist, log, synced, lease>>
                 \/ /\ \E n \in Brokers:
                         /\ ~parted[n]
                         /\ parted' = [parted EXCEPT ![n] = TRUE]
                    /\ UNCHANGED <<up, curEpoch, curLeader, view, caughtUp, leaderHist, log, synced, lease>>
                 \/ /\ \E n \in Brokers:
                         /\ parted[n]
                         /\ parted' = [parted EXCEPT ![n] = FALSE]
                         /\ view' = [view EXCEPT ![n] =
                                       [epoch |-> curEpoch, leader |-> curLeader]]
                    /\ UNCHANGED <<up, curEpoch, curLeader, caughtUp, leaderHist, log, synced, lease>>
              /\ UNCHANGED << committed, consumed >>

Broker(self) == /\ \/ /\ \E q \in Majors:
                           \E j \in q:
                             /\ /\ ~EagerLeader
                                /\ ThinksLeader(self)
                                /\ ~caughtUp[self]
                                /\ ~parted[self]
                                /\ up[j]
                                /\ ~parted[j]
                                /\ SuccessorOf(q, j)
                             /\ log' = [log EXCEPT ![self] = log[j]]
                             /\ caughtUp' = [caughtUp EXCEPT ![self] = TRUE]
                      /\ UNCHANGED <<view, committed, consumed>>
                   \/ /\ \E v \in Values:
                           /\ /\ Serving(self)
                              /\ Len(log[self]) < MaxLogLen
                           /\ log' = [log EXCEPT ![self] =
                                        Append(log[self], <<view[self].epoch, v>>)]
                      /\ UNCHANGED <<view, caughtUp, committed, consumed>>
                   \/ /\ \E f \in Brokers:
                           /\ /\ Serving(self)
                              /\ f # self
                              /\ up[f]
                              /\ ~parted[self]
                              /\ ~parted[f]
                              /\ log[f] # log[self]
                              /\ (\/ view[f].epoch < view[self].epoch
                                  \/ /\ view[f].epoch = view[self].epoch
                                     /\ view[f].leader = self)
                           /\ log' = [log EXCEPT ![f] = log[self]]
                           /\ view' = [view EXCEPT ![f] =
                                         [epoch |-> view[self].epoch, leader |-> self]]
                      /\ UNCHANGED <<caughtUp, committed, consumed>>
                   \/ /\ \E q \in Majors:
                           \E L \in Len(committed)+1..Len(log[self]):
                             /\ /\ Serving(self)
                                /\ IsValsPrefix(committed, log[self])
                                /\ \A r \in q :
                                     /\ L <= synced[r]
                                     /\ SameVals(log[r], log[self], L)
                                     /\ (\/ ~CommitChecksEpoch
                                         \/ /\ view[r].epoch = view[self].epoch
                                            /\ view[r].leader = self)
                             /\ committed' = [i \in 1..L |-> log[self][i][2]]
                      /\ UNCHANGED <<view, caughtUp, log, consumed>>
                   \/ /\ \E r \in Brokers:
                           /\ /\ curLeader = r
                              /\ view[r].leader = r
                              /\ view[r].epoch = curEpoch
                              /\ caughtUp[r]
                              /\ Len(consumed) < Len(log[r])
                              /\ Len(consumed) < Len(committed)
                           /\ consumed' = Append(consumed, log[r][Len(consumed)+1][2])
                      /\ UNCHANGED <<view, caughtUp, log, committed>>
                /\ UNCHANGED << up, parted, curEpoch, curLeader, leaderHist, 
                                synced, lease >>

Next == Controller
           \/ (\E self \in Brokers: Broker(self))

Spec == Init /\ [][Next]_vars

\* END TRANSLATION 
\* 
=============================================================================
