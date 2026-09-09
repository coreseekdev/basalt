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
          CommitChecksEpoch   \* TRUE = commit 多数派校验成员视图

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

  \* n 自认为主（SplitBrain=TRUE 时旧主在失联后仍自认为主）
  ThinksLeader(n) == /\ up[n]
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

  \* C2a：每个 epoch 至多一个被指派的 leader
  InvOneLeaderPerEpoch ==
    \A e \in 1..MaxEpochs :
      Cardinality({n \in Brokers : <<e, n>> \in leaderHist}) <= 1

  \* 无人知晓"未来"的 epoch
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

  \* C4（游标有界）：消费者只读已提交前缀
  InvConsumedBounded == IsPrefix(consumed, committed)

  \* C4'（端到端形态）：消费者读到的每条都能从任一多数派中恢复
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
        if curLeader = n then
          curLeader := NoLeader
        end if
      end with ;
    or
      \* Restart：view/log 依 leader-epoch checkpoint 假设持久保留
      with n \in Brokers do
        await ~up[n] ;
        up := [up EXCEPT ![n] = TRUE]
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
                   /\ SameVals(log[r], log[self], L)
                   /\ (\/ ~CommitChecksEpoch
                       \/ /\ view[r].epoch = view[self].epoch
                          /\ view[r].leader = self) ;
        committed := [i \in 1..L |-> log[self][i][2]]
      end with ;
    or
      \* Consume：消费已提交前缀的下一项（游标有界）
      await Len(consumed) < Len(committed) ;
      consumed := Append(consumed, committed[Len(consumed)+1])
    end either ;
  end while ;
end process ;

end algorithm ; *)
\* BEGIN TRANSLATION (chksum(pcal) = "132b50a9" /\ chksum(tla) = "34e14889")
VARIABLES up, parted, curEpoch, curLeader, view, caughtUp, leaderHist, log, 
          committed, consumed

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


InvConsumedBounded == IsPrefix(consumed, committed)


InvConsumedOnLeader ==
  \A q \in Majors : \E r \in q : IsPrefix(consumed, Vals(log[r]))


vars == << up, parted, curEpoch, curLeader, view, caughtUp, leaderHist, log, 
           committed, consumed >>

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

Controller == /\ \/ /\ \E n \in Brokers:
                         /\ curEpoch < MaxEpochs
                         /\ curEpoch' = curEpoch + 1
                         /\ curLeader' = n
                         /\ leaderHist' = (leaderHist \cup {<<curEpoch', n>>})
                         /\ caughtUp' = [caughtUp EXCEPT ![n] = EagerLeader]
                         /\ view' = [n2 \in Brokers |->
                                       IF parted[n2] THEN view[n2]
                                       ELSE [epoch |-> curEpoch', leader |-> n]]
                    /\ UNCHANGED <<up, parted>>
                 \/ /\ \E n \in Brokers:
                         /\ up[n]
                         /\ up' = [up EXCEPT ![n] = FALSE]
                         /\ IF curLeader = n
                               THEN /\ curLeader' = NoLeader
                               ELSE /\ TRUE
                                    /\ UNCHANGED curLeader
                    /\ UNCHANGED <<parted, curEpoch, view, caughtUp, leaderHist>>
                 \/ /\ \E n \in Brokers:
                         /\ ~up[n]
                         /\ up' = [up EXCEPT ![n] = TRUE]
                    /\ UNCHANGED <<parted, curEpoch, curLeader, view, caughtUp, leaderHist>>
                 \/ /\ \E n \in Brokers:
                         /\ ~parted[n]
                         /\ parted' = [parted EXCEPT ![n] = TRUE]
                    /\ UNCHANGED <<up, curEpoch, curLeader, view, caughtUp, leaderHist>>
                 \/ /\ \E n \in Brokers:
                         /\ parted[n]
                         /\ parted' = [parted EXCEPT ![n] = FALSE]
                         /\ view' = [view EXCEPT ![n] =
                                       [epoch |-> curEpoch, leader |-> curLeader]]
                    /\ UNCHANGED <<up, curEpoch, curLeader, caughtUp, leaderHist>>
              /\ UNCHANGED << log, committed, consumed >>

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
                                     /\ SameVals(log[r], log[self], L)
                                     /\ (\/ ~CommitChecksEpoch
                                         \/ /\ view[r].epoch = view[self].epoch
                                            /\ view[r].leader = self)
                             /\ committed' = [i \in 1..L |-> log[self][i][2]]
                      /\ UNCHANGED <<view, caughtUp, log, consumed>>
                   \/ /\ Len(consumed) < Len(committed)
                      /\ consumed' = Append(consumed, committed[Len(consumed)+1])
                      /\ UNCHANGED <<view, caughtUp, log, committed>>
                /\ UNCHANGED << up, parted, curEpoch, curLeader, leaderHist >>

Next == Controller
           \/ (\E self \in Brokers: Broker(self))

Spec == Init /\ [][Next]_vars

\* END TRANSLATION 
\* 
=============================================================================
