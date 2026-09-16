------------------------------- MODULE TransactionCommit -------------------------------
(***************************************************************************)
(* TransactionCommit —— 事务协议：协调器状态机 × 分区 marker 应用 ×        *)
(* 消费可见性（ADR-18 §11，T-M3.2 块 d 规格化）                            *)
(*                                                                         *)
(* 模型对象（单事务 ID、2 分区、按 epoch 递进的会话）：                     *)
(*  - InitProducerId：epoch bump（TV2 每事务 init bump；新会话重置分区态）  *)
(*  - Begin        ：Empty→Ongoing，TxnLog 落 Begin（durable）              *)
(*  - DataAppend(p)：事务数据批追加（开事务锚，hw+1）；终态后拒绝           *)
(*  - ZombieAppend(p)：终态 marker 后的僵尸续写——名义被终态 fence 拒绝     *)
(*    （FenceEnabled=FALSE 突变体放行 → txnOpen 复活，阴性对照②）          *)
(*  - Prepare(oc)  ：两段第一段——TxnLog 先落 Prepare 并 fsync（ADR-18 §5   *)
(*    lost-writes 落盘点）；PrepareDurable=FALSE 突变体跳过落盘             *)
(*    （阴性对照①）                                                        *)
(*  - Marker(p,oc) ：分区 marker 落盘（commit 可见 / abort 隐藏；关事务锚， *)
(*    LSO 释放）；marker="none" 守卫 = MarkerOnce                          *)
(*  - Complete     ：全部 marker 落定 → TxnLog 落 Complete → 终态           *)
(*  - Takeover     ：协调器崩溃重启——内存相位 := TxnLog 折叠（重驱语义：   *)
(*    Prepare{C} 折叠回 Prepared 后 Marker 续发即幂等补发）                 *)
(*  - OrphanedAbort：Ongoing 的强制 abort（超时 sweep / 接管 Orphaned——    *)
(*    安全方向：marker="none" 的分区落 ABORT 隐藏；对已有 commit marker     *)
(*    的分区置 everLost——名义下不可达（commit marker ⇒ durable Prepare     *)
(*    ⇒ 接管折叠为 PrepareC 重驱 COMMIT 永不 abort），突变体①可达          *)
(*                                                                         *)
(* 安全不变式（ADR-18 §11）：                                              *)
(*  - InvNoAbortedVisible   ：abort 应用的分区不得同时 commit 可见          *)
(*    （aborted reads）                                                    *)
(*  - InvMarkerOnce         ：同 (分区,epoch) 的 marker 恰一次效果          *)
(*    （torn transactions）                                                *)
(*  - InvFenceClosed        ：终态 marker 后 txnOpen 不得复活——与分区侧     *)
(*    last_marker fence 同构（zombie fence 直述性质）                      *)
(*  - InvLsoBounded         ：lso ≤ hw                                     *)
(*  - InvCommitEffectDurable：commit marker 已落 ⇒ TxnLog 含持久           *)
(*    Prepare/Complete（lost writes；前件=效果已现，防平凡绿——账本 ㊼）     *)
(*  - InvNoLostCommit       ：commit 可见的分区永不被翻转隐藏（everLost     *)
(*    绝对历史位；突变体①经 Takeover→OrphanedAbort 触发）                  *)
(*                                                                         *)
(* 判别力阴性对照（账本 ㊼ 纪律：两只突变体必须红）：                       *)
(*  ① PrepareDurable=FALSE：无 Prepare 持久化 → InvCommitEffectDurable /   *)
(*    InvNoLostCommit 必须红（Prepare 前崩溃无可见效果，不算数）           *)
(*  ② FenceEnabled=FALSE：无终态 fence → InvFenceClosed 必须红（不能拿     *)
(*    InvNoAbortedVisible 充数——LSO 锚住僵尸数据反而使其恒绿）             *)
(*                                                                         *)
(* 有限化：epoch ≤ MaxEpoch；每会话每分区 ≤ MaxAppends 批；hw ≤ MaxOffsets。 *)
(***************************************************************************)
EXTENDS Integers

CONSTANTS Parts,          \* 分区集合（模型取 {p1, p2}）
          MaxAppends,     \* 每会话每分区事务批上界（模型取 2）
          MaxEpoch,       \* epoch 上界（模型取 2）
          MaxOffsets,     \* hw 绝对上界（模型取 4）
          FenceEnabled,   \* TRUE=名义（终态 fence）；FALSE=突变体②（僵尸放行）
          PrepareDurable  \* TRUE=名义（Prepare 落 TxnLog）；FALSE=突变体①

VARIABLES
  epoch,            \* 当前生产者会话（0..MaxEpoch）
  phase,            \* 协调器内存相位："Empty"/"Ongoing"/"Prepared"/"Completed"
  oc,               \* 已决定的 outcome："none"/"commit"/"abort"（Prepared 起）
  logPhase,         \* TxnLog 折叠（durable）："None"/"Begin"/"PrepareC"/
                    \* "PrepareA"/"CompleteC"/"CompleteA"
  marker,           \* [p |-> "none"/"commit"/"abort"]：本会话 marker 应用态
  txnOpen,          \* [p |-> BOOL]：开事务锚（LSO 锚定来源）
  appended,         \* [p |-> 0..MaxAppends]：本会话事务批计数
  committedVisible, \* [p |-> BOOL]：commit marker 已应用（数据可见）
  abortedApplied,   \* [p |-> BOOL]：abort marker 已应用（数据隐藏）
  hw,               \* 高水位（0..MaxOffsets）
  lso,              \* 最后稳定 offset（0..MaxOffsets；marker 落定释放到 hw）
  everLost          \* 绝对历史位：commit 可见后被翻转为隐藏（lost write）

Vars == <<epoch, phase, oc, logPhase, marker, txnOpen, appended,
          committedVisible, abortedApplied, hw, lso, everLost>>

none == "none"
commitOc == "commit"
abortOc == "abort"

terminal(p) == marker[p] # none

Init ==
  /\ epoch = 0
  /\ phase = "Empty"
  /\ oc = none
  /\ logPhase = "None"
  /\ marker = [p \in Parts |-> none]
  /\ txnOpen = [p \in Parts |-> FALSE]
  /\ appended = [p \in Parts |-> 0]
  /\ committedVisible = [p \in Parts |-> FALSE]
  /\ abortedApplied = [p \in Parts |-> FALSE]
  /\ hw = 0
  /\ lso = 0
  /\ everLost = FALSE

(* 新会话：epoch bump + 分区事务态重置（durable TxnLog 是历史，保留） *)
InitProducerId ==
  /\ epoch < MaxEpoch
  /\ epoch' = epoch + 1
  /\ phase' = "Empty"
  /\ oc' = none
  /\ marker' = [p \in Parts |-> none]
  /\ txnOpen' = [p \in Parts |-> FALSE]
  /\ appended' = [p \in Parts |-> 0]
  /\ committedVisible' = [p \in Parts |-> FALSE]
  /\ abortedApplied' = [p \in Parts |-> FALSE]
  /\ UNCHANGED <<logPhase, hw, lso, everLost>>

(* Empty（或上一事务终态）→ Ongoing：TxnLog 落 Begin *)
Begin ==
  /\ phase = "Empty"
  /\ logPhase \in {"None", "CompleteC", "CompleteA"}
  /\ phase' = "Ongoing"
  /\ logPhase' = "Begin"
  /\ UNCHANGED <<epoch, oc, marker, txnOpen, appended,
                 committedVisible, abortedApplied, hw, lso, everLost>>

(* 事务数据批：开事务锚 + hw 推进；终态后拒绝（终态 fence） *)
DataAppend(p) ==
  /\ phase = "Ongoing"
  /\ ~terminal(p)
  /\ appended[p] < MaxAppends
  /\ hw < MaxOffsets
  /\ txnOpen' = [txnOpen EXCEPT ![p] = TRUE]
  /\ appended' = [appended EXCEPT ![p] = appended[p] + 1]
  /\ hw' = hw + 1
  /\ UNCHANGED <<epoch, phase, oc, logPhase, marker,
                 committedVisible, abortedApplied, lso, everLost>>

(* 僵尸续写：终态 marker 已落、事务已了——名义实现被分区侧 last_marker
   fence 拒绝（ADR-18 §4.1 终态 fence）；FenceEnabled=FALSE 突变体放行，
   txnOpen 复活（无 marker 可再关 → LSO 永锚，InvFenceClosed 检出） *)
ZombieAppend(p) ==
  /\ ~FenceEnabled
  /\ terminal(p)
  /\ phase # "Ongoing"
  /\ appended[p] < MaxAppends
  /\ hw < MaxOffsets
  /\ txnOpen' = [txnOpen EXCEPT ![p] = TRUE]
  /\ appended' = [appended EXCEPT ![p] = appended[p] + 1]
  /\ hw' = hw + 1
  /\ UNCHANGED <<epoch, phase, oc, logPhase, marker,
                 committedVisible, abortedApplied, lso, everLost>>

(* 两段第一段：Prepare 落 TxnLog（fsync 语义）；突变体①跳过落盘
   （logPhase 停在 Begin——「无 Prepare 持久化」缺陷形态） *)
Prepare(ocd) ==
  /\ phase = "Ongoing"
  /\ phase' = "Prepared"
  /\ oc' = ocd
  /\ logPhase' = IF PrepareDurable
                 THEN IF ocd = commitOc THEN "PrepareC" ELSE "PrepareA"
                 ELSE "Begin"
  /\ UNCHANGED <<epoch, marker, txnOpen, appended,
                 committedVisible, abortedApplied, hw, lso, everLost>>

(* 分区 marker 落盘：恰一次（none 守卫）；关事务锚；LSO 释放到 hw *)
Marker(p, ocd) ==
  /\ phase = "Prepared"
  /\ oc = ocd
  /\ marker[p] = none
  /\ marker' = [marker EXCEPT ![p] = ocd]
  /\ txnOpen' = [txnOpen EXCEPT ![p] = FALSE]
  /\ committedVisible' =
       [committedVisible EXCEPT ![p] = committedVisible[p] \/ ocd = commitOc]
  /\ abortedApplied' =
       [abortedApplied EXCEPT ![p] = abortedApplied[p] \/ ocd = abortOc]
  /\ lso' = hw
  /\ UNCHANGED <<epoch, phase, oc, logPhase, appended, hw, everLost>>

(* 两段第二段：全部 marker 落定 → TxnLog 落 Complete → 终态 *)
Complete ==
  /\ phase = "Prepared"
  /\ \A p \in Parts : marker[p] # none
  /\ phase' = "Completed"
  /\ logPhase' = IF oc = commitOc THEN "CompleteC" ELSE "CompleteA"
  /\ UNCHANGED <<epoch, oc, marker, txnOpen, appended,
                 committedVisible, abortedApplied, hw, lso, everLost>>

(* TxnLog 折叠出的内存相位（接管判定 = ADR-18 §6 纯函数的模型面） *)
Derived(phaseL) ==
  CASE phaseL = "None" -> "Empty"
      [] phaseL = "Begin" -> "Ongoing"
      [] phaseL = "PrepareC" -> "Prepared"
      [] phaseL = "PrepareA" -> "Prepared"
      [] phaseL \in {"CompleteC", "CompleteA"} -> "Completed"

(* 协调器崩溃重启：内存相位 := TxnLog 折叠。Prepared 折叠后 Marker 续发
   即重驱补发（分区侧幂等）；Ongoing 折叠（Begin 残留）→ OrphanedAbort *)
Takeover ==
  /\ Derived(logPhase) # phase
  /\ phase' = Derived(logPhase)
  /\ oc' = CASE logPhase = "PrepareC" -> commitOc
              [] logPhase = "PrepareA" -> abortOc
              [] OTHER -> oc
  /\ UNCHANGED <<epoch, logPhase, marker, txnOpen, appended,
                 committedVisible, abortedApplied, hw, lso, everLost>>

(* 强制 abort（超时 sweep / 接管 Orphaned，安全方向）：无 marker 分区落
   ABORT 隐藏；**对已 commit marker 的分区置 everLost**——名义下该分区集
   恒空（commit marker ⇒ durable Prepare ⇒ 接管折叠 PrepareC 重驱 COMMIT，
   本动作只在 Ongoing/Begin 残留可达），突变体①下可达 = lost write *)
OrphanedAbort ==
  /\ phase = "Ongoing"
  /\ phase' = "Completed"
  /\ oc' = abortOc
  /\ logPhase' = "CompleteA"
  /\ marker' = [p \in Parts |-> IF marker[p] = commitOc THEN commitOc ELSE abortOc]
  /\ txnOpen' = [p \in Parts |-> FALSE]
  /\ abortedApplied' = [p \in Parts |-> abortedApplied[p] \/ marker[p] # commitOc]
  /\ everLost' = everLost \/ (\E p \in Parts : marker[p] = commitOc)
  /\ UNCHANGED <<epoch, appended, committedVisible, hw, lso>>

Next ==
  \/ InitProducerId
  \/ Begin
  \/ \E p \in Parts : DataAppend(p)
  \/ \E p \in Parts : ZombieAppend(p)
  \/ \E ocd \in {commitOc, abortOc} : Prepare(ocd)
  \/ \E p \in Parts, ocd \in {commitOc, abortOc} : Marker(p, ocd)
  \/ Complete
  \/ Takeover
  \/ OrphanedAbort

Spec == Init /\ [][Next]_Vars

(* ---------- 安全不变式（ADR-18 §11） ---------- *)

InvTypeOK ==
  /\ epoch \in 0..MaxEpoch
  /\ phase \in {"Empty", "Ongoing", "Prepared", "Completed"}
  /\ oc \in {none, commitOc, abortOc}
  /\ logPhase \in {"None", "Begin", "PrepareC", "PrepareA", "CompleteC", "CompleteA"}
  /\ hw \in 0..MaxOffsets
  /\ lso \in 0..MaxOffsets

(* aborted reads：abort 已应用 ⇒ 该分区不处于 commit 可见 *)
InvNoAbortedVisible ==
  \A p \in Parts : ~abortedApplied[p] \/ ~committedVisible[p]

(* torn transactions：同 (分区, 会话) 的 marker 恰一次效果——commit 可见的
   分区永不被 abort 触及（abortedApplied 一旦置位即互斥） *)
InvMarkerOnce ==
  \A p \in Parts : marker[p] = commitOc => ~abortedApplied[p]

(* zombie fence 直述性质：终态 marker 后开事务锚不得复活 *)
InvFenceClosed ==
  \A p \in Parts : marker[p] # none => ~txnOpen[p]

InvLsoBounded == lso <= hw

(* lost writes：commit 效果已现 ⇒ TxnLog 含持久 Prepare/Complete
   （前件=效果已现——「Prepare 落盘前崩溃无可见效果」不算数，账本 ㊼
   平凡绿教训的反向应用） *)
InvCommitEffectDurable ==
  \A p \in Parts : marker[p] = commitOc =>
      logPhase \in {"PrepareC", "PrepareA", "CompleteC", "CompleteA"}

InvNoLostCommit == ~everLost

=============================================
