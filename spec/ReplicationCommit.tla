------------------------------- MODULE ReplicationCommit -------------------------------
(***************************************************************************)
(* ReplicationCommit v1 —— 数据面提交协议：HW / ISR / 冻结提交面 /         *)
(* leader reconciliation 四者一致性（账本 ㉟，ADR-17 后记，T-M2.2 首项）    *)
(*                                                                         *)
(* 模型对象（3 节点：初始主 L + 两 follower）：                            *)
(*  - Produce   ：leader 追加一条目（next+1），记录冻结提交面 = 当期 ISR    *)
(*  - Pull(f)   ：follower 拉取并 truncate-to-match（leo' = leader 日志末） *)
(*                + 上报（age 清零；完全追平 → 重回 ISR，账本 ㉟ 重入规则）  *)
(*  - Tick      ：环境时间步（follower 上报年龄增长，饱和于 LagTicks+1）    *)
(*  - Shrink(f) ：ISR 显式收缩（age ≥ LagTicks 的落后者移出——此前缺陷      *)
(*                形态是"fresh-only min"让它从 HW 计算里静默消失，㉟）      *)
(*  - HWAdvance ：hw' = min(next, min_{f∈ISR} leo[f])（单调前进）           *)
(*  - Ack(o)    ：o < hw 且冻结面全员（leo > o 或 stale 豁免）→ acked      *)
(*  - CrashL / Elect / Reconcile：主崩溃 → 任意存活 follower 当选 →        *)
(*    **就任拉齐**：nextL' = max(自身, 存活副本 leo)（并集语义——            *)
(*    Reconciliation=FALSE 的突变体退化为不拉齐，用于阴性对照）             *)
(*                                                                         *)
(* 核心安全不变式：                                                        *)
(*  - InvAckedSurvivable：已 ack 条目恒存在于某存活副本（k-39 类丢失的     *)
(*    规格化——Reconciliation=FALSE 时 TLC 必须检出反例，判别力门禁）       *)
(*  - InvLeaderServingHasAcked：服务中的 leader 持有全部已 ack 条目         *)
(*  - InvHWBounded：hw ≤ next ∧ hw 单调结构性满足（动作只前移）            *)
(*                                                                         *)
(* 有限化：日志 = 前缀序列 ⇒ "副本持有条目 o" ≡ leo > o；                  *)
(* age 饱和；日志长度 ≤ MaxEntries=2。                                     *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS Followers,      \* follower 集合（模型取 {f1, f2}）
          MaxEntries,     \* 日志长度上界（模型取 2）
          LagTicks,       \* ISR 收缩阈值（tick 数，模型取 1）
          Reconciliation, \* TRUE=名义；FALSE=突变体（不拉齐就任）
          CrashEnabled,   \* TRUE=环境可崩溃初始主（failover 恢复活性实验）
          FrozenFace      \* TRUE=名义（冻结提交面放行）；FALSE=突变体（HW-only，
                          \* 即 ㉟ 原始缺陷：fresh-shrink 使 HW 越过 laggard）

Nodes == Followers \* 可当选集合（初始主 L 崩溃后不复活——有界视界）

MinOf(S) == CHOOSE m \in S : \A x \in S : m <= x
MaxOf(S) == CHOOSE m \in S : \A x \in S : m >= x
MinOf2(a, b) == IF a <= b THEN a ELSE b
MaxOf2(a, b) == IF a >= b THEN a ELSE b

VARIABLES
  \* leader 侧
  leader,     \* 当前主 ∈ Nodes
  nextL,      \* 主日志末（0..MaxEntries）
  hw,         \* 高水位（0..MaxEntries）
  serving,    \* 主可服务（就任后须 reconcile 才 TRUE）
  crashedL,   \* 初始主已崩溃
  elected,    \* 崩溃后是否已选举（每纪元至多一次——生产：控制器一次指派）
  epoch,      \* 主纪元（0..1）
  \* follower 侧
  leo,        \* [f ∈ Followers |-> 日志末 0..MaxEntries]
  age,        \* [f ∈ Followers |-> leader 侧上报年龄 0..LagTicks+1]
  isr,        \* ⊆ Followers（follower ISR，不含 leader）
  face,       \* [o ∈ 0..MaxEntries-1 |-> 冻结提交面 ⊆ Followers]
  acked       \* ⊆ 0..MaxEntries-1

vars == <<leader, nextL, hw, serving, crashedL, elected, epoch,
          leo, age, isr, face, acked>>

AllAlive == {leader} \cup Followers   \* 初始主崩溃后 leader ∈ Nodes

LogEnd(n) == IF n = leader THEN nextL ELSE leo[n]

TypeOK ==
  /\ leader \in Nodes
  /\ nextL \in 0..MaxEntries
  /\ hw \in 0..MaxEntries
  /\ serving \in BOOLEAN
  /\ crashedL \in BOOLEAN
  /\ elected \in BOOLEAN
  /\ epoch \in 0..1
  /\ leo \in [Followers -> 0..MaxEntries]
  /\ age \in [Followers -> 0..LagTicks+1]
  /\ isr \subseteq Followers
  /\ face \in [0..MaxEntries-1 -> SUBSET Followers]
  /\ acked \subseteq 0..MaxEntries-1

Init ==
  /\ leader = "L0"
  /\ nextL = 0
  /\ hw = 0
  /\ serving = TRUE
  /\ crashedL = FALSE
  /\ elected = FALSE
  /\ epoch = 0
  /\ leo = [f \in Followers |-> 0]
  /\ age = [f \in Followers |-> 0]
  /\ isr = {}
  /\ face = [o \in 0..MaxEntries-1 |-> {}]
  /\ acked = {}

IsLeader(n) == leader = n
Alive(n) == ~crashedL \/ n # "L0"      \* 只有初始主可崩

(* ── 主侧动作 ─────────────────────────────────────────────────────────── *)

Produce ==
  /\ serving
  /\ nextL < MaxEntries
  /\ nextL' = nextL + 1
  /\ face' = [face EXCEPT ![nextL] = isr]     \* 冻结提交面 = 当期 ISR
  /\ UNCHANGED <<leader, hw, serving, crashedL, elected, epoch, leo, age, isr, acked>>

HWAdvance ==
  /\ isr # {}
  /\ hw < MinOf({nextL} \cup {leo[f] : f \in isr})
  /\ hw' = MinOf({nextL} \cup {leo[f] : f \in isr})
  /\ UNCHANGED <<leader, nextL, serving, crashedL, elected, epoch, leo, age, isr, face, acked>>

Ack(o) ==
  /\ o \in 0..nextL-1
  /\ o \notin acked
  /\ o < hw
  /\ ~FrozenFace \/ \A f \in face[o] : leo[f] > o \/ age[f] >= LagTicks+1
  /\ acked' = acked \cup {o}
  /\ UNCHANGED <<leader, nextL, hw, serving, crashedL, elected, epoch, leo, age, isr, face>>

(* ── follower 侧动作 ──────────────────────────────────────────────────── *)

Pull(f) ==
  /\ f # leader
  /\ serving                        \* 当前主存活且可服务（换主后以新主为准）
  \* 使能：有数据可拉（含截断对齐）或已追平但不在 ISR（上报即重回——
  \* 实现同款：FetchSlice 请求恒发，长轮询挂起亦携带 LEO 上报）
  /\ leo[f] # nextL \/ f \notin isr
  /\ leo' = [leo EXCEPT ![f] = nextL]   \* fetch + truncate-to-match
  /\ age' = [age EXCEPT ![f] = 0]
  /\ isr' = isr \cup {f}                 \* 追平上报 ⇒ 重回 ISR
  \* 上报驱动 HW 推进（实现同款：advance_hw 随每次上报调用）——
  \* HW 不是独立可被无限绕过的动作，活性不依赖调度偏好
  /\ hw' = MaxOf2(hw, MinOf2(nextL, MinOf({leo'[g] : g \in isr'})))
  /\ UNCHANGED <<leader, nextL, serving, crashedL, elected, epoch, face, acked>>

Tick ==
  /\ \E f \in Followers : age[f] < LagTicks + 1   \* 饱和后禁用：公平性下不让 Tick 独占
  /\ age' = [f \in Followers |-> MinOf2(age[f] + 1, LagTicks + 1)]
  /\ UNCHANGED <<leader, nextL, hw, serving, crashedL, elected, epoch, leo, isr, face, acked>>

Shrink(f) ==
  /\ f \in isr
  /\ age[f] >= LagTicks            \* 上报过期：显式收缩（非静默消失）
  /\ isr' = isr \ {f}
  /\ UNCHANGED <<leader, nextL, hw, serving, crashedL, elected, epoch, leo, age, face, acked>>

(* ── failover：崩溃 → 选举 → 就任拉齐 → 服务 ──────────────────────────── *)

CrashL ==
  /\ CrashEnabled
  /\ ~crashedL
  /\ crashedL' = TRUE
  /\ serving' = FALSE              \* 主死即不可服务
  /\ UNCHANGED <<leader, nextL, hw, elected, epoch, leo, age, isr, face, acked>>

Elect(f) ==
  /\ crashedL
  /\ ~elected                       \* 每次故障至多一次选举（防选举风暴）
  /\ f \in Followers
  /\ leader' = f
  /\ epoch' = MinOf2(epoch + 1, 1)
  /\ nextL' = leo[f]               \* 先以自身日志就任（尚未服务）
  /\ hw' = 0                        \* HW 是 per-leader 状态：换主即重算
  /\ elected' = TRUE
  /\ serving' = FALSE
  /\ UNCHANGED <<crashedL, leo, age, isr, face, acked>>

Reconcile ==
  /\ ~serving
  /\ leader \in Followers          \* 新主就任拉齐
  /\ nextL' = IF Reconciliation
                THEN MaxOf({nextL} \cup {leo[f] : f \in Followers})
                ELSE nextL          \* 突变体：不拉齐（阴性对照）
  /\ hw' = nextL'                   \* 服务水位 = 拉齐后的日志末（实现同款：
                                     \* SetRole leader 即 HW=next_offset）
  /\ serving' = TRUE
  /\ UNCHANGED <<leader, crashedL, elected, epoch, leo, age, isr, face, acked>>

Next ==
  \/ Produce
  \/ HWAdvance
  \/ \E o \in 0..nextL-1 : Ack(o)
  \/ \E f \in Followers : Pull(f) \/ Shrink(f)
  \/ Tick
  \/ CrashL
  \/ \E f \in Followers : Elect(f)
  \/ Reconcile

(* ── 不变式 ───────────────────────────────────────────────────────────── *)


(* 主安全性质：已 ack 条目恒存在于某存活副本（k-39 类丢失的规格化）。
   "存活副本持有 o" ≡ LogEnd(存活副本) > o。 *)
InvAckedSurvivable ==
  \A o \in acked :
    \E n \in AllAlive : LogEnd(n) > o

(* 服务中的主必须持有全部已 ack 条目（reconciliation 的直接契约） *)
InvLeaderServingHasAcked ==
  serving => \A o \in acked : nextL > o

(* HW 不越过主日志末 *)
InvHWBounded == hw <= nextL

(* 诊断探针：crash 后仍有已 ack 条目（可被落后的当选者丢失的状态存在性） *)
ProbeAckedAfterCrash == ~ (acked # {} /\ crashedL /\ leader # "L0")
ProbeAckedNonEmpty == ~(acked # {})
ProbeHWAdvanced == ~(hw > 0)
ProbeISRNonEmpty == ~(isr # {})

Spec == Init /\ [][Next]_vars

(* ── 活性（T-M2.2/㉟ 规格化下半场）──────────────────────────────────────
   公平性假设：produce/拉取/HW 推进/ack 各自动作弱公平（持续可用则最终
   发生）；Reconcile 强公平（持续可用则最终发生——failover 恢复不空转）。
   CrashEnabled=FALSE 的名义活性：产出最终全部被 ack（ack 管线无活锁）。
   CrashEnabled=TRUE 的恢复活性：崩溃 ⇒ 最终恢复服务（reconciliation
   不空转）。*)

(* 🚧 WIP（2026-09-16）：名义活性（EventuallyAllAcked）反例未解。
   反例形态：Produce×2 → Tick×2（age 饱和）→ 永久 stutter——follower 从不
   pull、ack 从不发生。该 stutter 尾部中 Pull 持续 ENABLED 却从不发生，
   WF/SF 均应排除之（已试 WF/ disjunctive/ SF 三种公平性形态 + 单
   follower 最小模型，均复现）。怀疑点：TLC tableau 与「\E 动作上的
   SF」/量化 WF 合取的交互，或活性性质需要条件化（稳定环境假设）。
   下一步：fresh/ISR 双阈值分离建模 + 条件化活性重述（稳定 pull 环境
   ⇒ ack 推进），参考 BasaltDataPlaneLiveness 的 FairDP 结构。 *)
(* 逐 follower WF(Pull)：每个存活副本的 pull 循环持续运行（实现现实——
   pull 循环永不退出）。f1 永不拉取的行为违反 WF(Pull(f1)) 而被排除——
   冻结提交面等待其追平是系统诚实性，不是活锁。 *)
FairSpec ==
  /\ Spec
  /\ WF_vars(Produce)
  /\ WF_vars(HWAdvance)
  /\ \A f \in Followers : WF_vars(Pull(f))
  /\ \A o \in 0..MaxEntries-1 : WF_vars(Ack(o))

EventuallyAllAcked == \A o \in 0..MaxEntries-1 : <>(o \in acked)

FairSpecCrash ==
  /\ FairSpec
  /\ \A f \in Followers : WF_vars(Elect(f))   \* 控制器公平选举假设
  /\ SF_vars(Reconcile)       \* 就任拉齐不空转

FailoverRecovers == (crashedL /\ leader \in Followers) ~> serving

================================================================================
