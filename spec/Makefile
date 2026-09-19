# Basalt 数据面规约验证（docs/VERIFICATION.md C1-C4/C9）
# 依赖：JDK 11+；tla2tools.jar 放本目录或用 TLATOOLS 指定路径。
#   wget -P . https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar
TLATOOLS ?= $(CURDIR)/tools/tla2tools.jar
TLC = java -Xmx8g -cp $(TLATOOLS) tlc2.TLC -workers 8 -deadlock -cleanup

check:            ; $(TLC) -config check.cfg BasaltDataPlane.tla
demo-eager:       ; $(TLC) -config demo-eager.cfg BasaltDataPlane.tla
splitbrain:       ; $(TLC) -config splitbrain.cfg BasaltDataPlane.tla
splitbrain-cepoch:; $(TLC) -config splitbrain-cepoch.cfg BasaltDataPlane.tla

.PHONY: check demo-eager splitbrain splitbrain-cepoch

consumer-group:        ; $(TLC) -config consumer-group.cfg ConsumerGroup.tla
consumer-group-demo:   ; $(TLC) -config consumer-group-demo.cfg ConsumerGroup.tla
consumer-group-liveness: ; $(TLC) -config consumer-group-liveness.cfg ConsumerGroup.tla

# 活性二分实验（C9 收敛性，CONSUMERGROUP-LIVENESS-WIP.md）：
#   cg-liveness-l0/l2/l3 全部期望红——已证明的负结果（四级公平性均不可满足）
cg-liveness-l0: ; $(TLC) -config consumer-group-l0.cfg ConsumerGroupLiveness.tla
cg-liveness-l2: ; $(TLC) -config consumer-group-l2.cfg ConsumerGroupLiveness.tla
cg-liveness-l3: ; $(TLC) -config consumer-group-l3.cfg ConsumerGroupLiveness.tla

# 提交协议（账本 ㉟，T-M2.2）：名义全空间绿；无 reconciliation 突变体必须红
replication-commit:         ; $(TLC) -config replication-commit.cfg ReplicationCommit.tla
replication-commit-liveness:      ; $(TLC) -deadlock -config replication-commit-liveness.cfg ReplicationCommit.tla
replication-commit-liveness-2f:   ; $(TLC) -deadlock -config replication-commit-liveness-2f.cfg ReplicationCommit.tla
replication-commit-recovery:      ; $(TLC) -deadlock -config replication-commit-recovery.cfg ReplicationCommit.tla
# 阴性对照（判别力已恢复 2026-09-16：全 UNCHANGED 集修复后故障路径可达）：
# 无 reconciliation 突变体必须被 InvLeaderServingHasAcked 检出
replication-commit-norecon: ; $(TLC) -config replication-commit-norecon.cfg ReplicationCommit.tla

# 事务协议（ADR-18 §11，T-M3.2 块 d）：名义全空间绿（1,088 态）；
# 两只判别力阴性对照必须红——①无 Prepare 持久化（InvCommitEffectDurable，
# lost writes）②无终态 fence（InvFenceClosed，僵尸续写复活开事务）
transaction-commit:           ; $(TLC) -config transaction-commit.cfg TransactionCommit.tla
transaction-commit-noprepare: ; $(TLC) -config transaction-commit-noprepare.cfg TransactionCommit.tla
transaction-commit-nofence:   ; $(TLC) -config transaction-commit-nofence.cfg TransactionCommit.tla
.PHONY: transaction-commit transaction-commit-noprepare transaction-commit-nofence

# KIP-848 新消费组协议（ADR-19 §6d，T-M3.3 块 d）：名义全空间绿；
# 两只判别力阴性对照必须红——①无心跳 member-epoch fence（突变常数
# HeartbeatFence=FALSE，InvHeartbeatFencedMon 抓）②无提交成员级双令牌
# （CommitFence=FALSE，InvCommitFencedMon 抓）。㊼ 纪律：突变常数与
# ADR-19 §6d / 账本三方一致
consumer-group-848:               ; $(TLC) -config consumer-group-848.cfg ConsumerGroup848.tla
consumer-group-848-nohbfence:     ; $(TLC) -config consumer-group-848-nohbfence.cfg ConsumerGroup848.tla
consumer-group-848-nocommitfence: ; $(TLC) -config consumer-group-848-nocommitfence.cfg ConsumerGroup848.tla

# share group ack 状态机（KIP-932 块 e，B5）：名义全空间绿；
# phantom 突变（允许未交付区间 Accept）必须被 InvCursorDelivered 抓——
# 游标跳过未交付记录 = 消费者丢数据面（share-ack.tla 头注释三方一致）
share-ack:         ; $(TLC) -config share-ack.cfg ShareAck.tla
share-ack-phantom: ; $(TLC) -config share-ack-phantom.cfg ShareAck.tla

# 数据面活性（P2-2 闭合）：稳定变体期望绿；churn 变体期望红（负结果已入账）
dp-liveness-stable: ; $(TLC) -deadlock -config dp-liveness-stable.cfg BasaltDataPlaneLiveness.tla
dp-liveness:         ; $(TLC) -deadlock -config dp-liveness.cfg BasaltDataPlaneLiveness.tla

.PHONY: dp-liveness-stable dp-liveness
.PHONY: share-ack share-ack-phantom
.PHONY: consumer-group-848 consumer-group-848-nohbfence consumer-group-848-nocommitfence
.PHONY: replication-commit replication-commit-norecon replication-commit-liveness replication-commit-recovery

.PHONY: consumer-group consumer-group-demo consumer-group-liveness
.PHONY: cg-liveness-l0 cg-liveness-l2 cg-liveness-l3
group-state-ha:      ; $(TLC) -config group-state-ha.cfg GroupStateHA.tla
