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

# 数据面活性（P2-2 闭合）：稳定变体期望绿；churn 变体期望红（负结果已入账）
dp-liveness-stable: ; $(TLC) -deadlock -config dp-liveness-stable.cfg BasaltDataPlaneLiveness.tla
dp-liveness:         ; $(TLC) -deadlock -config dp-liveness.cfg BasaltDataPlaneLiveness.tla

.PHONY: dp-liveness-stable dp-liveness

.PHONY: consumer-group consumer-group-demo consumer-group-liveness
.PHONY: cg-liveness-l0 cg-liveness-l2 cg-liveness-l3
