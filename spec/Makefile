# Basalt 数据面规约验证（docs/VERIFICATION.md C1-C4）
# 依赖：JDK 11+；tla2tools.jar 放本目录或用 TLATOOLS 指定路径。
#   wget -P . https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar
TLATOOLS ?= tla2tools.jar
TLC = java -Xmx8g -cp $(TLATOOLS) tlc2.TLC -workers 8 -deadlock -cleanup

check:            ; $(TLC) -config check.cfg BasaltDataPlane.tla
demo-eager:       ; $(TLC) -config demo-eager.cfg BasaltDataPlane.tla
splitbrain:       ; $(TLC) -config splitbrain.cfg BasaltDataPlane.tla
splitbrain-cepoch:; $(TLC) -config splitbrain-cepoch.cfg BasaltDataPlane.tla

.PHONY: check demo-eager splitbrain splitbrain-cepoch

consumer-group:        ; $(TLC) -config consumer-group.cfg ConsumerGroup.tla
consumer-group-demo:   ; $(TLC) -config consumer-group-demo.cfg ConsumerGroup.tla

.PHONY: consumer-group consumer-group-demo
