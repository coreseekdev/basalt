#!/usr/bin/env bash
# k8s 故障注入脚本：对 basalt 集群做 kill -9 / 网络分区注入
# 用法: chaos.sh kill-pod <pod>  |  chaos.sh netem <pod> <loss%>  |  chaos.sh recover <pod>
set -eu
K="sudo -n microk8s kubectl"
case "${1:-}" in
  kill-pod)
    POD="$2"
    echo ">> kill -9 等价：force delete $POD（StatefulSet/Deployment 秒级重建）"
    $K delete pod "$POD" --force --grace-period=0
    ;;
  netem)
    POD="$2"; LOSS="${3:-100}"
    echo ">> $POD 注入 ${LOSS}% 丢包（需 NET_ADMIN）"
    $K exec "$POD" -- tc qdisc add dev eth0 root netem loss "$LOSS%" || \
      echo "（tc 注入失败：容器需 NET_ADMIN capability；在 spec 中加 securityContext）"
    ;;
  recover)
    POD="$2"
    $K exec "$POD" -- tc qdisc del dev eth0 root 2>/dev/null || true
    echo ">> $POD 网络恢复"
    ;;
  cycle)
    # 随机 kill 循环（混沌长跑）：每 20s 杀一个随机 broker，共 5 轮
    for r in 1 2 3 4 5; do
      POD=$(printf "basalt-%d" $((RANDOM % 3)))
      echo "== round $r: killing $POD =="
      $K delete pod "$POD" --force --grace-period=0
      sleep 20
    done
    ;;
  *) echo "usage: $0 {kill-pod <pod> | netem <pod> <loss%> | recover <pod> | cycle}"; exit 1;;
esac
