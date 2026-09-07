# Basalt on k8s（microk8s 实测路线，参考 dendro docs/ops/local-k8s.md）

## 部署
```bash
# 1. 构建镜像并导入 microk8s containerd（无 registry 场景）
docker build -t basalt-server:dev .
docker save basalt-server:dev | sudo microk8s ctr images import -

# 2. 部署
sudo microk8s kubectl apply -f deploy/k8s/basalt-cluster.yaml

# 3. 验证
sudo microk8s kubectl get pods -l app=basalt
```

## 故障注入
```bash
# kill -9 一个 broker（pod 立即重建，验证复制与 failover）
sudo microk8s kubectl delete pod basalt-1 --force --grace=0

# 网络分区（需 NET_ADMIN；对 pod netns 加 100% 丢包）
sudo microk8s kubectl exec basalt-1 -- tc qdisc add dev eth0 root netem loss 100%
sudo microk8s kubectl exec basalt-1 -- tc qdisc del dev eth0 root
```

## 已知注意事项（来自 dendro 实测）
- kind 在嵌套 netns 下起不来；microk8s 组件直跑宿主网络可一次装通
- 默认绑 127.0.0.1 的服务在 pod 里必须 --host 0.0.0.0 / advertised host 用 pod FQDN
- 镜像源受限时：docker save | microk8s ctr images import 绕过 registry
