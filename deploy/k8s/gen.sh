#!/usr/bin/env bash
# 生成 3 个独立 Deployment + Service（微环境 POC 形态；生产建议 StatefulSet + 动态 PV）
set -e
for i in 0 1 2; do
cat <<YAML
apiVersion: v1
kind: Service
metadata:
  name: basalt-svc-$i
  labels:
    app: basalt
    node: "basalt-$i"
spec:
  type: ClusterIP
  selector:
    app: basalt
    node: "basalt-$i"
  ports:
    - name: client
      port: 9092
      targetPort: 9092
    - name: internal
      port: 9093
      targetPort: 9093
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: basalt-$i
spec:
  replicas: 1
  strategy: { type: Recreate }
  selector:
    matchLabels:
      app: basalt
      node: "basalt-$i"
  template:
    metadata:
      labels:
        app: basalt
        node: "basalt-$i"
    spec:
      terminationGracePeriodSeconds: 0
      containers:
        - name: basalt
          image: basalt-server:dev
          imagePullPolicy: IfNotPresent
          ports:
            - containerPort: 9092
            - containerPort: 9093
          env:
            - name: BASALT_NODE_ID
              value: "$i"
            - name: BASALT_HOST
              value: "basalt-$i.basalt-headless.default.svc.cluster.local"
            - name: BASALT_NODES
              value: "0=basalt-0.basalt-headless.default.svc.cluster.local:9092,1=basalt-1.basalt-headless.default.svc.cluster.local:9092,2=basalt-2.basalt-headless.default.svc.cluster.local:9092"
            - name: BASALT_RF
              value: "3"
            - name: BASALT_NUM_PARTITIONS
              value: "2"
            - name: BASALT_DATA_DIR
              value: /data
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          hostPath:
            path: /var/basalt-k8s-data-$i
            type: DirectoryOrCreate
YAML
  [ $i -lt 2 ] && echo "---"
done
