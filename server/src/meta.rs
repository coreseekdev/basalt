//! 元数据 actor（集群驱动，多节点 POC）：
//! - 集群真源在控制器（ClusterState）；broker 经 MetaSync 拿快照；
//! - MetaService 应用快照：为本地副本 spawn 分区 actor、SetRole、发布路由；
//! - 自动建题转发控制器（本机是控制器则直调），等待版本推进后应答。

use crate::config::Config;
use crate::partition::{PartitionActor, PartitionCmd};
use basalt_metadata::cluster::{BrokerInfo, ClusterState};
use basalt_metadata::TopicMeta;
use basalt_storage::pool::BufferPool;
use basalt_storage::log::{FsyncSchedule, LogOptions};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::{mpsc, oneshot, watch};

#[derive(Clone)]
pub struct Route {
    pub tx: mpsc::Sender<PartitionCmd>,
    pub leader: i32,
    #[allow(dead_code)]
    pub epoch: i32,
    /// 副本集（就任拉齐的副本间 FetchSlice 互信依据）
    pub replicas: Vec<i32>,
}

#[derive(Clone, Default)]
pub struct RoutingTable {
    by_name: HashMap<(String, i32), Route>,
    by_id: HashMap<(u128, i32), Route>,
}

impl RoutingTable {
    pub fn find(&self, topic: &str, partition: i32) -> Option<&Route> {
        self.by_name.get(&(topic.to_string(), partition))
    }

    pub fn find_by_id(&self, id: u128, partition: i32) -> Option<&Route> {
        self.by_id.get(&(id, partition))
    }

    /// TopicId → topic 名反查（KIP-848 块 b：心跳请求的 TopicPartitions 只带
    /// TopicId，组状态机以名字为键）。topic id 是名字哈希（topic_id_from），
    /// 故由 by_name 键派生反查——零新增状态面，免三表同步失联（㊽ 族教训）；
    /// 心跳频率低（5s 档）+ POC topic 量级小，线性扫描可接受。
    pub fn name_for(&self, id: u128) -> Option<String> {
        self.by_name
            .keys()
            .find(|(n, _)| topic_id_from(n) == id)
            .map(|(n, _)| n.to_string())
    }

    pub fn iter_all_topics(&self) -> Vec<String> {
        self.by_name
            .keys()
            .map(|(n, _)| n.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    }
}

#[allow(dead_code)]
pub enum MetaCmd {
    /// 控制器快照到达：应用集群态（spawn/角色/路由）。
    ApplyCluster(Box<ClusterState>),
    /// 查 topic（names=None 全量）；allow_create 时转发控制器。
    /// 同时返回全量 broker 列表（客户端路由必需）。
    Lookup {
        names: Option<Vec<String>>,
        allow_create: bool,
        reply: oneshot::Sender<(Vec<TopicMeta>, Vec<BrokerInfo>)>,
    },
    /// 建题（转发控制器并等待本地版本推进）。
    CreateTopic {
        name: String,
        partitions: i32,
        rf: i32,
        tiered: bool,
        reply: oneshot::Sender<bool>,
    },
    /// CreateTopics 语义：缺则按请求的 partitions/rf 建，存则查回。
    EnsureTopic {
        name: String,
        partitions: i32,
        rf: i32,
        /// per-topic 分层存储（CreateTopics configs: basalt.storage.mode）
        tiered: bool,
        reply: oneshot::Sender<(Vec<TopicMeta>, Vec<BrokerInfo>)>,
    },
    /// 删题（转发控制器，raft 复制后本地 assignment 消失）。
    DeleteTopic {
        name: String,
        reply: oneshot::Sender<bool>,
    },
    /// 查节点客户端地址（marker 路由器跨节点下发用；内部端口 = +1）。
    BrokerAddr {
        node: i32,
        reply: oneshot::Sender<Option<(String, u16)>>,
    },
}

pub struct MetaService {
    // BufferPool 进程单例共享资源池：Arc 表达资源共享而非共享可变所有权（ADR-13）。
    #[allow(clippy::disallowed_types)]
    pub pool: std::sync::Arc<BufferPool>,
    cfg: Config,
    /// 控制器内部地址（host:internal_port），非控制器节点经它转发建题
    controller_addr: Option<String>,
    cluster: ClusterState,
    /// 活跃拉取任务的停止信号 + 当前目标地址
    active_pulls: HashMap<(String, i32), tokio::sync::watch::Sender<bool>>,
    #[allow(dead_code)]
    active_pull_addrs: HashMap<(String, i32), String>,
    /// topic name -> (topic_id, 本地已 spawn 的分区集合)
    local: HashMap<String, (u128, std::collections::HashSet<i32>)>,
    routes: RoutingTable,
    tx_watch: watch::Sender<RoutingTable>,
    rx: mpsc::Receiver<MetaCmd>,
    #[allow(dead_code)]
    controller_tx: Option<mpsc::Sender<crate::internal::ControllerCmd>>,
}

fn topic_meta_from_cluster(cluster: &ClusterState, name: &str) -> Option<TopicMeta> {
    let parts: Vec<_> = cluster
        .assignments
        .iter()
        .filter(|a| a.topic == name)
        .collect();
    if parts.is_empty() {
        return None;
    }
    let topic_id = topic_id_from(name);
    let partitions = parts
        .iter()
        .map(|a| basalt_metadata::PartitionMeta {
            index: a.partition,
            leader: a.leader,
            leader_epoch: a.epoch,
            replicas: a.replicas.clone(),
            isr: a.replicas.clone(),
        })
        .collect();
    Some(TopicMeta { name: name.to_string(), topic_id, internal: false, partitions })
}

/// 与集群态无关的稳定 topic id（POC：名字哈希；删除重建刷新由控制器保证——
/// 重建经 CreateTopic 记录，名字相同也会因哈希盐一致而相同 → POC 已知限制，
/// 完整方案（M2）：topic id 存控制器 record）。
fn topic_id_from(name: &str) -> u128 {
    let mut h: u128 = 0x9E3779B97F4A7C15;
    for b in name.bytes() {
        h ^= u128::from(b);
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

impl MetaService {
    #[allow(clippy::disallowed_types)] // pool 参数：进程单例共享资源池（ADR-13 豁免）
    pub fn spawn(
        cfg: Config,
        controller_addr: Option<String>,
        #[allow(dead_code)]
    controller_tx: Option<mpsc::Sender<crate::internal::ControllerCmd>>,
        pool: std::sync::Arc<BufferPool>,
    ) -> (mpsc::Sender<MetaCmd>, watch::Receiver<RoutingTable>) {
        let (tx, rx) = mpsc::channel(256);
        let (tw, tr) = watch::channel(RoutingTable::default());
        let svc = MetaService {
            cfg: cfg.clone(),
            pool,
            controller_addr,
            cluster: ClusterState::default(),
            active_pulls: HashMap::new(),
            active_pull_addrs: HashMap::new(),
            local: HashMap::new(),
            routes: RoutingTable::default(),
            tx_watch: tw,
            rx,
            controller_tx,
        };
        tokio::spawn(svc.run());
        (tx, tr)
    }

    async fn run(mut self) {
        while let Some(cmd) = self.rx.recv().await {
            match cmd {
                MetaCmd::ApplyCluster(state) => self.apply_cluster(state).await,
                MetaCmd::BrokerAddr { node, reply } => {
                    let r = self.cluster.brokers.get(&node).map(|b| (b.host.clone(), b.port));
                    let _ = reply.send(r);
                }
                MetaCmd::DeleteTopic { name, reply } => {
                    if let Some(tx) = &self.controller_tx {
                        let (txr, rxr) = tokio::sync::oneshot::channel();
                        let ok = tx
                            .send(crate::internal::ControllerCmd::DeleteTopic {
                                name: name.clone(),
                                reply: txr,
                            })
                            .await
                            .is_ok()
                                && rxr.await.unwrap_or(false);
                        // 控制器已删 → 刷新本地簇视图（否则 post-lookup 仍见
                        // 旧 assignment，删除被误报为 UNKNOWN——Java 档实证）
                        if ok {
                            self.refresh_cluster_snapshot().await;
                        }
                        let _ = reply.send(ok);
                    } else {
                        // 无控制器（单点自治理）：本地集群态即权威——
                        // 移除 assignment 与路由后即达成删除语义
                        self.cluster
                            .assignments
                            .retain(|a| a.topic != *name);
                        let live: std::collections::BTreeSet<(String, i32)> = self
                            .cluster
                            .assignments
                            .iter()
                            .map(|a| (a.topic.clone(), a.partition))
                            .collect();
                        self.routes.by_name.retain(|k, _| live.contains(k));
                        let live_ids: std::collections::BTreeSet<(u128, i32)> = self
                            .cluster
                            .assignments
                            .iter()
                            .map(|a| (topic_id_from(&a.topic), a.partition))
                            .collect();
                        self.routes.by_id.retain(|k, _| live_ids.contains(k));
                        let _ = reply.send(true);
                    }
                }
                MetaCmd::Lookup { names, allow_create, reply } => {
                    let mut out = Vec::new();
                    match &names {
                        None => {
                            let topics: std::collections::BTreeSet<String> = self
                                .cluster
                                .assignments
                                .iter()
                                .map(|a| a.topic.clone())
                                .collect();
                            for t in topics {
                                if let Some(m) = topic_meta_from_cluster(&self.cluster, &t) {
                                    out.push(m);
                                }
                            }
                        }
                        Some(req_names) => {
                            for n in req_names {
                                if let Some(m) = topic_meta_from_cluster(&self.cluster, n) {
                                    out.push(m);
                                } else if allow_create {
                                    if self.create_via_controller(n, self.cfg.num_partitions, self.cfg.default_rf, false).await {
                                        if let Some(m) = topic_meta_from_cluster(&self.cluster, n) {
                                            out.push(m);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    let brokers: Vec<BrokerInfo> = self.cluster.brokers.values().cloned().collect();
                    let _ = reply.send((out, brokers));
                }
                MetaCmd::CreateTopic { name, partitions, rf, tiered, reply } => {
                    let ok = self.create_via_controller(&name, partitions, rf, tiered).await;
                    let _ = reply.send(ok);
                }
                MetaCmd::EnsureTopic { name, partitions, rf, tiered, reply } => {
                    // CreateTopics 语义：请求的 NumPartitions/RF 是**建题参数**
                    // 而非查询——缺失时按请求值建（≠ Lookup allow_create 的
                    // broker 默认值；账本 59：请求值被默认值覆盖）。
                    let mut out = Vec::new();
                    if let Some(m) = topic_meta_from_cluster(&self.cluster, &name) {
                        out.push(m);
                    } else if self.create_via_controller(&name, partitions, rf, tiered).await {
                        if let Some(m) = topic_meta_from_cluster(&self.cluster, &name) {
                            out.push(m);
                        }
                    }
                    let brokers: Vec<BrokerInfo> = self.cluster.brokers.values().cloned().collect();
                    let _ = reply.send((out, brokers));
                }
            }
        }
    }

    /// 停止指定分区的拉取任务。
    fn stop_pull(&mut self, topic: &str, partition: i32) {
        let key = (topic.to_string(), partition);
        if let Some(tx) = self.active_pulls.remove(&key) {
            let _ = tx.send(true); // 信号：停止
            tracing::info!(topic=%topic, partition=%partition, "pull task stopped");
        }
    }

    /// 启动拉取任务（带 watch 停止信号）。
    fn spawn_pull(&mut self, topic: &str, partition: i32, leader: i32, leader_addr: &str, local_tx: &mpsc::Sender<PartitionCmd>) {
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let pull = FollowerPull {
            topic: topic.to_string(),
            partition,
            node_id: self.cfg.node_id,
            leader,
            leader_addr: leader_addr.to_string(),
            local_tx: local_tx.clone(),
            stop_rx,
        };
        self.active_pulls.insert((topic.to_string(), partition), stop_tx);
        tokio::spawn(pull.run());
    }

    /// 就任拉齐（账本 ㉟ reconciliation）：升主前探测副本集内各存活副本的
    /// LEO，若有人比本地长则拉回缺失尾部并 ReconcileAppend 落盘。
    /// 有界等待（每副本 800ms），全部失败则按现状直接就任（降级 = 旧行为）。
    async fn reconcile_leader_tail(
        &self,
        topic: &str,
        partition: i32,
        replicas: &[i32],
        tx: &mpsc::Sender<PartitionCmd>,
    ) {
        // 本地 LEO
        let (lx, lrx) = tokio::sync::oneshot::channel();
        if tx.send(PartitionCmd::LocalLeo { reply: lx }).await.is_err() {
            return;
        }
        let my_leo = lrx.await.unwrap_or(0);
        // 探测各副本：取 leader_next 最大且 > 本地的响应
        let mut best: Option<(i64, bytes::Bytes)> = None;
        for r in replicas.iter().filter(|r| **r != self.cfg.node_id) {
            let Some(info) = self.cluster.brokers.get(r) else { continue };
            let addr = format!("{}:{}", info.host, info.port + 1);
            let client = crate::internal::InternalClient::new(addr);
            let fut = client.fetch_slice(topic, partition, self.cfg.node_id, my_leo, 8 * 1024 * 1024);
            if let Ok(Ok(resp)) = tokio::time::timeout(
                std::time::Duration::from_millis(800),
                fut,
            ).await {
                if resp.error == 0 {
                    let next = resp.leader_next_offset as i64;
                    if next > my_leo && best.as_ref().map(|(n, _)| next > *n).unwrap_or(true) {
                        best = Some((next, resp.data));
                    }
                }
            }
        }
        if let Some((_, blob)) = best {
            if !blob.is_empty() {
                tracing::info!(topic=%topic, partition=%partition, bytes=blob.len(), "leader reconcile: pulling missed tail");
                let (ax, arx) = tokio::sync::oneshot::channel();
                if tx.send(PartitionCmd::ReconcileAppend { batches: blob, reply: ax }).await.is_ok() {
                    match arx.await {
                        Ok(true) => {}
                        _ => tracing::warn!(topic=%topic, partition=%partition, "leader reconcile append failed"),
                    }
                }
            }
        }
    }

    async fn create_via_controller(&mut self, name: &str, partitions: i32, rf: i32, tiered: bool) -> bool {
        // 创建：本机持有控制器 actor → 直调；否则 RPC 到控制器内部端口
        if let Some(tx) = &self.controller_tx {
            let (txr, rxr) = oneshot::channel();
            let _ = tx
                .send(crate::internal::ControllerCmd::CreateTopic {
                    name: name.to_string(),
                    partitions,
                    rf,
                    tiered,
                    reply: txr,
                })
                .await;
            if rxr.await.is_err() {
                return false;
            }
        } else {
            let Some(addr) = self.controller_addr.clone() else { return false };
            let client = crate::internal::InternalClient::new(addr);
            if client.create_topic(name, partitions, rf, tiered).await.is_err() {
                return false;
            }
        }
        // 创建后立即拉最新快照内联应用——绝不能在本 actor 内等待自己的 ApplyCluster（自死锁）
        self.refresh_cluster_snapshot().await;
        topic_meta_from_cluster(&self.cluster, name).is_some()
    }

    /// 从控制器拉取最新快照并应用（控制器节点走 controller_tx， follower 走 RPC）。
    async fn refresh_cluster_snapshot(&mut self) {
        if let Some(tx) = &self.controller_tx {
            let (txr, rxr) = oneshot::channel();
            let _ = tx
                .send(crate::internal::ControllerCmd::Sync { version: 0, reply: txr })
                .await;
            if let Ok(Some(snap)) = rxr.await {
                if let Some(state) = ClusterState::decode(&snap) {
                    self.apply_cluster(Box::new(state)).await;
                }
            }
            return;
        }
        let Some(addr) = self.controller_addr.clone() else { return };
        let client = crate::internal::InternalClient::new(addr);
        if let Ok(resp) = client.meta_sync(0).await {
            if resp.len() >= 1 && resp[0] == 1 {
                if let Some(state) = ClusterState::decode(&resp[1..]) {
                    self.apply_cluster(Box::new(state)).await;
                }
            }
        }
    }

    async fn apply_cluster(&mut self, state: Box<ClusterState>) {
        self.cluster = *state;
        // 为本地副本确保 actor；角色更新
        for a in self.cluster.assignments.clone() {
            let is_local = a.replicas.contains(&self.cfg.node_id);
            if !is_local {
                continue;
            }
            let entry = self.local.entry(a.topic.clone()).or_insert_with(|| (topic_id_from(&a.topic), std::collections::HashSet::new()));
            if !entry.1.contains(&a.partition) {
                let dir = PathBuf::from(self.cfg.data_dir.clone())
                    .join(&a.topic)
                    .join(format!("p{}", a.partition));
                let opts = LogOptions {
                    segment_max_bytes: self.cfg.segment_max_bytes,
                    fsync: FsyncSchedule::Os,
                    retention_ms: 7 * 24 * 3600 * 1000,
                    retention_max_bytes: 0,
                };
                match PartitionActor::spawn(a.topic.clone(), a.partition, self.cfg.node_id, dir, opts, self.cfg.replica_config(), self.pool.clone(), a.tiered) {
                    Ok(tx) => {
                        entry.1.insert(a.partition);
                        let route = Route { tx: tx.clone(), leader: a.leader, epoch: a.epoch, replicas: a.replicas.clone() };
                        self.routes.by_name.insert((a.topic.clone(), a.partition), route.clone());
                        self.routes.by_id.insert((topic_id_from(&a.topic), a.partition), route);
                        let _ = tx
                            .send(PartitionCmd::SetRole {
                                leader: a.leader == self.cfg.node_id,
                                epoch: a.epoch,
                                replicas: a.replicas.clone(),
                            })
                            .await;
                        // follower：启动拉取任务
                        if a.leader != self.cfg.node_id {
                            if let Some(leader_info) = self.cluster.brokers.get(&a.leader) {
                                let leader_addr = format!("{}:{}", leader_info.host, leader_info.port + 1);
                                self.spawn_pull(&a.topic, a.partition, a.leader, &leader_addr, &tx);
                            }
                        }
                    }
                    Err(e) => tracing::error!(topic=%a.topic, partition=a.partition, error=%e, "spawn failed"),
                }
            } else if let Some(route) = self.routes.by_name.get_mut(&(a.topic.clone(), a.partition)) {
                // 提取 route 信息（限制 borrow 范围）
                let route_epoch_before = route.epoch;
                let (_was_leader, tx_clone) = {
                    route.leader = a.leader;
                    route.epoch = a.epoch;
                    (route.leader == self.cfg.node_id, route.tx.clone())
                };
                let is_leader = a.leader == self.cfg.node_id;
                // 就任拉齐（账本 ㉟ reconciliation）：升主前从存活副本拉回
                // 本地缺失的尾部——任何副本被选为新 leader 都能补齐后再服务，
                // 「豁免掉队成员后来当 leader 丢已 ack 消息」的窗口由此关闭
                if is_leader && a.epoch > route_epoch_before {
                    self.reconcile_leader_tail(&a.topic, a.partition, &a.replicas, &tx_clone).await;
                }
                // drop route borrow 后再 await
                let _ = tx_clone
                    .send(PartitionCmd::SetRole {
                        leader: is_leader,
                        epoch: a.epoch,
                        replicas: a.replicas.clone(),
                    })
                    .await;
                // 拉取任务管理
                let key = (a.topic.clone(), a.partition);
                if is_leader {
                    self.stop_pull(&a.topic, a.partition);
                } else if let Some(leader_info) = self.cluster.brokers.get(&a.leader) {
                    let new_addr = format!("{}:{}", leader_info.host, leader_info.port + 1);
                    if self.active_pull_addrs.get(&key).map(|old| old != &new_addr).unwrap_or(true) {
                        self.stop_pull(&a.topic, a.partition);
                        self.spawn_pull(&a.topic, a.partition, a.leader, &new_addr, &tx_clone);
                    }
                }
                self.active_pull_addrs.insert(key, if is_leader { String::new() } else {
                    self.cluster.brokers.get(&a.leader)
                        .map(|b| format!("{}:{}", b.host, b.port + 1))
                        .unwrap_or_default()
                });
            }
        }
        // 已删除的 topic（DeleteTopic 后 assignment 消失）：路由移除。
        // actor 孤儿为 POC 边界——数据由 retention 清理，路由语义立即
        // 正确（metadata UNKNOWN_TOPIC、produce 走建题路径）
        let live: std::collections::BTreeSet<(String, i32)> = self
            .cluster
            .assignments
            .iter()
            .map(|a| (a.topic.clone(), a.partition))
            .collect();
        self.routes.by_name.retain(|k, _| live.contains(k));
        let live_ids: std::collections::BTreeSet<(u128, i32)> = self
            .cluster
            .assignments
            .iter()
            .map(|a| (topic_id_from(&a.topic), a.partition))
            .collect();
        self.routes.by_id.retain(|k, _| live_ids.contains(k));
        // 删除 topic 的孤儿 actor 关停（DeleteTopic 落地的最后一口气）：
        // 路由已移除，actor 不可达但句柄/内存仍在——Shutdown 使其退出
        let dead_topics: Vec<String> = self
            .local
            .keys()
            .filter(|t| !self.cluster.assignments.iter().any(|a| &a.topic == *t))
            .cloned()
            .collect();
        for t in dead_topics {
            if let Some((_, parts)) = self.local.remove(&t) {
                for p in parts {
                    if let Some(route) = self.routes.by_name.remove(&(t.clone(), p)) {
                        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
                        let _ = route.tx.send(PartitionCmd::Shutdown { reply: done_tx }).await;
                        let _ = done_rx.await;
                    }
                }
            }
        }
        let _ = self.tx_watch.send(self.routes.clone());
    }
}

// follower 拉取任务的停止标志（failover 时由 MetaService 置位）




/// 启动期恢复：数据目录里已有的 topic 在控制器未登记时补登记（幂等）。
#[allow(dead_code)]
pub async fn recover_existing(cfg: &Config, meta_tx: &mpsc::Sender<MetaCmd>) {
    let Ok(entries) = std::fs::read_dir(&cfg.data_dir) else { return };
    for e in entries.flatten() {
        if !e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let topic = e.file_name().to_string_lossy().into_owned();
        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = meta_tx
            .send(MetaCmd::Lookup { names: Some(vec![topic]), allow_create: true, reply: reply_tx })
            .await;
        let _ = reply_rx.await;
    }
}


/// follower 拉取循环：从 leader 内部端口持续拉切片，Absolute 追加本地日志。
/// 拉取请求本身即 LEO 上报（leader 侧据此推进 HW）。
struct FollowerPull {
    topic: String,
    partition: i32,
    node_id: i32,
    leader: i32,
    leader_addr: String,
    local_tx: mpsc::Sender<PartitionCmd>,
    stop_rx: tokio::sync::watch::Receiver<bool>,
}

impl FollowerPull {
    async fn run(self) {
        // 分区注入：leader 在断边集内则不拉取（追赶场景的注入点）
        if crate::internal::blocked_peers().contains(&self.leader) {
            tracing::warn!(topic=%self.topic, leader=self.leader, "follower pull: leader blocked (partition injection)");
            return;
        }
        let client = crate::internal::InternalClient::new(self.leader_addr.clone());
        // 启动 LEO：从本地 actor 查询真实日志末尾（重启/带数据重启场景不能从 0 开始）
        let Some(mut next_offset) = self.local_leo().await else {
            tracing::error!(topic=%self.topic, "follower pull: local_leo failed");
            return;
        };
        tracing::info!(topic=%self.topic, partition=self.partition, leader=self.leader, start=next_offset, "follower pull started");
        eprintln!("PULL-START t={} p={} addr={}", self.topic, self.partition, self.leader_addr);
        loop {
            if self.stop_requested() {
                tracing::info!(topic=%self.topic, partition=self.partition, "follower pull stopped");
                return;
            }
            match client
                .fetch_slice(&self.topic, self.partition, self.node_id, next_offset, 8 << 20)
                .await
            {
                Ok(res) => {
                    if res.error != 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                        continue;
                    }
                    if res.leader_next_offset < next_offset {
                        // leader 数据短于本地：分叉尾巴 → 截断到 leader 末尾后重拉
                        tracing::warn!(
                            topic=%self.topic, partition=self.partition,
                            leader_next=res.leader_next_offset, local=next_offset,
                            "divergent tail: truncating to leader"
                        );
                        if self.truncate_to(res.leader_next_offset).await.is_none() {
                            return;
                        }
                        next_offset = res.leader_next_offset;
                        continue;
                    }
                    eprintln!("PULL t={} p={} off={} got={}B hw={} lnext={}", self.topic, self.partition, next_offset, res.data.len(), res.high_watermark, res.leader_next_offset);
                    if !res.data.is_empty() {
                        let (tx, rx) = oneshot::channel();
                        if self
                            .local_tx
                            .send(PartitionCmd::Produce {
                                batches: res.data.clone(),
                                policy: basalt_storage::log::AssignPolicy::Absolute,
                                acks: 1,
                                reply: tx,
                            })
                            .await
                            .is_err()
                        {
                            return; // actor 关闭（关机/角色变更重建）
                        }
                        match rx.await {
                            Ok(out) => {
                                eprintln!("PULL-ACK t={} p={} err={:?} last={}", self.topic, self.partition, out.error.is_some(), out.last_offset);
                                if out.error.is_none() {
                                    next_offset = out.last_offset + 1;
                                    // T-M2.5 in-flight 重复投递注入：同一切片重放一次。
                                    // 绝对偏移策略下重放 = base 不匹配被拒（replica gap
                                    // 路径）或 0 新记录——offset 纪律保证幂等。
                                    if std::env::var("BASALT_PULL_REPLAY").ok().as_deref() == Some("1") {
                                        let (tx2, rx2) = oneshot::channel();
                                        if self.local_tx.send(PartitionCmd::Produce {
                                            batches: res.data.clone(),
                                            policy: basalt_storage::log::AssignPolicy::Absolute,
                                            acks: 1,
                                            reply: tx2,
                                        }).await.is_ok() {
                                            let _ = rx2.await; // 拒绝/空均可——幂等由 offset 纪律保证
                                        }
                                    }
                                } else if let Some(basalt_storage::error::StorageError::Other(m)) = &out.error {
                                    if m.contains("replica gap") {
                                        // gap：本地 LEO 与 leader 错位——以本地真实 LEO 重试
                                        next_offset = self.local_leo().await.unwrap_or(next_offset);
                                    }
                                }
                            }
                            Err(_) => {}
                        }
                    } else {
                        // leader 无新数据：长轮询（FetchSlice 250ms 兜底）已
                        // 在 leader 侧挂起——立即重新发起，数据到达即刻响应
                    }
                }
                Err(e) => {
                    eprintln!("PULL-ERR t={} p={} {}", self.topic, self.partition, e);
                    tracing::debug!(topic=%self.topic, error=%e, "pull failed, retrying");
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            }
        }
    }

    async fn local_leo(&self) -> Option<i64> {
        let (tx, rx) = oneshot::channel();
        self.local_tx.send(PartitionCmd::LocalLeo { reply: tx }).await.ok()?;
        rx.await.ok()
    }

    async fn truncate_to(&self, offset: i64) -> Option<()> {
        let (tx, rx) = oneshot::channel();
        self.local_tx.send(PartitionCmd::TruncateTo { offset, reply: tx }).await.ok()?;
        rx.await.ok().map(|_| ())
    }

    fn stop_requested(&self) -> bool {
        // 由 MetaService 在 failover 时置位；POC 经全局注册表轮询
        *self.stop_rx.borrow()
    }
}


// ===================== 协议 handler 布局回归（永久机制，五轮 review 沉淀）====
#[cfg(test)]
#[path = "handlers_layout_tests.rs"]
mod handlers_layout_tests;
