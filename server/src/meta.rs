//! 元数据 actor（集群驱动，多节点 POC）：
//! - 集群真源在控制器（ClusterState）；broker 经 MetaSync 拿快照；
//! - MetaService 应用快照：为本地副本 spawn 分区 actor、SetRole、发布路由；
//! - 自动建题转发控制器（本机是控制器则直调），等待版本推进后应答。

use crate::config::Config;
use crate::partition::{PartitionActor, PartitionCmd};
use basalt_metadata::cluster::{BrokerInfo, ClusterState};
use basalt_metadata::TopicMeta;
use basalt_storage::log::{FsyncSchedule, LogOptions};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};
use tokio::sync::{mpsc, oneshot, watch};

#[derive(Clone)]
pub struct Route {
    pub tx: mpsc::Sender<PartitionCmd>,
    pub leader: i32,
    #[allow(dead_code)]
    pub epoch: i32,
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
        reply: oneshot::Sender<bool>,
    },
}

pub struct MetaService {
    cfg: Config,
    /// 控制器内部地址（host:internal_port），非控制器节点经它转发建题
    controller_addr: Option<String>,
    cluster: ClusterState,
    /// 已停掉拉取任务的分区（failover 成为 leader 后）
    #[allow(dead_code)]
    stop_pulls: HashSet<(String, i32)>,
    /// 每分区当前拉取目标地址（leader 变更检测）
    pull_addrs: HashMap<(String, i32), String>,
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
    pub fn spawn(
        cfg: Config,
        controller_addr: Option<String>,
        #[allow(dead_code)]
    controller_tx: Option<mpsc::Sender<crate::internal::ControllerCmd>>,
    ) -> (mpsc::Sender<MetaCmd>, watch::Receiver<RoutingTable>) {
        let (tx, rx) = mpsc::channel(256);
        let (tw, tr) = watch::channel(RoutingTable::default());
        let svc = MetaService {
            cfg,
            controller_addr,
            cluster: ClusterState::default(),
            stop_pulls: std::collections::HashSet::new(),
            pull_addrs: HashMap::new(),
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
                                    if self.create_via_controller(n, self.cfg.num_partitions, self.cfg.default_rf).await {
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
                MetaCmd::CreateTopic { name, partitions, rf, reply } => {
                    let ok = self.create_via_controller(&name, partitions, rf).await;
                    let _ = reply.send(ok);
                }
            }
        }
    }

    async fn create_via_controller(&mut self, name: &str, partitions: i32, rf: i32) -> bool {
        // 统一走控制器 RPC（控制器节点对自己的内部端口回环亦可），
        // 创建后立即拉最新快照内联应用——绝不能在本 actor 内等待自己的 ApplyCluster（自死锁）
        let Some(addr) = self.controller_addr.clone() else { return false };
        let client = crate::internal::InternalClient::new(addr);
        if client.create_topic(name, partitions, rf).await.is_err() {
            return false;
        }
        match client.meta_sync(self.cluster.version).await {
            Ok(resp) if resp.len() >= 1 && resp[0] == 1 => {
                if let Some(state) = ClusterState::decode(&resp[1..]) {
                    self.apply_cluster(Box::new(state)).await;
                }
            }
            _ => {}
        }
        topic_meta_from_cluster(&self.cluster, name).is_some()
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
                };
                match PartitionActor::spawn(a.topic.clone(), a.partition, self.cfg.node_id, dir, opts, self.cfg.replica_config()) {
                    Ok(tx) => {
                        entry.1.insert(a.partition);
                        let route = Route { tx: tx.clone(), leader: a.leader, epoch: a.epoch };
                        self.routes.by_name.insert((a.topic.clone(), a.partition), route.clone());
                        self.routes.by_id.insert((topic_id_from(&a.topic), a.partition), route);
                        let _ = tx
                            .send(PartitionCmd::SetRole {
                                leader: a.leader == self.cfg.node_id,
                                epoch: a.epoch,
                                replicas: a.replicas.clone(),
                            })
                            .await;
                        // follower：启动拉取任务（leader 变更时由 ApplyCluster 停旧起新）
                        if a.leader != self.cfg.node_id {
                            if let Some(leader_info) = self.cluster.brokers.get(&a.leader) {
                                let leader_addr = format!("{}:{}", leader_info.host, leader_info.port + 1);
                                clear_pull_stop(&a.topic, a.partition);
                                let pull = FollowerPull {
                                    topic: a.topic.clone(),
                                    partition: a.partition,
                                    node_id: self.cfg.node_id,
                                    leader: a.leader,
                                    leader_addr,
                                    local_tx: tx,
                                };
                                tokio::spawn(pull.run());
                            }
                        }
                    }
                    Err(e) => tracing::error!(topic=%a.topic, partition=a.partition, error=%e, "spawn failed"),
                }
            } else if let Some(route) = self.routes.by_name.get_mut(&(a.topic.clone(), a.partition)) {
                // 已有 actor：仅更新角色/路由
                let was_leader = route.leader == self.cfg.node_id;
                let is_leader = a.leader == self.cfg.node_id;
                route.leader = a.leader;
                route.epoch = a.epoch;
                let _ = route
                    .tx
                    .send(PartitionCmd::SetRole {
                        leader: is_leader,
                        epoch: a.epoch,
                        replicas: a.replicas.clone(),
                    })
                    .await;
                if is_leader && !was_leader {
                    // failover：本副本从 follower 变为 leader → 停掉拉取任务
                    set_pull_stop(&a.topic, a.partition);
                } else if !is_leader && was_leader {
                    // 原 leader 失去 leadership → 必须起新拉取任务（指向新 leader）
                    if let Some(leader_info) = self.cluster.brokers.get(&a.leader) {
                        let leader_addr = format!("{}:{}", leader_info.host, leader_info.port + 1);
                        clear_pull_stop(&a.topic, a.partition);
                        let pull = FollowerPull {
                            topic: a.topic.clone(),
                            partition: a.partition,
                            node_id: self.cfg.node_id,
                            leader: a.leader,
                            leader_addr,
                            local_tx: route.tx.clone(),
                        };
                        tokio::spawn(pull.run());
                    }
                } else if !is_leader {
                    // follower 不变但 leader 换了人 → 停旧起新（指向新 leader 地址）
                    if let Some(leader_info) = self.cluster.brokers.get(&a.leader) {
                        let new_addr = format!("{}:{}", leader_info.host, leader_info.port + 1);
                        if self.pull_addrs.get(&(a.topic.clone(), a.partition)).map(|old| old != &new_addr).unwrap_or(true) {
                            set_pull_stop(&a.topic, a.partition);
                            clear_pull_stop(&a.topic, a.partition);
                            let pull = FollowerPull {
                                topic: a.topic.clone(),
                                partition: a.partition,
                                node_id: self.cfg.node_id,
                                leader: a.leader,
                                leader_addr: new_addr.clone(),
                                local_tx: route.tx.clone(),
                            };
                            tokio::spawn(pull.run());
                        }
                    }
                }
                self.pull_addrs.insert((a.topic.clone(), a.partition), format!("{}:{}", {
                    let info = self.cluster.brokers.get(&a.leader);
                    match info { Some(b) => format!("{}:{}", b.host, b.port), None => String::new() }
                }, a.leader));
            }
        }
        let _ = self.tx_watch.send(self.routes.clone());
    }
}

// follower 拉取任务的停止标志（failover 时由 MetaService 置位）
fn pull_stops() -> &'static Mutex<HashSet<(String, i32)>> {
    static S: OnceLock<Mutex<HashSet<(String, i32)>>> = OnceLock::new();
    S.get_or_init(|| Mutex::new(HashSet::new()))
}

pub fn set_pull_stop(topic: &str, partition: i32) {
    pull_stops().lock().unwrap().insert((topic.to_string(), partition));
}

pub fn clear_pull_stop(topic: &str, partition: i32) {
    pull_stops().lock().unwrap().remove(&(topic.to_string(), partition));
}

pub fn pull_stop_requested(topic: &str, partition: i32) -> bool {
    pull_stops().lock().unwrap().contains(&(topic.to_string(), partition))
}

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
}

impl FollowerPull {
    async fn run(self) {
        let client = crate::internal::InternalClient::new(self.leader_addr.clone());
        // 启动 LEO：从本地 actor 查询真实日志末尾（重启/带数据重启场景不能从 0 开始）
        let Some(mut next_offset) = self.local_leo().await else {
            return;
        };
        tracing::info!(topic=%self.topic, partition=self.partition, leader=self.leader, start=next_offset, "follower pull started");
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
                                if out.error.is_none() {
                                    next_offset = out.last_offset + 1;
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
                        // leader 无新数据：等 leader HW 或下一个间隔
                        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
                    }
                }
                Err(e) => {
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
        crate::meta::pull_stop_requested(&self.topic, self.partition)
    }
}
