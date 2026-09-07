//! 元数据 actor：TopicTable 的独占所有者 + 路由表广播（watch）。
//!
//! watch 传播 owned 快照（RoutingTable: Clone），连接侧 `borrow()` 只读查询，
//! `Sender` clone 即路由句柄——所有权图无 Arc（性能纪律）。

use crate::config::Config;
use crate::partition::{PartitionActor, PartitionCmd};
use basalt_metadata::{CreateError, TopicMeta, TopicTable};
use basalt_storage::log::{FsyncSchedule, LogOptions};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::{mpsc, oneshot, watch};

#[derive(Clone)]
pub struct Route {
    #[allow(dead_code)]
    pub name: String,
    #[allow(dead_code)]
    pub topic_id: u128,
    #[allow(dead_code)]
    pub partition: i32,
    #[allow(dead_code)]
    pub leader: i32,
    pub tx: mpsc::Sender<PartitionCmd>,
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
}

pub enum MetaCmd {
    /// 按 names（None = 全量）查 topic；allow_create 时自动建题。
    Lookup {
        names: Option<Vec<String>>,
        allow_create: bool,
        reply: oneshot::Sender<Vec<TopicMeta>>,
    },
}

pub struct MetaService {
    cfg: Config,
    table: TopicTable,
    routes: RoutingTable,
    tx_watch: watch::Sender<RoutingTable>,
    rx: mpsc::Receiver<MetaCmd>,
}

impl MetaService {
    pub fn spawn(cfg: Config) -> (mpsc::Sender<MetaCmd>, watch::Receiver<RoutingTable>) {
        let (tx, rx) = mpsc::channel(256);
        let (tw, tr) = watch::channel(RoutingTable::default());
        let svc = MetaService { cfg, table: TopicTable::new(), routes: RoutingTable::default(), tx_watch: tw, rx };
        tokio::spawn(svc.run());
        (tx, tr)
    }

    async fn run(mut self) {
        while let Some(cmd) = self.rx.recv().await {
            match cmd {
                MetaCmd::Lookup { names, allow_create, reply } => {
                    let mut out = Vec::new();
                    match names {
                        None => {
                            out.extend(self.table.topics().cloned());
                        }
                        Some(names) => {
                            for n in names {
                                if let Some(t) = self.table.get(&n) {
                                    out.push(t.clone());
                                } else if allow_create {
                                    match self.create_and_spawn(&n, self.cfg.num_partitions, self.cfg.default_rf) {
                                        Ok(t) => out.push(t),
                                        Err(_) => {/* 由调用方按 UNKNOWN 处理 */}
                                    }
                                }
                            }
                        }
                    }
                    let _ = reply.send(out);
                }
            }
        }
    }

    fn create_and_spawn(&mut self, name: &str, partitions: i32, rf: i32) -> Result<TopicMeta, CreateError> {
        let brokers = self.cfg.broker_ids();
        let meta = self.table.create(name, partitions, rf, &brokers)?.clone();
        // 本地 actor：单机/POC 阶段所有副本都在本地（rf=1）；多节点阶段只 spawn leader==node_id 的
        for p in &meta.partitions {
            if p.leader != self.cfg.node_id && !self.cfg.nodes.is_empty() {
                continue; // 多节点：非本节点 leader 暂不 spawn（POC2 接管）
            }
            let dir = PathBuf::from(self.cfg.data_dir.clone())
                .join(name)
                .join(format!("p{}", p.index));
            let opts = LogOptions {
                segment_max_bytes: self.cfg.segment_max_bytes,
                fsync: FsyncSchedule::Os,
            };
            match PartitionActor::spawn(name.to_string(), p.index, dir, opts) {
                Ok(tx) => {
                    let route = Route { name: meta.name.clone(), topic_id: meta.topic_id, partition: p.index, leader: p.leader, tx };
                    self.routes.by_name.insert((meta.name.clone(), p.index), route.clone());
                    self.routes.by_id.insert((meta.topic_id, p.index), route);
                }
                Err(e) => tracing::error!(topic=%name, partition=p.index, error=%e, "spawn partition actor failed"),
            }
        }
        let _ = self.tx_watch.send(self.routes.clone());
        tracing::info!(topic=%name, partitions=meta.partitions.len(), rf=rf, "topic created (auto)");
        Ok(meta)
    }
}
