//! API handlers：请求 Value → 响应 Value。
//!
//! 编码器按 schema@version 门控字段——handler 统一塞入全部已知字段，
//! 版本裁剪由 protocol 层完成（多余 key 被忽略）。

use crate::meta::RoutingTable;
use crate::partition::{now_ms, PartitionCmd, ProduceOutcome};
use basalt_protocol::api::ErrorCode;
use basalt_protocol::registry::Registry;
use basalt_protocol::value::{s, Value};
use bytes::Bytes;
use basalt_coordinator::GroupCmd;
use basalt_storage::error::StorageError;
use tokio::sync::{mpsc, oneshot};

use crate::meta::MetaCmd;

pub struct Ctx {
    pub node_id: i32,
    /// 控制器推举（cfg.nodes 最小 id；单节点 = 自身）——事务协调器驻留点
    /// （ADR-18 §9）。运行期地址经 MetaCmd::BrokerAddr 查询（all_brokers
    /// 生产恒空，review P0-1 实证不可作地址源）。
    pub controller_id: i32,
    #[allow(dead_code)]
    pub all_brokers: Vec<(i32, String, u16)>,
    pub host: String,
    pub port: u16,
    pub meta_tx: mpsc::Sender<MetaCmd>,
    pub group_tx: tokio::sync::mpsc::Sender<GroupCmd>,
    /// KIP-848 consumer 组 actor 句柄（每节点一个，组协调器全节点拓扑——
    /// 与 classic 同构，ADR-19 §4）
    pub cg_tx: tokio::sync::mpsc::Sender<basalt_coordinator::CGCmd>,
    pub routes_rx: tokio::sync::watch::Receiver<crate::meta::RoutingTable>,
    /// 事务协调器句柄（仅 controller 节点为 Some；§9 拓扑）。
    pub txn_tx: Option<mpsc::Sender<crate::txn::TxnCmd>>,
    pub brokers_cache: std::sync::Mutex<Option<Vec<basalt_metadata::cluster::BrokerInfo>>>,
    /// 读缓冲池（perf #2：writer 归还 + actor 读复用共享同一池）
    // BufferPool 进程单例共享资源池（内部 Mutex 串行化）：Arc 表达资源共享而非
    // 共享可变所有权，与 Bytes/mpsc 内部引用计数同级豁免（ADR-13）。
    #[allow(clippy::disallowed_types)]
    pub pool: std::sync::Arc<basalt_storage::pool::BufferPool>,
}

impl Ctx {
    /// 为每请求任务克隆上下文（brokers_cache 取当前快照）。
    pub fn clone_for_request(&self) -> Ctx {
        Ctx {
            node_id: self.node_id,
            controller_id: self.controller_id,
            host: self.host.clone(),
            port: self.port,
            all_brokers: self.all_brokers.clone(),
            meta_tx: self.meta_tx.clone(),
            group_tx: self.group_tx.clone(),
            cg_tx: self.cg_tx.clone(),
            routes_rx: self.routes_rx.clone(),
            txn_tx: self.txn_tx.clone(),
            pool: self.pool.clone(),
            brokers_cache: std::sync::Mutex::new(self.brokers_cache.lock().unwrap().clone()),
        }
    }
}

impl Clone for Ctx {
    fn clone(&self) -> Self {
        self.clone_for_request()
    }
}

impl Ctx {
    /// metadata 响应携带的 broker 全集（后续响应复用；单节点为空）
    pub fn set_brokers(&self, brokers: Vec<basalt_metadata::cluster::BrokerInfo>) {
        self.brokers_cache.lock().unwrap().replace(brokers);
    }

    pub fn broker_array(&self) -> Value {
        let cached = self.brokers_cache.lock().unwrap().clone().unwrap_or_default();
        let mut brokers: Vec<(i32, String, u16)> = cached
            .into_iter()
            .map(|b| (b.node_id, b.host, b.port))
            .collect();
        if brokers.is_empty() {
            brokers = vec![(self.node_id, self.host.clone(), self.port)];
        }
        brokers.sort_unstable();
        Value::Array(
            brokers
                .into_iter()
                .map(|(id, host, port)| {
                    s([
                        ("NodeId", Value::I32(id)),
                        ("Host", Value::str(host)),
                        ("Port", Value::I32(port as i32)),
                        ("Rack", Value::Null),
                    ])
                })
                .collect(),
        )
    }

    pub fn routes(&self) -> tokio::sync::watch::Ref<'_, RoutingTable> {
        self.routes_rx.borrow()
    }
}

// ---------- ApiVersions ----------

pub fn api_versions(req: &basalt_protocol::value::Struct, version: i16) -> Value {
    let reg = Registry::global();
    let mut api_keys = Vec::new();
    for (k, lo, hi, _name) in reg.advertised() {
        api_keys.push(s([
            ("ApiKey", Value::I16(k)),
            ("MinVersion", Value::I16(lo)),
            ("MaxVersion", Value::I16(hi)),
        ]));
    }
    api_keys.sort_by_key(|v| match v {
        Value::Struct(st) => st.get("ApiKey").map(|x| x.as_i16()).unwrap_or(0),
        _ => 0,
    });

    let _ = version;
    let _ = req;
    s([
        ("ErrorCode", Value::I16(ErrorCode::None as i16)),
        ("ApiKeys", Value::Array(api_keys)),
        ("ThrottleTimeMs", Value::I32(0)),
    ])
}

// ---------- Metadata ----------

pub async fn metadata(req: &basalt_protocol::value::Struct, version: i16, ctx: &Ctx) -> Value {
    // Topics: v1+ nullable（null = 全量）
    // Kafka 语义：null（v1+）或空数组（v0 无 null 表达）→ 返回全部 topic
    let names: Option<Vec<String>> = match req.get("Topics") {
        Some(Value::Null) | None => None,
        Some(Value::Array(a)) => {
            let mut v = Vec::new();
            for t in a {
                let Value::Struct(ts) = t else { continue };
                let n = ts.get("Name").map(|x| x.as_str().to_string());
                match n {
                    Some(n) if !n.is_empty() => v.push(n),
                    _ => continue,
                }
            }
            if v.is_empty() { None } else { Some(v) }
        }
        _ => None,
    };
    // Kafka 语义：AllowAutoTopicCreation 自 v4 才引入（客户端用来退出自动建题）；
    // v1-v3 的 metadata 请求按 broker 配置默认允许自动建题。
    let allow_create = if version < 4 {
        true
    } else {
        req.get("AllowAutoTopicCreation").map(|v| v.as_bool()).unwrap_or(false)
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    let _ = ctx
        .meta_tx
        .send(MetaCmd::Lookup { names: names.clone(), allow_create, reply: reply_tx })
        .await;
    let (topics, brokers) = reply_rx.await.unwrap_or_default();
    ctx.set_brokers(brokers);

    // 请求了具体 topic 但元数据没有 → UNKNOWN_TOPIC_OR_PARTITION 条目
    let mut topic_vals = Vec::new();
    if let Some(req_names) = &names {
        for rn in req_names {
            if let Some(t) = topics.iter().find(|t| &t.name == rn) {
                topic_vals.push(topic_value(t, ctx));
            } else {
                topic_vals.push(s([
                    ("ErrorCode", Value::I16(ErrorCode::UnknownTopicOrPartition as i16)),
                    ("Name", Value::str(rn.clone())),
                    ("TopicId", Value::Uuid(0)),
                    ("IsInternal", Value::Bool(false)),
                    ("Partitions", Value::Array(vec![])),
                    ("TopicAuthorizedOperations", Value::I32(-2147483648)),
                ]));
            }
        }
    } else {
        for t in &topics {
            topic_vals.push(topic_value(t, ctx));
        }
    }

    s([
        ("ThrottleTimeMs", Value::I32(0)),
        ("Brokers", ctx.broker_array()),
        ("ClusterId", Value::Null),
        ("ControllerId", Value::I32(ctx.node_id)),
        ("Topics", Value::Array(topic_vals)),
        ("ClusterAuthorizedOperations", Value::I32(-2147483648)),
    ])
}

fn topic_value(t: &basalt_metadata::TopicMeta, ctx: &Ctx) -> Value {
    let parts: Vec<Value> = t
        .partitions
        .iter()
        .map(|p| {
            let _ = ctx;
            s([
                ("ErrorCode", Value::I16(ErrorCode::None as i16)),
                ("PartitionIndex", Value::I32(p.index)),
                ("LeaderId", Value::I32(p.leader)),
                ("LeaderEpoch", Value::I32(p.leader_epoch)),
                ("ReplicaNodes", Value::Array(p.replicas.iter().map(|&b| Value::I32(b)).collect())),
                ("IsrNodes", Value::Array(p.isr.iter().map(|&b| Value::I32(b)).collect())),
                ("OfflineReplicas", Value::Array(vec![])),
            ])
        })
        .collect();
    s([
        ("ErrorCode", Value::I16(ErrorCode::None as i16)),
        ("Name", Value::str(t.name.clone())),
        ("TopicId", Value::Uuid(t.topic_id)),
        ("IsInternal", Value::Bool(t.internal)),
        ("Partitions", Value::Array(parts)),
        ("TopicAuthorizedOperations", Value::I32(-2147483648)),
    ])
}

// ---------- Produce ----------

pub struct ProduceTarget {
    pub topic: String,
    pub topic_id: u128,
    pub partition: i32,
    pub batches: Bytes,
}

struct Resolved {
    tx: Option<mpsc::Sender<PartitionCmd>>,
    err: Option<ErrorCode>,
    leader: i32,
}

/// StorageError → 线上错误码的**唯一**映射（T-M3.6 语义表面锁定；三处
/// 调用点：produce / fetch / txn——兜底 15 可重试，勿用 83，见 api.rs 注）。
pub(crate) fn storage_error_code(e: &StorageError) -> ErrorCode {
    match e {
        StorageError::CorruptBatch { .. } => ErrorCode::CorruptMessage,
        StorageError::NotEnoughReplicas => ErrorCode::NotEnoughReplicas,
        StorageError::OutOfOrderSequence(..) => ErrorCode::OutOfOrderSequence,
        StorageError::InvalidProducerEpoch => ErrorCode::InvalidProducerEpoch,
        StorageError::InvalidTxnState => ErrorCode::InvalidTxnState,
        StorageError::InvalidProducerIdMapping => ErrorCode::InvalidProducerIdMapping,
        StorageError::NotLeader => ErrorCode::NotLeaderOrFollower,
        StorageError::OffsetOutOfRange(_) => ErrorCode::OffsetOutOfRange,
        // 前事务 Prepare 残留（CONCURRENT_TRANSACTIONS 的 basalt 映射：
        // 14 可重试，客户端退避重试；官方 51 的 20ms 快退避不做）
        StorageError::Other(m) if m.contains("concurrent") => ErrorCode::CoordinatorLoadInProgress,
        _ => ErrorCode::CoordinatorNotAvailable,
    }
}

pub async fn produce(version: i16, acks: i16, targets: Vec<ProduceTarget>, ctx: &Ctx) -> Value {
    // 快照路由（clone Sender），borrow 不跨 await
    let resolved: Vec<Resolved> = {
        let routes = ctx.routes();
        targets
            .iter()
            .map(|t| {
                let route = routes
                    .find(&t.topic, t.partition)
                    .or_else(|| routes.find_by_id(t.topic_id, t.partition));
                match route {
                    None => Resolved { tx: None, err: Some(ErrorCode::UnknownTopicOrPartition), leader: -1 },
                    Some(r) if r.leader != ctx.node_id => {
                        Resolved { tx: None, err: Some(ErrorCode::NotLeaderOrFollower), leader: r.leader }
                    }
                    Some(r) => Resolved { tx: Some(r.tx.clone()), err: None, leader: r.leader },
                }
            })
            .collect()
    };

    let mut replies: Vec<(&ProduceTarget, Value, i64, i64, i64, i32)> = Vec::new();

    if acks != -1 && acks != 0 && acks != 1 {
        for t in &targets {
            replies.push((t, Value::I16(ErrorCode::InvalidRequiredAcks as i16), -1, -1, 0, 0));
        }
    } else {
        // Phase 1: 并行 send 到不同分区 actor + 收集 receiver（不 await 每个）
        struct InFlight<'a> {
            target: &'a ProduceTarget,
            rx: Option<oneshot::Receiver<ProduceOutcome>>,
            pre_err: Option<Value>,
            leader: i32,
        }
        let mut in_flight: Vec<InFlight> = Vec::new();
        for (t, r) in targets.iter().zip(&resolved) {
            if let Some(err) = r.err {
                in_flight.push(InFlight { target: t, rx: None, pre_err: Some(Value::I16(err as i16)), leader: r.leader });
                continue;
            }
            let Some(tx) = &r.tx else {
                in_flight.push(InFlight { target: t, rx: None, pre_err: Some(Value::I16(ErrorCode::UnknownTopicOrPartition as i16)), leader: -1 });
                continue;
            };
            let (txr, rx) = oneshot::channel();
            if tx
                .send(PartitionCmd::Produce {
                    batches: t.batches.clone(),
                    policy: basalt_storage::log::AssignPolicy::Assign,
                    acks,
                    reply: txr,
                })
                .await
                .is_err()
            {
                in_flight.push(InFlight { target: t, rx: None, pre_err: Some(Value::I16(ErrorCode::BrokerNotAvailable as i16)), leader: -1 });
                continue;
            }
            in_flight.push(InFlight { target: t, rx: Some(rx), pre_err: None, leader: -1 });
        }

        // Phase 2: 统一 await（不同分区 actor 已并行处理）
        for f in in_flight {
            let (err, base, last, lat) = match f.rx {
                None => (f.pre_err.unwrap_or(Value::I16(0)), -1i64, -1i64, -1i64),
                Some(rx) => {
                    let out = match rx.await {
                        Ok(o) => o,
                        Err(_) => ProduceOutcome { base_offset: -1, last_offset: -1, log_append_time: now_ms(), error: None },
                    };
                    let err = match &out.error {
                        None => ErrorCode::None,
                        Some(e) => storage_error_code(e),
                    };
                    (Value::I16(err as i16), out.base_offset, out.last_offset, out.log_append_time)
                }
            };
            replies.push((f.target, err, base, last, lat, f.leader));
        }
    }

    let mut topic_groups: Vec<(String, u128, Vec<Value>)> = Vec::new();
    for (t, err, base, _last, lat, leader) in replies {
        let entry = s([
            ("Index", Value::I32(t.partition)),
            ("ErrorCode", err),
            ("BaseOffset", Value::I64(base)),
            ("LogAppendTimeMs", Value::I64(lat)),
            ("LogStartOffset", Value::I64(0)),
            ("RecordErrors", Value::Array(vec![])),
            ("ErrorMessage", Value::Null),
            ("CurrentLeader", s([("LeaderId", Value::I32(leader)), ("LeaderEpoch", Value::I32(-1))])),
        ]);
        match topic_groups.iter_mut().find(|(n, id, _)| n == &t.topic && *id == t.topic_id) {
            Some((_, _, parts)) => parts.push(entry),
            None => topic_groups.push((t.topic.clone(), t.topic_id, vec![entry])),
        }
    }
    let _ = version;
    let responses: Vec<Value> = topic_groups
        .into_iter()
        .map(|(n, id, parts)| {
            s([
                ("Name", Value::str(n)),
                ("TopicId", Value::Uuid(id)),
                ("PartitionResponses", Value::Array(parts)),
            ])
        })
        .collect();
    s([
        ("Responses", Value::Array(responses)),
        ("ThrottleTimeMs", Value::I32(0)),
        ("NodeEndpoints", Value::Array(vec![])),
    ])
}

// ---------- Fetch ----------

pub struct FetchTarget {
    pub topic: String,
    pub topic_id: u128,
    pub partition: i32,
    pub offset: i64,
    pub max_bytes: usize,
    pub max_wait_ms: i32,
    #[allow(dead_code)]
    pub min_bytes: i32,
    /// Fetch v4+ IsolationLevel（0=read_uncommitted, 1=read_committed）。
    pub isolation: crate::partition::Isolation,
}

pub async fn fetch(targets: Vec<FetchTarget>, ctx: &Ctx) -> Value {
    // 快照路由 + 发送（不等待），await 只发生在末段收集
    struct Fetched {
        idx: usize,
        err: ErrorCode,
        rx: Option<oneshot::Receiver<crate::partition::FetchOutcome>>,
    }
    let mut futs: Vec<Fetched> = Vec::new();
    // 阶段一（borrow 作用域内只做解析与 clone，绝无 await）
    enum Resolved {
        Missing,
        NotLeader(#[allow(dead_code)] i32),
        Ready(mpsc::Sender<PartitionCmd>),
    }
    let resolved: Vec<Resolved> = {
        let routes = ctx.routes();
        targets
            .iter()
            .map(|t| {
                let route = routes
                    .find(&t.topic, t.partition)
                    .or_else(|| routes.find_by_id(t.topic_id, t.partition));
                match route {
                    None => Resolved::Missing,
                    Some(r) if r.leader != ctx.node_id => Resolved::NotLeader(r.leader),
                    Some(r) => Resolved::Ready(r.tx.clone()),
                }
            })
            .collect()
    };
    // 阶段二：发送（await 时不持有 borrow）
    for (i, t) in targets.iter().enumerate() {
        match &resolved[i] {
            Resolved::Missing => futs.push(Fetched { idx: i, err: ErrorCode::UnknownTopicOrPartition, rx: None }),
            Resolved::NotLeader(_) => futs.push(Fetched { idx: i, err: ErrorCode::NotLeaderOrFollower, rx: None }),
            Resolved::Ready(tx) => {
                let deadline = std::time::Instant::now()
                    + std::time::Duration::from_millis(t.max_wait_ms.clamp(0, 60_000) as u64);
                let (txr, rx) = oneshot::channel();
                let sent = tx
                    .send(PartitionCmd::Fetch {
                        offset: t.offset,
                        max_bytes: t.max_bytes,
                        deadline,
                        isolation: t.isolation,
                        reply: txr,
                    })
                    .await;
                match sent {
                    Ok(()) => futs.push(Fetched { idx: i, err: ErrorCode::None, rx: Some(rx) }),
                    Err(_) => futs.push(Fetched { idx: i, err: ErrorCode::BrokerNotAvailable, rx: None }),
                }
            }
        }
    }

    let mut topic_groups: Vec<(String, u128, Vec<Value>)> = Vec::new();
    for f in futs {
        let t = &targets[f.idx];
        let (err, data, hw, log_start, lso) = match f.rx {
            None => (f.err, Bytes::new(), -1, -1, -1),
            Some(rx) => match rx.await {
                Ok(out) => {
                    if let Some(e) = &out.error {
                        let code = storage_error_code(e);
                        (code, Bytes::new(), -1, -1, -1)
                    } else {
                        match out.result {
                            Some(r) => (
                                ErrorCode::None,
                                r.data,
                                r.high_watermark,
                                r.log_start_offset,
                                out.last_stable_offset,
                            ),
                            None => (ErrorCode::OffsetOutOfRange, Bytes::new(), -1, -1, -1),
                        }
                    }
                }
                Err(_) => (ErrorCode::BrokerNotAvailable, Bytes::new(), -1, -1, -1),
            },
        };
        // AbortedTransactions（ADR-18 §4.2 投递模型 (b)）：服务端已预过滤
        // aborted 批，恒发空数组——客户端无需条目（四档实测兼容）。
        let aborted_val = Value::Array(vec![]);
        let entry = s([
            ("PartitionIndex", Value::I32(t.partition)),
            ("ErrorCode", Value::I16(err as i16)),
            ("HighWatermark", Value::I64(hw)),
            ("LastStableOffset", Value::I64(lso)),
            ("LogStartOffset", Value::I64(log_start)),
            ("AbortedTransactions", aborted_val),
            ("PreferredReadReplica", Value::I32(-1)),
            ("Records", Value::Bytes(data)),
        ]);
        match topic_groups.iter_mut().find(|(n, id, _)| n == &t.topic && *id == t.topic_id) {
            Some((_, _, parts)) => parts.push(entry),
            None => topic_groups.push((t.topic.clone(), t.topic_id, vec![entry])),
        }
    }

    let responses: Vec<Value> = topic_groups
        .into_iter()
        .map(|(n, id, parts)| {
            s([
                ("Topic", Value::str(n)),
                ("TopicId", Value::Uuid(id)),
                ("Partitions", Value::Array(parts)),
            ])
        })
        .collect();
    s([
        ("ThrottleTimeMs", Value::I32(0)),
        ("ErrorCode", Value::I16(ErrorCode::None as i16)),
        ("SessionId", Value::I32(0)),
        ("Responses", Value::Array(responses)),
        ("NodeEndpoints", Value::Array(vec![])),
    ])
}

// ---------- ListOffsets ----------

pub async fn list_offsets(req: &basalt_protocol::value::Struct, ctx: &Ctx) -> Value {
    // 阶段一：解析请求 + 快照路由（borrow 不跨 await）

    let mut pendings: Vec<(String, Vec<(i32, i64, Option<mpsc::Sender<PartitionCmd>>)>)> = Vec::new();
    {
        let routes = ctx.routes();
        if let Some(Value::Array(topics)) = req.get("Topics") {
            for t in topics {
                let Value::Struct(ts) = t else { continue };
                let name = ts.get("Name").map(|x| x.as_str().to_string()).unwrap_or_default();
                let mut parts: Vec<(i32, i64, Option<mpsc::Sender<PartitionCmd>>)> = Vec::new();
                if let Some(Value::Array(parr)) = ts.get("Partitions") {
                    for p in parr {
                        let Value::Struct(ps) = p else { continue };
                        let index = ps.get("PartitionIndex").map(|v| v.as_i32()).unwrap_or(0);
                        let ts_req = ps.get("Timestamp").map(|v| v.as_i64()).unwrap_or(-1);
                        // 只在 borrow 内解析出 Sender 克隆，发送放阶段二（无 borrow await）
                        let tx_clone = routes.find(&name, index).map(|r| r.tx.clone());
                        parts.push((index, ts_req, tx_clone));
                    }
                }
                pendings.push((name, parts));
            }
        }
    }
    // 阶段二：发送并收集（无 borrow）
    let mut responses = Vec::new();
    for (name, parts) in pendings {
        let mut pvals = Vec::new();
        for (index, ts_req, tx_clone) in parts {
            let (err, offset, found_ts) = match tx_clone {
                Some(tx) => {
                    let (txr, rx) = oneshot::channel();
                    match tx.send(PartitionCmd::ListOffsets { timestamp: ts_req, reply: txr }).await {
                        Ok(()) => match rx.await {
                            Ok(Ok((off, ts))) => (ErrorCode::None, off, ts),
                            Ok(Err(_)) => (ErrorCode::OffsetOutOfRange, -1, -1),
                            Err(_) => (ErrorCode::BrokerNotAvailable, -1, -1),
                        },
                        Err(_) => (ErrorCode::BrokerNotAvailable, -1, -1),
                    }
                }
                None => (ErrorCode::UnknownTopicOrPartition, -1, -1),
            };
            pvals.push(s([
                ("PartitionIndex", Value::I32(index)),
                ("ErrorCode", Value::I16(err as i16)),
                ("Timestamp", Value::I64(if ts_req < 0 { -1 } else { found_ts })),
                ("Offset", Value::I64(offset)),
                ("LeaderEpoch", Value::I32(-1)),
            ]));
        }
        responses.push(s([("Name", Value::str(name)), ("Partitions", Value::Array(pvals))]));
    }
    s([("ThrottleTimeMs", Value::I32(0)), ("Topics", Value::Array(responses))])
}


/// OffsetForLeaderEpoch（key 23）：返回指定 epoch 的 end offset。
pub async fn offset_for_leader_epoch(req: &basalt_protocol::value::Struct, ctx: &Ctx) -> Value {
    // 阶段 1：快照路由（borrow 不跨 await）
    #[allow(dead_code)]
    #[allow(dead_code)]
    #[allow(dead_code)]
    struct Pending {
        index: i32,
        rx: Option<oneshot::Receiver<(i32, i64)>>,
    }
    // 步骤 A：resolve（borrow 内无 await）
    struct Resolved {
        #[allow(dead_code)]
        name: String,
        index: i32,
        epoch: i32,
        tx: Option<mpsc::Sender<PartitionCmd>>,
    }
    let resolved: Vec<(String, Vec<Resolved>)> = {
        let routes = ctx.routes();
        let mut out = Vec::new();
        if let Some(Value::Array(topics)) = req.get("Topics") {
            for t in topics {
                let Value::Struct(ts) = t else { continue };
                let name = ts.get("Name").map(|x| x.as_str().to_string()).unwrap_or_default();
                let mut parts = Vec::new();
                if let Some(Value::Array(parr)) = ts.get("Partitions") {
                    for pd in parr {
                        let Value::Struct(ps) = pd else { continue };
                        let index = ps.get("PartitionIndex").map(|v| v.as_i32()).unwrap_or(0);
                        let req_epoch = ps.get("CurrentLeaderEpoch").map(|v| v.as_i32()).unwrap_or(-1);
                        let tx = routes.find(&name, index).map(|r| r.tx.clone());
                        parts.push(Resolved { name: name.clone(), index, epoch: req_epoch, tx });
                    }
                }
                out.push((name, parts));
            }
        }
        out
    };
    // 步骤 B：send（无 borrow）
    let mut pendings: Vec<(String, Vec<Pending>)> = Vec::new();
    for (name, parts) in resolved {
        let mut pending_parts = Vec::new();
        for r in parts {
            let (reply_tx, reply_rx) = oneshot::channel();
            let sent = match &r.tx {
                Some(actor_tx) => actor_tx
                    .send(PartitionCmd::EndOffsetForEpoch { epoch: r.epoch, reply: reply_tx })
                    .await
                    .is_ok(),
                None => false,
            };
            pending_parts.push(Pending { index: r.index, rx: sent.then_some(reply_rx) });
        }
        pendings.push((name, pending_parts));
    }
    // 阶段 2：收集
    let mut responses = Vec::new();
    for (name, parts) in pendings {
        let mut pvals = Vec::new();
        for p in parts {
            let (err, leader_epoch, end) = match p.rx {
                Some(rx) => match rx.await {
                    Ok((le, end_off)) => (0i16, le, end_off),
                    Err(_) => (16i16, -1, -1),
                },
                None => (3i16, -1, -1),
            };
            pvals.push(s([
                ("ErrorCode", Value::I16(err)),
                ("PartitionIndex", Value::I32(p.index)),
                ("LeaderEpoch", Value::I32(leader_epoch)),
                ("EndOffset", Value::I64(end)),
            ]));
        }
        responses.push(s([("Name", Value::str(name)), ("Partitions", Value::Array(pvals))]));
    }
    s([("ThrottleTimeMs", Value::I32(0)), ("Topics", Value::Array(responses))])
}


/// DeleteRecords (key 21)：设置 log_start_offset，删除之前的段。
pub async fn delete_records(req: &basalt_protocol::value::Struct, ctx: &Ctx) -> Value {
    #[allow(dead_code)]
    struct Pending {
        name: String,
        index: i32,
        offset: i64,
        rx: Option<oneshot::Receiver<Result<i64, StorageError>>>,
    }
    let mut resolved: Vec<(String, Vec<ResolvedDR>)> = Vec::new();
    struct ResolvedDR {
        index: i32,
        offset: i64,
        tx: Option<mpsc::Sender<PartitionCmd>>,
    }
    {
        let routes = ctx.routes();
        if let Some(Value::Array(topics)) = req.get("Topics") {
            for t in topics {
                let Value::Struct(ts) = t else { continue };
                let name = ts.get("Name").map(|x| x.as_str().to_string()).unwrap_or_default();
                let mut parts = Vec::new();
                if let Some(Value::Array(parr)) = ts.get("Partitions") {
                    for pd in parr {
                        let Value::Struct(ps) = pd else { continue };
                        let index = ps.get("PartitionIndex").map(|v| v.as_i32()).unwrap_or(0);
                        let offset = ps.get("Offset").map(|v| v.as_i64()).unwrap_or(-1);
                        let tx = routes.find(&name, index).map(|r| r.tx.clone());
                        parts.push(ResolvedDR { index, offset, tx });
                    }
                }
                resolved.push((name, parts));
            }
        }
    }
    let mut responses = Vec::new();
    for (name, parts) in resolved {
        let mut pvals = Vec::new();
        for r in parts {
            let (err, low_watermark) = match &r.tx {
                Some(actor_tx) => {
                    let (tx, rx) = oneshot::channel();
                    match actor_tx.send(PartitionCmd::DeleteRecords { offset: r.offset, reply: tx }).await {
                        Ok(()) => match rx.await {
                            Ok(Ok(lw)) => (0i16, lw),
                            Ok(Err(StorageError::OffsetOutOfRange(_))) => (1i16, -1),
                            Ok(Err(_)) => (2i16, -1),
                            Err(_) => (8i16, -1),
                        },
                        Err(_) => (8i16, -1),
                    }
                }
                None => (3i16, -1),
            };
            pvals.push(s([
                ("PartitionIndex", Value::I32(r.index)),
                ("LowWatermark", Value::I64(low_watermark)),
                ("ErrorCode", Value::I16(err)),
            ]));
        }
        responses.push(s([("Name", Value::str(name)), ("Partitions", Value::Array(pvals))]));
    }
    s([("ThrottleTimeMs", Value::I32(0)), ("Topics", Value::Array(responses))])
}

#[cfg(test)]
mod error_semantics_tests {
    //! T-M3.6 尾项：错误码分流语义表——官方数值 + retriable 分类**双重**
    //! 锁定。权威面 = apache/kafka `Errors.java`（客户端重试决策依据）：
    //! basalt 返回的每个码，客户端将按官方 retriable 标志决定重试或终止
    //! ——数值对而分类错 = 客户端行为错（㊻/㊿ 同族教训）。

    use basalt_protocol::api::ErrorCode;

    /// (我们的变体, 官方名, 官方数值, Errors.java isRetriable)
    const TABLE: &[(ErrorCode, &'static str, i16, bool)] = &[
        (ErrorCode::None, "NONE", 0, false),
        (ErrorCode::OffsetOutOfRange, "OFFSET_OUT_OF_RANGE", 1, true),
        (ErrorCode::CorruptMessage, "CORRUPT_MESSAGE", 2, true),
        (ErrorCode::NotLeaderOrFollower, "NOT_LEADER_OR_FOLLOWER", 6, true),
        (ErrorCode::CoordinatorLoadInProgress, "COORDINATOR_LOAD_IN_PROGRESS", 14, true),
        (ErrorCode::CoordinatorNotAvailable, "COORDINATOR_NOT_AVAILABLE", 15, true),
        (ErrorCode::NotCoordinator, "NOT_COORDINATOR", 16, true),
        (ErrorCode::MessageTooLarge, "MESSAGE_TOO_LARGE", 10, false),
        (ErrorCode::RecordListTooLarge, "RECORD_LIST_TOO_LARGE", 18, false),
        (ErrorCode::NotEnoughReplicas, "NOT_ENOUGH_REPLICAS", 19, true),
        (ErrorCode::OutOfOrderSequence, "OUT_OF_ORDER_SEQUENCE", 45, true),
        (ErrorCode::DuplicateSequenceNumber, "DUPLICATE_SEQUENCE_NUMBER", 46, false),
        (ErrorCode::InvalidProducerEpoch, "INVALID_PRODUCER_EPOCH", 47, false),
        (ErrorCode::InvalidProducerIdMapping, "INVALID_PRODUCER_ID_MAPPING", 48, false),
        (ErrorCode::InvalidTxnState, "INVALID_TXN_STATE", 49, false),
        (ErrorCode::InvalidProducerId, "INVALID_PRODUCER_ID", 50, false),
        (ErrorCode::EligibleLeadersNotAvailable, "ELIGIBLE_LEADERS_NOT_AVAILABLE", 83, true),
        (ErrorCode::FencedMemberEpoch, "FENCED_MEMBER_EPOCH", 82, false),
        (ErrorCode::GroupIdNotFound, "GROUP_ID_NOT_FOUND", 69, false),
        (ErrorCode::UnsupportedAssignor, "UNSUPPORTED_ASSIGNOR", 112, false),
        (ErrorCode::GroupMaxSizeReached, "GROUP_MAX_SIZE_REACHED", 81, false),
    ];

    /// 官方数值权威表（Errors.java 转录，独立于我们枚举的第二真相源——
    /// 两表逐项对撞即双向锁定）。
    const OFFICIAL: &[(&str, i16)] = &[
        ("NONE", 0),
        ("OFFSET_OUT_OF_RANGE", 1),
        ("CORRUPT_MESSAGE", 2),
        ("NOT_LEADER_OR_FOLLOWER", 6),
        ("COORDINATOR_LOAD_IN_PROGRESS", 14),
        ("COORDINATOR_NOT_AVAILABLE", 15),
        ("NOT_COORDINATOR", 16),
        ("MESSAGE_TOO_LARGE", 10),
        ("RECORD_LIST_TOO_LARGE", 18),
        ("NOT_ENOUGH_REPLICAS", 19),
        ("OUT_OF_ORDER_SEQUENCE", 45),
        ("DUPLICATE_SEQUENCE_NUMBER", 46),
        ("INVALID_PRODUCER_EPOCH", 47),
        ("INVALID_PRODUCER_ID_MAPPING", 48),
        ("INVALID_TXN_STATE", 49),
        ("INVALID_PRODUCER_ID", 50),
        ("ELIGIBLE_LEADERS_NOT_AVAILABLE", 83),
        ("FENCED_MEMBER_EPOCH", 82),
        ("GROUP_ID_NOT_FOUND", 69),
        ("UNSUPPORTED_ASSIGNOR", 112),
        ("GROUP_MAX_SIZE_REACHED", 81),
    ];

    #[test]
    fn official_numbers_locked() {
        for (code, name, want, _) in TABLE {
            let official = OFFICIAL.iter().find(|(n, _)| n == name).unwrap();
            assert_eq!(*code as i16, *want, "{name} 枚举判别值漂移");
            assert_eq!(*want, official.1, "{name} 与官方转录表不一致");
        }
    }

    #[test]
    fn retriable_classification_matches_official() {
        // 分类面独立转录（Errors.java isRetriable）：数值对而分类错 =
        // 客户端行为错（83 误用作兜底即此类，㊿ 族）
        let official_retriable: &[(&str, bool)] = &[
            ("NONE", false),
            ("OFFSET_OUT_OF_RANGE", true),
            ("CORRUPT_MESSAGE", true),
            ("NOT_LEADER_OR_FOLLOWER", true),
            ("COORDINATOR_LOAD_IN_PROGRESS", true),
            ("COORDINATOR_NOT_AVAILABLE", true),
            ("NOT_COORDINATOR", true),
            ("MESSAGE_TOO_LARGE", false),
            ("RECORD_LIST_TOO_LARGE", false),
            ("NOT_ENOUGH_REPLICAS", true),
            ("OUT_OF_ORDER_SEQUENCE", true),
            ("DUPLICATE_SEQUENCE_NUMBER", false),
            ("INVALID_PRODUCER_EPOCH", false),
            ("INVALID_PRODUCER_ID_MAPPING", false),
            ("INVALID_TXN_STATE", false),
            ("INVALID_PRODUCER_ID", false),
            ("ELIGIBLE_LEADERS_NOT_AVAILABLE", true),
            ("FENCED_MEMBER_EPOCH", false),
            ("GROUP_ID_NOT_FOUND", false),
            ("UNSUPPORTED_ASSIGNOR", false),
            ("GROUP_MAX_SIZE_REACHED", false),
        ];
        for (code, name, _, retriable) in TABLE {
            let official = official_retriable.iter().find(|(n, _)| n == name).unwrap();
            assert_eq!(retriable, &official.1, "{name} retriable 分类漂移（{}）", *code as i16);
        }
    }

    /// 统一映射函数的输出必须全部落在语义表内（无表外码逃逸到客户端）。
    #[test]
    fn storage_error_mapping_lands_in_table() {
        use basalt_storage::error::StorageError;
        let samples: Vec<StorageError> = vec![
            StorageError::CorruptBatch { path: "p".into(), pos: 0, reason: "r".into() },
            StorageError::NotEnoughReplicas,
            StorageError::OutOfOrderSequence(3, 2),
            StorageError::InvalidProducerEpoch,
            StorageError::InvalidTxnState,
            StorageError::InvalidProducerIdMapping,
            StorageError::NotLeader,
            StorageError::OffsetOutOfRange(7),
            StorageError::Other("concurrent transaction".into()),
            StorageError::Other("anything else".into()),
        ];
        for e in &samples {
            let code = crate::handlers::storage_error_code(e);
            let hit = TABLE.iter().find(|(c, _, _, _)| c == &code);
            assert!(hit.is_some(), "{e:?} 映射到表外错误码 {}", code as i16);
            // 终态 vs 可重试显式断言（分流面）
            let (_, name, _, retriable) = hit.unwrap();
            if e.to_string().contains("concurrent") {
                assert!(retriable, "并发冲突必须可重试");
            }
            let _ = name;
        }
    }
}
