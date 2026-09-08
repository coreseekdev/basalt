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
    #[allow(dead_code)]
    pub all_brokers: Vec<(i32, String, u16)>,
    pub host: String,
    pub port: u16,
    pub meta_tx: mpsc::Sender<MetaCmd>,
    pub group_tx: tokio::sync::mpsc::Sender<GroupCmd>,
    pub routes_rx: tokio::sync::watch::Receiver<crate::meta::RoutingTable>,
    pub brokers_cache: std::sync::Mutex<Option<Vec<basalt_metadata::cluster::BrokerInfo>>>,
}

impl Ctx {
    /// 为每请求任务克隆上下文（brokers_cache 取当前快照）。
    pub fn clone_for_request(&self) -> Ctx {
        Ctx {
            node_id: self.node_id,
            host: self.host.clone(),
            port: self.port,
            all_brokers: self.all_brokers.clone(),
            meta_tx: self.meta_tx.clone(),
            group_tx: self.group_tx.clone(),
            routes_rx: self.routes_rx.clone(),
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
        for (t, r) in targets.iter().zip(&resolved) {
            if let Some(err) = r.err {
                replies.push((t, Value::I16(err as i16), -1, -1, -1, r.leader));
                continue;
            }
            let Some(tx) = &r.tx else {
                replies.push((t, Value::I16(ErrorCode::UnknownTopicOrPartition as i16), -1, -1, -1, -1));
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
                replies.push((t, Value::I16(ErrorCode::BrokerNotAvailable as i16), -1, -1, -1, -1));
                continue;
            }
            let out = match rx.await {
                Ok(o) => o,
                Err(_) => ProduceOutcome { base_offset: -1, last_offset: -1, log_append_time: now_ms(), error: None },
            };
            let err = match &out.error {
                Some(basalt_storage::StorageError::CorruptBatch { .. }) => ErrorCode::CorruptMessage,
                Some(StorageError::NotEnoughReplicas) => ErrorCode::NotEnoughReplicas,
                Some(StorageError::NotLeader) => ErrorCode::NotLeaderOrFollower,
                Some(StorageError::OffsetOutOfRange(_)) => ErrorCode::OffsetOutOfRange,
                Some(_) => ErrorCode::UnknownServer,
                None => ErrorCode::None,
            };
            replies.push((t, Value::I16(err as i16), out.base_offset, out.last_offset, out.log_append_time, -1));
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
        let (err, data, hw, log_start) = match f.rx {
            None => (f.err, Bytes::new(), -1, -1),
            Some(rx) => match rx.await {
                Ok(out) => {
                    if let Some(e) = &out.error {
                        let code = match e {
                            StorageError::NotLeader => ErrorCode::NotLeaderOrFollower,
                            StorageError::OffsetOutOfRange(_) => ErrorCode::OffsetOutOfRange,
                            _ => ErrorCode::UnknownServer,
                        };
                        (code, Bytes::new(), -1, -1)
                    } else {
                        match out.result {
                            Some(r) => (ErrorCode::None, r.data, r.high_watermark, r.log_start_offset),
                            None => (ErrorCode::OffsetOutOfRange, Bytes::new(), -1, -1),
                        }
                    }
                }
                Err(_) => (ErrorCode::BrokerNotAvailable, Bytes::new(), -1, -1),
            },
        };
        let entry = s([
            ("PartitionIndex", Value::I32(t.partition)),
            ("ErrorCode", Value::I16(err as i16)),
            ("HighWatermark", Value::I64(hw)),
            ("LastStableOffset", Value::I64(if hw < 0 { -1 } else { hw })),
            ("LogStartOffset", Value::I64(log_start)),
            ("AbortedTransactions", Value::Null),
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
