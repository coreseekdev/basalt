//! 事务 API handlers（ADR-18 §8，块 c）：AddPartitionsToTxn(24)/EndTxn(26)/
//! TxnOffsetCommit(28)/DescribeTransactions(65)/ListTransactions(66) +
//! InitProducerId(22) 的事务分支。
//!
//! 路由纪律（§9 拓扑）：事务协调器单实例驻 controller 节点。EndTxn/
//! AddPartitionsToTxn/Describe/List 只被发往 coordinator（FindCoordinator
//! Type=Transaction 回 controller）——非 controller 节点收到即回
//! NotCoordinator(16)（客户端重查 coordinator 重试，协议自愈）。
//! 例外：TxnOffsetCommit 发往**组**协调器（任意节点）——非 controller 节点
//! 经内部 RPC 代理到 controller（MSG_TXN_OFFSET_COMMIT），否则客户端会
//! 把 NOT_COORDINATOR 当组协调器漂移无限重试。

use basalt_protocol::api::ErrorCode;
use basalt_protocol::value::{s, Value};
use tokio::sync::oneshot;

use basalt_storage::error::StorageError;
use crate::meta::MetaCmd;
use crate::txn::{PendingOffset, TxnCmd, TxnOutcome};

/// 事务命令的本地/代理双路：controller 节点直发本地协调器；其余节点对
/// TxnOffsetCommit 走内部 RPC 代理（其余 API 回 NotCoordinator 即可）。
async fn txn_offset_commit_via(
    ctx: &crate::handlers::Ctx,
    txn_id: &str,
    pid: i64,
    epoch: i16,
    offsets: Vec<PendingOffset>,
) -> Result<(), ErrorCode> {
    if let Some(tx) = &ctx.txn_tx {
        let (rtx, rrx) = oneshot::channel();
        tx.send(TxnCmd::TxnOffsetCommit {
            txn_id: txn_id.to_string(),
            pid,
            epoch,
            offsets,
            reply: rtx,
        })
        .await
        .map_err(|_| ErrorCode::CoordinatorNotAvailable)?;
        rrx
            .await
            .map_err(|_| ErrorCode::CoordinatorNotAvailable)?
            .map_err(|e| storage_err_to_code(&e))
    } else {
        // 代理：controller = all_brokers 最小 id（main.rs 控制器推举同款）
        let controller = ctx
            .all_brokers
            .iter()
            .min_by_key(|(id, _, _)| *id)
            .ok_or(ErrorCode::CoordinatorNotAvailable)?;
        let addr = format!("{}:{}", controller.1, controller.2 + 1);
        crate::internal::txn_offset_commit_proxy(&addr, txn_id, pid, epoch, &offsets)
            .await
            .map_err(|e| storage_err_to_code(&e))
    }
}

async fn txn_cmd_describe(
    ctx: &crate::handlers::Ctx,
    txn_id: &str,
) -> Result<Option<(String, i64, i16, Vec<(String, i32)>)>, ErrorCode> {
    let tx = ctx.txn_tx.as_ref().ok_or(ErrorCode::NotCoordinator)?;
    let (dtx, drx) = oneshot::channel();
    tx.send(TxnCmd::Describe { txn_id: txn_id.to_string(), reply: dtx })
        .await
        .map_err(|_| ErrorCode::CoordinatorNotAvailable)?;
    drx.await.map_err(|_| ErrorCode::CoordinatorNotAvailable)
}

async fn txn_cmd_list(ctx: &crate::handlers::Ctx) -> Result<Vec<(String, i64, i16, String)>, ErrorCode> {
    let tx = ctx.txn_tx.as_ref().ok_or(ErrorCode::NotCoordinator)?;
    let (ltx, lrx) = oneshot::channel();
    tx.send(TxnCmd::ListTransactions { reply: ltx })
        .await
        .map_err(|_| ErrorCode::CoordinatorNotAvailable)?;
    lrx.await.map_err(|_| ErrorCode::CoordinatorNotAvailable)
}

/// AddPartitionsToTxn (24)：v0-3 客户端形状（v4+ 为 broker 互信形态，
/// 不对客户端宣告——ADR-18 §8）。
pub async fn add_partitions_to_txn(version: i16, req: &basalt_protocol::value::Struct, ctx: &crate::handlers::Ctx) -> Value {
    let _ = version;
    // V3AndBelow 形状（宣告面 v0-3）
    let txn_id = req.get("V3AndBelowTransactionalId").map(|v| v.as_str().to_string()).unwrap_or_default();
    let pid = req.get("V3AndBelowProducerId").map(|v| v.as_i64()).unwrap_or(-1);
    let epoch = req.get("V3AndBelowProducerEpoch").map(|v| v.as_i16()).unwrap_or(-1);

    // 收集分区
    let mut parts: Vec<(String, i32)> = Vec::new();
    if let Some(Value::Array(topics)) = req.get("V3AndBelowTopics") {
        for t in topics {
            let Value::Struct(ts) = t else { continue };
            let name = ts.get("Name").map(|v| v.as_str().to_string()).unwrap_or_default();
            if let Some(Value::Array(ps)) = ts.get("Partitions") {
                for p in ps {
                    let idx = p.as_i32();
                    parts.push((name.clone(), idx));
                }
            }
        }
    }

    let err: ErrorCode = match &ctx.txn_tx {
        None => ErrorCode::NotCoordinator,
        Some(tx) => {
            let (atx, arx) = oneshot::channel();
            let sent = tx
                .send(TxnCmd::AddPartitionsToTxn {
                    txn_id: txn_id.clone(),
                    pid,
                    epoch,
                    partitions: parts.clone(),
                    reply: atx,
                })
                .await;
            match sent {
                Err(_) => ErrorCode::CoordinatorNotAvailable,
                Ok(()) => match arx.await {
                    Ok(Ok(())) => ErrorCode::None,
                    Ok(Err(e)) => storage_err_to_code(&e),
                    Err(_) => ErrorCode::CoordinatorNotAvailable,
                },
            }
        }
    };

    // 响应：ResultsByTopicV3AndBelow（按 topic 分组，分区同码）
    let mut by_topic: Vec<(String, Vec<i32>)> = Vec::new();
    for (t, p) in &parts {
        match by_topic.iter_mut().find(|(n, _)| n == t) {
            Some((_, ps)) => ps.push(*p),
            None => by_topic.push((t.clone(), vec![*p])),
        }
    }
    let topic_results = by_topic
        .into_iter()
        .map(|(name, ps)| {
            s([
                ("Name", Value::str(name)),
                (
                    "ResultsByPartition",
                    Value::Array(
                        ps.into_iter()
                            .map(|p| {
                                s([
                                    ("PartitionIndex", Value::I32(p)),
                                    ("PartitionErrorCode", Value::I16(err as i16)),
                                ])
                            })
                            .collect(),
                    ),
                ),
            ])
        })
        .collect();
    s([
        ("ThrottleTimeMs", Value::I32(0)),
        ("ErrorCode", Value::I16(if ctx.txn_tx.is_some() { ErrorCode::None as i16 } else { ErrorCode::NotCoordinator as i16 })),
        ("ResultsByTransaction", Value::Array(vec![])),
        ("ResultsByTopicV3AndBelow", Value::Array(topic_results)),
    ])
}

/// AddOffsetsToTxn (25)：sendOffsetsToTransaction(groupMetadata)（KIP-447）
/// 的前导——把消费组挂进事务。协调器侧与 AddPartitionsToTxn 同语义
/// （Empty→Begin 空分区清单开启服务端事务；Ongoing 幂等 Ok；Prepare 拒），
/// 组挂靠的实际承载在 TxnOffsetCommit 的 pending 记录。
pub async fn add_offsets_to_txn(req: &basalt_protocol::value::Struct, ctx: &crate::handlers::Ctx) -> Value {
    let txn_id = req.get("TransactionalId").map(|v| v.as_str().to_string()).unwrap_or_default();
    let pid = req.get("ProducerId").map(|v| v.as_i64()).unwrap_or(-1);
    let epoch = req.get("ProducerEpoch").map(|v| v.as_i16()).unwrap_or(-1);
    let err = match &ctx.txn_tx {
        None => ErrorCode::NotCoordinator,
        Some(tx) => {
            let (atx, arx) = oneshot::channel();
            let sent = tx
                .send(TxnCmd::AddPartitionsToTxn {
                    txn_id: txn_id.clone(),
                    pid,
                    epoch,
                    partitions: vec![],
                    reply: atx,
                })
                .await;
            match sent {
                Err(_) => ErrorCode::CoordinatorNotAvailable,
                Ok(()) => match arx.await {
                    Ok(Ok(())) => ErrorCode::None,
                    Ok(Err(e)) => storage_err_to_code(&e),
                    Err(_) => ErrorCode::CoordinatorNotAvailable,
                },
            }
        }
    };
    s([
        ("ThrottleTimeMs", Value::I32(0)),
        ("ErrorCode", Value::I16(err as i16)),
    ])
}

/// EndTxn (26)：两段提交/放弃的客户端入口。
pub async fn end_txn(req: &basalt_protocol::value::Struct, ctx: &crate::handlers::Ctx) -> Value {
    let txn_id = req.get("TransactionalId").map(|v| v.as_str().to_string()).unwrap_or_default();
    let pid = req.get("ProducerId").map(|v| v.as_i64()).unwrap_or(-1);
    let epoch = req.get("ProducerEpoch").map(|v| v.as_i16()).unwrap_or(-1);
    let committed = req.get("Committed").map(|v| matches!(v, Value::Bool(true) | Value::I8(1))).unwrap_or(false);

    let err = match &ctx.txn_tx {
        None => ErrorCode::NotCoordinator,
        Some(tx) => {
            let (etx, erx) = oneshot::channel();
            let sent = tx
                .send(TxnCmd::EndTxn {
                    txn_id: txn_id.clone(),
                    pid,
                    epoch,
                    commit: committed,
                    reply: etx,
                })
                .await;
            match sent {
                Err(_) => ErrorCode::CoordinatorNotAvailable,
                Ok(()) => match erx.await {
                    Ok(Ok(())) => ErrorCode::None,
                    Ok(Err(e)) => storage_err_to_code(&e),
                    Err(_) => ErrorCode::CoordinatorNotAvailable,
                },
            }
        }
    };
    s([
        ("ThrottleTimeMs", Value::I32(0)),
        ("ErrorCode", Value::I16(err as i16)),
        ("ProducerId", Value::I64(pid)),
        ("ProducerEpoch", Value::I16(epoch)),
    ])
}

/// TxnOffsetCommit (28)：消费位事务提交（KIP-447 essential——epoch 校验 +
/// pending 落 TxnLog，commit 生效/abort 丢弃）。
pub async fn txn_offset_commit(version: i16, req: &basalt_protocol::value::Struct, ctx: &crate::handlers::Ctx) -> Value {
    let txn_id = req.get("TransactionalId").map(|v| v.as_str().to_string()).unwrap_or_default();
    let pid = req.get("ProducerId").map(|v| v.as_i64()).unwrap_or(-1);
    let epoch = req.get("ProducerEpoch").map(|v| v.as_i16()).unwrap_or(-1);

    let mut topics: Vec<(String, Vec<(i32, i64, String)>)> = Vec::new();
    if let Some(Value::Array(ts)) = req.get("Topics") {
        for t in ts {
            let Value::Struct(ts) = t else { continue };
            let name = ts.get("Name").map(|v| v.as_str().to_string()).unwrap_or_default();
            let mut parts = Vec::new();
            if let Some(Value::Array(ps)) = ts.get("Partitions") {
                for p in ps {
                    let Value::Struct(ps) = p else { continue };
                    let idx = ps.get("PartitionIndex").map(|v| v.as_i32()).unwrap_or(0);
                    let off = ps.get("CommittedOffset").map(|v| v.as_i64()).unwrap_or(-1);
                    let meta = ps.get("CommittedMetadata").map(|v| v.as_str().to_string()).unwrap_or_default();
                    parts.push((idx, off, meta));
                }
            }
            topics.push((name, parts));
        }
    }

    // 扁平化为 PendingOffset（组侧 epoch/generation 校验属 §10 边界；
    // epoch fence 本体在协调器）
    let mut pending: Vec<PendingOffset> = Vec::new();
    for (topic, parts) in &topics {
        for (idx, off, meta) in parts {
            pending.push(PendingOffset {
                group: req.get("GroupId").map(|v| v.as_str().to_string()).unwrap_or_default(),
                topic: topic.clone(),
                partition: *idx,
                offset: *off,
                metadata: meta.clone(),
            });
        }
    }

    let err = match txn_offset_commit_via(ctx, &txn_id, pid, epoch, pending).await {
        Ok(()) => ErrorCode::None,
        Err(code) => code,
    };

    let topic_results = topics
        .into_iter()
        .map(|(name, parts)| {
            s([
                ("Name", Value::str(name)),
                (
                    "Partitions",
                    Value::Array(
                        parts
                            .into_iter()
                            .map(|(idx, _, _)| {
                                s([("PartitionIndex", Value::I32(idx)), ("ErrorCode", Value::I16(err as i16))])
                            })
                            .collect(),
                    ),
                ),
            ])
        })
        .collect();
    let _ = version;
    s([
        ("ThrottleTimeMs", Value::I32(0)),
        ("Topics", Value::Array(topic_results)),
    ])
}

/// DescribeTransactions (65)。
pub async fn describe_transactions(req: &basalt_protocol::value::Struct, ctx: &crate::handlers::Ctx) -> Value {
    let ids: Vec<String> = match req.get("TransactionalIds") {
        Some(Value::Array(xs)) => xs.iter().map(|v| v.as_str().to_string()).collect(),
        _ => vec![],
    };
    let mut states = Vec::new();
    for id in ids {
        match txn_cmd_describe(ctx, &id).await {
            Ok(Some((phase, pid, epoch, parts))) => {
                let mut by_topic: Vec<(String, Vec<i32>)> = Vec::new();
                for (t, p) in parts {
                    match by_topic.iter_mut().find(|(n, _)| n == &t) {
                        Some((_, ps)) => ps.push(p),
                        None => by_topic.push((t, vec![p])),
                    }
                }
                states.push(s([
                    ("ErrorCode", Value::I16(ErrorCode::None as i16)),
                    ("TransactionalId", Value::str(id)),
                    ("TransactionState", Value::str(phase)),
                    ("TransactionTimeoutMs", Value::I32(60_000)),
                    ("TransactionStartTimeMs", Value::I64(-1)),
                    ("ProducerId", Value::I64(pid)),
                    ("ProducerEpoch", Value::I16(epoch)),
                    (
                        "Topics",
                        Value::Array(
                            by_topic
                                .into_iter()
                                .map(|(t, ps)| {
                                    s([
                                        ("Topic", Value::str(t)),
                                        ("Partitions", Value::Array(ps.into_iter().map(Value::I32).collect())),
                                    ])
                                })
                                .collect(),
                        ),
                    ),
                ]));
            }
            Ok(None) => states.push(s([
                ("ErrorCode", Value::I16(ErrorCode::InvalidProducerIdMapping as i16)),
                ("TransactionalId", Value::str(id)),
                ("TransactionState", Value::str("")),
                ("TransactionTimeoutMs", Value::I32(0)),
                ("TransactionStartTimeMs", Value::I64(-1)),
                ("ProducerId", Value::I64(-1)),
                ("ProducerEpoch", Value::I16(-1)),
                ("Topics", Value::Array(vec![])),
            ])),
            Err(code) => states.push(s([
                ("ErrorCode", Value::I16(code as i16)),
                ("TransactionalId", Value::str(id)),
                ("TransactionState", Value::str("")),
                ("TransactionTimeoutMs", Value::I32(0)),
                ("TransactionStartTimeMs", Value::I64(-1)),
                ("ProducerId", Value::I64(-1)),
                ("ProducerEpoch", Value::I16(-1)),
                ("Topics", Value::Array(vec![])),
            ])),
        }
    }
    s([
        ("ThrottleTimeMs", Value::I32(0)),
        ("TransactionStates", Value::Array(states)),
    ])
}

/// ListTransactions (66)：v0 形态（StateFilters 过滤本地支持）。
pub async fn list_transactions(req: &basalt_protocol::value::Struct, ctx: &crate::handlers::Ctx) -> Value {
    let state_filters: Vec<String> = match req.get("StateFilters") {
        Some(Value::Array(xs)) => xs.iter().map(|v| v.as_str().to_string()).collect(),
        _ => vec![],
    };
    let mut unknown_filters: Vec<String> = Vec::new();
    let known = ["Empty", "Ongoing", "Prepare", "Complete"];
    for f in &state_filters {
        if !known.contains(&f.as_str()) {
            unknown_filters.push(f.clone());
        }
    }
    let err = match txn_cmd_list(ctx).await {
        Ok(rows) => {
            let states = rows
                .into_iter()
                .filter(|(_, _, _, phase)| state_filters.is_empty() || state_filters.iter().any(|f| f == phase))
                .map(|(id, pid, _epoch, phase)| {
                    s([
                        ("TransactionalId", Value::str(id)),
                        ("ProducerId", Value::I64(pid)),
                        ("TransactionState", Value::str(phase)),
                    ])
                })
                .collect();
            return s([
                ("ThrottleTimeMs", Value::I32(0)),
                ("ErrorCode", Value::I16(ErrorCode::None as i16)),
                ("UnknownStateFilters", Value::Array(unknown_filters.into_iter().map(Value::str).collect())),
                ("TransactionStates", Value::Array(states)),
            ]);
        }
        Err(code) => code,
    };
    s([
        ("ThrottleTimeMs", Value::I32(0)),
        ("ErrorCode", Value::I16(err as i16)),
        ("UnknownStateFilters", Value::Array(vec![])),
        ("TransactionStates", Value::Array(vec![])),
    ])
}

/// InitProducerId 的事务分支（handlers_groups 委派）：coordinator 分配
/// PID/每事务 bump epoch。客户端超时字段 POC 忽略（协调器用 broker 配置）。
pub async fn init_producer_id_transactional(txn_id: &str, ctx: &crate::handlers::Ctx) -> Value {
    let Some(tx) = &ctx.txn_tx else {
        return s([
            ("ThrottleTimeMs", Value::I32(0)),
            ("ErrorCode", Value::I16(ErrorCode::NotCoordinator as i16)),
            ("ProducerId", Value::I64(-1)),
            ("ProducerEpoch", Value::I16(-1)),
        ]);
    };
    let (itx, irx) = oneshot::channel();
    let sent = tx.send(TxnCmd::InitProducerId { txn_id: txn_id.to_string(), reply: itx }).await;
    let err_pid = (Value::I64(-1), Value::I16(-1));
    match sent {
        Err(_) => s([
            ("ThrottleTimeMs", Value::I32(0)),
            ("ErrorCode", Value::I16(ErrorCode::CoordinatorNotAvailable as i16)),
            ("ProducerId", err_pid.0),
            ("ProducerEpoch", err_pid.1),
        ]),
        Ok(()) => match irx.await {
            Ok((pid, epoch)) => s([
                ("ThrottleTimeMs", Value::I32(0)),
                ("ErrorCode", Value::I16(ErrorCode::None as i16)),
                ("ProducerId", Value::I64(pid)),
                ("ProducerEpoch", Value::I16(epoch)),
            ]),
            Err(_) => s([
                ("ThrottleTimeMs", Value::I32(0)),
                ("ErrorCode", Value::I16(ErrorCode::CoordinatorNotAvailable as i16)),
                ("ProducerId", err_pid.0),
                ("ProducerEpoch", err_pid.1),
            ]),
        },
    }
}

fn storage_err_to_code(e: &StorageError) -> ErrorCode {
    match e {
        StorageError::InvalidProducerEpoch => ErrorCode::InvalidProducerEpoch,
        StorageError::InvalidTxnState => ErrorCode::InvalidTxnState,
        StorageError::InvalidProducerIdMapping => ErrorCode::InvalidProducerIdMapping,
        StorageError::Other(m) if m.contains("concurrent") => ErrorCode::CoordinatorLoadInProgress,
        // 兜底 15（可重试，客户端重查 coordinator）——83 常量实为
        // EligibleLeadersNotAvailable（review P1-1 核对 Errors.java），
        // 事务面误用它会给客户端错的重试语义
        _ => ErrorCode::CoordinatorNotAvailable,
    }
}

/// controller 地址解析（FindCoordinator Type=Transaction / TxnOffsetCommit
/// 代理用）：controller_id == 自身 → self；否则经 MetaCmd::BrokerAddr 查询
/// 运行期地址（meta-sync 填充）。查无（controller 未注册/已死）→ 15 可重试。
pub async fn controller_endpoint(
    ctx: &crate::handlers::Ctx,
) -> Result<(i32, String, u16), ErrorCode> {
    if ctx.controller_id == ctx.node_id {
        return Ok((ctx.node_id, ctx.host.clone(), ctx.port));
    }
    let (atx, arx) = oneshot::channel();
    ctx.meta_tx
        .send(MetaCmd::BrokerAddr { node: ctx.controller_id, reply: atx })
        .await
        .map_err(|_| ErrorCode::CoordinatorNotAvailable)?;
    match arx.await.map_err(|_| ErrorCode::CoordinatorNotAvailable)? {
        Some((host, port)) => Ok((ctx.controller_id, host, port)),
        None => Err(ErrorCode::CoordinatorNotAvailable),
    }
}

/// MarkerJob 生产端路由器（ADR-18 §5，main 在 controller 节点拉起）：
/// 本地 leader 直发分区 actor；远端 leader 查地址后走内部 RPC。
pub async fn marker_router(
    mut rx: tokio::sync::mpsc::Receiver<crate::txn::MarkerJob>,
    routes_rx: tokio::sync::watch::Receiver<crate::meta::RoutingTable>,
    meta_tx: tokio::sync::mpsc::Sender<MetaCmd>,
    self_node: i32,
) {
    while let Some(job) = rx.recv().await {
        let res = route_marker(&job, &routes_rx, &meta_tx, self_node).await;
        let _ = job.reply.send(res);
    }
}

async fn route_marker(
    job: &crate::txn::MarkerJob,
    routes_rx: &tokio::sync::watch::Receiver<crate::meta::RoutingTable>,
    meta_tx: &tokio::sync::mpsc::Sender<MetaCmd>,
    self_node: i32,
) -> Result<i64, StorageError> {
    use crate::partition::PartitionCmd;
    use basalt_storage::error::StorageError;
    let route = routes_rx.borrow().find(&job.topic, job.partition).cloned();
    match route {
        Some(r) if r.leader == self_node => {
            let (rtx, rrx) = oneshot::channel();
            r.tx.send(PartitionCmd::WriteTxnMarker {
                producer_id: job.producer_id,
                producer_epoch: job.producer_epoch,
                outcome: job.outcome,
                reply: rtx,
            })
            .await
            .map_err(|_| StorageError::Other("local partition actor closed".into()))?;
            rrx.await.map_err(|_| StorageError::Other("marker reply dropped".into()))?
        }
        Some(r) => {
            // 远端 leader：查地址 → 内部 RPC（client port + 1）
            let (atx, arx) = oneshot::channel();
            meta_tx
                .send(MetaCmd::BrokerAddr { node: r.leader, reply: atx })
                .await
                .map_err(|_| StorageError::Other("meta closed".into()))?;
            let Some((host, port)) = arx.await.map_err(|_| StorageError::Other("broker addr dropped".into()))? else {
                return Err(StorageError::Other(format!("broker {} unknown", r.leader)));
            };
            crate::internal::write_txn_marker(
                &format!("{host}:{}", port + 1),
                &job.topic,
                job.partition,
                job.producer_id,
                job.producer_epoch,
                job.outcome,
            )
            .await
        }
        None => Err(StorageError::Other(format!(
            "unknown partition {}-{}",
            job.topic, job.partition
        ))),
    }
}
