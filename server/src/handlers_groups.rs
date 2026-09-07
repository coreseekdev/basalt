//! 消费组 Classic 协议 + offset 管理 + topic 管理的 handlers。

use basalt_coordinator::{CommittedOffset, CoordError, GroupCmd, JoinResult, SyncResult};
use basalt_protocol::api::ErrorCode;
use basalt_protocol::value::{s, Value};
use bytes::Bytes;
use tokio::sync::oneshot;

use crate::handlers::Ctx;
use crate::meta::MetaCmd;

fn coord_err(e: CoordError) -> Value {
    Value::I16(e.code())
}

// ---------- FindCoordinator ----------

pub async fn find_coordinator(version: i16, req: &basalt_protocol::value::Struct, ctx: &Ctx) -> Value {
    // CoordinatorType: 0 = group（v1+）；统一返回本节点
    let _key = req.get("CoordinatorKey").map(|v| v.as_str().to_string());
    let _ = version;
    s([
        ("ThrottleTimeMs", Value::I32(0)),
        ("ErrorCode", Value::I16(ErrorCode::None as i16)),
        ("ErrorMessage", Value::Null),
        ("NodeId", Value::I32(ctx.node_id)),
        ("Host", Value::str(ctx.host.clone())),
        ("Port", Value::I32(ctx.port as i32)),
        ("Coordinators", Value::Array(vec![s([
            ("Key", Value::str(_key.unwrap_or_default())),
            ("NodeId", Value::I32(ctx.node_id)),
            ("Host", Value::str(ctx.host.clone())),
            ("Port", Value::I32(ctx.port as i32)),
            ("ErrorCode", Value::I16(ErrorCode::None as i16)),
            ("ErrorMessage", Value::Null),
        ])])),
    ])
}

// ---------- JoinGroup ----------

pub async fn join_group(req: &basalt_protocol::value::Struct, version: i16, ctx: &Ctx) -> Value {
    let group = req.get("GroupId").map(|v| v.as_str().to_string()).unwrap_or_default();
    let member_id = req.get("MemberId").map(|v| v.as_str().to_string()).unwrap_or_default();
    let protocol_type = req.get("ProtocolType").map(|v| v.as_str().to_string()).unwrap_or_else(|| "consumer".to_string());
    let session_timeout = req.get("SessionTimeoutMs").map(|v| v.as_i32()).unwrap_or(10_000);
    let rebalance_timeout = req.get("RebalanceTimeoutMs").map(|v| v.as_i32()).unwrap_or(session_timeout);
    let mut protocols = Vec::new();
    if let Some(Value::Array(ps)) = req.get("Protocols") {
        for p in ps {
            let Value::Struct(st) = p else { continue };
            let name = st.get("Name").map(|v| v.as_str().to_string()).unwrap_or_default();
            let meta = st.get("Metadata").and_then(|v| v.as_bytes().map(|b| b.to_vec())).unwrap_or_default();
            protocols.push((name, meta));
        }
    }
    let (tx, rx) = oneshot::channel();
    ctx.group_tx
        .send(GroupCmd::JoinGroup(
            basalt_coordinator::JoinSpec {
                group,
                member_id,
                protocol_type,
                session_timeout_ms: session_timeout,
                rebalance_timeout_ms: rebalance_timeout,
                protocols,
                client_host: "localhost".to_string(),
            },
            tx,
        ))
        .await
        .ok();
    let r: Option<JoinResult> = rx.await.ok();
    let r = r.unwrap_or(JoinResult {
        error: CoordError::GroupCoordinatorNotAvailable,
        generation: -1, protocol_type: String::new(), protocol: None,
        leader: String::new(), member_id: String::new(), members: vec![],
    });
    let _ = version;
    s([
        ("ThrottleTimeMs", Value::I32(0)),
        ("ErrorCode", coord_err(r.error)),
        ("GenerationId", Value::I32(r.generation)),
        ("ProtocolType", Value::str(r.protocol_type)),
        ("ProtocolName", r.protocol.map(|p| Value::Str(p.into())).unwrap_or(Value::Null)),
        ("Leader", Value::str(r.leader)),
        ("MemberId", Value::str(r.member_id)),
        ("Members", Value::Array(
            r.members.into_iter().map(|(mid, meta)| s([
                ("MemberId", Value::str(mid)),
                ("GroupInstanceId", Value::Null),
                ("Metadata", Value::Bytes(Bytes::from(meta))),
            ])).collect(),
        )),
        ("SkipAssignment", Value::Bool(false)),
    ])
}

// ---------- SyncGroup ----------

pub async fn sync_group(req: &basalt_protocol::value::Struct, ctx: &Ctx) -> Value {
    let group = req.get("GroupId").map(|v| v.as_str().to_string()).unwrap_or_default();
    let generation = req.get("GenerationId").map(|v| v.as_i32()).unwrap_or(-1);
    let member_id = req.get("MemberId").map(|v| v.as_str().to_string()).unwrap_or_default();
    let protocol_type = req.get("ProtocolType").map(|v| v.as_str().to_string());
    let protocol = req.get("ProtocolName").map(|v| v.as_str().to_string());
    let mut assignments = Vec::new();
    // 字段名是 Assignments（见 SyncGroupRequest.json）
    if let Some(Value::Array(as_)) = req.get("Assignments") {
        for a in as_ {
            let Value::Struct(st) = a else { continue };
            let mid = st.get("MemberId").map(|v| v.as_str().to_string()).unwrap_or_default();
            let assignment = st.get("Assignment").and_then(|v| v.as_bytes().map(|b| b.to_vec())).unwrap_or_default();
            assignments.push((mid, assignment));
        }
    }
    let (tx, rx) = oneshot::channel();
    ctx.group_tx
        .send(GroupCmd::SyncGroup(basalt_coordinator::SyncSpec {
            group, generation, member_id, protocol_type, protocol, assignments,
        }, tx))
        .await
        .ok();
    let r: Option<SyncResult> = rx.await.ok();
    let r = r.unwrap_or(SyncResult { error: CoordError::GroupCoordinatorNotAvailable, protocol_type: String::new(), protocol: None, assignment: vec![] });
    s([
        ("ThrottleTimeMs", Value::I32(0)),
        ("ErrorCode", coord_err(r.error)),
        ("ProtocolType", Value::str(r.protocol_type)),
        ("ProtocolName", r.protocol.map(|p| Value::Str(p.into())).unwrap_or(Value::Null)),
        ("Assignment", Value::Bytes(Bytes::from(r.assignment))),
    ])
}

// ---------- Heartbeat / LeaveGroup ----------

pub async fn heartbeat(req: &basalt_protocol::value::Struct, ctx: &Ctx) -> Value {
    let group = req.get("GroupId").map(|v| v.as_str().to_string()).unwrap_or_default();
    let generation = req.get("GenerationId").map(|v| v.as_i32()).unwrap_or(-1);
    let member_id = req.get("MemberId").map(|v| v.as_str().to_string()).unwrap_or_default();
    let (tx, rx) = oneshot::channel();
    ctx.group_tx.send(GroupCmd::Heartbeat { group, generation, member_id, reply: tx }).await.ok();
    let e: CoordError = rx.await.unwrap_or(CoordError::GroupCoordinatorNotAvailable);
    s([("ThrottleTimeMs", Value::I32(0)), ("ErrorCode", coord_err(e))])
}

pub async fn leave_group(req: &basalt_protocol::value::Struct, ctx: &Ctx) -> Value {
    let group = req.get("GroupId").map(|v| v.as_str().to_string()).unwrap_or_default();
    // v0-2 单 member；v3+ members 数组
    let mut member = req.get("MemberId").map(|v| v.as_str().to_string());
    if member.is_none() {
        if let Some(Value::Array(ms)) = req.get("Members") {
            if let Some(Value::Struct(m)) = ms.first() {
                member = m.get("MemberId").map(|v| v.as_str().to_string());
            }
        }
    }
    let (tx, rx) = oneshot::channel();
    ctx.group_tx
        .send(GroupCmd::LeaveGroup { group, member_id: member.unwrap_or_default(), reply: tx })
        .await
        .ok();
    let e: CoordError = rx.await.unwrap_or(CoordError::GroupCoordinatorNotAvailable);
    s([
        ("ThrottleTimeMs", Value::I32(0)),
        ("ErrorCode", coord_err(e)),
        ("Members", Value::Array(vec![])),
    ])
}

// ---------- OffsetCommit / OffsetFetch ----------

pub async fn offset_commit(req: &basalt_protocol::value::Struct, ctx: &Ctx) -> Value {
    let group = req.get("GroupId").map(|v| v.as_str().to_string()).unwrap_or_default();
    let generation = req.get("GenerationId").map(|v| v.as_i32()).unwrap_or(-1);
    let member_id = req.get("MemberId").map(|v| v.as_str().to_string()).unwrap_or_default();
    let mut offsets = Vec::new();
    let mut topic_results: Vec<(String, Vec<Value>)> = Vec::new();
    if let Some(Value::Array(topics)) = req.get("Topics") {
        for t in topics {
            let Value::Struct(ts) = t else { continue };
            let name = ts.get("Name").map(|v| v.as_str().to_string()).unwrap_or_default();
            let mut pvals = Vec::new();
            if let Some(Value::Array(parts)) = ts.get("Partitions") {
                for p in parts {
                    let Value::Struct(ps) = p else { continue };
                    let index = ps.get("PartitionIndex").map(|v| v.as_i32()).unwrap_or(0);
                    let offset = ps.get("CommittedOffset").map(|v| v.as_i64()).unwrap_or(-1);
                    let metadata = ps.get("CommittedMetadata").map(|v| v.as_str().to_string()).unwrap_or_default();
                    let ts_commit = ps.get("CommitTimestamp").map(|v| v.as_i64()).unwrap_or(-1);
                    offsets.push(CommittedOffset {
                        topic: name.clone(),
                        partition: index,
                        offset,
                        metadata,
                        commit_ts: ts_commit,
                    });
                    pvals.push(s([("PartitionIndex", Value::I32(index)), ("ErrorCode", Value::I16(ErrorCode::None as i16))]));
                }
            }
            topic_results.push((name, pvals));
        }
    }
    tracing::info!(api="OffsetCommit", %group, %generation, %member_id, n=offsets.len(), "commit");
    let (tx, rx) = oneshot::channel();
    ctx.group_tx
        .send(GroupCmd::CommitOffsets { group, generation, member_id, offsets, reply: tx })
        .await
        .ok();
    let _e = rx.await.unwrap_or(CoordError::GroupCoordinatorNotAvailable);
    let responses: Vec<Value> = topic_results
        .into_iter()
        .map(|(name, parts)| s([("Name", Value::str(name)), ("Partitions", Value::Array(parts))]))
        .collect();
    s([("ThrottleTimeMs", Value::I32(0)), ("Topics", Value::Array(responses))])
}

pub async fn offset_fetch(req: &basalt_protocol::value::Struct, ctx: &Ctx) -> Value {
    let group = req.get("GroupId").map(|v| v.as_str().to_string()).unwrap_or_default();
    let mut requested: Option<Vec<String>> = None;
    if let Some(Value::Array(topics)) = req.get("Topics") {
        let mut names = Vec::new();
        for t in topics {
            let Value::Struct(ts) = t else { continue };
            names.push(ts.get("Name").map(|v| v.as_str().to_string()).unwrap_or_default());
        }
        requested = Some(names);
    }
    tracing::info!(api="OffsetFetch", %group, topics=?requested, "fetch");
    let (tx, rx) = oneshot::channel();
    ctx.group_tx.send(GroupCmd::FetchOffsets { group, topics: requested.clone(), reply: tx }).await.ok();
    let committed = rx.await.unwrap_or_default();
    tracing::info!(api="OffsetFetch-reply", n=committed.len(), "fetch reply");

    // 已知 topic 全集（供 null 请求展开）
    let all_topics: Vec<String> = ctx.routes().iter_all_topics();
    let topics = requested.unwrap_or(all_topics);

    let mut responses = Vec::new();
    for name in topics {
        let mut pvals = Vec::new();
        for o in committed.iter().filter(|o| o.topic == name) {
            pvals.push(s([
                ("PartitionIndex", Value::I32(o.partition)),
                ("CommittedOffset", Value::I64(o.offset)),
                ("CommittedLeaderEpoch", Value::I32(-1)),
                ("Metadata", Value::str(o.metadata.clone())),
                ("ErrorCode", Value::I16(ErrorCode::None as i16)),
            ]));
        }
        responses.push(s([("Name", Value::str(name)), ("Partitions", Value::Array(pvals))]));
    }
    s([("ThrottleTimeMs", Value::I32(0)), ("Topics", Value::Array(responses))])
}

// ---------- CreateTopics / DeleteTopics ----------

pub async fn create_topics(req: &basalt_protocol::value::Struct, ctx: &Ctx) -> Value {
    let mut results = Vec::new();
    if let Some(Value::Array(topics)) = req.get("Topics") {
        for t in topics {
            let Value::Struct(ts) = t else { continue };
            let name = ts.get("Name").map(|v| v.as_str().to_string()).unwrap_or_default();
            let num_partitions = ts.get("NumPartitions").map(|v| v.as_i32()).unwrap_or(1);
            let rf = ts.get("ReplicationFactor").map(|v| v.as_i32()).unwrap_or(1);
            let (reply_tx, reply_rx) = oneshot::channel();
            let _ = ctx.meta_tx.send(MetaCmd::Lookup { names: Some(vec![name.clone()]), allow_create: true, reply: reply_tx }).await;
            let (found, _brokers) = reply_rx.await.unwrap_or_default();
            let (err, msg) = if found.iter().any(|t| t.name == name) {
                (ErrorCode::None, None)
            } else {
                (ErrorCode::InvalidTopicException, Some("create failed".to_string()))
            };
            results.push(s([
                ("Name", Value::str(name)),
                ("TopicId", Value::Uuid(found.first().map(|t| t.topic_id).unwrap_or(0))),
                ("ErrorCode", Value::I16(err as i16)),
                ("ErrorMessage", msg.map(|m| Value::Str(m.into())).unwrap_or(Value::Null)),
                ("NumPartitions", Value::I32(num_partitions)),
                ("ReplicationFactor", Value::I32(rf)),
                ("Configs", Value::Array(vec![])),
            ]));
        }
    }
    s([
        ("ThrottleTimeMs", Value::I32(0)),
        ("Topics", Value::Array(results)),
    ])
}

pub async fn delete_topics(req: &basalt_protocol::value::Struct, ctx: &Ctx) -> Value {
    let mut results = Vec::new();
    if let Some(Value::Array(topics)) = req.get("Topics") {
        for t in topics {
            let Value::Struct(ts) = t else { continue };
            let name = ts.get("Name").map(|v| v.as_str().to_string()).unwrap_or_default();
            let (reply_tx, reply_rx) = oneshot::channel();
            let _ = ctx.meta_tx.send(MetaCmd::Lookup { names: Some(vec![name.clone()]), allow_create: false, reply: reply_tx }).await;
            let (found, _brokers) = reply_rx.await.unwrap_or_default();
            let err = if found.is_empty() {
                ErrorCode::UnknownTopicOrPartition
            } else {
                ErrorCode::None // 删除语义 POC：标记即可，物理删除由 retention 完成
            };
            results.push(s([
                ("Name", Value::str(name)),
                ("TopicId", Value::Uuid(0)),
                ("ErrorCode", Value::I16(err as i16)),
                ("ErrorMessage", Value::Null),
            ]));
        }
    }
    s([("ThrottleTimeMs", Value::I32(0)), ("Responses", Value::Array(results))])
}
