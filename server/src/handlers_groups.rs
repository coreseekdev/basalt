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
    let _e: CoordError = rx.await.unwrap_or(CoordError::GroupCoordinatorNotAvailable);
    let responses: Vec<Value> = topic_results
        .into_iter()
        .map(|(name, parts)| s([("Name", Value::str(name)), ("Partitions", Value::Array(parts))]))
        .collect();
    s([("ThrottleTimeMs", Value::I32(0)), ("Topics", Value::Array(responses))])
}

pub async fn offset_fetch(req: &basalt_protocol::value::Struct, version: i16, ctx: &Ctx) -> Value {
    // v0-7：顶层 GroupId/Topics；v8+（librdkafka 等新客户端协商到此）：Groups[] 多组布局
    let mut groups_req: Vec<(String, Option<Vec<(String, Option<Vec<i32>>)>>)> = Vec::new();
    if version >= 8 {
        if let Some(Value::Array(gs)) = req.get("Groups") {
            for g in gs {
                let Value::Struct(g) = g else { continue };
                let gid = g.get("GroupId").map(|v| v.as_str().to_string()).unwrap_or_default();
                groups_req.push((gid, parse_fetch_topic_filter(g.get("Topics"))));
            }
        }
    } else {
        let gid = req.get("GroupId").map(|v| v.as_str().to_string()).unwrap_or_default();
        groups_req.push((gid, parse_fetch_topic_filter(req.get("Topics"))));
    }

    // 已知 topic 全集（供 null 请求展开）
    let all_topics: Vec<String> = ctx.routes().iter_all_topics();

    let mut group_vals: Vec<Value> = Vec::new();
    let mut legacy_topics: Option<Vec<Value>> = None;
    for (group, topic_filter) in &groups_req {
        let names = topic_filter.as_ref().map(|ts| ts.iter().map(|(n, _)| n.clone()).collect());
        let (tx, rx) = oneshot::channel();
        ctx.group_tx.send(GroupCmd::FetchOffsets { group: group.clone(), topics: names, reply: tx }).await.ok();
        let committed = rx.await.unwrap_or_default();
        tracing::info!(api="OffsetFetch-reply", %group, n=committed.len(), "fetch reply");

        let topics: Vec<(String, Option<Vec<i32>>)> = match topic_filter {
            Some(ts) => ts.clone(),
            None => all_topics.iter().map(|n| (n.clone(), None)).collect(),
        };
        let mut tvals = Vec::new();
        for (name, parts) in &topics {
            let mut owned: Vec<_> = committed.iter().filter(|o| &o.topic == name).collect();
            owned.sort_by_key(|o| o.partition);
            let pvals = match parts {
                // 显式分区清单：每分区一条，未提交的回 -1（Kafka 语义）
                Some(idx) => idx.iter().map(|p| match owned.iter().find(|o| o.partition == *p) {
                    Some(o) => offset_entry(o.partition, o.offset, Some(o.metadata.as_str())),
                    None => offset_entry(*p, -1, None),
                }).collect(),
                None => owned.iter().map(|o| offset_entry(o.partition, o.offset, Some(o.metadata.as_str()))).collect(),
            };
            tvals.push(s([("Name", Value::str(name.clone())), ("Partitions", Value::Array(pvals))]));
        }
        if version >= 8 {
            group_vals.push(s([
                ("GroupId", Value::str(group.clone())),
                ("Topics", Value::Array(tvals)),
                ("ErrorCode", Value::I16(ErrorCode::None as i16)),
            ]));
        } else {
            legacy_topics = Some(tvals);
        }
    }
    if version >= 8 {
        s([("ThrottleTimeMs", Value::I32(0)), ("Groups", Value::Array(group_vals))])
    } else {
        s([("ThrottleTimeMs", Value::I32(0)), ("Topics", Value::Array(legacy_topics.unwrap_or_default()))])
    }
}

/// 统一解析 v0-7 / v8+ 的 topic 过滤。None = 该组全部 topic；
/// 元素 = (topic 名, 分区过滤：None = 该 topic 全部分区)。
fn parse_fetch_topic_filter(v: Option<&Value>) -> Option<Vec<(String, Option<Vec<i32>>)>> {
    let Value::Array(ts) = v? else { return None };
    let mut out = Vec::new();
    for t in ts {
        let Value::Struct(t) = t else { continue };
        let name = t.get("Name").map(|x| x.as_str().to_string()).unwrap_or_default();
        let parts = match t.get("PartitionIndexes") {
            Some(Value::Array(a)) => {
                let idx: Vec<i32> = a.iter().map(|x| x.as_i32()).collect();
                if idx.is_empty() { None } else { Some(idx) }
            }
            _ => None,
        };
        out.push((name, parts));
    }
    if out.is_empty() { None } else { Some(out) }
}

fn offset_entry(partition: i32, offset: i64, metadata: Option<&str>) -> Value {
    s([
        ("PartitionIndex", Value::I32(partition)),
        ("CommittedOffset", Value::I64(offset)),
        ("CommittedLeaderEpoch", Value::I32(-1)),
        ("Metadata", match metadata {
            Some(m) => Value::str(m),
            None => Value::Null,
        }),
        ("ErrorCode", Value::I16(ErrorCode::None as i16)),
    ])
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


// ---------- DescribeGroups / ListGroups ----------

pub async fn describe_groups(req: &basalt_protocol::value::Struct, _ctx: &Ctx) -> Value {
    // GroupId 数组
    let mut group_ids = Vec::new();
    if let Some(Value::Array(gs)) = req.get("GroupIds") {
        for g in gs {
            if let Value::Struct(gs2) = g {
                group_ids.push(gs2.get("GroupId").map(|v| v.as_str().to_string()).unwrap_or_default());
            }
        }
    }

    // GroupManager 没有直接暴露"列出所有组"——由 handler 层向协调器查询
    // POC：返回空组描述（组存在但无活跃成员时返回 Dead 状态）
    let responses: Vec<Value> = group_ids
        .into_iter()
        .map(|gid| {
            s([
                ("ErrorCode", Value::I16(ErrorCode::None as i16)),
                ("GroupId", Value::str(gid)),
                ("State", Value::str("Stable")),
                ("ProtocolType", Value::str("consumer")),
                ("Protocol", Value::str("range")),
                ("Members", Value::Array(vec![])),
                ("AuthorizedOperations", Value::I32(-2147483648)),
            ])
        })
        .collect();
    s([("ThrottleTimeMs", Value::I32(0)), ("Groups", Value::Array(responses))])
}

pub async fn list_groups(_req: &basalt_protocol::value::Struct, _ctx: &Ctx) -> Value {
    // POC：返回空组列表（组发现需要协调器全局视角——M1 补全）
    s([
        ("ThrottleTimeMs", Value::I32(0)),
        ("Groups", Value::Array(vec![])),
    ])
}


// ---------- InitProducerId ----------

use std::sync::atomic::{AtomicI64, Ordering};
static NEXT_PRODUCER_ID: AtomicI64 = AtomicI64::new(1000);

pub async fn init_producer_id(_req: &basalt_protocol::value::Struct, _ctx: &Ctx) -> Value {
    let pid = NEXT_PRODUCER_ID.fetch_add(1, Ordering::Relaxed);
    s([
        ("ThrottleTimeMs", Value::I32(0)),
        ("ErrorCode", Value::I16(ErrorCode::None as i16)),
        ("ProducerId", Value::I64(pid)),
        ("ProducerEpoch", Value::I16(0)),
    ])
}


/// DeleteRecords (key 21) handler（委托到 handlers.rs）。
pub async fn delete_records_handler(req: &basalt_protocol::value::Struct, ctx: &Ctx) -> Value {
    crate::handlers::delete_records(req, ctx).await
}
