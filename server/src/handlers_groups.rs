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
    // v0-3：Key（单 string）；v4+：CoordinatorKeys（[]string 批量）。
    let keys: Vec<String> = match req.get("CoordinatorKeys") {
        Some(Value::Array(ks)) => ks.iter().map(|k| k.as_str().to_string()).collect(),
        _ => vec![req.get("Key").map(|v| v.as_str().to_string()).unwrap_or_default()],
    };
    // KeyType=1（Transaction）：事务协调器驻 controller（ADR-18 §9）。
    // 无版本门（review P0-2 实证）：KeyType 字段 v1+ 恒存在，Java 3.x/
    // franz-go 的事务查找按 broker 宣告版本发 v4+。
    // 响应形状（review 探针实证）：v1-3 客户端读扁平 NodeId/Host/Port（=
    // 协调器地址），v4+ 读 Coordinators[]——扁平位在事务查找时必须回
    // controller，否则 v1-3 档（librdkafka/kafka-python）拿到自身无限重试
    let key_type = req.get("KeyType").map(|v| v.as_i8()).unwrap_or(0) as i32;
    let txn_lookup = key_type == 1;
    let mut coordinators = Vec::new();
    let mut top = (ctx.node_id, ctx.host.clone(), ctx.port as i32, ErrorCode::None);
    for k in &keys {
        let (nid, host, port, ec) = if txn_lookup {
            match crate::handlers_txn::controller_endpoint(ctx).await {
                Ok((id, h, p)) => (id, h, p as i32, ErrorCode::None),
                Err(code) => (ctx.node_id, ctx.host.clone(), ctx.port as i32, code),
            }
        } else {
            // 方案 B 块 b2：hash(group_id) → __basalt_group_state 分区 → leader
            // 单节点时 leader 恒为自身；多节点时 leader 可能是其他 broker
            let internal_tid = crate::meta::topic_id_from("__basalt_group_state");
            let part = (crate::meta::topic_id_from(k) % 1) as i32; // 1 分区（spike）
            let routes = ctx.routes();
            let leader_info = routes.find("__basalt_group_state", part).map(|r| (r.leader, r.tx.clone()));
            drop(routes);
            match leader_info {
                Some((leader_id, _tx)) if leader_id != ctx.node_id => {
                    // 非 leader broker：查 brokers cache 获取 leader 地址
                    let broker = ctx.brokers_cache.lock().unwrap().clone()
                        .unwrap_or_default()
                        .into_iter()
                        .find(|b| b.node_id == leader_id);
                    match broker {
                        Some(b) => (leader_id, b.host.clone(), b.port as i32, ErrorCode::None),
                        None => (ctx.node_id, ctx.host.clone(), ctx.port as i32, ErrorCode::None),
                    }
                }
                _ => (ctx.node_id, ctx.host.clone(), ctx.port as i32, ErrorCode::None),
            }
        };
        if txn_lookup && version <= 3 {
            // v1-3：扁平位即协调器（单 key 语义）
            top = (nid, host.clone(), port, ec);
        }
        coordinators.push(s([
            ("Key", Value::str(k.clone())),
            ("NodeId", Value::I32(nid)),
            ("Host", Value::str(host)),
            ("Port", Value::I32(port)),
            ("ErrorCode", Value::I16(ec as i16)),
            ("ErrorMessage", Value::Null),
        ]));
    }
    s([
        ("ThrottleTimeMs", Value::I32(0)),
        ("ErrorCode", Value::I16(ErrorCode::None as i16)),
        ("ErrorMessage", Value::Null),
        ("NodeId", Value::I32(top.0)),
        ("Host", Value::str(top.1)),
        ("Port", Value::I32(top.2)),
        ("Coordinators", Value::Array(coordinators)),
    ])
}

// ---------- JoinGroup ----------

pub async fn join_group(req: &basalt_protocol::value::Struct, version: i16, ctx: &Ctx) -> Value {
    let group = req.get("GroupId").map(|v| v.as_str().to_string()).unwrap_or_default();
    // ACL（T-M4.1）：GROUP:READ（骨架）——join 是组数据面入口，拒绝即
    // 无法 commit/heartbeat（member 状态不存在时 commit 自然失败）
    if !crate::acl::authorize(&ctx.principal, crate::acl::OP_READ, crate::acl::RT_GROUP, &group) {
        return s([
            ("ThrottleTimeMs", Value::I32(0)),
            ("ErrorCode", Value::I16(ErrorCode::GroupAuthorizationFailed as i16)),
            ("GenerationId", Value::I32(-1)),
            ("GroupProtocol", Value::Null),
            ("Leader", Value::str("")),
            ("MemberId", Value::Str("".into())),
            ("Members", Value::Array(vec![])),
        ]);
    }
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
            // null 请求 = 该组全部：只回**有已提交 offset** 的 topic（Kafka 语义；
            // 五轮探针实证——未知组/无提交组回路由全集属语义偏离）
            None => all_topics
                .iter()
                .filter(|n| committed.iter().any(|o| o.topic == **n))
                .map(|n| (n.clone(), None))
                .collect(),
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
            // topic 名校验（Kafka 同型）：空名/超长/非法字符拒绝——请求侧
            // 编码按计划名查值，查不到写默认空串；裸放行会建出空名 topic
            // 污染集群状态（basalt-cli 首轮实证，账本 65）
            let invalid_name = name.is_empty()
                || name.len() > 249
                || name == "." || name == ".."
                || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-');
            if invalid_name {
                results.push(s([
                    ("Name", Value::str(name.clone())),
                    ("TopicId", Value::Uuid(0)),
                    ("ErrorCode", Value::I16(ErrorCode::InvalidTopicException as i16)),
                    ("ErrorMessage", Value::str(format!("invalid topic name {name:?}"))),
                    ("NumPartitions", Value::I32(-1)),
                    ("ReplicationFactor", Value::I32(-1)),
                    ("Configs", Value::Array(vec![])),
                ]));
                continue;
            }
            let num_partitions = ts.get("NumPartitions").map(|v| v.as_i32()).unwrap_or(1);
            // ReplicationFactor 线上是 int16——as_i32 窄匹配静默返 0（账本 66，
            // 同 ㊿ as_i8 家族；rf=0 → 无副本分区 → failover 无候选人）
            let rf = ts.get("ReplicationFactor").map(|v| v.as_i16() as i32).unwrap_or(1);
            // per-topic 分层存储（T-M4.3 v2）：configs 键 basalt.storage.mode=tiered
            let tiered = match ts.get("Configs") {
                Some(Value::Array(cfgs)) => cfgs.iter().any(|c| {
                    let Value::Struct(cs) = c else { return false };
                    cs.get("Name").map(|v| v.as_str()) == Some("basalt.storage.mode")
                        && cs.get("Value").map(|v| v.as_str()) == Some("tiered")
                }),
                _ => false,
            };
            // ACL（T-M4.1）：TOPIC:CREATE 或 CLUSTER:CREATE（骨架）
            let authorized = crate::acl::authorize(&ctx.principal, crate::acl::OP_CREATE, crate::acl::RT_TOPIC, &name)
                || crate::acl::authorize(&ctx.principal, crate::acl::OP_CREATE, crate::acl::RT_CLUSTER, "");
            if !authorized {
                results.push(s([
                    ("Name", Value::str(name)),
                    ("TopicId", Value::Uuid(0)),
                    ("ErrorCode", Value::I16(ErrorCode::TopicAuthorizationFailed as i16)),
                    ("ErrorMessage", Value::Null),
                    ("NumPartitions", Value::I32(num_partitions)),
                    ("ReplicationFactor", Value::I32(rf)),
                    ("Configs", Value::Array(vec![])),
                ]));
                continue;
            }
            let (reply_tx, reply_rx) = oneshot::channel();
            // EnsureTopic：请求的 NumPartitions/RF 是建题参数（≠ Lookup
            // allow_create 的 broker 默认——那会把请求值静默覆盖，账本 59）
            let _ = ctx.meta_tx.send(MetaCmd::EnsureTopic {
                name: name.clone(),
                partitions: num_partitions,
                rf,
                tiered,
                reply: reply_tx,
            }).await;
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
    // v0-5：TopicNames（[]string）；v6+：Topics（[]DeleteTopicState{Name?, TopicId}）
    let mut targets: Vec<(String, u128)> = Vec::new();
    if let Some(Value::Array(topics)) = req.get("Topics") {
        for t in topics {
            let Value::Struct(ts) = t else { continue };
            let name = ts.get("Name").map(|v| v.as_str().to_string()).unwrap_or_default();
            let tid = ts.get("TopicId").map(|v| v.as_uuid()).unwrap_or(0);
            targets.push((name, tid));
        }
    } else if let Some(Value::Array(tn)) = req.get("TopicNames") {
        for t in tn {
            targets.push((t.as_str().to_string(), 0));
        }
    }
    let mut results = Vec::new();
    for (name, tid) in targets {
        // 仅按名删除；纯 TopicId（Name 为空）请求暂不支持（返回 UNKNOWN_TOPIC_ID）
        // ACL（T-M4.1）：TOPIC:DELETE（骨架）
        if !crate::acl::authorize(&ctx.principal, crate::acl::OP_DELETE, crate::acl::RT_TOPIC, &name) {
            results.push(s([
                ("Name", Value::str(name)),
                ("TopicId", Value::Uuid(0)),
                ("ErrorCode", Value::I16(ErrorCode::TopicAuthorizationFailed as i16)),
                ("ErrorMessage", Value::Null),
            ]));
            continue;
        }
        let err = if name.is_empty() {
            ErrorCode::UnknownTopicId
        } else {
            // 先查存在性（Kafka 语义：删未知 topic = UNKNOWN_TOPIC_OR_PARTITION）
            let (ptx, prx) = oneshot::channel();
            let _ = ctx.meta_tx.send(MetaCmd::Lookup { names: Some(vec![name.clone()]), allow_create: false, reply: ptx }).await;
            let (pre, _brokers) = prx.await.unwrap_or_default();
            if pre.iter().all(|t| t.name != name) {
                results.push(s([
                    ("Name", Value::str(name)),
                    ("TopicId", Value::Uuid(0)),
                    ("ErrorCode", Value::I16(ErrorCode::UnknownTopicOrPartition as i16)),
                    ("ErrorMessage", Value::Null),
                ]));
                continue;
            }
            let (dtx, drx) = oneshot::channel();
            let _ = ctx.meta_tx.send(MetaCmd::DeleteTopic { name: name.clone(), reply: dtx }).await;
            let ok = drx.await.unwrap_or(false);
            // 删除后回查：仍可见 = 删除失败（真错误）；不可见 = 成功
            let (ltx, lrx) = oneshot::channel();
            let _ = ctx.meta_tx.send(MetaCmd::Lookup { names: Some(vec![name.clone()]), allow_create: false, reply: ltx }).await;
            let (found, _brokers) = lrx.await.unwrap_or_default();
            if found.iter().any(|t| t.name == name) {
                ErrorCode::UnknownTopicOrPartition
            } else {
                ErrorCode::None
            }
        };
        results.push(s([
            ("Name", Value::str(name)),
            ("TopicId", Value::Uuid(tid)),
            ("ErrorCode", Value::I16(err as i16)),
            ("ErrorMessage", Value::Null),
        ]));
    }
    s([("ThrottleTimeMs", Value::I32(0)), ("Responses", Value::Array(results))])
}


// ---------- DescribeGroups / ListGroups ----------

pub async fn describe_groups(req: &basalt_protocol::value::Struct, ctx: &Ctx) -> Value {
    // 请求 v0-5：Groups 是 []string（非 struct 数组）
    let mut group_ids = Vec::new();
    if let Some(Value::Array(gs)) = req.get("Groups") {
        for g in gs {
            group_ids.push(g.as_str().to_string());
        }
    }
    let include_ops = req.get("IncludeAuthorizedOperations").map(|v| v.as_bool()).unwrap_or(false);

    let mut responses = Vec::new();
    for gid in group_ids {
        let (tx, rx) = oneshot::channel();
        ctx.group_tx.send(GroupCmd::DescribeGroup { group: gid.clone(), reply: tx }).await.ok();
        let detail = rx.await.ok().flatten();
        match detail {
            Some(d) => responses.push(s([
                ("ErrorCode", Value::I16(ErrorCode::None as i16)),
                ("ErrorMessage", Value::Null),
                ("GroupId", Value::str(gid)),
                ("GroupState", Value::str(d.state)),
                ("ProtocolType", Value::str(d.protocol_type)),
                ("ProtocolData", Value::str(d.protocol)),
                ("Members", Value::Array(
                    d.members.iter().map(|m| s([
                        ("MemberId", Value::str(m.member_id.clone())),
                        ("GroupInstanceId", Value::Null),
                        ("ClientId", Value::str(m.client_id.clone())),
                        ("ClientHost", Value::str(m.client_host.clone())),
                        ("MemberMetadata", Value::Bytes(Bytes::copy_from_slice(&m.metadata))),
                        ("MemberAssignment", Value::Bytes(Bytes::copy_from_slice(&m.assignment))),
                    ])).collect(),
                )),
                ("AuthorizedOperations", Value::I32(if include_ops { 0 } else { -2147483648 })),
            ])),
            None => responses.push(s([
                ("ErrorCode", Value::I16(ErrorCode::None as i16)),
                ("ErrorMessage", Value::Null),
                ("GroupId", Value::str(gid)),
                ("GroupState", Value::str("Dead")),
                ("ProtocolType", Value::str("")),
                ("ProtocolData", Value::str("")),
                ("Members", Value::Array(vec![])),
                ("AuthorizedOperations", Value::I32(-2147483648)),
            ])),
        }
    }
    s([("ThrottleTimeMs", Value::I32(0)), ("Groups", Value::Array(responses))])
}

pub async fn list_groups(req: &basalt_protocol::value::Struct, ctx: &Ctx) -> Value {
    let states_filter: Option<Vec<String>> = req.get("StatesFilter").and_then(|v| match v {
        Value::Array(a) => Some(a.iter().map(|x| x.as_str().to_string()).collect()),
        _ => None,
    });
    let (tx, rx) = oneshot::channel();
    ctx.group_tx.send(GroupCmd::ListGroups { reply: tx }).await.ok();
    let groups = rx.await.unwrap_or_default();
    let groups: Vec<Value> = groups
        .into_iter()
        .filter(|g| states_filter.as_ref().map_or(true, |f| f.iter().any(|s| s == &g.state)))
        .map(|g| s([
            ("GroupId", Value::str(g.group)),
            ("ProtocolType", Value::str(g.protocol_type)),
            ("GroupState", Value::str(g.state)),
            ("GroupType", Value::str("classic")),
        ]))
        .collect();
    s([
        ("ThrottleTimeMs", Value::I32(0)),
        ("ErrorCode", Value::I16(ErrorCode::None as i16)),
        ("Groups", Value::Array(groups)),
    ])
}


// ---------- InitProducerId ----------

use std::sync::atomic::{AtomicI64, Ordering};
static NEXT_PRODUCER_ID: AtomicI64 = AtomicI64::new(1000);

pub async fn init_producer_id(req: &basalt_protocol::value::Struct, ctx: &Ctx) -> Value {
    // 事务分支（ADR-18 §3/§5）：带事务 ID 的 init 走协调器（每事务 bump
    // epoch；TV2 语义）。非事务路径保持 T-M3.1 原样。
    let txn_id = req.get("TransactionalId").map(|v| v.as_str().to_string()).unwrap_or_default();
    if !txn_id.is_empty() {
        return crate::handlers_txn::init_producer_id_transactional(&txn_id, ctx).await;
    }
    // 跨 broker 唯一性（T-M3.1）：PID = node_id(高 24 位) | 进程内计数器
    // ——构造性唯一，零协调成本（经控制器分配的方案待 PID 语义扩展时再做）
    let counter = NEXT_PRODUCER_ID.fetch_add(1, Ordering::Relaxed);
    let pid = ((ctx.node_id as i64 & 0xFF) << 40) | (counter & 0xFF_FFFF_FFFF);
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
