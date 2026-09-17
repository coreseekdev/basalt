//! KIP-848 新消费组协议 handlers（T-M3.3 块 b，ADR-19 §4/§6）：
//! ConsumerGroupHeartbeat(68 v0) 单循环 + ConsumerGroupDescribe(69 v0) 管理面。
//! 组状态机/分配在 coordinator::consumer_group actor（块 a），本层只做
//! 协议形状适配：TopicId↔topic 名双向映射、分区数快照经 meta 查询携带、
//! fenced 三态 → 错误码映射（82/25）。

use basalt_coordinator::{CGCmd, CGHeartbeat};
use basalt_protocol::api::ErrorCode;
use basalt_protocol::value::{s, Value};
use std::collections::{BTreeMap, HashMap};
use tokio::sync::oneshot;

use crate::handlers::Ctx;
use crate::meta::MetaCmd;

/// POC 固定心跳间隔（ADR-19 §2：服务端下发）。
const HEARTBEAT_INTERVAL_MS: i32 = 5_000;

fn throttle_field() -> (&'static str, Value) {
    ("ThrottleTimeMs", Value::I32(0))
}

/// 组 actor 失联（进程关闭路径）：COORDINATOR_NOT_AVAILABLE(15)——可重试
/// 兜底（语义表面内，勿用终态码）。
fn heartbeat_unavailable() -> Value {
    s([
        throttle_field(),
        ("ErrorCode", Value::I16(ErrorCode::CoordinatorNotAvailable as i16)),
        ("ErrorMessage", Value::Null),
        ("MemberId", Value::str("")),
        ("MemberEpoch", Value::I32(-1)),
        ("HeartbeatIntervalMs", Value::I32(HEARTBEAT_INTERVAL_MS)),
        ("Assignment", Value::Null),
    ])
}

// ---------- ConsumerGroupHeartbeat (68 v0) ----------

pub async fn consumer_group_heartbeat(req: &basalt_protocol::value::Struct, ctx: &Ctx) -> Value {
    let group = req.get("GroupId").map(|v| v.as_str().to_string()).unwrap_or_default();
    // 空串 = 新成员注册（服务端分配 MemberId）；-1 = 离开
    let member_id = req.get("MemberId").map(|v| v.as_str().to_string()).unwrap_or_default();
    let member_epoch = req.get("MemberEpoch").map(|v| v.as_i32()).unwrap_or(-1);
    let subscribed: Vec<String> = match req.get("SubscribedTopicNames") {
        Some(Value::Array(a)) => a
            .iter()
            .map(|x| x.as_str().to_string())
            .filter(|n| !n.is_empty())
            .collect(),
        _ => vec![],
    };
    // 请求 owned 的 TopicId → 名反查（接线要点①）：组状态机以名字为键。
    // 未知 TopicId 的 owned 条目丢弃——owned 是增量确认面，状态机不据此分配。
    let owned: BTreeMap<String, Vec<i32>> = {
        let routes = ctx.routes();
        let mut out = BTreeMap::new();
        if let Some(Value::Array(tps)) = req.get("TopicPartitions") {
            for tp in tps {
                let Value::Struct(ts) = tp else { continue };
                let tid = ts.get("TopicId").map(|v| v.as_uuid()).unwrap_or(0);
                let Some(name) = routes.name_for(tid) else { continue };
                let parts: Vec<i32> = match ts.get("Partitions") {
                    Some(Value::Array(ps)) => ps.iter().map(|p| p.as_i32()).collect(),
                    _ => vec![],
                };
                out.insert(name, parts);
            }
        }
        out
    };

    // 订阅 topic 的分区数快照 + 名→TopicId 映射（接线要点②：经 Lookup 携带；
    // TopicMeta 有 name/topic_id/partitions）。Some(vec![]) 恒定——None 是
    // 全量语义，会把全集群 topic 灌进组（退订成员误扩 partition_counts）。
    let (mtx, mrx) = oneshot::channel();
    let _ = ctx
        .meta_tx
        .send(MetaCmd::Lookup { names: Some(subscribed.clone()), allow_create: false, reply: mtx })
        .await;
    let (metas, _brokers) = mrx.await.unwrap_or_default();
    let counts: Vec<(String, i32)> = metas
        .iter()
        .map(|t| (t.name.clone(), t.partitions.len() as i32))
        .collect();
    let tids: HashMap<String, u128> = metas.iter().map(|t| (t.name.clone(), t.topic_id)).collect();

    let (tx, rx) = oneshot::channel();
    let cmd = CGCmd::Heartbeat(CGHeartbeat {
        group,
        member_id,
        member_epoch,
        subscribed,
        owned,
        counts,
        reply: tx,
    });
    if ctx.cg_tx.send(cmd).await.is_err() {
        return heartbeat_unavailable();
    }
    let Ok(res) = rx.await else {
        return heartbeat_unavailable();
    };

    // fenced 三态 → 错误码（接线要点③）：fenced 心跳不带 assignment、零状态
    let (err, assignment_val) = if res.fenced {
        let code = if res.unknown_member { ErrorCode::UnknownMemberId } else { ErrorCode::FencedMemberEpoch };
        (code, Value::Null)
    } else {
        (ErrorCode::None, assignment_value(&res.assignment, &tids))
    };

    s([
        throttle_field(),
        ("ErrorCode", Value::I16(err as i16)),
        ("ErrorMessage", Value::Null),
        ("MemberId", Value::str(res.member_id)),
        ("MemberEpoch", Value::I32(res.member_epoch)),
        ("HeartbeatIntervalMs", Value::I32(HEARTBEAT_INTERVAL_MS)),
        ("Assignment", assignment_val),
    ])
}

/// 名字键的 assignment → 线上 TopicPartitions（名→TopicId 经 Lookup 快照）。
fn assignment_value(assignment: &BTreeMap<String, Vec<i32>>, tids: &HashMap<String, u128>) -> Value {
    if assignment.is_empty() {
        return Value::Null;
    }
    let parts: Vec<Value> = assignment
        .iter()
        .filter_map(|(topic, ps)| {
            let tid = tids.get(topic)?;
            Some(s([
                ("TopicId", Value::Uuid(*tid)),
                ("Partitions", Value::Array(ps.iter().map(|p| Value::I32(*p)).collect())),
            ]))
        })
        .collect();
    s([("TopicPartitions", Value::Array(parts))])
}

// ---------- ConsumerGroupDescribe (69 v0) ----------

pub async fn consumer_group_describe(req: &basalt_protocol::value::Struct, ctx: &Ctx) -> Value {
    let group_ids: Vec<String> = match req.get("GroupIds") {
        Some(Value::Array(gs)) => gs.iter().map(|g| g.as_str().to_string()).collect(),
        _ => vec![],
    };
    let _include_ops = req.get("IncludeAuthorizedOperations").map(|v| v.as_bool()).unwrap_or(false);

    // 逐组快照：Found(Some)/不存在(None)/actor 失联(Unavailable)——后两者
    // 错误码分流（69 终态 vs 15 可重试）。
    enum Snap {
        Found(Option<basalt_coordinator::GroupDescribe>),
        Unavailable,
    }
    let mut snapshots: Vec<Snap> = Vec::new();
    for gid in &group_ids {
        let (tx, rx) = oneshot::channel();
        if ctx.cg_tx.send(CGCmd::Describe { group: gid.clone(), reply: tx }).await.is_err() {
            snapshots.push(Snap::Unavailable);
            continue;
        }
        snapshots.push(Snap::Found(rx.await.unwrap_or(None)));
    }

    // 全部快照的 assignment topic 并集 → 一次 Lookup 拿 名→TopicId
    let mut names: std::collections::BTreeSet<String> = Default::default();
    for snap in snapshots.iter().filter_map(|s| match s {
        Snap::Found(Some(d)) => Some(d),
        _ => None,
    }) {
        for m in &snap.members {
            for t in m.assignment.keys() {
                names.insert(t.clone());
            }
        }
    }
    let (mtx, mrx) = oneshot::channel();
    let _ = ctx
        .meta_tx
        .send(MetaCmd::Lookup { names: Some(names.into_iter().collect()), allow_create: false, reply: mtx })
        .await;
    let (metas, _brokers) = mrx.await.unwrap_or_default();
    let tids: HashMap<String, u128> = metas.iter().map(|t| (t.name.clone(), t.topic_id)).collect();

    let groups: Vec<Value> = group_ids
        .iter()
        .zip(snapshots)
        .map(|(gid, snap)| match snap {
            Snap::Found(Some(d)) => {
                let members = d
                    .members
                    .iter()
                    .map(|m| {
                        let tp = assignment_tp(&m.assignment, &tids);
                        s([
                            ("MemberId", Value::str(m.member_id.clone())),
                            ("InstanceId", Value::Null),
                            ("RackId", Value::Null),
                            ("MemberEpoch", Value::I32(m.member_epoch)),
                            // POC：心跳请求头之外未携带 clientId（经典面同边界）
                            ("ClientId", Value::str("")),
                            ("ClientHost", Value::str("localhost")),
                            ("SubscribedTopicNames", Value::Array(m.subscribed.iter().map(|t| Value::str(t.clone())).collect())),
                            ("SubscribedTopicRegex", Value::Null),
                            ("Assignment", s([("TopicPartitions", tp.clone())])),
                            ("TargetAssignment", s([("TopicPartitions", tp)])),
                        ])
                    })
                    .collect();
                s([
                    ("ErrorCode", Value::I16(ErrorCode::None as i16)),
                    ("ErrorMessage", Value::Null),
                    ("GroupId", Value::str(gid.clone())),
                    ("GroupState", Value::str(d.state)),
                    ("GroupEpoch", Value::I32(d.group_epoch)),
                    ("AssignmentEpoch", Value::I32(d.assignment_epoch)),
                    ("AssignorName", Value::str(d.assignor)),
                    ("Members", Value::Array(members)),
                    ("AuthorizedOperations", Value::I32(-2147483648)),
                ])
            }
            Snap::Found(None) | Snap::Unavailable => {
                let (err, state, epochs) = match snap {
                    // 组不存在：GROUP_ID_NOT_FOUND(69)——终态，不重试
                    Snap::Found(None) => (ErrorCode::GroupIdNotFound, "", -1),
                    // actor 失联：COORDINATOR_NOT_AVAILABLE(15)——可重试
                    _ => (ErrorCode::CoordinatorNotAvailable, "", -1),
                };
                s([
                    ("ErrorCode", Value::I16(err as i16)),
                    ("ErrorMessage", Value::Null),
                    ("GroupId", Value::str(gid.clone())),
                    ("GroupState", Value::str(state)),
                    ("GroupEpoch", Value::I32(epochs)),
                    ("AssignmentEpoch", Value::I32(epochs)),
                    ("AssignorName", Value::str("")),
                    ("Members", Value::Array(vec![])),
                    ("AuthorizedOperations", Value::I32(-2147483648)),
                ])
            }
        })
        .collect();

    s([throttle_field(), ("Groups", Value::Array(groups))])
}

/// describe 版 TopicPartitions：比心跳版多 TopicName 字段（schema commonStruct）。
fn assignment_tp(assignment: &BTreeMap<String, Vec<i32>>, tids: &HashMap<String, u128>) -> Value {
    let parts: Vec<Value> = assignment
        .iter()
        .filter_map(|(topic, ps)| {
            let tid = tids.get(topic)?;
            Some(s([
                ("TopicId", Value::Uuid(*tid)),
                ("TopicName", Value::str(topic.clone())),
                ("Partitions", Value::Array(ps.iter().map(|p| Value::I32(*p)).collect())),
            ]))
        })
        .collect();
    Value::Array(parts)
}
