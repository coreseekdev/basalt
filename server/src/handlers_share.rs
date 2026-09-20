//! Share groups 协议 handler（T-M3.6 spike）：ShareGroupHeartbeat(76) /
//! ShareFetch(78) / ShareAcknowledge(79)。状态面在 share_group.rs（内存）。
//!
//! spike 边界：无长轮询（MaxWait 忽略，立即返回当前可交付）；Assignment
//! 仅按 SubscribedTopicNames 轮转分配；HW 变更/CurrentLeader 不回填。

use basalt_protocol::value::{s, Value};
use bytes::{Bytes, BytesMut};
use tokio::sync::oneshot;

const HEARTBEAT_INTERVAL_MS: i32 = 1000;

/// ShareGroupHeartbeat(76) v1
pub async fn share_group_heartbeat(req: &basalt_protocol::value::Struct, ctx: &crate::handlers::Ctx) -> Value {
    let group = req.get("GroupId").map(|v| v.as_str().to_string()).unwrap_or_default();
    let member_id = req.get("MemberId").map(|v| v.as_str().to_string()).unwrap_or_default();
    let epoch = req.get("MemberEpoch").map(|v| v.as_i32()).unwrap_or(0);
    let subscribed: Vec<String> = match req.get("SubscribedTopicNames") {
        Some(Value::Array(a)) => a.iter().filter_map(|v| match v {
            Value::Str(s) => Some(s.to_string()),
            _ => None,
        }).collect(),
        _ => Vec::new(),
    };

    // 订阅名 → (topic_id, partitions)（路由快照解析；未知题跳过——
    // 客户端拿到空分配后经 metadata 再心跳重试）
    let subscribed_resolved: Vec<(u128, Vec<i32>)> = {
        let routes = ctx.routes();
        subscribed
            .iter()
            .filter_map(|name| {
                let tid = crate::meta::topic_id_from(name);
                let mut parts: Vec<i32> = routes
                    .partitions_of(name)
                    .unwrap_or_default();
                parts.sort_unstable();
                if parts.is_empty() { None } else { Some((tid, parts)) }
            })
            .collect()
    };

    tracing::debug!(group = %group, %member_id, epoch, subscribed = ?subscribed_resolved, "share heartbeat: enter");
    let mut err: Option<(i16, String)> = None;
    let mut member_epoch = epoch;
    let mut assignment: Option<BTreeMap<u128, Vec<i32>>> = None;
    match crate::share_group::heartbeat(&group, &member_id, epoch, &subscribed_resolved) {
        Ok((new_epoch, assign)) => {
            member_epoch = new_epoch;
            assignment = assign;
        }
        Err((code, msg)) => err = Some((code, msg)),
    }
    let _ = &err;
    if let Some((code, msg)) = &err {
        return s([
            ("ThrottleTimeMs", Value::I32(0)),
            ("ErrorCode", Value::I16(*code)),
            ("ErrorMessage", Value::str(msg.clone())),
            ("MemberId", Value::str(member_id)),
            ("MemberEpoch", Value::I32(epoch)),
            ("HeartbeatIntervalMs", Value::I32(HEARTBEAT_INTERVAL_MS)),
            ("Assignment", Value::Null),
        ]);
    }

    let tp: Vec<Value> = assignment
        .as_ref()
        .map(|m| {
            m.iter()
                .map(|(tid, parts)| {
                    s([
                        ("TopicId", Value::Uuid(*tid)),
                        ("Partitions", Value::Array(parts.iter().map(|p| Value::I32(*p)).collect())),
                    ])
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let assignment_val = match &assignment {
        Some(_) => s([("TopicPartitions", Value::Array(tp))]),
        None => Value::Null,
    };
    tracing::debug!(group = %group, %member_id, has_assignment = assignment.is_some(), "share heartbeat: reply");
    s([
        ("ThrottleTimeMs", Value::I32(0)),
        ("ErrorCode", Value::I16(0)),
        ("ErrorMessage", Value::Null),
        ("MemberId", Value::str(member_id)),
        ("MemberEpoch", Value::I32(member_epoch)),
        ("HeartbeatIntervalMs", Value::I32(HEARTBEAT_INTERVAL_MS)),
        ("Assignment", assignment_val),
    ])
}

use std::collections::BTreeMap;

/// ShareFetch(78) v1-2：按交付游标读分区数据 + 登记在途锁。
pub async fn share_fetch(req: &basalt_protocol::value::Struct, ctx: &crate::handlers::Ctx) -> Value {
    let group = req.get("GroupId").map(|v| v.as_str().to_string()).unwrap_or_default();
    let member_id = req.get("MemberId").map(|v| v.as_str().to_string()).unwrap_or_default();
    let session_epoch = req.get("ShareSessionEpoch").map(|v| v.as_i32()).unwrap_or(0);
    let max_bytes_total = req.get("MaxBytes").map(|v| v.as_i32()).unwrap_or(i32::MAX).max(0) as usize;
    let max_wait_ms = req.get("MaxWaitMs").map(|v| v.as_i32()).unwrap_or(500).clamp(0, 60_000);

    // member 机会性本地注册（B5 多节点）：数据面不做协调器侧成员反查
    // （跨节点反查在陈旧路由窗口会造成 25 循环）；成员权威在协调器，
    // 数据 leader 的视图是机会性副本
    if session_epoch != -1 {
        crate::share_group::ensure_member_local(&group, &member_id);
    }

    tracing::debug!(group = %group, %member_id, session_epoch, "share_fetch: enter");
    // 状态新鲜度门（B5 failover）：拓扑变化后重放完成前拒绝（retriable 6，
    // 客户端重试）——否则以陈旧游标服务 = accepted 记录重投
    let fp = crate::share_group::routes_fingerprint(&ctx.routes());
    let fresh = crate::share_group::share_state_fresh(fp);
    let mut responses: Vec<Value> = Vec::new();
    if session_epoch != -1 {
        // 会话结算：服务集 = 会话注册分区（增量请求仅带变化分区）
        let mut req_topics: Vec<(u128, i32, i32)> = Vec::new();
        if let Some(Value::Array(topics)) = req.get("Topics") {
            for t in topics {
                let Value::Struct(ts) = t else { continue };
                let topic_id = ts.get("TopicId").map(|v| v.as_uuid()).unwrap_or(0);
                if let Some(Value::Array(parts)) = ts.get("Partitions") {
                    for p in parts {
                        let Value::Struct(ps) = p else { continue };
                        let index = ps.get("PartitionIndex").map(|v| v.as_i32()).unwrap_or(0);
                        let pmax = ps.get("PartitionMaxBytes").map(|v| v.as_i32()).unwrap_or(1024).max(0) as usize;
                        req_topics.push((topic_id, index, pmax as i32));
                    }
                }
            }
        }
        let mut forgotten_removals: Vec<(u128, i32)> = Vec::new();
        if let Some(Value::Array(ft)) = req.get("ForgottenTopicsData") {
            for t in ft {
                let Value::Struct(ts) = t else { continue };
                let topic_id = ts.get("TopicId").map(|v| v.as_uuid()).unwrap_or(0);
                if let Some(Value::Array(parts)) = ts.get("Partitions") {
                    for p in parts {
                        forgotten_removals.push((topic_id, p.as_i32()));
                    }
                }
            }
        }
        let Some(serve_set) = crate::share_group::session_register(&group, &member_id, session_epoch, &req_topics, &forgotten_removals) else {
            tracing::warn!(group = %group, %member_id, "share fetch for unknown member (member lives on group coordinator node; multi-node share ops require per-partition share state — B5/块 b3-e)");
            return s([
                ("ThrottleTimeMs", Value::I32(0)),
                ("ErrorCode", Value::I16(25)), // UNKNOWN_MEMBER_ID
                ("ErrorMessage", Value::str("Unknown member id")),
                ("AcquisitionLockTimeoutMs", Value::I32(30_000)),
                ("Responses", Value::Array(vec![])),
                ("NodeEndpoints", Value::Array(vec![])),
            ]);
        };
        // 按 topic 分组服务（KIP-932：会话注册集内逐分区交付；遗忘分区在
        // session_register 的增量并入面里不处理——遗忘经 ForgottenTopicsData
        // 需删除注册项，spike 面暂由全量 fetch（epoch 0）重建覆盖）
        let mut by_topic: BTreeMap<u128, Vec<Value>> = BTreeMap::new();
        for (tid, index, pmax) in serve_set {
            if !fresh {
                by_topic.entry(tid).or_default().push(s([
                    ("PartitionIndex", Value::I32(index)),
                    ("ErrorCode", Value::I16(6)), // NOT_LEADER_OR_FOLLOWER（retriable）
                    ("ErrorMessage", Value::str("share state replaying after failover")),
                    ("AcknowledgeErrorCode", Value::I16(0)),
                    ("Records", Value::Null),
                ]));
                continue;
            }
            // 分配面过滤（会话注册面 ≠ 分配面：成员集变化后，会话中旧分区
            // 不再服务——分配由 rotation 权威决定）
            // 分配过滤已移除（B5 多节点）：客户端按协调器分配向各分区
            // leader 发 fetch——本地 rotation 视图在多节点下非权威
            if forgotten_removals.contains(&(tid, index)) {
                continue;
            }
            let topic_name = ctx.routes().name_for(tid).unwrap_or_default();
            // 分区 leader 门：非 leader 节点不服务（客户端经 metadata 重路由）
            if !partition_is_led_locally(ctx, &topic_name, index) {
                by_topic.entry(tid).or_default().push(s([
                    ("PartitionIndex", Value::I32(index)),
                    ("ErrorCode", Value::I16(6)), // NOT_LEADER_OR_FOLLOWER
                    ("ErrorMessage", Value::str("not leader for partition")),
                    ("AcknowledgeErrorCode", Value::I16(0)),
                    ("Records", Value::Null),
                ]));
                continue;
            }
            let entry = serve_share_partition(
                ctx, &group, &member_id, tid, &topic_name, index,
                (pmax as usize).min(max_bytes_total.max(1)), max_wait_ms,
            ).await;
            by_topic.entry(tid).or_default().push(entry);
        }
        for (tid, parts) in by_topic {
            tracing::debug!(group = %group, %member_id, responses = parts.len(), "share_fetch: served");
            responses.push(s([
                ("TopicId", Value::Uuid(tid)),
                ("Partitions", Value::Array(parts)),
            ]));
        }
    } else {
        // FINAL_EPOCH：关闭会话，释放该 member 全部在途
        crate::share_group::release_member(&group, &member_id);
    }
    s([
        ("ThrottleTimeMs", Value::I32(0)),
        ("ErrorCode", Value::I16(0)),
        ("ErrorMessage", Value::Null),
        ("AcquisitionLockTimeoutMs", Value::I32(30_000)),
        ("Responses", Value::Array(responses)),
        ("NodeEndpoints", Value::Array(vec![])),
    ])
}

/// 单分区交付：游标起读 + acquire + AcquiredRecords 回填
async fn serve_share_partition(
    ctx: &crate::handlers::Ctx,
    group: &str,
    member: &str,
    topic_id: u128,
    topic_name: &str,
    index: i32,
    max_bytes: usize,
    max_wait_ms: i32,
) -> Value {
    if topic_name.is_empty() {
        return s([
            ("PartitionIndex", Value::I32(index)),
            ("ErrorCode", Value::I16(3)), // UNKNOWN_TOPIC_OR_PARTITION
            ("ErrorMessage", Value::Null),
            ("AcknowledgeErrorCode", Value::I16(0)),
            ("AcknowledgeErrorMessage", Value::Null),
            ("CurrentLeader", s([("LeaderId", Value::I32(-1)), ("LeaderEpoch", Value::I32(-1))])),
            ("Records", Value::Bytes(Bytes::new())),
            ("AcquiredRecords", Value::Array(vec![])),
        ]);
    }
    let from = crate::share_group::deliverable_from(group, member, topic_id, index);
    tracing::debug!(group, member, topic = %topic_name, index, from, "share fetch serving");
    // 路由 + 读（与普通 fetch 同面；isolation 恒 RU——KIP-932 不支持事务读取）
    // routes guard 不跨 await：先取 leader/tx 克隆再发命令
    let (tx, leader, epoch) = {
        let routes = ctx.routes();
        match routes.find(topic_name, index) {
            None => return empty_part(index, 3),
            Some(r) => (r.tx.clone(), r.leader, r.epoch),
        }
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(max_wait_ms.max(0) as u64);
    let (txr, rx) = tokio::sync::oneshot::channel();
    if tx
        .send(crate::partition::PartitionCmd::Fetch {
            offset: from,
            max_bytes: max_bytes.min(1 << 20),
            deadline,
            isolation: crate::partition::Isolation::ReadUncommitted,
            reply: txr,
        })
        .await
        .is_err()
    {
        tracing::warn!(group, member, topic = %topic_name, index, "share fetch: local actor unavailable");
        return empty_part(index, 15);
    }
    let out = rx.await.unwrap_or_else(|_| crate::partition::FetchOutcome::err(
        basalt_storage::error::StorageError::Other("share fetch dropped".into()), -1));
    tracing::debug!(
        group, member, topic = %topic_name, index, from,
        hw = out.result.as_ref().map(|r| r.high_watermark).unwrap_or(-1),
        bytes = out.result.as_ref().map(|r| r.data.len()).unwrap_or(0),
        error = out.error.as_ref().map(|e| e.to_string()).unwrap_or_default(),
        "share fetch: read done"
    );
    if let Some(e) = &out.error {
        tracing::warn!(group, member, topic = %topic_name, index, error = %e, "share fetch read error");
        let code = crate::handlers::storage_error_code(e);
        return s([
            ("PartitionIndex", Value::I32(index)),
            ("ErrorCode", Value::I16(code as i16)),
            ("ErrorMessage", Value::Null),
            ("AcknowledgeErrorCode", Value::I16(0)),
            ("AcknowledgeErrorMessage", Value::Null),
            ("CurrentLeader", s([("LeaderId", Value::I32(-1)), ("LeaderEpoch", Value::I32(-1))])),
            ("Records", Value::Bytes(Bytes::new())),
            ("AcquiredRecords", Value::Array(vec![])),
        ]);
    }
    let Some(r) = out.result else {
        return empty_part(index, 1); // OFFSET_OUT_OF_RANGE
    };
    tracing::debug!(group, member, topic = %topic_name, index, from, hw = r.high_watermark, bytes = r.data.len(), "share fetch read");
    let data = r.data;
    if data.is_empty() {
        return empty_part(index, 0);
    }
    // 批边界解析 → 逐批切片交付（B5）：仅交付 ≥ 游标且未归档的记录
    // （服务端切片——整批透传会把 accepted 前缀重新暴露，failover e2e 实证）
    let archived = crate::share_group::archived_ranges(group, topic_id, index);
    let is_archived_range = |f: i64, l: i64| archived.iter().any(|(af, al)| *af <= f && l <= *al);
    let mut acquired: Vec<Value> = Vec::new();
    let mut out_records = BytesMut::new();
    {
        let mut pos = 0usize;
        while let Some(h) = basalt_record::BatchHeader::parse(&data[pos..]) {
            let total = h.total_len();
            if total == 0 || pos + total > data.len() { break; }
            let b_first = h.base_offset;
            let b_last = h.base_offset + h.record_count as i64 - 1;
            if !is_archived_range(b_first, b_last + 1) {
                let (sliced, runs) = slice_share_batch(
                    &data[pos..pos + total], b_first, b_last, from, &archived,
                );
                for (f, l) in &runs {
                    let count = crate::share_group::acquire(group, member, topic_id, index, *f, *l);
                    acquired.push(s([
                        ("FirstOffset", Value::I64(*f)),
                        ("LastOffset", Value::I64(*l)),
                        ("DeliveryCount", Value::I16(count)),
                    ]));
                }
                out_records.extend_from_slice(&sliced);
            }
            pos += total;
        }
    }
    let out_data = out_records.freeze();
    if out_data.is_empty() {
        return empty_part(index, 0);
    }
    s([
        ("PartitionIndex", Value::I32(index)),
        ("ErrorCode", Value::I16(0)),
        ("ErrorMessage", Value::Null),
        ("AcknowledgeErrorCode", Value::I16(0)),
        ("AcknowledgeErrorMessage", Value::Null),
        ("CurrentLeader", s([("LeaderId", Value::I32(leader)), ("LeaderEpoch", Value::I32(epoch))])),
        ("Records", Value::Bytes(out_data)),
        ("AcquiredRecords", Value::Array(acquired)),
    ])
}


/// 批切片交付（B5）：解码 → 过滤（< 游标、已归档）→ 连续段重编码。
/// 返回 (重编码批字节, 交付区间列表)。KIP-932 交付由 broker 控制——
/// 普通 fetch 的「整批返回客户端过滤」契约不适用（failover e2e 实证：
/// 整批透传把 accepted 前缀重新暴露 = 游标回退假象）。压缩批不可解码，
/// 整批透传并整段 acquire（v1 限制）。
fn slice_share_batch(
    batch: &[u8],
    b_first: i64,
    b_last: i64,
    from: i64,
    archived: &[(i64, i64)],
) -> (Bytes, Vec<(i64, i64)>) {
    let mut out = BytesMut::new();
    let archived_off = |o: i64| archived.iter().any(|(af, al)| *af <= o && o < *al);
    let Some(recs) = basalt_record::decode_records(batch) else {
        tracing::warn!(
            b_first, b_last, from,
            compression = ?basalt_record::BatchHeader::parse(batch).map(|h| h.compression()),
            records = basalt_record::BatchHeader::parse(batch).map(|h| h.record_count),
            "share slice: batch undecodable; passthrough"
        );
        out.extend_from_slice(batch);
        return (out.freeze(), vec![(b_first, b_last)]);
    };
    // 交付记录收集（>= from 且未归档），按 offset 连续段分组
    let mut runs: Vec<Vec<basalt_record::Rec>> = Vec::new();
    let mut run_bounds: Vec<(i64, i64)> = Vec::new();
    let mut cur: Vec<basalt_record::Rec> = Vec::new();
    let mut cur_first: i64 = -1;
    let mut expect: i64 = -1;
    for (i, rec) in recs.iter().enumerate() {
        let o = b_first + i as i64;
        if o < from || archived_off(o) {
            if !cur.is_empty() {
                runs.push(std::mem::take(&mut cur));
                run_bounds.push((cur_first, expect));
            }
            continue;
        }
        if !cur.is_empty() && o != expect {
            runs.push(std::mem::take(&mut cur));
            run_bounds.push((cur_first, expect));
        }
        if cur.is_empty() {
            cur_first = o;
        }
        expect = o;
        cur.push(basalt_record::Rec {
            timestamp_delta: rec.timestamp_delta,
            key: rec.key.clone(),
            value: rec.value.clone(),
            headers: rec.headers.clone(),
        });
    }
    if !cur.is_empty() {
        runs.push(std::mem::take(&mut cur));
        run_bounds.push((cur_first, expect));
    }
    for (run, (first, last)) in runs.iter().zip(run_bounds.iter()) {
        basalt_record::encode_batch(*first, 0, 0, 0, -1, -1, -1, run, &mut out);
    }
    let pairs: Vec<(i64, i64)> = run_bounds.iter().map(|(f, l)| (*f, *l)).collect();
    (out.freeze(), pairs)
}

fn empty_part(index: i32, code: i16) -> Value {
    s([
        ("PartitionIndex", Value::I32(index)),
        ("ErrorCode", Value::I16(code)),
        ("ErrorMessage", Value::Null),
        ("AcknowledgeErrorCode", Value::I16(0)),
        ("AcknowledgeErrorMessage", Value::Null),
        ("CurrentLeader", s([("LeaderId", Value::I32(-1)), ("LeaderEpoch", Value::I32(-1))])),
        ("Records", Value::Bytes(Bytes::new())),
        ("AcquiredRecords", Value::Array(vec![])),
    ])
}



/// ShareAcknowledge(79) v1-2

/// 分区 leader 门（B5）：非本节点 leader 的分区不服务/不受理写（客户端
/// 按 metadata 重路由；陈旧元数据竞态下的写拒绝面）。
fn partition_is_led_locally(
    ctx: &crate::handlers::Ctx,
    topic_name: &str,
    index: i32,
) -> bool {
    ctx.routes()
        .find(topic_name, index)
        .map(|r| r.leader == ctx.node_id)
        .unwrap_or(false)
}

pub async fn share_acknowledge(req: &basalt_protocol::value::Struct, ctx: &crate::handlers::Ctx) -> Value {
    let _ = ctx;
    let group = req.get("GroupId").map(|v| v.as_str().to_string()).unwrap_or_default();
    let member_id = req.get("MemberId").map(|v| v.as_str().to_string()).unwrap_or_default();
    let session_epoch = req.get("ShareSessionEpoch").map(|v| v.as_i32()).unwrap_or(0);

    let mut responses: Vec<Value> = Vec::new();
    if session_epoch == -1 {
        crate::share_group::release_member(&group, &member_id);
    } else if let Some(Value::Array(topics)) = req.get("Topics") {
        for t in topics {
            let Value::Struct(ts) = t else { continue };
            let topic_id = ts.get("TopicId").map(|v| v.as_uuid()).unwrap_or(0);
            let mut parts_out: Vec<Value> = Vec::new();
            if let Some(Value::Array(parts)) = ts.get("Partitions") {
                for p in parts {
                    let Value::Struct(ps) = p else { continue };
                    let index = ps.get("PartitionIndex").map(|v| v.as_i32()).unwrap_or(0);
                    let mut batches: Vec<(i64, i64, Vec<i8>)> = Vec::new();
                    if let Some(Value::Array(bs)) = ps.get("AcknowledgementBatches") {
                        for b in bs {
                            let Value::Struct(bsx) = b else { continue };
                            let f = bsx.get("FirstOffset").map(|v| v.as_i64()).unwrap_or(0);
                            let l = bsx.get("LastOffset").map(|v| v.as_i64()).unwrap_or(0);
                            let tys: Vec<i8> = match bsx.get("AcknowledgeTypes") {
                                Some(Value::Array(a)) => a.iter().filter_map(|v| match v {
                                    Value::I8(x) => Some(*x),
                                    _ => None,
                                }).collect(),
                                _ => Vec::new(),
                            };
                            batches.push((f, l, tys));
                        }
                    }
                    // 分区 leader 门（B5）：ack 交付状态权威在分区 leader 节点
                    let topic_name = ctx.routes().name_for(topic_id).unwrap_or_default();
                    let mut err_code = if partition_is_led_locally(&ctx, &topic_name, index) { 0 } else { 6 };
                    let applied = if err_code == 0 {
                        crate::share_group::acknowledge(&group, &member_id, topic_id, index, &batches)
                    } else {
                        0
                    };
                    if applied > 0 {
                        err_code = 0;
                    } else if batches.is_empty() {
                        err_code = 0;
                    } else if err_code == 0 {
                        err_code = 78; // 无有效变更（未知 ack 类型等）
                    }
                    parts_out.push(s([
                        ("PartitionIndex", Value::I32(index)),
                        ("ErrorCode", Value::I16(err_code)),
                        ("ErrorMessage", Value::Null),
                        ("CurrentLeader", s([("LeaderId", Value::I32(-1)), ("LeaderEpoch", Value::I32(-1))])),
                    ]));
                }
            }
            responses.push(s([
                ("TopicId", Value::Uuid(topic_id)),
                ("Partitions", Value::Array(parts_out)),
            ]));
        }
    }
    s([
        ("ThrottleTimeMs", Value::I32(0)),
        ("ErrorCode", Value::I16(0)),
        ("ErrorMessage", Value::Null),
        ("AcquisitionLockTimeoutMs", Value::I32(30_000)),
        ("Responses", Value::Array(responses)),
        ("NodeEndpoints", Value::Array(vec![])),
    ])
}
