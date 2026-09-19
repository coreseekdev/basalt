//! 组状态同步（方案 B 块 b2）：__basalt_group_state 权威持久化。
//!
//! GroupManager 的提交经 state_out 通道到达本模块的转发任务：内部 topic
//! 分区 actor produce 路径（acks=all，Append=durable，TLA+ GroupStateHA
//! 确认模型）→ 应答 Ok 即已持久化。重放 = 从分区 offset 0 全量读、解码
//! 记录值（沿用旧 OffsetLog 记录布局）重建 offsets 视图。
//!
//! 多节点注记（B3 面）：本模块只绑定**本地**分区 actor——本节点是内部
//! topic 分区 leader（或副本）时成立；非副本节点的跨节点下沉走内部
//! RPC，B3 落地。

use basalt_coordinator::CommittedOffset;
use basalt_record::{decode_records, encode_batch, Rec};
use basalt_storage::log::AssignPolicy;
use bytes::{Bytes, BytesMut};
use tokio::sync::{mpsc, oneshot};

use crate::partition::{Isolation, PartitionCmd};

/// state_out 通道消息：(group, 批量提交, 持久化应答)。
pub type SinkMsg = (String, Vec<CommittedOffset>, oneshot::Sender<Result<(), String>>);

/// 启动组状态转发任务。返回 GroupManager.state_out 绑定的发送端。
pub fn spawn(part_tx: mpsc::Sender<PartitionCmd>) -> mpsc::Sender<SinkMsg> {
    let (tx, mut rx): (mpsc::Sender<SinkMsg>, mpsc::Receiver<SinkMsg>) = mpsc::channel(512);
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let (group, offsets, reply) = msg;
            let res = append_commits(&part_tx, &group, &offsets).await;
            let _ = reply.send(res);
        }
    });
    tx
}

async fn append_commits(
    part_tx: &mpsc::Sender<PartitionCmd>,
    group: &str,
    offsets: &[CommittedOffset],
) -> Result<(), String> {
    let now = crate::partition::now_ms();
    let recs: Vec<Rec> = offsets
        .iter()
        .map(|o| Rec {
            timestamp_delta: 0,
            key: Some(Bytes::copy_from_slice(group.as_bytes())),
            value: Some(Bytes::from(encode_commit_value(group, o))),
            headers: vec![],
        })
        .collect();
    let mut buf = BytesMut::new();
    // base_offset = -1：actor Assign 策略就地改写批头（produce 路径复用）
    encode_batch(-1, 0, now, 0, -1, -1, -1, &recs, &mut buf);
    let (rtx, rrx) = oneshot::channel();
    part_tx
        .send(PartitionCmd::Produce {
            batches: buf.freeze(),
            policy: AssignPolicy::Assign,
            acks: -1,
            reply: rtx,
        })
        .await
        .map_err(|_| "internal topic partition unavailable".to_string())?;
    let outcome = rrx.await.map_err(|_| "partition actor dropped".to_string())?;
    match outcome.error {
        Some(e) => Err(format!("internal topic produce: {e}")),
        None => Ok(()),
    }
}

/// 重放：offset 0 起读到 exhaustion（空回 = 读尽）。坏尾容忍——遇解析
/// 失败即停（与旧 OffsetLog replay 同语义）。安装进 GroupManager 前
/// 必须完成（BindState 原子携带状态 + 通道）。
pub async fn replay(
    part_tx: mpsc::Sender<PartitionCmd>,
) -> std::collections::HashMap<(String, String, i32), CommittedOffset> {
    let mut map = std::collections::HashMap::new();
    let mut pos = 0i64;
    loop {
        let (rtx, rrx) = oneshot::channel();
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(500);
        let cmd = PartitionCmd::Fetch {
            offset: pos,
            max_bytes: 1 << 20,
            deadline,
            isolation: Isolation::ReadUncommitted,
            reply: rtx,
        };
        if part_tx.send(cmd).await.is_err() {
            break;
        }
        let Ok(outcome) = rrx.await else { break };
        let Some(result) = outcome.result else { break };
        if result.data.is_empty() {
            break;
        }
        let data = result.data;
        let mut off = 0usize;
        let mut advanced = false;
        while off + basalt_record::RECORD_BATCH_HEADER_LEN <= data.len() {
            let Some(blen) = basalt_record::batch_len_at(&data[off..]) else { break };
            let batch = &data[off..off + blen];
            if let Some(recs) = decode_records(batch) {
                for r in recs {
                    let Some(v) = r.value.as_deref() else { continue };
                    if let Some((g, o)) = parse_commit_value(v) {
                        map.insert((g.clone(), o.topic.clone(), o.partition), o);
                    }
                }
            }
            // 游标推进 = 批末 offset + 1（头内 base + lastOffsetDelta）
            if let Some(h) = basalt_record::BatchHeader::parse(batch) {
                pos = h.base_offset + i64::from(h.last_offset_delta) + 1;
                advanced = true;
            }
            off += blen;
        }
        if !advanced || pos >= result.high_watermark {
            break;
        }
    }
    tracing::info!(entries = map.len(), "group state replayed from internal topic");
    map
}

// ---------- 记录值布局（与旧 OffsetLog 逐字节同型：BE + i16 长度前缀串） ----------

fn put_str(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as i16).to_be_bytes());
    buf.extend_from_slice(s.as_bytes());
}

fn encode_commit_value(group: &str, o: &CommittedOffset) -> Vec<u8> {
    let mut v = Vec::with_capacity(64);
    put_str(&mut v, group);
    put_str(&mut v, &o.topic);
    v.extend_from_slice(&o.partition.to_be_bytes());
    v.extend_from_slice(&o.offset.to_be_bytes());
    put_str(&mut v, &o.metadata);
    v.extend_from_slice(&o.commit_ts.to_be_bytes());
    v
}

fn parse_commit_value(mut v: &[u8]) -> Option<(String, CommittedOffset)> {
    let rd_i16 = |v: &mut &[u8]| -> Option<i16> {
        if v.len() < 2 {
            return None;
        }
        let x = i16::from_be_bytes([v[0], v[1]]);
        *v = &v[2..];
        Some(x)
    };
    let rd_i32 = |v: &mut &[u8]| -> Option<i32> {
        if v.len() < 4 {
            return None;
        }
        let x = i32::from_be_bytes(v[..4].try_into().ok()?);
        *v = &v[4..];
        Some(x)
    };
    let rd_i64 = |v: &mut &[u8]| -> Option<i64> {
        if v.len() < 8 {
            return None;
        }
        let x = i64::from_be_bytes(v[..8].try_into().ok()?);
        *v = &v[8..];
        Some(x)
    };
    let rd_str = |v: &mut &[u8]| -> Option<String> {
        let n = rd_i16(v)?;
        if n < 0 || v.len() < n as usize {
            return None;
        }
        let s = String::from_utf8_lossy(&v[..n as usize]).into_owned();
        *v = &v[n as usize..];
        Some(s)
    };
    let group = rd_str(&mut v)?;
    let topic = rd_str(&mut v)?;
    let partition = rd_i32(&mut v)?;
    let offset = rd_i64(&mut v)?;
    let metadata = rd_str(&mut v)?;
    let commit_ts = rd_i64(&mut v)?;
    Some((
        group,
        CommittedOffset { topic, partition, offset, metadata, commit_ts },
    ))
}

// ---------- share 组事件（B4：__share_group_state） ----------

/// share ack 事件值布局：[group:S][tid:16B][part:i32][first:i64][last:i64][ty:i8]
fn encode_share_event(e: &crate::share_group::ShareEvent) -> Vec<u8> {
    let (group, tid, part, first, last, ty) = e;
    let mut v = Vec::with_capacity(48);
    put_str(&mut v, group);
    v.extend_from_slice(&tid.to_be_bytes());
    v.extend_from_slice(&part.to_be_bytes());
    v.extend_from_slice(&first.to_be_bytes());
    v.extend_from_slice(&last.to_be_bytes());
    v.push(*ty as u8);
    v
}

fn parse_share_event(group: &str, mut v: &[u8]) -> Option<crate::share_group::ShareEvent> {
    let rd_i16 = |v: &mut &[u8]| -> Option<i16> {
        if v.len() < 2 {
            return None;
        }
        let x = i16::from_be_bytes([v[0], v[1]]);
        *v = &v[2..];
        Some(x)
    };
    let rd_i32 = |v: &mut &[u8]| -> Option<i32> {
        if v.len() < 4 {
            return None;
        }
        let x = i32::from_be_bytes(v[..4].try_into().ok()?);
        *v = &v[4..];
        Some(x)
    };
    let rd_i64 = |v: &mut &[u8]| -> Option<i64> {
        if v.len() < 8 {
            return None;
        }
        let x = i64::from_be_bytes(v[..8].try_into().ok()?);
        *v = &v[8..];
        Some(x)
    };
    let rd_str = |v: &mut &[u8]| -> Option<String> {
        let n = rd_i16(v)?;
        if n < 0 || v.len() < n as usize {
            return None;
        }
        let s = String::from_utf8_lossy(&v[..n as usize]).into_owned();
        *v = &v[n as usize..];
        Some(s)
    };
    let group = rd_str(&mut v)?;
    if group.is_empty() {
        return None;
    }
    let _ = group;
    let group = group.to_string();
    if v.len() < 16 + 4 + 8 + 8 + 1 {
        return None;
    }
    let tid = u128::from_be_bytes(v[..16].try_into().ok()?);
    v = &v[16..];
    let part = rd_i32(&mut v)?;
    let first = rd_i64(&mut v)?;
    let last = rd_i64(&mut v)?;
    let ty = *v.first()? as i8;
    Some((group, tid, part, first, last, ty))
}

/// share 事件转发任务：SHARE_SINK 通道 → 内部分区 actor produce。
/// 事件丢失的后果 = 重启后游标回退 → 重投递（KIP-932 至多一次语义容忍），
/// 因此通道满/写失败只记日志不阻塞 ack 应答。
pub fn spawn_share_sink(
    mut rx: tokio::sync::mpsc::Receiver<crate::share_group::ShareEvent>,
    part_tx: mpsc::Sender<PartitionCmd>,
) {
    tokio::spawn(async move {
        loop {
            let Some(event) = rx.recv().await else { break };
            let recs = vec![Rec {
                timestamp_delta: 0,
                key: Some(Bytes::copy_from_slice(event.0.as_bytes())),
                value: Some(Bytes::from(encode_share_event(&event))),
                headers: vec![],
            }];
            let mut buf = BytesMut::new();
            encode_batch(-1, 0, crate::partition::now_ms(), 0, -1, -1, -1, &recs, &mut buf);
            let (rtx, rrx) = oneshot::channel();
            if part_tx
                .send(PartitionCmd::Produce {
                    batches: buf.freeze(),
                    policy: AssignPolicy::Assign,
                    acks: -1,
                    reply: rtx,
                })
                .await
                .is_err()
            {
                continue;
            }
            if let Ok(outcome) = rrx.await {
                if let Some(e) = outcome.error {
                    tracing::warn!(error = %e, "share event persist failed");
                }
            }
        }
    });
}

/// share 事件重放：读全部分区数据解码事件列表。
pub async fn replay_share(part_tx: mpsc::Sender<PartitionCmd>) -> Vec<crate::share_group::ShareEvent> {
    let mut out = Vec::new();
    let mut pos = 0i64;
    loop {
        let (rtx, rrx) = oneshot::channel();
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(500);
        let cmd = PartitionCmd::Fetch {
            offset: pos,
            max_bytes: 1 << 20,
            deadline,
            isolation: Isolation::ReadUncommitted,
            reply: rtx,
        };
        if part_tx.send(cmd).await.is_err() {
            break;
        }
        let Ok(outcome) = rrx.await else { break };
        let Some(result) = outcome.result else { break };
        if result.data.is_empty() {
            break;
        }
        let data = result.data;
        let mut off = 0usize;
        let mut advanced = false;
        while off + basalt_record::RECORD_BATCH_HEADER_LEN <= data.len() {
            let Some(blen) = basalt_record::batch_len_at(&data[off..]) else { break };
            let batch = &data[off..off + blen];
            if let Some(recs) = decode_records(batch) {
                for r in recs {
                    let (Some(k), Some(v)) = (r.key.as_deref(), r.value.as_deref()) else { continue };
                    let Some(group) = String::from_utf8(k.to_vec()).ok() else { continue };
                    if let Some(e) = parse_share_event(&group, v) {
                        out.push(e);
                    }
                }
            }
            if let Some(h) = basalt_record::BatchHeader::parse(batch) {
                pos = h.base_offset + i64::from(h.last_offset_delta) + 1;
                advanced = true;
            }
            off += blen;
        }
        if !advanced || pos >= result.high_watermark {
            break;
        }
    }
    tracing::info!(events = out.len(), "share state replayed from internal topic");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_value_round_trip() {
        let o = CommittedOffset {
            topic: "t1".into(),
            partition: 3,
            offset: 4217,
            metadata: "meta-文字".into(),
            commit_ts: 1_789_900_000_000,
        };
        let bytes = encode_commit_value("grp-1", &o);
        let (g, got) = parse_commit_value(&bytes).expect("parse");
        assert_eq!(g, "grp-1");
        assert_eq!(got.topic, o.topic);
        assert_eq!(got.partition, o.partition);
        assert_eq!(got.offset, o.offset);
        assert_eq!(got.metadata, o.metadata);
        assert_eq!(got.commit_ts, o.commit_ts);
    }

    #[test]
    fn parse_commit_value_rejects_truncated() {
        let o = CommittedOffset {
            topic: "t".into(), partition: 0, offset: 1, metadata: String::new(), commit_ts: 0,
        };
        let bytes = encode_commit_value("g", &o);
        for cut in [0, 3, bytes.len() - 1] {
            assert!(parse_commit_value(&bytes[..cut]).is_none(), "cut={cut}");
        }
    }
}
