//! 连接任务：帧读取 → 头解析 → 版本协商 → 分发 handler → 帧回写。
//!
//! 每连接独占读/写缓冲（BytesMut 复用 = 连接级内存池）。

use crate::handlers::{self, Ctx, FetchTarget, ProduceTarget};
use crate::handlers_groups;
use basalt_protocol::api::key;
use basalt_protocol::codec;
use basalt_protocol::error::ProtocolError;
use basalt_protocol::frame;
use basalt_protocol::registry::Registry;
use basalt_protocol::value::{Struct, Value};
use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MAX_FRAME: i32 = 100 * 1024 * 1024;

pub async fn serve_connection(
    sock: tokio::net::TcpStream,
    peer: std::net::SocketAddr,
    ctx: Ctx,
) {
    // 读半 + 写半分离：请求处理可并发（消除队头阻塞——长轮询 fetch 不再拖死同连接
    // 的 offset commit/heartbeat），响应经写通道串行化回写（保留单一写者）
    let (mut rd, mut wr) = sock.into_split();
    let (resp_tx, mut resp_rx) = tokio::sync::mpsc::channel::<Bytes>(256);

    // 写任务：唯一写者
    let writer = tokio::spawn(async move {
        while let Some(resp) = resp_rx.recv().await {
            if wr.write_all(&resp).await.is_err() {
                break;
            }
        }
    });

    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    loop {
        let len = match read_frame_len(&mut rd, &mut read_buf).await {
            Ok(l) => l,
            Err(FramedError::Io(_)) => break, // 连接关闭
            Err(FramedError::TooLarge) => break,
        };
        if len == 0 {
            break;
        }
        let frame_bytes = match read_exact_bytes(&mut rd, &mut read_buf, len as usize).await {
            Ok(b) => b,
            Err(_) => break,
        };
        // 慢消费者背压：响应通道满时暂停读（客户端不读就不给它读下一请求）
        let ctx = ctx.clone_for_request();
        let resp_tx = resp_tx.clone();
        tokio::spawn(async move {
            match dispatch(frame_bytes, &ctx).await {
                Ok(Some(resp)) => {
                    let _ = resp_tx.send(Bytes::from(resp)).await;
                }
                Ok(None) => {} // acks=0：无响应
                Err(DispatchError::UnknownApi) => {
                    tracing::warn!(peer = %peer, "unknown api key, closing");
                    // 关连接：丢弃写任务感知方式——发送空帧无意义，直接退出任务，
                    // 读循环在客户端超时后自行收敛。生产化时可引入 per-conn 关闭令牌。
                }
                Err(DispatchError::UnsupportedVersion(api_key, version)) => {
                    if let Some(b) = synth_unsupported(api_key, version) {
                        let _ = resp_tx.send(Bytes::from(b)).await;
                    }
                }
                Err(DispatchError::Protocol(e)) => {
                    tracing::warn!(peer = %peer, error = %e, "protocol error");
                }
            }
        });
    }
    drop(resp_tx);
    let _ = writer.await;
}

enum FramedError {
    Io(#[allow(dead_code)] std::io::Error),
    TooLarge,
}

async fn read_frame_len<R: tokio::io::AsyncRead + Unpin>(sock: &mut R, buf: &mut BytesMut) -> Result<i32, FramedError> {
    while buf.len() < 4 {
        let n = sock.read_buf(buf).await.map_err(FramedError::Io)?;
        if n == 0 {
            // EOF：连接已关闭——必须退出，否则空转烧满一个核
            return Err(FramedError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed",
            )));
        }
    }
    let len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    buf.advance(4);
    if len < 0 || len > MAX_FRAME {
        return Err(FramedError::TooLarge);
    }
    Ok(len)
}

async fn read_exact_bytes<R: tokio::io::AsyncRead + Unpin>(
    sock: &mut R,
    buf: &mut BytesMut,
    len: usize,
) -> std::io::Result<Bytes> {
    buf.reserve(len);
    while buf.len() < len {
        let n = sock.read_buf(buf).await?;
        if n == 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "conn closed"));
        }
    }
    Ok(buf.split_to(len).freeze())
}

enum DispatchError {
    UnknownApi,
    UnsupportedVersion(i16, i16),
    Protocol(ProtocolError),
}

impl From<ProtocolError> for DispatchError {
    fn from(e: ProtocolError) -> Self {
        DispatchError::Protocol(e)
    }
}

/// 处理一帧；返回完整响应帧（含长度前缀）。
async fn dispatch(frame_bytes: Bytes, ctx: &Ctx) -> Result<Option<Vec<u8>>, DispatchError> {
    let reg = Registry::global();
    // 预读 api_key/version 以定头版本（先偷看前 4 字节，读头时再正式解析）
    if frame_bytes.len() < 8 {
        // 头部最少 8 字节（api_key+api_version+correlation_id）
        return Err(ProtocolError::UnexpectedEof { pos: 0, need: 8 }.into());
    }
    let api_key = i16::from_be_bytes([frame_bytes[0], frame_bytes[1]]);
    let _api_version = i16::from_be_bytes([frame_bytes[2], frame_bytes[3]]);

    let Some(entry) = reg.api(api_key) else {
        return Err(DispatchError::UnknownApi);
    };
    let (head, body_start) = match frame::read_request_header(&frame_bytes, entry.flexible_from) {
        Ok(x) => x,
        Err(e) => {
            // KIP-511 探测语义：协商前的 ApiVersions 可能用旧头/无 tag section，
            // 解析失败也要以 v0 语义回 UNSUPPORTED_VERSION（corr 在固定偏移 4..8）
            if api_key == key::API_VERSIONS {
                return Ok(Some(synth_apiversions_unsupported(&frame_bytes)));
            }
            return Err(DispatchError::Protocol(e));
        }
    };
    let (api_key, api_version) = (head.api_key, head.api_version);

    let flexible = frame::is_flexible(api_version, entry.flexible_from);
    let supported = reg
        .advertised()
        .iter()
        .find(|(k, _, _, _)| *k == api_key)
        .map(|(_, lo, hi, _)| api_version >= *lo && api_version <= *hi)
        .unwrap_or(false);

    if !supported {
        return Err(DispatchError::UnsupportedVersion(api_key, api_version));
    }

    let body = match frame::split_body(&frame_bytes, body_start) {
        Ok(b) => b,
        Err(e) => return Err(DispatchError::Protocol(e)),
    };
    let req = match codec::decode(&entry.request.fields, api_version, flexible, &body) {
        Ok(r) => r,
        Err(e) => {
            // KIP-511：客户端在版本协商前可能用旧头发 ApiVersions 探测。
            // Kafka broker 语义：该请求解析失败 → 以 v0 格式回 UNSUPPORTED_VERSION，连接保活。
            if api_key == key::API_VERSIONS {
                return Ok(Some(synth_apiversions_unsupported(&frame_bytes)));
            }
            return Err(DispatchError::Protocol(e));
        }
    };

    tracing::trace!(api = api_key, version = api_version, corr = head.correlation_id, "request");

    let (resp_value, acks_none) = match api_key {
        key::API_VERSIONS => {
            let v = handlers::api_versions(&req, api_version);
            (v, false)
        }
        key::METADATA => (handlers::metadata(&req, api_version, ctx).await, false),
        key::PRODUCE => handle_produce(&req, api_version, ctx).await?,
        key::FETCH => (handlers::fetch(parse_fetch(&req, api_version)?, ctx).await, false),
        key::LIST_OFFSETS => (handlers::list_offsets(&req, ctx).await, false),
        key::FIND_COORDINATOR => (handlers_groups::find_coordinator(api_version, &req, ctx).await, false),
        key::JOIN_GROUP => (handlers_groups::join_group(&req, api_version, ctx).await, false),
        key::SYNC_GROUP => (handlers_groups::sync_group(&req, ctx).await, false),
        key::HEARTBEAT => (handlers_groups::heartbeat(&req, ctx).await, false),
        key::LEAVE_GROUP => (handlers_groups::leave_group(&req, ctx).await, false),
        key::OFFSET_COMMIT => (handlers_groups::offset_commit(&req, ctx).await, false),
        key::OFFSET_FETCH => (handlers_groups::offset_fetch(&req, ctx).await, false),
        key::CREATE_TOPICS => (handlers_groups::create_topics(&req, ctx).await, false),
        key::DELETE_TOPICS => (handlers_groups::delete_topics(&req, ctx).await, false),
        key::OFFSET_FOR_LEADER_EPOCH => (handlers::offset_for_leader_epoch(&req, ctx).await, false),
        key::DESCRIBE_GROUPS => (handlers_groups::describe_groups(&req, ctx).await, false),
        key::LIST_GROUPS => (handlers_groups::list_groups(&req, ctx).await, false),
        _ => {
            return Err(DispatchError::UnsupportedVersion(api_key, api_version));
        }
    };

    // 响应编码
    let mut out = BytesMut::new();
    let resp_header_v = frame::response_header_version(api_key, flexible);
    frame::write_response_header(&mut out, head.correlation_id, resp_header_v);
    codec::encode_struct_fields(&entry.response.fields, api_version, flexible, &resp_value_value(&resp_value), &mut out)?;
    let total = out.len() as i32;
    let mut framed = BytesMut::with_capacity(out.len() + 4);
    framed.extend_from_slice(&total.to_be_bytes());
    framed.extend_from_slice(&out);
    Ok(if acks_none { None } else { Some(framed.to_vec()) })
}

fn resp_value_value(v: &Value) -> &Struct {
    match v {
        Value::Struct(s) => s,
        _ => unreachable!("responses are structs"),
    }
}

async fn handle_produce(
    req: &Struct,
    version: i16,
    ctx: &Ctx,
) -> Result<(Value, bool), DispatchError> {
    let acks = req.get("Acks").map(|v| v.as_i16()).unwrap_or(1);
    let mut targets = Vec::new();
    if let Some(Value::Array(topics)) = req.get("TopicData") {
        for t in topics {
            let Value::Struct(ts) = t else { continue };
            let name = ts.get("Name").map(|x| x.as_str().to_string()).unwrap_or_default();
            let topic_id = ts.get("TopicId").map(|x| x.as_uuid()).unwrap_or(0);
            if let Some(Value::Array(parts)) = ts.get("PartitionData") {
                for p in parts {
                    let Value::Struct(ps) = p else { continue };
                    let index = ps.get("Index").map(|v| v.as_i32()).unwrap_or(0);
                    let records = ps.get("Records").cloned().unwrap_or(Value::Null);
                    let batches = match records {
                        Value::Bytes(b) => b,
                        Value::Null => Bytes::new(),
                        _ => Bytes::new(),
                    };
                    targets.push(ProduceTarget { topic: name.clone(), topic_id, partition: index, batches });
                }
            }
        }
    }
    let _ = version;
    let v = handlers::produce(version, acks, targets, ctx).await;
    Ok((v, acks == 0))
}

fn parse_fetch(req: &Struct, version: i16) -> Result<Vec<FetchTarget>, DispatchError> {
    let max_wait = req.get("MaxWaitMs").map(|v| v.as_i32()).unwrap_or(500);
    let min_bytes = req.get("MinBytes").map(|v| v.as_i32()).unwrap_or(1);
    let top_max = req.get("MaxBytes").map(|v| v.as_i32()).unwrap_or(i32::MAX).max(0) as usize;
    let mut out = Vec::new();
    if let Some(Value::Array(topics)) = req.get("Topics") {
        for t in topics {
            let Value::Struct(ts) = t else { continue };
            let name = ts.get("Topic").map(|x| x.as_str().to_string()).unwrap_or_default();
            let topic_id = ts.get("TopicId").map(|x| x.as_uuid()).unwrap_or(0);
            if let Some(Value::Array(parts)) = ts.get("Partitions") {
                for p in parts {
                    let Value::Struct(ps) = p else { continue };
                    let index = ps.get("Partition").map(|v| v.as_i32()).unwrap_or(0);
                    let offset = ps.get("FetchOffset").map(|v| v.as_i64()).unwrap_or(0);
                    let pmax = ps.get("PartitionMaxBytes").map(|v| v.as_i32()).unwrap_or(1024).max(0) as usize;
                    out.push(FetchTarget {
                        topic: name.clone(),
                        topic_id,
                        partition: index,
                        offset,
                        max_bytes: pmax.min(top_max.max(1)),
                        max_wait_ms: max_wait,
                        min_bytes,
                    });
                }
            }
        }
    }
    let _ = version;
    Ok(out)
}

/// 版本不支持时的兜底响应（ApiVersions 走 v0 特例语义）。
fn synth_unsupported(api_key: i16, version: i16) -> Option<Vec<u8>> {
    let reg = Registry::global();
    let entry = reg.api(api_key)?;
    let _flexible = frame::is_flexible(version, entry.flexible_from);
    let resp_version = if api_key == key::API_VERSIONS && version > entry.stable_max {
        0 // KIP-511：以 v0 回 UNSUPPORTED_VERSION
    } else {
        version.clamp(entry.valid.0, entry.stable_max)
    };
    let resp_flexible = frame::is_flexible(resp_version, entry.flexible_from);
    let mut st = Struct::new();
    st.set("ErrorCode", Value::I16(basalt_protocol::api::ErrorCode::UnsupportedVersion as i16));
    st.set("ThrottleTimeMs", Value::I32(0));
    let mut body = BytesMut::new();
    codec::encode_struct_fields(&entry.response.fields, resp_version, resp_flexible, &st, &mut body)
        .ok()?;
    let mut out = BytesMut::new();
    frame::write_response_header(&mut out, 0, frame::response_header_version(api_key, resp_flexible));
    out.extend_from_slice(&body);
    let total = out.len() as i32;
    let mut framed = BytesMut::with_capacity(out.len() + 4);
    framed.extend_from_slice(&total.to_be_bytes());
    framed.extend_from_slice(&out);
    Some(framed.to_vec())
}

/// ApiVersions 协商失败的固定应答：v0 格式 UNSUPPORTED_VERSION（KIP-511）。
fn synth_apiversions_unsupported(frame_bytes: &Bytes) -> Vec<u8> {
    let corr = i32::from_be_bytes([frame_bytes[4], frame_bytes[5], frame_bytes[6], frame_bytes[7]]);
    let mut out = BytesMut::new();
    frame::write_response_header(&mut out, corr, 0);
    out.extend_from_slice(&(basalt_protocol::api::ErrorCode::UnsupportedVersion as i16).to_be_bytes());
    out.extend_from_slice(&0i32.to_be_bytes()); // 空 ApiKeys
    let total = out.len() as i32;
    let mut framed = BytesMut::with_capacity(out.len() + 4);
    framed.extend_from_slice(&total.to_be_bytes());
    framed.extend_from_slice(&out);
    framed.to_vec()
}
