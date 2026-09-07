//! 连接任务：帧读取 → 头解析 → 版本协商 → 分发 handler → 帧回写。
//!
//! 每连接独占读/写缓冲（BytesMut 复用 = 连接级内存池）。

use crate::handlers::{self, Ctx, FetchTarget, ProduceTarget};
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
    mut sock: tokio::net::TcpStream,
    peer: std::net::SocketAddr,
    ctx: Ctx,
) {
    let _ = peer;
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    loop {
        // 帧长
        let len = match read_frame_len(&mut sock, &mut read_buf).await {
            Ok(l) => l,
            Err(FramedError::Io(_)) => return, // 连接关闭
            Err(FramedError::TooLarge) => return,
        };
        if len == 0 {
            return;
        }
        let frame_bytes = match read_exact_bytes(&mut sock, &mut read_buf, len as usize).await {
            Ok(b) => b,
            Err(_) => return,
        };

        match dispatch(frame_bytes, &ctx).await {
            Ok(Some(resp)) => {
                if let Err(e) = sock.write_all(&resp).await {
                    tracing::debug!(error = %e, "write failed");
                    return;
                }
            }
            Ok(None) => {} // acks=0：无响应
            Err(DispatchError::UnknownApi) => {
                // Kafka 语义：直接断连（客户端会刷新 metadata 重试）
                tracing::warn!(peer = %peer, "unknown api key, closing");
                return;
            }
            Err(DispatchError::UnsupportedVersion(api_key, version)) => {
                // 尝试合成 unsupported 响应；失败则断连
                if let Some(b) = synth_unsupported(api_key, version) {
                    if sock.write_all(&b).await.is_err() {
                        return;
                    }
                } else {
                    return;
                }
            }
            Err(DispatchError::Protocol(e)) => {
                tracing::warn!(peer = %peer, error = %e, "protocol error, closing");
                return;
            }
        }
    }
}

enum FramedError {
    Io(#[allow(dead_code)] std::io::Error),
    TooLarge,
}

async fn read_frame_len(sock: &mut tokio::net::TcpStream, buf: &mut BytesMut) -> Result<i32, FramedError> {
    while buf.len() < 4 {
        sock.read_buf(buf).await.map_err(FramedError::Io)?;
    }
    let len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    buf.advance(4);
    if len < 0 || len > MAX_FRAME {
        return Err(FramedError::TooLarge);
    }
    Ok(len)
}

async fn read_exact_bytes(
    sock: &mut tokio::net::TcpStream,
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
    if frame_bytes.len() < 4 {
        return Err(ProtocolError::UnexpectedEof { pos: 0, need: 4 }.into());
    }
    let api_key = i16::from_be_bytes([frame_bytes[0], frame_bytes[1]]);
    let _api_version = i16::from_be_bytes([frame_bytes[2], frame_bytes[3]]);

    let Some(entry) = reg.api(api_key) else {
        return Err(DispatchError::UnknownApi);
    };
    let (head, body_start) =
        frame::read_request_header(&frame_bytes, entry.flexible_from).map_err(DispatchError::from)?;
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

    let body = frame::split_body(&frame_bytes, body_start)?;
    let req = codec::decode(&entry.request.fields, api_version, flexible, &body)?;

    tracing::trace!(api = api_key, version = api_version, corr = head.correlation_id, "request");

    let (resp_value, acks_none) = match api_key {
        key::API_VERSIONS => {
            let v = handlers::api_versions(&req, api_version);
            (v, false)
        }
        key::METADATA => (handlers::metadata(&req, ctx).await, false),
        key::PRODUCE => handle_produce(&req, api_version, ctx).await?,
        key::FETCH => (handlers::fetch(parse_fetch(&req, api_version)?, ctx).await, false),
        key::LIST_OFFSETS => (handlers::list_offsets(&req, ctx).await, false),
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
