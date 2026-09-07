//! 请求/响应头规则（KIP-511）。
//!
//! - 请求头：消息 flexible → v2（= v1 字段 + tag section）；
//!   非 flexible → v1（无 tag section）。ApiVersions 特例：api_version ≥ 3
//!   才用 v2（客户端在协商前无从知晓 broker 的 flexible 起点）。
//! - 响应头：消息 flexible 且非 ApiVersions → v1（带 tag section）；否则 v0。
//! - ApiVersions 版本超限时：以 v0 语义回 UNSUPPORTED_VERSION（KIP-511）。

use crate::error::{ProtocolError, Result};
use crate::primitives::Reader;
use crate::value::{Struct, Value};
use bytes::{BufMut, Bytes, BytesMut};

#[derive(Debug)]
pub struct RequestHead {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<Box<str>>,
    pub header_version: i16,
}

/// 从帧首（已含完整帧缓冲）解析请求头。返回头与剩余 body 起点。
pub fn read_request_header(src: &Bytes, api_flexible_from: Option<i16>) -> Result<(RequestHead, usize)> {
    let mut r = Reader::new(src);
    let api_key = r.i16()?;
    let api_version = r.i16()?;
    let correlation_id = r.i32()?;

    // 头版本判定
    let header_version = if api_key == crate::api::key::API_VERSIONS {
        if api_version >= 3 {
            2
        } else {
            1
        }
    } else {
        let flexible = api_flexible_from.is_some_and(|f| api_version >= f);
        if flexible {
            2
        } else {
            1
        }
    };

    // client_id：legacy string（i32 len），v2 头仍是 legacy 编码（头的 flexible 只体现在 tag section）
    let mut client_id = None;
    if header_version >= 1 {
        let len = r.i32()?;
        if len >= 0 {
            let s = r.take(len as usize)?;
            client_id = Some(String::from_utf8_lossy(s).into());
        }
    }
    if header_version == 2 {
        let count = r.u8()?;
        for _ in 0..count {
            let _tag = r.uvarint()?;
            let size = r.uvarint()? as usize;
            r.skip(size)?;
        }
    }
    let head = RequestHead {
        api_key,
        api_version,
        correlation_id,
        client_id,
        header_version,
    };
    Ok((head, r.pos()))
}

/// 判定 flexible 与否（供调用方传给 codec）。
pub fn is_flexible(api_version: i16, flexible_from: Option<i16>) -> bool {
    flexible_from.is_some_and(|f| api_version >= f)
}

/// 写响应头（返回已写起点便于调用方回填帧长——帧长由外层负责）。
pub fn write_response_header(
    out: &mut BytesMut,
    correlation_id: i32,
    header_version: i16,
) {
    out.put_i32(correlation_id);
    if header_version == 1 {
        out.put_u8(0); // 空 tag section
    }
}

/// 响应头版本。
pub fn response_header_version(api_key: i16, message_flexible: bool) -> i16 {
    if api_key == crate::api::key::API_VERSIONS {
        0
    } else if message_flexible {
        1
    } else {
        0
    }
}

/// 解码请求头便捷封装，返回头 + body 剩余视图。
pub fn split_body(src: &Bytes, body_start: usize) -> Result<Bytes> {
    if body_start > src.len() {
        return Err(ProtocolError::UnexpectedEof { pos: src.len(), need: body_start - src.len() });
    }
    Ok(src.slice(body_start..))
}

/// 构造「版本不支持」回包（对 ApiVersions 之外的 API：按请求版本或
/// 我们可用的最近版本写一个只含顶层 error 的响应；上层须保证 schema 里有
/// 顶层 ErrorCode 字段，否则应直接断连）。
pub fn unsupported_version_body(
    fields: &[crate::plan::Node],
    version: i16,
    flexible: bool,
    error: crate::api::ErrorCode,
) -> Result<Bytes> {
    let mut st = Struct::new();
    // 常见顶层错误字段名
    let err_field = ["ErrorCode", "Error_code", "error_code"]
        .iter()
        .find(|n| fields.iter().any(|f| f.name.as_ref() == **n));
    match err_field {
        Some(name) => {
            st.set(name, Value::I16(error as i16));
            let mut out = BytesMut::new();
            crate::codec::encode_struct_fields(fields, version, flexible, &st, &mut out)?;
            Ok(out.freeze())
        }
        None => Err(ProtocolError::BadData(
            "cannot synthesize unsupported-version response (no top-level ErrorCode)".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_version_rules() {
        // ApiVersions: v3+ 用头 v2
        assert_eq!(read_request_header(&Bytes::from_static(&[
            0, 18, 0, 3, 0, 0, 0, 7, 0, 0, 0, 0, 0
        ]), Some(3)).unwrap().0.header_version, 2);
    }

    #[test]
    fn response_header_rules() {
        assert_eq!(response_header_version(crate::api::key::API_VERSIONS, true), 0);
        assert_eq!(response_header_version(crate::api::key::PRODUCE, true), 1);
        assert_eq!(response_header_version(crate::api::key::PRODUCE, false), 0);
    }
}
