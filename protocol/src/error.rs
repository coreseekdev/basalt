//! 协议层错误。

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("unexpected EOF at offset {pos} (need {need} more bytes)")]
    UnexpectedEof { pos: usize, need: usize },
    #[error("invalid varint at offset {pos}")]
    BadVarint { pos: usize },
    #[error("invalid data: {0}")]
    BadData(String),
    #[error("schema error: {0}")]
    Schema(String),
    #[error("unknown api key {api_key}")]
    UnknownApi { api_key: i16 },
    #[error("unsupported api version {api_key}-{version}")]
    UnsupportedVersion { api_key: i16, version: i16 },
}

pub type Result<T> = std::result::Result<T, ProtocolError>;
