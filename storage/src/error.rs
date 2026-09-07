//! 存储层错误。

use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("corrupt batch at {path}@{pos}: {reason}")]
    CorruptBatch { path: String, pos: u64, reason: String },
    #[error("offset {0} out of range (log start..hw)")]
    OffsetOutOfRange(i64),
    #[error("not leader for this partition")]
    NotLeader,
    #[error("not enough replicas (isr below min.insync)")]
    NotEnoughReplicas,
    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, StorageError>;
