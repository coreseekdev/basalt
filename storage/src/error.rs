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

// StorageError 手动 Clone（io::Error 不可 Clone → 转字符串）
impl Clone for StorageError {
    fn clone(&self) -> Self {
        match self {
            StorageError::Io(e) => StorageError::Other(format!("io: {e}")),
            StorageError::CorruptBatch { path, pos, reason } => StorageError::CorruptBatch {
                path: path.clone(), pos: *pos, reason: reason.clone(),
            },
            other => other.clone(),
        }
    }
}
// 其他 variant 都是 Clone（String/i64/u64）—— 除 Io 外 derive 可用
// 但 Io 不行 → 上面的手动 impl
// 需要 Other 也 Clone：String ✓，OffsetOutOfRange(i64) ✓ 等
// 此处手动 Clone 覆盖全枚举
