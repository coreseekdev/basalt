//! 段式日志存储引擎（TASK.md T-M0.3）。
//!
//! 架构约束（ADR-7，不可后补）：
//! 1. 所有磁盘 IO 经 [`disk::DiskIo`]——生产 [`disk::StdDisk`]，
//!    仿真/故障注入实现由 basalt-testing 提供（torn write/ENOSPC/崩溃丢 pending）。
//! 2. write 与 sync 显式分离：ack 语义绑定 sync（[`FsyncSchedule`]），而非 write。
//!
//! 文件家族对齐 Kafka（生态工具可直读）：`{base_offset:020}.log/.index/.timeindex`。
//! 单写者纪律：`Log` 由 partition actor 独占持有（&mut 写 / & 读），内部无锁；
//! 磁盘句柄缓存是 DiskIo 实现的内部细节。

pub mod disk;
pub mod error;
pub mod index;
pub mod log;
pub mod pool;
pub mod segment;
pub mod sim_disk;

pub use error::{StorageError, Result};
pub use log::{Log, LogOptions, AppendResult, ReadResult, FsyncSchedule};

/// RecordBatch v2 魔术（与 record crate 常量一致，避免循环依赖）。
pub const MAGIC_V2: i8 = 2;
pub const BATCH_HEADER_LEN: usize = 61;
