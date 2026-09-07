//! 段式日志存储引擎（TASK.md T-M0.3）。
//!
//! 文件家族对齐 Kafka（便于生态工具读盘）：
//! `{base_offset:020}.log` / `.index` / `.timeindex` + producer snapshot
//! + recovery checkpoint + leader-epoch checkpoint。
//!
//! 两个硬性架构约束（docs/11-testing-strategy.md §3.1，不可后补）：
//! 1. 所有磁盘 IO 走 [`DiskIo`] 抽象——生产实现用真实文件系统，
//!    仿真/故障注入实现由 basalt-testing 提供；
//! 2. write 与 sync 是显式分离的操作：ack 语义绑定 sync，而非 write。

/// 磁盘 IO 抽象。方法集在 T-M0.3 定稿，当前为骨架。
///
/// 实现方约束：
/// - `sync_*` 必须对应真实 fsync（或仿真中的持久化边界）；
/// - 实现必须暴露注入点：torn write（按 block 部分落盘）、ENOSPC、
///   崩溃时丢弃未 sync 的 pending 写。
pub trait DiskIo {
    /// 错误类型由具体实现定义（内存仿真实现与真实 fs 实现共用错误面）。
    type Error;
}

#[cfg(test)]
mod tests {
    #[test]
    fn skeleton_compiles() {
        // 占位：确保 trait 当前可被引用（T-M0.3 起替换为真实测试）。
        let _ = std::string::String::from("DiskIo");
    }
}
