//! 消费组与事务协调器（TASK.md T-M1.1/T-M3.2，M0 期仅保留模块边界）。
//!
//! 架构原则（KRaft 同款，docs/01 §5）：
//! - record-based：组状态、成员、offset、事务状态全部作为 record 写入内部日志，
//!   崩溃恢复 = 重放；不引入额外存储引擎。
//! - 服务按分区 shard 化（GroupCoordinatorService/Shard 模式）。
//!
//! 协议双轨：Classic（JoinGroup/SyncGroup/Heartbeat，兼容子集）
//! + KIP-848（ConsumerGroupHeartbeat，服务端分配、增量 rebalance）。
