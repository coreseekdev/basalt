//! 控制面元数据（TASK.md T-M2.1/T-M2.2，M0 期仅保留模块边界）。
//!
//! 架构（ADR-2，KRaft 范式）：
//! - 单写者控制器事件循环，变更以 record 追加到 controller log（openraft）；
//! - 内存态组织为 MetadataImage，增量以 Delta 广播给各 broker；
//! - broker 侧只持只读 MetadataCache。
//!
//! 参考：Kafka QuorumController / Redpanda controller_stm（docs/01 §4、docs/02 §4）。
