//! Basalt Kafka 协议层（sans-I/O）。
//!
//! 路线（ADR-1 修订）：上游 203 个官方协议 JSON 构建期内嵌，
//! 启动时一次性解析并编译为「编解码计划」，运行期按计划树做
//! 数据驱动的编解码——记录体（records）以 `bytes::Bytes` 零拷贝透传，
//! 仅元数据字段分配。
//!
//! 层级：
//! - [`api`]：API key 与错误码常量
//! - [`value`]：协议数据通用树（编解码产物/原料）
//! - [`schema`]：JSON 定义解析（容忍 // 注释）
//! - [`plan`]：编译后的字段计划树（版本门控 + tagged fields）
//! - [`codec`]：计划驱动的 encode/decode（sans-I/O：`Bytes` in/out）
//! - [`frame`]：请求/响应头规则（KIP-511）
//! - [`registry`]：内嵌定义 → 全量计划表（OnceLock 全局只读）

pub mod api;
pub mod codec;
pub mod error;
pub mod frame;
pub mod plan;
pub mod primitives;
pub mod registry;
pub mod schema;
pub mod value;

/// 协议 JSON 定义的落地目录（构建期内嵌，目录保留供 xtask diff 上游）。
pub const DEFINITION_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/definition");
