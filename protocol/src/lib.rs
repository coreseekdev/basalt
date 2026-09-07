//! Basalt Kafka 协议层（sans-I/O）。
//!
//! 职责（TASK.md T-0.3 / T-M0.2）：
//! - `build.rs` 读取 `definition/` 下 Kafka 官方协议 JSON，生成全部
//!   request/response 类型与编解码（含版本协商与 tagged fields）；
//! - 类型只做 `&[u8]` in/out，不做任何 IO，供 server 与未来 SDK 共用。
//!
//! 路线依据：Nisshi（官方 185 个 JSON + 代码生成）、Kafka 自身的
//! `clients/src/main/resources/common/message/*.json`；反例：手写解析。

/// 协议 JSON 定义的落地目录（T-0.3 供应商化后填充）。
pub const DEFINITION_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/definition");

#[cfg(test)]
mod tests {
    #[test]
    fn definition_dir_exists() {
        assert!(std::path::Path::new(super::DEFINITION_DIR).is_dir());
    }
}
