//! Basalt broker 进程壳（TASK.md T-M0.4/T-M0.5）。
//!
//! 组成：TCP accept/连接管理、有界请求队列、per-connection 响应通道、
//! 请求处理器（ApiVersions/Metadata/Produce/Fetch 先行）、配置与可观测性。

fn main() {
    println!("basalt-server {}", env!("CARGO_PKG_VERSION"));
}
