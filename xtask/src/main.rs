//! 构建与工程任务（TASK.md T-0.3 / T-M1.5 / T-Q.3）。
//!
//! 计划子命令：
//! - `protocol-check`：diff 上游 Kafka 协议 JSON，报告新增/变更 API；
//! - `compat-report`：汇总 librdkafka / franz-go / kafka-clients 对拍结果（report card）；
//! - `seed-sweep`：确定性仿真种子扫描（nightly 1000+）。

fn main() {
    println!("xtask: protocol-check | compat-report | seed-sweep");
}
