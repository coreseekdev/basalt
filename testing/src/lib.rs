//! 测试基建（TASK.md T-M0.6 / T-Q.x；设计依据 docs/11-testing-strategy.md）。
//!
//! 规划内容（按交付顺序）：
//! - `shim`：网络/磁盘/时钟/随机源四边界的仿真实现（对应 storage::DiskIo）；
//! - `harness`：Cluster 事件队列 + step 循环（turmoil examples/cluster 模式）；
//! - `verifier`：不丢/不重/单调 offset/事务原子/rebalance 收敛断言器，
//!   与 verifiable producer/consumer（黑盒轨迹）共用同一套不变式定义；
//! - `compat`：librdkafka / franz-go / kafka-clients 对拍管线与白名单；
//! - `opfuzz`：DiskIo 层随机操作序列 fuzzer。
//!
//! 铁律：测试基建与功能同里程碑交付（TASK.md 使用规则）。
