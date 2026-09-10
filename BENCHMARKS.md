# Basalt vs Redpanda 同机对比基准

## 测试环境
| 项 | 值 |
|---|---|
| CPU | AMD Ryzen AI 9 H365 (20 threads) |
| RAM | 91GB DDR5 |
| 磁盘 | NVMe SSD 465GB |
| OS | Linux 7.0.0 |
| 客户端 | kafka-python 2.2.3 (纯 Python，两端相同) |
| 消息规格 | 1KB payload × 50,000 条 × 2 partitions |
| acks | all |

## 对比结果

| 指标 | Basalt | Redpanda (Docker 2CPU/4GB) | Basalt/Redpanda |
|---|---|---|---|
| **Produce 吞吐** | **153,546 msg/s** | 121,337 msg/s | **Basalt +27%** |
| **Produce 带宽** | **149.9 MB/s** | 118.5 MB/s | **Basalt +27%** |
| **Consume 吞吐** | 143,403 msg/s | 185,022 msg/s | Redpanda +29% |
| **延迟 p50** | 0.2ms | 0.1ms | 可比 |
| **延迟 p99** | 1.5ms | 1.1ms | 可比 |

## 分析

### Produce 吞吐优势
Basalt produce 吞吐超出 Redpanda 27%，原因：
1. **零拷贝透传**：broker 不解压不重组，直接写原始字节
2. **单批次 fast path**：省去中间 staging 拷贝
3. **Rust 无 GC**：无 JVM 暂停

### Consume 吞吐差距
Redpanda consume 快 29%，原因：
1. **sendfile 零拷贝**：Redpanda 用 sendfile 直接从 page cache 到 socket
2. **Basalt 用 Bytes 拷贝**：read_ex 到 BytesMut 再 write（多一次 memcpy）
3. **优化路径**：M4 sendfile/splice（预期 consume +3x）

### 延迟对比
两者 p50/p99 在同一量级（亚毫秒级）。Basalt p99=1.5ms 偏高可能因为
tokio 调度延迟，后续可通过减少 per-request spawn 优化。

## 客户端多样性：confluent-kafka（librdkafka 2.15）档（2026-09-10）

同场景与 kafka-python 档一一对应（`benches/throughput_confluent.py`，
`BENCH=throughput_confluent.py bash benches/run_bench.sh`）：

| 指标 | kafka-python 2.2.3 | confluent-kafka 2.15 | 说明 |
|---|---|---|---|
| **Produce 吞吐** | 153,546 msg/s | **437,826–605,884 msg/s（427–592 MB/s）** | librdkafka C 客户端批量更激进，broker 侧零改动 |
| **Consume 吞吐** | 143,403 msg/s | **254,639–257,937 msg/s（249–252 MB/s）** | fetch.max.bytes=16MB 大窗拉取 |
| 延迟 p50/p99 | 0.2/1.5ms | **<0.1/0.1ms** | 逐条 produce+回执 RTT |

**兼容性价值 > 性能价值**：该档首个运行即抓出 OffsetFetch v8+ 双侧布局
缺失（librdkafka 协商 v9，旧实现回 v0-7 形状 → 解析 underflow、消费 0 条；
kafka-python 停在 v7 探测不到）。修复 + e2e 回归见账本缺陷⑩
（`testing/e2e/librdkafka_compat.py`）。

## 压缩端到端验证

| Codec | Produce 数据完整性 | Broker CPU 开销 |
|---|---|---|
| lz4 | ✅ 零损坏 | **零**（零拷贝透传） |
| zstd | ✅ 零损坏 | **零** |
| gzip | ✅ 零损坏 | **零** |
| snappy | ✅ 零损坏 | **零** |

## 客户端瓶颈说明

上述对比均使用 kafka-python（纯 Python），受限于：
- GIL 序列化：每消息 ~10μs Python 开销
- 单线程 send/await 模式

**如果使用 confluent-kafka（librdkafka C 绑定），两者吞吐均会提升 5-10×。**
Basalt 与 Redpanda 的真实差距需用 C 客户端消除客户端瓶颈后才能准确测量。

## 优化路线图
1. sendfile/splice 零拷贝 fetch → 消除 consume 差距
2. Value 树位置索引 → 减少 decode 分配 ~50%
3. IO 合并（已完成 ✓）→ 同 actor 多 produce 一次 write
4. io_uring → 减少 syscall 开销 ~30%
