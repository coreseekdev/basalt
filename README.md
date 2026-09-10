# Basalt

> rock-solid streaming logs —— 用 Rust 复刻 Kafka 协议兼容的消息流平台。

**Basalt（玄武岩）**：熔岩流（stream）凝固成的岩石——寓意本项目的两个目标：像流水一样快的日志流，像岩石一样可靠不丢。

## 定位

- **产品形态**：生产级开源替代 + 商业产品雏形；v2 起提供 per-topic 存储模式（local / tiered / cloud-direct，ADR-11）。
- **Kafka 协议兼容（drop-in）**：librdkafka / franz-go / Apache kafka-clients 三家客户端跑通为验收基线；协议基线锁 Kafka 4.x feature levels（ADR-9）。
- **本地盘段式日志 + 内置共识元数据**：KRaft 式"单写者控制器 + 记录日志 + image 快照"（openraft 仅管元数据）；数据面为 ISR 形态复制——控制器指派 leader + epoch fencing + follower-pull 多数派 append + Kafka 三水位（HW/LSO/log start），per-partition Raft 为后评估项（ADR-10）。
- **测试即架构**：确定性仿真（turmoil）、存储故障注入、真实客户端对拍 report card、混沌长跑——测试机制与功能同步交付。

## 仓库结构

| 目录 | crate | 职责 |
|---|---|---|
| `protocol/` | basalt-protocol | Kafka 协议：官方 JSON → 构建期代码生成（sans-I/O） |
| `record/` | basalt-record | RecordBatch v2 编解码、压缩、CRC |
| `storage/` | basalt-storage | 段式日志引擎：segment/index/checkpoint + DiskIo 抽象 |
| `metadata/` | basalt-metadata | 控制器状态机、MetadataImage/Delta、broker 缓存 |
| `coordinator/` | basalt-coordinator | 消费组与事务协调器（record-based） |
| `testing/` | basalt-testing | 仿真 shim、集群 harness、验证器、兼容性对拍 |
| `server/` | basalt-server | 进程壳：网络、请求流水线、配置、可观测性 |
| `xtask/` | xtask | 构建任务：协议 codegen、兼容性报表、种子扫描 |

## 入口文档

- **[TASK.md](TASK.md)** —— 任务分解树（WBS）入口，当前进度与所有任务的验收标准。
- 知识库（上游 13 个项目 + 1 个跨领域参考的技术要点，位于 `../docs/`）：
  - [架构蓝图](../docs/10-rust-blueprint.md) —— 本仓库的架构依据
  - [测试机制设计](../docs/11-testing-strategy.md) —— 测试分层与验收不变式
  - [项目全景](../docs/00-overview.md) 及各参考项目要点

## 状态

M0 阶段（工程基建 + 协议与单机）。2026-09-08 定位与架构决策定稿（ADR-8~13），v1 范围 = M0–M2 + 幂等 producer（T-M3.1）。详见 [TASK.md](TASK.md)。

## License

Apache-2.0
