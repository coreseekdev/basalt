# 协议定义（T-0.3 供应商化）

此目录存放从 Apache Kafka 上游复制的协议描述文件：

- 来源：`clients/src/main/resources/common/message/*.json`（约 185+ 个）
- 用途：`build.rs`（T-M0.2）据此生成全部 request/response 类型与编解码
- 版本管理：由 `xtask protocol-check` 跟踪上游增量，禁止手改 JSON

当前为空：T-0.3 任务导入后填充。
