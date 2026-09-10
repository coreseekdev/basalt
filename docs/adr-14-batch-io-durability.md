# ADR-14：batch_io 模式下的持久化点定义

状态：已采纳（方案 a + SyncEach 窗口末 fsync）｜日期：2026-09-10｜
关联：ADR-6（fsync 调度）、ADR-7（DiskIo 边界）、账本 C7'/C13、性能报告 #4/#8

## 1. 背景与问题

存储写路径有两级缓冲（性能优化，BENCHMARKS 确认收益）：

```
produce ──append──→ batch_staging（内存，跨调用累积）
          │ 窗口末 flush_batch
          ▼
    段文件（page cache / 设备缓存）
          │ sync_file（fsync）
          ▼
    磁盘持久
```

`batch_io = true` 时 append 不写文件——字节留在**用户态内存** `batch_staging`。
而 `sync()` 只 fsync **文件里已有的字节**。两者组合下的时序缺陷：

```
produce ① ──append──→ staging（仅内存）
SyncEach sync() ──→ fsync 段文件（不含 staging 字节，无效）
broker ack ①          ←── 承诺已持久
─── crash ───
重启：staging 丢失 → 消息 ① 丢失（已 ack 不持久 = 持久性违约）
```

opfuzz batch_io 档 + code review 二轮探针（P0-2）实证。

## 2. 决策

**方案 (a)：drain 窗口边界 = 持久化写盘点，应答在写盘点之后。**

1. partition actor 的 drain 窗口末调用 `end_batch_window()`：
   flush_batch（排空 staging 到段文件）→ SyncEach 档补 fsync；
2. 窗口内 produce 的应答**延后**到 end_batch_window 成功后统一发放；
   失败则整组改发存储错误（不 ack）；
3. `sync()` 语义强化为**先排空 staging 再 fsync**（防调用顺序陷阱）；
4. O_DIRECT（M4）在同契约下实现：append = 设备写，sync = FLUSH CACHE。

## 3. 丢失窗口的诚实刻画（各调度 × acks）

| 组合 | ack 时刻保证 | 掉电丢失窗口 |
|---|---|---|
| batch_io + SyncEach | 已 fsync（窗口末） | **无**（fsync 覆盖全部 acked）|
| batch_io + Os | 已写入 page cache/设备缓存 | 同 Kafka acks=1：掉电可丢，靠副本 |
| 非 batch_io + SyncEach | 每追加组后 fsync | 无 |
| 非 batch_io + Os | page cache | 同上 |

acks=all 时任何组合下仍需 ISR 多数派持久（§3 故障模型 + C1 适用条款）。

## 4. 备选方案（否决理由）

- **(b) sync() 内排空**：语义等价于窗口末统一（排空+fsync 同点），但 sync 的
  签名需 `&mut self` 且调用点分散——不如把收口收敛为 actor 窗口末一个调用。
  采纳其防御性部分：`sync()` 保持排空语义（`&mut self`）。
- **(c) 禁止 batch_io × SyncEach**：损失 SyncEach 用户的批量合并收益；
  方案 (a) 下该组合语义自洽，无需禁令。

## 5. 验证映射

- opfuzz batch_io 档（20 种子）：crash 断言 `LEO ≥ synced_upto`
  （sync 排空后 synced_upto = LEO，窗口内 crash 丢未 flush 组，与契约一致）；
- ADR 定案后 batch_io 档转正（解除 ignore，已并入主力套件）；
- 四档全绿：clean 40 + chaos 20 + batch_io 20 + repro（提交 4db04bf/5fee773 链）。

## 6. 残留与后续

- SyncEach 档端到端持久性测试（produce→crash→consume 循环，k8s 部署档）
  归 C7 深水区/e2e 扩展；
- batch_io 档的性能 A/B（confluent-kafka 基准）归性能 backlog。
