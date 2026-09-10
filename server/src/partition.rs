//! 分区 actor：Log 的独占所有者（无锁、无 Arc）。
//!
//! - 组提交：drain 队列内全部 produce 聚合为一次 Log::append（一次写盘/一次 fsync 决策）；
//! - fetch 长轮询（purgatory）：无数据时挂起，append 后唤醒，超时空回（带当前 HW）；
//! - HW 纪律：多副本时 HW 唯一推进入口 = follower LEO 上报（ISR 内、新鲜）；
//!   append 不自抬 HW；单副本分区 append 后 HW=LEO；
//! - epoch fencing：role != Leader 时拒绝 produce/fetch（复制拉取读 LEO，消费读 HW）。

use crate::config::ReplicaConfig;
use basalt_storage::disk::StdDisk;
use basalt_storage::error::StorageError;
use basalt_storage::log::{AssignPolicy, Log, LogOptions, ReadCap, ReadResult};
use basalt_storage::pool::BufferPool;
use bytes::Bytes;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub struct ProduceOutcome {
    pub base_offset: i64,
    pub last_offset: i64,
    pub log_append_time: i64,
    pub error: Option<StorageError>,
}

#[derive(Debug)]
pub struct FetchOutcome {
    pub result: Option<ReadResult>,
    pub error: Option<StorageError>,
}

#[derive(Debug)]
pub struct SliceOutcome {
    pub error: Option<StorageError>,
    pub high_watermark: i64,
    pub next_offset: i64,
    pub data: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Leader,
}

pub enum PartitionCmd {
    /// 元数据应用：设置本 actor 的角色与副本集（epoch fencing 依据）。
    SetRole { leader: bool, epoch: i32, replicas: Vec<i32> },
    Produce {
        batches: Bytes,
        policy: AssignPolicy,
        acks: i16,
        reply: oneshot::Sender<ProduceOutcome>,
    },
    Fetch {
        offset: i64,
        max_bytes: usize,
        deadline: Instant,
        reply: oneshot::Sender<FetchOutcome>,
    },
    /// follower 拉取（内部协议）：读取（可越 HW 至 LEO）+ 记录 follower LEO + 推进 HW。
    FetchSlice {
        follower: i32,
        offset: i64,
        max_bytes: usize,
        reply: oneshot::Sender<SliceOutcome>,
    },
    ListOffsets {
        timestamp: i64,
        reply: oneshot::Sender<Result<(i64, i64), StorageError>>,
    },
    /// 本地日志 LEO（follower 重启/追平校准用，区别于 HW）。
    LocalLeo {
        reply: oneshot::Sender<i64>,
    },
    /// 截断到 offset（分叉尾巴自愈）。
    TruncateTo {
        offset: i64,
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// OffsetForLeaderEpoch 语义：查指定 epoch 的 end offset。
    EndOffsetForEpoch {
        epoch: i32,
        reply: oneshot::Sender<(i32, i64)>,
    },
    /// 周期 retention 清理。
    #[allow(dead_code)]
    Retention {
        reply: oneshot::Sender<usize>,
    },
    /// DeleteRecords：设置 log_start_offset，删除之前的段。
    DeleteRecords {
        offset: i64,
        reply: oneshot::Sender<Result<i64, StorageError>>,
    },
}

struct PendingFetch {
    offset: i64,
    max_bytes: usize,
    deadline: Instant,
    reply: oneshot::Sender<FetchOutcome>,
}

struct ParkedAck {
    base_offset: i64,
    last_offset: i64,
    log_append_time: i64,
    reply: oneshot::Sender<ProduceOutcome>,
    deadline: Instant,
}

pub struct PartitionActor {
    #[allow(dead_code)]
    pub name: String,
    #[allow(dead_code)]
    pub index: i32,
    node_id: i32,
    log: Log<StdDisk>,
    // BufferPool 进程单例共享资源池：Arc 表达资源共享而非共享可变所有权（ADR-13）。
    #[allow(clippy::disallowed_types)]
    pool: std::sync::Arc<BufferPool>,
    rx: mpsc::Receiver<PartitionCmd>,
    pending: Vec<PendingFetch>,
    role: Role,
    epoch: i32,
    replicas: Vec<i32>,
    /// follower → (LEO, 最近一次上报时刻)
    follower_leos: HashMap<i32, (i64, Instant)>,
    parked_acks: Vec<ParkedAck>,
    /// batch_io 窗口内的 produce 应答（ADR-14）：flush 成功后才发放；
    /// flush 失败改发错误——杜绝"ack 而未写文件"（持久化点纪律）。
    deferred_produce: Vec<(oneshot::Sender<ProduceOutcome>, ProduceOutcome)>,
    repl: ReplicaConfig,
}

impl PartitionActor {
    #[allow(clippy::disallowed_types)] // pool 参数：进程单例共享资源池（ADR-13 豁免）
    pub fn spawn(
        name: String,
        index: i32,
        node_id: i32,
        dir: std::path::PathBuf,
        opts: LogOptions,
        repl: ReplicaConfig,
        pool: std::sync::Arc<BufferPool>,
    ) -> std::io::Result<mpsc::Sender<PartitionCmd>> {
        let (tx, rx) = mpsc::channel(1024);
        // Log::open 是阻塞 IO：专用线程打开后移交 actor task
        let (opened_tx, opened_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let log = Log::open(StdDisk::new(), dir, opts);
            let _ = opened_tx.send(log);
        });
        let log = match opened_rx.recv().expect("log open thread") {
            Ok(l) => l,
            Err(e) => return Err(std::io::Error::other(e.to_string())),
        };
        tracing::info!(topic = %name, partition = index, "partition actor started");
        let actor = PartitionActor {
            name,
            index,
            node_id,
            log,
            pool,
            rx,
            pending: Vec::new(),
            role: Role::Follower,
            epoch: 0,
            replicas: Vec::new(),
            follower_leos: HashMap::new(),
            parked_acks: Vec::new(),
            deferred_produce: Vec::new(),
            repl,
        };
        tokio::spawn(actor.run());
        Ok(tx)
    }

    async fn run(mut self) {
        loop {
            let Some(first) = self.rx.recv().await else { break };
            let mut group = vec![first];
            while let Ok(cmd) = self.rx.try_recv() {
                group.push(cmd);
            }
            // IO 批量合并：同轮 drain 的多个 produce 累积到 batch_staging，一次 write。
            // ADR-14：flush 在组内应答发放之前（batch_io 窗口的持久化写盘点）；
            // flush 失败 → 窗口内 produce 全部按存储错误应答。
            self.log.batch_io = true;
            self.process(group);
            let flush_result = self.log.end_batch_window();
            self.log.batch_io = false;
            if let Err(e) = &flush_result {
                tracing::error!(error = %e, "batch flush failed: 窗口内 produce 按错误应答");
            }
            self.settle_deferred_produce(flush_result.is_ok());
            self.on_deadline();
            self.serve_pending();
            // 唤醒时机：fetch 截止 / ack 停等超时，二者取最近
            let next_deadline = self
                .pending
                .iter()
                .map(|p| p.deadline)
                .chain(self.parked_acks.iter().map(|p| p.deadline))
                .min();
            if let Some(next_deadline) = next_deadline {
                let now = Instant::now();
                if next_deadline > now {
                    match tokio::time::timeout(next_deadline - now, self.rx.recv()).await {
                        Ok(Some(cmd)) => {
                            let mut group = vec![cmd];
                            while let Ok(c) = self.rx.try_recv() {
                                group.push(c);
                            }
                            self.log.batch_io = true;
                            self.process(group);
                            let flush_ok = self.log.end_batch_window().is_ok();
                            self.log.batch_io = false;
                            self.settle_deferred_produce(flush_ok);
                            self.on_deadline();
                            self.serve_pending();
                        }
                        Ok(None) => break,
                        Err(_elapsed) => self.on_deadline(),
                    }
                } else {
                    self.on_deadline();
                }
            }
        }
        tracing::info!(topic = %self.name, partition = self.index, "partition actor stopped");
    }

    fn on_deadline(&mut self) {
        let now = Instant::now();
        // fetch 超时：正常空回（带当前 HW），不可映射 OOR
        let mut i = 0;
        while i < self.pending.len() {
            if self.pending[i].deadline <= now {
                let p = self.pending.remove(i);
                let out = self.log.read_ex(p.offset, p.max_bytes, &self.pool, ReadCap::HighWatermark);
                let _ = p.reply.send(FetchOutcome { result: out.ok(), error: None });
            } else {
                i += 1;
            }
        }
        // ack 停等超时：NotEnoughReplicas
        let mut i = 0;
        while i < self.parked_acks.len() {
            if self.parked_acks[i].deadline <= now {
                let p = self.parked_acks.remove(i);
                let _ = p.reply.send(ProduceOutcome {
                    base_offset: -1,
                    last_offset: p.last_offset,
                    log_append_time: now_ms(),
                    error: Some(StorageError::NotEnoughReplicas),
                });
            } else {
                i += 1;
            }
        }
    }

    /// 新鲜 ISR 内的 follower 数（不含 leader）。
    fn fresh_followers(&self) -> Vec<i32> {
        let now = Instant::now();
        self.follower_leos
            .iter()
            .filter(|(_, (_, t))| now.duration_since(*t) <= self.repl.isr_lag)
            .map(|(k, _)| *k)
            .collect()
    }

    /// ADR-14：batch_io 窗口收口——flush 结果决定延后应答的最终状态。
    fn settle_deferred_produce(&mut self, flush_ok: bool) {
        let deferred = std::mem::take(&mut self.deferred_produce);
        for (reply, mut outcome) in deferred {
            if !flush_ok && outcome.error.is_none() {
                outcome.base_offset = -1;
                outcome.error = Some(StorageError::Other("batch flush failed".into()));
            }
            let _ = reply.send(outcome);
        }
    }

    fn process(&mut self, group: Vec<PartitionCmd>) {
        for cmd in group {
            match cmd {
                PartitionCmd::SetRole { leader, epoch, replicas } => {
                    let new_role = if leader { Role::Leader } else { Role::Follower };
                    let multi = replicas.len() > 1;
                    if new_role != self.role || epoch != self.epoch || multi != self.log.replicated {
                        tracing::info!(topic=%self.name, partition=self.index, ?new_role, epoch, replicas=?replicas, "role updated");
                        // 角色翻转：清空上一任期状态（LEO 上报/停等/挂起 fetch 全部失效）
                        self.follower_leos.clear();
                        for (_, outcome) in self.deferred_produce.drain(..) {
                            let _ = outcome;
                        }
                        self.deferred_produce.clear();
                        for p in self.parked_acks.drain(..) {
                            let _ = p.reply.send(ProduceOutcome {
                                base_offset: -1, last_offset: p.last_offset, log_append_time: now_ms(),
                                error: Some(StorageError::NotLeader),
                            });
                        }
                        for p in self.pending.drain(..) {
                            let _ = p.reply.send(FetchOutcome { result: None, error: Some(StorageError::NotLeader) });
                        }
                    }
                    self.role = new_role;
                    self.epoch = epoch;
                    self.replicas = replicas.clone();
                    self.log.set_replicated_internal(multi);
                    self.log.record_epoch(epoch);
                    if leader {
                        // 新 leader：以本地数据为准对外服务（HW=LEO）。
                        // 后续 follower 上报驱动 HW 前向推进；已确认数据不会少于此处。
                        self.log.high_watermark = self.log.next_offset;
                    } else {
                        // 新 follower：HW 归零，等从新 leader 拉齐后由上报驱动
                        self.log.high_watermark = 0;
                    }
                }
                PartitionCmd::Produce { batches, policy, acks, reply } => {
                    if self.role != Role::Leader && policy == AssignPolicy::Assign {
                        let _ = reply.send(ProduceOutcome {
                            base_offset: -1, last_offset: -1, log_append_time: now_ms(),
                            error: Some(StorageError::NotLeader),
                        });
                        continue;
                    }
                    // acks=all 前置检查：新鲜 ISR 数（含 leader）≥ min.insync
                    let fresh = self.fresh_followers().len() + 1;
                    if acks == -1 && self.log.replicated && (fresh as i32) < self.repl.min_insync {
                        let _ = reply.send(ProduceOutcome {
                            base_offset: -1, last_offset: -1, log_append_time: now_ms(),
                            error: Some(StorageError::NotEnoughReplicas),
                        });
                        continue;
                    }
                    let now = now_ms();
                    let m = crate::partition::metrics();
                    m.produce_requests.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let outcome = match self.log.append(&batches, policy, now) {
                        Ok(r) => {
                            m.messages_produced.fetch_add((r.last_offset - r.base_offset + 1) as u64, std::sync::atomic::Ordering::Relaxed);
                            m.bytes_produced.fetch_add(batches.len() as u64, std::sync::atomic::Ordering::Relaxed);
                            ProduceOutcome {
                                base_offset: r.base_offset,
                                last_offset: r.last_offset,
                                log_append_time: r.log_append_time,
                                error: None,
                            }
                        }
                        Err(e) => {
                            m.produce_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            tracing::warn!(topic=%self.name, partition=self.index, error=%e, "produce append failed");
                            ProduceOutcome { base_offset: -1, last_offset: -1, log_append_time: now, error: Some(e) }
                        }
                    };
                    // acks=all 且有其他副本：停等 HW 追上（follower 上报驱动；超时 NotEnoughReplicas）
                    let followers: Vec<i32> = self.replicas.iter().copied().filter(|r| *r != self.node_id).collect();
                    if acks == -1 && !followers.is_empty() && outcome.error.is_none() {
                        self.parked_acks.push(ParkedAck {
                            base_offset: outcome.base_offset,
                            last_offset: outcome.last_offset,
                            log_append_time: outcome.log_append_time,
                            reply,
                            deadline: Instant::now() + Duration::from_secs(10),
                        });
                        self.advance_hw();
                        self.release_acks();
                        continue;
                    }
                    // ADR-14：batch_io 窗口内应答延后到 flush 之后（flush 失败
                    // 改发错误）——杜绝"ack 而未写文件"。
                    if self.log.batch_io {
                        self.deferred_produce.push((reply, outcome));
                    } else {
                        let _ = reply.send(outcome);
                    }
                }
                PartitionCmd::Fetch { offset, max_bytes, deadline, reply } => {
                    crate::partition::metrics().fetch_requests.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if self.role != Role::Leader {
                        let _ = reply.send(FetchOutcome { result: None, error: Some(StorageError::NotLeader) });
                        continue;
                    }
                    if offset < self.log.high_watermark {
                        let out = self.log.read(offset, max_bytes, &self.pool);
                        match out {
                            Ok(r) => {
                                let _ = reply.send(FetchOutcome { result: Some(r), error: None });
                            }
                            Err(e) => {
                                let _ = reply.send(FetchOutcome { result: None, error: Some(e) });
                            }
                        }
                    } else {
                        self.pending.push(PendingFetch { offset, max_bytes, deadline, reply });
                    }
                }
                PartitionCmd::FetchSlice { follower, offset, max_bytes, reply } => {
                    crate::partition::metrics().fetch_requests.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    // fencing：仅 leader 服务复制拉取
                    if self.role != Role::Leader {
                        let _ = reply.send(SliceOutcome {
                            error: Some(StorageError::NotLeader),
                            high_watermark: -1, next_offset: -1, data: Bytes::new(),
                        });
                        continue;
                    }
                    // follower 的拉取起点即其 LEO：记录（>=0）并推进 HW
                    if offset >= 0 {
                        self.follower_leos.insert(follower, (offset, Instant::now()));
                    }
                    let out = if offset < self.log.next_offset {
                        self.log.read_ex(offset, max_bytes, &self.pool, ReadCap::LogEnd).ok()
                    } else {
                        Some(ReadResult {
                            data: Bytes::new(),
                            first_offset: offset,
                            high_watermark: self.log.high_watermark,
                            log_start_offset: self.log.log_start_offset,
                        })
                    };
                    let slice = SliceOutcome {
                        error: None,
                        high_watermark: self.log.high_watermark,
                        next_offset: self.log.next_offset,
                        data: out.map(|r| r.data).unwrap_or_else(Bytes::new),
                    };
                    let _ = reply.send(slice);
                    self.advance_hw();
                    self.release_acks();
                    self.serve_pending();
                }
                PartitionCmd::ListOffsets { timestamp, reply } => {
                    let _ = reply.send(self.log.list_offset(timestamp));
                }
                PartitionCmd::LocalLeo { reply } => {
                    let _ = reply.send(self.log.next_offset);
                }
                PartitionCmd::TruncateTo { offset, reply } => {
                    // 四轮 review P2-4 fencing：截断是 follower 侧分叉自愈动作。
                    // Leader 截自己的日志会让已 ack 的 offset 区间被后续
                    // produce 复用（offset 流回卷、同 offset 双 success）——
                    // 拒绝之；分叉自愈必须先经 SetRole Follower（控制器
                    // failover 路径保证）。窗口内 deferred/parked 不受影响。
                    if self.role == Role::Leader && offset < self.log.next_offset {
                        tracing::warn!(topic=%self.name, partition=self.index, offset, next=self.log.next_offset, "truncate rejected on leader");
                        let _ = reply.send(Err(StorageError::Other("truncate on leader rejected".into())));
                        continue;
                    }
                    // ADR-14：截断使窗口内 deferred produce（offset >= 截断点）
                    // 的数据失效——先按错误结算，再执行截断，杜绝
                    // "ack 成功但数据被截掉"（code review 三轮 P1-2）。
                    let deferred = std::mem::take(&mut self.deferred_produce);
                    for (reply, mut outcome) in deferred {
                        if outcome.error.is_none() && outcome.last_offset >= offset {
                            outcome.base_offset = -1;
                            outcome.error = Some(StorageError::Other("truncated by failover".into()));
                        }
                        let _ = reply.send(outcome);
                    }
                    // 四轮 review P1-2：parked_acks 中 last_offset >= offset 的
                    // 也按错误结算（防止 offset 复用别名放行）
                    let parked = std::mem::take(&mut self.parked_acks);
                    for p in parked {
                        if p.last_offset >= offset {
                            p.reply.send(ProduceOutcome {
                                base_offset: -1, last_offset: p.last_offset,
                                log_append_time: now_ms(),
                                error: Some(StorageError::Other("truncated by failover".into())),
                            }).ok();
                        } else {
                            self.parked_acks.push(p);
                        }
                    }
                    let _ = reply.send(self.log.truncate_to(offset));
                }
                PartitionCmd::DeleteRecords { offset, reply } => {
                    let _ = reply.send(self.log.delete_records(offset));
                }
                PartitionCmd::EndOffsetForEpoch { epoch, reply } => {
                    let _ = reply.send(self.log.end_offset_for_epoch(epoch));
                }
                PartitionCmd::Retention { reply } => {
                    let n = self.log.delete_old_segments();
                    let _ = reply.send(n);
                }
            }
        }
    }

    /// HW = min(自身 LEO, 新鲜 ISR 的 LEO)。只升不降（ISR 收缩不回退 HW）。
    fn advance_hw(&mut self) {
        if !self.log.replicated {
            return;
        }
        let min_follower = self.fresh_followers().len();
        if min_follower == 0 {
            return; // 无新鲜上报：HW 保持（等待上报或 ISR 超时收缩——POC 不自动收缩）
        }
        let min_leo = self
            .fresh_followers()
            .iter()
            .filter_map(|id| self.follower_leos.get(id).map(|(leo, _)| *leo))
            .min()
            .unwrap_or(i64::MAX);
        let new_hw = self.log.next_offset.min(min_leo);
        if new_hw > self.log.high_watermark {
            self.log.high_watermark = new_hw;
        }
    }

    /// HW 追上的停等 acks 放行。
    fn release_acks(&mut self) {
        let mut i = 0;
        while i < self.parked_acks.len() {
            if self.parked_acks[i].last_offset < self.log.high_watermark {
                let p = self.parked_acks.remove(i);
                let _ = p.reply.send(ProduceOutcome {
                    base_offset: p.base_offset,
                    last_offset: p.last_offset,
                    log_append_time: p.log_append_time,
                    error: None,
                });
            } else {
                i += 1;
            }
        }
    }

    /// append 之后重查挂起的 fetch。
    fn serve_pending(&mut self) {
        let mut i = 0;
        while i < self.pending.len() {
            if self.pending[i].offset < self.log.high_watermark {
                let p = self.pending.remove(i);
                let out = self.log.read_ex(p.offset, p.max_bytes, &self.pool, ReadCap::HighWatermark);
                let _ = p.reply.send(FetchOutcome { result: out.ok(), error: None });
            } else {
                i += 1;
            }
        }
    }
}

// ---- 全局指标（AtomicU64 无锁计数，Prometheus 格式暴露） ----
use std::sync::atomic::AtomicU64;

#[derive(Default)]
pub struct Metrics {
    pub messages_produced: AtomicU64,
    pub bytes_produced: AtomicU64,
    #[allow(dead_code)]
    pub messages_consumed: AtomicU64,
    #[allow(dead_code)]
    pub bytes_consumed: AtomicU64,
    pub produce_errors: AtomicU64,
    pub fetch_requests: AtomicU64,
    pub produce_requests: AtomicU64,
    #[allow(dead_code)]
    pub compressed_batches: AtomicU64,
}

static METRICS: std::sync::OnceLock<Metrics> = std::sync::OnceLock::new();

pub fn metrics() -> &'static Metrics {
    METRICS.get_or_init(Metrics::default)
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod truncate_fencing_tests {
    //! 四轮 review P2-4 回归：Leader 不得截自己的日志。
    //! 缺陷形态：TruncateTo 在 Leader 上生效 → 已 ack 的 offset 区间被
    //! 后续 produce 复用（offset 流回卷、同 offset 双 success）。

    use super::*;
    use basalt_record::{encode_batch, Rec};
    use basalt_storage::log::{FsyncSchedule, LogOptions};
    use bytes::{Bytes, BytesMut};

    fn batch_bytes(count: usize, tag: &str) -> Bytes {
        let recs: Vec<Rec> = (0..count)
            .map(|i| Rec {
                timestamp_delta: i as i64,
                key: Some(Bytes::from(format!("k{i}"))),
                value: Some(Bytes::from(format!("{tag}-{i}"))),
                headers: vec![],
            })
            .collect();
        let mut b = BytesMut::new();
        encode_batch(0, 0, 1000, 0, -1, -1, -1, &recs, &mut b);
        b.freeze()
    }

    #[tokio::test]
    async fn leader_rejects_truncate_no_offset_reuse() {
        let dir = std::env::temp_dir().join(format!(
            "basalt-truncfence-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let pool = std::sync::Arc::new(BufferPool::new());
        let tx = PartitionActor::spawn(
            "t".into(),
            0,
            0,
            dir.clone(),
            LogOptions { segment_max_bytes: 1 << 30, fsync: FsyncSchedule::Os, retention_ms: 0, retention_max_bytes: 0 },
            ReplicaConfig { min_insync: 1, isr_lag: Duration::from_millis(500) },
            pool,
        )
        .unwrap();

        // Leader 上任（单副本）
        tx.send(PartitionCmd::SetRole { leader: true, epoch: 1, replicas: vec![0] }).await.unwrap();

        // produce 5 批 ×2 记录 → offsets 0..10，全部 success
        for i in 0..5 {
            let (ptx, prx) = oneshot::channel();
            tx.send(PartitionCmd::Produce {
                batches: batch_bytes(2, &format!("b{i}")),
                policy: AssignPolicy::Assign,
                acks: 1,
                reply: ptx,
            })
            .await
            .unwrap();
            let o = prx.await.unwrap();
            assert!(o.error.is_none(), "produce {i} 失败：{:?}", o.error);
            assert_eq!(o.base_offset, i * 2);
        }

        // Leader 收到截断指令：必须被 fencing 拒绝（Err）
        let (ttx, trx) = oneshot::channel();
        tx.send(PartitionCmd::TruncateTo { offset: 4, reply: ttx }).await.unwrap();
        let r = trx.await.unwrap();
        assert!(r.is_err(), "Leader 上的 TruncateTo 必须被拒绝（P2-4 fencing）");

        // LEO 不动、后续 produce 接在 10 之后（offset 流不回卷）
        let (ltx, lrx) = oneshot::channel();
        tx.send(PartitionCmd::LocalLeo { reply: ltx }).await.unwrap();
        assert_eq!(lrx.await.unwrap(), 10, "被拒绝的截断不得改动 LEO");

        let (ptx, prx) = oneshot::channel();
        tx.send(PartitionCmd::Produce {
            batches: batch_bytes(2, "after"),
            policy: AssignPolicy::Assign,
            acks: 1,
            reply: ptx,
        })
        .await
        .unwrap();
        let o = prx.await.unwrap();
        assert!(o.error.is_none(), "{:?}", o.error);
        assert_eq!(o.base_offset, 10, "后续 produce 不得复用截断区 offset");

        drop(tx);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn follower_still_truncates_on_command() {
        // 对照组：Follower 角色的分叉自愈截断保持可用（探针分辨力自证）
        let dir = std::env::temp_dir().join(format!(
            "basalt-truncfence-f-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let pool = std::sync::Arc::new(BufferPool::new());
        let tx = PartitionActor::spawn(
            "t".into(),
            0,
            0,
            dir.clone(),
            LogOptions { segment_max_bytes: 1 << 30, fsync: FsyncSchedule::Os, retention_ms: 0, retention_max_bytes: 0 },
            ReplicaConfig { min_insync: 1, isr_lag: Duration::from_millis(500) },
            pool,
        )
        .unwrap();
        tx.send(PartitionCmd::SetRole { leader: true, epoch: 1, replicas: vec![0] }).await.unwrap();
        for i in 0..2 {
            let (ptx, prx) = oneshot::channel();
            tx.send(PartitionCmd::Produce {
                batches: batch_bytes(2, &format!("f{i}")),
                policy: AssignPolicy::Assign,
                acks: 1,
                reply: ptx,
            })
            .await
            .unwrap();
            assert_eq!(prx.await.unwrap().last_offset, i * 2 + 1);
        }

        // 降级为 Follower（分叉自愈前置条件）后截断：允许生效
        tx.send(PartitionCmd::SetRole { leader: false, epoch: 2, replicas: vec![0] }).await.unwrap();
        let (ttx, trx) = oneshot::channel();
        tx.send(PartitionCmd::TruncateTo { offset: 2, reply: ttx }).await.unwrap();
        let r = trx.await.unwrap();
        assert!(r.is_ok(), "Follower 的分叉自愈截断必须可用：{:?}", r.err());
        let (ltx, lrx) = oneshot::channel();
        tx.send(PartitionCmd::LocalLeo { reply: ltx }).await.unwrap();
        assert_eq!(lrx.await.unwrap(), 2);

        drop(tx);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
