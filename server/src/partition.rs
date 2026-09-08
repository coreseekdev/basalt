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
    pool: BufferPool,
    rx: mpsc::Receiver<PartitionCmd>,
    pending: Vec<PendingFetch>,
    role: Role,
    epoch: i32,
    replicas: Vec<i32>,
    /// follower → (LEO, 最近一次上报时刻)
    follower_leos: HashMap<i32, (i64, Instant)>,
    parked_acks: Vec<ParkedAck>,
    repl: ReplicaConfig,
}

impl PartitionActor {
    pub fn spawn(
        name: String,
        index: i32,
        node_id: i32,
        dir: std::path::PathBuf,
        opts: LogOptions,
        repl: ReplicaConfig,
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
            pool: BufferPool::new(),
            rx,
            pending: Vec::new(),
            role: Role::Follower,
            epoch: 0,
            replicas: Vec::new(),
            follower_leos: HashMap::new(),
            parked_acks: Vec::new(),
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
            // IO 批量合并：同轮 drain 的多个 produce 累积到 batch_staging，一次 write
            self.log.batch_io = true;
            self.process(group);
            let _ = self.log.flush_batch();
            self.log.batch_io = false;
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
                            let _ = self.log.flush_batch();
                            self.log.batch_io = false;
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
                    let _ = reply.send(outcome);
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
