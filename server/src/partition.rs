//! 分区 actor：Log 的独占所有者（无锁、无 Arc）。
//!
//! - 组提交：drain 队列内全部 produce 聚合为一次 Log::append（一次写盘/一次 fsync 决策）；
//! - fetch 长轮询（purgatory）：无数据时挂起，append 后唤醒，超时空回。

use basalt_storage::disk::StdDisk;
use basalt_storage::log::{AssignPolicy, Log, LogOptions, ReadResult};
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
    pub error: Option<basalt_storage::StorageError>,
}

#[derive(Debug)]
pub struct FetchOutcome {
    pub result: Option<ReadResult>,
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
    /// follower 拉取（内部协议）：读取 + 记录 follower LEO + 推进 HW。
    FetchSlice {
        follower: i32,
        offset: i64,
        max_bytes: usize,
        reply: oneshot::Sender<SliceOutcome>,
    },
    ListOffsets {
        timestamp: i64,
        reply: oneshot::Sender<Result<(i64, i64), basalt_storage::StorageError>>,
    },
}

#[derive(Debug)]
pub struct SliceOutcome {
    pub error: Option<basalt_storage::StorageError>,
    pub high_watermark: i64,
    pub next_offset: i64,
    pub data: Bytes,
}

struct ParkedAck {
    base_offset: i64,
    last_offset: i64,
    log_append_time: i64,
    reply: oneshot::Sender<ProduceOutcome>,
    deadline: Instant,
}

fn reply_send_acked(p: ParkedAck) {
    let _ = p.reply.send(ProduceOutcome {
        base_offset: p.base_offset,
        last_offset: p.last_offset,
        log_append_time: p.log_append_time,
        error: None,
    });
}

struct PendingFetch {
    offset: i64,
    deadline: Instant,
    reply: oneshot::Sender<FetchOutcome>,
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
    follower_leos: HashMap<i32, i64>,
    parked_acks: Vec<ParkedAck>,
}

impl PartitionActor {
    pub fn spawn(
        name: String,
        index: i32,
        node_id: i32,
        dir: std::path::PathBuf,
        opts: LogOptions,
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
            self.process(group);
            self.release_acks();
            self.serve_pending();
            if !self.parked_acks.is_empty() {
                let now = Instant::now();
                let mut i = 0;
                while i < self.parked_acks.len() {
                    if self.parked_acks[i].deadline <= now {
                        let p = self.parked_acks.remove(i);
                        let _ = p.reply.send(ProduceOutcome {
                            base_offset: -1, last_offset: p.last_offset, log_append_time: now_ms(),
                            error: Some(basalt_storage::StorageError::Other("not enough replicas".into())),
                        });
                    } else {
                        i += 1;
                    }
                }
            }
            if let Some(next_deadline) = self.pending.iter().map(|p| p.deadline).min() {
                let now = Instant::now();
                if next_deadline > now {
                    match tokio::time::timeout(next_deadline - now, self.rx.recv()).await {
                        Ok(Some(cmd)) => {
                            let mut group = vec![cmd];
                            while let Ok(c) = self.rx.try_recv() {
                                group.push(c);
                            }
                            self.process(group);
                            self.serve_pending();
                        }
                        Ok(None) => break,
                        Err(_elapsed) => self.expire_pending(),
                    }
                } else {
                    self.expire_pending();
                }
            }
        }
        tracing::info!(topic = %self.name, partition = self.index, "partition actor stopped");
    }

    fn process(&mut self, group: Vec<PartitionCmd>) {
        for cmd in group {
            match cmd {
                PartitionCmd::SetRole { leader, epoch, replicas } => {
                    let changed = self.role != (if leader { Role::Leader } else { Role::Follower });
                    self.role = if leader { Role::Leader } else { Role::Follower };
                    self.epoch = epoch;
                    self.replicas = replicas;
                    if changed {
                        tracing::info!(topic=%self.name, partition=self.index, ?self.role, epoch, "role updated");
                    }
                }
                PartitionCmd::Produce { batches, policy, acks, reply } => {
                    if self.role != Role::Leader && policy == AssignPolicy::Assign {
                        // follower 拒写（epoch fencing 的客户端可见面）
                        let _ = reply.send(ProduceOutcome {
                            base_offset: -1, last_offset: -1, log_append_time: now_ms(),
                            error: Some(basalt_storage::StorageError::Other("not leader".into())),
                        });
                        continue;
                    }
                    let now = now_ms();
                    let outcome = match self.log.append(&batches, policy, now) {
                        Ok(r) => ProduceOutcome {
                            base_offset: r.base_offset,
                            last_offset: r.last_offset,
                            log_append_time: r.log_append_time,
                            error: None,
                        },
                        Err(e) => {
                            tracing::warn!(topic=%self.name, partition=self.index, error=%e, "produce append failed");
                            ProduceOutcome { base_offset: -1, last_offset: -1, log_append_time: now, error: Some(e) }
                        }
                    };
                    // acks=all 且有其他副本：停等 HW 追上（超时 10s → NotEnoughReplicas 语义由上层映射）
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
                    if self.role != Role::Leader {
                        let _ = reply.send(FetchOutcome { result: None });
                        continue;
                    }
                    if offset < self.log.high_watermark {
                        let out = self.log.read(offset, max_bytes, &self.pool);
                        let _ = reply.send(FetchOutcome { result: out.ok() });
                    } else {
                        self.pending.push(PendingFetch { offset, deadline, reply });
                    }
                }
                PartitionCmd::FetchSlice { follower, offset, max_bytes, reply } => {
                    // follower 的拉取起点即其 LEO：记录并推进 HW
                    if offset > 0 {
                        self.follower_leos.insert(follower, offset);
                    }
                    let out = if offset < self.log.next_offset {
                        self.log.read(offset, max_bytes, &self.pool).ok()
                    } else {
                        Some(basalt_storage::log::ReadResult {
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
            }
        }
    }

    /// HW = min(自身 LEO, 已上报 follower LEO)。
    fn advance_hw(&mut self) {
        if self.follower_leos.is_empty() {
            return;
        }
        let min_follower = self.follower_leos.values().copied().min().unwrap_or(i64::MAX);
        let new_hw = self.log.next_offset.min(min_follower);
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
                let _ = reply_send_acked(p);
            } else {
                i += 1;
            }
        }
    }

    #[allow(dead_code)]
    fn self_broker_id(&self) -> i32 {
        self.node_id
    }

    /// append 之后重查挂起的 fetch。
    fn serve_pending(&mut self) {
        let mut i = 0;
        while i < self.pending.len() {
            if self.pending[i].offset < self.log.high_watermark {
                let p = self.pending.remove(i);
                let out = self.log.read(p.offset, usize::MAX, &self.pool);
                let _ = p.reply.send(FetchOutcome { result: out.ok() });
            } else {
                i += 1;
            }
        }
    }

    fn expire_pending(&mut self) {
        let now = Instant::now();
        let mut i = 0;
        while i < self.pending.len() {
            if self.pending[i].deadline <= now {
                let p = self.pending.remove(i);
                // 长轮询超时 = 正常空回（带当前 HW），绝不能映射为 OffsetOutOfRange——
                // 否则客户端会重置到 earliest 无限重读
                let out = self.log.read(p.offset, usize::MAX, &self.pool);
                let result = match out {
                    Ok(r) => Some(r),
                    Err(_) => None, // 仅真正的越界错误才让上层映射 OffsetOutOfRange
                };
                let _ = p.reply.send(FetchOutcome { result });
            } else {
                i += 1;
            }
        }
    }
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
