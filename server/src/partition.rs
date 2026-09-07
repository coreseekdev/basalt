//! 分区 actor：Log 的独占所有者（无锁、无 Arc）。
//!
//! - 组提交：drain 队列内全部 produce 聚合为一次 Log::append（一次写盘/一次 fsync 决策）；
//! - fetch 长轮询（purgatory）：无数据时挂起，append 后唤醒，超时空回。

use basalt_storage::disk::StdDisk;
use basalt_storage::log::{AssignPolicy, Log, LogOptions, ReadResult};
use basalt_storage::pool::BufferPool;
use bytes::Bytes;
use std::time::Instant;
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

pub enum PartitionCmd {
    Produce {
        batches: Bytes,
        policy: AssignPolicy,
        reply: oneshot::Sender<ProduceOutcome>,
    },
    Fetch {
        offset: i64,
        max_bytes: usize,
        deadline: Instant,
        reply: oneshot::Sender<FetchOutcome>,
    },
    ListOffsets {
        timestamp: i64,
        reply: oneshot::Sender<Result<(i64, i64), basalt_storage::StorageError>>,
    },
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
    log: Log<StdDisk>,
    pool: BufferPool,
    rx: mpsc::Receiver<PartitionCmd>,
    pending: Vec<PendingFetch>,
}

impl PartitionActor {
    pub fn spawn(
        name: String,
        index: i32,
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
        let actor = PartitionActor { name, index, log, pool: BufferPool::new(), rx, pending: Vec::new() };
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
            self.serve_pending();
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
                PartitionCmd::Produce { batches, policy, reply } => {
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
                    let _ = reply.send(outcome);
                }
                PartitionCmd::Fetch { offset, max_bytes, deadline, reply } => {
                    if offset < self.log.high_watermark {
                        let out = self.log.read(offset, max_bytes, &self.pool);
                        let _ = reply.send(FetchOutcome { result: out.ok() });
                    } else {
                        self.pending.push(PendingFetch { offset, deadline, reply });
                    }
                }
                PartitionCmd::ListOffsets { timestamp, reply } => {
                    let _ = reply.send(self.log.list_offset(timestamp));
                }
            }
        }
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
                let _ = p.reply.send(FetchOutcome { result: None });
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
