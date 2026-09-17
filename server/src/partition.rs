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
use bytes::{Bytes, BytesMut};
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
    /// 最后稳定 offset（ADR-18 §4.2）：read_committed 的可见上界；
    /// 无开事务 = HW。
    pub last_stable_offset: i64,
}

impl FetchOutcome {
    fn err(e: StorageError, lso: i64) -> FetchOutcome {
        FetchOutcome { result: None, error: Some(e), last_stable_offset: lso }
    }
}

/// 消费隔离级（Fetch v4+ IsolationLevel）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Isolation {
    ReadUncommitted,
    ReadCommitted,
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
        isolation: Isolation,
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
    /// topic 删除：actor 退出（路由已移除，句柄/内存随任务结束回收；
    /// 数据目录留给 retention 清理）。
    Shutdown { reply: oneshot::Sender<()> },
    /// 本地日志 LEO（follower 重启/追平校准用，区别于 HW）。
    LocalLeo {
        reply: oneshot::Sender<i64>,
    },
    /// 就任拉齐（账本 ㉟）：meta 层从存活副本拉回缺失尾部后按原偏移落盘
    /// （append 以 next_offset 为基重写批头——拉取起点 = 本地 LEO，偏移一致）
    ReconcileAppend {
        batches: Bytes,
        reply: oneshot::Sender<bool>,
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
    /// 事务 marker 落盘（ADR-18 §4.1，coordinator 编排路径；块 a 先行——
    /// 内部注入可用）。应答语义 = 冻结提交面放行（marker 也是数据，§13）；
    /// Ok(-1) = 幂等 no-op（同 (pid,epoch,outcome) marker 已存在）。
    WriteTxnMarker {
        producer_id: i64,
        producer_epoch: i16,
        outcome: basalt_record::ControlRecordType,
        reply: oneshot::Sender<Result<i64, StorageError>>,
    },
}

struct PendingFetch {
    offset: i64,
    max_bytes: usize,
    deadline: Instant,
    isolation: Isolation,
    reply: oneshot::Sender<FetchOutcome>,
}

/// 开事务（ADR-18 §4.1）：first_offset = LSO 锚点；deadline = 分区侧自
/// abort 上限（coordinator 侧超时的双保险，兜 TxnLog 清单外漂移批）。
struct OpenTxn {
    epoch: i16,
    first_offset: i64,
    last_offset: i64,
    deadline: Instant,
}

/// marker 应答的簿记载荷：字节确认后才落账（release/settle/即时三路），
/// 防「内存说已 abort、盘上无 marker」的重放 no-op 假 ack（ADR-18 §4.3）。
struct MarkerBook {
    pid: i64,
    epoch: i16,
    outcome: basalt_record::ControlRecordType,
}

impl MarkerBook {
    fn noop() -> MarkerBook {
        MarkerBook { pid: -1, epoch: -1, outcome: basalt_record::ControlRecordType::Abort }
    }
}

/// 过冻结提交面落定的 marker 应答（ParkedAck 同型，ADR-18 §4.1）。
struct ParkedMarker {
    offset: i64,
    book: MarkerBook,
    reply: oneshot::Sender<Result<i64, StorageError>>,
    deadline: Instant,
    face: Vec<i32>,
}

/// 幂等 producer 状态（T-M3.1，KIP-130）：PID → 最近 5 批的序列与偏移。
/// 重复批次（base_sequence ∈ [last-4, last]）返回原偏移不重复追加；
/// 乱序（> last+1）回 OutOfOrderSequence；epoch 升级重置会话。
struct IdemBatch {
    base_seq: i32,
    base_offset: i64,
    last_offset: i64,
}

#[derive(Default)]
struct IdemState {
    epoch: i16,
    last_seq: Option<i32>,
    recent: std::collections::VecDeque<IdemBatch>, // back = 最新
}

/// 副本拉取长轮询（T-M2.4）：follower 的 FetchSlice 在 leader 无新数据时
/// 挂起，数据到达（append/advance）即刻响应——消除 150ms 轮询节拍对
/// acks=all 延迟的主导（基线 p50=152ms 实证）。
struct PendingReplica {
    follower: i32,
    offset: i64,
    max_bytes: usize,
    deadline: Instant,
    reply: oneshot::Sender<SliceOutcome>,
}

struct ParkedAck {
    base_offset: i64,
    last_offset: i64,
    log_append_time: i64,
    reply: oneshot::Sender<ProduceOutcome>,
    deadline: Instant,
    /// 冻结提交面（账本 ㉟）：append 时的 fresh follower 集合。
    /// 放行要求面内全部成员的已知 LEO ≥ last_offset+1——此后 fresh
    /// 集合收缩（laggard 上报过期）不再使 HW 越权放行；面内成员未追平
    /// 就一直等到超时回 NotEnoughReplicas（可重试，客户端重试落到
    /// failover 后的新 leader——与 Kafka acks=all 语义一致）
    face: Vec<i32>,
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
    /// follower ISR（不含 leader；账本 ㉟）：上报新鲜且已追平的副本集合。
    /// 收缩 = isr_lag 内无上报（显式移除，advance_hw 扫描）；重回 = 上报
    /// offset ≥ next_offset（完全追平）。HW = min(ISR LEO)。
    isr: std::collections::BTreeSet<i32>,
    idem: std::collections::HashMap<i64, IdemState>,
    pending_replica: Vec<PendingReplica>,
    /// 开事务表（ADR-18 §4.1）：pid -> 开事务。first_offset 进 LSO 锚。
    txn_open: std::collections::HashMap<i64, OpenTxn>,
    /// 终态 marker（终态 fence + marker 幂等）。
    last_marker: std::collections::HashMap<i64, (i16, basalt_record::ControlRecordType)>,
    /// 已 abort 区间索引（review P2-2）：(pid, epoch) → 区间表（first, last
    /// 升序；同键多区间防御性保留——TV2 每 epoch 一事务，正常恰一条）。
    /// 过滤 = 键直查 + 键内短表扫描，O(批 × 键内区间)。
    aborted: std::collections::HashMap<(i64, i16), Vec<(i64, i64)>>,
    /// 最后稳定 offset = min(HW, 各开事务 first_offset)；无开事务 = HW。
    lso: i64,
    /// 过冻结提交面的 marker 应答。
    parked_markers: Vec<ParkedMarker>,
    /// batch_io 窗口内的 marker 应答（flush 成功后落账+应答）。
    deferred_markers: Vec<(
        oneshot::Sender<Result<i64, StorageError>>,
        Result<(i64, MarkerBook), StorageError>,
    )>,
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
        let mut actor = PartitionActor {
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
            isr: std::collections::BTreeSet::new(),
            idem: std::collections::HashMap::new(),
            pending_replica: Vec::new(),
            txn_open: std::collections::HashMap::new(),
            last_marker: std::collections::HashMap::new(),
            aborted: std::collections::HashMap::new(),
            lso: 0,
            parked_markers: Vec::new(),
            deferred_markers: Vec::new(),
            deferred_produce: Vec::new(),
            repl,
        };
        // 恢复收割（ADR-18 §4.4）：重启后事务视图从日志扫描重建——开事务
        // 锚住 LSO（abort 落地前不泄露）、终态 fence 与 aborted 过滤跨重启有效。
        let harvest = std::mem::take(&mut actor.log.txn_harvest);
        actor.adopt_harvest(harvest);
        tokio::spawn(actor.run());
        Ok(tx)
    }

    async fn run(mut self) {
        loop {
            // 唯一等待点：下一条命令，或最近 deadline（挂起 fetch 截止 /
            // ack 与 marker 停等超时 / 副本长轮询 / 开事务自 abort）。
            // 此前 deadline 感知等待只挂在循环尾部一次，超时分支处理完
            // 命令后回到顶部裸 recv——定时器失联，安静 actor 上挂起项
            // 永不超时（本轮 txn 泄露探针测试实证）。
            let next_deadline = self
                .pending
                .iter()
                .map(|p| p.deadline)
                .chain(self.parked_acks.iter().map(|p| p.deadline))
                .chain(self.parked_markers.iter().map(|p| p.deadline))
                .chain(self.pending_replica.iter().map(|p| p.deadline))
                .chain(self.txn_open.values().map(|t| t.deadline))
                .min();
            let first = match next_deadline {
                Some(d) => {
                    let now = Instant::now();
                    if d > now {
                        match tokio::time::timeout(d - now, self.rx.recv()).await {
                            Ok(Some(cmd)) => Some(cmd),
                            Ok(None) => break,
                            Err(_elapsed) => {
                                self.on_deadline();
                                continue;
                            }
                        }
                    } else {
                        self.on_deadline();
                        continue;
                    }
                }
                None => match self.rx.recv().await {
                    Some(cmd) => Some(cmd),
                    None => break,
                },
            };
            let mut group = vec![first.unwrap()];
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
            self.serve_replica_pends();
        }
        tracing::info!(topic = %self.name, partition = self.index, "partition actor stopped");
    }

    fn on_deadline(&mut self) {
        let now = Instant::now();
        // fetch 超时：正常空回（带上界内可读数据），不可映射 OOR。
        // 上界按隔离级取（read_committed = LSO）——超时回包同样不得
        // 泄露未提交/已 abort 数据（ADR-18 §4.2 三接触点之三）。
        let mut i = 0;
        while i < self.pending.len() {
            if self.pending[i].deadline <= now {
                let p = self.pending.remove(i);
                let out = self.read_for(p.offset, p.max_bytes, p.isolation);
                let _ = p.reply.send(out);
            } else {
                i += 1;
            }
        }
        // 副本长轮询到期：返回空切片（follower 立即重新发起）
        self.serve_replica_pends();
        // ack 停等超时：NotEnoughReplicas
        let mut i = 0;
        while i < self.parked_acks.len() {
            if self.parked_acks[i].deadline <= now {
                if std::env::var("BASALT_LEO_PROBE").is_ok() {
                    eprintln!("ACK-DEADLINE t={} p={} last={}", self.name, self.index, self.parked_acks[i].last_offset);
                }
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
        // marker 停等超时：NotEnoughReplicas（簿记不落——coordinator 收错误
        // 后重发/重对齐；重复 marker 由幂等面收敛，ADR-18 §4.3）
        let mut i = 0;
        while i < self.parked_markers.len() {
            if self.parked_markers[i].deadline <= now {
                let p = self.parked_markers.remove(i);
                let _ = p.reply.send(Err(StorageError::NotEnoughReplicas));
            } else {
                i += 1;
            }
        }
        // 开事务分区侧自 abort（deadline 兜底：coordinator 失联/漂移批）
        self.sweep_txn_deadlines();
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

    /// 幂等去重检查（T-M3.1，KIP-130 服务器侧）。
    /// 返回 Some(outcome) = 重复批（回放缓存偏移）或序列错误（不落盘）；
    /// None = 新批次，继续 append。producer_id < 0 走非幂等路径。
    fn idem_check(&mut self, batches: &Bytes) -> Option<ProduceOutcome> {
        let h = basalt_record::BatchHeader::parse(batches)?;
        if h.producer_id < 0 {
            return None;
        }
        let st = self.idem.entry(h.producer_id).or_default();
        // epoch 升级：新会话（重置序列）；epoch 倒退 = fence
        if h.producer_epoch > st.epoch {
            st.epoch = h.producer_epoch;
            st.last_seq = None;
            st.recent.clear();
        } else if h.producer_epoch < st.epoch {
            return Some(ProduceOutcome {
                base_offset: -1, last_offset: -1, log_append_time: now_ms(),
                error: Some(StorageError::Other("invalid producer epoch".into())),
            });
        }
        let Some(last) = st.last_seq else {
            // 会话首批：任意 base_sequence 皆可（客户端从 0 起）
            st.last_seq = Some(h.base_sequence);
            return None;
        };
        if h.base_sequence == last + 1 {
            st.last_seq = Some(h.base_sequence);
            return None; // 新批次 → 正常 append（偏移由 append 后记录）
        }
        if h.base_sequence <= last {
            // 重复或过期：最近 5 批内 → 回放缓存偏移（客户端重试语义）
            if h.base_sequence > last - 5 {
                if let Some(b) = st.recent.iter().rev().find(|b| b.base_seq == h.base_sequence) {
                    return Some(ProduceOutcome {
                        base_offset: b.base_offset, last_offset: b.last_offset,
                        log_append_time: now_ms(), error: None,
                    });
                }
            }
            return Some(ProduceOutcome {
                base_offset: -1, last_offset: -1, log_append_time: now_ms(),
                error: Some(StorageError::Other("duplicate sequence too old".into())),
            });
        }
        // 乱序（base > last+1）：gap
        Some(ProduceOutcome {
            base_offset: -1, last_offset: -1, log_append_time: now_ms(),
            error: Some(StorageError::OutOfOrderSequence(h.base_sequence, last + 1)),
        })
    }

    /// append 成功后记录幂等缓存（最近 5 批）。
    fn idem_record(&mut self, batches: &Bytes, outcome: &ProduceOutcome) {
        let Some(h) = basalt_record::BatchHeader::parse(batches) else { return };
        if h.producer_id < 0 {
            return;
        }
        let st = self.idem.entry(h.producer_id).or_default();
        st.last_seq = Some(h.base_sequence);
        st.recent.push_back(IdemBatch {
            base_seq: h.base_sequence,
            base_offset: outcome.base_offset,
            last_offset: outcome.last_offset,
        });
        while st.recent.len() > 5 {
            st.recent.pop_front();
        }
    }

    /// ADR-14：batch_io 窗口收口——flush 结果决定延后应答的最终状态。
    /// marker 同窗口结算：flush 成功才落账（字节确认后的簿记纪律，§4.3）。
    fn settle_deferred_produce(&mut self, flush_ok: bool) {
        let deferred = std::mem::take(&mut self.deferred_produce);
        for (reply, mut outcome) in deferred {
            if !flush_ok && outcome.error.is_none() {
                outcome.base_offset = -1;
                outcome.error = Some(StorageError::Other("batch flush failed".into()));
            }
            let _ = reply.send(outcome);
        }
        let markers = std::mem::take(&mut self.deferred_markers);
        for (reply, res) in markers {
            match res {
                Ok((offset, book)) if flush_ok => {
                    self.apply_marker_book(book);
                    let _ = reply.send(Ok(offset));
                }
                Ok((_offset, _book)) => {
                    let _ = reply.send(Err(StorageError::Other("batch flush failed".into())));
                }
                Err(e) => {
                    let _ = reply.send(Err(e));
                }
            }
        }
    }

    fn process(&mut self, group: Vec<PartitionCmd>) {
        for cmd in group {
            match cmd {
                PartitionCmd::SetRole { leader, epoch, replicas } => {
                    let new_role = if leader { Role::Leader } else { Role::Follower };
                    let multi = replicas.len() > 1;
                    let epoch_changed = epoch != self.epoch;
                    if new_role != self.role || epoch != self.epoch || multi != self.log.replicated {
                        tracing::info!(topic=%self.name, partition=self.index, ?new_role, epoch, replicas=?replicas, "role updated");
                        // 角色翻转：清空上一任期状态（LEO 上报/停等/挂起 fetch 全部失效）
                        self.follower_leos.clear();
                        self.isr.clear();
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
                            let _ = p.reply.send(FetchOutcome::err(StorageError::NotLeader, self.lso));
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
                    // 新任期刷新开事务 deadline（follower 期退避前推的锚由
                    // 新任期全量超时接管），随后 LSO 锚随 HW/角色重算。
                    // gate 在 epoch 变化上（review P2-b）：apply_cluster 对
                    // 集群事件无条件重发 SetRole，同 epoch 重复刷新会把分区
                    // 侧自 abort 无限推迟（coordinator 失联时兜底失效）
                    if leader && epoch_changed {
                        let now = Instant::now();
                        for t in self.txn_open.values_mut() {
                            t.deadline = now + self.repl.transaction_timeout;
                        }
                    }
                    self.recompute_lso();
                }
                PartitionCmd::Produce { batches, policy, acks, reply } => {
                    if self.role != Role::Leader && policy == AssignPolicy::Assign {
                        let _ = reply.send(ProduceOutcome {
                            base_offset: -1, last_offset: -1, log_append_time: now_ms(),
                            error: Some(StorageError::NotLeader),
                        });
                        continue;
                    }
                    // acks=all 前置检查：新鲜 ISR 数（含 leader）≥ 有效下限。
                    // C1 适用条款钉死（M2 义务，2026-09-14）：下限 =
                    // max(min.insync 配置, 多数派)——acks=all 的提交面不得
                    // 低于多数派（ISR 收缩只允许损失可用性，unclean=false）；
                    // 配置可收紧（更大）但不可放宽。
                    let majority = (self.replicas.len() as i32) / 2 + 1;
                    let effective_min = self.repl.min_insync.max(majority);
                    // 提交面 = leader + ISR（显式收缩后，落后副本不进提交面；
                    // C1：收缩只允许损失可用性，unclean=false）
                    let in_sync = (self.isr.len() + 1) as i32;
                    if acks == -1 && self.log.replicated && in_sync < effective_min {
                        let _ = reply.send(ProduceOutcome {
                            base_offset: -1, last_offset: -1, log_append_time: now_ms(),
                            error: Some(StorageError::NotEnoughReplicas),
                        });
                        continue;
                    }
                    // 复制流（Absolute，follower pull）不走幂等/fence/登记——
                    // 信任 leader 已 fencing；marker seq 恰为数据 last+1，
                    // 重查幂等会楔死多批切片复制（review P1-B）。事务视图由
                    // replication_txn_advance 按批推进（review P1-A）
                    let replication = policy == AssignPolicy::Absolute;
                    // 幂等去重（T-M3.1）：重复批回放缓存偏移（立即应答，
                    // 不进停等/窗口）；乱序回 OutOfOrderSequence（可重试）
                    if let Some(dup) = if replication { None } else { self.idem_check(&batches) } {
                        let _ = reply.send(dup);
                        continue;
                    }
                    // 终态 fence（ADR-18 §4.1）：同 (pid, epoch) 已有终态
                    // marker 后拒绝新事务批——封「marker 落地后僵尸同 epoch
                    // 续写复活开事务」（LSO 永久停滞的可用性洞）
                    let txn_header = basalt_record::BatchHeader::parse(&batches);
                    let is_txn = txn_header.as_ref().map(|h| h.is_transactional()).unwrap_or(false);
                    if is_txn && !replication {
                        let (pid, ep) = txn_header.as_ref().map(|h| (h.producer_id, h.producer_epoch)).unwrap_or((-1, -1));
                        // 序判定（review P2-1）+ 在途互斥（review P1-4）：终态
                        // marker 停等/窗口内同样拒绝同 pid 僵尸续写——否则
                        // 释放后 aborted 区间外出现已 abort 会话的数据（aborted
                        // reads 的服务端洞）
                        let in_flight = self
                            .parked_markers
                            .iter()
                            .any(|p| p.book.pid == pid)
                            || self
                                .deferred_markers
                                .iter()
                                .any(|(_, r)| r.as_ref().ok().map(|(_, b)| b.pid == pid).unwrap_or(false));
                        let fenced = self.last_marker.get(&pid).map(|&(e, _)| e >= ep).unwrap_or(false);
                        if fenced || in_flight {
                            let _ = reply.send(ProduceOutcome {
                                base_offset: -1, last_offset: -1, log_append_time: now_ms(),
                                error: Some(StorageError::InvalidTxnState),
                            });
                            continue;
                        }
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
                    if replication {
                        if outcome.error.is_none() {
                            self.replication_txn_advance(&batches, outcome.base_offset);
                        }
                    } else {
                        self.idem_record(&batches, &outcome);
                        // 事务登记（ADR-18 §4.1）：事务批成功 append 后开/续事务，
                        // first_offset 进 LSO 锚；epoch 更替的旧开事务入 aborted
                        // （安全方向，同收割语义）
                        if is_txn && outcome.error.is_none() {
                            if let Some(h) = &txn_header {
                                self.txn_register(h.producer_id, h.producer_epoch, outcome.base_offset, outcome.last_offset);
                            }
                        }
                    }
                    self.recompute_lso();
                    // acks=all 且有其他副本：停等 HW 追上（follower 上报驱动；超时 NotEnoughReplicas）
                    let followers: Vec<i32> = self.replicas.iter().copied().filter(|r| *r != self.node_id).collect();
                    self.serve_replica_pends();
                    if acks == -1 && !followers.is_empty() && outcome.error.is_none() {
                        self.parked_acks.push(ParkedAck {
                            base_offset: outcome.base_offset,
                            last_offset: outcome.last_offset,
                            log_append_time: outcome.log_append_time,
                            reply,
                            deadline: Instant::now() + Duration::from_secs(10),
                            face: self.isr.iter().copied().collect(),
                        });
                        self.advance_hw();
                        self.release_acks();
                        self.release_markers();
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
                PartitionCmd::Fetch { offset, max_bytes, deadline, isolation, reply } => {
                    crate::partition::metrics().fetch_requests.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if self.role != Role::Leader {
                        let _ = reply.send(FetchOutcome::err(StorageError::NotLeader, self.lso));
                        continue;
                    }
                    // 上界按隔离级（read_committed = LSO，ADR-18 §4.2）
                    if offset < self.cap_for(isolation) {
                        let out = self.read_for(offset, max_bytes, isolation);
                        let _ = reply.send(out);
                    } else {
                        self.pending.push(PendingFetch { offset, max_bytes, deadline, isolation, reply });
                    }
                }
                PartitionCmd::FetchSlice { follower, offset, max_bytes, reply } => {
                    crate::partition::metrics().fetch_requests.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    // fencing：leader 服务常规复制拉取；副本集内成员互为
                    // 就任拉齐（reconciliation，㉟）来源——新主升任与源副本
                    // 角色翻转存在竞态，拉齐请求落在翻转后时源仍须可服务
                    // （FetchSlice 是内部协议，外部消费者走 Fetch 不受影响）
                    let peer_reconcile = self.replicas.contains(&follower);
                    if self.role != Role::Leader && !peer_reconcile {
                        let _ = reply.send(SliceOutcome {
                            error: Some(StorageError::NotLeader),
                            high_watermark: -1, next_offset: -1, data: Bytes::new(),
                        });
                        continue;
                    }
                    // follower 的拉取起点即其 LEO：记录（>=0）并推进 HW
                    if offset >= 0 {
                        if std::env::var("BASALT_LEO_PROBE").is_ok() {
                            eprintln!("LEO-REPORT t={} p={} from={} leo={}", self.name, self.index, follower, offset);
                        }
                        // ISR 重回（账本 ㉟）：不在 ISR 的 follower 只有完全
                        // 追平（offset ≥ next_offset）才重回；落后者只记 LEO
                        // 供追赶，不进入提交面
                        if offset >= self.log.next_offset && !self.isr.contains(&follower) {
                            self.isr.insert(follower);
                            tracing::info!(topic=%self.name, partition=self.index, follower, offset, "ISR expand");
                        }
                        self.follower_leos.insert(follower, (offset, Instant::now()));
                    }
                    if offset >= self.log.next_offset {
                        // 长轮询（T-M2.4）：无新数据 → 挂起到数据到达或 250ms
                        // 兜底到期（事件唤醒主路径，150ms 轮询节拍退役）
                        self.pending_replica.push(PendingReplica {
                            follower,
                            offset,
                            max_bytes,
                            deadline: Instant::now() + Duration::from_millis(250),
                            reply,
                        });
                        self.advance_hw();
                        self.release_markers();
                        self.release_acks();
                        continue;
                    }
                    let slice = self.read_slice_for(offset, max_bytes);
                    let _ = reply.send(slice);
                    self.release_markers();
                    self.advance_hw();
                    self.release_acks();
                    self.serve_pending();
                }
                PartitionCmd::ReconcileAppend { batches, reply } => {
                    // 拉齐落盘不走 produce 角色/提交面检查——调用方（meta 就任
                    // 编排）保证只在升主路径上调用且数据来自副本集内
                    let r = self
                        .log
                        .append(&batches, AssignPolicy::Assign, now_ms())
                        .map(|o| o.last_offset)
                        .is_ok();
                    if r {
                        self.advance_hw();
                        self.serve_replica_pends(); // 拉齐落盘也是"新数据到达"
                    }
                    let _ = reply.send(r);
                }
                PartitionCmd::Shutdown { reply } => {
                    // topic 删除：actor 退出（路由已移除，句柄/内存随任务
                    // 结束回收；数据目录留给 retention 清理）
                    let _ = reply.send(());
                    return;
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
                    // 事务状态随截断收敛（ADR-18 §4.1 POC 边界）：截断点之下的
                    // 开事务锚随数据消失；aborted 区间残留在删除区间之下无害
                    self.txn_open.retain(|_, t| t.last_offset >= offset);
                    self.recompute_lso();
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
                PartitionCmd::WriteTxnMarker { producer_id, producer_epoch, outcome, reply } => {
                    // coordinator 编排路径：仅 leader 受理（迟到路由 → NotLeader
                    // 重对齐）；幂等/相异/迟到 epoch 判定在 append_txn_marker 内
                    if self.role != Role::Leader {
                        let _ = reply.send(Err(StorageError::NotLeader));
                        continue;
                    }
                    // acks=all 前置门与 produce 同款（P1-2）：新鲜 ISR 低于
                    // max(min.insync, 多数派) 不得受理——否则空 face「停等」
                    // 全称真即放行，coordinator 拿到提交成功而零副本持字节
                    if self.log.replicated && self.replicas.len() > 1 {
                        let majority = (self.replicas.len() as i32) / 2 + 1;
                        let effective_min = self.repl.min_insync.max(majority);
                        let in_sync = (self.isr.len() + 1) as i32;
                        if in_sync < effective_min {
                            let _ = reply.send(Err(StorageError::NotEnoughReplicas));
                            continue;
                        }
                    }
                    match self.append_txn_marker(producer_id, producer_epoch, outcome) {
                        Err(e) => {
                            let _ = reply.send(Err(e));
                        }
                        // 幂等 no-op：直接应答（不占新 offset，ADR-18 §4.3）
                        Ok((-1, _)) => {
                            let _ = reply.send(Ok(-1));
                        }
                        Ok((offset, book)) => {
                            let has_followers = self.log.replicated
                                && self.replicas.iter().any(|r| *r != self.node_id);
                            if has_followers {
                                // marker 是数据：过冻结提交面落定后才应答
                                // （ADR-18 §4.1/§13——提前应答 + failover 丢
                                // marker = 已提交事务成孤儿 = lost writes）
                                self.parked_markers.push(ParkedMarker {
                                    offset,
                                    book,
                                    reply,
                                    deadline: Instant::now() + Duration::from_secs(10),
                                    face: self.isr.iter().copied().collect(),
                                });
                                self.advance_hw();
                                self.release_acks();
                                self.release_markers();
                            } else if self.log.batch_io {
                                // 窗口收口后落账+应答（字节确认纪律）
                                self.deferred_markers.push((reply, Ok((offset, book))));
                            } else {
                                self.apply_marker_book(book);
                                let _ = reply.send(Ok(offset));
                            }
                        }
                    }
                }
            }
        }
    }

    /// HW = min(自身 LEO, 新鲜 ISR 的 LEO)。只升不降（ISR 收缩不回退 HW）。
    fn advance_hw(&mut self) {
        if !self.log.replicated {
            return;
        }
        // ISR 收缩（账本 ㉟，显式动作）：isr_lag 内无上报的 follower 移出——
        // 此前"fresh-only min"让它从 HW 计算里静默消失，ack 在部分副本缺失
        // 时放行、failover 选中该副本即丢已 ack 消息（k-39 实证）
        let now = Instant::now();
        let shrunk: Vec<i32> = self.isr.iter().copied()
            .filter(|id| match self.follower_leos.get(id) {
                Some((_, t)) => now.duration_since(*t) > self.repl.isr_lag,
                None => true,
            })
            .collect();
        for id in shrunk {
            self.isr.remove(&id);
            tracing::info!(topic=%self.name, partition=self.index, follower=id, "ISR shrink");
        }
        if self.isr.is_empty() {
            return; // 全员未上报：HW 保持（等上报/收缩）
        }
        let min_leo = self
            .isr
            .iter()
            .filter_map(|id| self.follower_leos.get(id).map(|(leo, _)| *leo))
            .min()
            .unwrap_or(i64::MAX);
        let new_hw = self.log.next_offset.min(min_leo);
        if new_hw > self.log.high_watermark {
            if std::env::var("BASALT_LEO_PROBE").is_ok() {
                eprintln!("HW-ADV t={} p={} hw={} isr={:?}", self.name, self.index, new_hw, self.isr);
            }
            self.log.high_watermark = new_hw;
        }
        // HW 推进 → LSO 上界随动（开事务锚不变时 LSO = min(HW, 锚)）
        self.recompute_lso();
    }

    /// HW 追上的停等 acks 放行。
    fn release_acks(&mut self) {
        let mut i = 0;
        while i < self.parked_acks.len() {
            // 冻结提交面放行（账本 ㉟）：面内全部成员的最后已知 LEO 追平
            // 才放行——不要求成员仍 fresh（上报过期 ≠ 数据消失）；
            // HW 自身仍按 fresh min 服务读路径，但不再是 ack 放行依据
            let need = self.parked_acks[i].last_offset + 1;
            let covered = self.parked_acks[i].face.iter().all(|f| {
                self.follower_leos.get(f).map(|(leo, _)| *leo >= need).unwrap_or(false)
            });
            if covered {
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

    /// 副本长轮询服务：有新数据即按 LogEnd 读取返回；到期返回空
    /// （follower 的 150ms 兜底退化为纯保险）。
    fn serve_replica_pends(&mut self) {
        let mut i = 0;
        while i < self.pending_replica.len() {
            let expired = self.pending_replica[i].deadline <= Instant::now();
            let ready = self.pending_replica[i].offset < self.log.next_offset;
            if !expired && !ready {
                i += 1;
                continue;
            }
            let p = self.pending_replica.remove(i);
            let slice = if ready {
                self.read_slice_for(p.offset, p.max_bytes)
            } else {
                SliceOutcome {
                    error: None,
                    high_watermark: self.log.high_watermark,
                    next_offset: self.log.next_offset,
                    data: Bytes::new(),
                }
            };
            let _ = p.reply.send(slice);
        }
    }

    fn read_slice_for(&mut self, offset: i64, max_bytes: usize) -> SliceOutcome {
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
        SliceOutcome {
            error: None,
            high_watermark: self.log.high_watermark,
            next_offset: self.log.next_offset,
            data: out.map(|r| r.data).unwrap_or_else(Bytes::new),
        }
    }

    /// append 之后重查挂起的 fetch。
    fn serve_pending(&mut self) {
        let mut i = 0;
        while i < self.pending.len() {
            // 唤醒谓词按请求隔离级取上界（read_committed 等 LSO，非 HW）
            if self.pending[i].offset < self.cap_for(self.pending[i].isolation) {
                let p = self.pending.remove(i);
                let out = self.read_for(p.offset, p.max_bytes, p.isolation);
                let _ = p.reply.send(out);
            } else {
                i += 1;
            }
        }
    }

    // ---------- 事务面（ADR-18 §4，块 a） ----------

    /// 请求隔离级的可见上界。
    fn cap_for(&self, iso: Isolation) -> i64 {
        match iso {
            Isolation::ReadUncommitted => self.log.high_watermark,
            Isolation::ReadCommitted => self.lso,
        }
    }

    /// LSO = min(HW, 各开事务 first_offset)，无开事务 = HW；
    /// 删除区间之下的锚钳制到 log_start（POC 边界，ADR-18 §10）。
    fn recompute_lso(&mut self) {
        let mut l = self.log.high_watermark;
        for t in self.txn_open.values() {
            l = l.min(t.first_offset);
        }
        if l < self.log.log_start_offset {
            l = self.log.log_start_offset;
        }
        self.lso = l;
    }

    /// 事务批成功 append 后的开/续事务登记。
    fn txn_register(&mut self, pid: i64, epoch: i16, base: i64, last: i64) {
        if std::env::var("BASALT_LEO_PROBE").is_ok() {
            eprintln!("DBG-REG t={} p={} pid={} epoch={} base={} last={}", self.name, self.index, pid, epoch, base, last);
        }
        match self.txn_open.get(&pid).map(|t| (t.epoch, t.first_offset, t.last_offset)) {
            Some((e, _first, _)) if e == epoch => {
                if let Some(t) = self.txn_open.get_mut(&pid) {
                    t.last_offset = last;
                }
            }
            Some((e, first, old_last)) => {
                // 会话更替：旧开事务被 fence 且不再会有其 marker——安全方向
                // 直接入 aborted（与恢复收割同语义）
                self.aborted.entry((pid, e)).or_default().push((first, old_last));
                self.txn_open.insert(pid, OpenTxn {
                    epoch, first_offset: base, last_offset: last,
                    deadline: Instant::now() + self.repl.transaction_timeout,
                });
            }
            None => {
                self.txn_open.insert(pid, OpenTxn {
                    epoch, first_offset: base, last_offset: last,
                    deadline: Instant::now() + self.repl.transaction_timeout,
                });
            }
        }
    }

    /// 复制路径（Absolute）的事务推进（review P1-A）：切片内逐批收割式处理
    /// ——控制批 → 终态簿记落账（复制落盘即字节确认）；事务数据批 → 开/续
    /// 事务。follower 由此获得与 leader 一致的 txn_open/last_marker/aborted
    /// 视图，升任继承不腐化。
    fn replication_txn_advance(&mut self, batches: &Bytes, base: i64) {
        let mut pos = 0usize;
        let mut cum = base;
        while let Some(h) = basalt_record::BatchHeader::parse(&batches[pos..]) {
            let total = h.total_len();
            if total == 0 || pos + total > batches.len() {
                break;
            }
            if h.producer_id >= 0 {
                let last = cum + h.record_count.max(0) as i64 - 1;
                if h.is_control() {
                    if let Some(ty) = basalt_record::control_record_type_of(&batches[pos..pos + total]) {
                        self.apply_marker_book(MarkerBook { pid: h.producer_id, epoch: h.producer_epoch, outcome: ty });
                    }
                } else if h.is_transactional() {
                    self.txn_register(h.producer_id, h.producer_epoch, cum, last);
                }
            }
            cum += h.record_count.max(0) as i64;
            pos += total;
        }
        self.recompute_lso();
    }

    /// marker 落盘核心：幂等 no-op（同 (pid,epoch,outcome)）、相异/迟到拒绝
    /// （COMMIT 永不覆盖 ABORT 终态——abort 是安全方向，ADR-18 §4.3）、
    /// 编码 append。**只 append 不落账**——簿记随字节确认路径
    /// （release/settle/即时）走 apply_marker_book。
    fn append_txn_marker(
        &mut self,
        pid: i64,
        epoch: i16,
        outcome: basalt_record::ControlRecordType,
    ) -> Result<(i64, MarkerBook), StorageError> {
        // 在途互斥（review P1-3c）：同 pid 已有 marker 在停等/窗口内未结算
        // 时拒绝新 marker——保证簿记顺序与字节顺序一致（否则迟到 COMMIT
        // 落账覆盖已落账 ABORT 的区间语义）。回可重试错误，coordinator 重试。
        let in_flight = self
            .parked_markers
            .iter()
            .any(|p| p.book.pid == pid)
            || self
                .deferred_markers
                .iter()
                .any(|(_, r)| r.as_ref().ok().map(|(_, b)| b.pid == pid).unwrap_or(false));
        if in_flight {
            return Err(StorageError::Other("txn marker in flight".into()));
        }
        if let Some(&(me, mo)) = self.last_marker.get(&pid) {
            // 序判定（review P2-1）：me > epoch = 旧纪元 marker 回拨终态，
            // 拒绝；相等才进入幂等/相异判定
            if me > epoch {
                return Err(StorageError::InvalidTxnState);
            }
            if me == epoch {
                return if mo == outcome {
                    Ok((-1, MarkerBook::noop()))
                } else {
                    Err(StorageError::InvalidTxnState)
                };
            }
        }
        if let Some(t) = self.txn_open.get(&pid) {
            if t.epoch != epoch {
                return Err(StorageError::InvalidTxnState); // 旧 epoch marker 迟到
            }
        }
        let seq = self.idem.get(&pid).and_then(|s| s.last_seq).map(|s| s.wrapping_add(1)).unwrap_or(0);
        let mut buf = BytesMut::new();
        basalt_record::encode_control_batch(0, 0, now_ms(), pid, epoch, seq, outcome, &mut buf);
        let raw = buf.freeze();
        let r = self.log.append(&raw, AssignPolicy::Assign, now_ms())?;
        Ok((r.last_offset, MarkerBook { pid, epoch, outcome }))
    }

    /// 簿记落账（字节确认后调用）：终态表、aborted 区间、LSO 重算。
    /// aborted 区间取落账时 txn_open 现值（review P1-4：append 与 release
    /// 之间被 fence 拦截失败的僵尸批不会扩大区间，但 epoch 更替等路径可能
    /// 已扩——现值是超集，安全方向）；Commit 清除同会话 aborted 残留
    /// （末写胜出，防 parked 竞态残留）。
    fn apply_marker_book(&mut self, b: MarkerBook) {
        if b.pid < 0 {
            return; // noop 载荷
        }
        if std::env::var("BASALT_LEO_PROBE").is_ok() {
            eprintln!("DBG-APPLY t={} p={} pid={} epoch={} oc={:?} open={:?} hw={}",
                self.name, self.index, b.pid, b.epoch, b.outcome, self.txn_open.get(&b.pid).map(|t| (t.epoch, t.first_offset)), self.log.high_watermark);
        }
        self.last_marker.insert(b.pid, (b.epoch, b.outcome));
        if let Some(t) = self.txn_open.remove(&b.pid) {
            if b.outcome == basalt_record::ControlRecordType::Abort && t.epoch == b.epoch {
                self.aborted.entry((b.pid, b.epoch)).or_default().push((t.first_offset, t.last_offset));
            }
        }
        if b.outcome == basalt_record::ControlRecordType::Commit {
            self.aborted.remove(&(b.pid, b.epoch));
        }
        self.recompute_lso();
        self.serve_pending();
    }

    /// 冻结提交面放行的 marker 应答（ParkedAck 同型判据）。
    fn release_markers(&mut self) {
        let mut i = 0;
        while i < self.parked_markers.len() {
            let need = self.parked_markers[i].offset + 1;
            let covered = self.parked_markers[i].face.iter().all(|f| {
                self.follower_leos.get(f).map(|(leo, _)| *leo >= need).unwrap_or(false)
            });
            if covered {
                let p = self.parked_markers.remove(i);
                self.apply_marker_book(p.book);
                let _ = p.reply.send(Ok(p.offset));
            } else {
                i += 1;
            }
        }
    }

    /// 开事务 deadline 兜底：分区侧自 abort（与 coordinator 驱动同一
    /// marker 管道；兜 coordinator 失联/TxnLog 清单外漂移批，ADR-18 §4.1）。
    /// 结算与 WriteTxnMarker 同路（review P1-3a）：多副本必过冻结提交面
    /// park，簿记随字节确认走——绝不绕过复制层直接落账。
    fn sweep_txn_deadlines(&mut self) {
        let now = Instant::now();
        // follower 不落 marker（review P1-1）：绕过复制层写日志 = 副本分叉
        //（TruncateTo fencing 同款纪律）；锚保留等重新升任后收敛。
        // 但过期 deadline 必须前推——否则主循环 deadline 已过 → on_deadline
        // → continue 热旋（P1-5 风暴的 follower 变体，本轮测试实证）
        if self.role != Role::Leader {
            for (pid, t) in self.txn_open.iter_mut() {
                if t.deadline <= now {
                    t.deadline = now + Duration::from_secs(1);
                    tracing::debug!(pid, "txn deadline deferred (follower)");
                }
            }
            return;
        }
        let expired: Vec<i64> = self
            .txn_open
            .iter()
            .filter(|(_, t)| t.deadline <= now)
            .map(|(p, _)| *p)
            .collect();
        for pid in expired {
            let (epoch, outcome) = (self.txn_open[&pid].epoch, basalt_record::ControlRecordType::Abort);
            match self.append_txn_marker(pid, epoch, outcome) {
                Ok((-1, _)) => {
                    // ㊾ 收敛契约闭合（review P2-a）：终态已同型而锚残留
                    // （未来路径可能造成共存）→ 清锚防 deadline 风暴复辟
                    self.txn_open.remove(&pid);
                    self.recompute_lso();
                }
                Ok((_offset, book)) => {
                    tracing::info!(topic = %self.name, partition = self.index, pid, "txn deadline self-abort");
                    let has_followers = self.log.replicated
                        && self.replicas.iter().any(|r| *r != self.node_id);
                    // ISR 前置门与 WriteTxnMarker 同款（review P2-c）：不过门
                    // 走 1s 退避（ABORT-only 后果良性，但纪律①须字面一致）
                    if has_followers {
                        let majority = (self.replicas.len() as i32) / 2 + 1;
                        let effective_min = self.repl.min_insync.max(majority);
                        let in_sync = (self.isr.len() + 1) as i32;
                        if in_sync < effective_min {
                            if let Some(t) = self.txn_open.get_mut(&pid) {
                                t.deadline = now + Duration::from_secs(1);
                            }
                            continue;
                        }
                    }
                    if has_followers {
                        // 无应答方（oneshot 对端即刻丢弃）：簿记随 release 落
                        let (trx, _rrx) = oneshot::channel();
                        self.parked_markers.push(ParkedMarker {
                            offset: _offset,
                            book,
                            reply: trx,
                            deadline: Instant::now() + Duration::from_secs(10),
                            face: self.isr.iter().copied().collect(),
                        });
                        self.advance_hw();
                        self.release_acks();
                        self.release_markers();
                    } else if self.log.batch_io {
                        let (trx, _rrx) = oneshot::channel();
                        self.deferred_markers.push((trx, Ok((_offset, book))));
                    } else {
                        self.apply_marker_book(book);
                    }
                }
                Err(e) => {
                    // 失败退避（review P1-5）：不清条目也不前推 deadline 会
                    // 让主循环 deadline 风暴（on_deadline→continue 热旋、
                    // 饿死全部命令）——前推 1s 再试
                    tracing::warn!(topic = %self.name, partition = self.index, pid, error = %e, "txn self-abort failed, backoff 1s");
                    if let Some(t) = self.txn_open.get_mut(&pid) {
                        t.deadline = now + Duration::from_secs(1);
                    }
                }
            }
        }
    }

    /// 消费读（隔离级感知）：上界 cap_for + 控制批剥离（两档隔离级都剥离，
    /// 应用永不见控制记录）+ read_committed 再剥 aborted 区间并汇总条目。
    fn read_for(&mut self, offset: i64, max_bytes: usize, iso: Isolation) -> FetchOutcome {
        let cap = self.cap_for(iso);
        match self.log.read_ex(offset, max_bytes, &self.pool, ReadCap::At(cap)) {
            Err(e) => FetchOutcome::err(e, self.lso),
            Ok(r) => {
                let data = self.filter_batches(r.data, iso == Isolation::ReadCommitted);
                FetchOutcome {
                    result: Some(ReadResult { data, ..r }),
                    error: None,
                    last_stable_offset: self.lso,
                }
            }
        }
    }

    /// 批走过滤：控制批恒剥离；read_committed 下命中 aborted 区间的事务批
    /// 剥离。**投递模型（ADR-18 §4.2，review P0 定案）**：服务端预过滤 +
    /// 响应恒发空 AbortedTransactions 数组——java/librdkafka/franz-go/
    /// kafka-python 四档对空数组均按「无 abort」处理，数据已被服务端滤掉
    /// 故客户端无需条目；Kafka 本尊的「wire 投递控制批 + 客户端按
    /// (pid, FirstOffset) 丢批」模型与「服务端预过滤」组合必坏（客户端
    /// 无 epoch、靠 ABORT 控制批终结区间——被剥离后无法收敛），二选一，
    /// basalt 取服务端滤（对客户端能力无假设，纵深防御）。
    fn filter_batches(&self, data: Bytes, committed: bool) -> Bytes {
        let mut out = BytesMut::new();
        let mut pos = 0usize;
        while let Some(h) = basalt_record::BatchHeader::parse(&data[pos..]) {
            let total = h.total_len();
            if total == 0 || pos + total > data.len() {
                break;
            }
            let mut skip = h.is_control();
            if !skip && committed && h.is_transactional() && h.producer_id >= 0 {
                let last = h.base_offset + h.last_offset_delta as i64;
                if let Some(ranges) = self.aborted.get(&(h.producer_id, h.producer_epoch)) {
                    if ranges.iter().any(|&(f, l)| f <= last && h.base_offset <= l) {
                        skip = true;
                    }
                }
            }
            if !skip {
                out.extend_from_slice(&data[pos..pos + total]);
            }
            pos += total;
        }
        out.freeze()
    }

    /// 恢复收割采纳（ADR-18 §4.4）：spawn 时一次，事务视图从日志扫描重建。
    fn adopt_harvest(&mut self, h: basalt_storage::log::TxnHarvest) {
        let now = Instant::now();
        for (pid, (epoch, first, last)) in h.open {
            self.txn_open.insert(pid, OpenTxn {
                epoch,
                first_offset: first,
                last_offset: last,
                deadline: now + self.repl.transaction_timeout,
            });
        }
        self.last_marker = h.last_marker;
        for (pid, epoch, first, last) in h.aborted {
            self.aborted.entry((pid, epoch)).or_default().push((first, last));
        }
        for (pid, epoch) in h.pid_epoch {
            self.idem.entry(pid).or_default().epoch = epoch;
        }
        self.recompute_lso();
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
            ReplicaConfig { min_insync: 1, isr_lag: Duration::from_millis(500), transaction_timeout: Duration::from_secs(60) },
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

    /// M2 义务（C1 适用条款钉死）：acks=all 前置校验含多数派下限——
    /// RF=3 且无新鲜 follower 上报时拒绝（即便 min.insync 配置为 1）。
    #[tokio::test]
    async fn acks_all_pinned_to_majority_floor() {
        let dir = std::env::temp_dir().join(format!(
            "basalt-ackfloor-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let pool = std::sync::Arc::new(BufferPool::new());
        // min.insync 配置 = 1（默认）：多数派下限 2 生效
        let tx = PartitionActor::spawn(
            "t".into(), 0, 0, dir.clone(),
            LogOptions { segment_max_bytes: 1 << 30, fsync: FsyncSchedule::Os, retention_ms: 0, retention_max_bytes: 0 },
            ReplicaConfig { min_insync: 1, isr_lag: Duration::from_millis(500), transaction_timeout: Duration::from_secs(60) },
            pool,
        ).unwrap();
        tx.send(PartitionCmd::SetRole { leader: true, epoch: 1, replicas: vec![0, 1, 2] }).await.unwrap();

        // RF=3、0 个新鲜 follower 上报 → fresh=1 < max(1, 2)=2 → NotEnoughReplicas
        let (ptx, prx) = oneshot::channel();
        tx.send(PartitionCmd::Produce {
            batches: batch_bytes(2, "a"),
            policy: AssignPolicy::Assign,
            acks: -1,
            reply: ptx,
        }).await.unwrap();
        let o = prx.await.unwrap();
        assert!(
            matches!(o.error, Some(StorageError::NotEnoughReplicas)),
            "acks=all 在新鲜 ISR 低于多数派时必须拒绝：{:?}",
            o.error
        );

        // 对照：acks=1（同参数）不受下限约束，正常写入
        let (ptx, prx) = oneshot::channel();
        tx.send(PartitionCmd::Produce {
            batches: batch_bytes(2, "b"),
            policy: AssignPolicy::Assign,
            acks: 1,
            reply: ptx,
        }).await.unwrap();
        let o = prx.await.unwrap();
        assert!(o.error.is_none(), "{:?}", o.error);
        assert_eq!(o.base_offset, 0);

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
            ReplicaConfig { min_insync: 1, isr_lag: Duration::from_millis(500), transaction_timeout: Duration::from_secs(60) },
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


#[cfg(test)]
mod frozen_face_tests {
    //! 账本 ㉟ 回归：parked ack 放行 = 冻结提交面全员追平。
    //! 缺陷形态：放行只看 HW（fresh-only min）——laggard 上报过期即被
    //! 跳过，HW 越权放行；failover 选中该副本即丢已 ack 消息。

    use super::*;
    use basalt_record::{encode_batch, Rec};
    use basalt_storage::log::{FsyncSchedule, LogOptions};
    use bytes::{Bytes, BytesMut};

    fn batch_bytes(tag: &str) -> Bytes {
        let recs: Vec<Rec> = (0..1)
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
    async fn parked_ack_waits_for_full_frozen_face() {
        let dir = std::env::temp_dir().join(format!(
            "basalt-frozenface-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let pool = std::sync::Arc::new(BufferPool::new());
        let tx = PartitionActor::spawn(
            "ff".into(),
            0,
            0,
            dir.clone(),
            LogOptions { segment_max_bytes: 1 << 30, fsync: FsyncSchedule::Os, retention_ms: 0, retention_max_bytes: 0 },
            ReplicaConfig { min_insync: 1, isr_lag: Duration::from_millis(500), transaction_timeout: Duration::from_secs(60) },
            pool,
        )
        .unwrap();

        tx.send(PartitionCmd::SetRole { leader: true, epoch: 1, replicas: vec![0, 1, 2] }).await.unwrap();

        // follower 上报：f1 追平（LEO=0→尚无数据）、f2 落后（LEO=0）
        for (fid, off) in [(1i32, 0i64), (2i32, 0i64)] {
            let (rtx, rrx) = oneshot::channel();
            tx.send(PartitionCmd::FetchSlice { follower: fid, offset: off, max_bytes: 1024, reply: rtx }).await.unwrap();
            let _ = rrx.await.unwrap();
        }

        // produce acks=-1 → park，face = {1, 2}
        let (ptx, mut prx) = oneshot::channel();
        tx.send(PartitionCmd::Produce {
            batches: batch_bytes("m0"),
            policy: AssignPolicy::Assign,
            acks: -1,
            reply: ptx,
        })
        .await
        .unwrap();

        // f1 追平到 LEO=1（offset 0 已拉取）；f2 仍停在 0
        let (rtx, rrx) = oneshot::channel();
        tx.send(PartitionCmd::FetchSlice { follower: 1, offset: 1, max_bytes: 1024, reply: rtx }).await.unwrap();
        let _ = rrx.await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(prx.try_recv().is_err(), "面内 f2 未追平不得放行（㉟ 冻结面）");

        // f2 追平 → 冻结面全员覆盖 → 放行
        let (rtx, rrx) = oneshot::channel();
        tx.send(PartitionCmd::FetchSlice { follower: 2, offset: 1, max_bytes: 1024, reply: rtx }).await.unwrap();
        let _ = rrx.await.unwrap();
        let o = tokio::time::timeout(Duration::from_secs(2), prx).await
            .expect("冻结面追平后应放行")
            .unwrap();
        assert!(o.error.is_none(), "{:?}", o.error);
        assert_eq!(o.last_offset, 0);

        drop(tx);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod idempotence_tests {
    //! T-M3.1 幂等 producer：服务端去重（KIP-130 服务器侧）。
    //! 重复批回放缓存偏移不重复追加；乱序回 OutOfOrderSequence；
    //! epoch 升级重置会话。

    use super::*;
    use basalt_record::{encode_batch, Rec};
    use basalt_storage::log::{FsyncSchedule, LogOptions};
    use bytes::{Bytes, BytesMut};

    pub(super) fn batch_bytes_idem(pid: i64, epoch: i16, seq: i32, tag: &str) -> Bytes {
        let recs: Vec<Rec> = (0..1)
            .map(|i| Rec {
                timestamp_delta: i as i64,
                key: Some(Bytes::from(format!("k{i}"))),
                value: Some(Bytes::from(format!("{tag}-{i}"))),
                headers: vec![],
            })
            .collect();
        let mut b = BytesMut::new();
        encode_batch(0, 0, 1000, 0, pid, epoch, seq, &recs, &mut b);
        b.freeze()
    }

    pub(super) async fn spawn_leader(tag: &str) -> (mpsc::Sender<PartitionCmd>, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "basalt-idem-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let pool = std::sync::Arc::new(BufferPool::new());
        let tx = PartitionActor::spawn(
            "idem".into(), 0, 0, dir.clone(),
            LogOptions { segment_max_bytes: 1 << 30, fsync: FsyncSchedule::Os, retention_ms: 0, retention_max_bytes: 0 },
            ReplicaConfig { min_insync: 1, isr_lag: Duration::from_millis(500), transaction_timeout: Duration::from_secs(60) },
            pool,
        ).unwrap();
        tx.send(PartitionCmd::SetRole { leader: true, epoch: 1, replicas: vec![0] }).await.unwrap();
        (tx, dir)
    }

    pub(super) async fn produce(tx: &mpsc::Sender<PartitionCmd>, pid: i64, epoch: i16, seq: i32, tag: &str) -> ProduceOutcome {
        let (ptx, prx) = oneshot::channel();
        tx.send(PartitionCmd::Produce {
            batches: batch_bytes_idem(pid, epoch, seq, tag),
            policy: AssignPolicy::Assign,
            acks: 1,
            reply: ptx,
        }).await.unwrap();
        prx.await.unwrap()
    }

    pub(super) async fn leo(tx: &mpsc::Sender<PartitionCmd>) -> i64 {
        let (ltx, lrx) = oneshot::channel();
        tx.send(PartitionCmd::LocalLeo { reply: ltx }).await.unwrap();
        lrx.await.unwrap()
    }

    #[tokio::test]
    async fn idempotent_dedup_and_out_of_order() {
        let (tx, dir) = spawn_leader("dedup").await;

        // seq 0：新会话首批
        let o0 = produce(&tx, 100, 0, 0, "m0").await;
        assert!(o0.error.is_none(), "{:?}", o0.error);
        assert_eq!(o0.base_offset, 0);
        // seq 1：顺序新批
        let o1 = produce(&tx, 100, 0, 1, "m1").await;
        assert!(o1.error.is_none() && o1.base_offset == 1);
        assert_eq!(leo(&tx).await, 2);

        // 重复 seq 0：回放缓存偏移，不重复追加
        let od = produce(&tx, 100, 0, 0, "dup").await;
        assert!(od.error.is_none(), "重复批应成功回放");
        assert_eq!(od.base_offset, 0, "重复批应回放原偏移");
        assert_eq!(leo(&tx).await, 2, "重复批不得推进 LEO");

        // 乱序 seq 3（gap）：OutOfOrderSequence 错误
        let og = produce(&tx, 100, 0, 3, "gap").await;
        assert!(og.error.is_some(), "gap 必须报错");
        assert_eq!(leo(&tx).await, 2, "gap 不得推进 LEO");

        // 补上 seq 2：恢复正常
        let o2 = produce(&tx, 100, 0, 2, "m2").await;
        assert!(o2.error.is_none() && o2.base_offset == 2);

        drop(tx);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn epoch_bump_resets_session() {
        let (tx, dir) = spawn_leader("epoch").await;
        let o0 = produce(&tx, 200, 0, 5, "e0").await;  // 任意首序列皆可
        assert!(o0.error.is_none());
        // epoch 升级：会话重置，序列从 0 重新开始
        let o1 = produce(&tx, 200, 1, 0, "e1").await;
        assert!(o1.error.is_none() && o1.base_offset == 1);
        drop(tx);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod txn_tests {
    //! ADR-18 块 a（数据面）：LSO 锚定/释放、控制批剥离、aborted 过滤、
    //! marker 幂等与终态 fence、恢复收割、deadline 自 abort、超时不泄露。

    use super::*;
    use basalt_record::{encode_batch, Rec, ATTR_TRANSACTIONAL};
    use basalt_storage::log::{FsyncSchedule, LogOptions};

    fn batch_bytes_txn(pid: i64, epoch: i16, seq: i32, tag: &str) -> Bytes {
        let recs: Vec<Rec> = (0..1)
            .map(|i| Rec {
                timestamp_delta: i as i64,
                key: Some(Bytes::from(format!("k{i}"))),
                value: Some(Bytes::from(format!("{tag}-{i}"))),
                headers: vec![],
            })
            .collect();
        let mut b = BytesMut::new();
        encode_batch(0, 0, 1000, ATTR_TRANSACTIONAL, pid, epoch, seq, &recs, &mut b);
        b.freeze()
    }

    fn opts() -> LogOptions {
        LogOptions { segment_max_bytes: 1 << 30, fsync: FsyncSchedule::Os, retention_ms: 0, retention_max_bytes: 0 }
    }

    fn cfg(txn_timeout: Duration) -> ReplicaConfig {
        ReplicaConfig { min_insync: 1, isr_lag: Duration::from_millis(500), transaction_timeout: txn_timeout }
    }

    fn fresh_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "basalt-txn-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    async fn spawn_on(dir: std::path::PathBuf, txn_timeout: Duration) -> mpsc::Sender<PartitionCmd> {
        let pool = std::sync::Arc::new(BufferPool::new());
        let tx = PartitionActor::spawn("txn".into(), 0, 0, dir, opts(), cfg(txn_timeout), pool).unwrap();
        tx.send(PartitionCmd::SetRole { leader: true, epoch: 1, replicas: vec![0] }).await.unwrap();
        tx
    }

    async fn produce(tx: &mpsc::Sender<PartitionCmd>, batches: Bytes) -> ProduceOutcome {
        let (ptx, prx) = oneshot::channel();
        tx.send(PartitionCmd::Produce { batches, policy: AssignPolicy::Assign, acks: 1, reply: ptx }).await.unwrap();
        prx.await.unwrap()
    }

    async fn marker(tx: &mpsc::Sender<PartitionCmd>, pid: i64, epoch: i16, out: basalt_record::ControlRecordType) -> Result<i64, StorageError> {
        let (mtx, mrx) = oneshot::channel();
        tx.send(PartitionCmd::WriteTxnMarker { producer_id: pid, producer_epoch: epoch, outcome: out, reply: mtx }).await.unwrap();
        mrx.await.unwrap()
    }

    async fn fetch(tx: &mpsc::Sender<PartitionCmd>, offset: i64, iso: Isolation, wait: Duration) -> FetchOutcome {
        let (ftx, frx) = oneshot::channel();
        tx.send(PartitionCmd::Fetch { offset, max_bytes: 1 << 20, deadline: Instant::now() + wait, isolation: iso, reply: ftx }).await.unwrap();
        frx.await.unwrap()
    }

    fn data_batches(out: &FetchOutcome) -> Vec<basalt_record::BatchHeader> {
        let mut v = Vec::new();
        let data = match &out.result { Some(r) => &r.data, None => return v };
        let mut pos = 0usize;
        while let Some(h) = basalt_record::BatchHeader::parse(&data[pos..]) {
            let total = h.total_len();
            if total == 0 || pos + total > data.len() { break; }
            v.push(h);
            pos += total;
        }
        v
    }

    /// LSO 锚定：开事务把 read_committed 上界钉在 first_offset；
    /// read_uncommitted 不受影响；控制批/事务批标志位符合预期。
    #[tokio::test]
    async fn txn_open_anchors_lso_read_committed_hides() {
        let dir = fresh_dir("anchor");
        let tx = spawn_on(dir.clone(), Duration::from_secs(60)).await;
        let o = produce(&tx, batch_bytes_txn(500, 0, 0, "m")).await;
        assert!(o.error.is_none(), "{:?}", o.error);
        assert_eq!(o.base_offset, 0);

        // committed：offset 0 不小于 lso(0) → 挂起 → 零 deadline 立即超时空回
        let f = fetch(&tx, 0, Isolation::ReadCommitted, Duration::from_secs(0)).await;
        assert!(f.result.as_ref().map(|r| r.data.is_empty()).unwrap_or(true), "开事务数据不得对 committed 可见");
        assert_eq!(f.last_stable_offset, 0);

        // uncommitted：offset 0 < hw(1) → 立即读
        let fu = fetch(&tx, 0, Isolation::ReadUncommitted, Duration::from_secs(1)).await;
        let hs = data_batches(&fu);
        assert_eq!(hs.len(), 1);
        assert!(hs[0].is_transactional() && !hs[0].is_control());

        drop(tx);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// commit marker：LSO 释放、数据对 committed 可见、控制批不投递。
    #[tokio::test]
    async fn commit_marker_releases_visibility_and_is_not_delivered() {
        let dir = fresh_dir("commit");
        let tx = spawn_on(dir.clone(), Duration::from_secs(60)).await;
        produce(&tx, batch_bytes_txn(500, 0, 0, "m")).await;
        let m = marker(&tx, 500, 0, basalt_record::ControlRecordType::Commit).await;
        assert_eq!(m.unwrap(), 1, "marker 占 offset 1");

        let f = fetch(&tx, 0, Isolation::ReadCommitted, Duration::from_secs(1)).await;
        assert!(f.error.is_none());
        assert_eq!(f.last_stable_offset, 2, "无开事务 → LSO = HW");
        let hs = data_batches(&f);
        assert_eq!(hs.len(), 1, "只回数据批，marker 控制批不投递");
        assert!(!hs[0].is_control() && hs[0].is_transactional());

        drop(tx);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// abort marker：数据对 committed 不可见、LSO 释放（投递模型 (b) 不发
    /// 条目）；uncommitted 仍可读（且控制批同样剥离）。
    #[tokio::test]
    async fn abort_marker_filters_data_and_releases_lso() {
        let dir = fresh_dir("abort");
        let tx = spawn_on(dir.clone(), Duration::from_secs(60)).await;
        produce(&tx, batch_bytes_txn(500, 0, 0, "m")).await;
        assert_eq!(marker(&tx, 500, 0, basalt_record::ControlRecordType::Abort).await.unwrap(), 1);

        let f = fetch(&tx, 0, Isolation::ReadCommitted, Duration::from_secs(1)).await;
        assert!(f.result.as_ref().map(|r| r.data.is_empty()).unwrap_or(true), "aborted 数据对 committed 不可见（投递模型 (b)：服务端预过滤，列表恒空）");
        assert_eq!(f.last_stable_offset, 2);

        let fu = fetch(&tx, 0, Isolation::ReadUncommitted, Duration::from_secs(1)).await;
        let hs = data_batches(&fu);
        assert_eq!(hs.len(), 1, "uncommitted 读到数据批、控制批仍被剥离");
        assert!(!hs[0].is_control());

        drop(tx);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// marker 幂等（同 outcome no-op 不占 offset）+ 相异 outcome 拒绝
    /// （COMMIT 永不覆盖 ABORT 终态，ADR-18 §4.3）。
    #[tokio::test]
    async fn marker_idempotent_and_mismatch_rejected() {
        let dir = fresh_dir("idem");
        let tx = spawn_on(dir.clone(), Duration::from_secs(60)).await;
        produce(&tx, batch_bytes_txn(500, 0, 0, "m")).await;
        assert_eq!(marker(&tx, 500, 0, basalt_record::ControlRecordType::Commit).await.unwrap(), 1);
        assert_eq!(marker(&tx, 500, 0, basalt_record::ControlRecordType::Commit).await.unwrap(), -1, "重复 marker no-op");

        let (ltx, lrx) = oneshot::channel();
        tx.send(PartitionCmd::LocalLeo { reply: ltx }).await.unwrap();
        assert_eq!(lrx.await.unwrap(), 2, "no-op 不占新 offset");

        let r = marker(&tx, 500, 0, basalt_record::ControlRecordType::Abort).await;
        assert!(matches!(r, Err(StorageError::InvalidTxnState)), "相异 outcome 必须拒绝：{:?}", r);

        drop(tx);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 终态 fence：同 (pid,epoch) 终态后拒绝新事务批（LSO 永久停滞的
    /// 可用性洞封口）；epoch 升级重置会话后放行。
    #[tokio::test]
    async fn terminal_fence_rejects_txn_data_after_marker() {
        let dir = fresh_dir("fence");
        let tx = spawn_on(dir.clone(), Duration::from_secs(60)).await;
        produce(&tx, batch_bytes_txn(500, 0, 0, "m")).await;
        marker(&tx, 500, 0, basalt_record::ControlRecordType::Commit).await.unwrap();

        let o = produce(&tx, batch_bytes_txn(500, 0, 1, "zombie")).await;
        assert!(matches!(o.error, Some(StorageError::InvalidTxnState)), "终态后同 epoch 事务批必须被拒：{:?}", o.error);
        let (ltx, lrx) = oneshot::channel();
        tx.send(PartitionCmd::LocalLeo { reply: ltx }).await.unwrap();
        assert_eq!(lrx.await.unwrap(), 2, "被拒批不得推进 LEO");

        let o2 = produce(&tx, batch_bytes_txn(500, 1, 0, "next")).await;
        assert!(o2.error.is_none() && o2.base_offset == 2, "新 epoch 会话放行：{:?}", o2.error);

        drop(tx);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 挂起 committed fetch 超时回包不得泄露（on_deadline 三接触点之三）。
    #[tokio::test]
    async fn committed_pending_fetch_timeout_does_not_leak() {
        let dir = fresh_dir("leak");
        let tx = spawn_on(dir.clone(), Duration::from_secs(60)).await;
        produce(&tx, batch_bytes_txn(500, 0, 0, "m")).await;

        let (ftx, mut frx) = oneshot::channel();
        tx.send(PartitionCmd::Fetch {
            offset: 0, max_bytes: 1 << 20,
            deadline: Instant::now() + Duration::from_millis(120),
            isolation: Isolation::ReadCommitted, reply: ftx,
        }).await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(frx.try_recv().is_err(), "超时前不得应答");
        let f = tokio::time::timeout(Duration::from_secs(2), frx).await.unwrap().unwrap();
        assert!(f.error.is_none());
        assert!(f.result.as_ref().map(|r| r.data.is_empty()).unwrap_or(true), "超时回包必须按 LSO 封顶，不得泄露开事务数据");
        assert_eq!(f.last_stable_offset, 0);

        drop(tx);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// review P1-1 回归：follower 上 deadline 到期不得自 abort（绕过复制层
    /// 写日志 = 副本分叉，TruncateTo fencing 同款纪律）；锚保留待升任收敛。
    #[tokio::test]
    async fn follower_does_not_self_abort() {
        let dir = fresh_dir("followersweep");
        let tx = spawn_on(dir.clone(), Duration::from_millis(120)).await;
        produce(&tx, batch_bytes_txn(800, 0, 0, "m")).await;
        // 降为 follower：deadline 到期也不得写本地日志
        tx.send(PartitionCmd::SetRole { leader: false, epoch: 2, replicas: vec![0, 1] }).await.unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;

        let (ltx, lrx) = oneshot::channel();
        tx.send(PartitionCmd::LocalLeo { reply: ltx }).await.unwrap();
        assert_eq!(lrx.await.unwrap(), 1, "follower 不得自 abort 推进 LEO（副本分叉）");

        // 重新升任：锚仍在 → 升任后 sweep 收敛
        tx.send(PartitionCmd::SetRole { leader: true, epoch: 3, replicas: vec![0] }).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let (ltx2, lrx2) = oneshot::channel();
        tx.send(PartitionCmd::LocalLeo { reply: ltx2 }).await.unwrap();
        assert_eq!(lrx2.await.unwrap(), 2, "升任后 deadline 兜底收敛（ABORT marker 落 offset 1）");

        drop(tx);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 恢复收割（§4.4）：aborted 过滤与终态 fence 跨重启有效。
    #[tokio::test]
    async fn recovery_harvest_rebuilds_filter_and_fence() {
        let dir = fresh_dir("harvest");
        let tx = spawn_on(dir.clone(), Duration::from_secs(60)).await;
        produce(&tx, batch_bytes_txn(500, 0, 0, "keep")).await;
        marker(&tx, 500, 0, basalt_record::ControlRecordType::Commit).await.unwrap();
        produce(&tx, batch_bytes_txn(501, 0, 0, "drop")).await;
        marker(&tx, 501, 0, basalt_record::ControlRecordType::Abort).await.unwrap();
        drop(tx);

        let tx2 = spawn_on(dir.clone(), Duration::from_secs(60)).await;
        let f = fetch(&tx2, 0, Isolation::ReadCommitted, Duration::from_secs(1)).await;
        let hs = data_batches(&f);
        assert_eq!(hs.len(), 1, "重启后 aborted 批仍被过滤（收割重建 aborted 集）");
        assert_eq!(hs[0].base_offset, 0, "留下的是 commit 会话数据");
        assert_eq!(f.last_stable_offset, 4);

        // 终态 fence 跨重启：harvest last_marker 拒绝同 epoch 僵尸续写
        let o = produce(&tx2, batch_bytes_txn(500, 0, 1, "zombie")).await;
        assert!(matches!(o.error, Some(StorageError::InvalidTxnState)), "{:?}", o.error);

        drop(tx2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 恢复收割：崩溃时未关事务的 LSO 锚保持（abort 落地前不泄露）。
    #[tokio::test]
    async fn recovery_harvest_anchors_lso_for_open_txn() {
        let dir = fresh_dir("openharvest");
        let tx = spawn_on(dir.clone(), Duration::from_secs(60)).await;
        produce(&tx, batch_bytes_txn(600, 0, 0, "m")).await;
        drop(tx);

        let tx2 = spawn_on(dir.clone(), Duration::from_secs(60)).await;
        let f = fetch(&tx2, 0, Isolation::ReadCommitted, Duration::from_secs(1)).await;
        assert!(f.result.as_ref().map(|r| r.data.is_empty()).unwrap_or(true), "重启后开事务数据仍不可见");
        assert_eq!(f.last_stable_offset, 0, "LSO 锚由收割重建");
        // 无 marker：无终态 fence，同会话可续写
        let o = produce(&tx2, batch_bytes_txn(600, 0, 1, "m2")).await;
        assert!(o.error.is_none() && o.base_offset == 1, "{:?}", o.error);

        drop(tx2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 分区侧 deadline 自 abort：coordinator 失联/漂移批的兜底（§4.1）。
    #[tokio::test]
    async fn deadline_self_abort_releases_lso() {
        let dir = fresh_dir("selfabort");
        let tx = spawn_on(dir.clone(), Duration::from_millis(100)).await;
        produce(&tx, batch_bytes_txn(700, 0, 0, "m")).await;

        // deadline 纳入唤醒源：100ms 后 loop 超时 → on_deadline → sweep
        tokio::time::sleep(Duration::from_millis(400)).await;

        let f = fetch(&tx, 0, Isolation::ReadCommitted, Duration::from_secs(1)).await;
        assert!(f.result.as_ref().map(|r| r.data.is_empty()).unwrap_or(true), "自 abort 后数据对 committed 不可见");
        assert_eq!(f.last_stable_offset, 2, "自 abort 落 ABORT marker（offset 1）并释放 LSO");

        let fu = fetch(&tx, 0, Isolation::ReadUncommitted, Duration::from_secs(1)).await;
        assert_eq!(data_batches(&fu).len(), 1, "uncommitted 仍见数据批（marker 已剥离）");

        let f2 = fetch(&tx, 1, Isolation::ReadCommitted, Duration::from_secs(1)).await;
        assert_eq!(f2.last_stable_offset, 2, "LSO 释放 = HW");

        drop(tx);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod retry_storm_tests {
    //! T-M3.1 收口：重试风暴下的去重稳定性（确定性，无随机）。
    //! 场景：客户端把"ack 丢失"的生产原样重试 N 轮（交错两 PID、两会话）——
    //! 每逻辑批 offset 恒定（缓存回放）、LEO 不漂移；epoch 升级后旧会话
    //! 重投被拒（duplicate sequence too old），新会话全新追加。

    use super::*;
    use basalt_record::{encode_batch, Rec};
    use basalt_storage::log::{FsyncSchedule, LogOptions};
    use std::collections::HashMap;

    use idempotence_tests::{batch_bytes_idem, spawn_leader, leo, produce};

    #[tokio::test]
    async fn retry_storm_dedup_stability() {
        let (tx, dir) = spawn_leader("storm").await;

        // 风暴：2 PID × 3 逻辑批 × 5 轮重试（PID 交替、轮内 seq 升序）
        let mut offset_of: HashMap<String, i64> = HashMap::new();
        for round in 0..5u32 {
            for pid in [100i64, 200i64] {
                for seq in 0..3i32 {
                    let v = format!("m{pid}-{seq}");
                    let o = produce(&tx, pid, 0, seq, &v).await;
                    assert!(o.error.is_none(), "round{round} {v}: {:?}", o.error);
                    match offset_of.get(&v) {
                        Some(prev) => assert_eq!(*prev, o.base_offset,
                            "round{round} {v}: 重试偏移漂移 {} -> {}", prev, o.base_offset),
                        None => { offset_of.insert(v, o.base_offset); }
                    }
                }
            }
            assert_eq!(leo(&tx).await, 6, "round{round}: LEO 漂移（重复批不得推进）");
        }

        // epoch 升级（新会话）：重置后同 seq 全新追加，offset 前进
        let o = produce(&tx, 100, 1, 0, "m100-0-e1").await;
        assert!(o.error.is_none(), "{:?}", o.error);
        assert_eq!(o.base_offset, 6, "新会话必须全新追加");

        // 旧会话（epoch 0）重投：缓存已随升级清空 → duplicate sequence too old
        let o = produce(&tx, 100, 0, 0, "m100-0-stale").await;
        assert!(o.error.is_some(), "旧会话重投必须被拒");
        assert_eq!(leo(&tx).await, 7, "被拒批不得推进 LEO");

        drop(tx);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
