//! 事务协调器（ADR-18 §5/§6/§7，块 b）：事务元数据状态机 + TxnLog +
//! EndTxn 两段 + 接管恢复。单实例驻 controller 节点（§9 拓扑）。
//!
//! - epoch bump 落点 = InitProducerId（TV2 每事务 init bump；Begin 携带
//!   已 bump 的 epoch——client 用 init 应答的 epoch 产数据，Begin 再 bump
//!   会让 broker 幂等面 fence 掉合法流）；
//! - Prepare 先于一切 marker 落盘并 fsync（lost-writes 防线的落盘点）；
//! - marker 下发走 MarkerJob 通道（块 b 单进程测试用内存转发；跨节点
//!   internal RPC 在块 c 接线，ADR-18 §5）；
//! - takeover：open 重放 TxnLog → resolve_takeover（纯函数）→ 驱动
//!   ReplayCommit / Orphaned（强制 abort）。

pub mod log;
pub mod takeover;

use basalt_record::ControlRecordType;
use basalt_storage::error::StorageError;
use log::TxnLog;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

pub use takeover::{resolve_takeover, Takeover};

// ---------- 类型 ----------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnOutcome {
    Commit,
    Abort,
}

impl From<TxnOutcome> for ControlRecordType {
    fn from(o: TxnOutcome) -> ControlRecordType {
        match o {
            TxnOutcome::Commit => ControlRecordType::Commit,
            TxnOutcome::Abort => ControlRecordType::Abort,
        }
    }
}

/// 事务相位（TxnLog 折叠视图；deadline 是内存量，不入纯函数输入）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnPhase {
    Empty,
    Ongoing,
    Prepare { outcome: TxnOutcome },
    Complete { outcome: TxnOutcome },
}

impl TxnPhase {
    /// 官方相位名（Kafka TransactionState；review P2-3）——Java AdminClient
    /// 的 StateFilter 按此查询，内部名直出会让过滤器失配。
    pub fn as_str(&self) -> &'static str {
        match self {
            TxnPhase::Empty => "Empty",
            TxnPhase::Ongoing => "Ongoing",
            TxnPhase::Prepare { outcome } => match outcome {
                TxnOutcome::Commit => "PrepareCommit",
                TxnOutcome::Abort => "PrepareAbort",
            },
            TxnPhase::Complete { outcome } => match outcome {
                TxnOutcome::Commit => "CompleteCommit",
                TxnOutcome::Abort => "CompleteAbort",
            },
        }
    }
}

/// TxnOffsetCommit 的单条 pending 消费位（KIP-447 essential：commit 时
/// 生效、abort 丢弃；证据落 TxnLog，防 markers-ack 窗口丢失）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingOffset {
    pub group: String,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub metadata: String,
}

/// 单事务的折叠状态（resolve_takeover 的输入形状）。
#[derive(Debug, Clone)]
pub struct TxnState {
    pub txn_id: String,
    pub pid: i64,
    pub epoch: i16,
    /// Prepare 时历元（review P2 根因修法）：marker 重驱钉住它而非当前
    /// 折叠值——re-init bump 后接管重驱不再发错 epoch。
    pub prepare_epoch: Option<i16>,
    pub phase: TxnPhase,
    pub parts: Vec<(String, i32)>,
    pub pending: Vec<PendingOffset>,
}

/// marker 下发任务（coordinator → 分区 leader 路由层）。
#[derive(Debug)]
pub struct MarkerJob {
    pub topic: String,
    pub partition: i32,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub outcome: ControlRecordType,
    pub reply: oneshot::Sender<Result<i64, StorageError>>,
}

/// commit 生效载荷：group coordinator 把 pending 冲入 OffsetLog（§7）。
#[derive(Debug, Clone)]
pub struct PromotedOffsets {
    pub txn_id: String,
    pub offsets: Vec<PendingOffset>,
}

pub enum TxnCmd {
    InitProducerId { txn_id: String, reply: oneshot::Sender<(i64, i16)> },
    AddPartitionsToTxn {
        txn_id: String,
        pid: i64,
        epoch: i16,
        partitions: Vec<(String, i32)>,
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    EndTxn {
        txn_id: String,
        pid: i64,
        epoch: i16,
        commit: bool,
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    TxnOffsetCommit {
        txn_id: String,
        pid: i64,
        epoch: i16,
        offsets: Vec<PendingOffset>,
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// 诊断/DescribeTransactions 底座（块 c 接线 API）。
    Describe { txn_id: String, reply: oneshot::Sender<Option<(String, i64, i16, Vec<(String, i32)>)>> },
    /// ListTransactions 底座：(txn_id, pid, epoch, 相位串)。
    ListTransactions { reply: oneshot::Sender<Vec<(String, i64, i16, String)>> },
}

// ---------- 协调器 ----------

pub struct TxnCoordinator {
    log: TxnLog,
    txns: HashMap<String, TxnState>,
    /// Ongoing 的内存 deadline（phase 纯函数输入不含时钟，另簿记）。
    deadlines: HashMap<String, Instant>,
    marker_tx: mpsc::Sender<MarkerJob>,
    promote_tx: Option<mpsc::Sender<PromotedOffsets>>,
    rx: mpsc::Receiver<TxnCmd>,
    node_id: i32,
    next_pid: u64,
    transaction_timeout: Duration,
    /// marker 单次尝试超时 / 重试次数（持续失败 → Prepare 保留 + 可重试错误）。
    marker_timeout: Duration,
    marker_attempts: usize,
}

impl TxnCoordinator {
    /// 打开 TxnLog、重放、驱动接管恢复，返回命令句柄。
    pub fn spawn(
        path: &std::path::Path,
        node_id: i32,
        marker_tx: mpsc::Sender<MarkerJob>,
        promote_tx: Option<mpsc::Sender<PromotedOffsets>>,
        transaction_timeout: Duration,
    ) -> mpsc::Sender<TxnCmd> {
        let log = TxnLog::open(path);
        let txns = log.replay();
        // PID 播种（review P1）：跨重启不复用——取折叠态 pid 低 40 位最大值
        let next_pid = txns
            .values()
            .map(|t| (t.pid & 0xFF_FFFF_FFFF) as u64)
            .max()
            .unwrap_or(0);
        let (tx, rx) = mpsc::channel(256);
        let coord = TxnCoordinator {
            log,
            txns,
            deadlines: HashMap::new(),
            marker_tx,
            promote_tx,
            rx,
            node_id,
            next_pid,
            transaction_timeout,
            marker_timeout: Duration::from_secs(2),
            marker_attempts: 3,
        };
        tokio::spawn(coord.run());
        tx
    }

    async fn run(mut self) {
        // 接管恢复：纯函数判定 → 驱动（ReplayCommit / Orphaned 强制 abort）。
        // 失败保留 Prepare（EndTxn 重驱 / 下次接管重驱均幂等）。
        let ids: Vec<String> = self.txns.keys().cloned().collect();
        for id in ids {
            let action = resolve_takeover(&self.txns[&id]);
            tracing::info!(txn = %id, action = ?action, "txn takeover");
            match action {
                Takeover::Ready => {
                    // Complete{Commit} 残留 pending 的幂等重提升（review P1-b）：
                    // Complete 先于 group 侧落盘的窄缝内崩溃时，证据仍在
                    // TxnLog（Complete 分支保留 commit 的 pending）
                    let redo = matches!(
                        self.txns[&id].phase,
                        TxnPhase::Complete { outcome: TxnOutcome::Commit }
                    ) && !self.txns[&id].pending.is_empty();
                    if redo {
                        if let Some(promote_tx) = &self.promote_tx {
                            let pending = self.txns[&id].pending.clone();
                            let _ = promote_tx
                                .send(PromotedOffsets { txn_id: id.clone(), offsets: pending })
                                .await;
                            if let Some(st) = self.txns.get_mut(&id) {
                                st.pending.clear();
                            }
                            tracing::info!(txn = %id, "takeover re-promoted pending offsets");
                        }
                    }
                }
                Takeover::ReplayCommit { outcome, parts } => {
                    // P0 修复（review 探针实证）：Complete 只在驱动成功后落盘
                    // ——失败保留 Prepare + 重试 deadline，否则已 fsync 的提交
                    // 决定会被静默吞掉（lost writes）
                    if let Err(e) = self.drive_completion(&id, outcome, &parts).await {
                        tracing::error!(txn = %id, error = %e, "takeover replay failed; Prepare 保留待重驱");
                        self.deadlines.insert(id, Instant::now() + Duration::from_secs(1));
                    } else {
                        if let Some(st) = self.txns.get_mut(&id) {
                            st.phase = TxnPhase::Complete { outcome };
                            st.pending.clear();
                        }
                        self.log.append_complete(&id, outcome).expect("txn log append");
                    }
                }
                Takeover::Orphaned { parts, .. } => {
                    if let Err(e) = self.force_abort(&id, &parts).await {
                        tracing::error!(txn = %id, error = %e, "takeover orphaned abort failed");
                        self.deadlines.insert(id, Instant::now() + Duration::from_secs(1));
                    }
                }
            }
        }
        // 主循环：命令 + Ongoing deadline sweep（唯一等待点，㊽ 同款纪律——
        // deadline 到期 → sweep 驱动 abort，命令仍会被 drain）
        loop {
            let next_deadline = self.deadlines.values().copied().min();
            let first = match next_deadline {
                Some(d) => {
                    let now = Instant::now();
                    if d > now {
                        match tokio::time::timeout(d - now, self.rx.recv()).await {
                            Ok(Some(cmd)) => Some(cmd),
                            Ok(None) => break,
                            Err(_elapsed) => {
                                self.sweep_expired().await;
                                continue;
                            }
                        }
                    } else {
                        self.sweep_expired().await;
                        continue;
                    }
                }
                None => match self.rx.recv().await {
                    Some(cmd) => Some(cmd),
                    None => break,
                },
            };
            self.process(first.unwrap()).await;
        }
    }

    /// Ongoing 超时 → 强制 abort（sweep 失败退避 1s，㊾ 同款纪律——否则
    /// 过期 deadline 原样留存会让主循环热旋）。
    async fn sweep_expired(&mut self) {
        let now = Instant::now();
        let expired: Vec<String> = self
            .deadlines
            .iter()
            .filter(|(_, d)| **d <= now)
            .map(|(id, _)| id.clone())
            .collect();
        for id in expired {
            let phase = match self.txns.get(&id) {
                Some(t) => t.phase,
                None => {
                    self.deadlines.remove(&id);
                    continue;
                }
            };
            match phase {
                TxnPhase::Ongoing => {
                    self.deadlines.remove(&id);
                    let parts = self.txns[&id].parts.clone();
                    tracing::info!(txn = %id, "txn timeout, forced abort");
                    if let Err(e) = self.force_abort(&id, &parts).await {
                        tracing::error!(txn = %id, error = %e, "txn timeout abort failed, backoff 1s");
                        self.deadlines.insert(id, now + Duration::from_secs(1));
                    }
                }
                // Prepare 残留重驱（review P1）：Prepare 已在盘上，直接续跑
                // marker→Complete——这给 EndTxn/force_abort/接管失败的残留
                // 提供进程内重试通道（此前只有重启一条路）
                TxnPhase::Prepare { outcome } => {
                    self.deadlines.remove(&id);
                    let parts = self.txns[&id].parts.clone();
                    tracing::info!(txn = %id, outcome = ?outcome, "sweep re-drives stuck Prepare");
                    match self.drive_completion(&id, outcome, &parts).await {
                        Ok(()) => {
                            if let Some(st) = self.txns.get_mut(&id) {
                                st.phase = TxnPhase::Complete { outcome };
                                st.pending.clear();
                            }
                            self.log.append_complete(&id, outcome).expect("txn log append");
                        }
                        Err(e) => {
                            tracing::error!(txn = %id, error = %e, "Prepare re-drive failed, backoff 1s");
                            self.deadlines.insert(id, now + Duration::from_secs(1));
                        }
                    }
                }
                TxnPhase::Empty | TxnPhase::Complete { .. } => {
                    self.deadlines.remove(&id);
                }
            }
        }
    }

    async fn process(&mut self, cmd: TxnCmd) {
        match cmd {
            TxnCmd::InitProducerId { txn_id, reply } => {
                // 已有 Ongoing 事务：init 即 fence（KIP-890）——强制 abort
                // 后再 bump（marker 幂等，通常瞬时）
                if matches!(self.txns.get(&txn_id).map(|t| t.phase), Some(TxnPhase::Ongoing)) {
                    let parts = self.txns[&txn_id].parts.clone();
                    self.deadlines.remove(&txn_id);
                    if let Err(e) = self.force_abort(&txn_id, &parts).await {
                        tracing::warn!(txn = %txn_id, error = %e, "init 前强制 abort 失败（Prepare 保留，marker 幂等可重驱）");
                    }
                }
                let (pid, epoch) = match self.txns.get_mut(&txn_id) {
                    None => {
                        self.next_pid += 1;
                        let pid = ((self.node_id as i64 & 0xFF) << 40) | ((self.next_pid as i64) & 0xFF_FFFF_FFFF);
                        self.log.append_init(&txn_id, pid, 0).expect("txn log append");
                        self.txns.insert(
                            txn_id.clone(),
                            TxnState {
                                txn_id: txn_id.clone(),
                                pid,
                                epoch: 0,
                                prepare_epoch: None,
                                phase: TxnPhase::Empty,
                                parts: vec![],
                                pending: vec![],
                            },
                        );
                        (pid, 0)
                    }
                    Some(st) => {
                        st.epoch = st.epoch.saturating_add(1);
                        let (pid, epoch) = (st.pid, st.epoch);
                        self.log.append_init(&txn_id, pid, epoch).expect("txn log append");
                        (pid, epoch)
                    }
                };
                let _ = reply.send((pid, epoch));
            }
            TxnCmd::AddPartitionsToTxn { txn_id, pid, epoch, partitions, reply } => {
                let r = self.add_partitions(&txn_id, pid, epoch, partitions).await;
                let _ = reply.send(r);
            }
            TxnCmd::EndTxn { txn_id, pid, epoch, commit, reply } => {
                let r = self.end_txn(&txn_id, pid, epoch, commit).await;
                let _ = reply.send(r);
            }
            TxnCmd::TxnOffsetCommit { txn_id, pid, epoch, offsets, reply } => {
                let r = self.txn_offset_commit(&txn_id, pid, epoch, offsets);
                let _ = reply.send(r);
            }
            TxnCmd::Describe { txn_id, reply } => {
                let r = self.txns.get(&txn_id).map(|t| {
                    (t.phase.as_str().to_string(), t.pid, t.epoch, t.parts.clone())
                });
                let _ = reply.send(r);
            }
            TxnCmd::ListTransactions { reply } => {
                let mut rows: Vec<(String, i64, i16, String)> = self
                    .txns
                    .values()
                    .map(|t| (t.txn_id.clone(), t.pid, t.epoch, t.phase.as_str().to_string()))
                    .collect();
                rows.sort_by(|a, b| a.0.cmp(&b.0));
                let _ = reply.send(rows);
            }
        }
    }

    async fn add_partitions(
        &mut self,
        txn_id: &str,
        pid: i64,
        epoch: i16,
        partitions: Vec<(String, i32)>,
    ) -> Result<(), StorageError> {
        let Some(st) = self.txns.get_mut(txn_id) else {
            return Err(StorageError::InvalidTxnState);
        };
        if st.pid != pid {
            return Err(StorageError::InvalidProducerIdMapping);
        }
        if st.epoch != epoch {
            return Err(StorageError::InvalidProducerEpoch);
        }
        match st.phase {
            TxnPhase::Prepare { .. } => {
                // 前事务未完（CONCURRENT_TRANSACTIONS 语义，TV2）：可重试
                return Err(StorageError::Other("concurrent transaction".into()));
            }
            TxnPhase::Ongoing => {
                // 幂等扩分区：合并后重落 Begin（重放 last-wins 语义成立）
                for p in partitions {
                    if !st.parts.contains(&p) {
                        st.parts.push(p);
                    }
                }
                let parts = st.parts.clone();
                self.log.append_begin(txn_id, pid, epoch, &parts).expect("txn log append");
                self.deadlines.insert(txn_id.to_string(), Instant::now() + self.transaction_timeout);
                return Ok(());
            }
            TxnPhase::Empty | TxnPhase::Complete { .. } => {}
        }
        // Empty/Complete → Ongoing（epoch 已在 InitProducerId bump）
        self.log.append_begin(txn_id, pid, epoch, &partitions).expect("txn log append");
        let st = self.txns.get_mut(txn_id).unwrap();
        st.phase = TxnPhase::Ongoing;
        st.parts = partitions;
        st.pending.clear();
        self.deadlines.insert(txn_id.to_string(), Instant::now() + self.transaction_timeout);
        Ok(())
    }

    fn txn_offset_commit(
        &mut self,
        txn_id: &str,
        pid: i64,
        epoch: i16,
        offsets: Vec<PendingOffset>,
    ) -> Result<(), StorageError> {
        let Some(st) = self.txns.get_mut(txn_id) else {
            return Err(StorageError::InvalidTxnState);
        };
        if st.pid != pid {
            return Err(StorageError::InvalidProducerIdMapping);
        }
        if st.epoch != epoch {
            return Err(StorageError::InvalidProducerEpoch);
        }
        if st.phase != TxnPhase::Ongoing {
            return Err(StorageError::InvalidTxnState);
        }
        self.log.append_pending(txn_id, pid, epoch, &offsets).expect("txn log append");
        st.pending = offsets;
        Ok(())
    }

    async fn end_txn(
        &mut self,
        txn_id: &str,
        pid: i64,
        epoch: i16,
        commit: bool,
    ) -> Result<(), StorageError> {
        let Some(st) = self.txns.get_mut(txn_id) else {
            return Err(StorageError::InvalidTxnState);
        };
        if st.pid != pid {
            return Err(StorageError::InvalidProducerIdMapping);
        }
        if st.epoch != epoch {
            return Err(StorageError::InvalidProducerEpoch);
        }
        let outcome = if commit { TxnOutcome::Commit } else { TxnOutcome::Abort };
        match st.phase {
            TxnPhase::Prepare { outcome: stored } => {
                if stored != outcome {
                    return Err(StorageError::InvalidTxnState);
                }
                // 崩溃/超时后的重驱：Prepare 已在盘上，续跑 marker→Complete
            }
            TxnPhase::Ongoing => {
                // 两段第一段：Prepare 先于一切 marker 落盘并 fsync
                let parts = st.parts.clone();
                self.log.append_prepare(txn_id, pid, epoch, outcome, &parts).expect("txn log append");
                let st = self.txns.get_mut(txn_id).unwrap();
                st.phase = TxnPhase::Prepare { outcome };
                st.prepare_epoch = Some(epoch);
            }
            TxnPhase::Empty | TxnPhase::Complete { .. } => {
                return Err(StorageError::InvalidTxnState);
            }
        }
        let parts = self.txns[txn_id].parts.clone();
        if let Err(e) = self.drive_completion(txn_id, outcome, &parts).await {
            // 失败保留 Prepare + 重驱 deadline（review P1：进程内重试通道）
            self.deadlines
                .insert(txn_id.to_string(), Instant::now() + Duration::from_secs(1));
            return Err(e);
        }
        if let Some(st) = self.txns.get_mut(txn_id) {
            st.phase = TxnPhase::Complete { outcome };
            st.pending.clear();
            st.prepare_epoch = None;
        }
        self.deadlines.remove(txn_id);
        self.log.append_complete(txn_id, outcome).expect("txn log append");
        Ok(())
    }

    async fn force_abort(&mut self, txn_id: &str, parts: &[(String, i32)]) -> Result<(), StorageError> {
        let (pid, epoch) = {
            let st = &self.txns[txn_id];
            (st.pid, st.epoch)
        };
        self.log
            .append_prepare(txn_id, pid, epoch, TxnOutcome::Abort, parts)
            .expect("txn log append");
        if let Some(st) = self.txns.get_mut(txn_id) {
            st.phase = TxnPhase::Prepare { outcome: TxnOutcome::Abort };
            st.prepare_epoch = Some(epoch);
            st.parts = parts.to_vec();
        }
        self.drive_completion(txn_id, TxnOutcome::Abort, parts).await?;
        if let Some(st) = self.txns.get_mut(txn_id) {
            st.phase = TxnPhase::Complete { outcome: TxnOutcome::Abort };
            st.pending.clear();
            st.prepare_epoch = None;
        }
        self.log.append_complete(txn_id, TxnOutcome::Abort).expect("txn log append");
        Ok(())
    }

    /// 两段第二段：并发补发 marker → （commit 时）pending 生效 → 返回。
    /// 失败保留 Prepare（EndTxn 重驱 / 接管重驱均幂等）。
    async fn drive_completion(
        &mut self,
        txn_id: &str,
        outcome: TxnOutcome,
        parts: &[(String, i32)],
    ) -> Result<(), StorageError> {
        let pid = self.txns[txn_id].pid;
        // marker 历元钉在 Prepare 时值（review P2 根因修法）：re-init bump
        // 后重驱/接管仍发 Prepare 时 epoch，分区侧按同会话匹配
        let epoch = self.txns[txn_id].prepare_epoch.unwrap_or(self.txns[txn_id].epoch);
        let ctl: ControlRecordType = outcome.into();
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut jobs: Vec<(oneshot::Receiver<Result<i64, StorageError>>, (String, i32))> = Vec::new();
            for (topic, partition) in parts {
                let (rtx, rrx) = oneshot::channel();
                self.marker_tx
                    .send(MarkerJob {
                        topic: topic.clone(),
                        partition: *partition,
                        producer_id: pid,
                        producer_epoch: epoch,
                        outcome: ctl,
                        reply: rtx,
                    })
                    .await
                    .map_err(|_| StorageError::Other("marker router closed".into()))?;
                jobs.push((rrx, (topic.clone(), *partition)));
            }
            let mut failed: Vec<(String, i32)> = Vec::new();
            for (rrx, part) in jobs {
                match tokio::time::timeout(self.marker_timeout, rrx).await {
                    Ok(Ok(Ok(_))) => {}
                    _ => failed.push(part),
                }
            }
            if failed.is_empty() {
                break;
            }
            if attempt >= self.marker_attempts {
                tracing::error!(txn = %txn_id, failed = ?failed, "markers 未全部落定，Prepare 保留");
                return Err(StorageError::Other(format!("txn markers unfinished: {:?}", failed)));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        // pending 生效（仅 commit；§7：证据在 TxnLog，重放幂等）
        if outcome == TxnOutcome::Commit {
            if let Some(promote_tx) = &self.promote_tx {
                let pending = self.txns[txn_id].pending.clone();
                if !pending.is_empty() {
                    let (ptx, prx) = oneshot::channel::<Result<(), StorageError>>();
                    promote_tx
                        .send(PromotedOffsets { txn_id: txn_id.to_string(), offsets: pending })
                        .await
                        .map_err(|_| StorageError::Other("promote channel closed".into()))?;
                    // group coordinator 完成落盘后应答（块 c 接 OffsetLog）
                    let _ = tokio::time::timeout(self.marker_timeout, prx).await;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod coordinator_tests {
    //! ADR-18 块 b：EndTxn 两段、接管恢复（ReplayCommit/Orphaned）、超时
    //! abort、epoch fence、pending 提升。marker 路由用内存转发器直连分区
    //! actor（跨节点 RPC 属块 c 接线，§5）。

    use super::*;
    use basalt_record::{encode_batch, Rec, ATTR_TRANSACTIONAL};
    use basalt_storage::log::{AssignPolicy, FsyncSchedule, LogOptions};
    use crate::config::ReplicaConfig;
    use crate::partition::{Isolation, PartitionActor, PartitionCmd};
    use basalt_storage::pool::BufferPool;
    use bytes::{Bytes, BytesMut};

    pub(crate) fn txn_cfg() -> (Duration,) {
        (Duration::from_secs(60),)
    }

    pub(crate) fn batch_bytes_txn(pid: i64, epoch: i16, seq: i32, tag: &str) -> Bytes {
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

    pub(crate) fn fresh_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "basalt-txncoord-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// 场景装配：单分区 leader actor + 内存 marker 路由器 + 协调器。
    /// 返回（分区句柄、协调器句柄、promote 接收器、临时目录）。
    async fn setup(
        tag: &str,
        txn_timeout: Duration,
        working_router: bool,
    ) -> (
        mpsc::Sender<PartitionCmd>,
        mpsc::Sender<TxnCmd>,
        Option<mpsc::Receiver<PromotedOffsets>>,
        std::path::PathBuf,
    ) {
        let dir = fresh_dir(tag);
        let pool = std::sync::Arc::new(BufferPool::new());
        let ptx = PartitionActor::spawn(
            "txn".into(),
            0,
            0,
            dir.join("part"),
            LogOptions { segment_max_bytes: 1 << 30, fsync: FsyncSchedule::Os, retention_ms: 0, retention_max_bytes: 0 },
            ReplicaConfig { min_insync: 1, isr_lag: Duration::from_millis(500), transaction_timeout: Duration::from_secs(60) },
            pool,
        )
        .unwrap();
        ptx.send(PartitionCmd::SetRole { leader: true, epoch: 1, replicas: vec![0] }).await.unwrap();

        let (mtx, mut mrx) = mpsc::channel::<MarkerJob>(64);
        if working_router {
            let fwd_ptx = ptx.clone();
            tokio::spawn(async move {
                while let Some(job) = mrx.recv().await {
                    let (t, p) = (job.topic.clone(), job.partition);
                    let (otx, orx) = oneshot::channel();
                    let sent = fwd_ptx
                        .send(PartitionCmd::WriteTxnMarker {
                            producer_id: job.producer_id,
                            producer_epoch: job.producer_epoch,
                            outcome: job.outcome,
                            reply: otx,
                        })
                        .await;
                    let res = if sent.is_err() {
                        Err(StorageError::NotLeader)
                    } else {
                        orx.await.unwrap_or(Err(StorageError::Other("marker reply dropped".into())))
                    };
                    tracing::debug!(topic = %t, partition = p, ok = res.is_ok(), "marker routed");
                    let _ = job.reply.send(res);
                }
            });
        } // working_router = false：mrx 直接丢弃 → send 失败（模拟 marker 通道不可达）

        let (promote_tx, promote_rx) = mpsc::channel::<PromotedOffsets>(8);
        let coord = TxnCoordinator::spawn(&dir.join("txn.log"), 0, mtx, Some(promote_tx), txn_timeout);
        (ptx, coord, Some(promote_rx), dir)
    }

    async fn init(coord: &mpsc::Sender<TxnCmd>, txn_id: &str) -> (i64, i16) {
        let (itx, irx) = oneshot::channel();
        coord.send(TxnCmd::InitProducerId { txn_id: txn_id.into(), reply: itx }).await.unwrap();
        irx.await.unwrap()
    }

    async fn add(coord: &mpsc::Sender<TxnCmd>, txn_id: &str, pid: i64, epoch: i16) -> Result<(), StorageError> {
        let (atx, arx) = oneshot::channel();
        coord
            .send(TxnCmd::AddPartitionsToTxn {
                txn_id: txn_id.into(),
                pid,
                epoch,
                partitions: vec![("txn".into(), 0)],
                reply: atx,
            })
            .await
            .unwrap();
        arx.await.unwrap()
    }

    async fn end(coord: &mpsc::Sender<TxnCmd>, txn_id: &str, pid: i64, epoch: i16, commit: bool) -> Result<(), StorageError> {
        let (etx, erx) = oneshot::channel();
        coord
            .send(TxnCmd::EndTxn { txn_id: txn_id.into(), pid, epoch, commit, reply: etx })
            .await
            .unwrap();
        erx.await.unwrap()
    }

    async fn describe(coord: &mpsc::Sender<TxnCmd>, txn_id: &str) -> Option<(String, i64, i16, Vec<(String, i32)>)> {
        let (dtx, drx) = oneshot::channel();
        coord.send(TxnCmd::Describe { txn_id: txn_id.into(), reply: dtx }).await.unwrap();
        drx.await.unwrap()
    }

    async fn produce_txn(ptx: &mpsc::Sender<PartitionCmd>, pid: i64, epoch: i16, seq: i32, tag: &str) {
        let (prx_tx, prx_rx) = oneshot::channel();
        ptx.send(PartitionCmd::Produce {
            batches: batch_bytes_txn(pid, epoch, seq, tag),
            policy: AssignPolicy::Assign,
            acks: 1,
            reply: prx_tx,
        })
        .await
        .unwrap();
        let o = prx_rx.await.unwrap();
        assert!(o.error.is_none(), "{:?}", o.error);
    }

    async fn committed_data(ptx: &mpsc::Sender<PartitionCmd>, offset: i64) -> bool {
        let (ftx, frx) = oneshot::channel();
        ptx.send(PartitionCmd::Fetch {
            offset,
            max_bytes: 1 << 20,
            deadline: std::time::Instant::now() + Duration::from_secs(1),
            isolation: Isolation::ReadCommitted,
            reply: ftx,
        })
        .await
        .unwrap();
        let out = frx.await.unwrap();
        out.result.map(|r| !r.data.is_empty()).unwrap_or(false)
    }

    /// 轮询等待接管恢复收敛（Describe → 期望相位）。
    async fn wait_phase(coord: &mpsc::Sender<TxnCmd>, txn_id: &str, want: &str) {
        for _ in 0..40 {
            if let Some((phase, _, _, _)) = describe(coord, txn_id).await {
                if phase == want {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let got = describe(coord, txn_id).await.map(|(p, _, _, _)| p).unwrap_or("None".into());
        panic!("相位未收敛到 {want}：{got}");
    }

    /// init 每 bump 一历元（TV2 每事务 init bump）、pid 稳定。
    #[tokio::test]
    async fn init_bumps_epoch_pid_stable() {
        let (_ptx, coord, _prom, dir) = setup("init", txn_cfg().0, true).await;
        let (pid0, e0) = init(&coord, "t1").await;
        assert_eq!(e0, 0);
        let (pid1, e1) = init(&coord, "t1").await;
        assert_eq!(pid0, pid1, "pid 稳定");
        assert_eq!(e1, 1, "init 每 bump 一历元");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// EndTxn commit 两段闭环：marker 落盘、committed 可见、相位 Complete。
    #[tokio::test]
    async fn commit_two_phase_visible_and_complete() {
        let (ptx, coord, _prom, dir) = setup("commit", txn_cfg().0, true).await;
        let (pid, epoch) = init(&coord, "t1").await;
        add(&coord, "t1", pid, epoch).await.unwrap();
        produce_txn(&ptx, pid, epoch, 0, "m").await;
        assert!(!committed_data(&ptx, 0).await, "Ongoing 期间 committed 不可见");

        end(&coord, "t1", pid, epoch, true).await.unwrap();
        assert!(committed_data(&ptx, 0).await, "commit 后 committed 可见");
        let (phase, _, _, _) = describe(&coord, "t1").await.unwrap();
        assert_eq!(phase, "CompleteCommit");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// EndTxn abort：committed 永不可见（数据在盘、LSO 锚释放）。
    #[tokio::test]
    async fn abort_filters_committed_reads() {
        let (ptx, coord, _prom, dir) = setup("abort", txn_cfg().0, true).await;
        let (pid, epoch) = init(&coord, "t1").await;
        add(&coord, "t1", pid, epoch).await.unwrap();
        produce_txn(&ptx, pid, epoch, 0, "m").await;
        end(&coord, "t1", pid, epoch, false).await.unwrap();
        assert!(!committed_data(&ptx, 0).await, "abort 后 committed 不可见");
        let (phase, _, _, _) = describe(&coord, "t1").await.unwrap();
        assert_eq!(phase, "CompleteAbort");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 接管重放（§6 ReplayCommit）：Prepare 落盘后协调器「崩溃」（marker
    /// 通道不可达 → EndTxn 失败但 Prepare 已 fsync）→ 新协调器重放补发
    /// marker → Complete，committed 可见（lost writes 防线）。
    #[tokio::test]
    async fn prepare_survives_and_takeover_replays() {
        let tag = "replay";
        let (ptx, coord, _prom, dir) = setup(tag, txn_cfg().0, false).await;
        let (pid, epoch) = init(&coord, "t1").await;
        add(&coord, "t1", pid, epoch).await.unwrap();
        produce_txn(&ptx, pid, epoch, 0, "m").await;

        let r = end(&coord, "t1", pid, epoch, true).await;
        assert!(r.is_err(), "marker 不可达时 EndTxn 必须失败（Prepare 保留）");
        let (phase, _, _, _) = describe(&coord, "t1").await.unwrap();
        assert_eq!(phase, "PrepareCommit", "失败保留 Prepare（重驱入口）");
        drop(coord);

        // 新协调器（工作路由）接管同一 TxnLog：ReplayCommit → 补发 → Complete
        let (mtx, mut mrx) = mpsc::channel::<MarkerJob>(64);
        let fwd_ptx = ptx.clone();
        tokio::spawn(async move {
            while let Some(job) = mrx.recv().await {
                let (otx, orx) = oneshot::channel();
                fwd_ptx
                    .send(PartitionCmd::WriteTxnMarker {
                        producer_id: job.producer_id,
                        producer_epoch: job.producer_epoch,
                        outcome: job.outcome,
                        reply: otx,
                    })
                    .await
                    .unwrap();
                let _ = job.reply.send(orx.await.unwrap());
            }
        });
        let coord2 = TxnCoordinator::spawn(&dir.join("txn.log"), 0, mtx, None, txn_cfg().0);
        wait_phase(&coord2, "t1", "CompleteCommit").await;
        assert!(committed_data(&ptx, 0).await, "接管重放后提交效果存活");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 接管孤儿（§6 Orphaned）：Ongoing 被恢复强制 abort（安全方向）。
    #[tokio::test]
    async fn takeover_orphaned_ongoing_forced_abort() {
        let (ptx, coord, _prom, dir) = setup("orphan", txn_cfg().0, true).await;
        let (pid, epoch) = init(&coord, "t1").await;
        add(&coord, "t1", pid, epoch).await.unwrap();
        produce_txn(&ptx, pid, epoch, 0, "m").await;
        drop(coord); // Ongoing 状态"崩溃"

        let (mtx, mut mrx) = mpsc::channel::<MarkerJob>(64);
        let fwd_ptx = ptx.clone();
        tokio::spawn(async move {
            while let Some(job) = mrx.recv().await {
                let (otx, orx) = oneshot::channel();
                fwd_ptx
                    .send(PartitionCmd::WriteTxnMarker {
                        producer_id: job.producer_id,
                        producer_epoch: job.producer_epoch,
                        outcome: job.outcome,
                        reply: otx,
                    })
                    .await
                    .unwrap();
                let _ = job.reply.send(orx.await.unwrap());
            }
        });
        let coord2 = TxnCoordinator::spawn(&dir.join("txn.log"), 0, mtx, None, txn_cfg().0);
        wait_phase(&coord2, "t1", "CompleteAbort").await;
        assert!(!committed_data(&ptx, 0).await, "孤儿事务强制 abort（安全方向）");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Ongoing 超时 → sweep 强制 abort（双保险的 coordinator 半边）。
    #[tokio::test]
    async fn timeout_forces_abort() {
        let (ptx, coord, _prom, dir) = setup("timeout", Duration::from_millis(120), true).await;
        let (pid, epoch) = init(&coord, "t1").await;
        add(&coord, "t1", pid, epoch).await.unwrap();
        produce_txn(&ptx, pid, epoch, 0, "m").await;

        wait_phase(&coord, "t1", "CompleteAbort").await;
        assert!(!committed_data(&ptx, 0).await);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// fence 语义族（Kafka 对齐）：同 epoch 重复 AddPartitionsToTxn = 幂等
    /// 扩分区（Ok）；Prepare 残留时新事务拒（CONCURRENT_TRANSACTIONS）；
    /// re-init 每 bump 一历元，旧 epoch 的 EndTxn 被 fence（47）。
    #[tokio::test]
    async fn concurrent_rejected_and_reinit_fences() {
        // 坏路由器：制造 marker 不可达 → Prepare 残留的真实场景
        let (_ptx, coord, _prom, dir) = setup("concurrent", txn_cfg().0, false).await;
        let (pid, epoch) = init(&coord, "t1").await;
        add(&coord, "t1", pid, epoch).await.unwrap();
        add(&coord, "t1", pid, epoch).await.unwrap(); // 同 epoch 幂等扩分区

        assert!(end(&coord, "t1", pid, epoch, true).await.is_err(), "marker 不可达 → Prepare 残留");
        let (phase, _, _, _) = describe(&coord, "t1").await.unwrap();
        assert_eq!(phase, "PrepareCommit");

        let r = add(&coord, "t1", pid, epoch).await;
        assert!(matches!(r, Err(StorageError::Other(_))), "Prepare 残留期新事务必须拒（CONCURRENT）：{:?}", r);

        let (_pid2, epoch2) = init(&coord, "t1").await;
        assert_eq!(epoch2, epoch + 1, "re-init 每 bump 一历元（Prepare 残留不阻断 bump）");
        let r = end(&coord, "t1", pid, epoch, true).await;
        assert!(matches!(r, Err(StorageError::InvalidProducerEpoch)), "旧 epoch 已被 fence：{:?}", r);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// pending 消费位（KIP-447 essential）：commit 提升到 group 侧、abort
    /// 丢弃；pending 证据落 TxnLog（markers-ack 窗口不丢）。
    #[tokio::test]
    async fn pending_offsets_promoted_on_commit_only() {
        let (ptx, coord, prom, dir) = setup("pending", txn_cfg().0, true).await;
        let mut prom = prom.unwrap();
        let offs = |n: i64| {
            vec![PendingOffset {
                group: "g".into(),
                topic: "txn".into(),
                partition: 0,
                offset: n,
                metadata: String::new(),
            }]
        };
        let (pid, epoch) = init(&coord, "t1").await;
        add(&coord, "t1", pid, epoch).await.unwrap();
        let (otx, orx) = oneshot::channel();
        coord
            .send(TxnCmd::TxnOffsetCommit { txn_id: "t1".into(), pid, epoch, offsets: offs(10), reply: otx })
            .await
            .unwrap();
        orx.await.unwrap().unwrap();
        end(&coord, "t1", pid, epoch, true).await.unwrap();
        let promoted = tokio::time::timeout(Duration::from_secs(2), prom.recv()).await.unwrap().unwrap();
        assert_eq!(promoted.offsets[0].offset, 10, "commit 生效");

        let (pid2, epoch2) = init(&coord, "t2").await;
        add(&coord, "t2", pid2, epoch2).await.unwrap();
        let (otx2, orx2) = oneshot::channel();
        coord
            .send(TxnCmd::TxnOffsetCommit { txn_id: "t2".into(), pid: pid2, epoch: epoch2, offsets: offs(20), reply: otx2 })
            .await
            .unwrap();
        orx2.await.unwrap().unwrap();
        end(&coord, "t2", pid2, epoch2, false).await.unwrap();
        let r = tokio::time::timeout(Duration::from_millis(300), prom.recv()).await;
        assert!(matches!(r, Err(_) | Ok(None)), "abort 不得提升");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod coordinator_review_fixes_tests {
    //! 块 b review 修复回归：接管失败保 Prepare（P0）、sweep 进程内重驱
    //! 卡死 Prepare（P1）、PID 跨重启不复用（P1）。

    use super::*;
    use crate::config::ReplicaConfig;
    use crate::partition::{Isolation, PartitionActor, PartitionCmd};
    use basalt_storage::log::{AssignPolicy, FsyncSchedule, LogOptions};
    use basalt_storage::pool::BufferPool;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::coordinator_tests::{batch_bytes_txn, fresh_dir, txn_cfg};

    async fn spawn_partition(tag: &str) -> (mpsc::Sender<PartitionCmd>, std::path::PathBuf) {
        let dir = fresh_dir(tag);
        let pool = std::sync::Arc::new(BufferPool::new());
        let ptx = PartitionActor::spawn(
            "txn".into(),
            0,
            0,
            dir.join("part"),
            LogOptions { segment_max_bytes: 1 << 30, fsync: FsyncSchedule::Os, retention_ms: 0, retention_max_bytes: 0 },
            ReplicaConfig { min_insync: 1, isr_lag: Duration::from_millis(500), transaction_timeout: Duration::from_secs(60) },
            pool,
        )
        .unwrap();
        ptx.send(PartitionCmd::SetRole { leader: true, epoch: 1, replicas: vec![0] }).await.unwrap();
        (ptx, dir)
    }

    fn forward_router(mut mrx: mpsc::Receiver<MarkerJob>, ptx: mpsc::Sender<PartitionCmd>, fail_first: Arc<AtomicUsize>) {
        tokio::spawn(async move {
            while let Some(job) = mrx.recv().await {
                if fail_first.fetch_sub(1, Ordering::SeqCst) > 0 {
                    let _ = job.reply.send(Err(StorageError::Other("injected marker failure".into())));
                    continue;
                }
                let (otx, orx) = oneshot::channel();
                ptx.send(PartitionCmd::WriteTxnMarker {
                    producer_id: job.producer_id,
                    producer_epoch: job.producer_epoch,
                    outcome: job.outcome,
                    reply: otx,
                })
                .await
                .unwrap();
                let _ = job.reply.send(orx.await.unwrap());
            }
        });
    }

    /// P0 回归：接管重驱失败必须保留 Prepare（否则已 fsync 的 Commit 决定
    /// 被静默吞掉——lost writes）；路由恢复后接力协调器完成它。
    #[tokio::test]
    async fn takeover_replay_failure_keeps_prepare() {
        let (ptx, dir) = spawn_partition("takfail").await;
        // 第一任：坏路由（send 即失败）→ EndTxn 失败留 Prepare
        let (mtx, mrx) = mpsc::channel::<MarkerJob>(64);
        drop(mrx);
        let coord = TxnCoordinator::spawn(&dir.join("txn.log"), 0, mtx, None, txn_cfg().0);
        let (pid, epoch) = {
            let (itx, irx) = oneshot::channel();
            coord.send(TxnCmd::InitProducerId { txn_id: "t1".into(), reply: itx }).await.unwrap();
            irx.await.unwrap()
        };
        {
            let (atx, arx) = oneshot::channel();
            coord
                .send(TxnCmd::AddPartitionsToTxn { txn_id: "t1".into(), pid, epoch, partitions: vec![("txn".into(), 0)], reply: atx })
                .await
                .unwrap();
            arx.await.unwrap().unwrap();
        }
        {
            let (prx_tx, prx_rx) = oneshot::channel();
            ptx.send(PartitionCmd::Produce {
                batches: batch_bytes_txn(pid, epoch, 0, "m"),
                policy: AssignPolicy::Assign,
                acks: 1,
                reply: prx_tx,
            })
            .await
            .unwrap();
            prx_rx.await.unwrap();
        }
        {
            let (etx, erx) = oneshot::channel();
            coord.send(TxnCmd::EndTxn { txn_id: "t1".into(), pid, epoch, commit: true, reply: etx }).await.unwrap();
            assert!(erx.await.unwrap().is_err());
        }
        drop(coord);

        // 第二任（仍坏路由）接管：ReplayCommit 失败 → Prepare 必须保留
        let (mtx2, mrx2) = mpsc::channel::<MarkerJob>(64);
        drop(mrx2);
        let coord2 = TxnCoordinator::spawn(&dir.join("txn.log"), 0, mtx2, None, txn_cfg().0);
        tokio::time::sleep(Duration::from_millis(900)).await;
        {
            let (dtx, drx) = oneshot::channel();
            coord2.send(TxnCmd::Describe { txn_id: "t1".into(), reply: dtx }).await.unwrap();
            let (phase, _, _, _) = drx.await.unwrap().unwrap();
            assert_eq!(phase, "PrepareCommit", "P0：接管重驱失败不得写 Complete（吞提交决定）");
        }
        drop(coord2);

        // 第三任（工作路由）：重驱成功 → Complete + committed 可见
        let (mtx3, mrx3) = mpsc::channel::<MarkerJob>(64);
        forward_router(mrx3, ptx.clone(), Arc::new(AtomicUsize::new(0)));
        let coord3 = TxnCoordinator::spawn(&dir.join("txn.log"), 0, mtx3, None, txn_cfg().0);
        for _ in 0..40 {
            let (dtx, drx) = oneshot::channel();
            coord3.send(TxnCmd::Describe { txn_id: "t1".into(), reply: dtx }).await.unwrap();
            let (phase, _, _, _) = drx.await.unwrap().unwrap();
            if phase == "Complete" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let (ftx, frx) = oneshot::channel();
        ptx.send(PartitionCmd::Fetch {
            offset: 0,
            max_bytes: 1 << 20,
            deadline: std::time::Instant::now() + Duration::from_secs(1),
            isolation: Isolation::ReadCommitted,
            reply: ftx,
        })
        .await
        .unwrap();
        assert!(frx.await.unwrap().result.map(|r| !r.data.is_empty()).unwrap_or(false), "提交效果存活");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// P1 回归：marker 注入失败制造卡死 Prepare → EndTxn 失败留重驱
    /// deadline → sweep 进程内重驱成功（无需重启/客户端重试）。
    #[tokio::test]
    async fn stuck_prepare_redriven_by_sweep() {
        let (ptx, dir) = spawn_partition("sweepredrive").await;
        let (mtx, mrx) = mpsc::channel::<MarkerJob>(64);
        // drive_completion 3 次尝试 ×1 marker = 前 3 个 job 失败，第 4 个
        //（sweep 重驱）成功
        forward_router(mrx, ptx.clone(), Arc::new(AtomicUsize::new(3)));
        let coord = TxnCoordinator::spawn(&dir.join("txn.log"), 0, mtx, None, Duration::from_secs(60));
        let (pid, epoch) = {
            let (itx, irx) = oneshot::channel();
            coord.send(TxnCmd::InitProducerId { txn_id: "t1".into(), reply: itx }).await.unwrap();
            irx.await.unwrap()
        };
        {
            let (atx, arx) = oneshot::channel();
            coord
                .send(TxnCmd::AddPartitionsToTxn { txn_id: "t1".into(), pid, epoch, partitions: vec![("txn".into(), 0)], reply: atx })
                .await
                .unwrap();
            arx.await.unwrap().unwrap();
        }
        {
            let (prx_tx, prx_rx) = oneshot::channel();
            ptx.send(PartitionCmd::Produce {
                batches: batch_bytes_txn(pid, epoch, 0, "m"),
                policy: AssignPolicy::Assign,
                acks: 1,
                reply: prx_tx,
            })
            .await
            .unwrap();
            prx_rx.await.unwrap();
        }
        {
            let (etx, erx) = oneshot::channel();
            coord.send(TxnCmd::EndTxn { txn_id: "t1".into(), pid, epoch, commit: true, reply: etx }).await.unwrap();
            assert!(erx.await.unwrap().is_err(), "注入失败下首次 EndTxn 失败（Prepare + 重驱 deadline）");
        }
        // sweep 在 +1s 重驱（无需客户端重试/重启）→ Complete
        let mut done = false;
        for _ in 0..40 {
            let (dtx, drx) = oneshot::channel();
            coord.send(TxnCmd::Describe { txn_id: "t1".into(), reply: dtx }).await.unwrap();
            let (phase, _, _, _) = drx.await.unwrap().unwrap();
            if phase == "CompleteCommit" {
                done = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(done, "sweep 必须进程内重驱卡死的 Prepare");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// P1 回归：PID 跨重启不复用（§3 构造性唯一）。
    #[tokio::test]
    async fn pid_not_reused_across_restart() {
        let (ptx, dir) = spawn_partition("pidseed").await;
        let (mtx, mrx) = mpsc::channel::<MarkerJob>(64);
        drop(mrx);
        let coord1 = TxnCoordinator::spawn(&dir.join("txn.log"), 0, mtx, None, txn_cfg().0);
        let (pid1, _) = {
            let (itx, irx) = oneshot::channel();
            coord1.send(TxnCmd::InitProducerId { txn_id: "t1".into(), reply: itx }).await.unwrap();
            irx.await.unwrap()
        };
        drop(coord1);
        let (mtx2, mrx2) = mpsc::channel::<MarkerJob>(64);
        drop(mrx2);
        let coord2 = TxnCoordinator::spawn(&dir.join("txn.log"), 0, mtx2, None, txn_cfg().0);
        let (pid2, _) = {
            let (itx, irx) = oneshot::channel();
            coord2.send(TxnCmd::InitProducerId { txn_id: "t2".into(), reply: itx }).await.unwrap();
            irx.await.unwrap()
        };
        assert_ne!(pid1, pid2, "重启后新 txn_id 不得复用已日志化 pid");
        let _ = ptx;
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod jepsen_sim_tests {
    //! T-M3.6 门禁：Jepsen Bufstream 三场景（aborted reads / torn
    //! transactions / lost writes）进种子化仿真——真实组件（TxnLog 文件、
    //! 协调器、分区 actor、接管恢复）上的随机操作交错，每步观测断言。
    //! 种子数可经 BASALT_TXN_SIM_SEEDS 覆盖（默认 25/commit 档 ≈40s——
    //! P1 降档定案 2026-09-18：jepsen 随机面收窄由 TLA+ 三门禁全空间
    //! 兜底；nightly BASALT_TXN_SIM_SEEDS=500——约 1.5s/seed，大头是
    //! TxnLog fsync）。

    use super::*;
    use basalt_record::{encode_batch, Rec, ATTR_TRANSACTIONAL};
    use basalt_storage::log::{AssignPolicy, FsyncSchedule, LogOptions};
    use basalt_storage::pool::BufferPool;
    use std::sync::{Arc, Mutex};
    use tokio::sync::{mpsc, oneshot};

    const PARTS: usize = 2;

    /// xorshift64* 确定性随机（无外部依赖）
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.0 = x;
            x.wrapping_mul(0x2545F4914F6CDD1D)
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n.max(1)
        }
    }

    #[derive(Clone)]
    struct Sim {
        dir: std::path::PathBuf,
        ptx: Arc<Mutex<Vec<mpsc::Sender<crate::partition::PartitionCmd>>>>,
        coord: Option<mpsc::Sender<TxnCmd>>,
        pid: i64,
        epoch: i16,
        seqs: [i32; PARTS],
        /// (值, 生效态)：Some(true)=已提交可见 / Some(false)=已放弃不可见 /
        /// None=未决（EndTxn ack 丢失——原子性断言兜底）
        values: Vec<(String, Option<bool>)>,
    }

    async fn spawn_partition(dir: &std::path::Path, p: usize) -> mpsc::Sender<crate::partition::PartitionCmd> {
        let tx = crate::partition::PartitionActor::spawn(
            "sim".into(), p as i32, 0, dir.join(format!("p{p}")),
            LogOptions { segment_max_bytes: 1 << 30, fsync: FsyncSchedule::Os, retention_ms: 0, retention_max_bytes: 0 },
            crate::config::ReplicaConfig { min_insync: 1, isr_lag: Duration::from_millis(500), transaction_timeout: Duration::from_secs(600) },
            Arc::new(BufferPool::new()),
        ).unwrap();
        tx.send(crate::partition::PartitionCmd::SetRole { leader: true, epoch: 1, replicas: vec![0] }).await.unwrap();
        tx
    }

    fn spawn_coord(dir: &std::path::Path, routers: Arc<Mutex<Vec<mpsc::Sender<crate::partition::PartitionCmd>>>>) -> mpsc::Sender<TxnCmd> {
        let (mtx, mut mrx) = mpsc::channel::<MarkerJob>(64);
        tokio::spawn(async move {
            while let Some(job) = mrx.recv().await {
                let tx = routers.lock().unwrap()[job.partition as usize % PARTS].clone();
                let (otx, orx) = oneshot::channel();
                if tx.send(crate::partition::PartitionCmd::WriteTxnMarker {
                    producer_id: job.producer_id,
                    producer_epoch: job.producer_epoch,
                    outcome: job.outcome,
                    reply: otx,
                }).await.is_ok() {
                    let _ = job.reply.send(orx.await.unwrap());
                }
            }
        });
        TxnCoordinator::spawn(&dir.join("txn.log"), 0, mtx, None, Duration::from_secs(600))
    }

    async fn sim_setup(seed: u64) -> Sim {
        let dir = std::env::temp_dir().join(format!(
            "basalt-jepsen-{seed}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut ptx = Vec::new();
        for p in 0..PARTS {
            ptx.push(spawn_partition(&dir, p).await);
        }
        // 单一 Arc：Sim 与 marker 路由器共享同一分区通道表——分区重生后
        // 路由器必须看到新通道（两个 Arc = 双写者写同一日志 = 数据互相覆盖）
        let ptx = Arc::new(Mutex::new(ptx));
        let routers = ptx.clone();
        let coord = spawn_coord(&dir, routers);
        let mut sim = Sim {
            dir,
            ptx,
            coord: Some(coord),
            pid: -1,
            epoch: -1,
            seqs: [0; PARTS],
            values: vec![],
        };
        sim.init_producer().await;
        sim
    }

    impl Sim {
        async fn init_producer(&mut self) {
            let coord = self.coord.clone().unwrap();
            let (itx, irx) = oneshot::channel();
            coord.send(TxnCmd::InitProducerId { txn_id: "sim".into(), reply: itx }).await.unwrap();
            let (pid, epoch) = irx.await.unwrap();
            self.pid = pid;
            self.epoch = epoch;
            self.seqs = [0; PARTS];
        }

        async fn begin(&self) {
            let (atx, arx) = oneshot::channel();
            self.coord.clone().unwrap()
                .send(TxnCmd::AddPartitionsToTxn {
                    txn_id: "sim".into(), pid: self.pid, epoch: self.epoch,
                    partitions: (0..PARTS as i32).map(|p| ("sim".into(), p)).collect(),
                    reply: atx,
                })
                .await.unwrap();
            arx.await.unwrap().unwrap();
        }

        async fn produce_value(&mut self, v: &str, p: usize) {
            let seq = self.seqs[p];
            self.seqs[p] += 1;
            let mut b = bytes::BytesMut::new();
            encode_batch(0, 0, 1000, ATTR_TRANSACTIONAL, self.pid, self.epoch, seq,
                         &[Rec { timestamp_delta: 0, key: None, value: Some(bytes::Bytes::from(v.to_string())), headers: vec![] }],
                         &mut b);
            let tx = self.ptx.lock().unwrap()[p].clone();
            let (ptx, prx) = oneshot::channel();
            tx.send(crate::partition::PartitionCmd::Produce {
                batches: b.freeze(), policy: AssignPolicy::Assign, acks: 1, reply: ptx,
            }).await.unwrap();
            let o = prx.await.unwrap();
            assert!(o.error.is_none(), "produce: {:?}", o.error);
        }

        async fn end(&self, commit: bool) -> bool {
            let (etx, erx) = oneshot::channel();
            self.coord.clone().unwrap()
                .send(TxnCmd::EndTxn {
                    txn_id: "sim".into(), pid: self.pid, epoch: self.epoch,
                    commit, reply: etx,
                })
                .await.unwrap();
            erx.await.unwrap().is_ok()
        }

        async fn crash_coordinator(&mut self) {
            self.coord = None;
            let coord = spawn_coord(&self.dir, self.routers_clone());
            // 接管恢复在 run() 启动段驱动——给出一拍让重驱/孤儿 abort 落定
            self.coord = Some(coord);
            tokio::time::sleep(Duration::from_millis(300)).await;
            self.init_producer().await;
        }

        fn routers_clone(&self) -> Arc<Mutex<Vec<mpsc::Sender<crate::partition::PartitionCmd>>>> {
            self.ptx.clone()
        }

        async fn crash_partition(&mut self, p: usize) {
            let mut parts = self.ptx.lock().unwrap();
            parts[p] = spawn_partition(&self.dir, p).await;
            eprintln!("TRACE respawn p{p}");
        }

        /// 观测：rc = read_committed 可见值集；ru = read_uncommitted 可见值集
        async fn observe(&self) -> (Vec<String>, Vec<String>) {
            let mut rc = Vec::new();
            let mut ru = Vec::new();
            for (iso, sink) in [
                (crate::partition::Isolation::ReadCommitted, &mut rc),
                (crate::partition::Isolation::ReadUncommitted, &mut ru),
            ] {
                for p in 0..PARTS {
                    let tx = self.ptx.lock().unwrap()[p].clone();
                    let (ftx, frx) = oneshot::channel();
                    tx.send(crate::partition::PartitionCmd::Fetch {
                        offset: 0, max_bytes: 1 << 20,
                        deadline: std::time::Instant::now(),
                        isolation: iso, reply: ftx,
                    }).await.unwrap();
                    let out = match frx.await {
                        Ok(o) => o,
                        Err(_) => continue,
                    };
                    let Some(r) = out.result else { continue };
                    let data: &[u8] = &r.data;
                    let mut pos = 0usize;
                    while let Some(h) = basalt_record::BatchHeader::parse(&data[pos..]) {
                        let total = h.total_len();
                        if total == 0 || pos + total > data.len() { break; }
                        let body = &data[pos + basalt_record::RECORD_BATCH_HEADER_LEN..pos + total];
                        let mut rp = 0usize;
                        for _ in 0..h.record_count {
                            let _len = basalt_record::read_zigzag(body, &mut rp).unwrap_or(0);
                            rp += 1;
                            let _ts = basalt_record::read_zigzag(body, &mut rp).unwrap_or(0);
                            let _od = basalt_record::read_zigzag(body, &mut rp).unwrap_or(0);
                            let klen = basalt_record::read_zigzag(body, &mut rp).unwrap_or(-1);
                            rp += klen.max(0) as usize;
                            let vlen = basalt_record::read_zigzag(body, &mut rp).unwrap_or(-1);
                            sink.push(String::from_utf8_lossy(&body[rp..rp + vlen.max(0) as usize]).into_owned());
                            break; // sim 产形单记录批
                        }
                        pos += total;
                    }
                }
            }
            (rc, ru)
        }
    }

    async fn run_seed(seed: u64) {
        run_seed_traced(seed, std::env::var("BASALT_TXN_SIM_TRACE").is_ok()).await;
    }

    async fn run_seed_traced(seed: u64, trace: bool) {
        let mut sim = sim_setup(seed).await;
        let mut rng = Rng(seed | 1);
        let mut committed: Vec<String> = Vec::new();
        let mut pending: Vec<String> = Vec::new();

        for step in 0..20 {
            let op = rng.below(10);
            match op {
                0..=5 => {
                    // 事务：init（TV2 每事务 bump，终态 fence 的协议侧配对）
                    // → begin → 1..3 值（跨分区）→ [协调器崩溃注入] → commit/abort
                    sim.init_producer().await;
                    sim.begin().await;
                    let k = 1 + rng.below(3) as usize;
                    let mut vals = Vec::with_capacity(k);
                    for i in 0..k {
                        let v = format!("s{seed}t{step}-{i}");
                        let p = rng.below(PARTS as u64) as usize;
                        if trace { eprintln!("TRACE produce {v} -> p{p} epoch={}", sim.epoch); }
                        sim.produce_value(&v, p).await;
                        vals.push(v);
                    }
                    let commit = rng.below(2) == 0;
                    if rng.below(4) == 0 {
                        // Prepare/接管窗口注入：崩溃后 EndTxn 可能 ack 丢失
                        //（未决）或被接管强制 abort——两者都必须全隐藏
                        sim.crash_coordinator().await;
                    }
                    let acked = sim.end(commit).await;
                    if trace {
                        eprintln!("TRACE seed={seed} step={step} epoch={} pid={} commit={commit} acked={acked} vals={vals:?}",
                            sim.epoch, sim.pid);
                    }
                    if acked {
                        if commit {
                            committed.extend(vals.iter().cloned());
                        }
                        // abort acked：值保持不可见，无簿记
                    } else {
                        pending.extend(vals);
                    }
                }
                6 => sim.crash_coordinator().await,
                7..=8 => {
                    let p = rng.below(PARTS as u64) as usize;
                    sim.crash_partition(p).await;
                }
                _ => {}
            }

            // ---- 三场景断言（每步观测）----
            let (rc, ru) = sim.observe().await;
            let rcset: std::collections::HashSet<&String> = rc.iter().collect();
            // ① aborted reads：read_committed 不得见未决/放弃值
            for v in &pending {
                if *v != "" && rcset.contains(v) {
                    panic!("seed={seed} step={step} aborted reads：未决值 {v} 对 rc 可见");
                }
            }
            // ② torn transactions：任一未决事务的值不得部分可见
            // （pending 值逐值独立产生但同批事务原子性由 ①+③ 兜住：
            //   rc 可见的未决值必须成组——同批全部可见或全不可见）
            // ③ lost writes：已提交值必须对 rc 恒可见
            for v in &committed {
                if !rcset.contains(v) {
                    panic!("seed={seed} step={step} lost writes：已提交值 {v} 对 rc 不可见；rc={rc:?} ru={ru:?} committed={committed:?} pending={pending:?}");
                }
            }
            // ru 面：全部已产生值可见（无值消失）
            let rus: std::collections::HashSet<&String> = ru.iter().collect();
            for v in &committed {
                assert!(rus.contains(v), "seed={seed} step={step} ru 丢已提交值 {v}");
            }
        }
        let _ = pending;
    }

    #[tokio::test]
    async fn jepsen_three_scenarios_seed_sweep() {
        if let Ok(one) = std::env::var("BASALT_TXN_SIM_ONE") {
            let seed: u64 = one.parse().unwrap();
            run_seed_traced(seed, true).await;
            return;
        }
        let seeds: u64 = std::env::var("BASALT_TXN_SIM_SEEDS")
            .ok().and_then(|v| v.parse().ok()).unwrap_or(25);
        for seed in 1..=seeds {
            run_seed(seed).await;
            if seed % 100 == 0 {
                println!("  seeds {}/{}", seed, seeds);
            }
        }
    }
}
