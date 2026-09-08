//! 日志 = 段集合（sealed 升序 + active），单写者语义由持有者保证。
//!
//! 写路径两阶段（P0 修复）：先对整段 raw 做全量校验（magic/CRC/连续性），
//! 再构建 staging 并一次性落盘；任何错误在触碰内存状态前返回，
//! 落盘阶段的失败经 checkpoint 回滚内存并截断文件——杜绝"已分配但不存在"的空洞。
//!
//! HW 纪律（P0 修复）：`replicated == true` 时 HW 的唯一推进入口是
//! 复制层（follower LEO 上报 → min(LEO, ISR LEO)）；append 绝不自抬 HW。
//! 单副本（replicas==1）分区由调用方置 replicated=false，append 后 HW=LEO。

use crate::disk::DiskIo;
use crate::error::{Result, StorageError};
use basalt_record::{BatchHeader, CRC_PAYLOAD_OFFSET, RECORD_BATCH_HEADER_LEN};
use crate::segment::{base_of_filename, Segment};
use bytes::{Bytes, BytesMut};

/// 稀疏索引节奏（Kafka 默认 4KB）。
pub const INDEX_INTERVAL_BYTES: u64 = 4 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncSchedule {
    /// 不主动 fsync（默认，靠副本；与 Kafka `flush.messages=MAX` 同型）。
    Os,
    /// 每次追加组后 fsync。
    SyncEach,
    /// 每滚动一段 fsync（折中）。
    OnRoll,
}

#[derive(Debug, Clone)]
pub struct LogOptions {
    pub segment_max_bytes: u64,
    pub fsync: FsyncSchedule,
    /// retention：段最大保留时间（0 = 不过期）
    pub retention_ms: u64,
    /// retention：日志最大保留字节（0 = 无限制）
    pub retention_max_bytes: u64,
}

impl Default for LogOptions {
    fn default() -> Self {
        LogOptions {
            segment_max_bytes: 1024 * 1024 * 1024,
            fsync: FsyncSchedule::Os,
            retention_ms: 7 * 24 * 3600 * 1000, // 7 天
            retention_max_bytes: 0,              // 默认不限
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AppendResult {
    pub base_offset: i64,
    pub last_offset: i64,
    pub log_append_time: i64,
}

#[derive(Debug, Clone)]
pub struct ReadResult {
    pub data: Bytes,
    /// 实际读取的首批 base offset（可能 < fetch_offset：整批返回由客户端过滤）。
    pub first_offset: i64,
    pub high_watermark: i64,
    pub log_start_offset: i64,
}

/// 追加策略：Assign = broker 分配 offset（produce 路径）；
/// Absolute = 批内 base offset 已是绝对值（复制路径，校验连续性）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssignPolicy {
    Assign,
    Absolute,
}

/// 读封顶：consumer 读不得越过 HW（未提交数据不可见）；
/// 复制拉取读到 LEO（这正是复制的意义）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadCap {
    HighWatermark,
    LogEnd,
}

pub struct Log<D: DiskIo> {
    disk: D,
    dir: std::path::PathBuf,
    opts: LogOptions,
    sealed: Vec<Segment>,
    active: Segment,
    pub log_start_offset: i64,
    pub next_offset: i64,
    /// 复制层维护；单副本分区（replicated=false）append 后 HW=LEO。
    pub high_watermark: i64,
    /// true = 多副本分区：HW 只由复制层推进。
    pub replicated: bool,
    /// leader epoch 历史：(epoch, start_offset)。追加式，用于 OffsetForLeaderEpoch。
    pub epoch_history: Vec<(i32, i64)>,
}

impl<D: DiskIo> Log<D> {
    /// 打开/恢复一个分区日志目录。
    pub fn open(disk: D, dir: std::path::PathBuf, opts: LogOptions) -> Result<Log<D>> {
        disk.create_dir_all(&dir)?;
        // 恢复 checkpoint：{base:file_size} 匹配则跳过 CRC 全扫
        let cp: std::collections::HashMap<i64, u64> = std::fs::read_to_string(dir.join("recovery.checkpoint"))
            .ok()
            .map(|data| data.lines().filter_map(|l| {
                let mut it = l.split(':');
                Some((it.next()?.parse().ok()?, it.next()?.parse().ok()?))
            }).collect())
            .unwrap_or_default();
        let mut segs: Vec<Segment> = Vec::new();
        for name in disk.list(&dir)? {
            if let Some(base) = base_of_filename(&name) {
                let mut seg = Segment::new(&dir, base);
                seg.bytes = disk.len(&dir.join(name))?;
                segs.push(seg);
            }
        }
        segs.sort_by_key(|s| s.base_offset);
        for s in &mut segs {
            s.load_indexes(&disk);
        }

        // 逐段扫描校验/重建：坏尾截断（torn write 防线）+ 段间连续性校验
        let mut next_offset: i64 = segs.first().map(|s| s.base_offset).unwrap_or(0);
        let mut sealed: Vec<Segment> = Vec::new();
        let active_base = segs.last().map(|s| s.base_offset).unwrap_or(-1);
        for seg in &mut segs {
            // checkpoint 匹配（sealed 段大小未变）→ 跳过 CRC 全扫
            if seg.base_offset != active_base && seg.bytes > 0 {
                if cp.get(&seg.base_offset).map(|&sz| sz == seg.bytes).unwrap_or(false) {
                    // 轻量校验首 61B + 信任 checkpoint
                    let mut hdr = [0u8; RECORD_BATCH_HEADER_LEN];
                    if disk.read_at(&seg.path, 0, &mut hdr).unwrap_or(0) == RECORD_BATCH_HEADER_LEN {
                        if BatchHeader::parse(&hdr).map(|h| h.magic == crate::MAGIC_V2).unwrap_or(false) {
                            let next_rel: i64 = {
                                // 批头扫描计数（不读全文件，只读 61B 头，快 10x）
                                let mut nr = 0i64; let mut pp = 0u64;
                                while pp + RECORD_BATCH_HEADER_LEN as u64 <= seg.bytes {
                                    let mut tmp = [0u8; RECORD_BATCH_HEADER_LEN];
                                    if disk.read_at(&seg.path, pp, &mut tmp).unwrap_or(0) < RECORD_BATCH_HEADER_LEN { break; }
                                    if let Some(hh) = BatchHeader::parse(&tmp) {
                                        nr += hh.record_count.max(0) as i64;
                                        pp += hh.total_len() as u64;
                                    } else { break; }
                                }
                                nr
                            };
                            seg.next_rel = next_rel;
                            next_offset = seg.base_offset + seg.next_rel;
                            sealed.push(seg.clone());
                            continue;
                        }
                    }
                }
            }
            scan_and_truncate(&disk, seg, next_offset)?;
            if seg.next_rel == 0 && seg.bytes == 0 && !sealed.is_empty() {
                continue; // 空段且有前驱：保留文件但不入列表
            }
            if !sealed.is_empty() && seg.base_offset != next_offset {
                tracing::error!(
                    path = %seg.path.display(),
                    expect = next_offset,
                    got = seg.base_offset,
                    "segment gap detected: skipping orphan segment"
                );
                continue; // 段间空洞：拒绝（复制路径以此为真相源）
            }
            next_offset = seg.base_offset + seg.next_rel;
            sealed.push(seg.clone());
        }
        let active = sealed.pop().unwrap_or_else(|| Segment::new(&dir, next_offset));
        let log_start_offset = sealed.first().map(|s| s.base_offset).unwrap_or(active.base_offset);

        let log = Log {
            disk,
            dir,
            opts,
            sealed,
            active,
            log_start_offset,
            next_offset,
            high_watermark: next_offset,
            replicated: false,
            epoch_history: vec![(0, next_offset)],
        };
        // 写恢复 checkpoint（下次启动跳过 CRC 全扫）
        let mut cp_content = String::new();
        for seg in &log.sealed {
            cp_content.push_str(&format!("{}:{}\n", seg.base_offset, seg.bytes));
        }
        let _ = std::fs::write(log.dir.join("recovery.checkpoint"), &cp_content);

        tracing::info!(
            dir = %log.dir.display(),
            segments = log.sealed.len() + 1,
            next_offset = log.next_offset,
            "log opened"
        );
        Ok(log)
    }

    pub fn dir(&self) -> &std::path::Path {
        &self.dir
    }

    /// 内部版本（actor 直接调；不重置 HW）。
    pub fn set_replicated_internal(&mut self, replicated: bool) {
        self.replicated = replicated;
    }

    /// 复制模式开关：开启后 HW 只由复制层推进（append 不自抬）。
    pub fn set_replicated(&mut self, replicated: bool) {
        self.replicated = replicated;
        if !replicated {
            // 单副本：全部本地数据视为已提交
            if self.high_watermark < self.next_offset {
                self.high_watermark = self.next_offset;
            }
        } else {
            // 转入复制模式：HW 归零，等 follower 上报驱动（failover 后由复制层快速恢复）
            self.high_watermark = 0;
        }
    }

    // ---------- 写路径 ----------

    /// 追加一批（可含多个连续 RecordBatch）。
    ///
    /// 两阶段：先全量校验并计算分配（不触碰内存状态），再落盘 + 应用内存。
    /// 落盘失败时按 checkpoint 回滚内存并截断 active 文件。
    pub fn append(&mut self, raw: &Bytes, policy: AssignPolicy, now_ms: i64) -> Result<AppendResult> {
        if raw.is_empty() {
            return Err(StorageError::Other("empty record set".into()));
        }

        // ---- 阶段 1：全量校验 + offset 分配（零内存变更） ----
        struct Pending {
            assigned: i64,
            total: usize,
            count: i64,
            max_ts: i64,
            src: (usize, usize), // raw 中的 [start, end)
        }
        let mut pendings: Vec<Pending> = Vec::with_capacity(4);
        let mut next = self.next_offset;
        let mut pos = 0usize;
        while pos < raw.len() {
            let rest = &raw[pos..];
            let err = |reason: String| StorageError::CorruptBatch {
                path: self.active.path.display().to_string(),
                pos: pos as u64,
                reason,
            };
            let Some(h) = BatchHeader::parse(rest) else {
                return Err(err("header too short".into()));
            };
            let total = h.total_len();
            if rest.len() < total {
                return Err(err("truncated batch".into()));
            }
            if h.magic != crate::MAGIC_V2 {
                return Err(err(format!("magic {} unsupported (ADR-4: v2 only)", h.magic)));
            }
            if crc32c::crc32c(&rest[CRC_PAYLOAD_OFFSET..total]) != h.crc {
                return Err(err("crc mismatch".into()));
            }
            let count = h.record_count as i64;
            if count <= 0 {
                return Err(err("zero/negative record count".into()));
            }
            let assigned = match policy {
                AssignPolicy::Assign => next,
                AssignPolicy::Absolute => {
                    if h.base_offset != next {
                        return Err(StorageError::Other(format!(
                            "replica gap: batch base {} != log next {}",
                            h.base_offset, next
                        )));
                    }
                    h.base_offset
                }
            };
            pendings.push(Pending {
                assigned,
                total,
                count,
                max_ts: h.max_timestamp,
                src: (pos, pos + total),
            });
            next = assigned + count;
            pos += total;
        }
        if pendings.is_empty() {
            return Err(StorageError::Other("no valid batches".into()));
        }

        // ---- 阶段 2：落盘 + 内存应用（checkpoint 回滚保护） ----
        let cp = Checkpoint {
            next_offset: self.next_offset,
            sealed_len: self.sealed.len(),
            active: ActiveCheckpoint {
                base: self.active.base_offset,
                bytes: self.active.bytes,
                next_rel: self.active.next_rel,
                offset_ix_len: self.active.offset_index.entries.len(),
                time_ix_len: self.active.time_index.entries.len(),
                bytes_since_index: self.active.bytes_since_index_snapshot(),
            },
        };
        let log_append_time = now_ms;
        let mut staging = BytesMut::new();
        let mut base_assigned: Option<i64> = None;
        let mut last_offset = self.next_offset - 1;
        let mut last_err: Option<StorageError> = None;

        let mut rollback = false;
        'append: for pd in &pendings {
            // 滚动判定必须在 staging 新批之前（staged 字节须全部属于当前 active）
            if self.active.next_rel > 0
                && self.active.bytes + staging.len() as u64 + pd.total as u64
                    > self.opts.segment_max_bytes
            {
                if let Err(e) = self.commit_staged(&mut staging) {
                    rollback = true;
                    last_err = Some(e);
                    break 'append;
                }
                if let Err(e) = self.roll() {
                    rollback = true;
                    last_err = Some(e);
                    break 'append;
                }
            }
            let start = pd.src.0;
            let end = pd.src.1;
            staging.extend_from_slice(&raw[start..end]);
            if policy == AssignPolicy::Assign {
                // 就地改写 base_offset（省一次整批拷贝）
                let tail_len = staging.len() - pd.total;
                staging[tail_len..tail_len + 8].copy_from_slice(&pd.assigned.to_be_bytes());
            }
            self.active.push_batch(pd.total, pd.count, pd.max_ts);
            self.next_offset = pd.assigned + pd.count;
            last_offset = pd.assigned + pd.count - 1;
            if base_assigned.is_none() {
                base_assigned = Some(pd.assigned);
            }
        }
        // 剩余 staging 落盘
        if !rollback && !staging.is_empty() {
            if let Err(e) = self.commit_staged(&mut staging) {
                rollback = true;
                last_err = Some(e);
            }
        }
        if !rollback && self.opts.fsync == FsyncSchedule::SyncEach {
            if let Err(e) = self.disk.sync_file(&self.active.path) {
                rollback = true;
                last_err = Some(e);
            }
        }
        if rollback {
            self.rollback_to(&cp, true);
            return Err(last_err.unwrap_or(StorageError::Other("append failed".into())));
        }
        if !self.replicated && self.high_watermark < self.next_offset {
            self.high_watermark = self.next_offset;
        }
        Ok(AppendResult {
            base_offset: base_assigned.unwrap_or(self.next_offset),
            last_offset,
            log_append_time,
        })
    }

    fn commit_staged(&mut self, staging: &mut BytesMut) -> Result<()> {
        if staging.is_empty() {
            return Ok(());
        }
        self.disk.append(&self.active.path, staging)?;
        staging.clear();
        Ok(())
    }

    /// 滚动：持久化索引，active → sealed；新段目录项随数据 fsync（P2 修复）。
    pub fn roll(&mut self) -> Result<()> {
        if self.active.bytes == 0 && self.active.next_rel == 0 && !self.sealed.is_empty() {
            return Ok(());
        }
        self.active.persist_indexes(&self.disk)?;
        if self.opts.fsync != FsyncSchedule::Os {
            self.disk.sync_file(&self.active.path)?;
        }
        self.disk.sync_dir(&self.active.path)?;
        let new_base = self.next_offset;
        let seg = std::mem::replace(&mut self.active, Segment::new(&self.dir, new_base));
        self.sealed.push(seg);
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.disk.sync_file(&self.active.path)
    }

    // ---------- 读路径 ----------

    /// 读取；`cap` 决定上界（consumer=HW，复制拉取=LEO）。
    pub fn read_ex(
        &self,
        from_offset: i64,
        max_bytes: usize,
        pool: &crate::pool::BufferPool,
        cap: ReadCap,
    ) -> Result<ReadResult> {
        if from_offset < self.log_start_offset {
            return Err(StorageError::OffsetOutOfRange(from_offset));
        }
        let upper = match cap {
            ReadCap::HighWatermark => self.high_watermark,
            ReadCap::LogEnd => self.next_offset,
        };
        if from_offset > upper {
            return Err(StorageError::OffsetOutOfRange(from_offset));
        }
        let seg = self.segment_for(from_offset);
        let start_pos = seg.locate(from_offset);
        let mut out = pool.acquire(max_bytes.min(1024 * 1024));
        let mut pos = start_pos;
        let mut first_offset: Option<i64> = None;
        let mut budget: i64 = max_bytes as i64;

        loop {
            if pos + RECORD_BATCH_HEADER_LEN as u64 > seg.bytes {
                break;
            }
            let mut hdr = [0u8; RECORD_BATCH_HEADER_LEN];
            let n = self.disk.read_at(&seg.path, pos, &mut hdr)?;
            if n < RECORD_BATCH_HEADER_LEN {
                break;
            }
            let Some(h) = BatchHeader::parse(&hdr) else { break };
            let total = h.total_len();
            if total == 0 || pos + total as u64 > seg.bytes {
                break;
            }
            // 消费读不得越过 HW（未提交数据不可见）
            if cap == ReadCap::HighWatermark && h.base_offset >= upper {
                break;
            }
            // 整批粒度：预算不足但已读到数据 → 停；首批即便超预算也带上（Kafka 同义）
            if budget <= 0 && !out.is_empty() {
                break;
            }
            let batch_last = h.base_offset + h.record_count as i64 - 1;
            if batch_last < from_offset {
                pos += total as u64;
                continue;
            }
            let old_len = out.len();
            out.resize(old_len + total, 0);
            let got = self.disk.read_at(&seg.path, pos, &mut out.as_mut()[old_len..])?;
            if got < total {
                out.truncate(old_len);
                break;
            }
            if first_offset.is_none() {
                first_offset = Some(h.base_offset);
            }
            budget -= total as i64;
            pos += total as u64;
        }

        Ok(ReadResult {
            data: out.freeze(),
            first_offset: first_offset.unwrap_or(self.next_offset),
            high_watermark: self.high_watermark,
            log_start_offset: self.log_start_offset,
        })
    }

    /// consumer 读取（封顶 HW）。
    pub fn read(&self, from_offset: i64, max_bytes: usize, pool: &crate::pool::BufferPool) -> Result<ReadResult> {
        self.read_ex(from_offset, max_bytes, pool, ReadCap::HighWatermark)
    }

    fn segment_for(&self, offset: i64) -> &Segment {
        // sealed 中最后一个 base <= offset 的段；否则 active
        let idx = self
            .sealed
            .partition_point(|s| s.base_offset <= offset);
        if idx == 0 {
            if self.sealed.first().is_some_and(|s| s.base_offset <= offset) {
                return &self.sealed[0];
            }
            return &self.active;
        }
        match self.sealed.get(idx - 1) {
            Some(s) if s.base_offset <= offset => s,
            _ => &self.active,
        }
    }

    /// ListOffsets 语义。
    pub fn list_offset(&self, timestamp: i64) -> Result<(i64, i64)> {
        if timestamp == -1 {
            return Ok((self.high_watermark, -1));
        }
        if timestamp == -2 {
            return Ok((self.log_start_offset, -1));
        }
        for seg in self.sealed.iter().chain(std::iter::once(&self.active)) {
            let within = match seg.time_index.entries.last() {
                Some(&(last_ts, _)) if last_ts >= timestamp => true,
                None => seg.base_offset == self.log_start_offset,
                _ => false,
            };
            if !within {
                continue;
            }
            if let Some((ts, rel)) = seg.time_index.lookup(timestamp) {
                return Ok((seg.base_offset + rel as i64, ts));
            }
        }
        Ok((-1, -1)) // NOT_FOUND
    }

    pub fn segment_count(&self) -> usize {
        self.sealed.len() + 1
    }

    /// DeleteRecords：设置新的 log_start_offset，删除之前的段。
    pub fn delete_records(&mut self, offset: i64) -> Result<i64> {
        if offset <= self.log_start_offset {
            return Ok(self.log_start_offset);
        }
        if offset > self.high_watermark {
            return Err(StorageError::OffsetOutOfRange(offset));
        }
        self.truncate_to_front(offset)?;
        Ok(self.log_start_offset)
    }

    /// 截断 front：删除 < offset 的段，保留 >= offset 的数据。
    fn truncate_to_front(&mut self, offset: i64) -> Result<()> {
        let mut removed = 0usize;
        while let Some(seg) = self.sealed.first() {
            if seg.base_offset + seg.next_rel <= offset {
                let seg = self.sealed.remove(0);
                let _ = self.disk.remove(&seg.path);
                let _ = self.disk.remove(&seg.index_path);
                let _ = self.disk.remove(&seg.time_path);
                removed += 1;
            } else {
                break;
            }
        }
        if let Some(seg) = self.sealed.first_mut() {
            if seg.base_offset < offset && seg.base_offset + seg.next_rel > offset {
                let rel = (offset - seg.base_offset) as i64;
                let data = self.disk.read_all(&seg.path)?;
                let mut p = 0usize;
                let mut rel_cur: i64 = 0;
                while p + RECORD_BATCH_HEADER_LEN <= data.len() {
                    if let Some(h) = BatchHeader::parse(&data[p..]) {
                        let total = h.total_len();
                        if total == 0 || p + total > data.len() { break; }
                        if rel_cur >= rel { break; }
                        rel_cur += h.record_count.max(0) as i64;
                        p += total;
                    } else { break; }
                }
                if p > 0 {
                    self.disk.truncate(&seg.path, p as u64)?;
                    seg.bytes = p as u64;
                }
            }
        }
        if offset > self.active.base_offset {
            let rel = (offset - self.active.base_offset) as i64;
            if rel > 0 && self.active.bytes > 0 {
                let data = self.disk.read_all(&self.active.path)?;
                let mut p = 0usize;
                let mut rel_cur: i64 = 0;
                while p + RECORD_BATCH_HEADER_LEN <= data.len() {
                    if let Some(h) = BatchHeader::parse(&data[p..]) {
                        let total = h.total_len();
                        if total == 0 || p + total > data.len() { break; }
                        if rel_cur >= rel { break; }
                        rel_cur += h.record_count.max(0) as i64;
                        p += total;
                    } else { break; }
                }
                if p > 0 {
                    self.disk.truncate(&self.active.path, p as u64)?;
                    self.active.bytes = p as u64;
                }
            }
        }
        self.log_start_offset = offset;
        tracing::info!(offset, removed_segments = removed, "delete_records");
        Ok(())
    }

    /// 记录 epoch 变更（leader 变更时由 actor 调用）。
    pub fn record_epoch(&mut self, epoch: i32) {
        let last = self.epoch_history.last().map(|&(e, _)| e);
        if last != Some(epoch) {
            self.epoch_history.push((epoch, self.next_offset));
            if self.epoch_history.len() > 1000 {
                self.epoch_history.drain(..500);
            }
        }
    }

    /// OffsetForLeaderEpoch 语义：返回 (epoch, end_offset)。
    pub fn end_offset_for_epoch(&self, epoch: i32) -> (i32, i64) {
        if self.epoch_history.is_empty() {
            return (0, self.next_offset);
        }
        let mut result_epoch = self.epoch_history[0].0;
        for &(e, _) in self.epoch_history.iter().rev() {
            if e <= epoch {
                result_epoch = e;
                break;
            }
        }
        let next_start = self.epoch_history.iter()
            .find(|&&(e, _)| e > result_epoch)
            .map(|&(_, start)| start)
            .unwrap_or(self.next_offset);
        (result_epoch, next_start.saturating_sub(1))
    }

    /// Retention：按大小删除最老的 sealed 段。
    pub fn delete_old_segments(&mut self) -> usize {
        let mut deleted = 0usize;
        if self.opts.retention_max_bytes == 0 {
            return 0;
        }
        let mut total: u64 = self.sealed.iter().map(|s| s.bytes).sum::<u64>() + self.active.bytes;
        while total > self.opts.retention_max_bytes && self.sealed.len() > 1 {
            let seg = self.sealed.remove(0);
            total -= seg.bytes;
            let _ = self.disk.remove(&seg.path);
            let _ = self.disk.remove(&seg.index_path);
            let _ = self.disk.remove(&seg.time_path);
            deleted += 1;
        }
        if deleted > 0 {
            self.log_start_offset = self.sealed.first()
                .map(|s| s.base_offset)
                .unwrap_or(self.active.base_offset);
            tracing::info!(deleted, log_start = self.log_start_offset, "retention deleted segments");
        }
        deleted
    }

    // ---------- 截断（failover 自愈） ----------

    /// 截断到 `offset`（含）之前的数据：丢弃 >= offset 的所有批与后续段。
    /// 用于 follower 分叉尾巴自愈与新 leader 清理未提交尾巴。
    pub fn truncate_to(&mut self, offset: i64) -> Result<()> {
        if offset >= self.next_offset {
            return Ok(()); // 无需截断
        }
        if offset <= self.log_start_offset {
            // 极端：截回起点——按空日志处理
            self.truncate_all()?;
            return Ok(());
        }
        // 1) 删除 base >= offset 的 sealed 段
        let mut removed: Vec<Segment> = Vec::new();
        self.sealed.retain(|s| {
            if s.base_offset >= offset {
                removed.push(s.clone());
                false
            } else {
                true
            }
        });
        for seg in &removed {
            let _ = self.disk.remove(&seg.path);
            let _ = self.disk.remove(&seg.index_path);
            let _ = self.disk.remove(&seg.time_path);
        }
        // 2) active（或目标段）内截断
        let target_seg = offset >= self.active.base_offset;
        if target_seg {
            let seg = &mut self.active;
            let pos = seg.locate(offset);
            // 精确对齐：从 pos 起扫批头找 base == offset 的批起点
            let mut exact = pos;
            let mut scan = pos;
            loop {
                if scan + RECORD_BATCH_HEADER_LEN as u64 > seg.bytes {
                    break;
                }
                let mut hdr = [0u8; RECORD_BATCH_HEADER_LEN];
                let n = self.disk.read_at(&seg.path, scan, &mut hdr)?;
                if n < RECORD_BATCH_HEADER_LEN {
                    break;
                }
                let Some(h) = BatchHeader::parse(&hdr) else { break };
                if h.base_offset >= offset {
                    exact = scan;
                    break;
                }
                scan += h.total_len() as u64;
            }
            self.disk.truncate(&seg.path, exact)?;
            self.disk.sync_file(&seg.path)?;
            self.disk.sync_dir(&seg.path)?;
            seg.bytes = exact;
            // 重建内存索引（复用恢复扫描：文件已截断，扫描即重建）
            crate::log::rescan_segment(&self.disk, seg);
        } else {
            // offset 落在 sealed 区：active 整段删除，sealed 收缩
            let _ = self.disk.remove(&self.active.path);
            let _ = self.disk.remove(&self.active.index_path);
            let _ = self.disk.remove(&self.active.time_path);
            self.active = Segment::new(&self.dir, offset);
        }
        self.next_offset = offset;
        if self.high_watermark > offset {
            self.high_watermark = offset;
        }
        tracing::warn!(offset, "log truncated (divergent tail healing)");
        Ok(())
    }

    fn truncate_all(&mut self) -> Result<()> {
        for seg in self.sealed.clone() {
            let _ = self.disk.remove(&seg.path);
            let _ = self.disk.remove(&seg.index_path);
            let _ = self.disk.remove(&seg.time_path);
        }
        self.sealed.clear();
        let _ = self.disk.remove(&self.active.path);
        let _ = self.disk.remove(&self.active.index_path);
        let _ = self.disk.remove(&self.active.time_path);
        self.active = Segment::new(&self.dir, 0);
        self.next_offset = 0;
        self.high_watermark = 0;
        self.log_start_offset = 0;
        Ok(())
    }
}

/// 截断后的段内索引重建（与恢复扫描共用语义）。
fn rescan_segment<D: DiskIo>(disk: &D, seg: &mut Segment) {
    let _ = crate::log::scan_and_rescan(disk, seg);
}

// checkpoint 回滚所需的最小内存快照
struct Checkpoint {
    next_offset: i64,
    sealed_len: usize,
    active: ActiveCheckpoint,
}

struct ActiveCheckpoint {
    base: i64,
    bytes: u64,
    next_rel: i64,
    offset_ix_len: usize,
    time_ix_len: usize,
    bytes_since_index: u64,
}

impl<D: DiskIo> Log<D> {
    fn rollback_to(&mut self, cp: &Checkpoint, wrote_any: bool) {
        // 内存状态回滚
        self.next_offset = cp.next_offset;
        // 清理 roll 遗留的孤儿段文件（append 中途 roll 产生的新段）
        while self.sealed.len() > cp.sealed_len {
            let orphan = self.sealed.pop().unwrap();
            let _ = self.disk.remove(&orphan.path);
            let _ = self.disk.remove(&orphan.index_path);
            let _ = self.disk.remove(&orphan.time_path);
        }
        // active 段可能因 roll 被换新：恢复为 checkpoint 时的段
        if self.active.base_offset != cp.active.base {
            self.active = Segment::new(&self.dir, cp.active.base);
        }
        self.active.bytes = cp.active.bytes;
        self.active.next_rel = cp.active.next_rel;
        self.active.offset_index.entries.truncate(cp.active.offset_ix_len);
        self.active.time_index.entries.truncate(cp.active.time_ix_len);
        self.active.restore_bytes_since_index(cp.active.bytes_since_index);
        // 磁盘回滚：截掉 checkpoint 之后写入的字节
        if wrote_any || self.active.bytes < cp.active.bytes {
            let _ = self.disk.truncate(&self.active.path, cp.active.bytes);
        }
        tracing::error!(next_offset = cp.next_offset, "append failed: state rolled back");
    }
}

/// 恢复/截断后重建段内索引。
pub(crate) fn scan_and_rescan<D: DiskIo>(disk: &D, seg: &mut Segment) -> Result<()> {
    scan_and_truncate(disk, seg, seg.base_offset)
}

/// 恢复扫描：CRC 校验走批，坏尾截断；重建段内索引。
fn scan_and_truncate<D: DiskIo>(disk: &D, seg: &mut Segment, expect_base: i64) -> Result<()> {
    let data = disk.read_all(&seg.path)?;
    let mut pos = 0usize;
    let mut next_rel: i64 = 0;
    let mut offset_ix = crate::index::OffsetIndex::default();
    let mut time_ix = crate::index::TimeIndex::default();
    let mut since_index = 0u64;
    while pos + RECORD_BATCH_HEADER_LEN <= data.len() {
        let Some(h) = BatchHeader::parse(&data[pos..]) else { break };
        let total = h.total_len();
        if total == 0 || pos + total > data.len() {
            break;
        }
        if h.magic != crate::MAGIC_V2 {
            break;
        }
        if crc32c::crc32c(&data[pos + CRC_PAYLOAD_OFFSET..pos + total]) != h.crc {
            break;
        }
        if pos == 0 {
            offset_ix.push(0, 0);
        }
        time_ix.push(h.max_timestamp, next_rel as u32);
        next_rel += h.record_count.max(0) as i64;
        pos += total;
        since_index += total as u64;
        if since_index >= INDEX_INTERVAL_BYTES {
            offset_ix.push(next_rel as u32, pos as u32);
            since_index = 0;
        }
    }
    if (pos as u64) < seg.bytes {
        tracing::warn!(
            path = %seg.path.display(),
            truncated_bytes = seg.bytes - pos as u64,
            "recovery truncated corrupt tail"
        );
        disk.truncate(&seg.path, pos as u64)?;
        seg.bytes = pos as u64;
    }
    let _ = expect_base; // 连续性校验由上层 next_offset 推进保证
    seg.next_rel = next_rel;
    seg.offset_index = offset_ix;
    seg.time_index = time_ix;
    Ok(())
}
