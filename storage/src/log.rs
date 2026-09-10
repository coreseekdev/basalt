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
    /// IO 批量合并：true 时 append() 只累积到 staging 不写盘，flush_batch() 统一落盘。
    pub batch_io: bool,
    batch_staging: BytesMut,
    /// leader epoch 历史：(epoch, start_offset)。追加式，用于 OffsetForLeaderEpoch。
    pub epoch_history: Vec<(i32, i64)>,
}

impl<D: DiskIo> Log<D> {
    /// 打开/恢复一个分区日志目录。
    pub fn open(disk: D, dir: std::path::PathBuf, opts: LogOptions) -> Result<Log<D>> {
        disk.create_dir_all(&dir)?;
        // 恢复 checkpoint：{base:file_size} 匹配则跳过 CRC 全扫；
        // start:<offset> 行持久化 log_start_offset（P1-1，删除/retention 后重写）
        let mut persisted_start: Option<i64> = None;
        let cp: std::collections::HashMap<i64, u64> = std::fs::read_to_string(dir.join("recovery.checkpoint"))
            .ok()
            .map(|data| {
                let mut map = std::collections::HashMap::new();
                for l in data.lines() {
                    if let Some(v) = l.strip_prefix("start:") {
                        if let Ok(v) = v.parse() { persisted_start = Some(v); }
                        continue;
                    }
                    let mut it = l.split(':');
                    if let (Some(b), Some(sz)) = (it.next(), it.next()) {
                        if let (Ok(b), Ok(sz)) = (b.parse(), sz.parse()) {
                            map.insert(b, sz);
                        }
                    }
                }
                map
            })
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
                // 空段且有前驱：不入列表。文件必须删除——保留会让后续恢复
                // 重新收养（opfuzz 实证 LEO 复活）。
                let _ = disk.remove(&seg.path);
                let _ = disk.remove(&seg.index_path);
                let _ = disk.remove(&seg.time_path);
                continue;
            }
            if !sealed.is_empty() && seg.base_offset != next_offset {
                tracing::error!(
                    path = %seg.path.display(),
                    expect = next_offset,
                    got = seg.base_offset,
                    "segment gap detected: removing orphan segment"
                );
                // 段间空洞：拒绝并删除文件——保留会让后续恢复重新收养，
                // 被截断/删除的记录复活（opfuzz 实证）。
                let _ = disk.remove(&seg.path);
                let _ = disk.remove(&seg.index_path);
                let _ = disk.remove(&seg.time_path);
                continue;
            }
            next_offset = seg.base_offset + seg.next_rel;
            sealed.push(seg.clone());
        }
        let active = sealed.pop().unwrap_or_else(|| Segment::new(&dir, next_offset));
        let derived_start = sealed.first().map(|s| s.base_offset).unwrap_or(active.base_offset);
        let log_start_offset = persisted_start
            .map(|s| s.max(derived_start))
            .unwrap_or(derived_start)
            .min(next_offset);

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
            batch_io: false,
            batch_staging: BytesMut::new(),
            epoch_history: vec![(0, next_offset)],
        };
        // 写恢复 checkpoint（下次启动跳过 CRC 全扫；含 log_start 持久化）
        log.persist_checkpoint();

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
            // 批必须至少容纳 12..61 的头部其余部分（opfuzz/C13 同族：
            // batch_length < 49 时 CRC 覆盖域 [21,total) 空或倒挂）
            if total < RECORD_BATCH_HEADER_LEN {
                return Err(err(format!("batch_length too small: total={total}")));
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

        // ---- Fast path：单批次 + 无需 patch + 适配当前段 → 直接写 raw（零 staging 拷贝） ----
        if pendings.len() == 1 {
            let pd = &pendings[0];
            let needs_roll = self.active.next_rel > 0
                && self.active.bytes + pd.total as u64 > self.opts.segment_max_bytes;
            let needs_patch = policy == AssignPolicy::Assign
                && raw[0..8] != pd.assigned.to_be_bytes();

            if !needs_roll && !needs_patch && self.batch_staging.is_empty() {
                // 直接写 raw 到磁盘（无中间缓冲）
                self.disk.append(&self.active.path, raw)?;
                self.active.push_batch(pd.total, pd.count, pd.max_ts);
                self.next_offset = pd.assigned + pd.count;
                if !self.replicated && self.high_watermark < self.next_offset {
                    self.high_watermark = self.next_offset;
                }
                if self.opts.fsync == FsyncSchedule::SyncEach {
                    self.disk.sync_file(&self.active.path)?;
                }
                return Ok(AppendResult {
                    base_offset: pd.assigned,
                    last_offset: pd.assigned + pd.count - 1,
                    log_append_time: now_ms,
                });
            }
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
        // 剩余 staging 落盘或累积（batch_io 模式下延迟写盘）
        if !rollback && !staging.is_empty() {
            if self.batch_io {
                self.batch_staging.extend_from_slice(&staging);
                staging.clear();
            } else if let Err(e) = self.commit_staged(&mut staging) {
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
        // 换段前必须排空跨调用累积的 batch_staging——否则旧段数据写入新段
        // 文件（opfuzz 未覆盖 batch_io 档，code review P0-2 实证 ack 丢失）
        self.flush_batch()?;
        // 空 active 封存是无条件 no-op：封存会创建与 active 同 base 的新段
        // （next_offset == active.base 时路径重合），两个 Segment 对象共享
        // 同一路径，任何一侧的文件删除都会炸掉另一侧的数据
        // （opfuzz seed=1 实证：delete 删 sealed 空段文件 = 删 active 数据）。
        if self.active.bytes == 0 && self.active.next_rel == 0 {
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

    /// IO 批量合并：将 batch_io 模式下累积的数据一次性写入磁盘。
    pub fn flush_batch(&mut self) -> Result<()> {
        if self.batch_staging.is_empty() {
            return Ok(());
        }
        self.disk.append(&self.active.path, &self.batch_staging)?;
        self.batch_staging.clear();
        Ok(())
    }

    /// 回滚时清空累积的 batch_staging。
    pub fn clear_batch(&mut self) {
        self.batch_staging.clear();
    }

    /// 持久化点（ADR-14）：排空 staging 后 fsync——调用后本日志全部已
    /// append 数据对掉电持久。本契约与 DiskIo 实现无关：
    /// StdDisk（缓冲写）= page cache 写入 + fsync；
    /// DirectDisk（O_DIRECT，M4 预留）= 设备写 + FLUSH CACHE。
    /// 禁止假设缓冲 I/O 特有行为（如"未 fsync 仍可读"作为持久性依据）。
    pub fn sync(&mut self) -> Result<()> {
        self.flush_batch()?;
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
        // perf #1：单次大读——将 [start_pos, start_pos+want) 一次读入内存，
        // 逐批解析筛选。消除每批 2 次 pread 与 resize 零填充。
        // want 上界：max_bytes + 索引间隔（from_offset 前跳批窗口）+ 批头，
        // 封顶段剩余。（SemDisk/StdDisk 的 read_at 均按 effective 长度截断。）
        let remaining = seg.bytes.saturating_sub(start_pos);
        let want = (max_bytes as u64 + INDEX_INTERVAL_BYTES + RECORD_BATCH_HEADER_LEN as u64)
            .min(remaining) as usize;
        let mut raw = pool.acquire(want);
        raw.resize(want, 0);
        let got = self.disk.read_at(&seg.path, start_pos, &mut raw)?;
        raw.truncate(got);

        let mut out = pool.acquire(max_bytes.min(1024 * 1024) as usize);
        let mut pos: usize = 0;
        let mut first_offset: Option<i64> = None;
        let mut budget: i64 = max_bytes as i64;

        loop {
            if pos + RECORD_BATCH_HEADER_LEN > raw.len() {
                // 读窗口耗尽：若段仍有数据且尚未返回任何批，补充读窗口至段尾
                // ——保持旧实现"首批即便超预算/超窗口也整批带上"的语义，
                // 否则消费端对 HW 内位点立即重取形成活锁（三轮 review P1）。
                if out.is_empty() && (pos as u64) < seg.bytes {
                    let more = (seg.bytes - pos as u64) as usize;
                    raw.resize(raw.len() + more, 0);
                    let got = self
                        .disk
                        .read_at(&seg.path, pos as u64, &mut raw[pos..])
                        .unwrap_or(0);
                    raw.truncate(pos + got);
                    continue;
                }
                break;
            }
            let Some(h) = BatchHeader::parse(&raw[pos..]) else { break };
            let total = h.total_len() as usize;
            if total == 0 || pos + total > raw.len() {
                break;
            }
            // 消费读不得越过 HW（未提交数据不可见）
            if cap == ReadCap::HighWatermark && (h.base_offset as i64) >= upper {
                break;
            }
            // 整批粒度：预算不足但已读到数据 → 停；首批即便超预算也带上（Kafka 同义）
            if budget <= 0 && !out.is_empty() {
                break;
            }
            let batch_last = h.base_offset as i64 + h.record_count as i64 - 1;
            if batch_last < from_offset {
                pos += total;
                continue;
            }
            out.extend_from_slice(&raw[pos..pos + total]);
            if first_offset.is_none() {
                first_offset = Some(h.base_offset);
            }
            budget -= total as i64;
            pos += total;
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
        // active 是最后一个段：offset >= active.base 必然命中 active。
        // （opfuzz 实证：此前只在 sealed 中找，多段日志下 fetch active 尾部
        //  会返回最后一个 sealed 段——读到旧数据或空。）
        if offset >= self.active.base_offset {
            return &self.active;
        }
        // 否则：sealed 中最后一个 base <= offset 的段；不存在则 active（空日志）
        let idx = self
            .sealed
            .partition_point(|s| s.base_offset <= offset);
        if idx == 0 {
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

    pub fn log_start_offset(&self) -> i64 {
        self.log_start_offset
    }

    /// 段调试转储（opfuzz 诊断用；只读，无副作用）。
    pub fn debug_segments(&self) -> Vec<(i64, u64, i64)> {
        self.sealed
            .iter()
            .map(|s| (s.base_offset, s.bytes, s.next_rel))
            .chain(std::iter::once((
                self.active.base_offset,
                self.active.bytes,
                self.active.next_rel,
            )))
            .collect()
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

    /// 删除 < offset 的记录（DeleteRecords 语义，Kafka 同型）：
    /// - 整段位于 offset 之前的段整体删除；
    /// - 包含 offset 的段**文件原样保留**——批打包格式下物理删前缀必须
    ///   整文件重写 + 段 rebase，否则恢复按位置重排 offset（opfuzz C7'
    ///   两次实证：LEO 复活 / 客户端可见漂移）。读取按 log_start_offset
    ///   过滤，ReadResult 整批返回由客户端过滤；
    /// - log_start_offset 持久化于 recovery.checkpoint（P1-1）。
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
        self.log_start_offset = offset;
        self.persist_checkpoint();
        tracing::info!(offset, removed_segments = removed, "delete_records");
        Ok(())
    }

    /// 恢复 checkpoint：段大小表 + log_start（P1-1）。
    fn persist_checkpoint(&self) {
        let mut cp = String::new();
        if self.log_start_offset > 0 {
            cp.push_str(&format!("start:{}\n", self.log_start_offset));
        }
        for seg in &self.sealed {
            cp.push_str(&format!("{}:{}\n", seg.base_offset, seg.bytes));
        }
        let cp_path = self.dir.join("recovery.checkpoint");
        let _ = std::fs::write(&cp_path, &cp);
        // checkpoint 必须扛掉电：写后 fsync（真实 FS 语义；SimDisk 为 no-op）
        let _ = std::fs::File::open(&cp_path).and_then(|f| f.sync_all());
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
            self.persist_checkpoint();
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
        // 2) offset 落在保留的最后一段内：整段提升为 active，统一批对齐截断
        //    （P0-2 修复：原实现不删该段数据、以 offset 重建空 active——重开复活）
        if offset < self.active.base_offset {
            let old = std::mem::replace(&mut self.active, Segment::new(&self.dir, offset));
            let _ = self.disk.remove(&old.path);
            let _ = self.disk.remove(&old.index_path);
            let _ = self.disk.remove(&old.time_path);
            if let Some(seg) = self.sealed.pop() {
                self.active = seg;
            }
        }
        // 清空批量合并缓冲：截断后旧 staging 写入会流错位（P2）
        self.batch_staging.clear();
        // 3) active 内批对齐截断：**从段首扫描**，保留完整位于 offset 之前的
        //    批，在首个跨线批处截断。不得用 locate(offset) 作扫描起点——
        //    那会跳过包含 < offset 记录的更早批（opfuzz 实证数据丢失）。
        let seg = &mut self.active;
        // 一次性读入内存后批走（P2-2：消除每批 61B pread 的 3 次 syscall；
        // 段大小受 segment_max_bytes 约束，failover 路径可接受）
        let data = self.disk.read_all(&seg.path)?;
        let mut exact: u64 = 0;
        let mut rel_kept: i64 = 0;
        let mut scan: u64 = 0;
        while scan + RECORD_BATCH_HEADER_LEN as u64 <= data.len() as u64 {
            let Some(h) = BatchHeader::parse(&data[scan as usize..]) else { break };
            let total = h.total_len() as u64;
            if total == 0 || scan + total > data.len() as u64 {
                break;
            }
            let end = h.base_offset + h.record_count.max(0) as i64;
            if end > offset {
                break;
            }
            rel_kept = end - seg.base_offset;
            exact = scan + total;
            scan += total;
        }
        let kept_end = seg.base_offset + rel_kept;
        self.disk.truncate(&seg.path, exact)?;
        self.disk.sync_file(&seg.path)?;
        self.disk.sync_dir(&seg.path)?;
        seg.bytes = exact;
        // 重建内存索引（截断后必须：next_rel/offset_index/bytes 与文件一致
        // ——P3 清理时误删本调用导致 LEO 簿记失真，opfuzz batch_io 档实证）
        crate::log::rescan_segment(&self.disk, seg);
        self.next_offset = kept_end;
        // 运行期不变式：log_start 不得高于 LEO（DeleteRecords 落批中 +
        // 分叉尾巴截断落入同一批时，kept_end 会低于 log_start——钳制，
        // 否则 read 永久 OffsetOutOfRange、list_offset 虚报，三轮 review P2-1）
        if self.log_start_offset > kept_end {
            self.log_start_offset = kept_end;
            self.persist_checkpoint();
        }
        if self.high_watermark > kept_end {
            self.high_watermark = kept_end;
        }
        // P2：epoch 历史按 kept_end 裁尾（end_offset_for_epoch 防虚报）
        while self
            .epoch_history
            .last()
            .map(|&(_, o)| o > kept_end)
            .unwrap_or(false)
        {
            self.epoch_history.pop();
        }
        if self.epoch_history.is_empty() {
            self.epoch_history.push((0, kept_end));
        }
        self.persist_checkpoint();
        tracing::warn!(offset, kept_end, "log truncated (divergent tail healing)");
        Ok(())
    }

    fn truncate_all(&mut self) -> Result<()> {
        for seg in self.sealed.clone() {
            let _ = self.disk.remove(&seg.path);
            let _ = self.disk.remove(&seg.index_path);
            let _ = self.disk.remove(&seg.time_path);
        }
        self.sealed.clear();
        self.batch_staging.clear();
        let _ = self.disk.remove(&self.active.path);
        let _ = self.disk.remove(&self.active.index_path);
        let _ = self.disk.remove(&self.active.time_path);
        self.active = Segment::new(&self.dir, 0);
        self.next_offset = 0;
        self.high_watermark = 0;
        self.log_start_offset = 0;
        self.persist_checkpoint();
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
        // total < 61 视为损坏尾（bit rot 可把 batch_length 打成 < 49）
        if total < RECORD_BATCH_HEADER_LEN || pos + total > data.len() {
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
