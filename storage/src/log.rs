//! 日志 = 段集合（sealed 升序 + active），单写者语义由持有者保证。

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
}

impl Default for LogOptions {
    fn default() -> Self {
        LogOptions { segment_max_bytes: 1024 * 1024 * 1024, fsync: FsyncSchedule::Os }
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

pub struct Log<D: DiskIo> {
    disk: D,
    dir: std::path::PathBuf,
    opts: LogOptions,
    sealed: Vec<Segment>,
    active: Segment,
    pub log_start_offset: i64,
    pub next_offset: i64,
    /// 复制层维护；单机 = next_offset。
    pub high_watermark: i64,
}

impl<D: DiskIo> Log<D> {
    /// 打开/恢复一个分区日志目录。
    pub fn open(disk: D, dir: std::path::PathBuf, opts: LogOptions) -> Result<Log<D>> {
        disk.create_dir_all(&dir)?;
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

        // 逐段扫描校验/重建：坏尾截断（torn write 防线）
        let mut next_offset: i64 = segs.first().map(|s| s.base_offset).unwrap_or(0);
        let mut sealed: Vec<Segment> = Vec::new();
        for seg in &mut segs {
            scan_and_truncate(&disk, seg, next_offset)?;
            if seg.next_rel == 0 && seg.bytes == 0 && !sealed.is_empty() {
                continue; // 空段且有前驱：保留文件但不入列表
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
        };
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

    // ---------- 写路径 ----------

    /// 追加一批（可含多个连续 RecordBatch）。
    pub fn append(&mut self, raw: &Bytes, policy: AssignPolicy, now_ms: i64) -> Result<AppendResult> {
        let log_append_time = now_ms;
        let mut staging = BytesMut::new();
        let mut base_assigned: Option<i64> = None;
        let mut last_offset = self.next_offset - 1;
        let mut pos = 0usize;

        while pos < raw.len() {
            let rest = &raw[pos..];
            let Some(h) = BatchHeader::parse(rest) else {
                return Err(StorageError::CorruptBatch {
                    path: self.active.path.display().to_string(),
                    pos: pos as u64,
                    reason: "header too short".into(),
                });
            };
            let total = h.total_len();
            if rest.len() < total {
                return Err(StorageError::CorruptBatch {
                    path: self.active.path.display().to_string(),
                    pos: pos as u64,
                    reason: "truncated batch".into(),
                });
            }
            if h.magic != crate::MAGIC_V2 {
                return Err(StorageError::CorruptBatch {
                    path: self.active.path.display().to_string(),
                    pos: pos as u64,
                    reason: format!("magic {} unsupported (ADR-4: v2 only)", h.magic),
                });
            }
            let payload = &rest[CRC_PAYLOAD_OFFSET..total];
            if crc32c::crc32c(payload) != h.crc {
                return Err(StorageError::CorruptBatch {
                    path: self.active.path.display().to_string(),
                    pos: pos as u64,
                    reason: "crc mismatch".into(),
                });
            }

            let assigned = match policy {
                AssignPolicy::Assign => self.next_offset,
                AssignPolicy::Absolute => {
                    if h.base_offset != self.next_offset {
                        return Err(StorageError::Other(format!(
                            "replica gap: batch base {} != log next {}",
                            h.base_offset, self.next_offset
                        )));
                    }
                    h.base_offset
                }
            };
            if base_assigned.is_none() {
                base_assigned = Some(assigned);
            }

            // 滚动判定必须在 staging 新批之前：staged 字节必须全部属于当前 active，
            // 索引项才能与物理布局对齐（此前把 roll 放在 staging 之后是错位 bug）。
            if self.active.next_rel > 0
                && self.active.bytes + staging.len() as u64 + total as u64
                    > self.opts.segment_max_bytes
            {
                self.commit_staged(&mut staging, log_append_time)?;
                self.roll()?;
            }

            let count = h.record_count.max(0) as i64;
            let mut batch = BytesMut::from(&rest[..total]);
            h.write_base_offset(&mut batch, assigned);
            staging.extend_from_slice(&batch);
            self.active.push_batch(total, count, h.max_timestamp);
            self.next_offset = assigned + count;
            last_offset = assigned + count - 1;
            pos += total;
        }

        if !staging.is_empty() {
            self.commit_staged(&mut staging, log_append_time)?;
        }
        if self.opts.fsync == FsyncSchedule::SyncEach && pos > 0 {
            self.disk.sync_file(&self.active.path)?;
        }
        if self.high_watermark < self.next_offset {
            self.high_watermark = self.next_offset; // 单机语义：HW=LEO
        }
        Ok(AppendResult {
            base_offset: base_assigned.unwrap_or(self.next_offset),
            last_offset,
            log_append_time,
        })
    }

    fn commit_staged(&mut self, staging: &mut BytesMut, _now: i64) -> Result<()> {
        if staging.is_empty() {
            return Ok(());
        }
        self.disk.append(&self.active.path, staging)?;
        staging.clear();
        Ok(())
    }

    /// 滚动：持久化索引，active → sealed。
    pub fn roll(&mut self) -> Result<()> {
        if self.active.bytes == 0 && self.active.next_rel == 0 && !self.sealed.is_empty() {
            return Ok(());
        }
        self.active.persist_indexes(&self.disk)?;
        if self.opts.fsync != FsyncSchedule::Os {
            self.disk.sync_file(&self.active.path)?;
        }
        let new_base = self.next_offset;
        let seg = std::mem::replace(&mut self.active, Segment::new(&self.dir, new_base));
        self.sealed.push(seg);
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.disk.sync_file(&self.active.path)
    }

    // ---------- 读路径 ----------

    pub fn read(&self, from_offset: i64, max_bytes: usize, pool: &crate::pool::BufferPool) -> Result<ReadResult> {
        if from_offset < self.log_start_offset {
            return Err(StorageError::OffsetOutOfRange(from_offset));
        }
        if from_offset > self.high_watermark {
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
        // 返回 (offset, timestamp)；timestamp=-1 latest、-2 earliest、其余按时间
        if timestamp == -1 {
            return Ok((self.high_watermark, -1));
        }
        if timestamp == -2 {
            return Ok((self.log_start_offset, -1));
        }
        // 按时间：跨段时间索引（每段最后一项）
        for seg in self.sealed.iter().chain(std::iter::once(&self.active)) {
            let within = match seg.time_index.entries.last() {
                Some(&(last_ts, _)) if last_ts >= timestamp => true,
                None => seg.base_offset == self.log_start_offset,
                _ => false,
            };
            if !within {
                continue;
            }
            if let Some((_, rel)) = seg.time_index.lookup(timestamp) {
                return Ok((seg.base_offset + rel as i64, timestamp));
            }
        }
        Ok((-1, -1)) // NOT_FOUND
    }

    pub fn segment_count(&self) -> usize {
        self.sealed.len() + 1
    }
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
