//! 单段：一个 .log 文件 + 内存索引。
//! 段的所有权归 `Log`（partition actor 独占），无内部锁。

use crate::index::{OffsetIndex, TimeIndex};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct Segment {
    pub base_offset: i64,
    pub path: PathBuf,
    pub index_path: PathBuf,
    pub time_path: PathBuf,
    pub bytes: u64,
    /// 段内已占用的相对 offset 数（next_rel - 1 = 段内最后 offset）。
    pub next_rel: i64,
    pub offset_index: OffsetIndex,
    pub time_index: TimeIndex,
    bytes_since_index: u64,
}

/// 文件名对齐 Kafka：`00000000000000000000.log`。
pub fn base_of_filename(name: &str) -> Option<i64> {
    let stem = name.strip_suffix(".log")?;
    if stem.len() != 20 || !stem.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    stem.parse().ok()
}

pub fn log_filename(base: i64) -> String {
    format!("{base:020}.log")
}

impl Segment {
    pub fn new(dir: &Path, base_offset: i64) -> Segment {
        let name = log_filename(base_offset);
        Segment {
            base_offset,
            path: dir.join(&name),
            index_path: dir.join(name.replace(".log", ".index")),
            time_path: dir.join(name.replace(".log", ".timeindex")),
            bytes: 0,
            next_rel: 0,
            offset_index: OffsetIndex::default(),
            time_index: TimeIndex::default(),
            bytes_since_index: 0,
        }
    }

    /// checkpoint 快照/恢复（append 回滚用）。
    pub fn bytes_since_index_snapshot(&self) -> u64 {
        self.bytes_since_index
    }

    pub fn restore_bytes_since_index(&mut self, v: u64) {
        self.bytes_since_index = v;
    }

    pub fn last_offset(&self) -> i64 {
        self.base_offset + self.next_rel - 1
    }

    pub fn contains(&self, offset: i64) -> bool {
        offset >= self.base_offset && offset <= self.last_offset()
    }

    /// 追加一个已校验批次：物理长度 total、记录数 count、max_ts。
    pub fn push_batch(&mut self, total: usize, count: i64, max_ts: i64) {
        let pos = self.bytes as u32;
        if self.bytes_since_index == 0 {
            self.offset_index.push(self.next_rel as u32, pos);
            self.time_index.push(max_ts, self.next_rel as u32);
        }
        self.next_rel += count;
        self.bytes += total as u64;
        self.bytes_since_index += total as u64;
        if self.bytes_since_index >= crate::log::INDEX_INTERVAL_BYTES {
            self.offset_index.push(self.next_rel as u32, self.bytes as u32);
            self.bytes_since_index = 0;
        }
    }

    /// ≤ target 的最近物理位置。
    pub fn locate(&self, offset: i64) -> u64 {
        self.offset_index.lookup(offset - self.base_offset) as u64
    }

    pub fn persist_indexes(&self, disk: &dyn crate::disk::DiskIo) -> crate::error::Result<()> {
        disk.truncate(&self.index_path, 0)?;
        disk.append(&self.index_path, &self.offset_index.encode())?;
        disk.truncate(&self.time_path, 0)?;
        disk.append(&self.time_path, &self.time_index.encode())?;
        disk.sync_file(&self.index_path)?;
        disk.sync_file(&self.time_path)?;
        Ok(())
    }

    pub fn load_indexes(&mut self, disk: &dyn crate::disk::DiskIo) {
        if let Ok(data) = disk.read_all(&self.index_path) {
            self.offset_index = OffsetIndex::decode(&data);
        }
        if let Ok(data) = disk.read_all(&self.time_path) {
            self.time_index = TimeIndex::decode(&data);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filename_codec() {
        assert_eq!(base_of_filename("00000000000000000042.log"), Some(42));
        assert_eq!(base_of_filename("42.log"), None);
        assert_eq!(base_of_filename("00000000000000000042.index"), None);
        assert_eq!(log_filename(42), "00000000000000000042.log");
    }
}
