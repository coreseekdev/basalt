//! SimDisk：故障注入仿真 DiskIo（ADR-7 硬约束的仿真面）。
//!
//! 特性：
//! - `pending` 缓冲：写先入 pending，`sync_file` 才落 committed
//! - `crash()`: 丢弃全部 pending（模拟进程崩溃）
//! - `torn_write_prob`: sync 时按概率只写前半（torn write 注入）
//! - `enospc_after`: 从第 N 次 write 开始返回 ENOSPC
//! - `fail_prob`: 每次操作按概率返回随机错误

use crate::disk::DiskIo;
use crate::error::{Result, StorageError};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

#[derive(Default)]
struct SimFile {
    committed: Vec<u8>,
    pending: Vec<u8>,
}

#[derive(Default)]
pub struct SimDiskInner {
    files: Mutex<HashMap<PathBuf, SimFile>>,
    write_count: std::sync::atomic::AtomicU64,
    pub torn_write_prob: f64,
    pub enospc_after: u64,
    pub fail_prob: f64,
}

#[derive(Clone)]
pub struct SimDisk {
    inner: std::sync::Arc<SimDiskInner>,
}

impl SimDisk {
    pub fn new() -> SimDisk {
        SimDisk { inner: std::sync::Arc::new(SimDiskInner::default()) }
    }

    pub fn with_faults(torn_write_prob: f64, enospc_after: u64, fail_prob: f64) -> SimDisk {
        SimDisk {
            inner: std::sync::Arc::new(SimDiskInner {
                torn_write_prob,
                enospc_after,
                fail_prob,
                ..Default::default()
            }),
        }
    }

    /// 模拟进程崩溃：丢弃全部 pending 写（已 sync 的 committed 保留）。
    pub fn crash(&self) {
        let mut files = self.inner.files.lock().unwrap();
        for f in files.values_mut() {
            f.pending.clear();
        }
    }

    /// 读取 committed 数据（排除 pending）。
    pub fn committed_data(&self, path: &Path) -> Option<Vec<u8>> {
        self.inner.files.lock().unwrap().get(path).map(|f| f.committed.clone())
    }

    fn should_fail(&self) -> bool {
        self.inner.fail_prob > 0.0 && rand_fail(self.inner.fail_prob)
    }

    fn should_tear(&self) -> bool {
        self.inner.torn_write_prob > 0.0 && rand_fail(self.inner.torn_write_prob)
    }
}

fn rand_fail(prob: f64) -> bool {
    // 无外部 rand crate：用时间戳低位近似
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    (nanos % 1000) as f64 / 1000.0 < prob
}

impl DiskIo for SimDisk {
    fn create_dir_all(&self, _dir: &Path) -> Result<()> { Ok(()) }
    fn exists(&self, path: &Path) -> bool {
        self.inner.files.lock().unwrap().contains_key(path)
    }
    fn remove(&self, path: &Path) -> Result<()> {
        self.inner.files.lock().unwrap().remove(path);
        Ok(())
    }
    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let mut files = self.inner.files.lock().unwrap();
        if let Some(f) = files.remove(from) {
            files.insert(to.to_path_buf(), f);
        }
        Ok(())
    }
    fn list(&self, dir: &Path) -> Result<Vec<String>> {
        let files = self.inner.files.lock().unwrap();
        let mut out = Vec::new();
        for path in files.keys() {
            if let Some(parent) = path.parent() {
                if parent == dir {
                    out.push(path.file_name().unwrap_or_default().to_string_lossy().into_owned());
                }
            }
        }
        out.sort();
        Ok(out)
    }
    fn append(&self, path: &Path, data: &[u8]) -> Result<u64> {
        if self.should_fail() {
            return Err(StorageError::Other("simulated IO failure".into()));
        }
        let wc = self.inner.write_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if self.inner.enospc_after > 0 && wc >= self.inner.enospc_after {
            return Err(StorageError::Io(std::io::Error::from_raw_os_error(28))); // ENOSPC
        }
        let mut files = self.inner.files.lock().unwrap();
        let f = files.entry(path.to_path_buf()).or_default();
        f.pending.extend_from_slice(data);
        Ok((f.pending.len()) as u64)
    }
    fn sync_file(&self, path: &Path) -> Result<()> {
        if self.should_fail() {
            return Err(StorageError::Other("simulated sync failure".into()));
        }
        let mut files = self.inner.files.lock().unwrap();
        let Some(f) = files.get_mut(path) else { return Ok(()) };
        if self.should_tear() && f.pending.len() > 8 {
            // torn write：只 commit 前半
            let half = f.pending.len() / 2;
            f.committed.extend_from_slice(&f.pending[..half]);
            f.pending.clear();
            return Err(StorageError::Other("torn write".into()));
        }
        f.committed.append(&mut f.pending);
        Ok(())
    }
    fn len(&self, path: &Path) -> Result<u64> {
        let files = self.inner.files.lock().unwrap();
        Ok(files.get(path).map(|f| f.pending.len() as u64).unwrap_or(0))
    }
    fn truncate(&self, path: &Path, size: u64) -> Result<()> {
        let mut files = self.inner.files.lock().unwrap();
        let Some(f) = files.get_mut(path) else { return Ok(()) };
        f.committed.truncate(size as usize);
        f.pending.clear();
        Ok(())
    }
    fn read_at(&self, path: &Path, pos: u64, buf: &mut [u8]) -> Result<usize> {
        // 读 committed+pending（模拟 OS page cache：write 后 read 可见）
        let files = self.inner.files.lock().unwrap();
        let Some(f) = files.get(path) else { return Ok(0) };
        let mut effective = f.committed.clone();
        effective.extend_from_slice(&f.pending);
        let start = pos as usize;
        if start >= effective.len() { return Ok(0); }
        let end = (start + buf.len()).min(effective.len());
        let n = end - start;
        buf[..n].copy_from_slice(&effective[start..end]);
        Ok(n)
    }
    fn read_all(&self, path: &Path) -> Result<Vec<u8>> {
        let files = self.inner.files.lock().unwrap();
        let Some(f) = files.get(path) else { return Ok(Vec::new()) };
        let mut effective = f.committed.clone();
        effective.extend_from_slice(&f.pending);
        Ok(effective)
    }
    fn sync_dir(&self, _path: &Path) -> Result<()> { Ok(()) }
}
