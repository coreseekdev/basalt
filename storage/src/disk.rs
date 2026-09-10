//! DiskIo 抽象（ADR-7 硬约束）：路径寻址的读写/同步/目录操作。
//!
//! - [`StdDisk`]：真实文件系统；内部按路径缓存追加句柄，`write_all` 直写
//!   （ADR-14 写边界：append 即 page-cache 可见；持久性仅由 [`DiskIo::sync_file`]
//!   保证，无 BufWriter 式隐式缓冲）。
//! - 仿真实现（torn write/ENOSPC/crash 丢 pending）由 basalt-testing 提供，
//!   实现同一 trait——「ack 前必须 sync」的正确性由仿真逼出。
//!
//! 线程模型：实现内部用 Mutex 保护句柄表（写路径由单一 actor 串行化，
//! Mutex 只保护句柄复用，不引入数据竞争）。

use crate::error::Result;
use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub trait DiskIo: Send + Sync + 'static {
    fn create_dir_all(&self, dir: &Path) -> Result<()>;
    fn exists(&self, path: &Path) -> bool;
    fn remove(&self, path: &Path) -> Result<()>;
    fn rename(&self, from: &Path, to: &Path) -> Result<()>;
    /// 列出目录下文件名（不含子目录）。
    fn list(&self, dir: &Path) -> Result<Vec<String>>;
    /// 追加写（不保证持久；持久性由 [`DiskIo::sync_file`] 显式保证）。
    fn append(&self, path: &Path, data: &[u8]) -> Result<u64>;
    /// flush + fsync（持久化边界）。
    fn sync_file(&self, path: &Path) -> Result<()>;
    fn len(&self, path: &Path) -> Result<u64>;
    fn truncate(&self, path: &Path, size: u64) -> Result<()>;
    fn read_at(&self, path: &Path, pos: u64, buf: &mut [u8]) -> Result<usize>;
    /// 整文件读（恢复扫描用）。
    fn read_all(&self, path: &Path) -> Result<Vec<u8>>;
    /// fsync 目录项（段创建/截断/删除后的持久性）。
    fn sync_dir(&self, path: &Path) -> Result<()>;
}

/// 生产实现：文件系统 + 句柄缓存。
pub struct StdDisk {
    append_handles: Mutex<HashMap<PathBuf, std::fs::File>>,
    read_handles: Mutex<HashMap<PathBuf, std::fs::File>>,
}

impl StdDisk {
    pub fn new() -> Self {
        StdDisk {
            append_handles: Mutex::new(HashMap::new()),
            read_handles: Mutex::new(HashMap::new()),
        }
    }

    fn append_handle(&self, path: &Path) -> Result<std::fs::File> {
        let mut hs = self.append_handles.lock().unwrap();
        if let Some(f) = hs.get(path) {
            return match f.try_clone() {
                Ok(c) => Ok(c),
                Err(_) => {
                    hs.remove(path);
                    Ok(std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(path)?)
                }
            };
        }
        let f = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
        hs.insert(path.to_path_buf(), f.try_clone()?);
        Ok(f)
    }

    fn read_handle(&self, path: &Path) -> Result<std::fs::File> {
        let mut hs = self.read_handles.lock().unwrap();
        if let Some(f) = hs.get(path) {
            return match f.try_clone() {
                Ok(c) => Ok(c),
                Err(_) => {
                    hs.remove(path);
                    Ok(std::fs::File::open(path)?)
                }
            };
        }
        let f = std::fs::File::open(path)?;
        hs.insert(path.to_path_buf(), f.try_clone()?);
        Ok(f)
    }
}

impl Default for StdDisk {
    fn default() -> Self {
        Self::new()
    }
}

impl DiskIo for StdDisk {
    fn create_dir_all(&self, dir: &Path) -> Result<()> {
        std::fs::create_dir_all(dir)?;
        Ok(())
    }

    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn remove(&self, path: &Path) -> Result<()> {
        self.append_handles.lock().unwrap().remove(path);
        self.read_handles.lock().unwrap().remove(path);
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        std::fs::rename(from, to)?;
        Ok(())
    }

    fn list(&self, dir: &Path) -> Result<Vec<String>> {
        let mut out = Vec::new();
        for e in std::fs::read_dir(dir)? {
            let e = e?;
            if e.file_type()?.is_file() {
                out.push(e.file_name().to_string_lossy().into_owned());
            }
        }
        out.sort();
        Ok(out)
    }

    fn append(&self, path: &Path, data: &[u8]) -> Result<u64> {
        let mut f = self.append_handle(path)?;
        f.write_all(data)?;
        // 尺寸由 Log 内存记账，避免每 append 一次 stat
        Ok(0)
    }

    fn sync_file(&self, path: &Path) -> Result<()> {
        let hs = self.append_handles.lock().unwrap();
        match hs.get(path) {
            Some(f) => f.sync_all()?,
            None => std::fs::OpenOptions::new().append(true).open(path)?.sync_all()?,
        }
        Ok(())
    }

    fn len(&self, path: &Path) -> Result<u64> {
        Ok(std::fs::metadata(path)?.len())
    }

    fn truncate(&self, path: &Path, size: u64) -> Result<()> {
        let mut f = std::fs::OpenOptions::new().write(true).create(true).open(path)?;
        f.set_len(size)?;
        f.flush()?;
        Ok(())
    }

    fn read_at(&self, path: &Path, pos: u64, buf: &mut [u8]) -> Result<usize> {
        use std::os::unix::fs::FileExt;
        let f = self.read_handle(path)?;
        // pread：无 seek 竞态、免 lseek syscall
        let mut total = 0;
        while total < buf.len() {
            match f.read_at(&mut buf[total..], pos + total as u64)? {
                0 => break,
                n => total += n,
            }
        }
        Ok(total)
    }

    fn read_all(&self, path: &Path) -> Result<Vec<u8>> {
        Ok(std::fs::read(path)?)
    }

    fn sync_dir(&self, path: &Path) -> Result<()> {
        // fsync 目录项（Linux）；对父目录 open+sync_all
        let dir = path.parent().unwrap_or(Path::new("."));
        let f = std::fs::File::open(dir)?;
        f.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_read_sync_roundtrip() {
        let dir = std::env::temp_dir().join(format!("basalt-disk-test-{}", std::process::id()));
        let disk = StdDisk::new();
        let p = dir.join("t.log");
        disk.create_dir_all(&dir).unwrap();
        disk.append(&p, b"hello").unwrap();
        disk.append(&p, b" world").unwrap();
        assert_eq!(disk.len(&p).unwrap(), 11);
        let mut buf = [0u8; 5];
        disk.read_at(&p, 6, &mut buf).unwrap();
        assert_eq!(&buf, b"world");
        disk.sync_file(&p).unwrap();
        disk.remove(&p).unwrap();
        std::fs::remove_dir(&dir).ok();
    }
}
