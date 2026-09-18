//! 对象存储原语（T-M4.3，ADR-21 §2）：四原语契约按 Arroyo §11 收窄——
//! get / put（限可变对象）/ **create**（CAS put_if_not_exists，冲突回读
//! 已有字节让调用方区分「幂等成功」与「所有权冲突」）/ delete + list。
//! v1 实现：LocalFsObjectStore（CAS = O_EXCL 原子创建）+ MemoryObjectStore
//! （测试假体，Arroyo 内存假体同款——全部协议单测不碰真 S3）。
//!
//! S3 适配器 = 同契约换入（边界见 ADR-21 §3）：S3 无原生 CAS，惯例是
//! put-if-absent（If-None-Match: *）+ 冲突 GET 回读，与本契约同形。

use crate::error::{StorageError, Result};
use std::collections::BTreeMap;
use std::path::PathBuf;

pub struct GetResult {
    pub bytes: Vec<u8>,
}

/// 对象存储四原语。key 视为无层级字符串（实现方自行映射前缀目录）。
pub trait ObjectStore: Send + Sync {
    /// 读取；不存在 → StorageError::OffsetOutOfRange 承载「未找到」
    /// （语义 = 该 offset/对象不可读，调用方按未命中分流）。
    fn get(&self, key: &str) -> Result<Vec<u8>>;
    /// 覆盖写（仅限可变对象；本设计内段对象/段记录均 immutable，不用它）。
    fn put(&self, key: &str, bytes: &[u8]) -> Result<()>;
    /// CAS 条件创建：已存在时不覆盖、**回读已有字节** → Err(AlreadyExists
    /// 携带已有内容)。契约要求 read-after-write 一致（Arroyo store.rs 契约）。
    fn create(&self, key: &str, bytes: &[u8]) -> Result<CreateOutcome>;
    fn delete(&self, key: &str) -> Result<()>;
    /// 列出前缀下全部 key（字典序）。
    fn list(&self, prefix: &str) -> Result<Vec<String>>;
    /// 范围读（大段部分读；S3 = Range header）。越界部分截断。
    fn get_range(&self, key: &str, start: u64, len: usize) -> Result<Vec<u8>> {
        let all = self.get(key)?;
        let s = (start as usize).min(all.len());
        let e = (s + len).min(all.len());
        Ok(all[s..e].to_vec())
    }
    /// 对象修改时刻（epoch ms）；None = 实现不提供（GC 跳过该对象）。
    fn mtime_ms(&self, _key: &str) -> Option<u64> {
        None
    }
}

/// create 的成功/幂等结果：Stored = 本调用写入；AlreadyExistsStored(已有字节)
/// = 条件创建撞车（幂等成功路径——Arroyo「冲突回读比对」）。
#[derive(Debug, PartialEq)]
pub enum CreateOutcome {
    Stored,
    /// 已有对象的内容（幂等重试路径）
    Existed(Vec<u8>),
}

// ---------- 本地文件系统实现（CAS = O_EXCL） ----------

pub struct LocalFsObjectStore {
    root: PathBuf,
}

impl LocalFsObjectStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        std::fs::create_dir_all(&root).ok();
        LocalFsObjectStore { root }
    }

    fn path_of(&self, key: &str) -> PathBuf {
        // key 约定不含 ".."、以 alnum//.- 组成（段 key 由本 crate 生成）；
        // 防 path traversal：拒绝包含 ".." 组件
        debug_assert!(!key.split('/').any(|c| c == ".."));
        self.root.join(key.trim_start_matches('/'))
    }
}

impl ObjectStore for LocalFsObjectStore {
    fn get(&self, key: &str) -> Result<Vec<u8>> {
        match std::fs::read(self.path_of(key)) {
            Ok(b) => Ok(b),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(StorageError::OffsetOutOfRange(-1))
            }
            Err(e) => Err(StorageError::Other(format!("objectstore get {key}: {e}"))),
        }
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<()> {
        let p = self.path_of(key);
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| StorageError::Other(format!("objectstore mkdir: {e}")))?;
        }
        std::fs::write(&p, bytes).map_err(|e| StorageError::Other(format!("objectstore put {key}: {e}")))
    }

    fn create(&self, key: &str, bytes: &[u8]) -> Result<CreateOutcome> {
        let p = self.path_of(key);
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| StorageError::Other(format!("objectstore mkdir: {e}")))?;
        }
        // CAS：O_EXCL 原子创建；已存在 → 回读已有字节（幂等成功）
        match std::fs::OpenOptions::new().write(true).create_new(true).open(&p) {
            Ok(mut f) => {
                use std::io::Write;
                f.write_all(bytes)
                    .map_err(|e| StorageError::Other(format!("objectstore create {key}: {e}")))?;
                f.sync_all().ok();
                Ok(CreateOutcome::Stored)
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                let existing = self.get(key)?;
                Ok(CreateOutcome::Existed(existing))
            }
            Err(e) => Err(StorageError::Other(format!("objectstore create {key}: {e}"))),
        }
    }

    fn get_range(&self, key: &str, start: u64, len: usize) -> Result<Vec<u8>> {
        use std::io::{Read, Seek, SeekFrom};
        let p = self.path_of(key);
        let mut f = std::fs::File::open(&p)
            .map_err(|e| StorageError::Other(format!("objectstore open {key}: {e}")))?;
        let flen = f.metadata().map(|m| m.len()).unwrap_or(0);
        let start = start.min(flen);
        let len = len.min((flen - start) as usize);
        f.seek(SeekFrom::Start(start))
            .map_err(|e| StorageError::Other(format!("objectstore seek {key}: {e}")))?;
        let mut buf = vec![0u8; len];
        f.read_exact(&mut buf).map_err(|e| StorageError::Other(format!("objectstore read {key}: {e}")))?;
        Ok(buf)
    }

    fn mtime_ms(&self, key: &str) -> Option<u64> {
        std::fs::metadata(self.path_of(key))
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as u64)
    }

    fn delete(&self, key: &str) -> Result<()> {
        match std::fs::remove_file(self.path_of(key)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(StorageError::Other(format!("objectstore delete {key}: {e}"))),
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let dir = self.path_of(prefix.trim_end_matches('/'));
        let mut out = Vec::new();
        let mut stack = vec![dir];
        while let Some(d) = stack.pop() {
            let entries = match std::fs::read_dir(&d) {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(StorageError::Other(format!("objectstore list: {e}"))),
            };
            for ent in entries.flatten() {
                let p = ent.path();
                if p.is_dir() {
                    stack.push(p);
                } else {
                    let rel = p
                        .strip_prefix(&self.root)
                        .map_err(|e| StorageError::Other(format!("objectstore list strip: {e}")))?;
                    out.push(format!("/{}", rel.to_string_lossy()));
                }
            }
        }
        out.sort();
        Ok(out)
    }
}

// ---------- 内存假体（单测；Arroyo MemoryProtocolStore 同款） ----------

#[derive(Default)]
pub struct MemoryObjectStore {
    objects: std::sync::Mutex<BTreeMap<String, Vec<u8>>>,
}

impl ObjectStore for MemoryObjectStore {
    fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.objects
            .lock().unwrap()
            .get(key)
            .cloned()
            .ok_or(StorageError::OffsetOutOfRange(-1))
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<()> {
        self.objects.lock().unwrap().insert(key.to_string(), bytes.to_vec());
        Ok(())
    }

    fn create(&self, key: &str, bytes: &[u8]) -> Result<CreateOutcome> {
        let mut m = self.objects.lock().unwrap();
        match m.entry(key.to_string()) {
            std::collections::btree_map::Entry::Vacant(e) => {
                e.insert(bytes.to_vec());
                Ok(CreateOutcome::Stored)
            }
            std::collections::btree_map::Entry::Occupied(e) => {
                Ok(CreateOutcome::Existed(e.get().clone()))
            }
        }
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self
            .objects
            .lock().unwrap()
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, _)| k.clone())
            .collect())
    }
}

#[cfg(test)]
mod object_store_tests {
    //! 四原语契约对两个实现双重锁定：CAS 冲突回读 / list 字典序 /
    //! 未命中语义一致——S3 适配器将来按同表换入。

    use super::*;

    fn contract(s: &dyn ObjectStore) {
        // key 规范：带前导 "/"（keys::* 生成即此形态）
        // create 成功 + 幂等撞车回读
        assert_eq!(s.create("/a/seg/0001.log", b"v1").unwrap(), CreateOutcome::Stored);
        assert_eq!(
            s.create("/a/seg/0001.log", b"v2").unwrap(),
            CreateOutcome::Existed(b"v1".to_vec()),
            "冲突必须回读已有字节（幂等成功 ≠ 覆盖）"
        );
        assert_eq!(s.get("/a/seg/0001.log").unwrap(), b"v1");
        // 未命中语义一致
        assert!(s.get("/a/seg/missing").is_err());
        // put 覆盖写（可变对象面）+ list 字典序
        s.put("/a/seg/0000.log", b"z").unwrap();
        s.put("/a/rec/0000.json", b"{}").unwrap();
        assert_eq!(
            s.list("/a/").unwrap(),
            vec!["/a/rec/0000.json", "/a/seg/0000.log", "/a/seg/0001.log"]
        );
        // delete 幂等（不存在也 Ok）
        s.delete("/a/seg/0000.log").unwrap();
        s.delete("/a/seg/0000.log").unwrap();
        assert!(s.get("/a/seg/0000.log").is_err());
    }

    #[test]
    fn local_fs_contract() {
        let dir = std::env::temp_dir().join(format!(
            "basalt-objstore-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let s = LocalFsObjectStore::new(&dir);
        contract(&s);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn memory_contract() {
        contract(&MemoryObjectStore::default());
    }

    /// CAS 并发竞争：双写者同 key create，恰一人 Stored、一人 Existed。
    #[test]
    fn create_cas_race_single_winner() {
        let s = std::sync::Arc::new(MemoryObjectStore::default());
        let mut handles = Vec::new();
        for i in 0..8 {
            let s = s.clone();
            handles.push(std::thread::spawn(move || {
                s.create("/race/x", format!("w{i}").as_bytes()).unwrap()
            }));
        }
        let mut stored = 0;
        for h in handles {
            if h.join().unwrap() == CreateOutcome::Stored {
                stored += 1;
            }
        }
        assert_eq!(stored, 1, "CAS 恰一赢家");
        assert_eq!(s.get("/race/x").unwrap().len(), 2);
    }
}

/// 段对象/段记录 key 生成（ADR-21 §2 + v1.1：key 编入 leader epoch——
/// failover/截断后的同 base 新内容 = 新 key，对象层面永不覆盖；
/// Redpanda term-in-key 同款）
pub mod keys {
    pub fn segment(topic: &str, partition: i32, base: i64, epoch: i32) -> String {
        format!("/{topic}/p{partition}/seg/{base:020}.e{epoch}.log")
    }
    pub fn record(topic: &str, partition: i32, base: i64, epoch: i32) -> String {
        format!("/{topic}/p{partition}/rec/{base:020}.e{epoch}.json")
    }
    pub fn segment_prefix(topic: &str, partition: i32) -> String {
        format!("/{topic}/p{partition}/seg/")
    }
    pub fn record_prefix(topic: &str, partition: i32) -> String {
        format!("/{topic}/p{partition}/rec/")
    }
}

/// 段记录（权威、immutable）：指针 ≠ 事实——任何内存视图只是缓存，
/// 恢复以 store 上的记录为准（Arroyo epoch-record 分层同款）。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct SegmentRecord {
    pub base: i64,
    pub last_offset: i64,
    pub bytes: u64,
    /// 段对象 key（含 leader epoch）
    pub key: String,
    /// 上传时的 leader epoch（记录追加式：同 base 不同 epoch = 不同对象）
    pub epoch: i32,
    /// 段对象 crc32c（读回校验）
    pub crc32: u32,
    /// 稀疏批边界索引 (rel_offset, pos)——range 读定位用
    pub index: Vec<(u32, u32)>,
}

pub fn encode_record(r: &SegmentRecord) -> Result<Vec<u8>> {
    serde_json::to_vec(r).map_err(|e| StorageError::Other(format!("record encode: {e}")))
}

pub fn decode_record(b: &[u8]) -> Result<SegmentRecord> {
    serde_json::from_slice(b).map_err(|e| StorageError::Other(format!("record decode: {e}")))
}

/// 路径卫生（Arroyo CheckpointRef 同款）：拒绝 ".." 组件与空组件。
pub fn validate_key(key: &str) -> Result<()> {
    if key.split('/').any(|c| c == ".." || c.is_empty()) {
        return Err(StorageError::Other(format!("bad object key {key}")));
    }
    Ok(())
}
