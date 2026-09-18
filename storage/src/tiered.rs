//! 分层注册表与读穿透（T-M4.3 块 b，ADR-21 §2）。
//!
//! 权威 = 对象存储上的每段一条 immutable 记录（create CAS）；本结构的
//! 内存 map 只是缓存——恢复（load）以 store 为准，「指针 ≠ 事实」。
//!
//! 写序（Morax 孤儿无害方向）：段对象 create → 段记录 create → 本地回收。
//! 任一步失败重试安全：对象已存在且内容一致 = 幂等成功；记录已存在 =
//! 已上传（以 store 内容为准）。记录成功前的孤儿对象 = 垃圾，启动期
//! GC 兜底（v1 不做运行期 GC——上传在途竞态需要 mtime/宽限窗口，P2）。

use crate::error::{Result, StorageError};
use crate::object_store::{
    keys, decode_record, encode_record, CreateOutcome, ObjectStore, SegmentRecord,
};
use basalt_record::{BatchHeader, RECORD_BATCH_HEADER_LEN};
use bytes::Bytes;
use std::collections::BTreeMap;

pub struct TieredPartition {
    pub topic: String,
    pub partition: i32,
    /// base → 段记录（缓存；权威在 store）
    segments: BTreeMap<i64, SegmentRecord>,
}

impl TieredPartition {
    /// 恢复：list 记录前缀重建注册表（分区 actor 启动路径）。
    pub fn load(store: &dyn ObjectStore, topic: &str, partition: i32) -> Result<Self> {
        let prefix = keys::record_prefix(topic, partition);
        let mut segments: BTreeMap<i64, SegmentRecord> = BTreeMap::new();
        for key in store.list(&prefix)? {
            let rec = decode_record(&store.get(&key)?)?;
            // 同 base 多 epoch 记录：最大 epoch 为权威（failover 后新内容）
            match segments.get(&rec.base) {
                Some(old) if old.epoch >= rec.epoch => {}
                _ => {
                    segments.insert(rec.base, rec);
                }
            }
        }
        Ok(Self {
            topic: topic.to_string(),
            partition,
            segments,
        })
    }

    pub fn min_base(&self) -> Option<i64> {
        self.segments.keys().next().copied()
    }

    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// 该 base 是否已分层（上传触发面的去重）
    pub fn has(&self, base: i64) -> bool {
        self.segments.contains_key(&base)
    }

    /// from 是否落在某已分层段的 [base, last] 内（读穿透可服务的必要条件）
    pub fn covers(&self, from: i64) -> bool {
        self.segments
            .values()
            .any(|r| r.base <= from && from <= r.last_offset)
    }

    /// 上传一个 sealed 段（幂等）。key 编入 leader epoch（v1.1）：同 base
    /// 不同 epoch = 不同对象（failover/截断后内容漂移不再覆盖，记录追加
    /// 式）；同 key 内容不一致 = 真 异常（同 epoch 单写者下不应发生）→
    /// warn + 覆盖兜底。crc32c 随记录存档，读回校验；稀疏批边界索引用于
    /// range 读定位。
    pub fn upload(
        &mut self,
        store: &dyn ObjectStore,
        base: i64,
        last_offset: i64,
        epoch: i32,
        bytes: &[u8],
        index: Vec<(u32, u32)>,
    ) -> Result<SegmentRecord> {
        let skey = keys::segment(&self.topic, self.partition, base, epoch);
        let crc32 = crc32c::crc32c(bytes);
        match store.create(&skey, bytes)? {
            CreateOutcome::Stored => {}
            CreateOutcome::Existed(existing) => {
                if existing != bytes {
                    tracing::warn!(
                        topic = %self.topic, partition = self.partition, base, epoch,
                        "tiered segment content mismatch within same epoch (anomaly) → overwrite"
                    );
                    store.put(&skey, bytes)?;
                }
            }
        }
        let rec = SegmentRecord {
            base,
            last_offset,
            bytes: bytes.len() as u64,
            key: skey,
            epoch,
            crc32,
            index,
        };
        let rkey = keys::record(&self.topic, self.partition, base, epoch);
        match store.create(&rkey, &encode_record(&rec)?)? {
            CreateOutcome::Stored => {}
            // 记录已存在：以 store 权威内容为准（幂等重试路径）
            CreateOutcome::Existed(existing) => {
                let stored = decode_record(&existing)?;
                return Ok(stored);
            }
        }
        self.segments.insert(rec.base, rec.clone());
        Ok(rec)
    }

    /// 读穿透：从 from 起收集 ≤ max_bytes 的原始批流（跳过段内更早的批；
    /// 与本地 .log 同格式，fetch 直连）。无覆盖段 / 无可读批 → None。
    pub fn read_through(
        &self,
        store: &dyn ObjectStore,
        from: i64,
        max_bytes: usize,
    ) -> Result<Option<Bytes>> {
        let Some(rec) = self
            .segments
            .values()
            .rev()
            .find(|r| r.base <= from && from <= r.last_offset)
            .cloned()
        else {
            return Ok(None);
        };
        // range 读定位：稀疏索引取 ≤ from-base 的最近批边界；无索引则段首
        let rel = (from - rec.base) as u32;
        let start_pos = rec
            .index
            .iter()
            .filter(|(r, _)| *r <= rel)
            .map(|(_, pos)| *pos as u64)
            .max()
            .unwrap_or(0);
        // crc32 校验（读穿透端到端完整性）
        let data = store.get_range(&rec.key, start_pos, rec.bytes as usize - start_pos as usize)?;
        if crc32c::crc32c(&data) != rec.crc32 {
            return Err(StorageError::Other(format!(
                "tiered segment crc mismatch {}",
                rec.key
            )));
        }
        let mut pos = 0usize;
        let mut out: Vec<u8> = Vec::with_capacity(max_bytes.min(1 << 20));
        let mut budget = max_bytes as i64;
        while pos + RECORD_BATCH_HEADER_LEN <= data.len() {
            let Some(h) = BatchHeader::parse(&data[pos..]) else { break };
            let total = h.total_len();
            if total == 0 || pos + total > data.len() {
                break;
            }
            let batch_last = h.base_offset as i64 + h.record_count as i64 - 1;
            if batch_last >= from {
                if budget <= 0 && !out.is_empty() {
                    break;
                }
                out.extend_from_slice(&data[pos..pos + total]);
                budget -= total as i64;
            }
            pos += total;
        }
        if out.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Bytes::from(out)))
        }
    }

    /// 纯 store 侧上传（异步 offloader 用；注册表插入由 actor 在完成
    /// 回调时经 insert_record 进行）。语义同 upload。
    #[allow(clippy::too_many_arguments)]
    pub fn upload_static(
        store: &dyn ObjectStore,
        topic: &str,
        partition: i32,
        base: i64,
        last_offset: i64,
        epoch: i32,
        bytes: &[u8],
        index: Vec<(u32, u32)>,
    ) -> Result<SegmentRecord> {
        let skey = keys::segment(topic, partition, base, epoch);
        let crc32 = crc32c::crc32c(bytes);
        match store.create(&skey, bytes)? {
            CreateOutcome::Stored => {}
            CreateOutcome::Existed(existing) => {
                if existing != bytes {
                    tracing::warn!(
                        topic, partition, base, epoch,
                        "tiered segment content mismatch within same epoch (anomaly) → overwrite"
                    );
                    store.put(&skey, bytes)?;
                }
            }
        }
        let rec = SegmentRecord {
            base,
            last_offset,
            bytes: bytes.len() as u64,
            key: skey,
            epoch,
            crc32,
            index,
        };
        let rkey = keys::record(topic, partition, base, epoch);
        match store.create(&rkey, &encode_record(&rec)?)? {
            CreateOutcome::Stored => {}
            CreateOutcome::Existed(existing) => return Ok(decode_record(&existing)?),
        }
        Ok(rec)
    }

    /// offloader 完成回调：注册表（缓存）插入；同 base 取更大 epoch。
    pub fn insert_record(&mut self, rec: SegmentRecord) {
        match self.segments.get(&rec.base) {
            Some(old) if old.epoch >= rec.epoch => return,
            _ => {}
        }
        self.segments.insert(rec.base, rec);
    }

    /// 孤儿 GC（有段对象、无段记录 = 上传中途崩溃的垃圾）：mtime 早于
    /// 宽限窗的才删——在途上传的保护窗（v1 启动期调用传 grace=0；运行期
    /// 由 retention sweep 周期调用，grace 经环境注入）。返回删除数。
    pub fn gc_orphans(
        store: &dyn ObjectStore,
        topic: &str,
        partition: i32,
        now_ms: u64,
        grace_ms: u64,
    ) -> Result<usize> {
        let recs = store.list(&keys::record_prefix(topic, partition))?;
        let mut has_record: std::collections::BTreeSet<String> = Default::default();
        for rk in recs {
            if let Ok(rec) = decode_record(&store.get(&rk)?) {
                has_record.insert(rec.key);
            }
        }
        let seg_prefix = keys::segment_prefix(topic, partition);
        let mut deleted = 0;
        for sk in store.list(&seg_prefix)? {
            if has_record.contains(&sk) {
                continue;
            }
            // mtime 未知（假体）→ 视为足够老可删；本地实现有真实 mtime
            let aged = match store.mtime_ms(&sk) {
                Some(mt) => now_ms.saturating_sub(mt) >= grace_ms,
                None => true,
            };
            if aged {
                store.delete(&sk)?;
                deleted += 1;
            }
        }
        Ok(deleted)
    }
}

#[cfg(test)]
mod tiered_tests {
    //! 内存假体全离线（Arroyo 测试纪律）：CAS 幂等 / 写序崩溃点 / 读穿透
    //! 批过滤 / 孤儿 GC。

    use super::*;
    use crate::object_store::MemoryObjectStore;
    use basalt_record::{encode_batch, Rec};

    fn seg_bytes(base: i64, count: i64) -> Vec<u8> {
        let recs: Vec<Rec> = (0..count)
            .map(|i| Rec { timestamp_delta: i, key: None, value: Some(bytes::Bytes::from(format!("v{}", base + i))), headers: vec![] })
            .collect();
        let mut b = bytes::BytesMut::new();
        encode_batch(base, 0, 1000 + base, 0, -1, -1, -1, &recs, &mut b);
        b.freeze().to_vec()
    }

    #[test]
    fn upload_idempotent_and_load_roundtrip() {
        let store = MemoryObjectStore::default();
        let mut t = TieredPartition::load(&store, "t", 0).unwrap();
        assert_eq!(t.min_base(), None);
        let r1 = t.upload(&store, 0, 4, 0, &seg_bytes(0, 5), vec![]).unwrap();
        assert_eq!(r1.base, 0);
        // 幂等重试：同段再传 → 回 store 权威记录
        let r2 = t.upload(&store, 0, 4, 0, &seg_bytes(0, 5), vec![]).unwrap();
        assert_eq!(r1, r2);
        // 新实例恢复：缓存可丢弃，load 重建
        let t2 = TieredPartition::load(&store, "t", 0).unwrap();
        assert_eq!(t2.segment_count(), 1);
        assert!(t2.covers(3));
        assert!(!t2.covers(5));
    }

    #[test]
    fn read_through_skips_old_batches() {
        let store = MemoryObjectStore::default();
        let mut t = TieredPartition::load(&store, "t", 0).unwrap();
        t.upload(&store, 0, 9, 0, &seg_bytes(0, 10), vec![]).unwrap();
        // 从段首读
        let d = t.read_through(&store, 0, 1 << 20).unwrap().unwrap();
        assert!(!d.is_empty());
        // 从段中读：跳过更早批（首批 base ≥ 3）
        let d = t.read_through(&store, 3, 1 << 20).unwrap().unwrap();
        let h = BatchHeader::parse(&d).unwrap();
        assert!(h.base_offset + h.record_count as i64 - 1 >= 3, "读穿透必须跳过段内更早批");
        // budget 收口
        let d = t.read_through(&store, 0, 1).unwrap().unwrap();
        assert!(!d.is_empty(), "首批即便超预算也带上（log 同义）");
        // 无覆盖
        assert!(t.read_through(&store, 100, 1 << 20).unwrap().is_none());
    }

    #[test]
    fn gc_orphans_removes_unrecorded_segments() {
        let store = MemoryObjectStore::default();
        let mut t = TieredPartition::load(&store, "t", 0).unwrap();
        t.upload(&store, 0, 4, 0, &seg_bytes(0, 5), vec![]).unwrap();
        // 模拟上传中途崩溃：段对象在、记录缺失
        store.put(&keys::segment("t", 0, 5, 0), b"orphan").unwrap();
        let now_ms = 10_000u64;
        assert_eq!(TieredPartition::gc_orphans(&store, "t", 0, now_ms, 0).unwrap(), 1);
        assert!(store.get(&keys::segment("t", 0, 5, 0)).is_err(), "孤儿被清");
        assert!(store.get(&keys::segment("t", 0, 0, 0)).is_ok(), "有记录的段不动");
    }
}
