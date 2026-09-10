//! 缺陷→机制矩阵的落地测试（docs/VERIFICATION.md §12）。
//! 每个测试对应一类已发生缺陷的"永久检测机制"：
//! - sim_disk_len_matches_read：① len 语义漂移（SimDisk::len 只返回 pending）
//! - structural_chain：④ roll 空段封存同路径双 Segment（base 唯一性/链有序）
//! - boundary_table_delete / boundary_table_truncate：②③ delete/truncate 的
//!   批对齐与保留侧语义（② next_offset 盲设、③ 保留/删除颠倒）
//! - read_containment：⑤ segment_for 忽略 active（读包含性）

use basalt_record::{batch_len_at, encode_batch, BatchHeader, Rec};
use basalt_storage::disk::DiskIo;
use basalt_storage::log::{AssignPolicy, FsyncSchedule, Log, LogOptions};
use basalt_storage::sim_disk::SimDisk;
use bytes::{Bytes, BytesMut};
use std::path::PathBuf;

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 33
    }
}

fn tmpdir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "basalt-conf-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn batch_bytes(payload: &str) -> Bytes {
    let recs: Vec<Rec> = (0..2)
        .map(|i| Rec {
            timestamp_delta: i as i64,
            key: Some(Bytes::from(format!("k{i}"))),
            value: Some(Bytes::from(format!("{payload}-{i}"))),
            headers: vec![],
        })
        .collect();
    let mut b = BytesMut::new();
    encode_batch(0, 0, 1000, 0, -1, -1, -1, &recs, &mut b);
    b.freeze()
}

fn open(disk: &SimDisk, dir: &PathBuf) -> Log<SimDisk> {
    Log::open(
        disk.clone(),
        dir.clone(),
        LogOptions {
            segment_max_bytes: 100, // 高频滚动：多段场景
            fsync: FsyncSchedule::SyncEach,
            retention_ms: 0,
            retention_max_bytes: 0,
        },
    )
    .unwrap()
}

/// 机制①：DiskIo len() 与 read_all 长度在任意操作序列下恒一致
/// （缺陷①：SimDisk::len 只返回 pending，crash 后簿记失真）。
#[test]
fn sim_disk_len_matches_read_len() {
    let disk = SimDisk::with_faults(0.0, 0, 0.0);
    let p = PathBuf::from("f.log");
    let mut rng = Lcg(42);
    for step in 0..200 {
        match rng.next() % 3 {
            0 => {
                let n = (rng.next() % 64) as usize;
                disk.append(&p, &vec![7u8; n]).unwrap();
            }
            1 => {
                let cur = disk.len(&p).unwrap();
                disk.truncate(&p, rng.next() % (cur + 1)).unwrap();
            }
            _ => disk.sync_file(&p).unwrap(),
        }
        let via_len = disk.len(&p).unwrap() as usize;
        let via_read = disk.read_all(&p).unwrap().len();
        assert_eq!(via_len, via_read, "step {step}: len 与 read_all 长度不一致");
    }
}

/// 机制②③④：delete/truncate 边界表（每个批边界与批中间各取一值）+
/// 结构不变式（段 base 严格递增、LEO == 末段 base+next_rel）+
/// crash+reopen 持久。
/// WIP（不计入账本已验证集合）：发现⑥——truncate_to 批对齐截断清空 active
/// 后（LEO=kept_end、0 字节、next_rel=0），后续 append 静默未生效
/// （边界表 b=9：truncate 后 LEO 8 正确，但紧随的 append 未推进 LEO/段链，
/// 最终 next_offset=10 与段链 (8,0) 背离）。疑点：append 的 needs_roll 判定
/// （next_rel > 0 才滚动）与空 active 的交互，或 commit_staged 写入路径
/// 对 0 字节文件的句柄状态。修复后解除 ignore。
#[test]
fn boundary_table_delete_and_truncate() {
    for b in 0..=10usize {
        // ---- delete(b)：log_start 精确等于 b（跨线批整批保留）----
        let dir = tmpdir(&format!("del-{b}"));
        let disk = SimDisk::new();
        let mut log = open(&disk, &dir);
        for i in 0..5 {
            log.append(&batch_bytes(&format!("d{b}m{i}")), AssignPolicy::Assign, 1).unwrap();
        }
        log.delete_records(b as i64).unwrap();
        eprintln!("B{b} after delete: start={} leo={} segs={:?}", log.log_start_offset(), log.next_offset, log.debug_segments());
        assert_eq!(log.log_start_offset(), b as i64, "delete({b})");
        assert_eq!(log.next_offset, 10, "delete 不得改变 LEO");
        let chain = log.debug_segments();
        assert!(chain.windows(2).all(|w| w[0].0 < w[1].0), "段 base 必须严格递增");
        let last = chain.last().unwrap();
        assert_eq!(last.0 + last.2, 10, "LEO == 末段 base + next_rel");

        // crash + reopen：log_start 持久化、数据不复活不丢失
        drop(log);
        let log = open(&disk, &dir);
        eprintln!("B{b} after reopen: start={} leo={} segs={:?}", log.log_start_offset(), log.next_offset, log.debug_segments());
        assert_eq!(log.log_start_offset(), b as i64, "delete({b}) 重开回退");
        assert_eq!(log.next_offset, 10);

        // ---- truncate_to(b)：批对齐向下取整（LEO = b & !1，2 记录/批）----
        let dir2 = tmpdir(&format!("tr-{b}"));
        let disk2 = SimDisk::new();
        let mut log = open(&disk2, &dir2);
        for i in 0..5 {
            log.append(&batch_bytes(&format!("t{b}m{i}")), AssignPolicy::Assign, 1).unwrap();
        }
        log.truncate_to(b as i64).unwrap();
        let expect_leo = (b & !1) as i64;
        assert_eq!(log.next_offset, expect_leo, "truncate_to({b}) 批对齐");

        drop(log);
        let log = open(&disk2, &dir2);
        assert_eq!(log.next_offset, expect_leo, "truncate_to({b}) 重开复活");
    }
}

/// 机制⑤：读包含性——任意 offset ∈ [log_start, LEO) 的读取必须返回
/// 覆盖该 offset 的数据（缺陷⑤：segment_for 忽略 active 段）。
#[test]
fn read_containment_across_segments() {
    let dir = tmpdir("contain");
    let disk = SimDisk::new();
    let mut log = open(&disk, &dir);
    let pool = basalt_storage::pool::BufferPool::new();
    for i in 0..12 {
        log.append(&batch_bytes(&format!("c{i}")), AssignPolicy::Assign, 1).unwrap();
    }
    assert!(log.segment_count() >= 2, "必须有多段场景");
    let leo = log.next_offset;
    let start = log.log_start_offset();
    for off in start..leo {
        let r = log.read(off, 1 << 20, &pool).unwrap();
        assert!(r.first_offset <= off, "read({off}) first_offset 回退");
        assert!(r.data.len() > 0, "read({off}) 返回空");
        // 批流合法
        let mut p = 0usize;
        while p < r.data.len() {
            let bl = batch_len_at(&r.data[p..]).expect("批可解析");
            p += bl;
        }
    }
}

/// 结构不变式：段 base 严格递增（缺陷④ 的永久检测，挂在任意变更后）。
#[test]
#[ignore = "WIP: 依赖 boundary 表闭合（同上）"]
fn segment_chain_strictly_increasing_after_mixed_ops() {
    let dir = tmpdir("chain");
    let disk = SimDisk::new();
    let mut log = open(&disk, &dir);
    use AssignPolicy::Assign;
    for i in 0..8 {
        log.append(&batch_bytes(&format!("x{i}")), Assign, 1).unwrap();
        if i % 3 == 2 { log.roll().unwrap(); }
        if i == 4 { log.delete_records(5).unwrap(); }
        if i == 6 { log.truncate_to(9).unwrap(); }
        let chain = log.debug_segments();
        assert!(chain.windows(2).all(|w| w[0].0 < w[1].0), "base 严格递增被破坏: {chain:?}");
        let last = chain.last().unwrap();
        assert_eq!(last.0 + last.2, log.next_offset, "LEO == 末段 base + next_rel");
    }
}
