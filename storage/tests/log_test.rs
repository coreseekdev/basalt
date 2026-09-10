//! Log 级集成测试：append → read → roll → recover 往返。

use basalt_record::{encode_batch, Rec};
use basalt_storage::disk::StdDisk;
use basalt_storage::log::{AssignPolicy, FsyncSchedule, Log, LogOptions};
use basalt_storage::pool::BufferPool;
use bytes::{Bytes, BytesMut};

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("basalt-log-{tag}-{}-{}", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn batch_bytes(base: i64, count: usize, payload: &str) -> Bytes {
    let recs: Vec<Rec> = (0..count)
        .map(|i| Rec {
            timestamp_delta: i as i64,
            key: Some(Bytes::from(format!("k{i}"))),
            value: Some(Bytes::from(format!("{payload}-{i}"))),
            headers: vec![],
        })
        .collect();
    let mut b = BytesMut::new();
    encode_batch(base, 0, 1000, 0, -1, -1, -1, &recs, &mut b);
    b.freeze()
}

#[test]
fn append_read_roll_recover() {
    let dir = tmpdir("full");
    let disk = StdDisk::new();
    let mut log = Log::open(
        disk,
        dir.clone(),
        LogOptions { segment_max_bytes: 512, fsync: FsyncSchedule::OnRoll, retention_ms: 0, retention_max_bytes: 0 },
    )
    .unwrap();
    let pool = BufferPool::new();

    // 追加 6 批（小段滚动）
    for i in 0..6u64 {
        let raw = batch_bytes(0, 2, &format!("m{i}"));
        let r = log.append(&raw, AssignPolicy::Assign, 1234).unwrap();
        assert_eq!(r.base_offset, (i * 2) as i64);
        assert_eq!(r.last_offset, (i * 2 + 1) as i64);
    }
    assert_eq!(log.next_offset, 12);
    assert!(log.segment_count() > 1, "segments should have rolled");

    // 从头读
    let r = log.read(0, 1 << 20, &pool).unwrap();
    assert_eq!(r.first_offset, 0);
    assert_eq!(r.data.len() > 0, true);
    assert_eq!(r.high_watermark, 12);

    // 从中段读（整批粒度，first_offset <= 请求 offset）
    let r5 = log.read(5, 1 << 20, &pool).unwrap();
    assert!(r5.first_offset <= 5);

    // 越界
    assert!(log.read(99, 1024, &pool).is_err());

    // 恢复：同目录重开，数据一致
    let next = log.next_offset;
    drop(log);
    let disk2 = StdDisk::new();
    let log2 = Log::open(disk2, dir.clone(), LogOptions::default()).unwrap();
    assert_eq!(log2.next_offset, next);
    let r2 = log2.read(0, 1 << 20, &pool).unwrap();
    assert_eq!(r2.data, r.data);

    // 续写接续 offset
    let mut log2 = log2;
    let raw = batch_bytes(0, 3, "after-recover");
    let r = log2.append(&raw, AssignPolicy::Assign, 5678).unwrap();
    assert_eq!(r.base_offset, 12);
    assert_eq!(log2.next_offset, 15);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn corrupt_tail_truncated_on_recover() {
    let dir = tmpdir("corrupt");
    let disk = StdDisk::new();
    let mut log = Log::open(disk, dir.clone(), LogOptions::default()).unwrap();
    for i in 0..3 {
        let raw = batch_bytes(0, 2, &format!("c{i}"));
        log.append(&raw, AssignPolicy::Assign, 1).unwrap();
    }
    let log_size = log_segment0_size(&dir);
    drop(log);

    // 模拟 torn write：文件尾追加垃圾
    let path = dir.join("00000000000000000000.log");
    let mut f = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
    use std::io::Write;
    f.write_all(&[0xde, 0xad, 0xbe, 0xef, 0x00, 0x01, 0x02, 0x03]).unwrap();
    drop(f);

    let disk2 = StdDisk::new();
    let log2 = Log::open(disk2, dir.clone(), LogOptions::default()).unwrap();
    assert_eq!(log2.next_offset, 6);
    assert_eq!(log_segment0_size(&dir), log_size, "corrupt tail must be truncated back");

    let _ = std::fs::remove_dir_all(&dir);
}

fn log_segment0_size(dir: &std::path::Path) -> u64 {
    std::fs::metadata(dir.join("00000000000000000000.log")).unwrap().len()
}

#[test]
fn absolute_policy_for_replication() {
    let dir = tmpdir("abs");
    let mut log = Log::open(StdDisk::new(), dir.clone(), LogOptions::default()).unwrap();
    let raw = batch_bytes(5, 2, "leader-assigned");
    // 期望 base=0，但批携带 5 → gap 错误
    assert!(log.append(&raw, AssignPolicy::Absolute, 0).is_err());
    // Assign 写入后，绝对值连续的批可以继续
    log.append(&batch_bytes(0, 2, "a"), AssignPolicy::Assign, 0).unwrap();
    let raw2 = batch_bytes(2, 2, "b");
    log.append(&raw2, AssignPolicy::Absolute, 0).unwrap();
    assert_eq!(log.next_offset, 4);
    let _ = std::fs::remove_dir_all(&dir);
}

/// P0 探针回归（review-storage-c7-20260909）：delete_records 保留 >= offset
/// 数据、跨 crash+reopen 与 log_start 持久化。
#[test]
fn delete_records_keeps_suffix_across_reopen() {
    let dir = tmpdir("c7-delete");
    let disk = StdDisk::new();
    let mut log = Log::open(
        disk,
        dir.clone(),
        LogOptions { segment_max_bytes: 4096, fsync: FsyncSchedule::SyncEach, retention_ms: 0, retention_max_bytes: 0 },
    )
    .unwrap();
    let pool = BufferPool::new();
    for i in 0..5u64 {
        let raw = batch_bytes(0, 2, &format!("m{i}"));
        log.append(&raw, AssignPolicy::Assign, 1000).unwrap();
    }
    assert_eq!(log.next_offset, 10);

    log.delete_records(9).unwrap();
    assert_eq!(log.log_start_offset(), 9);
    // 删除后 9..10 仍可读
    let r = log.read(9, 1 << 20, &pool).unwrap();
    assert!(r.data.len() > 0);

    // crash + reopen（SyncEach：ack 即持久；log_start 不得回退）
    drop(log);
    let mut log = Log::open(
        StdDisk::new(),
        dir.clone(),
        LogOptions { segment_max_bytes: 4096, fsync: FsyncSchedule::SyncEach, retention_ms: 0, retention_max_bytes: 0 },
    )
    .unwrap();
    assert_eq!(log.log_start_offset(), 9, "log_start 必须持久化");
    assert_eq!(log.next_offset, 10, ">= offset 的数据不得丢失");
    let r = log.read(9, 1 << 20, &pool).unwrap();
    assert!(r.data.len() > 0);
}

/// P0 探针回归（review-storage-c7-20260909 P0-2）：truncate_to 落在 sealed 区——
/// 批对齐向下取整、包含 offset 的段提升为 active、重开不复活。
#[test]
fn truncate_to_sealed_region_batch_aligned() {
    let dir = tmpdir("c7-trunc");
    let disk = StdDisk::new();
    let mut log = Log::open(
        disk,
        dir.clone(),
        LogOptions { segment_max_bytes: 4096, fsync: FsyncSchedule::SyncEach, retention_ms: 0, retention_max_bytes: 0 },
    )
    .unwrap();
    for i in 0..5u64 {
        let raw = batch_bytes(0, 2, &format!("m{i}"));
        log.append(&raw, AssignPolicy::Assign, 1000).unwrap();
    }
    assert_eq!(log.next_offset, 10);
    // roll：batch[0..2] sealed，active base=6
    log.roll().unwrap();
    assert_eq!(log.next_offset, 10);

    // offset=3 落在 sealed [0,6) 内：批对齐向下取整 → LEO=2
    log.truncate_to(3).unwrap();
    assert_eq!(log.next_offset, 2, "批对齐：LEO 向下取整到批边界");

    // crash + reopen：不得复活
    drop(log);
    let log = Log::open(
        StdDisk::new(),
        dir.clone(),
        LogOptions { segment_max_bytes: 4096, fsync: FsyncSchedule::SyncEach, retention_ms: 0, retention_max_bytes: 0 },
    )
    .unwrap();
    assert_eq!(log.next_offset, 2, "截断不得在重开后复活");
    let pool = BufferPool::new();
    let r = log.read(0, 1 << 20, &pool).unwrap();
    assert_eq!(r.first_offset, 0);
    assert!(r.data.len() > 0 && r.data.len() < 4096);
}

/// P0-1 回归（code review 二轮）：append 路径的畸形批（合法 magic +
/// batch_length < 49）必须返回 Err 而非 panic——此前 &rest[21..total]
/// 直接切片（C13 同族，但位于 append 路径，validate_crc 防线不覆盖）。
#[test]
fn append_rejects_malformed_batch_length() {
    let dir = tmpdir("c7-append-malformed");
    let disk = StdDisk::new();
    let mut log = Log::open(
        disk,
        dir.clone(),
        LogOptions { segment_max_bytes: 4096, fsync: FsyncSchedule::SyncEach, retention_ms: 0, retention_max_bytes: 0 },
    )
    .unwrap();

    // 构造：合法头（magic=2）+ batch_length=0 的 61B 载荷
    let mut malformed = vec![0u8; 61];
    malformed[16] = 2; // magic v2
    malformed[8..12].copy_from_slice(&0i32.to_be_bytes()); // batch_length = 0
    let raw = Bytes::from(malformed);

    let r = log.append(&raw, AssignPolicy::Assign, 1000);
    assert!(r.is_err(), "畸形批必须被拒绝");

    // 略大但仍不足 49 的：batch_length = 30 → total = 42 ∈ [21,61)
    let mut malformed2 = vec![0u8; 80];
    malformed2[16] = 2;
    malformed2[8..12].copy_from_slice(&30i32.to_be_bytes());
    let r2 = log.append(&Bytes::from(malformed2), AssignPolicy::Assign, 1001);
    assert!(r2.is_err(), "total ∈ [21,61) 的畸形批必须被拒绝");

    // 正常批仍可写
    let raw = batch_bytes(0, 2, "ok");
    log.append(&raw, AssignPolicy::Assign, 1002).unwrap();
}
