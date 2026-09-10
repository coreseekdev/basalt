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

// ==================== 四轮 code review（docs/review-adr14-pool-20260910.md）回归 ====================

use basalt_storage::log::ReadCap;
use basalt_storage::sim_disk::SimDisk;

/// P1-1：read_ex 首批批体越过读窗口时曾无条件 break——补读结果从不参与
/// 解析，返回空数据 + HW 前探。小 max_bytes 消费者拉到大批分区永久活锁。
#[test]
fn read_ex_large_batch_small_max_bytes_returns_batch() {
    let dir = tmpdir("readex-bigbatch");
    let disk = StdDisk::new();
    let mut log = Log::open(
        disk,
        dir.clone(),
        LogOptions { segment_max_bytes: 1 << 30, fsync: FsyncSchedule::OnRoll, retention_ms: 0, retention_max_bytes: 0 },
    )
    .unwrap();
    let pool = BufferPool::new();

    // 单批 ~30KB（1 条记录），窗口 want = 1024 + 索引间隔 + 批头 < 批长
    let raw = batch_bytes(0, 1, &"x".repeat(30 * 1024));
    let r = log.append(&raw, AssignPolicy::Assign, 1234).unwrap();
    assert_eq!(r.last_offset, 0);
    log.sync().unwrap();

    let rr = log.read_ex(0, 1024, &pool, ReadCap::LogEnd).unwrap();
    assert!(
        rr.data.len() >= raw.len(),
        "大批 + 小 max_bytes 必须整批返回（防消费活锁），实得 {}B / 批 {}B",
        rr.data.len(),
        raw.len()
    );
    assert_eq!(rr.first_offset, 0);
    let _ = std::fs::remove_dir_all(&dir);
}

/// P1-2（append 错误路径）：batch_io 窗口内 append 失败（roll → flush 失败）
/// 时，失败 append 自身的字节不得残留 staging（否则随窗口收口落盘=僵尸），
/// 而**先于失败的 staged 批必须保留**（它们仍属未结算 produce——清空会把
/// ack 边界搞错）。回滚按检查点的 staged_len 截除。
#[test]
fn append_failure_clears_staging_no_zombie() {
    let dir = tmpdir("appendfail-staging");
    let disk = SimDisk::new();
    let mut log = Log::open(
        disk.clone(),
        dir.clone(),
        LogOptions { segment_max_bytes: 500, fsync: FsyncSchedule::SyncEach, retention_ms: 0, retention_max_bytes: 0 },
    )
    .unwrap();
    let pool = BufferPool::new();
    log.batch_io = true;

    // ① produce X：进 staging（batch_io fast path 收编，未写盘）
    let x = batch_bytes(0, 2, "X");
    log.append(&x, AssignPolicy::Assign, 1000).unwrap();
    // ② 注入瞬时故障：produce A（大批，触发 roll → flush_batch 失败 →
    //    rollback_to）。A 报错；其前成功但未落盘的 X 不得残留在 staging
    disk.set_fail_writes(true);
    let a = batch_bytes(0, 1, &"A".repeat(600));
    assert!(log.append(&a, AssignPolicy::Assign, 1001).is_err(), "注入点：A 必须失败");
    // ③ 故障解除：produce B 复用回卷后的 offset 区间，成功
    disk.set_fail_writes(false);
    let b = batch_bytes(0, 2, "B");
    let rb = log.append(&b, AssignPolicy::Assign, 1002).unwrap();
    assert_eq!(rb.base_offset, 2, "B 接在 X 之后（A 的分配已回卷）");
    assert_eq!(rb.last_offset, 3);
    // ④ 窗口收口后：X（先于失败、仍在窗口内）与 B 都必须真实落盘——
    //    回滚只截除失败 append 自身的字节，不得波及更早窗口数据
    log.end_batch_window().unwrap();

    let rr = log.read_ex(0, 1 << 20, &pool, ReadCap::LogEnd).unwrap();
    let text = String::from_utf8_lossy(&rr.data).into_owned();
    assert!(!text.contains("A-"), "失败批（错误应答）数据不得落盘：{text:?}");
    assert!(text.contains("X-"), "先于失败的 staged 批 X 不得被回滚波及：{text:?}");
    assert!(text.contains("B-"), "成功批数据必须在：{text:?}");
    // offset 流不得回卷：X(0..1) + B(2..3)
    assert_eq!(rr.first_offset, 0);

    // crash + reopen：数据不复活不丢失
    drop(log);
    let log2 = Log::open(disk.clone(), dir.clone(), LogOptions::default()).unwrap();
    assert_eq!(log2.next_offset, 4, "重开 LEO == B 末尾");
    let _ = std::fs::remove_dir_all(&dir);
}

/// P1-2（flush 错误路径）：flush_batch 写失败曾保留 staging，下一次 flush
/// 把[失败批][新批]一并落盘。错误结算的窗口数据必须随失败丢弃。
#[test]
fn flush_failure_drops_staged_bytes() {
    let dir = tmpdir("flushfail-staging");
    let disk = SimDisk::new();
    let mut log = Log::open(
        disk.clone(),
        dir.clone(),
        LogOptions { segment_max_bytes: 1 << 30, fsync: FsyncSchedule::SyncEach, retention_ms: 0, retention_max_bytes: 0 },
    )
    .unwrap();
    log.batch_io = true;

    // A 进 staging（尚未写盘）
    let a = batch_bytes(0, 2, "A");
    log.append(&a, AssignPolicy::Assign, 1000).unwrap();
    // flush 失败（actor 层会把窗口内 produce 全部按错误结算）
    disk.set_fail_writes(true);
    assert!(log.flush_batch().is_err(), "注入点：flush 必须失败");
    disk.set_fail_writes(false);
    // 恢复后再次 flush：不得把失败批字节带出
    log.flush_batch().unwrap();
    let files = std::fs::read_dir(&dir).unwrap();
    let _ = files; // 内容断言经 SimDisk committed 数据
    let seg_path = dir.join("00000000000000000000.log");
    let committed = disk.committed_data(&seg_path).unwrap_or_default();
    let text = String::from_utf8_lossy(&committed).into_owned();
    assert!(!text.contains("A-"), "flush 失败的 staged 字节不得随后续 flush 落盘：{text:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// P1-3：batch_io 窗口内 truncate_to 曾无条件 clear staging——完整位于
/// offset 之下的 staged 批从未写盘却被按"保留"结算。截断前必须先收口窗口。
#[test]
fn truncate_to_flushes_window_first_keeps_staged_below_offset() {
    let dir = tmpdir("trunc-staged");
    let disk = SimDisk::new();
    let mut log = Log::open(
        disk.clone(),
        dir.clone(),
        LogOptions { segment_max_bytes: 1 << 30, fsync: FsyncSchedule::SyncEach, retention_ms: 0, retention_max_bytes: 0 },
    )
    .unwrap();
    let pool = BufferPool::new();
    log.batch_io = true;

    // A(0..1) + B(2..3) 全部在 staging（未写盘）
    let a = batch_bytes(0, 2, "A");
    log.append(&a, AssignPolicy::Assign, 1000).unwrap();
    let b = batch_bytes(0, 2, "B");
    log.append(&b, AssignPolicy::Assign, 1001).unwrap();
    assert_eq!(log.next_offset, 4);

    // 截断到 3：A(0..1) 完整位于截断点之下，必须真实保留；
    // B(2..3) 跨线（end=4 > 3），按整批粒度截掉 → LEO = A 末尾 2（批对齐）
    log.truncate_to(3).unwrap();
    assert_eq!(log.next_offset, 2, "B（跨线批）必须被截掉，LEO 批对齐到 A 末尾");

    let rr = log.read_ex(0, 1 << 20, &pool, ReadCap::LogEnd).unwrap();
    let text = String::from_utf8_lossy(&rr.data).into_owned();
    assert!(text.contains("A-"), "低于截断点的 staged 批必须真实落盘并可读：{text:?}");
    assert!(!text.contains("B-"), "截断点之上的批不得残留：{text:?}");
    assert_eq!(rr.high_watermark, 2);

    // crash + reopen：A 不复活也不丢失
    drop(log);
    let log2 = Log::open(disk.clone(), dir.clone(), LogOptions::default()).unwrap();
    assert_eq!(log2.next_offset, 2, "重开 LEO == A 末尾");
    let rr2 = log2.read_ex(0, 1 << 20, &pool, ReadCap::LogEnd).unwrap();
    let text2 = String::from_utf8_lossy(&rr2.data).into_owned();
    assert!(text2.contains("A-") && !text2.contains("B-"));
    let _ = std::fs::remove_dir_all(&dir);
}
