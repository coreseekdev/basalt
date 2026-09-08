//! SimDisk 故障注入测试（ADR-7 核心验证：ack 前必须 sync）

use basalt_record::{encode_batch, Rec};
use basalt_storage::disk::{DiskIo, StdDisk};
use basalt_storage::log::{AssignPolicy, FsyncSchedule, Log, LogOptions};
use basalt_storage::pool::BufferPool;
use basalt_storage::sim_disk::SimDisk;
use bytes::{Bytes, BytesMut};

fn batch_bytes(base: i64, count: usize, tag: &str) -> Bytes {
    let recs: Vec<Rec> = (0..count)
        .map(|i| Rec {
            timestamp_delta: i as i64,
            key: Some(Bytes::from(format!("k{i}"))),
            value: Some(Bytes::from(format!("{tag}-{i}"))),
            headers: vec![],
        })
        .collect();
    let mut b = BytesMut::new();
    encode_batch(base, 0, 1000, 0, -1, -1, -1, &recs, &mut b);
    b.freeze()
}

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("basalt-sim-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// 正常路径：SimDisk 上写入 + 读回。
#[test]
fn sim_normal_write_read() {
    let dir = tmpdir("normal");
    let disk = SimDisk::new();
    let mut log = Log::open(disk, dir.clone(), LogOptions::default()).unwrap();
    let raw = batch_bytes(0, 5, "test");
    log.append(&raw, AssignPolicy::Assign, 1000).unwrap();
    assert_eq!(log.next_offset, 5);
    let pool = BufferPool::new();
    let r = log.read(0, 1 << 20, &pool).unwrap();
    assert_eq!(r.data.len() > 0, true);
    assert_eq!(r.high_watermark, 5);
    let _ = std::fs::remove_dir_all(&dir);
}

/// SyncEach 模式：sync 后数据在 committed 中（crash 安全）。
#[test]
fn sim_sync_each_crash_safe() {
    let dir = tmpdir("sync-each");
    let disk = SimDisk::new();
    let mut log = Log::open(
        disk.clone(),
        dir.clone(),
        LogOptions { segment_max_bytes: 1 << 30, fsync: FsyncSchedule::SyncEach, retention_ms: 0, retention_max_bytes: 0 },
    ).unwrap();
    for i in 0..3 {
        let raw = batch_bytes(0, 2, &format!("s{i}"));
        log.append(&raw, AssignPolicy::Assign, 1).unwrap();
    }
    // committed 中有全部数据（SyncEach 每次 append 后 sync）
    let hex_log = format!("{}/00000000000000000000.log", dir.display());
    let committed = disk.committed_data(std::path::Path::new(&hex_log));
    assert!(committed.is_some());
    assert!(committed.unwrap().len() > 0);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Os 模式（不 sync）：crash 后 pending 数据丢失。
#[test]
fn sim_os_mode_crash_loses_pending() {
    let dir = tmpdir("os-crash");
    let disk = SimDisk::new();
    let mut log = Log::open(
        disk.clone(),
        dir.clone(),
        LogOptions { segment_max_bytes: 1 << 30, fsync: FsyncSchedule::Os, retention_ms: 0, retention_max_bytes: 0 },
    ).unwrap();
    let raw = batch_bytes(0, 3, "data");
    log.append(&raw, AssignPolicy::Assign, 1).unwrap();
    // Os 模式不 sync → 数据在 pending 中
    // 模拟 crash
    disk.crash();
    // committed 数据为空（未 sync 过）
    let path = dir.join("00000000000000000000.log");
    let committed = disk.committed_data(&path).unwrap_or_default();
    assert_eq!(committed.len(), 0, "Os mode: unsynced data lost after crash");
    let _ = std::fs::remove_dir_all(&dir);
}

/// ENOSPC：磁盘满后 append 返回错误，log 状态不损坏。
#[test]
fn sim_enospc_graceful() {
    let dir = tmpdir("enospc");
    let disk = SimDisk::with_faults(0.0, 2, 0.0); // 第 3 次 write 后 ENOSPC
    let mut log = Log::open(disk, dir.clone(), LogOptions::default()).unwrap();
    // 第 1、2 次 append 成功（write_count 0→2）
    let raw = batch_bytes(0, 2, "ok1");
    log.append(&raw, AssignPolicy::Assign, 1).unwrap();
    let raw = batch_bytes(0, 2, "ok2");
    log.append(&raw, AssignPolicy::Assign, 1).unwrap();
    // 第 3 次 append（write_count 2→3 ≥ enospc_after=3）→ ENOSPC
    let raw = batch_bytes(0, 2, "fail");
    let result = log.append(&raw, AssignPolicy::Assign, 1);
    assert!(result.is_err(), "expected ENOSPC error");
    // log 状态不损坏：next_offset 不变
    assert_eq!(log.next_offset, 4, "next_offset must not advance on ENOSPC");
    let _ = std::fs::remove_dir_all(&dir);
}
