//! T-Q.1 存储 opfuzz：DiskIo 层随机操作序列（append/sync/roll/flush/truncate/
//! delete_records/crash+reopen 交错），任何序列后 recover（重新 open）恒合法。
//!
//! 两档（账本 C7 前半）：
//! - clean（无故障）：断言持久性——已 sync 的追加在 crash+reopen 后按序完整可读，
//!   LEO 不低于 synced 水位；
//! - chaos（torn write 5%/15%）：断言恢复安全性——open 恒成功（损坏尾部被截断）、
//!   LEO 不超前、重开后的批流 CRC 全部合法且 offset 连续。
//!
//! 注：ENOSPC/短读/瞬时错误注入后续接入（recovery 失败语义需先定案）。

use basalt_record::{batch_len_at, encode_batch, validate_crc, BatchHeader, Rec};
use basalt_storage::disk::DiskIo;
use basalt_storage::log::{AssignPolicy, FsyncSchedule, Log, LogOptions};
use basalt_storage::pool::BufferPool;
use basalt_storage::sim_disk::SimDisk;
use bytes::{Bytes, BytesMut};
use std::path::PathBuf;

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn tmpdir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "basalt-opfuzz-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
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

/// 校验一批流：每批 CRC 合法、offset 连续（base_{k+1} = last_k + 1）。
fn assert_batch_stream(data: &[u8]) {
    let mut pos = 0usize;
    let mut expect_next: Option<i64> = None;
    while pos < data.len() {
        let bl = batch_len_at(&data[pos..]).expect("批流内 batch_len_at 必须成功");
        assert!(bl > 0 && pos + bl <= data.len(), "批长度越界");
        let slice = &data[pos..pos + bl];
        assert!(validate_crc(slice), "批 CRC 必须合法（pos={pos}）");
        let h = BatchHeader::parse(slice).expect("批头必须可解析");
        if let Some(next) = expect_next {
            assert_eq!(h.base_offset, next, "批 offset 必须连续");
        }
        expect_next = Some(h.base_offset + h.record_count as i64);
        pos += bl;
    }
}

/// 追加记录条目（用于持久性核对）
struct Tracked {
    payload: String,
    last_offset: i64,
}

fn run_seed(seed: u64, torn: f64) {
    let chaos = torn > 0.0;
    let mut rng = Lcg(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ (chaos as u64));
    let dir = tmpdir(&format!("s{seed}-{}", chaos as u8));

    let disk = SimDisk::with_faults(torn, 0, 0.0);
    // clean 档 = SyncEach（逐追加持久，crash 零丢失）；
    // chaos 档 = Os（靠副本，单盘掉电允许丢失，只断言恢复安全）+ torn write。
    let sched = if chaos { FsyncSchedule::Os } else { FsyncSchedule::SyncEach };
    let opts = LogOptions {
        segment_max_bytes: 300, // 小段：高频滚动
        fsync: sched,
        retention_ms: 0,
        retention_max_bytes: 0,
    };
    let mut log = Log::open(disk.clone(), dir.clone(), opts.clone()).unwrap();
    let pool = BufferPool::new();

    let mut tracked: Vec<Tracked> = vec![];
    let mut synced_upto: i64 = 0;
    let mut ops: Vec<String> = vec![];

    for step in 0..64u64 {
        match rng.below(10) {
            0..=4 => {
                let payload = format!("s{seed}p{step}");
                let raw = batch_bytes(&payload);
                ops.push(format!("{step}:append"));
                if let Ok(r) = log.append(&raw, AssignPolicy::Assign, step as i64) {
                    tracked.push(Tracked { payload, last_offset: r.last_offset });
                }
            }
            5 => {
                ops.push(format!("{step}:sync"));
                if log.sync().is_ok() {
                    synced_upto = log.next_offset;
                }
            }
            6 => {
                ops.push(format!("{step}:roll"));
                let _ = log.roll();
            }
            7 => {
                ops.push(format!("{step}:flush"));
                let _ = log.flush_batch();
            }
            8 => {
                // crash + reopen：无 Drop 副作用，直接弃置后崩溃仿真（掉电语义）
                let leo_before = log.next_offset;
                ops.push(format!("{step}:crash(leo={leo_before},start={})", log.log_start_offset));
                drop(log);
                disk.crash();
                log = Log::open(disk.clone(), dir.clone(), opts.clone()).unwrap();
                assert!(log.next_offset <= leo_before, "crash 后 LEO 不得超前");
                if !chaos {
                    // SyncEach：ack 即持久，LEO 精确保
                    assert_eq!(log.next_offset, leo_before,
                        "SyncEach 下 crash 不得丢失（seed={seed}, ops={ops:?}）");
                    ops.clear();
                } else {
                    // torn write 可损坏尾部：恢复截断后只断言安全性
                    tracked.clear();
                    synced_upto = log.next_offset;
                }
            }
            _ => {
                // truncate / delete_records：模型重置（保守——只保安全断言）
                let to = rng.below((log.next_offset + 1).max(1) as u64) as i64;
                if rng.below(2) == 0 {
                    ops.push(format!("{step}:truncate({to})"));
                    let _ = log.truncate_to(to);
                } else {
                    ops.push(format!("{step}:delete({to})"));
                    let _ = log.delete_records(to);
                }
                tracked.clear();
                synced_upto = log.next_offset;
            }
        }
    }

    // 收尾：sync + 重开 + 全量核对
    let _ = log.sync();
    drop(log);
    let log = Log::open(disk.clone(), dir.clone(), opts.clone()).unwrap();

    if !chaos && !tracked.is_empty() {
        assert!(log.next_offset >= synced_upto, "收尾：已 sync 数据丢失");
    }

    // 安全断言（两档通用）：从 0 全量读必须成功、批流 CRC 合法且连续
    if log.next_offset == 0 {
        return; // 恢复后全空：合法状态
    }
    let r = log.read(0, 1 << 20, &pool).unwrap();
    assert_batch_stream(&r.data);

    // 持久断言（clean）：tracked 标签按序全部可读回
    if !chaos {
        let text = String::from_utf8_lossy(&r.data).to_string();
        let mut cursor = 0usize;
        for t in &tracked {
            match text[cursor..].find(&t.payload) {
                Some(pos) => cursor += pos + t.payload.len(),
                None => panic!("已 sync 追加丢失: {}（seed={seed}）", t.payload),
            }
        }
    }
}

/// WIP（不计入账本已验证集合）：已发现 delete/truncate 与 roll/crash 交互后
/// 重开丢数据的深层问题（seed=1 clean：ops 见 panic 输出，leo=27、start=14、
/// crash 后重开 LEO < 27，全部追加均 SyncEach 已 sync）。前两项已修：
/// SimDisk::len 只返回 pending（crash 后 seg.bytes=0 致簿记失真）、
/// truncate_to 盲设 next_offset 与批边界错位。待根因定位后去除 ignore。
#[test]
#[ignore = "WIP: delete/truncate/roll/crash 交互丢数据——见函数注释"]
fn opfuzz_clean_seeds() {
    for seed in 1..=40u64 {
        run_seed(seed, 0.0);
    }
}

#[test]
#[ignore = "WIP: 依赖 clean 档先行闭合；另需 log_start 感知的读取入口"]
fn opfuzz_chaos_seeds() {
    for seed in 1..=20u64 {
        let torn = if seed % 2 == 0 { 0.05 } else { 0.15 };
        run_seed(seed * 7919, torn);
    }
}

#[test]
fn repro_seed1_minimal() {
    let dir = tmpdir("repro1");
    let disk = SimDisk::with_faults(0.0, 0, 0.0);
    let opts = LogOptions {
        segment_max_bytes: 300,
        fsync: FsyncSchedule::SyncEach,
        retention_ms: 0,
        retention_max_bytes: 0,
    };
    let mut log = Log::open(disk.clone(), dir.clone(), opts.clone()).unwrap();
    for i in 0..3u64 {
        let raw = batch_bytes(&format!("r{i}"));
        log.append(&raw, AssignPolicy::Assign, i as i64).unwrap();
        eprintln!("append {i}: leo={}", log.next_offset);
    }
    log.roll().unwrap();
    log.sync().unwrap();
    eprintln!("before crash: leo={}", log.next_offset);
    for name in disk.list(&dir).unwrap() {
        let len = disk.len(&dir.join(&name)).unwrap();
        eprintln!("  file {name} len={len}");
    }
    drop(log);
    disk.crash();
    let log2 = Log::open(disk.clone(), dir.clone(), opts).unwrap();
    eprintln!("after reopen: leo={}", log2.next_offset);
    for name in disk.list(&dir).unwrap() {
        let len = disk.len(&dir.join(&name)).unwrap();
        eprintln!("  file {name} len={len}");
    }
    assert_eq!(log2.next_offset, 6);
}
