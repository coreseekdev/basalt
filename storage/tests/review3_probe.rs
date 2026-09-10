//! 第三轮评审临时探针（评审结束后删除）。
use basalt_record::{encode_batch, Rec};
use basalt_storage::log::{AssignPolicy, FsyncSchedule, Log, LogOptions};
use basalt_storage::pool::BufferPool;
use basalt_storage::sim_disk::SimDisk;
use bytes::{Bytes, BytesMut};
use std::path::PathBuf;

fn tmpdir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "basalt-review3-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn batch_bytes(payload: &str, value_size: usize, count: i64) -> Bytes {
    let recs: Vec<Rec> = (0..count)
        .map(|i| Rec {
            timestamp_delta: i,
            key: Some(Bytes::from(format!("k{i}"))),
            value: Some(Bytes::from(format!("{payload}-{i}-").repeat(value_size / 8))),
            headers: vec![],
        })
        .collect();
    let mut b = BytesMut::new();
    encode_batch(0, 0, 1000, 0, -1, -1, -1, &recs, &mut b);
    b.freeze()
}

fn open(dir: &PathBuf, disk: &SimDisk) -> Log<SimDisk> {
    Log::open(
        disk.clone(),
        dir.clone(),
        LogOptions {
            segment_max_bytes: 64 * 1024 * 1024,
            fsync: FsyncSchedule::SyncEach,
            retention_ms: 0,
            retention_max_bytes: 0,
        },
    )
    .unwrap()
}

/// 探针1：首批超过读窗口（max_bytes + 4096 + 61）时 read 是否返回空。
#[test]
fn probe_read_ex_oversized_first_batch() {
    let dir = tmpdir("bigfirst");
    let disk = SimDisk::new();
    let mut log = open(&dir, &disk);
    let pool = BufferPool::new();

    let big = batch_bytes("big", 4096, 8); // 单批约 33KB+
    println!("batch len = {}", big.len());
    let r = log.append(&big, AssignPolicy::Assign, 1).unwrap();
    let leo = log.next_offset;
    println!("leo = {leo}, append last = {}", r.last_offset);

    // max_bytes=1024 → 窗口 = 1024+4096+61 = 5181 < 批大小
    let out = log.read(0, 1024, &pool).unwrap();
    println!(
        "read(0, 1024): data.len={} first_offset={} hw={}",
        out.data.len(),
        out.first_offset,
        out.high_watermark
    );
    assert!(
        !out.data.is_empty(),
        "REGRESSION: 首批超窗返回空（Kafka 语义：首批必须整批带上） leo={leo}"
    );

    // from_offset 落在该批内（批中间 offset）
    let out2 = log.read(3, 1024, &pool).unwrap();
    println!(
        "read(3, 1024): data.len={} first_offset={}",
        out2.data.len(),
        out2.first_offset
    );
    assert!(!out2.data.is_empty(), "REGRESSION: 批内 offset 超窗返回空");

    // 第二批紧跟其后，从第二批读（start_pos > 0，命中跳批/HW 边界场景检查）
    let small = batch_bytes("small", 64, 2);
    log.append(&small, AssignPolicy::Assign, 2).unwrap();
    let out3 = log.read(8, 1024, &pool).unwrap();
    println!(
        "read(8, 1024): data.len={} first_offset={}",
        out3.data.len(),
        out3.first_offset
    );
    assert!(!out3.data.is_empty(), "REGRESSION: 第二批读取返回空");
}

/// 探针2：truncate_to 边界（批边界/段边界/批中间）+ promotion 后读包含性。
#[test]
fn probe_truncate_boundaries_and_promotion() {
    let dir = tmpdir("trunc");
    let disk = SimDisk::new();
    let mut log = open(&dir, &disk);
    let pool = BufferPool::new();
    // 小段强制滚动，制造多段
    let mut log2 = Log::open(
        disk.clone(),
        dir.clone(),
        LogOptions {
            segment_max_bytes: 200,
            fsync: FsyncSchedule::SyncEach,
            retention_ms: 0,
            retention_max_bytes: 0,
        },
    )
    .unwrap();
    for i in 0..10 {
        log2
            .append(&batch_bytes(&format!("t{i}"), 32, 2), AssignPolicy::Assign, i)
            .unwrap();
    }
    println!("segs = {:?} leo = {}", log2.debug_segments(), log2.next_offset);
    let leo = log2.next_offset;

    // 批中间 truncate（offset=13，批为 2 记录 → kept_end=12）
    log2.truncate_to(13).unwrap();
    assert_eq!(log2.next_offset, 12, "批对齐向下取整");
    // 全范围读包含性
    let mut off = log2.log_start_offset();
    let mut it = 0;
    while off < log2.next_offset && it < 50 {
        let r = log2.read(off, 1024, &pool).unwrap();
        if r.data.is_empty() {
            panic!("read({off}) 空——截断后读包含性破坏 segs={:?}", log2.debug_segments());
        }
        assert!(r.first_offset <= off);
        // 推进
        let mut p = 0usize;
        let mut cnt = 0i64;
        while p < r.data.len() {
            let h = basalt_record::BatchHeader::parse(&r.data[p..]).unwrap();
            cnt += h.record_count as i64;
            p += h.total_len();
        }
        off = r.first_offset + cnt;
        it += 1;
    }
    assert_eq!(off, log2.next_offset, "读覆盖到 LEO");

    // promotion 路径：truncate 回到早期段内
    let segs = log2.debug_segments();
    println!("before promotion: {:?} leo={}", segs, log2.next_offset);
    let target = segs[1].0 + 1; // 第二段内某 offset
    log2.truncate_to(target).unwrap();
    println!(
        "after truncate_to({target}): leo={} segs={:?} start={}",
        log2.next_offset,
        log2.debug_segments(),
        log2.log_start_offset()
    );
    // promotion 后紧接 append，验证簿记一致
    let r = log2
        .append(&batch_bytes("post", 32, 2), AssignPolicy::Assign, 99)
        .unwrap();
    println!("post-append: base={} last={} leo={}", r.base_offset, r.last_offset, log2.next_offset);
    assert_eq!(log2.next_offset, r.last_offset + 1, "append 后 LEO 一致");
    let chain = log2.debug_segments();
    let last = chain.last().unwrap();
    assert_eq!(last.0 + last.2, log2.next_offset, "LEO == 末段 base+next_rel");

    // crash + reopen 不复活
    let leo_before = log2.next_offset;
    drop(log2);
    let log3 = open(&dir, &disk);
    assert_eq!(
        log3.next_offset, leo_before,
        "重开 LEO 复活: {} -> {}",
        leo_before, log3.next_offset
    );
    let _ = leo;
}
