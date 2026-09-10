//! Kani L1 harness：BatchHeader::parse 边界与畸形输入（账本 C13'/C10）。
//! 同源副本声明与 record_l1.rs 相同——真实现变更必须同步本文件。
//!
//! 运行：kani batch_header.rs

pub const RECORD_BATCH_HEADER_LEN: usize = 61;
pub const BATCH_LENGTH_OFFSET: usize = 12;
pub const MAGIC_V2: i8 = 2;

#[derive(Debug, Clone, PartialEq)]
pub struct BatchHeader {
    pub base_offset: i64,
    pub batch_length: i32,
    pub leader_epoch: i32,
    pub magic: i8,
    pub crc: u32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub first_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub record_count: i32,
}

/// 与 record/src/lib.rs BatchHeader::parse 逐行同源
pub fn parse(buf: &[u8]) -> Option<BatchHeader> {
    if buf.len() < RECORD_BATCH_HEADER_LEN {
        return None;
    }
    let g64 = |off: usize| -> i64 {
        let mut b = [0u8; 8];
        b.copy_from_slice(&buf[off..off + 8]);
        i64::from_be_bytes(b)
    };
    let g32 = |off: usize| -> i32 {
        let mut b = [0u8; 4];
        b.copy_from_slice(&buf[off..off + 4]);
        i32::from_be_bytes(b)
    };
    let g16 = |off: usize| -> i16 {
        let mut b = [0u8; 2];
        b.copy_from_slice(&buf[off..off + 2]);
        i16::from_be_bytes(b)
    };
    if buf[16] as i8 != MAGIC_V2 {
        return None;
    }
    Some(BatchHeader {
        base_offset: g64(0),
        batch_length: g32(8),
        leader_epoch: g32(12),
        magic: MAGIC_V2,
        crc: u32::from_be_bytes([buf[17], buf[18], buf[19], buf[20]]),
        attributes: g16(21),
        last_offset_delta: g32(23),
        first_timestamp: g64(27),
        max_timestamp: g64(35),
        producer_id: g64(43),
        producer_epoch: g16(51),
        base_sequence: g32(53),
        record_count: g32(57),
    })
}

/// 与 crate total_len 同源
pub fn total_len(batch_length: i32) -> usize {
    BATCH_LENGTH_OFFSET + batch_length.max(0) as usize
}

/// L1：任意输入 parse 不 panic（无越界/无算术 panic）
#[kani::proof]
fn parse_no_panic_any_input() {
    let buf: [u8; 96] = kani::any();
    let n: usize = kani::any();
    kani::assume(n <= 96);
    let _ = parse(&buf[..n]);
}

/// C13 定理的 Kani 侧：parse == Some ⇒ 头部约束成立（magic/magic 位置）
#[kani::proof]
fn parse_some_implies_magic_at_16() {
    let buf: [u8; 96] = kani::any();
    let n: usize = kani::any();
    kani::assume(n <= 96);
    if let Some(h) = parse(&buf[..n]) {
        assert!(h.magic == MAGIC_V2);
        assert!(buf[16] as i8 == MAGIC_V2);
    }
}

/// C13 定理：total_len 畸形域（batch_length < 49 ⇒ total < 61）
/// 该输入在 log.rs append 路径必须被拒绝（已由 log_test 回归锁定）。
#[kani::proof]
fn total_len_malformed_domain() {
    let batch_length: i32 = kani::any();
    if batch_length >= 0 && batch_length < 49 {
        assert!(total_len(batch_length) < RECORD_BATCH_HEADER_LEN);
    }
}
