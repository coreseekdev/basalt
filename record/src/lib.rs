//! RecordBatch v2 编解码（TASK.md T-M0.1，ADR-4：仅 magic v2；ADR-9：压缩 none/lz4/zstd/gzip）。
//!
//! 线格式（Kafka `DefaultRecordBatch`，全大端）：
//! ```text
//! baseOffset:i64 | batchLength:i32 | partitionLeaderEpoch:i32 | magic:i8
//! crc:u32        | attributes:u16   | lastOffsetDelta:i32       | firstTimestamp:i64
//! maxTimestamp:i64 | producerId:i64 | producerEpoch:i16         | baseSequence:i32
//! recordsCount:i32 | records...
//! ```
//! - CRC32C 覆盖从 attributes 起到批尾（不含 baseOffset/batchLength/leaderEpoch/magic/crc 自身），
//!   因此 broker 改写 baseOffset 不需要重算 CRC。
//! - 记录内 varint 为 zigzag（与协议帧的无符号 varint 不同源）。
//! - attributes 压缩位 0-2：0 none / 1 gzip / 2 snappy / 3 lz4 / 4 zstd；
//!   bit3 timestampType、bit4 transaction、bit5 control。

use bytes::{BufMut, Bytes, BytesMut};

pub const MAGIC_V2: i8 = 2;

/// attributes bit4：事务批（ADR-18）。
pub const ATTR_TRANSACTIONAL: i16 = 0b0001_0000;
/// attributes bit5：控制批（txn marker 等，应用永不可见）。
pub const ATTR_CONTROL: i16 = 0b0010_0000;

/// 控制记录类型（EndTransactionMarker，KIP-98）。
/// wire：record key = [version:i16=0][type:i16]（key 非空正是控制记录的
/// 判别特征——java 事务消费者从 key 解析本类型），value 空/不透明。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum ControlRecordType {
    Abort = 0,
    Commit = 1,
}

impl ControlRecordType {
    pub fn from_i16(v: i16) -> Option<ControlRecordType> {
        match v {
            0 => Some(ControlRecordType::Abort),
            1 => Some(ControlRecordType::Commit),
            _ => None,
        }
    }

    fn key(&self) -> Bytes {
        let mut k = BytesMut::with_capacity(4);
        k.put_i16(0); // version
        k.put_i16(*self as i16);
        k.freeze()
    }
}

/// 批头（到 recordsCount 为止）的固定长度。
pub const RECORD_BATCH_HEADER_LEN: usize = 61;
/// CRC 覆盖起点相对批头的偏移（attributes 起，= magic 16 + crc 4 + 1）。
pub const CRC_PAYLOAD_OFFSET: usize = 21;
/// batchLength 的度量起点：Kafka 语义 total = LOG_OVERHEAD(12) + batchLength
/// （即 batchLength 含 partitionLeaderEpoch..records，不含 baseOffset 与自身）。
pub const BATCH_LENGTH_OFFSET: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum Compression {
    None = 0,
    Gzip = 1,
    Snappy = 2,
    Lz4 = 3,
    Zstd = 4,
}

impl Compression {
    pub fn from_bits(bits: i16) -> Option<Compression> {
        Some(match bits & 0b0000_0111 {
            0 => Compression::None,
            1 => Compression::Gzip,
            2 => Compression::Snappy,
            3 => Compression::Lz4,
            4 => Compression::Zstd,
            _ => return None,
        })
    }

    pub fn bits(self) -> i16 {
        self as i16
    }
}

/// 批头视图（解析自 batch 前 RECORD_BATCH_HEADER_LEN 字节）。
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

impl BatchHeader {
    /// 从 `buf[0..61]` 解析；不做 CRC 校验（见 [`validate_crc`]）。
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

    pub fn compression(&self) -> Option<Compression> {
        Compression::from_bits(self.attributes)
    }

    pub fn is_control(&self) -> bool {
        self.attributes & 0b0010_0000 != 0
    }

    pub fn is_transactional(&self) -> bool {
        self.attributes & 0b0001_0000 != 0
    }

    pub fn total_len(&self) -> usize {
        BATCH_LENGTH_OFFSET + self.batch_length.max(0) as usize
    }

    /// 改写 base_offset（CRC 不覆盖该字段，无需重算）。
    pub fn write_base_offset(&self, buf: &mut [u8], base_offset: i64) {
        buf[0..8].copy_from_slice(&base_offset.to_be_bytes());
    }
}

/// CRC32C 校验：覆盖 attributes 起至批尾。
pub fn validate_crc(buf: &[u8]) -> bool {
    let Some(h) = BatchHeader::parse(buf) else {
        return false;
    };
    let total = h.total_len();
    // batchLength 必须至少覆盖 12..61 的头部其余部分，否则 CRC 覆盖域
    // （从 21 起）为空/倒挂——crafted 报文（如 batch_length=0）在此拒绝。
    if buf.len() < total || total < RECORD_BATCH_HEADER_LEN {
        return false;
    }
    let payload = &buf[CRC_PAYLOAD_OFFSET..total];
    crc32c::crc32c(payload) == h.crc
}

/// 便捷：从缓冲中读出第一个完整批的长度（不足/非法返回 None）。
/// batchLength 不足以容纳头部其余部分（total < 61）视为非法帧。
pub fn batch_len_at(buf: &[u8]) -> Option<usize> {
    let total = BatchHeader::parse(buf)?.total_len();
    if total < RECORD_BATCH_HEADER_LEN { return None; }
    Some(total)
}

/// 编码一个事务 marker 控制批（单记录，ADR-18 §4.2）。
/// attributes = transactional|control；key = [version:0][type]；value 空。
#[allow(clippy::too_many_arguments)]
pub fn encode_control_batch(
    base_offset: i64,
    leader_epoch: i32,
    first_timestamp: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    ty: ControlRecordType,
    out: &mut BytesMut,
) {
    let rec = Rec { timestamp_delta: 0, key: Some(ty.key()), value: None, headers: vec![] };
    encode_batch(
        base_offset,
        leader_epoch,
        first_timestamp,
        ATTR_TRANSACTIONAL | ATTR_CONTROL,
        producer_id,
        producer_epoch,
        base_sequence,
        &[rec],
        out,
    );
}

/// 读出控制批的 marker 类型（非控制批/解析失败返回 None）。
/// 只需首条记录的 key——记录布局：len(zigzag)|attrs:i8|tsDelta|offsetDelta|
/// keyLen(zigzag)|key。
pub fn control_record_type_of(batch: &[u8]) -> Option<ControlRecordType> {
    let h = BatchHeader::parse(batch)?;
    if !h.is_control() {
        return None;
    }
    let mut pos = RECORD_BATCH_HEADER_LEN;
    let _len = read_zigzag(batch, &mut pos)?;
    let _attrs = *batch.get(pos)?;
    pos += 1;
    let _ts = read_zigzag(batch, &mut pos)?;
    let _od = read_zigzag(batch, &mut pos)?;
    let klen = read_zigzag(batch, &mut pos)?;
    if klen < 4 {
        return None;
    }
    let key = batch.get(pos..pos + 4)?;
    ControlRecordType::from_i16(i16::from_be_bytes([key[2], key[3]]))
}

// ---------- 构造（测试与内部复制/标记用） ----------

/// 一条待编码记录。
#[derive(Debug, Clone)]
pub struct Rec {
    pub timestamp_delta: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<(Bytes, Option<Bytes>)>,
}

/// 编码一个未压缩 RecordBatch v2。
///
/// `base_offset` 由调用方指定（broker 侧为分配到的日志位点）。
#[allow(clippy::too_many_arguments)]
pub fn encode_batch(
    base_offset: i64,
    leader_epoch: i32,
    first_timestamp: i64,
    attributes: i16,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    records: &[Rec],
    out: &mut BytesMut,
) {
    let record_count = records.len() as i32;
    let last_offset_delta = record_count.saturating_sub(1);

    // 先编记录区（未压缩）
    let mut body = BytesMut::new();
    for (i, rec) in records.iter().enumerate() {
        let mut entry = BytesMut::new();
        entry.put_i8(0); // record attributes
        put_zigzag(&mut entry, rec.timestamp_delta);
        put_zigzag(&mut entry, i as i64); // offsetDelta
        match &rec.key {
            Some(k) => {
                put_zigzag(&mut entry, k.len() as i64);
                entry.extend_from_slice(k);
            }
            None => put_zigzag(&mut entry, -1),
        }
        match &rec.value {
            Some(v) => {
                put_zigzag(&mut entry, v.len() as i64);
                entry.extend_from_slice(v);
            }
            None => put_zigzag(&mut entry, -1),
        }
        put_zigzag(&mut entry, rec.headers.len() as i64);
        for (hk, hv) in &rec.headers {
            put_zigzag(&mut entry, hk.len() as i64);
            entry.extend_from_slice(hk);
            match hv {
                Some(v) => {
                    put_zigzag(&mut entry, v.len() as i64);
                    entry.extend_from_slice(v);
                }
                None => put_zigzag(&mut entry, -1),
            }
        }
        put_zigzag(&mut body, entry.len() as i64);
        body.extend_from_slice(&entry);
    }

    // batchLength 从偏移 12 起度量（含 leaderEpoch/magic/crc/头其余与记录区）
    let batch_length = (RECORD_BATCH_HEADER_LEN - BATCH_LENGTH_OFFSET + body.len()) as i32;
    out.reserve(RECORD_BATCH_HEADER_LEN + body.len());

    // CRC 覆盖自 attributes 起 —— 先拼出 CRC 覆盖区再算
    let mut crc_region = BytesMut::new();
    crc_region.put_i16(attributes);
    crc_region.put_i32(last_offset_delta);
    crc_region.put_i64(first_timestamp);
    crc_region.put_i64(first_timestamp); // maxTimestamp = firstTimestamp（未压缩构造）
    crc_region.put_i64(producer_id);
    crc_region.put_i16(producer_epoch);
    crc_region.put_i32(base_sequence);
    crc_region.put_i32(record_count);
    crc_region.extend_from_slice(&body);
    let crc = crc32c::crc32c(&crc_region);

    out.put_i64(base_offset);
    out.put_i32(batch_length);
    out.put_i32(leader_epoch);
    out.put_i8(MAGIC_V2);
    out.put_u32(crc);
    out.extend_from_slice(&crc_region);
}

// ---------- zigzag varint（记录内部用） ----------

/// 解码未压缩批的记录区（内部消费面：组状态重放等）。produce 路径不解
/// 记录体；压缩批返回 None（内部 topic 恒未压缩）。buf = 完整批。
pub fn decode_records(buf: &[u8]) -> Option<Vec<Rec>> {
    let h = BatchHeader::parse(buf)?;
    // from_bits(0) = Some(Compression::None)：未压缩也是 Some——只拒真压缩批
    if !matches!(h.compression(), None | Some(Compression::None)) {
        return None;
    }
    if h.is_control() {
        return None;
    }
    let count = h.record_count.max(0) as usize;
    let end = h.total_len().min(buf.len());
    let mut pos = RECORD_BATCH_HEADER_LEN;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        if pos >= end {
            return None;
        }
        let _len = read_zigzag(buf, &mut pos)?;
        let _attrs = *buf.get(pos)?;
        pos += 1;
        let ts = read_zigzag(buf, &mut pos)?;
        let _off_delta = read_zigzag(buf, &mut pos)?;
        let klen = read_zigzag(buf, &mut pos)?;
        let key = if klen < 0 {
            None
        } else {
            let k = klen as usize;
            if pos + k > end {
                return None;
            }
            let b = Bytes::copy_from_slice(&buf[pos..pos + k]);
            pos += k;
            Some(b)
        };
        let vlen = read_zigzag(buf, &mut pos)?;
        let value = if vlen < 0 {
            None
        } else {
            let v = vlen as usize;
            if pos + v > end {
                return None;
            }
            let b = Bytes::copy_from_slice(&buf[pos..pos + v]);
            pos += v;
            Some(b)
        };
        let hcount = read_zigzag(buf, &mut pos)?.max(0) as usize;
        let mut headers = Vec::with_capacity(hcount);
        for _ in 0..hcount {
            let hklen = read_zigzag(buf, &mut pos)?;
            if hklen < 0 {
                return None;
            }
            let hk = Bytes::copy_from_slice(&buf[pos..pos + hklen as usize]);
            pos += hklen as usize;
            let hvlen = read_zigzag(buf, &mut pos)?;
            let hv = if hvlen < 0 {
                None
            } else {
                let b = Bytes::copy_from_slice(&buf[pos..pos + hvlen as usize]);
                pos += hvlen as usize;
                Some(b)
            };
            headers.push((hk, hv));
        }
        out.push(Rec { timestamp_delta: ts, key, value, headers });
    }
    Some(out)
}

pub fn put_zigzag(buf: &mut BytesMut, v: i64) {
    let z = ((v << 1) ^ (v >> 63)) as u64;
    let mut shift = 0;
    loop {
        if shift >= 63 || (z >> shift) < 0x80 {
            buf.put_u8((z >> shift) as u8);
            return;
        }
        buf.put_u8((((z >> shift) & 0x7f) | 0x80) as u8);
        shift += 7;
    }
}

pub fn read_zigzag(buf: &[u8], pos: &mut usize) -> Option<i64> {
    let mut result: u64 = 0;
    let mut shift = 0;
    loop {
        let b = *buf.get(*pos)?;
        *pos += 1;
        if shift < 64 {
            result |= u64::from(b & 0x7f) << shift;
        }
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift > 70 {
            return None;
        }
    }
    Some(((result >> 1) as i64) ^ -((result & 1) as i64))
}

// ---------- 压缩（codec 用于测试/内部标记；produce 路径原样透传不解压） ----------

pub fn decompress(codec: Compression, data: &[u8], expected_uncompressed: usize) -> Option<Vec<u8>> {
    match codec {
        Compression::None => Some(data.to_vec()),
        Compression::Lz4 => {
            let mut out = Vec::with_capacity(expected_uncompressed);
            let mut dec = lz4_flex::frame::FrameDecoder::new(std::io::Cursor::new(data));
            std::io::Read::read_to_end(&mut dec, &mut out).ok()?;
            Some(out)
        }
        Compression::Zstd => {
            let mut out = Vec::with_capacity(expected_uncompressed);
            let mut dec = zstd::stream::Decoder::new(std::io::Cursor::new(data)).ok()?;
            std::io::Read::read_to_end(&mut dec, &mut out).ok()?;
            Some(out)
        }
        Compression::Gzip => {
            let mut out = Vec::with_capacity(expected_uncompressed);
            let mut dec = flate2::read::MultiGzDecoder::new(std::io::Cursor::new(data));
            std::io::Read::read_to_end(&mut dec, &mut out).ok()?;
            Some(out)
        }
        Compression::Snappy => None, // KIP-375 xerial 框架；由 report card 缺口驱动补齐
    }
}

pub fn compress(codec: Compression, data: &[u8]) -> Option<Vec<u8>> {
    match codec {
        Compression::None => Some(data.to_vec()),
        Compression::Lz4 => {
            let mut enc = lz4_flex::frame::FrameEncoder::new(Vec::new());
            std::io::Write::write_all(&mut enc, data).ok()?;
            enc.finish().ok()
        }
        Compression::Zstd => zstd::bulk::compress(data, 3).ok(),
        Compression::Gzip => {
            let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            std::io::Write::write_all(&mut enc, data).ok();
            enc.finish().ok()
        }
        Compression::Snappy => None,
    }
}

#[cfg(test)]
mod tests {
    //! 编解码 round-trip 与畸形输入面。

    use super::*;
    use bytes::BufMut as _;

    #[test]
    fn decode_records_round_trip() {
        let recs = vec![
            Rec { timestamp_delta: 0, key: Some(Bytes::from_static(b"grp")),
                  value: Some(Bytes::from_static(b"payload-1")), headers: vec![] },
            Rec { timestamp_delta: 5, key: None,
                  value: Some(Bytes::from_static(b"p2")), headers: vec![
                      (Bytes::from_static(b"h"), Some(Bytes::from_static(b"v")))] },
        ];
        let mut out = BytesMut::new();
        encode_batch(42, 0, 1000, 0, -1, -1, -1, &recs, &mut out);
        let got = decode_records(&out.freeze()).expect("decode");
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].key.as_deref(), Some(b"grp".as_ref()));
        assert_eq!(got[0].value.as_deref(), Some(b"payload-1".as_ref()));
        assert_eq!(got[1].timestamp_delta, 5);
        assert_eq!(got[1].headers.len(), 1);
    }
    use super::*;

    fn sample_records() -> Vec<Rec> {
        vec![
            Rec {
                timestamp_delta: 0,
                key: Some(Bytes::from_static(b"k1")),
                value: Some(Bytes::from_static(b"hello-world")),
                headers: vec![],
            },
            Rec {
                timestamp_delta: 5,
                key: None,
                value: Some(Bytes::from_static(b"v2")),
                headers: vec![(Bytes::from_static(b"h"), Some(Bytes::from_static(b"1")))],
            },
        ]
    }

    #[test]
    fn encode_validate_roundtrip() {
        let mut buf = BytesMut::new();
        encode_batch(42, 0, 1_000, 0, -1, -1, -1, &sample_records(), &mut buf);
        let buf = buf.freeze();
        let h = BatchHeader::parse(&buf).unwrap();
        assert_eq!(h.magic, MAGIC_V2);
        assert_eq!(h.base_offset, 42);
        assert_eq!(h.record_count, 2);
        assert_eq!(h.last_offset_delta, 1);
        assert_eq!(h.compression(), Some(Compression::None));
        assert!(validate_crc(&buf));
        // 改写 base offset 不破坏 CRC
        let mut tmp = buf.to_vec();
        h.write_base_offset(&mut tmp, 100);
        assert!(validate_crc(&tmp));
        assert_eq!(BatchHeader::parse(&tmp).unwrap().base_offset, 100);
    }

    #[test]
    fn crc_detects_corruption() {
        let mut buf = BytesMut::new();
        encode_batch(0, 0, 0, 0, -1, -1, -1, &sample_records(), &mut buf);
        let mut tmp = buf.to_vec();
        let last = tmp.len() - 1;
        tmp[last] ^= 0xff;
        assert!(!validate_crc(&tmp));
    }

    #[test]
    fn batch_len_scanning() {
        let mut buf = BytesMut::new();
        encode_batch(0, 0, 0, 0, -1, -1, -1, &sample_records(), &mut buf);
        encode_batch(2, 0, 0, 0, -1, -1, -1, &sample_records(), &mut buf);
        let buf = buf.freeze();
        let l1 = batch_len_at(&buf).unwrap();
        let h1 = BatchHeader::parse(&buf).unwrap();
        assert_eq!(l1, h1.total_len());
        // Kafka 语义：total = 12 + batchLength（batchLength 从偏移 12 度量）
        assert_eq!(l1, BATCH_LENGTH_OFFSET + h1.batch_length as usize);
        let h2 = BatchHeader::parse(&buf[l1..]).unwrap();
        assert_eq!(h2.base_offset, 2);
    }

    #[test]
    fn zigzag_roundtrip() {
        for v in [0i64, -1, 1, -64, 64, i32::MIN as i64, i64::MAX, i64::MIN + 1] {
            let mut b = BytesMut::new();
            put_zigzag(&mut b, v);
            let mut pos = 0usize;
            assert_eq!(read_zigzag(&b, &mut pos), Some(v));
            assert_eq!(pos, b.len());
        }
    }

    #[test]
    fn compressed_roundtrip_lz4_zstd_gzip() {
        for codec in [Compression::Lz4, Compression::Zstd, Compression::Gzip] {
            let mut raw = BytesMut::new();
            encode_batch(0, 0, 0, 0, -1, -1, -1, &sample_records(), &mut raw);
            let body = &raw[RECORD_BATCH_HEADER_LEN..];
            let packed = compress(codec, body).unwrap();
            let unpacked = decompress(codec, &packed, body.len()).unwrap();
            assert_eq!(&unpacked[..], body, "{codec:?}");
        }
    }

    /// ADR-18：控制批编解码往返——attributes 双位置位、类型从 key 解出。
    #[test]
    fn control_batch_roundtrip() {
        for (ty, wire) in [(ControlRecordType::Abort, 0i16), (ControlRecordType::Commit, 1)] {
            let mut buf = BytesMut::new();
            encode_control_batch(7, 0, 1_000, 42, 3, 9, ty, &mut buf);
            let buf = buf.freeze();
            let h = BatchHeader::parse(&buf).unwrap();
            assert!(h.is_control() && h.is_transactional());
            assert_eq!(h.producer_id, 42);
            assert_eq!(h.producer_epoch, 3);
            assert_eq!(h.record_count, 1);
            assert!(validate_crc(&buf));
            assert_eq!(control_record_type_of(&buf), Some(ty));
            let _ = wire;
        }
        // 非控制批 → None
        let mut plain = BytesMut::new();
        encode_batch(0, 0, 0, 0, -1, -1, -1, &sample_records(), &mut plain);
        assert_eq!(control_record_type_of(&plain.freeze()), None);
    }
}

#[cfg(test)]
mod crafted_tests {
    use super::*;

    /// 合法 magic + batch_length=0 的报文：total_len()=12 < CRC 起点 21。
    #[test]
    fn validate_crc_on_crafted_zero_batch_length() {
        let mut m = vec![0u8; 64];
        m[16] = MAGIC_V2 as u8;                   // magic 合法
        m[8..12].copy_from_slice(&0i32.to_be_bytes()); // batch_length = 0
        // 修复前：此处 panic（&buf[21..12]）；修复后：应返回 false
        assert_eq!(validate_crc(&m), false);
    }

    #[test]
    fn batch_len_at_on_crafted_zero_batch_length() {
        let mut m = vec![0u8; 64];
        m[16] = MAGIC_V2 as u8;
        m[8..12].copy_from_slice(&0i32.to_be_bytes());
        // 修复前：Some(12)（调用方按此切片 [21..12] 必 panic）；修复后：None
        assert_eq!(batch_len_at(&m), None);
    }
}
