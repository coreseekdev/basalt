//! 线格式原语：大端定宽整数、unsigned varint（compact/tag 用）、
//! compact string/bytes/array 长度约定（len+1，0 = null）。
//!
//! Kafka wire 是大端；varint 是 LEB128 无符号（无 zigzag——zigzag 只在
//! record 内部，归 record crate）。

use crate::error::{ProtocolError, Result};
use bytes::{BufMut, BytesMut};

#[derive(Debug)]
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    pub fn pos(&self) -> usize {
        self.pos
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    fn need(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(ProtocolError::UnexpectedEof { pos: self.pos, need: n - self.remaining() });
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.need(1)?[0])
    }
    pub fn i8(&mut self) -> Result<i8> {
        Ok(self.need(1)?[0] as i8)
    }
    pub fn i16(&mut self) -> Result<i16> {
        let b = self.need(2)?;
        Ok(i16::from_be_bytes([b[0], b[1]]))
    }
    pub fn u16(&mut self) -> Result<u16> {
        let b = self.need(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }
    pub fn i32(&mut self) -> Result<i32> {
        let b = self.need(4)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }
    pub fn u32(&mut self) -> Result<u32> {
        let b = self.need(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }
    pub fn i64(&mut self) -> Result<i64> {
        let b = self.need(8)?;
        Ok(i64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
    }
    pub fn f64(&mut self) -> Result<f64> {
        Ok(f64::from_bits(self.i64()? as u64))
    }

    pub fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        self.need(n)
    }

    /// 无符号 LEB128 varint（最多 5 字节 / 32 位用途）。
    pub fn uvarint(&mut self) -> Result<u32> {
        let mut result: u32 = 0;
        let mut shift = 0u32;
        for _ in 0..5 {
            let b = self.u8()?;
            result |= u32::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
        }
        Err(ProtocolError::BadVarint { pos: self.pos })
    }

    pub fn skip(&mut self, n: usize) -> Result<()> {
        self.need(n)?;
        Ok(())
    }

    /// 跳到末尾（丢弃尾部未知字节——tag section 之后理论上无内容，防御性）。
    pub fn finish(&mut self) {
        self.pos = self.buf.len();
    }
}

/// 便捷：把整个 `&[u8]` 剩余部分作为 records/bytes 拷贝出去。
pub fn to_writer(f: impl FnOnce(&mut BytesMut)) -> BytesMut {
    let mut b = BytesMut::new();
    f(&mut b);
    b
}

pub fn put_uvarint(buf: &mut BytesMut, mut v: u32) {
    loop {
        if v < 0x80 {
            buf.put_u8(v as u8);
            return;
        }
        buf.put_u8(((v & 0x7f) | 0x80) as u8);
        v >>= 7;
    }
}

pub fn uvarint_len(v: u32) -> usize {
    let mut n = 1;
    let mut v = v >> 7;
    while v > 0 {
        n += 1;
        v >>= 7;
    }
    n
}

/// Compact 长度编码：len+1，0 表示 null。
pub fn put_compact_len(buf: &mut BytesMut, len: Option<usize>) {
    match len {
        Some(l) => put_uvarint(buf, (l + 1) as u32),
        None => put_uvarint(buf, 0),
    }
}

pub fn compact_null_len(buf: &mut BytesMut) {
    put_uvarint(buf, 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn uvarint_roundtrip() {
        for v in [0u32, 1, 127, 128, 300, 16383, 16384, u32::MAX >> 3, 0x0FFF_FFFF] {
            let mut b = BytesMut::new();
            put_uvarint(&mut b, v);
            let mut r = Reader::new(&b);
            assert_eq!(r.uvarint().unwrap(), v, "roundtrip {v}");
        }
    }

    #[test]
    fn big_endian_ints() {
        let mut b = BytesMut::new();
        b.put_i16(-2);
        b.put_i32(0x0102_0304);
        b.put_i64(-1);
        let mut r = Reader::new(&b);
        assert_eq!(r.i16().unwrap(), -2);
        assert_eq!(r.i32().unwrap(), 0x0102_0304);
        assert_eq!(r.i64().unwrap(), -1);
    }
}
