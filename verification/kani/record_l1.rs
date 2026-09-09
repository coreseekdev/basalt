//! Kani L1 harness（账本 C13'：任意输入无 panic）——与 record/src/lib.rs
//! 同源的独立副本（standalone 模式不拉 cargo 依赖；与真实现的一致性由
//! CI 的同源比对注释维护，见 README）。
//!
//! 运行：kani record_l1.rs

#[inline(never)]
fn put_zigzag_impl(buf: &mut Vec<u8>, v: i64) {
    let z = ((v << 1) ^ (v >> 63)) as u64;
    let mut shift = 0;
    loop {
        if shift >= 63 || (z >> shift) < 0x80 {
            buf.push((z >> shift) as u8);
            return;
        }
        buf.push((((z >> shift) & 0x7f) | 0x80) as u8);
        shift += 7;
    }
}

#[inline(never)]
fn read_zigzag_impl(buf: &[u8], pos: &mut usize) -> Option<i64> {
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

#[kani::proof]
fn zigzag_roundtrip_no_panic() {
    let v: i64 = kani::any();
    let mut buf = Vec::new();
    put_zigzag_impl(&mut buf, v);
    let mut pos = 0usize;
    let r = read_zigzag_impl(&buf, &mut pos);
    assert_eq!(r, Some(v));
    assert_eq!(pos, buf.len());
}

#[kani::proof]
fn read_zigzag_arbitrary_bytes_no_panic() {
    // 任意 ≤3 字节输入：解码不 panic（overlong 走 shift>70 的 None 分支）
    let (b0, b1, b2): (u8, u8, u8) = (kani::any(), kani::any(), kani::any());
    let buf = [b0, b1, b2];
    let mut pos = 0usize;
    let _ = read_zigzag_impl(&buf, &mut pos);
}
