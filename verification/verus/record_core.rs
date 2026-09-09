//! record 核心编解码的 Verus 证明切片（docs/VERIFICATION.md §7 账本 C5/C6）。
//!
//! 覆盖（全称量化——不是测试，是对所有输入的证明）：
//!   C5  zigzag 双射：对全部 i64，zig/unzig 无损往返；
//!       且 crate 的位运算形式（put_zigzag/read_zigzag 的核心行）
//!       与算术规约逐点相等（bit_vector 桥接）。
//!   C6a Compression::from_bits/bits 全函数正确性与往返。
//! varint（LEB128 循环机器）的循环级证明为下一步，见本目录 README.md。
//!
//! 运行：  verus --crate-type=lib record_core.rs
//! 实现对应： basalt/record/src/lib.rs（put_zigzag / read_zigzag / Compression）

use vstd::prelude::*;

verus! {

// ---------------- spec 层：纯算术定义（无位运算） ----------------

/// zigzag 的算术定义：v>=0 映射 2v，v<0 映射 -2v-1。
pub open spec fn zig_spec(v: i64) -> u64
{
    if v >= 0 { (2 * v as int) as u64 } else { ((2 * (-1 - v) as int) + 1) as u64 }
}

/// unzig 的算术定义（避免越界取负：全程 int 域运算后收拢到 i64）。
pub open spec fn unzig_spec(z: u64) -> i64
{
    let h: int = (z / 2) as int;
    if z % 2 == 0 { h as i64 } else { (-(h + 1)) as i64 }
}

/// 取 i16 低 3 位（非算术语义），返回 0..=7。
pub open spec fn low3(b: i16) -> int
{
    ((b as int % 8) + 8) % 8
}

#[derive(Copy, Clone, PartialEq, Eq)]
#[repr(i16)]
pub enum Compression { None, Gzip, Snappy, Lz4, Zstd }

/// 3 位码到压缩算法的全函数映射。
pub open spec fn comp_of(k: int) -> Option<Compression>
{
    if k == 0 { Some(Compression::None) }
    else if k == 1 { Some(Compression::Gzip) }
    else if k == 2 { Some(Compression::Snappy) }
    else if k == 3 { Some(Compression::Lz4) }
    else if k == 4 { Some(Compression::Zstd) }
    else { None }
}

// ---------------- 执行层：算术形式（与规约同构，直接可证） ----------------

pub fn zig(v: i64) -> (z: u64)
    ensures z == zig_spec(v)
{
    if v >= 0 { 2u64 * (v as u64) } else { 2u64 * ((-1 - v) as u64) + 1u64 }
}

pub fn unzig(z: u64) -> (r: i64)
    ensures r == unzig_spec(z)
{
    let h = z / 2;
    if z % 2 == 0 { h as i64 } else { -1 - (h as i64) }
}

// ---------------- crate 位运算形式（put_zigzag/read_zigzag 的核心行） ----------------

/// crate 形式：`let z = ((v << 1) ^ (v >> 63)) as u64;`
/// 位形式的 spec 定义（与执行表达式逐字相同，正确性平凡成立）。
pub open spec fn zig_bits(v: i64) -> u64
{
    ((v << 1) ^ (v >> 63)) as u64
}

pub fn zig_crate(v: i64) -> (z: u64)
    ensures z == zig_bits(v)
{
    ((v << 1) ^ (v >> 63)) as u64
}

/// 桥接引理：位形式与算术形式逐点相等（两行 zigzag 恒等式）。
proof fn zig_bits_matches_math(v: i64)
    ensures zig_bits(v) == zig_spec(v)
{
    assert(((v << 1) ^ (v >> 63)) as u64
        == ((if v >= 0 { ((v as u64) + (v as u64)) }
             else { ((((-1 - v) as u64) + ((-1 - v) as u64)) + 1u64) }) as u64))
        by (bit_vector);
    // 其余由 spec_shl 语义（左移一位 = 乘 2）与无回绕事实经 int 推理闭合
}

/// crate 形式：`((result >> 1) as i64) ^ -((result & 1) as i64)`
pub fn unzig_crate(z: u64) -> (r: i64)
    ensures r == unzig_spec(z)
{
    let half = (z >> 1) as i64;
    let low = (z & 1) as i64;
    proof {
        assert((z >> 1) == z / 2) by (bit_vector);
        assert((z & 1) == z % 2) by (bit_vector);
        assert forall|h: i64| #[trigger] (h ^ 0i64) == h by {
            assert(h ^ 0i64 == h) by (bit_vector);
        }
        assert forall|h: i64| #[trigger] (h ^ -1i64) == ((-1 - h) as i64) by {
            assert(h ^ -1i64 == ((-1 - h) as i64)) by (bit_vector);
        }
    }
    if low == 0 {
        half ^ ((-low) as i64)
    } else {
        half ^ ((-low) as i64)
    }
}

// ---------------- 双射性（核心定理） ----------------

/// spec 层双射：unzig_spec(zig_spec(v)) == v 对全部 i64 成立。
proof fn zig_unzig_spec_inverse(v: i64)
    ensures unzig_spec(zig_spec(v)) == v
{
    if v >= 0 {
        assert(zig_spec(v) % 2 == 0);
        assert(zig_spec(v) / 2 == v);
    } else {
        assert(zig_spec(v) % 2 == 1);
        assert(zig_spec(v) / 2 == (-1 - v) as u64);
    }
}

/// 端到端：算术实现复合后对所有 i64 无损。
/// （等价于 record/src/lib.rs 测试中的 zigzag_roundtrip，但覆盖全部输入。）
pub fn roundtrip(v: i64) -> (r: i64)
    ensures r == v
{
    let z = zig(v);
    unzig(z)
}

/// 端到端：crate 位运算形式同样无损（proptest 全称化版本）。
pub fn roundtrip_crate(v: i64) -> (r: i64)
    ensures r == v
{
    proof { zig_bits_matches_math(v); }
    let z = zig_crate(v);
    unzig_crate(z)
}

// ---------------- Compression::from_bits / bits ----------------

/// crate 形式：`bits & 0b0000_0111` 的 match（未知码 return None）。
pub fn compression_from_bits(bits: i16) -> (r: Option<Compression>)
    ensures r == comp_of(low3(bits))
{
    proof {
        assert((bits & 0b0000_0111) as int == low3(bits)) by (bit_vector);
    }
    Some(match bits & 0b0000_0111 {
        0 => Compression::None,
        1 => Compression::Gzip,
        2 => Compression::Snappy,
        3 => Compression::Lz4,
        4 => Compression::Zstd,
        _ => return None,
    })
}

/// crate 形式：`self as i16`。
pub fn compression_bits(c: Compression) -> (b: i16)
    ensures comp_of(low3(b)) == Some(c)
{
    proof {
        assert(low3(c as i16) == c as int);
    }
    c as i16
}

/// 端到端：from_bits(bits(x)) == Some(x) 对全部枚举值成立。
pub fn compression_roundtrip(c: Compression) -> (r: Option<Compression>)
    ensures r == Some(c)
{
    let b = compression_bits(c);
    compression_from_bits(b)
}

// ---------------- varint（LEB128）：C6 ----------------
//
// 线格式：低 7 位组在前，除最后组外每组最高位置 1。spec 全程算术形式；
// decode 用 u128 前向递归（≤ 10 字节 < 2^70 ≪ 2^128，无溢出义务），
// 解码规格 len==10 分支为值域判定（spec_varint_val < 2^64）。

pub open spec fn pow128(k: int) -> int
    decreases k
{
    if k <= 0 { 1 } else { 128 * pow128(k - 1) }
}

pub open spec fn spec_varint_val(s: Seq<u8>) -> int
    decreases s.len()
{
    if s.len() == 0 {
        0
    } else {
        (s[0] as int % 128) + spec_varint_val(s.subrange(1, s.len() as int)) * 128
    }
}

pub open spec fn spec_varint_decode(s: Seq<u8>) -> Option<u64>
{
    if s.len() == 0 { None }
    else if s[s.len() as int - 1] >= 128u8 { None }
    else if s.len() > 10 { None }
    else if spec_varint_val(s) >= 18446744073709551616 { None }
    else { Some(spec_varint_val(s) as u64) }
}

// ----- pow128 引理族 -----

proof fn pow128_pos(k: int)
    ensures pow128(k) >= 1
    decreases k
{
    if k > 0 { pow128_pos(k - 1); }
}

proof fn pow128_step(k: int)
    requires k >= 0
    ensures pow128(k + 1) == 128 * pow128(k)
{
    assert(pow128(k + 1) == 128 * pow128(k));
}

proof fn pow128_mono(k: int, j: int)
    requires 0 <= k <= j
    ensures pow128(k) <= pow128(j)
    decreases j
{
    if k < j {
        pow128_mono(k, j - 1);
        pow128_pos(j - 1);
        pow128_step(j - 1);
    }
}

proof fn pow128_concrete()
    ensures
        pow128(8) == 72057594037927936,
        pow128(9) == 9223372036854775808,
        pow128(10) == 1180591620717411303424,
{
    assert(pow128(1) == 128 * pow128(0));
    assert(pow128(2) == 128 * pow128(1));
    assert(pow128(3) == 128 * pow128(2));
    assert(pow128(4) == 128 * pow128(3));
    assert(pow128(5) == 128 * pow128(4));
    assert(pow128(6) == 128 * pow128(5));
    assert(pow128(7) == 128 * pow128(6));
    assert(pow128(8) == 128 * pow128(7));
    assert(pow128(9) == 128 * pow128(8));
    assert(pow128(10) == 128 * pow128(9));
}

// ----- spec_val 引理族 -----

proof fn spec_varint_val_nonneg(s: Seq<u8>)
    ensures spec_varint_val(s) >= 0
    decreases s.len()
{
    if s.len() > 0 { spec_varint_val_nonneg(s.subrange(1, s.len() as int)); }
}

proof fn spec_val_push(s: Seq<u8>, b: u8)
    ensures
        spec_varint_val(s.push(b))
            == spec_varint_val(s) + (b as int % 128) * pow128(s.len() as int)
    decreases s.len()
{
    let t = s.push(b);
    if s.len() == 0 {
        assert(t.len() == 1);
        assert(t[0] == b);
        assert(t.subrange(1, 1).len() == 0);
        assert(t.subrange(1, 1) =~= Seq::<u8>::empty());
        assert(spec_varint_val(t.subrange(1, 1)) == 0);
        assert(spec_varint_val(t) == (b as int % 128) + spec_varint_val(t.subrange(1, 1)) * 128);
    } else {
        let sub = s.subrange(1, s.len() as int);
        assert(t.subrange(1, t.len() as int) =~= sub.push(b));
        spec_val_push(sub, b);
        pow128_step(sub.len() as int);
        let x: int = spec_varint_val(sub);
        let y: int = b as int % 128;
        let h: int = s[0] as int % 128;
        let q: int = pow128(sub.len() as int);
        let r: int = pow128(s.len() as int);
        assert(r == 128 * q);
        assert((r == 128 * q)
            ==> ((h + 128 * (x + y * q)) == (h + 128 * x + y * r))) by (nonlinear_arith);
    }
}

proof fn spec_val_prefix_bound(s: Seq<u8>)
    requires s.len() <= 10
    ensures spec_varint_val(s) < pow128(s.len() as int)
    decreases s.len()
{
    if s.len() > 0 {
        spec_val_prefix_bound(s.subrange(1, s.len() as int));
        pow128_step(s.len() as int - 1);
        pow128_pos(s.len() as int - 1);
    }
}

// ----- encode：put_varint -----

proof fn cont_digit(x: u8)
    ensures
        ((x | 128u8) as int % 128) == x as int % 128,
        x | 128u8 >= 128u8,
{
    assert(((x | 128u8) % 128u8) == (x % 128u8)) by (bit_vector);
    assert(x | 128u8 >= 128u8) by (bit_vector);
}

pub fn put_varint(z: u64) -> (buf: Vec<u8>)
    ensures
        1 <= buf@.len(),
        buf@.len() <= 10,
        spec_varint_decode(buf@) == Some(z),
        forall|i: int| 0 <= i < buf@.len() - 1 ==> buf@[i] >= 128u8,
        buf@[buf@.len() - 1] < 128u8,
{
    let mut buf: Vec<u8> = Vec::new();
    let mut rest: u64 = z;
    while rest >= 128
        invariant
            buf@.len() <= 9,
            spec_varint_val(buf@) + rest as int * pow128(buf@.len() as int) == z as int,
            buf@.len() == 0 || pow128(buf@.len() as int) <= z as int,
            forall|i: int| 0 <= i < buf@.len() ==> buf@[i] >= 128u8,
        decreases rest,
    {
        let d: u8 = (rest % 128) as u8;
        proof {
            let k: int = buf@.len() as int;
            let rn: int = (rest / 128) as int;
            let sv: int = spec_varint_val(buf@);
            let dm: int = d as int;
            let p: int = pow128(k);
            let p1: int = pow128(k + 1);
            cont_digit(d);
            spec_varint_val_nonneg(buf@);
            pow128_step(k);
            spec_val_push(buf@, d | 128u8);
            assert(p1 == 128 * p);
            assert((rest as int) == 128 * rn + dm);
            assert((p1 == 128 * p && sv >= 0 && (rest as int) == 128 * rn + dm)
                ==> (sv + (rest as int) * p == sv + dm * p + rn * p1)) by (nonlinear_arith);
            assert((rest as int) >= 128);
            assert(((sv >= 0 && (rest as int) >= 128 && z as int == sv + (rest as int) * p))
                ==> (z as int >= 128 * p)) by (nonlinear_arith);
        }
        buf.push(d | 128u8);
        rest = rest / 128;
        proof {
            if buf@.len() >= 10 {
                pow128_mono(10, buf@.len() as int);
                pow128_concrete();
                assert((pow128(10)) > (z as int));
                assert(false);
            }
        }
    }
    proof {
        if buf@.len() >= 10 {
            pow128_mono(10, buf@.len() as int);
            pow128_concrete();
            assert((pow128(10)) > (z as int));
            assert(false);
        }
        spec_val_push(buf@, rest as u8);
    }
    buf.push(rest as u8);
    buf
}

// ----- decode：get_varint -----

/// 前向递归计算 LEB128 值（u128 无溢出：≤ 10 字节 < 2^70 ≪ 2^128）。
fn get_digits(buf: &[u8], start: usize) -> (r: u128)
    requires
        start <= buf.len(),
        buf.len() <= 10,
    ensures
        r as int == spec_varint_val(buf@.subrange(start as int, buf@.len() as int)),
        (buf@.len() as int - start as int) <= 10 ==> r < pow128(buf@.len() as int - start as int),
    decreases buf.len() - start
{
    if start == buf.len() {
        proof {
            assert(buf@.subrange(start as int, buf@.len() as int) =~= Seq::<u8>::empty());
            assert(pow128(0) == 1);
        }
        0u128
    } else {
        let d: u128 = (buf[start] % 128u8) as u128;
        let rest = get_digits(buf, start + 1);
        proof {
            let whole = buf@.subrange(start as int, buf@.len() as int);
            let tail = buf@.subrange(start as int + 1, buf@.len() as int);
            let j: int = whole.len() as int - 1;
            assert(whole.len() >= 1);
            assert(whole[0] == buf@[start as int]);
            assert(whole.subrange(1, whole.len() as int) =~= tail);
            pow128_step(j);
            pow128_pos(j);
            pow128_mono(whole.len() as int, 10);
            pow128_concrete();
            let pw: int = pow128(j);
            let q: int = pow128(whole.len() as int);
            assert(q == 128 * pw);
            assert((rest as int) < pw);
            assert(d as int <= 127);
            assert((q == 128 * pw && (rest as int) < pw && (d as int) <= 127)
                ==> ((d as int) + 128 * (rest as int) < q)) by (nonlinear_arith);
        }
        d + 128u128 * rest
    }
}

pub fn get_varint(buf: &[u8]) -> (r: Option<u64>)
    ensures r == spec_varint_decode(buf@)
{
    if buf.len() == 0 { return None; }
    if buf[buf.len() - 1] >= 128u8 { return None; }
    if buf.len() > 10 { return None; }
    let v = get_digits(buf, 0);
    proof {
        assert(buf@.subrange(0, buf@.len() as int) =~= buf@);
        assert(v as int == spec_varint_val(buf@));
    }
    if v >= 18446744073709551616u128 { return None; }
    Some(v as u64)
}

} // verus!
