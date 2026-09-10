//! C7 第二件：恢复安全性模型 —— WIP（不编译通过，不计入账本已验证集合）
//!
//! 状态：模型与定理陈述完备（T1 前缀连续/T2 长度有界/T3 synced 存活），
//! 证明卡在 Seq subrange/skip 的索引公理触发（tail[i] == b[i+1] 桥接）。
//! 下一轮修法：(a) 改用 vstd::seq_lib 的 ext_equal + skip 索引引理族；
//! (b) 或以 Map<int, u8> 建模代替 Seq 递归；(c) 或 T2/T3 先证、T1 单列。
//! 现存编译状态见 git log（本文件此前一轮已通过 4 verified 的中间态）。
//!
//! 模型：日志 = 追加序列（每批 1 条记录，打包细节由 L1 测试覆盖）。
//! 每批带 intact 标志（sync 过的批 intact，crash/torn 后的尾部批可能非 intact）。
//!
//! 核心定理：
//!   T1 恢复前缀性：recovery 返回的批序列是 appended 的前缀（无跳变/无重复）
//!   T2 长度有界：恢复长度 ≤ min(批数, intact 前缀长度)
//!   T3 持久性：已 sync 的批必然 intact ⇒ 恢复包含全部已 sync 记录
//!
//! 运行：verus --crate-type=lib log_recovery_model.rs

use vstd::prelude::*;

verus! {

/// 抽象批：offset + 完整性标志
#[derive(Copy, Clone)]
pub struct Entry {
    pub off: int,
    pub intact: bool,
}

/// 恢复扫描：返回首个不完整批之前的有效批数
/// （与 log.rs scan_and_truncate 的"坏尾截断"语义同构）
pub open spec fn scan_valid_prefix(b: Seq<Entry>) -> int
    decreases b.len()
{
    if b.len() == 0 {
        0
    } else if !b[0].intact {
        0
    } else {
        1 + scan_valid_prefix(b.skip(1))
    }
}

// ---------- 引理族 ----------

/// 有效批的 offset 单调 +1：完整前缀的第 i 批 offset == 首批 offset + i
/// （前提：批序列本身满足 offset 连续性——由 append 路径的分配保证）
pub open spec fn chain_ok(b: Seq<Entry>) -> bool
    decreases b.len()
{
    if b.len() <= 1 {
        true
    } else {
        b[1].off == b[0].off + 1 && chain_ok(b.skip(1))
    }
}

/// T1：扫描返回的有效前缀，其 offset 序列连续（无跳变）
proof fn scan_prefix_offsets_contiguous(b: Seq<Entry>, base: int)
    requires
        b.len() > 0,
        b[0].off == base,
        chain_ok(b),
    ensures
        forall|i: int|
            (0 <= i && i < scan_valid_prefix(b)) ==> (b[i].off == base + i),
    decreases b.len()
{
    if b.len() == 0 {
    } else if scan_valid_prefix(b) == 0 {
    } else {
        assert(b[0].intact);
        let tail = b.skip(1);
        assert(tail[0].off == base + 1) by {
            assert(b[1].off == b[0].off + 1);
        }
        scan_prefix_offsets_contiguous(tail, base + 1);
    }
}

/// T2：恢复长度不超过批数
proof fn scan_len_bounded(b: Seq<Entry>)
    ensures scan_valid_prefix(b) <= b.len()
    decreases b.len()
{
    if b.len() == 0 {
    } else {
        let tail = b.skip(1);
        scan_len_bounded(tail);
        assert(b.skip(1).len() == b.len() - 1);
    }
}

/// T3（持久性）：已 sync 的批必然 intact——
/// 前提 sync_count ≤ 批数： crash 后的前 sync_count 批保持 intact，
/// 于是恢复至少包含前 sync_count 条记录（已 sync 数据不丢）。
proof fn synced_records_survive(b: Seq<Entry>, sync_count: int)
    requires
        b.len() >= sync_count,
        sync_count >= 0,
        forall|i: int| (0 <= i && i < sync_count) ==> b[i].intact,
    ensures scan_valid_prefix(b) >= sync_count
    decreases sync_count
{
    if sync_count == 0 {
        return;
    }
    assert(b[0].intact) by {
        if sync_count > 0 {
            assert forall|i: int| (0 <= i && i < sync_count) ==> b[i].intact by {};
            assert(b[0].intact);
        }
    }
    let tail = b.skip(1);
    // 归纳步：尾部的前 sync_count - 1 批也满足 intact 前提
    assert forall|i: int| (0 <= i && i < sync_count - 1) ==> tail[i].intact by {
        assert(tail[i] == b[i + 1]);
        assert(b[i + 1].intact);
    }
    synced_records_survive(tail, sync_count - 1);
}

} // verus!
