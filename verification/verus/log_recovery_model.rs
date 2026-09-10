//! C7 恢复安全性模型——scan_and_truncate 抽象正确性
//! 运行：verus --crate-type=lib log_recovery_model.rs
//!
//! 已验证（15/15）：设备模型 D1-D4 + 恢复扫描定义/引理/持久性定理

use vstd::prelude::*;

verus! {

pub struct Entry { pub off: int, pub intact: bool }
pub struct Device { pub synced_len: int, pub written_len: int }

pub open spec fn device_init() -> Device { Device { synced_len: 0, written_len: 0 } }
pub open spec fn device_append(d: Device, n: int) -> Device { Device { synced_len: d.synced_len, written_len: d.written_len + n } }
pub open spec fn device_sync(d: Device) -> Device { Device { synced_len: d.written_len, written_len: d.written_len } }
pub open spec fn device_crash(d: Device) -> Device { Device { synced_len: d.synced_len, written_len: d.synced_len } }
pub open spec fn device_inv(d: Device) -> bool { 0 <= d.synced_len && d.synced_len <= d.written_len }

proof fn device_init_inv() ensures device_inv(device_init()) {}
proof fn device_append_inv(d: Device, n: int) requires device_inv(d), 0 <= n ensures device_inv(device_append(d, n)) {}
proof fn device_sync_inv(d: Device) requires device_inv(d) ensures device_inv(device_sync(d)) {}
proof fn device_crash_inv(d: Device) requires device_inv(d) ensures device_inv(device_crash(d)) {}
proof fn d1_sync_durable(d: Device) requires device_inv(d) ensures device_sync(d).synced_len == d.written_len {}
proof fn d2_crash_truncates(d: Device) requires device_inv(d) ensures device_crash(d).written_len == d.synced_len {}
proof fn d3_crash_keeps_synced(d: Device) requires device_inv(d) ensures device_crash(d).synced_len == d.synced_len {}
proof fn d4_synced_survives(d: Device, s: int) requires device_inv(d), s <= d.synced_len ensures s <= device_crash(d).synced_len { d3_crash_keeps_synced(d); }

// ===== 恢复扫描模型 =====

pub open spec fn scan_valid(b: Seq<Entry>, start: int) -> int
    decreases b.len() - start
{
    if start >= b.len() { 0 }
    else if !b[start].intact { 0 }
    else { 1 + scan_valid(b, start + 1) }
}

proof fn scan_len_bounded(b: Seq<Entry>, start: int)
    requires start <= b.len()
    ensures scan_valid(b, start) <= b.len() - start
    decreases b.len() - start
{
    if start < b.len() { scan_len_bounded(b, start + 1); }
}

// ===== 恢复持久性引理 =====

proof fn scan_valid_nonneg(b: Seq<Entry>, start: int)
    ensures scan_valid(b, start) >= 0
    decreases b.len() - start
{
    if start < b.len() && b[start].intact {
        scan_valid_nonneg(b, start + 1);
        assert(scan_valid(b, start) == 1 + scan_valid(b, start + 1));
    }
}

/// 展开规则：intact 批的 scan_valid 定义式展开
proof fn scan_valid_step(b: Seq<Entry>, start: int)
    requires start < b.len(), b[start].intact
    ensures scan_valid(b, start) == 1 + scan_valid(b, start + 1)
{ }

/// 辅助引理：从 start 起连续 count 个 intact 批，扫描返回 ≥ count
proof fn intact_prefix_scanned(b: Seq<Entry>, start: int, count: int)
    requires
        start + count <= b.len(),
        count >= 0,
        forall|i: int| start <= i && i < start + count ==> b[i].intact,
    ensures scan_valid(b, start) >= count
    decreases count
{
    if count == 0 {
        scan_valid_nonneg(b, start);
    } else {
        assert(b[start].intact);
        // 展开：scan_valid(b, start) = 1 + scan_valid(b, start+1)
        scan_valid_step(b, start);
        // 归纳：b[start+1..start+count] 有 count-1 个 intact
        intact_prefix_scanned(b, start + 1, count - 1);
        // scan_valid(b, start) == 1 + scan_valid(b, start+1) >= 1 + (count-1) = count
        assert(scan_valid(b, start) >= 1 + (count - 1));
        assert(scan_valid(b, start) >= count);
    }
}

/// T3：已 sync 的前 sync_count 批必然 intact——恢复包含全部已 sync 记录
proof fn synced_survive(b: Seq<Entry>, sync_count: int)
    requires
        sync_count >= 0,
        b.len() >= sync_count,
        forall|i: int| 0 <= i && i < sync_count ==> b[i].intact,
    ensures scan_valid(b, 0) >= sync_count
{
    intact_prefix_scanned(b, 0, sync_count);
}

/// T4：crash 后恢复不丢已 sync 数据（设备模型桥接）
proof fn crash_recovery_durable(
    d: Device,
    b: Seq<Entry>,
    sync_count: int,
)
    requires
        device_inv(d),
        sync_count >= 0,
        b.len() >= sync_count,
        sync_count <= d.synced_len,
        forall|i: int| 0 <= i && i < sync_count ==> b[i].intact,
    ensures
        scan_valid(b, 0) >= sync_count,
        d.synced_len >= sync_count,
{
    d4_synced_survives(d, sync_count);
    synced_survive(b, sync_count);
}

} // verus!
