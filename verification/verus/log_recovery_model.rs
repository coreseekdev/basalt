//! C7 恢复安全性模型——scan_and_truncate 抽象正确性
//! 运行：verus --crate-type=lib log_recovery_model.rs
//!
//! 已验证（10/10）：设备模型 D1-D4 + scan_valid 定义 + scan_len_bounded
//! WIP：synced_prefix_is_valid / synced_survive 归纳引理（递归 spec fn
//! 展开的 SMT 触发需进一步 proof 工程——见 git log 中间态）

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

// WIP：synced_prefix_is_valid / synced_survive 归纳引理待补
// （递归 spec fn 展开的 SMT 触发需进一步 proof 工程——已尝试
//   显式 assert 链 / forall 传递 / IH 引用，均因 Verus proof
//   工程限制失败。修法：改用 Map<int, u8> 建模或分步 assert。）

} // verus!
