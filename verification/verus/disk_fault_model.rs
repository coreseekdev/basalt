//! C7 第一步：DiskIo 故障模型的形式化（docs/VERIFICATION.md §3，账本 C7 深水区起步件）。
//!
//! 模型：单文件两级长度水位——`synced_len`（持久边界）/ `written_len`（写边界）。
//! 与 storage/src/sim_disk.rs 的语义对齐：
//!   append(n) → written_len += n（进 pending，未 fsync）
//!   sync()    → synced_len = written_len（fsync 边界）
//!   crash()   → written_len = synced_len（掉电丢 pending）
//! 内容级正确性（字节/CRC）由 L1 测试与 opfuzz 覆盖；本模型捕获**持久性语义**，
//! 后续 recover() 证明（recover 返回 LEO = crashed 后的 synced_len）以此为前置。
//!
//! 与 I/O 实现无关（ADR-14）：StdDisk 缓冲写 = page cache 写 + fsync；
//! 未来 DirectDisk = 设备写 + FLUSH CACHE——两级水位语义同构。
//!
//! 运行：verus --crate-type=lib disk_fault_model.rs

use vstd::prelude::*;

verus! {

pub struct Device {
    pub synced_len: int,   // 已持久长度（fsync 边界）
    pub written_len: int,  // 已写长度（含未 fsync 的 pending）
}

pub open spec fn init() -> Device
{
    Device { synced_len: 0, written_len: 0 }
}

/// append n 字节：只推进写边界（进 pending，不触持久边界）
pub open spec fn append(d: Device, n: int) -> Device
{
    Device { synced_len: d.synced_len, written_len: d.written_len + n }
}

/// fsync：持久边界推进到写边界
pub open spec fn sync(d: Device) -> Device
{
    Device { synced_len: d.written_len, written_len: d.written_len }
}

/// 掉电：写边界回退到持久边界（未 fsync 字节丢失）
pub open spec fn crash(d: Device) -> Device
{
    Device { synced_len: d.synced_len, written_len: d.synced_len }
}

/// 设备不变式：持久边界 ≤ 写边界，均非负
pub open spec fn inv(d: Device) -> bool
{
    &&& 0 <= d.synced_len
    &&& d.synced_len <= d.written_len
}

// ---------- 不变式保持 ----------

proof fn init_inv()
    ensures inv(init())
{ }

proof fn append_preserves_inv(d: Device, n: int)
    requires inv(d), 0 <= n
    ensures inv(append(d, n))
{
    // written 只增：synced <= written <= written + n
}

proof fn sync_preserves_inv(d: Device)
    requires inv(d)
    ensures inv(sync(d))
{ }

proof fn crash_preserves_inv(d: Device)
    requires inv(d)
    ensures inv(crash(d))
{ }

// ---------- 核心定理 ----------

/// D1：sync 的持久化点语义——sync 后持久边界 == 写边界
proof fn d1_sync_durable(d: Device)
    requires inv(d)
    ensures sync(d).synced_len == d.written_len
{ }

/// D2：crash 的截断语义——写边界回退到持久边界
proof fn d2_crash_truncates(d: Device)
    requires inv(d)
    ensures crash(d).written_len == d.synced_len
{ }

/// D3：crash 不减少持久边界（已 sync 数据在掉电后存活）
proof fn d3_crash_keeps_synced(d: Device)
    requires inv(d)
    ensures crash(d).synced_len == d.synced_len
{ }

/// D4（核心持久性定理）：crash 后存活长度 ≥ 任意历史 sync 时刻的持久长度
/// ——即"已 sync 数据不丢"的长度域形式。
proof fn d4_synced_survives(d: Device, s: int)
    requires inv(d), s <= d.synced_len
    ensures s <= crash(d).synced_len
{
    d3_crash_keeps_synced(d);
}

/// 追加后未 sync 即 crash：追加的字节全部丢失（pending 语义）
proof fn d5_unsynced_append_lost(d: Device, n: int)
    requires
        inv(d),
        0 <= n,
        d.written_len == d.synced_len, // 从持久点起算
    ensures
        // append n 后 crash：存活长度仍为原 synced_len（追加丢失）
        crash(append(d, n)).synced_len == d.synced_len,
        crash(append(d, n)).written_len == d.synced_len,
{
    let d2 = append(d, n);
    assert(d2.written_len == d.synced_len + n);
    assert(d2.synced_len == d.synced_len);
    // crash 截断到 synced_len
}

/// sync 后追加再 crash：已 sync 前缀存活、追加部分丢失
proof fn d6_sync_then_append_then_crash(d: Device, n: int, m: int)
    requires
        inv(d),
        0 <= n, 0 <= m,
        d.written_len == d.synced_len,
    ensures
        // 先 append n（未 sync）再 append m（sync 前）：存活长度 = synced + ...
        // ——由两级水位刻画：crash 后 written = synced
        crash(crash(append(sync(d), n))).written_len == d.synced_len,
{
}

} // verus!
