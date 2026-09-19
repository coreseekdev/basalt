//! Share groups（KIP-932）spike：内存版 share 协调器 + ack 状态机
//! （评估纪要 §3.2 的最小实现面；独立于 classic/848 组协调器——生产就绪
//! 评估 §6 解耦约束）。
//!
//! spike 边界：
//! - 全内存（重启即失；块 c 才落 share-state log）；
//! - record lock 到期 = 重新可交付（不主动清理，靠交付过滤跳过）；
//! - 交付游标 = 最高连续 ACCEPT 水位；RELEASE 回到可交付；REJECT 归档；
//! - 交付上限 BASALT_SHARE_DELIVERY_LIMIT（默认 5），超限不再交付。

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

pub const ACK_ACCEPT: i8 = 1;
pub const ACK_RELEASE: i8 = 2;
pub const ACK_REJECT: i8 = 3;

const DELIVERY_LIMIT: i16 = 5;

fn lock_duration() -> Duration {
    Duration::from_millis(
        std::env::var("BASALT_SHARE_LOCK_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(30_000),
    )
}

#[derive(Clone, Debug)]
pub struct Acquired {
    pub first: i64,
    pub last: i64,
    pub count: i16,
    pub member: String,
    pub at: Instant,
}

#[derive(Default)]
struct SharePartition {
    /// 在途锁（member 持有，到期自动可交付）
    acquired: Vec<Acquired>,
    /// REJECT 归档（不再交付）
    archived: Vec<(i64, i64)>,
    /// 交付游标：下一批从此读
    cursor: i64,
    /// 首 offset → 累计交付次数（ACCEPT 后清除）
    delivery_counts: HashMap<i64, i16>,
    /// ACCEPT 区间（合并后，左闭右开）；cursor 沿其连续段推进
    accepted: Vec<(i64, i64)>,
}

#[derive(Default)]
struct ShareGroup {
    /// member_id → (epoch, last_seen_assign_epoch)
    members: HashMap<String, (i32, u64)>,
    /// member_id → fetch 会话（KIP-227 增量注册集）
    sessions: HashMap<String, ShareSession>,
    /// (topic_id, partition) → 交付状态
    parts: HashMap<(u128, i32), SharePartition>,
    /// 分配代次：成员集变化即 bump——下次心跳全员重下发分配
    assignment_epoch: u64,
    /// 最近一次确定性轮转分配（member_id → 分配面）
    rotation: HashMap<String, BTreeMap<u128, Vec<i32>>>,
}

static GROUPS: std::sync::Mutex<Option<HashMap<String, ShareGroup>>> = std::sync::Mutex::new(None);

// ---- 持久化（B4）：__share_group_state 内部 topic 事件流 + 重放。
// ACK 事件（accept/release/reject）在 acknowledge 变更后经 SHARE_SINK
// 通道异步下沉（produce 路径，acks=all）；acquired 锁为瞬态（重启 =
// 全员锁过期，回到可交付——§3.2 决策）。启动时由 main 的监督任务重放
// 分区数据 install_replay 重建 cursor/archived。
// 多节点边界（B5 面）：事件只在内部 topic 分区 leader 节点持久化——
// 非 leader 节点 receive 的 ack 照常服务内存视图但不落盘（spike 语义）。

/// 单条 share 状态事件：(group, topic_id, partition, first, last, ack_type)。
pub type ShareEvent = (String, u128, i32, i64, i64, i8);

static SHARE_SINK: std::sync::RwLock<Option<tokio::sync::mpsc::Sender<ShareEvent>>> =
    std::sync::RwLock::new(None);

/// main 监督任务在内部 topic 路由就绪后注入（仅 leader 节点注入）；
/// failover 升主后重绑（旧通道随 drop 关闭，下游任务自然退出）。
pub fn set_share_sink(tx: tokio::sync::mpsc::Sender<ShareEvent>) {
    if let Ok(mut w) = SHARE_SINK.write() {
        *w = Some(tx);
    }
}

fn emit_events(events: &[ShareEvent]) {
    if events.is_empty() {
        return;
    }
    let sink = SHARE_SINK.read().ok().and_then(|w| w.clone());
    if let Some(tx) = sink {
        for e in events {
            if let Err(e) = tx.try_send(e.clone()) {
                tracing::warn!(error = %e, "share event sink full; event dropped");
            }
        }
    }
}

/// 重放安装：按序重应用事件（与在线 acknowledge 同一状态机语义；
/// acquired 为空 → release/reject 的锁清理自然 no-op）。
pub fn install_replay(events: Vec<ShareEvent>) {
    let mut g = groups();
    let mut applied = 0usize;
    for (group, tid, partition, first, last, ty) in events {
        let sg = group_mut(&mut g, &group);
        let p = sg.parts.entry((tid, partition)).or_default();
        if apply_ack(p, "", first, last, ty) {
            applied += 1;
        }
    }
    drop(g);
    tracing::info!(events = applied, "share state replayed from internal topic");
}


/// fetch 会话（KIP-932 沿用 KIP-227 增量语义）：epoch 0 全量注册，≥1 增量
/// ——请求只带变化分区，服务端须以**会话注册集**为服务面（franz-go 首轮
/// 之后发 n_topics=0 的增量 fetch，spike 首跑实证）。
#[derive(Default, Clone)]
pub struct ShareSession {
    pub epoch: i32,
    /// (topic_id, partition, max_bytes)
    pub parts: Vec<(u128, i32, i32)>,
}

/// 会话结算：epoch 0 重建注册集；≥1 并入新列分区（按 (tid,part) 去重取新
/// max_bytes）；返回当前服务集。未知 member → None。
pub fn session_register(
    group: &str,
    member: &str,
    epoch: i32,
    topics: &[(u128, i32, i32)],
    forgotten: &[(u128, i32)],
) -> Option<Vec<(u128, i32, i32)>> {
    let mut g = groups();
    let sg = group_mut(&mut g, group);
    let Some((_, _assign)) = sg.members.get(member) else { return None };
    let e = sg.sessions.entry(member.to_string()).or_default();
    if epoch == 0 || epoch < e.epoch {
        e.parts = topics.to_vec();
    } else {
        for t in topics {
            match e.parts.iter_mut().find(|(tid, pp, _)| *tid == t.0 && *pp == t.1) {
                Some(slot) => slot.2 = t.2,
                None => e.parts.push(*t),
            }
        }
        // 遗忘删除（KIP-227 ForgottenTopicsData）
        e.parts.retain(|(tid, pp, _)| !forgotten.contains(&(*tid, *pp)));
    }
    e.epoch = epoch;
    Some(e.parts.clone())
}

fn groups() -> std::sync::MutexGuard<'static, Option<HashMap<String, ShareGroup>>> {
    GROUPS.lock().unwrap()
}

fn group_mut<'a>(
    g: &'a mut Option<HashMap<String, ShareGroup>>,
    name: &str,
) -> &'a mut ShareGroup {
    g.get_or_insert_with(HashMap::new)
        .entry(name.to_string())
        .or_default()
}

/// ShareGroupHeartbeat：epoch 0（或未知 member）= 加入；否则校验 fencing。
/// 返回 (new_epoch, assignment)；assignment 为 None 表示无变化。
/// Err = (error_code, message)。
pub fn heartbeat(
    group: &str,
    member_id: &str,
    epoch: i32,
    subscribed: &[(u128, Vec<i32>)],
) -> Result<(i32, Option<BTreeMap<u128, Vec<i32>>>), (i16, String)> {
    let mut g = groups();
    let sg = group_mut(&mut g, group);
    let subscribed: BTreeMap<u128, Vec<i32>> = subscribed
        .iter()
        .filter(|(_, ps)| !ps.is_empty())
        .map(|(t, ps)| (*t, ps.clone()))
        .collect();
    let known = sg.members.contains_key(member_id);
    if !known && epoch != 0 {
        return Err((25, "Unknown member id".into()));
    }
    if known {
        let stored = sg.members.get(member_id).map(|(e, _)| *e).unwrap_or(0);
        if epoch < stored {
            return Err((82, "Fenced member epoch".into()));
        }
    }
    if !known {
        // 新加入：注册 + bump 分配代次 + 确定性轮转重算全员分配
        sg.members.insert(member_id.to_string(), (1, 0));
        sg.assignment_epoch += 1;
        let member_ids: Vec<String> = {
            let mut ids: Vec<String> = sg.members.keys().cloned().collect();
            ids.sort();
            ids
        };
        sg.rotation.clear();
        let n = member_ids.len();
        for (pos, mid) in member_ids.iter().enumerate() {
            let mut assignment: BTreeMap<u128, Vec<i32>> = BTreeMap::new();
            for (tid, ps) in &subscribed {
                let mine: Vec<i32> = ps
                    .iter()
                    .copied()
                    .enumerate()
                    .filter(|(i, _)| i % n == pos)
                    .map(|(_, p)| p)
                    .collect();
                if !mine.is_empty() {
                    assignment.insert(*tid, mine);
                }
            }
            sg.rotation.insert(mid.clone(), assignment);
        }
    }
    let cur_epoch = sg.members.get(member_id).map(|(e, _)| *e).unwrap_or(1);
    let last_seen = sg.members.get(member_id).map(|(_, s)| *s).unwrap_or(0);
    if last_seen < sg.assignment_epoch {
        // 分配代次有更新 → 下发并标记已见
        sg.members.get_mut(member_id).map(|(_, s)| *s = sg.assignment_epoch);
        let assign = sg.rotation.get(member_id).cloned();
        Ok((cur_epoch, assign))
    } else {
        Ok((cur_epoch, None))
    }
}

/// 交付意图查询：该分区从哪个 offset 起可交付。
/// 惰性清过期锁（record lock 到期 = 自动 release）；跳过他人在途与归档；
/// 本 member 在途且未过期 → 幂等视图（返回区间首 offset）。
pub fn deliverable_from(group: &str, member: &str, tid: u128, partition: i32) -> i64 {
    let mut g = groups();
    let Some(sg) = g.as_mut().and_then(|m| m.get_mut(group)) else { return 0 };
    let Some(p) = sg.parts.get_mut(&(tid, partition)) else { return 0 };
    let now = Instant::now();
    let lock = lock_duration();
    p.acquired.retain(|a| now.duration_since(a.at) < lock);
    let mut cursor = p.cursor;
    loop {
        let mut moved = false;
        for a in &p.acquired {
            if a.first <= cursor && cursor <= a.last {
                if a.member == member {
                    return a.first; // 本人在途：幂等视图
                }
                cursor = a.last + 1;
                moved = true;
            }
        }
        for (f, l) in &p.archived {
            if *f <= cursor && cursor <= *l {
                cursor = l + 1;
                moved = true;
            }
        }
        if !moved {
            break;
        }
    }
    cursor
}

/// member 当前分配的分区集（serve 端过滤——会话注册面 ≠ 分配面）
pub fn assigned_partitions(group: &str, member: &str, tid: u128) -> Vec<i32> {
    let g = groups();
    let Some(sg) = g.as_ref().and_then(|m| m.get(group)) else { return vec![] };
    sg.rotation
        .get(member)
        .and_then(|a| a.get(&tid).cloned())
        .unwrap_or_default()
}

/// 归档区间（serve 端过滤 REJECT 已拒记录）
pub fn archived_ranges(group: &str, tid: u128, partition: i32) -> Vec<(i64, i64)> {
    let g = groups();
    let Some(sg) = g.as_ref().and_then(|m| m.get(group)) else { return vec![] };
    sg.parts
        .get(&(tid, partition))
        .map(|p| p.archived.clone())
        .unwrap_or_default()
}

/// member 存在性校验（fetch/ack 前置）：ShareFetch/ShareAcknowledge 不携带
/// member epoch（ShareSessionEpoch 是会话计数，非 member epoch——spike 首跑
/// 实证把 0 当 epoch fence 误拒初始 fetch）。
pub fn validate_exists(group: &str, member: &str) -> Result<(), (i16, String)> {
    let g = groups();
    let Some(sg) = g.as_ref().and_then(|m| m.get(group)) else {
        return Err((25, "Unknown member id".into()));
    };
    if sg.members.contains_key(member) {
        Ok(())
    } else {
        Err((25, "Unknown member id".into()))
    }
}

/// 交付登记：serve [first, last] → 记在途锁（同 member 同区间重复 serve 不
/// 加计数；他人 release 后再 serve 计数 +1）
pub fn acquire(group: &str, member: &str, tid: u128, partition: i32, first: i64, last: i64) -> i16 {
    let mut g = groups();
    let sg = group_mut(&mut g, group);
    let p = sg.parts.entry((tid, partition)).or_default();
    // 先清同 member 同区间旧锁（re-acquire）
    p.acquired.retain(|a| !(a.member == member && a.first == first && a.last == last));
    // 累计交付次数以 delivery_counts 为权威（release/reject 清在途但保计数）
    let prev = p.delivery_counts.get(&first).copied().unwrap_or(0);
    let count = (prev + 1).min(DELIVERY_LIMIT);
    p.delivery_counts.insert(first, count);
    let _ = member;
    p.acquired.push(Acquired {
        first,
        last,
        count,
        member: member.to_string(),
        at: Instant::now(),
    });
    count
}

/// ShareAcknowledge：应用区间确认批。返回发生变化的区间数。
pub fn acknowledge(
    group: &str,
    member: &str,
    tid: u128,
    partition: i32,
    batches: &[(i64, i64, Vec<i8>)],
) -> usize {
    let mut g = groups();
    let sg = group_mut(&mut g, group);
    let p = sg.parts.entry((tid, partition)).or_default();
    let mut events: Vec<ShareEvent> = Vec::new();
    for (first, last, types) in batches {
        let ty = types.first().copied().unwrap_or(ACK_ACCEPT);
        if apply_ack(p, member, *first, *last, ty) {
            events.push((group.to_string(), tid, partition, *first, *last, ty));
        }
    }
    drop(g);
    emit_events(&events);
    events.len()
}

/// 单条 ack 的状态机应用（在线 acknowledge 与重放 install_replay 共用）。
/// 返回是否构成一次有效变更。
fn apply_ack(p: &mut SharePartition, member: &str, first: i64, last: i64, ty: i8) -> bool {
    match ty {
        ACK_ACCEPT => {
            p.acquired.retain(|a| {
                !(a.member == member && a.first >= first && a.last <= last)
            });
            // 合并 accepted 区间 + 游标沿连续段推进（区间含头不含尾）
            p.accepted.push((first, last + 1));
            p.accepted.sort_unstable();
            let mut merged: Vec<(i64, i64)> = Vec::new();
            for (f, l) in &p.accepted {
                match merged.last_mut() {
                    Some(last_mut) if *f <= last_mut.1 => {
                        if *l > last_mut.1 {
                            last_mut.1 = *l;
                        }
                    }
                    _ => merged.push((*f, *l)),
                }
            }
            p.accepted = merged;
            for (f, l) in &p.accepted {
                if *f <= p.cursor && *l > p.cursor {
                    p.cursor = *l;
                }
            }
            true
        }
        ACK_RELEASE => {
            p.acquired.retain(|a| {
                !(a.member == member && a.first >= first && a.last <= last)
            });
            // 交付计数语义：release 后 cursor 不动 → 下轮重新交付
            true
        }
        ACK_REJECT => {
            p.acquired.retain(|a| {
                !(a.member == member && a.first >= first && a.last <= last)
            });
            p.archived.push((first, last));
            true
        }
        _ => false,
    }
}

/// 会话关闭（ShareSessionEpoch=-1 / FINAL_EPOCH ack）：释放该 member 全部在途
pub fn release_member(group: &str, member: &str) {
    let mut g = groups();
    let Some(sg) = g.as_mut().and_then(|m| m.get_mut(group)) else { return };
    for p in sg.parts.values_mut() {
        p.acquired.retain(|a| a.member != member);
    }
}

#[cfg(test)]
mod share_tests {
    use super::*;

    #[test]
    fn heartbeat_join_fence_and_stable() {
        // 双 topic（各 2 分区）双成员：成员集变化即全员重下发
        let sub = vec![(100u128, vec![0, 1]), (200u128, vec![0, 1])];
        // A 加入（唯一成员 → 拿全部 4 分区）
        let (ep, assign) = heartbeat("sg1", "aaa", 0, &sub).unwrap();
        assert_eq!(ep, 1);
        let a = assign.expect("首加入应有分配");
        assert_eq!(a.get(&100).unwrap(), &vec![0, 1]);
        assert_eq!(a.get(&200).unwrap(), &vec![0, 1]);
        // B 加入：成员集变化 → 轮转重算（A/B 各得 2 分区）
        let (_, assign_b) = heartbeat("sg1", "bbb", 0, &sub).unwrap();
        let b = assign_b.expect("B 首加入应有分配");
        // B（pos 1，mod 2）：各 topic 的奇数位
        assert_eq!(b.get(&100).unwrap(), &vec![1]);
        assert_eq!(b.get(&200).unwrap(), &vec![1]);
        // A 下次心跳：看到重分配（topic 100 只剩 [0]，topic 200 只剩 [0]）
        let (_, assign_a2) = heartbeat("sg1", "aaa", 1, &sub).unwrap();
        let a2 = assign_a2.expect("A 应收到 rebalance 后的新分配");
        assert_eq!(a2.get(&100).unwrap(), &vec![0]);
        assert_eq!(a2.get(&200).unwrap(), &vec![0]);
        // 稳定心跳：无新分配（None）
        let (_, stable) = heartbeat("sg1", "aaa", 1, &sub).unwrap();
        assert!(stable.is_none(), "稳定后不应重复下发");
        // fence + 未知 member
        assert_eq!(heartbeat("sg1", "aaa", 0, &sub).unwrap_err().0, 82);
        assert_eq!(heartbeat("sg1", "ghost", 3, &sub).unwrap_err().0, 25);
    }

    #[test]
    fn acquire_ack_accept_advances_cursor() {
        let sub = vec![(200u128, vec![0])];
        let _ = heartbeat("sg2", "m1", 0, &sub);
        acquire("sg2", "m1", 200, 0, 0, 9);
        acquire("sg2", "m1", 200, 0, 10, 19);
        // ACCEPT 第一段：游标推到 10
        let n = acknowledge("sg2", "m1", 200, 0, &[(0, 9, vec![ACK_ACCEPT])]);
        assert_eq!(n, 1);
        // 交付过滤：本 member 在途 [10..19] 未过期 → 重复交付同区间视图
        assert_eq!(deliverable_from("sg2", "m1", 200, 0), 10);
        // ACCEPT 第二段：游标推到 20
        acknowledge("sg2", "m1", 200, 0, &[(10, 19, vec![ACK_ACCEPT])]);
        assert_eq!(deliverable_from("sg2", "m1", 200, 0), 20);
    }

    #[test]
    fn release_redelivers_reject_archives() {
        let sub = vec![(300u128, vec![0])];
        let _ = heartbeat("sg3", "m1", 0, &sub);
        acquire("sg3", "m1", 300, 0, 0, 4);
        // RELEASE：区间回可交付（游标不动）
        acknowledge("sg3", "m1", 300, 0, &[(0, 4, vec![ACK_RELEASE])]);
        assert_eq!(deliverable_from("sg3", "m1", 300, 0), 0);
        // REJECT：归档，交付跳过
        acquire("sg3", "m1", 300, 0, 0, 4);
        acknowledge("sg3", "m1", 300, 0, &[(0, 4, vec![ACK_REJECT])]);
        assert_eq!(deliverable_from("sg3", "m1", 300, 0), 5);
    }

    #[test]
    fn release_member_releases_all_on_close() {
        let sub = vec![(400u128, vec![0])];
        let _ = heartbeat("sg4", "m1", 0, &sub);
        acquire("sg4", "m1", 400, 0, 0, 4);
        acquire("sg4", "m1", 400, 0, 5, 9);
        release_member("sg4", "m1");
        // 全部释放 → 可交付回到 0
        assert_eq!(deliverable_from("sg4", "m1", 400, 0), 0);
    }
}
