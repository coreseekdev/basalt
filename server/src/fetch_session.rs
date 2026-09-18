//! Fetch session（KIP-227 增量 fetch，T-M4.2 尾项）。
//!
//! 会话缓存：epoch 0 = 建会话（全量 fetch），≥1 = 增量（服务端记住注册
//! 分区集，未列分区被动监视），-1 = 关会话。增量响应省略「空且无错」的
//! 分区——长轮询下空闲分区不再占响应字节。v1 边界：请求外的注册分区仅
//! 协议记账（Java/librdkafka 每轮重发全部分区，行为等价）；HW 变更条目
//! 不回填（客户端 HW 时效性仅影响滞后指标，不影响正确性）。
//!
//! 兼容性：session_id=0 = 无会话（既有行为的显式化）；未知会话/epoch 错
//! 误回响应级 70/71 + 空 Responses，客户端按 KIP-227 重建会话。

use std::collections::HashMap;
use std::time::{Duration, Instant};

const MAX_SESSIONS: usize = 1000;
const IDLE_EVICT: Duration = Duration::from_secs(120);

/// 注册分区（会话内记账；v1 仅协议面，被动监视）
#[derive(Debug, Clone)]
pub struct Registered {
    pub offset: i64,
    pub max_bytes: usize,
}

struct Session {
    /// 最近接受的 epoch（客户端下次发 epoch+1；同 epoch 重试合法）
    epoch: i32,
    last_seen: Instant,
    /// (topic name, partition) → 注册态
    parts: HashMap<(String, i32), Registered>,
    /// topic_id → name（v13+ 遗忘条目仅带 id；全量 fetch 学习）
    id_to_name: HashMap<u128, String>,
}

#[derive(Debug)]
pub enum SessionOutcome {
    /// 会话有效：serve 请求分区；session_id 回给响应
    Ok { session_id: i32 },
    /// 未知会话（响应级 70 + session_id 0 + 空 Responses；客户端重建）
    Unknown,
    /// epoch 不匹配（响应级 71 + session_id 0 + 空 Responses）
    InvalidEpoch,
}

/// 遗忘条目（v7-12 带 topic 名；v13+ 仅 TopicId——按已学习映射尽力删）
#[derive(Debug, Clone)]
pub struct Forgotten {
    pub topic: Option<String>,
    pub topic_id: Option<u128>,
    pub partitions: Vec<i32>,
}

static SESSIONS: std::sync::Mutex<Option<Cache>> = std::sync::Mutex::new(None);

struct Cache {
    next_id: i32,
    map: HashMap<i32, Session>,
}

fn cache() -> std::sync::MutexGuard<'static, Option<Cache>> {
    SESSIONS.lock().unwrap()
}

/// epoch 0（建会话）：注册全部分区，返回新 session_id（缓存满 → 0 = 无会话，
/// 客户端按无会话继续工作——优雅降级）。targets = (name, topic_id, partition,
/// offset, max_bytes)
pub fn begin(targets: &[(String, u128, i32, i64, usize)]) -> i32 {
    let mut guard = cache();
    let c = cache_mut(&mut guard);
    sweep(c);
    if c.map.len() >= MAX_SESSIONS {
        return 0;
    }
    // id 非零循环取号
    let mut id = c.next_id;
    while id == 0 || c.map.contains_key(&id) {
        id = id.wrapping_add(1);
    }
    c.next_id = id.wrapping_add(1);
    let mut sess = Session {
        epoch: 0,
        last_seen: Instant::now(),
        parts: HashMap::new(),
        id_to_name: HashMap::new(),
    };
    learn_and_register(&mut sess, targets);
    c.map.insert(id, sess);
    id
}

fn learn_and_register(sess: &mut Session, targets: &[(String, u128, i32, i64, usize)]) {
    for (t, tid, p, o, m) in targets {
        if *tid != 0 {
            sess.id_to_name.insert(*tid, t.clone());
        }
        sess.parts.insert((t.clone(), *p), Registered { offset: *o, max_bytes: *m });
    }
}

/// epoch ≥1（增量）：校验会话与 epoch，合并请求分区/删除遗忘分区。
/// 返回会话有效性（调用方据以决定全量服务或 70/71 错误响应）。
pub fn incremental(session_id: i32, epoch: i32, targets: &[(String, u128, i32, i64, usize)], forgotten: &[Forgotten]) -> SessionOutcome {
    let mut guard = cache();
    let c = cache_mut(&mut guard);
    sweep(c);
    let Some(sess) = c.map.get_mut(&session_id) else {
        return SessionOutcome::Unknown;
    };
    // 同 epoch 重试合法（响应丢失后的重发）；跨 epoch 只接受 +1 顺序推进
    if epoch != sess.epoch && epoch != sess.epoch + 1 {
        return SessionOutcome::InvalidEpoch;
    }
    for f in forgotten {
        for p in &f.partitions {
            if let Some(t) = &f.topic {
                sess.parts.remove(&(t.clone(), *p));
            } else if let Some(id) = f.topic_id {
                // 仅 TopicId（v13+）：按已学习映射删；未知 id 忽略
                if let Some(t) = sess.id_to_name.get(&id).cloned() {
                    sess.parts.remove(&(t, *p));
                }
            }
        }
    }
    learn_and_register(sess, targets);
    sess.epoch = epoch;
    sess.last_seen = Instant::now();
    SessionOutcome::Ok { session_id }
}

/// epoch -1（关闭会话）
pub fn close(session_id: i32) {
    if let Some(c) = cache().as_mut() {
        c.map.remove(&session_id);
    }
}

/// 空闲淘汰（begin/incremental 惰性触发）
fn sweep(c: &mut Cache) {
    let now = Instant::now();
    let expired: Vec<i32> = c
        .map
        .iter()
        .filter(|(_, s)| now.duration_since(s.last_seen) > IDLE_EVICT)
        .map(|(id, _)| *id)
        .collect();
    for id in expired {
        c.map.remove(&id);
    }
}

/// 会话数（观测/测试）
pub fn session_count() -> usize {
    cache().as_ref().map(|c| c.map.len()).unwrap_or(0)
}

fn cache_mut(g: &mut Option<Cache>) -> &mut Cache {
    g.get_or_insert_with(|| Cache { next_id: 1, map: HashMap::new() })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tgts(base: i64) -> Vec<(String, u128, i32, i64, usize)> {
        vec![("t".into(), 7, 0, base, 1024), ("t".into(), 7, 1, base, 1024)]
    }

    #[test]
    fn begin_incremental_forget_close_lifecycle() {
        let id = begin(&tgts(0));
        assert_ne!(id, 0, "建会话");
        // 增量 +1：合并（p1 offset 变化）+ 遗忘 p1
        let out = incremental(id, 1, &[("t".into(), 7, 0, 50, 1024)],
                              &[Forgotten { topic: None, topic_id: Some(7), partitions: vec![1] }]);
        assert!(matches!(out, SessionOutcome::Ok { .. }));
        // 同 epoch 重试合法
        assert!(matches!(incremental(id, 1, &tgts(50), &[]), SessionOutcome::Ok { .. }));
        // 跳跃 epoch 拒绝
        assert!(matches!(incremental(id, 5, &tgts(50), &[]), SessionOutcome::InvalidEpoch));
        // 顺序推进合法
        assert!(matches!(incremental(id, 2, &tgts(60), &[]), SessionOutcome::Ok { .. }));
        // 关闭后未知
        close(id);
        assert!(matches!(incremental(id, 3, &tgts(60), &[]), SessionOutcome::Unknown));
        // 会话总数断言不放此处——并行测试共享进程级缓存（唯一性由 id
        // 断言与 Unknown 结果承载）
    }

    #[test]
    fn unknown_session_and_epoch_guard() {
        assert!(matches!(incremental(98_765_432, 1, &tgts(0), &[]), SessionOutcome::Unknown));
        let id = begin(&tgts(0));
        // 直接从 0 跳到 2（不合法——必须 +1）
        assert!(matches!(incremental(id, 2, &tgts(0), &[]), SessionOutcome::InvalidEpoch));
        close(id);
    }

    #[test]
    fn session_ids_unique_nonzero() {
        let mut ids = Vec::new();
        for _ in 0..10 {
            let id = begin(&tgts(0));
            assert_ne!(id, 0);
            ids.push(id);
        }
        let uniq: std::collections::BTreeSet<i32> = ids.iter().copied().collect();
        assert_eq!(uniq.len(), 10);
        for id in ids {
            close(id);
        }
    }
}
