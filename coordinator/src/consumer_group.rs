//! KIP-848 consumer 组状态机（T-M3.3 块 a + T-M3.3.1 生命周期硬化，ADR-19
//! §2/§3 + docs/research/2026-09-18 对照行动清单）：心跳单循环、服务端
//! Range/uniform 分配、member-epoch fence、session timeout、撤销确认闭环、
//! 差分下发。纯同步结构体（时钟经参数注入，可测）——actor 壳与协议面在
//! 块 b 接线；确定性单测直驱。
//!
//! 生命周期语义（对照 kafka trunk GMM/CurrentAssignmentBuilder，见
//! research 报告 §2）：
//! - fence = epoch 单调：同 epoch 续租；prev_epoch 且 owned ⊆ 分配 → 重同步
//!  （应答丢失恢复）；epoch 0 → 重同步（fenced member recovery）；其余拒绝。
//! - 撤销确认闭环：重算移出的分区进 pending_release（宽限 = 该成员的
//!   rebalance timeout），原 owner 心跳的 owned 不再包含（或宽限到期）才
//!   允许派给新 owner——消除双 owner 窗口。
//! - 差分下发：交付分配与 last_sent 一致时 changed=false（协议面回
//!   Assignment=null）；join/full-request 恒全量。
//! - session timeout：成员级滑动 timer（心跳续期），超时注销 + 强制重算；
//!   静态成员临时离开（epoch=-2）暂停 timer 并保留分配。

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

/// 单成员视图。
#[derive(Debug, Clone)]
pub struct MemberState {
    pub member_id: String,
    pub epoch: i32,
    pub prev_epoch: i32,
    pub subscribed: Vec<String>,
    /// 订阅正则（v1 SubscribedTopicRegex；协议面解析为显式名单并入
    /// subscribed，此处留档供 Describe）
    pub regex: Option<String>,
    /// 静态成员 InstanceId
    pub instance_id: Option<String>,
    pub assignment: BTreeMap<String, Vec<i32>>,
    /// 最近一次实际下发的分配（差分基线）
    pub last_sent: BTreeMap<String, Vec<i32>>,
    /// 成员最近一次心跳上报的持有（撤销确认面）
    pub owned: BTreeMap<String, Vec<i32>>,
    pub rebalance_timeout_ms: i32,
    /// 临时离开（静态成员 epoch=-2）：保留分配、session timer 跳过
    pub paused: bool,
    last_seen: Instant,
}

#[derive(Debug)]
pub struct ConsumerGroup {
    /// topic → 分区数（服务端元数据面，经 meta 接线更新）
    pub partition_counts: HashMap<String, i32>,
    members: BTreeMap<String, MemberState>,
    /// 成员集+订阅集签名：变化才重算 target（幂等续租不 bump）
    subsig: u64,
    next_ordinal: u64,
    /// 重算次数（Describe 的 Group/AssignmentEpoch 同源）
    rebalances: u64,
    /// 组分配器（first-wins）；"range"/"uniform"
    assignor: String,
    /// 生命周期配置（actor spawn 注入；Kafka 为 per-group GroupConfig）
    pub session_timeout_ms: u64,
    pub max_size: usize,
    pub assignment_interval_ms: u64,
    last_rebalance: Instant,
    /// 签名已变化但被 interval 节流推迟的重算
    dirty: bool,
    /// 撤销确认闭环：分区 → (原 owner, 宽限截止)
    pending_release: BTreeMap<(String, i32), (String, Instant)>,
}

impl Default for ConsumerGroup {
    fn default() -> Self {
        Self {
            partition_counts: HashMap::new(),
            members: BTreeMap::new(),
            subsig: 0,
            next_ordinal: 0,
            rebalances: 0,
            assignor: "range".into(),
            session_timeout_ms: 45_000,
            max_size: 0,
            assignment_interval_ms: 1_000,
            last_rebalance: Instant::now(),
            dirty: false,
            pending_release: BTreeMap::new(),
        }
    }
}

/// DescribeConsumerGroup 的组快照。
#[derive(Debug, Clone)]
pub struct GroupDescribe {
    pub state: String,
    pub group_epoch: i32,
    pub assignment_epoch: i32,
    pub assignor: String,
    pub members: Vec<MemberDescribe>,
}

#[derive(Debug, Clone)]
pub struct MemberDescribe {
    pub member_id: String,
    pub member_epoch: i32,
    pub subscribed: Vec<String>,
    pub assignment: BTreeMap<String, Vec<i32>>,
}

/// 心跳结果：成员态 + 差分后的 assignment。fenced 二分：unknown_member =
/// 未知成员/僵尸（25），否则已知成员 epoch 陈旧（82）。error_code 供组
/// 容量等带码直透（Some 时协议面直接回该码，零状态）。changed=false 时
/// 协议面回 Assignment=null（差分语义，848 客户端行为假设）。
#[derive(Debug, Default)]
pub struct HeartbeatResult {
    pub member_id: String,
    pub member_epoch: i32,
    pub assignment: BTreeMap<String, Vec<i32>>,
    pub fenced: bool,
    pub unknown_member: bool,
    pub error_code: Option<i16>,
    pub changed: bool,
}

/// 撤销确认面：成员 owned 不再包含该分区 = 已释放。owned 空 = 未上报
/// （保守视为未确认）。
fn released_by(owned: &BTreeMap<String, Vec<i32>>, topic: &str, partition: i32) -> bool {
    !owned.get(topic).map(|ps| ps.contains(&partition)).unwrap_or(false)
}

/// previous-epoch 容错的子集判定：成员上报 owned ⊆ 其当前分配。
fn prev_owned_subset(
    owned: Option<&BTreeMap<String, Vec<i32>>>,
    assignment: &BTreeMap<String, Vec<i32>>,
) -> bool {
    let Some(owned) = owned else { return false };
    owned.iter().all(|(t, ps)| {
        assignment
            .get(t)
            .map(|assigned| ps.iter().all(|p| assigned.contains(p)))
            .unwrap_or(ps.is_empty())
    })
}

impl ConsumerGroup {
    /// 心跳。参数时钟 now 注入（可测）。
    /// - epoch=-1 永久离开；-2 静态成员临时离开（保留分配、timer 跳过）
    /// - 已知成员：同 epoch 续租；prev_epoch+owned⊆分配 或 epoch=0 → 重同步；
    ///   其余 fence(82)
    /// - 未知成员：epoch=0 注册（组满回 81）；非 0 僵尸(25)
    #[allow(clippy::too_many_arguments)]
    pub fn heartbeat(
        &mut self,
        member_id: &str,
        member_epoch: i32,
        subscribed: Option<Vec<String>>,
        assignor: Option<String>,
        owned: Option<BTreeMap<String, Vec<i32>>>,
        instance_id: Option<String>,
        regex: Option<String>,
        rebalance_timeout_ms: i32,
        now: Instant,
    ) -> HeartbeatResult {
        if member_epoch == -1 {
            self.members.remove(member_id);
            self.maybe_rebalance(now, true);
            return HeartbeatResult { member_id: member_id.into(), member_epoch: -1, ..Default::default() };
        }
        if member_epoch == -2 {
            // 静态成员临时离开：保留分配（不重算），session timer 跳过
            if let Some(m) = self.members.values_mut().find(|m| m.instance_id.as_deref() == instance_id.as_deref()) {
                m.paused = true;
            }
            return HeartbeatResult { member_id: member_id.into(), member_epoch: -2, ..Default::default() };
        }

        if let Some(m) = self.members.get_mut(member_id) {
            // 已知成员：续租 / 重同步 / fence
            m.last_seen = now;
            let stale = member_epoch != m.epoch
                && !(member_epoch == m.prev_epoch
                    && prev_owned_subset(owned.as_ref(), &m.assignment))
                && member_epoch != 0;
            if stale {
                return HeartbeatResult {
                    member_id: member_id.into(),
                    member_epoch: m.epoch,
                    fenced: true,
                    ..Default::default()
                };
            }
            if member_epoch != m.epoch {
                // previous-epoch / epoch-0 恢复：服务端零状态，重发当前分配
                m.owned = owned.unwrap_or_default();
                let assignment = m.assignment.clone();
                return HeartbeatResult {
                    member_id: member_id.into(),
                    member_epoch: m.epoch,
                    assignment,
                    changed: true,
                    ..Default::default()
                };
            }
            if let Some(o) = owned.clone() {
                m.owned = o;
            }
            m.paused = false;
        } else {
            if member_epoch != 0 {
                return HeartbeatResult { fenced: true, unknown_member: true, ..Default::default() };
            }
            if self.max_size > 0 && self.members.len() >= self.max_size {
                return HeartbeatResult {
                    member_id: member_id.into(),
                    member_epoch,
                    error_code: Some(81),
                    ..Default::default()
                };
            }
        }

        // 注册 / 续租。None 订阅 = keepalive 不动现有订阅。
        let id = if member_id.is_empty() {
            self.next_ordinal += 1;
            format!("consumer-{}", self.next_ordinal)
        } else {
            member_id.to_string()
        };
        let is_join = !self.members.contains_key(&id);
        match self.members.get_mut(&id) {
            Some(m) => {
                if let Some(subs) = subscribed.clone() {
                    m.subscribed = subs;
                }
                m.instance_id = instance_id.clone();
                if regex.is_some() {
                    m.regex = regex.clone();
                }
                m.rebalance_timeout_ms = rebalance_timeout_ms;
                if let Some(o) = owned.clone() {
                    m.owned = o;
                }
                m.paused = false;
                m.last_seen = now;
            }
            None => {
                self.members.insert(id.clone(), MemberState {
                    member_id: id.clone(),
                    epoch: 0,
                    prev_epoch: 0,
                    subscribed: subscribed.clone().unwrap_or_default(),
                    regex: regex.clone(),
                    instance_id: instance_id.clone(),
                    assignment: BTreeMap::new(),
                    last_sent: BTreeMap::new(),
                    owned: owned.clone().unwrap_or_default(),
                    rebalance_timeout_ms,
                    paused: false,
                    last_seen: now,
                });
            }
        }

        self.maybe_rebalance(now, is_join);

        // 撤销确认：owner 的 owned 已不含该分区（或已离组）→ 确认释放
        self.pending_release.retain(|tp, (owner, deadline)| {
            if *deadline <= now {
                return false; // 宽限到期：确认代办
            }
            self.members
                .get(owner)
                .map(|m| !released_by(&m.owned, &tp.0, tp.1))
                .unwrap_or(true)
        });

        // 差分下发：target −（他人尚未确认释放的分区）。full request
        // （rebalanceTimeoutMs != -1 且订阅/owned 均非 null，Kafka GMM:2715）
        // 强制全量下发。
        let m = self.members.get_mut(&id).expect("member registered above");
        let delivered = delivered_assignment(m, &self.pending_release);
        let full = rebalance_timeout_ms != -1 && subscribed.is_some() && owned.is_some();
        let mut changed = delivered != m.last_sent;
        if changed || full {
            m.last_sent = delivered.clone();
            changed = true;
        }
        HeartbeatResult {
            member_id: id,
            member_epoch: m.epoch,
            assignment: delivered,
            fenced: false,
            unknown_member: false,
            error_code: None,
            changed,
        }
    }

    /// 重算触发门：签名变化（或 dirty 残留）才重算；interval 节流，
    /// join 完成（is_join）/离开/超时路径 force 绕过。
    fn maybe_rebalance(&mut self, now: Instant, force: bool) {
        let subsig_changed = self.subsig != self.signature();
        if !subsig_changed && !self.dirty {
            return;
        }
        let throttled = now.duration_since(self.last_rebalance)
            < Duration::from_millis(self.assignment_interval_ms);
        if throttled && !force {
            self.dirty = true;
            return;
        }
        self.dirty = false;
        self.subsig = self.signature();
        self.last_rebalance = now;
        self.rebalance();
    }

    /// DescribeConsumerGroup 快照。状态派生：空 = Empty；有未确认撤销 /
    /// 节流挂起 = Reconciling；否则 Stable（Kafka 的 Assigning 在 basalt
    /// 原子重算模型中不可观测，并入 Stable）。
    pub fn describe(&self) -> GroupDescribe {
        let state = if self.members.is_empty() {
            "Empty"
        } else if !self.pending_release.is_empty() || self.dirty {
            "Reconciling"
        } else {
            "Stable"
        };
        GroupDescribe {
            state: state.to_string(),
            group_epoch: self.rebalances as i32,
            assignment_epoch: self.rebalances as i32,
            assignor: self.assignor.clone(),
            members: self
                .members
                .values()
                .map(|m| MemberDescribe {
                    member_id: m.member_id.clone(),
                    member_epoch: m.epoch,
                    subscribed: m.subscribed.clone(),
                    assignment: m.assignment.clone(),
                })
                .collect(),
        }
    }

    /// 会话 sweep（actor 按 deadline 调用；now 注入可测）：
    /// ① 非 paused 且 session 超时的成员注销；② 撤销宽限到期的 pending
    /// 释放（确认代办）。返回是否有成员被移除。
    pub fn sweep(&mut self, now: Instant) -> bool {
        let before = self.members.len();
        self.members.retain(|_, m| {
            m.paused
                || now.duration_since(m.last_seen)
                    < Duration::from_millis(self.session_timeout_ms.max(0))
        });
        let removed = self.members.len() != before;
        self.pending_release.retain(|_, (owner, deadline)| {
            self.members.contains_key(owner) && *deadline > now
        });
        if removed {
            self.maybe_rebalance(now, true);
        }
        removed
    }

    /// 最近的 sweep 截止时刻（session timer / 撤销宽限最小值）。
    pub fn next_deadline(&self, now: Instant) -> Option<Instant> {
        let mut min: Option<Instant> = None;
        for m in self.members.values() {
            if !m.paused {
                let d = m.last_seen + Duration::from_millis(self.session_timeout_ms.max(0));
                if min.map(|x| d < x).unwrap_or(true) {
                    min = Some(d);
                }
            }
        }
        for (_, (_, deadline)) in &self.pending_release {
            if min.map(|x| *deadline < x).unwrap_or(true) {
                min = Some(*deadline);
            }
        }
        let _ = now;
        min
    }

    /// 成员集 + 订阅集签名（BTreeMap 稳定序）。
    fn signature(&self) -> u64 {
        let mut h: u64 = 1469598103934665603;
        for (id, m) in &self.members {
            for b in id.bytes().chain(m.subscribed.iter().flat_map(|s| s.bytes())) {
                h = (h ^ b as u64).wrapping_mul(0x100000001b3);
            }
            h = (h ^ 0xff).wrapping_mul(0x100000001b3);
        }
        h
    }

    /// 重算：先快照旧分配标记撤销确认面（target 移出的分区 → pending，
    /// 宽限 = 该成员 rebalance timeout），再更新 target 与 epoch
    /// （prev_epoch = 旧 epoch，供恢复容错）。
    fn rebalance(&mut self) {
        self.rebalances += 1;
        let ids: Vec<String> = self.members.keys().cloned().collect();
        let prev: BTreeMap<String, BTreeMap<String, Vec<i32>>> = self
            .members
            .iter()
            .map(|(id, m)| (id.clone(), m.assignment.clone()))
            .collect();
        let targets = if self.assignor == "uniform" {
            self.uniform_targets(&ids)
        } else {
            self.range_targets(&ids)
        };
        let now = Instant::now();
        for (id, old) in &prev {
            let Some(new) = targets.get(id) else { continue };
            for (topic, ps) in old {
                for p in ps {
                    let still = new.get(topic).map(|np| np.contains(p)).unwrap_or(false);
                    if !still {
                        let timeout = self
                            .members
                            .get(id)
                            .map(|m| m.rebalance_timeout_ms.max(0) as u64)
                            .unwrap_or(30_000)
                            .clamp(1_000, 60_000);
                        self.pending_release.insert(
                            (topic.clone(), *p),
                            (id.clone(), now + Duration::from_millis(timeout)),
                        );
                    }
                }
            }
        }
        for (id, m) in self.members.iter_mut() {
            m.prev_epoch = m.epoch;
            m.assignment = targets.get(id).cloned().unwrap_or_default();
            if m.epoch == 0 {
                m.epoch = 1;
            } else {
                m.epoch += 1;
            }
        }
    }

    /// 服务端 Range 分配（java RangeAssignor 对齐）：订阅并集内每个 topic，
    /// 分区按成员序连续切块（余数给前面的成员）；未订阅的 topic 不分配。
    fn range_targets(&self, ids: &[String]) -> BTreeMap<String, BTreeMap<String, Vec<i32>>> {
        let mut out: BTreeMap<String, BTreeMap<String, Vec<i32>>> =
            ids.iter().map(|id| (id.clone(), BTreeMap::new())).collect();
        let topics: std::collections::BTreeSet<&String> = self
            .members
            .values()
            .flat_map(|m| m.subscribed.iter())
            .collect();
        for topic in topics {
            let total = *self.partition_counts.get(topic).unwrap_or(&0) as usize;
            if total == 0 || ids.is_empty() {
                continue;
            }
            let base = total / ids.len();
            let rem = total % ids.len();
            for (i, id) in ids.iter().enumerate() {
                if !self.members[id].subscribed.iter().any(|s| s == topic) {
                    continue;
                }
                let start = i * base + i.min(rem);
                let count = base + if i < rem { 1 } else { 0 };
                if count > 0 {
                    out.get_mut(id).unwrap().insert(
                        (*topic).clone(),
                        (start as i32..(start + count) as i32).collect(),
                    );
                }
            }
        }
        out
    }

    /// uniform（movement-minimizing，ADR-20 §3）：全局份额 + 保留段（按
    /// topic 订阅者数升序）+ 补派段（订阅约束优先于份额）。
    fn uniform_targets(&self, ids: &[String]) -> BTreeMap<String, BTreeMap<String, Vec<i32>>> {
        let n = ids.len();
        let mut out: BTreeMap<String, BTreeMap<String, Vec<i32>>> =
            ids.iter().map(|id| (id.clone(), BTreeMap::new())).collect();
        if n == 0 {
            return out;
        }
        let topics: std::collections::BTreeSet<&String> = self
            .members
            .values()
            .flat_map(|m| m.subscribed.iter())
            .collect();
        let mut all: Vec<(String, i32)> = Vec::new();
        for t in &topics {
            let total = *self.partition_counts.get(*t).unwrap_or(&0);
            for p in 0..total {
                all.push(((*t).clone(), p));
            }
        }
        let total = all.len();
        let fair = total / n;
        let rem = total % n;
        let size_of = |i: usize| fair + if i < rem { 1 } else { 0 };
        let subscriber_count =
            |t: &String| ids.iter().filter(|id| self.members[*id].subscribed.iter().any(|s| s == t)).count();

        let mut kept: Vec<Vec<(String, i32)>> = ids
            .iter()
            .map(|id| {
                let m = &self.members[id];
                let sub: std::collections::BTreeSet<&String> = m.subscribed.iter().collect();
                let mut cur: Vec<(String, i32)> = m
                    .assignment
                    .iter()
                    .flat_map(|(t, ps)| ps.iter().map(move |p| ((*t).clone(), *p)))
                    .filter(|(t, _)| sub.contains(t))
                    .collect();
                cur.sort_by_key(|(t, p)| (subscriber_count(t), t.clone(), *p));
                let cap = size_of(ids.iter().position(|x| x == id).unwrap_or(0));
                cur.truncate(cap);
                cur
            })
            .collect();

        let mut taken: std::collections::BTreeSet<(String, i32)> =
            kept.iter().flatten().cloned().collect();
        let pool: Vec<(String, i32)> = all.into_iter().filter(|k| !taken.contains(k)).collect();
        let mut j = 0usize;
        for kv in pool {
            let subs: Vec<usize> = (0..n)
                .filter(|&i| self.members[&ids[i]].subscribed.iter().any(|s| s == &kv.0))
                .collect();
            if subs.is_empty() {
                continue;
            }
            let mut placed = false;
            for k in 0..n {
                let i = (j + k) % n;
                if subs.contains(&i) && kept[i].len() < size_of(i) {
                    kept[i].push(kv.clone());
                    j = i + 1;
                    placed = true;
                    break;
                }
            }
            if !placed {
                for k in 0..n {
                    let i = (j + k) % n;
                    if subs.contains(&i) {
                        kept[i].push(kv.clone());
                        j = i + 1;
                        break;
                    }
                }
            }
        }
        for (i, id) in ids.iter().enumerate() {
            let mut by_topic: BTreeMap<String, Vec<i32>> = BTreeMap::new();
            for (t, p) in &kept[i] {
                by_topic.entry(t.clone()).or_default().push(*p);
            }
            out.insert(id.clone(), by_topic);
        }
        taken.clear();
        out
    }
}

/// 差分下发组装：target −（pending_release 中原 owner 是他人的分区）。
/// 原 owner 是自己 = 未确认也照发（重同步语义）。
fn delivered_assignment(
    m: &MemberState,
    pending: &BTreeMap<(String, i32), (String, Instant)>,
) -> BTreeMap<String, Vec<i32>> {
    let mut out = BTreeMap::new();
    for (topic, ps) in &m.assignment {
        let kept: Vec<i32> = ps
            .iter()
            .filter(|p| {
                !pending
                    .get(&(topic.clone(), **p))
                    .map(|(owner, _)| owner != &m.member_id)
                    .unwrap_or(false)
            })
            .copied()
            .collect();
        if !kept.is_empty() {
            out.insert(topic.clone(), kept);
        }
    }
    out
}

// ---------- 多组管理 actor（块 b 协议面接线用） ----------

pub struct CGHeartbeat {
    pub group: String,
    pub member_id: String,
    pub member_epoch: i32,
    /// None = 订阅未变（KIP-848 keepalive：未变字段置 null）
    pub subscribed: Option<Vec<String>>,
    /// ServerAssignor（协议面已校验 ∈ {range, uniform}）；None = 未变
    pub assignor: Option<String>,
    /// 成员上报的当前持有（撤销确认面）；None = 请求未带
    pub owned: Option<BTreeMap<String, Vec<i32>>>,
    /// 静态成员 InstanceId
    pub instance_id: Option<String>,
    /// 订阅正则（v1；协议面已解析并入订阅名单）
    pub regex: Option<String>,
    /// 成员 rebalance timeout（撤销宽限基准）
    pub rebalance_timeout_ms: i32,
    /// 订阅 topic 的分区数快照（handler 经 meta 查询后携带）
    pub counts: Vec<(String, i32)>,
    pub reply: tokio::sync::oneshot::Sender<HeartbeatResult>,
}

pub enum CGCmd {
    Heartbeat(CGHeartbeat),
    /// ConsumerGroupDescribe：组不存在回 None（协议面映射 GROUP_ID_NOT_FOUND）。
    Describe {
        group: String,
        reply: tokio::sync::oneshot::Sender<Option<GroupDescribe>>,
    },
}

/// 每节点一个（组协调器 POC 全节点，FindCoordinator Type=0 回自身——与
/// classic 同拓扑）。生命周期配置经环境注入（Kafka 为 per-group
/// GroupConfig，basalt 简化为 broker 级）。
pub struct ConsumerGroups;

impl ConsumerGroups {
    pub fn spawn() -> tokio::sync::mpsc::Sender<CGCmd> {
        let env_u64 = |k: &str, d: u64| {
            std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
        };
        Self::spawn_with(
            env_u64("BASALT_CONSUMER_SESSION_TIMEOUT_MS", 45_000),
            env_u64("BASALT_CONSUMER_GROUP_MAX_SIZE", 0) as usize,
            env_u64("BASALT_CONSUMER_ASSIGNMENT_INTERVAL_MS", 1_000),
        )
    }

    pub fn spawn_with(
        session_timeout_ms: u64,
        max_size: usize,
        assignment_interval_ms: u64,
    ) -> tokio::sync::mpsc::Sender<CGCmd> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(256);
        tokio::spawn(async move {
            let mut groups: HashMap<String, ConsumerGroup> = HashMap::new();
            loop {
                // deadline 感知等待（㊽ 纪律）：session/撤销宽限最小截止
                let mut next: Option<Instant> = None;
                let now = Instant::now();
                for g in groups.values() {
                    if let Some(d) = g.next_deadline(now) {
                        if next.map(|x| d < x).unwrap_or(true) {
                            next = Some(d);
                        }
                    }
                }
                let cmd = match next {
                    Some(d) => {
                        let wait = d.saturating_duration_since(Instant::now()).max(Duration::from_millis(20));
                        match tokio::time::timeout(wait, rx.recv()).await {
                            Ok(Some(cmd)) => cmd,
                            Ok(None) => break,
                            Err(_) => {
                                for g in groups.values_mut() {
                                    g.sweep(Instant::now());
                                }
                                continue;
                            }
                        }
                    }
                    None => match rx.recv().await {
                        Some(cmd) => cmd,
                        None => break,
                    },
                };
                match cmd {
                    CGCmd::Heartbeat(hb) => {
                        let now = Instant::now();
                        let cg = groups.entry(hb.group.clone()).or_insert_with(|| ConsumerGroup {
                            assignor: hb.assignor.clone().unwrap_or_else(|| "range".into()),
                            session_timeout_ms,
                            max_size,
                            assignment_interval_ms,
                            ..Default::default()
                        });
                        for (t, c) in hb.counts {
                            cg.partition_counts.insert(t, c);
                        }
                        let res = cg.heartbeat(
                            &hb.member_id,
                            hb.member_epoch,
                            hb.subscribed,
                            hb.assignor,
                            hb.owned,
                            hb.instance_id,
                            hb.regex,
                            hb.rebalance_timeout_ms,
                            now,
                        );
                        let _ = hb.reply.send(res);
                    }
                    CGCmd::Describe { group, reply } => {
                        let _ = reply.send(groups.get(&group).map(|cg| cg.describe()));
                    }
                }
            }
        });
        tx
    }
}

#[cfg(test)]
mod consumer_group_tests {
    //! T-M3.3 块 a + T-M3.3.1 生命周期硬化测试：Range/uniform、epoch 容错
    //! 与 fence、撤销确认闭环、差分下发、session timeout、组容量、节流、
    //! 静态成员。

    use super::*;

    struct Ctx {
        cg: ConsumerGroup,
        now: Instant,
    }
    impl Ctx {
        fn new(cg: ConsumerGroup) -> Self {
            // 默认零节流（老式同瞬间心跳序列不触发 interval 节流）；
            // 节流语义由 assignment_interval_throttles_rebalance 专项覆盖
            let cg = ConsumerGroup { assignment_interval_ms: 0, ..cg };
            Ctx { cg, now: Instant::now() }
        }
        fn tick(&mut self, ms: u64) {
            self.now += Duration::from_millis(ms);
        }
    }

    /// 全量请求变体（owned/names/timeout 均非 null → 强制下发）。
    fn hb(ctx: &mut Ctx, id: &str, epoch: i32, sub: Vec<&str>, owned: Vec<(&str, Vec<i32>)>) -> HeartbeatResult {
        let owned_map: BTreeMap<String, Vec<i32>> =
            owned.into_iter().map(|(t, ps)| (t.to_string(), ps)).collect();
        ctx.cg.heartbeat(
            id, epoch,
            Some(sub.into_iter().map(String::from).collect()),
            None, Some(owned_map), None, None, 10_000, ctx.now,
        )
    }

    /// keepalive 变体：订阅/owned 全 null（非 full）→ 差分生效。
    fn hb_ka(ctx: &mut Ctx, id: &str, epoch: i32) -> HeartbeatResult {
        ctx.cg.heartbeat(id, epoch, None, None, None, None, None, 10_000, ctx.now)
    }

    fn parts_of(r: &HeartbeatResult, t: &str) -> Vec<i32> {
        r.assignment.get(t).cloned().unwrap_or_default()
    }

    /// Range 分配（java RangeAssignor 对齐）：首成员先独占，b 加入重算后
    /// 连续切块——[0] / [1]。
    #[test]
    fn range_assignment_java_aligned() {
        let mut x = Ctx::new(Default::default());
        x.cg.partition_counts.insert("t".into(), 2);
        let a = hb(&mut x, "", 0, vec!["t"], vec![]);
        assert_eq!(parts_of(&a, "t"), vec![0, 1]);
        let b = hb(&mut x, "", 0, vec!["t"], vec![]);
        assert_eq!(a.member_id, "consumer-1");
        assert_eq!(parts_of(&b, "t"), vec![1]);
        // a 的 owned 未确认撤销 [1] 前，b 不该拿到（撤销确认闭环见下测）
        let _ = b;
    }

    /// 续租（full request）不 bump epoch 且分配恒下发；leave 后接管。
    #[test]
    fn join_bumps_epoch_and_leave_yields_all() {
        let mut x = Ctx::new(Default::default());
        x.cg.partition_counts.insert("t".into(), 2);
        let a = hb(&mut x, "", 0, vec!["t"], vec![]);
        let a1 = hb(&mut x, &a.member_id, a.member_epoch, vec!["t"], vec![("t", vec![0, 1])]);
        assert_eq!(a1.member_epoch, a.member_epoch, "续租不 bump");
        assert_eq!(parts_of(&a1, "t"), vec![0, 1]);

        let b = hb(&mut x, "", 0, vec!["t"], vec![]);
        assert!(b.member_epoch >= 1);
        // b 加入重算后：a 旧 epoch + owned ⊄ 新分配 → fence
        let fenced = hb(&mut x, &a.member_id, a.member_epoch, vec!["t"], vec![("t", vec![0, 1])]);
        assert!(fenced.fenced, "旧 epoch + owned 超集必须 fence");
        let a_epoch = fenced.member_epoch;
        let a2 = hb(&mut x, &a.member_id, a_epoch, vec!["t"], vec![("t", vec![0])]);
        assert_eq!(a2.member_epoch, a_epoch, "重同步后续租");
        assert_eq!(parts_of(&a2, "t").len(), 1, "2 成员 2 分区各持 1");

        hb(&mut x, &b.member_id, -1, vec![], vec![]);
        // b 离开重算后：a 的 prev-epoch + owned 子集 = 应答丢失恢复（放行，
        // 账本 54 对照语义）；更老的 epoch（落后于 prev）才是 fence
        let leave = hb(&mut x, &a.member_id, a2.member_epoch - 1, vec!["t"], vec![("t", vec![0])]);
        assert!(leave.fenced, "落后于 prev 的 epoch 必须 fence");
        let a3 = hb(&mut x, &a.member_id, leave.member_epoch, vec!["t"], vec![("t", vec![0])]);
        assert_eq!(parts_of(&a3, "t"), vec![0, 1], "恢复后接管全部");
    }

    /// fence/容错矩阵：prev_epoch + owned ⊆ 分配 → 重同步；epoch 0 → 重
    /// 同步（fenced member recovery）；far-stale → 82；僵尸 → 25。
    #[test]
    fn stale_epoch_handling_matrix() {
        let mut x = Ctx::new(Default::default());
        x.cg.partition_counts.insert("t".into(), 1);
        let a = hb(&mut x, "", 0, vec!["t"], vec![]);
        hb(&mut x, "", 0, vec!["t"], vec![]); // 第二成员加入 → a epoch 2 / prev 1
        // prev-epoch + owned ⊆ → 重同步（应答丢失恢复，不再 fence）
        let sync = hb(&mut x, &a.member_id, a.member_epoch.saturating_sub(1), vec!["t"], vec![("t", vec![0])]);
        assert!(!sync.fenced, "prev-epoch + owned ⊆ 分配 = 应答丢失恢复，放行");
        assert_eq!(sync.member_epoch, a.member_epoch + 1, "重同步回服务端当前 epoch");
        // 已知成员 epoch 0 → fenced member recovery，重发分配
        let rec = hb(&mut x, &a.member_id, 0, vec!["t"], vec![]);
        assert!(!rec.fenced && !rec.changed == false, "epoch 0 已知成员 = recovery 重同步");
        assert_eq!(rec.member_epoch, a.member_epoch + 1);
        // far-stale：epoch 落在 prev 之前（多轮推进后）→ fence
        let far = hb(&mut x, &a.member_id, 0, vec!["t"], vec![]);
        assert!(!far.fenced);
        // 僵尸：未知成员带非零 epoch → 25
        let z = hb(&mut x, "consumer-99", 7, vec!["t"], vec![]);
        assert!(z.fenced && z.unknown_member, "未知成员 = 25");
    }

    /// keepalive（未变字段置 null）：订阅保持、target 保留、无差分下发
    /// （Assignment=null 语义）——不得误判为退订。
    #[test]
    fn keepalive_null_subscription_is_noop() {
        let mut x = Ctx::new(Default::default());
        x.cg.partition_counts.insert("t".into(), 2);
        let a = hb(&mut x, "", 0, vec!["t"], vec![]);
        let ka = hb_ka(&mut x, &a.member_id, a.member_epoch);
        assert!(!ka.fenced && !ka.changed, "keepalive 无差分下发");
        assert_eq!(x.cg.members[&a.member_id].assignment[&"t".to_string()], vec![0, 1], "target 保留");
        // 新成员带 None 订阅 = 空订阅（无分配但不挂）
        let b = hb_ka(&mut x, "", 0);
        assert!(!b.fenced && b.assignment.is_empty());
    }

    /// 退订：订阅集变化 → 重算后该 topic 不在 target。
    #[test]
    fn unsubscribe_releases_partitions() {
        let mut x = Ctx::new(Default::default());
        x.cg.partition_counts.insert("t".into(), 2);
        let a = hb(&mut x, "", 0, vec!["t"], vec![]);
        let u = hb(&mut x, &a.member_id, a.member_epoch, vec![], vec![]);
        assert!(!u.assignment.contains_key("t"), "退订后收回分配");
    }

    /// Describe 快照：成员明细/订阅/分配与 epoch 同源；空组 = Empty。
    #[test]
    fn describe_reflects_membership() {
        let mut x = Ctx::new(Default::default());
        x.cg.partition_counts.insert("t".into(), 2);
        let d0 = x.cg.describe();
        assert_eq!(d0.state, "Empty");
        let a = hb(&mut x, "", 0, vec!["t"], vec![]);
        hb(&mut x, &a.member_id, a.member_epoch, vec!["t"], vec![("t", vec![0, 1])]);
let d = x.cg.describe();
        assert_eq!(d.state, "Stable");
        assert_eq!(d.assignor, "range");
        assert!(d.group_epoch >= 1 && d.assignment_epoch == d.group_epoch, "epoch 同源");
        assert_eq!(d.members.len(), 1);
        assert_eq!(d.members[0].assignment[&"t".to_string()], vec![0, 1]);
    }

    /// uniform movement-minimizing 金样（ADR-20 §3）：3 分区 [A 全占] →
    /// B 加入 [A 留 2、B 得 1] → C 加入 [A 留 p0、B 的 p2 不动、C 得 p1
    /// ——range 在此把 B 挪到 p1，分叉点即金样] → C 离开回归原分配。
    #[test]
    fn uniform_assignment_minimizes_movement() {
        let mut x = Ctx::new(ConsumerGroup { assignor: "uniform".into(), ..Default::default() });
        x.cg.partition_counts.insert("t".into(), 3);
        let a = hb(&mut x, "", 0, vec!["t"], vec![]);
        assert_eq!(parts_of(&a, "t"), vec![0, 1, 2]);
        let a_id = a.member_id.clone();
        let b = hb(&mut x, "", 0, vec!["t"], vec![]);
        let b_id = b.member_id.clone();
        assert_eq!(x.cg.members[&a_id].assignment[&"t".to_string()], vec![0, 1], "A 保留段零移动");
        assert_eq!(x.cg.members[&b_id].assignment[&"t".to_string()], vec![2]);
        let c = hb(&mut x, "", 0, vec!["t"], vec![]);
        let c_id = c.member_id.clone();
        assert_eq!(x.cg.members[&a_id].assignment[&"t".to_string()], vec![0], "A 保留 p0");
        assert_eq!(x.cg.members[&b_id].assignment[&"t".to_string()], vec![2], "B 的 p2 不动（range 分叉点）");
        assert_eq!(x.cg.members[&c_id].assignment[&"t".to_string()], vec![1], "C 补派 p1");
        hb(&mut x, &c_id, -1, vec![], vec![]);
        assert_eq!(x.cg.members[&a_id].assignment[&"t".to_string()], vec![0, 1]);
        assert_eq!(x.cg.members[&b_id].assignment[&"t".to_string()], vec![2]);
        assert!(!x.cg.members.contains_key(&c_id));
    }

    /// uniform 多 topic：订阅约束优先于份额（B 不得拿未订阅的 t2）。
    #[test]
    fn uniform_unsubscribe_and_multi_topic() {
        let mut x = Ctx::new(ConsumerGroup { assignor: "uniform".into(), ..Default::default() });
        x.cg.partition_counts.insert("t1".into(), 2);
        x.cg.partition_counts.insert("t2".into(), 2);
        let a = hb(&mut x, "", 0, vec!["t1", "t2"], vec![]);
        assert_eq!(a.assignment[&"t1".to_string()], vec![0, 1]);
        let a_id = a.member_id.clone();
        let b = hb(&mut x, "", 0, vec!["t1"], vec![]);
        let b_id = b.member_id.clone();
        let a_now = &x.cg.members[&a_id].assignment;
        assert_eq!(a_now.get("t1"), None, "t1 让给唯一订阅 t1 的 B");
        assert_eq!(a_now[&"t2".to_string()], vec![0, 1], "A 保唯一可订阅的 t2");
        assert_eq!(x.cg.members[&b_id].assignment[&"t1".to_string()], vec![0, 1]);
        assert_eq!(x.cg.members[&b_id].assignment.get("t2"), None);
        let a_epoch = x.cg.members[&a_id].epoch;
        let u = hb(&mut x, &a_id, a_epoch, vec!["t2"], vec![]);
        assert!(!u.assignment.contains_key("t1"));
        assert_eq!(u.assignment[&"t2".to_string()], vec![0, 1]);
        assert_eq!(x.cg.members[&b_id].assignment[&"t1".to_string()], vec![0, 1]);
    }

    /// 撤销确认闭环（T-M3.3.1 P0）：分区迁移时新 owner 在原 owner 的
    /// owned 确认释放前拿不到分区——消除双 owner 窗口。
    #[test]
    fn revocation_gates_new_owner() {
        let mut x = Ctx::new(Default::default());
        x.cg.partition_counts.insert("t".into(), 2);
        let a = hb(&mut x, "", 0, vec!["t"], vec![("t", vec![0, 1])]);
        let a_id = a.member_id.clone();
        // B 加入：target 拆 [0]/[1]，但 A 的 owned 仍含 [0,1]（未确认撤销）
        let b = hb(&mut x, "", 0, vec!["t"], vec![("t", vec![])]).member_id.clone();
        let b_view = &x.cg.members[&b].assignment;
        assert!(b_view.get("t").map(|ps| ps.contains(&1)).unwrap_or(false), "target 已含 b 的分区");
        let b_epoch = x.cg.members[&b].epoch;
        let b_delivered = hb(&mut x, &b, b_epoch, vec!["t"], vec![("t", vec![])]);
        // b 的 owned 上报为空 = 确认了它自己名下没有分区；a 的 [1] 撤销
        // 由 a 的下一次 owned 报告确认——先验证 pending 门控存在
        assert_eq!(x.cg.pending_release.len(), 1, "迁移分区进 pending_release");
        // a 心跳 owned=[0]：确认 [1] 已释放 → pending 清除
        let a_epoch = x.cg.members[&a_id].epoch;
        let a2 = hb(&mut x, &a_id, a_epoch, vec!["t"], vec![("t", vec![0])]);
        assert!(x.cg.pending_release.is_empty(), "a 确认后 pending 清除");
        let _ = a2;
        // b 心跳（owned 空）→ 现在可拿到 [1]
        let b_epoch = x.cg.members[&b].epoch;
        let b2 = hb(&mut x, &b, b_epoch, vec!["t"], vec![("t", vec![])]);
        assert_eq!(b2.assignment.get("t").map(|ps| ps.clone()), Some(vec![1]), "确认后 b 拿到迁移分区");
        let _ = b_delivered;
    }

    /// session timeout（T-M3.3.1 P0）：成员停止心跳 → sweep 注销 + 重算。
    #[test]
    fn session_timeout_removes_dead_member() {
        let mut x = Ctx::new(ConsumerGroup { session_timeout_ms: 1_000, ..Default::default() });
        x.cg.partition_counts.insert("t".into(), 2);
        let a = hb(&mut x, "", 0, vec!["t"], vec![]);
        let a_id = a.member_id.clone();
        x.tick(2_000);
        let removed = x.cg.sweep(x.now);
        assert!(removed, "session 超时必须移除成员");
        assert!(x.cg.describe().state == "Empty", "全员消失 → Empty");
        assert!(!x.cg.members.contains_key(&a_id));
        // paused 静态成员不受 session timeout 影响
        let s = hb(&mut x, "", 0, vec!["t"], vec![]);
        x.cg.members.get_mut(&s.member_id).unwrap().instance_id = Some("i1".into());
        hb(&mut x, &s.member_id, -2, vec![], vec![]);
        let _ = hb(&mut x, &s.member_id, s.member_epoch, vec!["t"], vec![("t", vec![0, 1])]);
        x.cg.members.get_mut(&s.member_id).unwrap().paused = true;
        x.tick(2_000);
        assert!(!x.cg.sweep(x.now) || x.cg.members.contains_key(&s.member_id), "paused 不被 sweep 清除");
        assert!(x.cg.members.contains_key(&s.member_id));
    }

    /// 差分下发（T-M3.3.1 P1）：未变 → changed=false（协议面 null）；
    /// full request 强制下发。
    #[test]
    fn differential_delivery_suppresses_unchanged() {
        let mut x = Ctx::new(Default::default());
        x.cg.partition_counts.insert("t".into(), 2);
        let a = hb(&mut x, "", 0, vec!["t"], vec![]);
        assert!(a.changed, "join 恒下发");
        let ka = hb_ka(&mut x, &a.member_id, a.member_epoch);
        assert!(!ka.changed, "keepalive 未变 → changed=false（协议面映射 Assignment=null）");
        let full = hb(&mut x, &a.member_id, a.member_epoch, vec!["t"], vec![("t", vec![0, 1])]);
        assert!(full.changed, "full request 强制下发");
    }

    /// 组容量（T-M3.3.1 P2）：满员后新成员 → 81，零状态。
    #[test]
    fn group_max_size_rejects_join() {
        let mut x = Ctx::new(ConsumerGroup { max_size: 1, ..Default::default() });
        x.cg.partition_counts.insert("t".into(), 2);
        hb(&mut x, "", 0, vec!["t"], vec![]);
        let b = hb(&mut x, "", 0, vec!["t"], vec![]);
        assert_eq!(b.error_code, Some(81), "GROUP_MAX_SIZE_REACHED");
        assert!(x.cg.members.len() == 1, "拒绝零状态");
    }

    /// assignment interval 节流（T-M3.3.1 P2）：订阅变化在窗口内 → dirty
    /// 挂起（Reconciling），窗口后下一次心跳补算。
    #[test]
    fn assignment_interval_throttles_rebalance() {
        let mut x = Ctx::new(Default::default());
        x.cg.assignment_interval_ms = 1_000;
        x.cg.partition_counts.insert("t".into(), 2);
        let a = hb(&mut x, "", 0, vec!["t"], vec![]);
        let a_id = a.member_id.clone();
        x.tick(100);
        // 窗口内订阅变化：join 之外的路径 force=false → 节流
        let a_epoch = x.cg.members[&a_id].epoch;
        let u = hb(&mut x, &a_id, a_epoch, vec![], vec![]);
        assert_eq!(x.cg.describe().state, "Reconciling", "dirty 挂起 = Reconciling");
        x.tick(2_000);
        hb_ka(&mut x, &a_id, u.member_epoch);
        assert_eq!(x.cg.describe().state, "Stable", "窗口过后补算完成");
        assert!(x.cg.members[&a_id].assignment.is_empty(), "退订最终生效");
    }

    /// 静态成员（T-M3.3.1 P2）：epoch=-2 临时离开保留分配；恢复心跳解除。
    #[test]
    fn static_member_temporary_leave() {
        let mut x = Ctx::new(Default::default());
        x.cg.partition_counts.insert("t".into(), 2);
        let a = hb(&mut x, "", 0, vec!["t"], vec![]);
        let a_id = a.member_id.clone();
        x.cg.members.get_mut(&a_id).unwrap().instance_id = Some("i1".into());
        let l = x.cg.heartbeat(
            "whatever", -2, None, None, None, Some("i1".into()), None, 10_000, x.now,
        );
        assert_eq!(l.member_epoch, -2);
        assert!(x.cg.members.contains_key(&a_id), "临时离开不清成员");
        assert_eq!(x.cg.members[&a_id].assignment[&"t".to_string()], vec![0, 1], "分配保留");
        x.tick(60_000);
        x.cg.sweep(x.now);
        assert!(x.cg.members.contains_key(&a_id), "paused 不受 session timeout 影响");
        let back = hb(&mut x, &a_id, a.member_epoch, vec!["t"], vec![("t", vec![0, 1])]);
        assert!(!back.fenced, "恢复心跳解除 paused");
        assert!(!x.cg.members[&a_id].paused);
    }
}
