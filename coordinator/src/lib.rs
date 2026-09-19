//! 消费组协调器（Classic 协议）+ offset 管理（TASK.md T-M1.1/T-M1.2）。
//!
//! 架构原则（KRaft 同款）：
//! - offset 持久化走 append-only 日志，恢复=重放（record-based）；
//! - 组成员关系 POC 阶段驻内存（broker 重启即失，客户端会重新 join——
//!   语义等价于 session 过期，符合协议；完整 record-based 组日志在 M1 补齐）；
//! - 单任务独占状态（actor），内部无锁无 Arc。

use std::collections::HashMap;
use std::time::{Duration, Instant};

pub mod consumer_group;

// KIP-848 新消费组协议面（T-M3.3 块 b）——server 侧 handler 直接引用
pub use consumer_group::{CGCmd, CGHeartbeat, ConsumerGroups, GroupDescribe, MemberDescribe};


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupState {
    Empty,
    PreparingRebalance,
    CompletingSync,
    Stable,
}

impl GroupState {
    /// Kafka DescribeGroups/ListGroups 的状态串。
    pub fn as_str(&self) -> &'static str {
        match self {
            GroupState::Empty => "Empty",
            GroupState::PreparingRebalance => "PreparingRebalance",
            GroupState::CompletingSync => "CompletingSync",
            GroupState::Stable => "Stable",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Member {
    pub member_id: String,
    pub client_host: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub subscription: Vec<u8>,
    /// 成员支持的协议名序（请求偏好序）——组协议选择面（T-M3.4，ADR-20 §2：
    /// leader 偏好序 ∩ 全体成员支持集，替代硬编码 range）
    pub protocol_names: Vec<String>,
    pub assignment: Vec<u8>,
    pub last_heartbeat: Instant,
}

#[derive(Debug)]
pub struct Group {
    pub state: GroupState,
    pub generation: i32,
    pub protocol_type: String,
    /// 已选定的分配策略（Leader 的订阅协议里所有成员都支持的首个）。
    pub protocol: Option<String>,
    pub leader: Option<String>,
    pub members: Vec<Member>,
    /// PreparingRebalance 的截止时刻：到期未重新入组的成员被踢除。
    pub rebalance_deadline: Option<Instant>,
}

impl Group {
    fn new(protocol_type: String) -> Group {
        Group { state: GroupState::Empty, generation: 0, protocol_type, protocol: None, leader: None, members: Vec::new(), rebalance_deadline: None }
    }

    pub fn member(&self, id: &str) -> Option<&Member> {
        self.members.iter().find(|m| m.member_id == id)
    }

    fn member_mut(&mut self, id: &str) -> Option<&mut Member> {
        self.members.iter_mut().find(|m| m.member_id == id)
    }
}

// ---------- 命令 ----------

pub struct JoinSpec {
    pub group: String,
    pub member_id: String,
    pub protocol_type: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub protocols: Vec<(String, Vec<u8>)>,
    pub client_host: String,
}

pub struct SyncSpec {
    pub group: String,
    pub generation: i32,
    pub member_id: String,
    pub protocol_type: Option<String>,
    pub protocol: Option<String>,
    /// (member_id, assignment) —— 仅 leader 携带非空
    pub assignments: Vec<(String, Vec<u8>)>,
}

#[derive(Debug, Clone)]
pub struct CommittedOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub metadata: String,
    pub commit_ts: i64,
}

pub enum GroupCmd {
    JoinGroup(JoinSpec, tokio::sync::oneshot::Sender<JoinResult>),
    SyncGroup(SyncSpec, tokio::sync::oneshot::Sender<SyncResult>),
    Heartbeat { group: String, generation: i32, member_id: String, reply: tokio::sync::oneshot::Sender<CoordError> },
    LeaveGroup { group: String, member_id: String, reply: tokio::sync::oneshot::Sender<CoordError> },
    CommitOffsets { group: String, generation: i32, member_id: String, offsets: Vec<CommittedOffset>, reply: tokio::sync::oneshot::Sender<CoordError> },
    /// 事务消费位提升（ADR-18 §7，KIP-447 essential）：pending 在协调器侧
    /// 已过 epoch fence（TxnLog 为权威），此处免成员/generation 校验直接
    /// 落 OffsetLog——EndTxn(commit) 完成路径的生效点。
    PromoteTxnOffsets { group: String, offsets: Vec<CommittedOffset>, reply: tokio::sync::oneshot::Sender<()> },
    FetchOffsets { group: String, topics: Option<Vec<String>>, reply: tokio::sync::oneshot::Sender<Vec<CommittedOffset>> },
    DeleteGroup { group: String },
    /// 管理面：列出全部组（ListGroups v0-4）。
    ListGroups { reply: tokio::sync::oneshot::Sender<Vec<GroupSummary>> },
    /// 管理面：查询单组详情（DescribeGroups）。组不存在回 None。
    DescribeGroup { group: String, reply: tokio::sync::oneshot::Sender<Option<GroupDetail>> },
    /// 方案 B 块 b2：绑定内部 topic 状态通道 + 安装重放状态。绑定前
    /// commit/promote 一律回 CoordinatorNotAvailable（客户端重试）——
    /// 绑定点在客户端监听建立之前，窗口内无真实流量。
    BindState {
        out: StateSinkTx,
        install: HashMap<(String, String, i32), CommittedOffset>,
        reply: tokio::sync::oneshot::Sender<()>,
    },
}

/// 组状态下沉通道（server 侧 GroupStateSync 实现：内部 topic produce 路径）。
/// 消息 = (group, 批量提交, 持久化完成应答)——应答 Ok 即已落内部 topic
/// （acks=all 语义，B2 权威存储；本地 OffsetLog 已移除）。
pub type StateSinkTx = tokio::sync::mpsc::Sender<(
    String,
    Vec<CommittedOffset>,
    tokio::sync::oneshot::Sender<Result<(), String>>,
)>;

/// ListGroups 条目。
#[derive(Debug, Clone)]
pub struct GroupSummary {
    pub group: String,
    pub protocol_type: String,
    pub state: String,
}

/// DescribeGroups 成员条目（client_id 当前不可得，恒空串）。
#[derive(Debug, Clone)]
pub struct MemberDetail {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub metadata: Vec<u8>,
    pub assignment: Vec<u8>,
}

/// DescribeGroups 组详情。
#[derive(Debug, Clone)]
pub struct GroupDetail {
    pub state: String,
    pub protocol_type: String,
    pub protocol: String,
    pub members: Vec<MemberDetail>,
}

pub struct JoinResult {
    pub error: CoordError,
    pub generation: i32,
    pub protocol_type: String,
    pub protocol: Option<String>,
    pub leader: String,
    pub member_id: String,
    /// 仅 leader：全部成员订阅
    pub members: Vec<(String, Vec<u8>)>,
}

pub struct SyncResult {
    pub error: CoordError,
    pub protocol_type: String,
    pub protocol: Option<String>,
    pub assignment: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordError {
    None,
    GroupCoordinatorNotAvailable,
    NotCoordinator,
    IllegalGeneration,
    UnknownMemberId,
    RebalanceInProgress,
    GroupAuthorizationFailed,
}

impl CoordError {
    pub fn code(self) -> i16 {
        match self {
            CoordError::None => 0,
            CoordError::GroupCoordinatorNotAvailable => 15,
            CoordError::NotCoordinator => 16,
            CoordError::IllegalGeneration => 22,
            CoordError::UnknownMemberId => 25,
            CoordError::RebalanceInProgress => 27,
            CoordError::GroupAuthorizationFailed => 30,
        }
    }
}

struct PendingJoin {
    spec_member: String,
    reply: tokio::sync::oneshot::Sender<JoinResult>,
}

struct PendingSync {
    member_id: String,
    reply: tokio::sync::oneshot::Sender<SyncResult>,
}

pub struct GroupManager {
    groups: HashMap<String, Group>,
    offsets: HashMap<(String, String, i32), CommittedOffset>,
    /// 方案 B：组状态权威存储 = __basalt_group_state（produce 路径）。
    /// None = 未绑定（见 BindState）。
    state_out: Option<StateSinkTx>,
    rx: tokio::sync::mpsc::Receiver<GroupCmd>,
    pending_joins: HashMap<String, Vec<PendingJoin>>, // group → 等待 join 完成的请求
    pending_syncs: HashMap<String, Vec<PendingSync>>,
    next_member_seq: u64,
}

impl GroupManager {
    pub fn spawn() -> tokio::sync::mpsc::Sender<GroupCmd> {
        let (tx, rx) = tokio::sync::mpsc::channel(512);
        let mgr = GroupManager {
            groups: HashMap::new(),
            offsets: HashMap::new(),
            state_out: None,
            rx,
            pending_joins: HashMap::new(),
            pending_syncs: HashMap::new(),
            next_member_seq: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0),
        };
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(mgr.run());
        });
        tx
    }

    async fn run(mut self) {
        // commit/promote 需要等待内部 topic 持久化应答，不能在 drain 的
        // try_recv 循环里同步处理——暂存队列，主循环按序 await
        let mut deferred: std::collections::VecDeque<GroupCmd> = Default::default();
        loop {
            if let Some(cmd) = deferred.pop_front() {
                self.handle_persisting(cmd).await;
                self.drain_into(&mut deferred);
                continue;
            }
            let deadline = self.next_deadline();
            match tokio::time::timeout(deadline, self.rx.recv()).await {
                Ok(Some(cmd)) => self.handle_persisting(cmd).await,
                Ok(None) => break,
                Err(_) => self.sweep(),
            }
            self.drain_into(&mut deferred);
        }
    }

    /// 持久化敏感命令的统一入口（B2）：commit/promote 先落内部 topic
    /// （await 应答）再更新内存视图并应答；失败回 CoordinatorNotAvailable
    /// （可重试 15——客户端按退避重投，内存视图不更新 = 未提交）。
    async fn handle_persisting(&mut self, cmd: GroupCmd) {
        match cmd {
            GroupCmd::CommitOffsets { group, generation, member_id, offsets, reply } => {
                let v = self.validate_commit(&group, generation, &member_id);
                if v != CoordError::None {
                    let _ = reply.send(v);
                    return;
                }
                let e = match self.persist(&group, &offsets).await {
                    Ok(()) => CoordError::None,
                    Err(()) => CoordError::GroupCoordinatorNotAvailable,
                };
                let _ = reply.send(e);
            }
            GroupCmd::PromoteTxnOffsets { group, offsets, reply } => {
                if self.persist(&group, &offsets).await.is_ok() {
                    self.promote(&group, &offsets);
                }
                let _ = reply.send(());
            }
            other => self.handle(other),
        }
    }

    fn next_deadline(&self) -> Duration {
        let mut min = Duration::from_secs(3600);
        for g in self.groups.values() {
            for m in &g.members {
                let elapsed = m.last_heartbeat.elapsed();
                let ttl = Duration::from_millis(m.session_timeout_ms.max(0) as u64);
                if ttl > elapsed {
                    min = min.min(ttl - elapsed);
                } else {
                    return Duration::from_millis(0);
                }
            }
            if g.state == GroupState::PreparingRebalance {
                if let Some(d) = g.rebalance_deadline {
                    let now = Instant::now();
                    let wait = d.saturating_duration_since(now);
                    min = min.min(wait.max(Duration::from_millis(20)));
                } else {
                    min = min.min(Duration::from_millis(20));
                }
            }
        }
        min
    }

    fn drain_into(&mut self, deferred: &mut std::collections::VecDeque<GroupCmd>) {
        while let Ok(cmd) = self.rx.try_recv() {
            match cmd {
                c @ (GroupCmd::CommitOffsets { .. } | GroupCmd::PromoteTxnOffsets { .. }) => {
                    deferred.push_back(c);
                }
                other => self.handle(other),
            }
        }
    }

    fn sweep(&mut self) {
        let now = Instant::now();
        let mut emptied: Vec<String> = Vec::new();
        for (name, g) in self.groups.iter_mut() {
            let before = g.members.len();
            g.members.retain(|m| {
                now.duration_since(m.last_heartbeat) < Duration::from_millis(m.session_timeout_ms.max(0) as u64)
            });
            if g.members.len() != before && g.state == GroupState::Stable {
                g.state = GroupState::PreparingRebalance;
                g.rebalance_deadline = None;
                tracing::info!(removed = before - g.members.len(), "session expired → rebalance");
            }
            if g.members.is_empty() {
                g.state = GroupState::Empty;
                g.leader = None;
                g.protocol = None;
                g.rebalance_deadline = None;
                emptied.push(name.clone());
            }
        }
        // 组清空时悬挂的 pending join/sync 必须答复（可重试 27）——否则
        // 客户端等满自己的超时（协作组中途全灭的场景实证）
        for name in emptied {
            self.fail_pending_joins(&name, CoordError::RebalanceInProgress);
            self.fail_pending_syncs(&name, CoordError::RebalanceInProgress);
        }
        // rebalance 截止：踢除未重新入组的成员后完成
        for name in self.groups.keys().cloned().collect::<Vec<_>>() {
            let deadline = self.groups[&name].rebalance_deadline;
            if self.groups[&name].state == GroupState::PreparingRebalance {
                if let Some(d) = deadline {
                    if now < d {
                        continue;
                    }
                } else {
                    continue;
                }
                // 到期：未在 pending_joins 的成员踢除
                let pending: Vec<String> = self
                    .pending_joins
                    .get(&name)
                    .map(|v| v.iter().map(|p| p.spec_member.clone()).collect())
                    .unwrap_or_default();
                let g = self.groups.get_mut(&name).unwrap();
                let before = g.members.len();
                g.members.retain(|m| pending.contains(&m.member_id));
                if g.members.len() != before {
                    tracing::info!(kicked = before - g.members.len(), group=%name, "rebalance deadline: kicked non-rejoined members");
                }
                if g.leader.as_ref().is_some_and(|l| !pending.contains(l)) {
                    g.leader = pending.first().cloned();
                }
                if g.members.is_empty() {
                    g.state = GroupState::Empty;
                    g.rebalance_deadline = None;
                    self.fail_pending_joins(&name, CoordError::RebalanceInProgress);
                    self.fail_pending_syncs(&name, CoordError::RebalanceInProgress);
                } else {
                    g.rebalance_deadline = None;
                    self.complete_rebalance(&name);
                }
            }
        }
    }

    /// 冲刷组的悬挂 pending join（组已无法完成本轮 rebalance 时）：逐个回
    /// 可重试错误，客户端按退避重入组。
    fn fail_pending_joins(&mut self, group: &str, error: CoordError) {
        if let Some(pendings) = self.pending_joins.remove(group) {
            for p in pendings {
                let _ = p.reply.send(JoinResult {
                    error,
                    generation: -1,
                    protocol_type: String::new(),
                    protocol: None,
                    leader: String::new(),
                    member_id: p.spec_member,
                    members: vec![],
                });
            }
        }
    }

    fn handle(&mut self, cmd: GroupCmd) {
        match cmd {
            GroupCmd::JoinGroup(spec, reply) => self.join(spec, reply),
            GroupCmd::SyncGroup(spec, reply) => self.sync(spec, reply),
            GroupCmd::Heartbeat { group, generation, member_id, reply } => {
                let e = self.heartbeat(&group, generation, &member_id);
                let _ = reply.send(e);
            }
            GroupCmd::LeaveGroup { group, member_id, reply } => {
                let e = self.leave(&group, &member_id);
                let _ = reply.send(e);
            }
            GroupCmd::BindState { out, install, reply } => {
                self.offsets.extend(install);
                self.state_out = Some(out);
                tracing::info!(installed = self.offsets.len(), "group state bound to internal topic");
                let _ = reply.send(());
            }
            // commit/promote 走 handle_persisting（run 循环拦截）；此分支
            // 兜底防御性不可达
            GroupCmd::CommitOffsets { reply, .. } => {
                let _ = reply.send(CoordError::GroupCoordinatorNotAvailable);
            }
            GroupCmd::PromoteTxnOffsets { reply, .. } => {
                let _ = reply.send(());
            }
            GroupCmd::FetchOffsets { group, topics, reply } => {
                let out: Vec<CommittedOffset> = self
                    .offsets
                    .iter()
                    .filter(|((g, t, _), _)| {
                        g == &group && topics.as_ref().map_or(true, |ts| ts.iter().any(|x| x == t))
                    })
                    .map(|(_, o)| o.clone())
                    .collect();
                let _ = reply.send(out);
            }
            GroupCmd::DeleteGroup { group } => {
                self.groups.remove(&group);
            }
            GroupCmd::ListGroups { reply } => {
                let out = self
                    .groups
                    .iter()
                    .map(|(name, g)| GroupSummary {
                        group: name.clone(),
                        protocol_type: g.protocol_type.clone(),
                        state: g.state.as_str().to_string(),
                    })
                    .collect();
                let _ = reply.send(out);
            }
            GroupCmd::DescribeGroup { group, reply } => {
                let detail = self.groups.get(&group).map(|g| GroupDetail {
                    state: g.state.as_str().to_string(),
                    protocol_type: g.protocol_type.clone(),
                    protocol: g.protocol.clone().unwrap_or_default(),
                    members: g
                        .members
                        .iter()
                        .map(|m| MemberDetail {
                            member_id: m.member_id.clone(),
                            client_id: String::new(),
                            client_host: m.client_host.clone(),
                            metadata: m.subscription.clone(),
                            assignment: m.assignment.clone(),
                        })
                        .collect(),
                });
                let _ = reply.send(detail);
            }
        }
    }

    fn join(&mut self, spec: JoinSpec, reply: tokio::sync::oneshot::Sender<JoinResult>) {
        let g = self.groups.entry(spec.group.clone()).or_insert_with(|| Group::new(spec.protocol_type.clone()));
        if g.protocol_type != spec.protocol_type {
            let _ = reply.send(JoinResult {
                error: CoordError::None, generation: -1,
                protocol_type: g.protocol_type.clone(), protocol: None,
                leader: String::new(), member_id: String::new(), members: vec![],
            });
            return;
        }

        // 新成员或空 member_id → 生成 id
        let member_id = if spec.member_id.is_empty() {
            self.next_member_seq += 1;
            format!("basalt-{}", self.next_member_seq)
        } else {
            spec.member_id.clone()
        };

        if g.member(&member_id).is_none() {
            g.members.push(Member {
                member_id: member_id.clone(),
                client_host: spec.client_host.clone(),
                session_timeout_ms: spec.session_timeout_ms,
                rebalance_timeout_ms: spec.rebalance_timeout_ms,
                subscription: Vec::new(),
                protocol_names: spec.protocols.iter().map(|(n, _)| n.clone()).collect(),
                assignment: Vec::new(),
                last_heartbeat: Instant::now(),
            });
        }
        let known_protocols: Vec<(String, Vec<u8>)> = spec.protocols.clone();
        if let Some(m) = g.member_mut(&member_id) {
            m.last_heartbeat = Instant::now();
            m.session_timeout_ms = spec.session_timeout_ms;
            m.protocol_names = known_protocols.iter().map(|(n, _)| n.clone()).collect();
            if let Some((_name, meta)) = known_protocols.first() {
                m.subscription = meta.clone();
            }
        }

        let need_rebalance =
            g.state == GroupState::Empty || g.state == GroupState::Stable || g.state == GroupState::CompletingSync;
        if need_rebalance {
            // CompletingSync 期间新成员加入：立即进入下一轮 rebalance
            // （否则新成员的 JoinGroup 在 pending_joins 里无人应答直至客户端超时）
            g.state = GroupState::PreparingRebalance;
            g.rebalance_deadline = None;
            if g.leader.is_none() {
                g.leader = Some(member_id.clone());
            }
        }
        if g.state == GroupState::PreparingRebalance && g.rebalance_deadline.is_none() {
            // Kafka 语义：截止 = 成员最大 rebalance timeout；到期未重入组者踢除
            let max_rt = g
                .members
                .iter()
                .map(|m| m.rebalance_timeout_ms.max(0) as u64)
                .max()
                .unwrap_or(5_000)
                .clamp(500, 30_000);
            g.rebalance_deadline = Some(Instant::now() + Duration::from_millis(max_rt));
        }

        tracing::debug!(group=%spec.group, member=%member_id, state=?g.state, "joingroup");
        self.pending_joins.entry(spec.group.clone()).or_default().push(PendingJoin { spec_member: member_id.clone(), reply });
        self.maybe_complete_rebalance(&spec.group);
    }

    fn maybe_complete_rebalance(&mut self, group: &str) {
        let Some(g) = self.groups.get(group) else { return };
        if g.state != GroupState::PreparingRebalance {
            return;
        }
        // 只数仍然在组的 pending（session 过期成员的挂起请求不算数——否则
        // 过期成员会触发「完成」并给已不在组的人发 success）
        let alive = self
            .pending_joins
            .get(group)
            .map(|v| v.iter().filter(|p| g.member(&p.spec_member).is_some()).count())
            .unwrap_or(0);
        if alive >= g.members.len() && !g.members.is_empty() {
            self.complete_rebalance(group);
        }
    }

    fn complete_rebalance(&mut self, group: &str) {
        let Some(g) = self.groups.get_mut(group) else { return };
        if g.state != GroupState::PreparingRebalance || g.members.is_empty() {
            return;
        }
        // 选分配策略（T-M3.4，ADR-20 §2）：leader 偏好序 ∩ 全体成员支持集
        // 的首个——cooperative-sticky 等非 range 协议组依赖此处选中正确
        // 协议名（客户端校验应答协议）。空交集兜底 range（POC 不做
        // InconsistentGroupProtocol=23 拒绝，注释即边界）。
        let leader_names = g
            .member(g.leader.as_deref().unwrap_or(""))
            .map(|m| m.protocol_names.clone())
            .unwrap_or_default();
        let protocol = leader_names
            .into_iter()
            .find(|name| {
                g.members
                    .iter()
                    .all(|m| m.protocol_names.iter().any(|n| n == name))
            })
            .unwrap_or_else(|| "range".to_string());
        g.protocol = Some(protocol);
        g.generation += 1;
        g.state = GroupState::CompletingSync;
        g.rebalance_deadline = None;

        let generation = g.generation;
        let protocol_type = g.protocol_type.clone();
        let protocol = g.protocol.clone();
        let leader = g.leader.clone().unwrap_or_default();
        let members_meta: Vec<(String, Vec<u8>)> = g
            .members
            .iter()
            .map(|m| (m.member_id.clone(), m.subscription.clone()))
            .collect();

        tracing::info!(
            group=%group, gen=g.generation, members=g.members.len(), leader=%leader,
            subs=?g.members.iter().map(|m| (m.member_id.clone(), m.subscription.len())).collect::<Vec<_>>(),
            "rebalance completed");
        let pendings = self.pending_joins.remove(group).unwrap_or_default();
        for p in pendings {
            // stale pending（成员已被 session 过期踢除）：回可重试 27 而非
            // success——不在组的人不能拿到新代分配
            let alive = g.member(&p.spec_member).is_some();
            let _ = p.reply.send(JoinResult {
                error: if alive { CoordError::None } else { CoordError::RebalanceInProgress },
                generation: if alive { generation } else { -1 },
                protocol_type: protocol_type.clone(),
                protocol: protocol.clone(),
                leader: leader.clone(),
                member_id: p.spec_member.clone(),
                members: if alive && p.spec_member == leader { members_meta.clone() } else { vec![] },
            });
        }
    }

    /// 冲刷组的悬挂 pending sync（组已无法完成本轮 rebalance 时）：逐个回
    /// 可重试错误。
    fn fail_pending_syncs(&mut self, group: &str, error: CoordError) {
        if let Some(pendings) = self.pending_syncs.remove(group) {
            for p in pendings {
                let _ = p.reply.send(SyncResult {
                    error,
                    protocol_type: String::new(),
                    protocol: None,
                    assignment: vec![],
                });
            }
        }
    }

    fn sync(&mut self, spec: SyncSpec, reply: tokio::sync::oneshot::Sender<SyncResult>) {
        tracing::debug!(group=%spec.group, member=%spec.member_id, gen=spec.generation, n_assign=spec.assignments.len(), state=?self.groups.get(&spec.group).map(|g| g.state), "syncgroup recv");
        let Some(g) = self.groups.get_mut(&spec.group) else {
            let _ = reply.send(SyncResult { error: CoordError::UnknownMemberId, protocol_type: String::new(), protocol: None, assignment: vec![] });
            return;
        };
        if g.member(&spec.member_id).is_none() {
            let _ = reply.send(SyncResult { error: CoordError::UnknownMemberId, protocol_type: g.protocol_type.clone(), protocol: g.protocol.clone(), assignment: vec![] });
            return;
        }
        if spec.generation != g.generation {
            let _ = reply.send(SyncResult { error: CoordError::IllegalGeneration, protocol_type: g.protocol_type.clone(), protocol: g.protocol.clone(), assignment: vec![] });
            return;
        }
        if g.state == GroupState::PreparingRebalance || g.state == GroupState::Empty {
            let _ = reply.send(SyncResult { error: CoordError::RebalanceInProgress, protocol_type: g.protocol_type.clone(), protocol: g.protocol.clone(), assignment: vec![] });
            return;
        }
        // 非 leader 且尚无 assignments：挂入 pending_syncs（等 leader 分配后统一应答）
        if spec.assignments.is_empty() && g.state == GroupState::CompletingSync {
            self.pending_syncs.entry(spec.group.clone()).or_default().push(PendingSync {
                member_id: spec.member_id.clone(),
                reply,
            });
            return;
        }
        // leader：登记 assignments
        if !spec.assignments.is_empty() {
            for (mid, assignment) in &spec.assignments {
                if let Some(m) = g.member_mut(mid) {
                    m.assignment = assignment.clone();
                }
            }
            g.state = GroupState::Stable;
            tracing::info!(group=%spec.group, n=spec.assignments.len(),
                detail=?spec.assignments.iter().map(|(m,a)|(m.clone(), a.len())).collect::<Vec<_>>(),
                "sync: assignments stored → stable");
            let pends = self.pending_syncs.remove(&spec.group).unwrap_or_default();
            for p in pends {
                let a = g.member(&p.member_id).map(|m| m.assignment.clone()).unwrap_or_default();
                let _ = p.reply.send(SyncResult {
                    error: CoordError::None,
                    protocol_type: g.protocol_type.clone(),
                    protocol: g.protocol.clone(),
                    assignment: a,
                });
            }
        }
        // 本成员的 assignment
        let assignment = g.member(&spec.member_id).map(|m| m.assignment.clone()).unwrap_or_default();
        let _ = reply.send(SyncResult {
            error: CoordError::None,
            protocol_type: g.protocol_type.clone(),
            protocol: g.protocol.clone(),
            assignment,
        });
    }

    fn heartbeat(&mut self, group: &str, generation: i32, member_id: &str) -> CoordError {
        let Some(g) = self.groups.get_mut(group) else { return CoordError::GroupCoordinatorNotAvailable };
        let Some(m) = g.member_mut(member_id) else { return CoordError::UnknownMemberId };
        m.last_heartbeat = Instant::now();
        if generation != g.generation { return CoordError::IllegalGeneration; }
        if g.state != GroupState::Stable { return CoordError::RebalanceInProgress; }
        CoordError::None
    }

    fn leave(&mut self, group: &str, member_id: &str) -> CoordError {
        let Some(g) = self.groups.get_mut(group) else { return CoordError::GroupCoordinatorNotAvailable };
        let before = g.members.len();
        g.members.retain(|m| m.member_id != member_id);
        if g.members.len() == before { return CoordError::UnknownMemberId; }
        if g.leader.as_deref() == Some(member_id) {
            g.leader = g.members.first().map(|m| m.member_id.clone());
        }
        if g.members.is_empty() {
            g.state = GroupState::Empty;
            g.leader = None;
        } else if g.state == GroupState::Stable {
            g.state = GroupState::PreparingRebalance;
        }
        CoordError::None
    }

    /// 事务提升：与 commit 同一持久化点（B2 起为内部 topic），但以
    /// 协调器 TxnLog 的 epoch fence 为准——不做成员/generation 校验。
    /// 持久化由 run 循环的 persist 路径先行完成，此处只应用内存视图。
    fn promote(&mut self, group: &str, offsets: &[CommittedOffset]) {
        self.apply_commits(group, offsets);
        tracing::info!(group=%group, promoted=offsets.len(), "txn offsets promoted");
    }

    /// commit 的持久化前置校验（失败不落存储）。
    fn validate_commit(&self, group: &str, generation: i32, member_id: &str) -> CoordError {
        // POC：仅校验成员存在；generation 校验放宽（stable 组必须匹配）
        if let Some(g) = self.groups.get(group) {
            if g.member(member_id).is_none() {
                return CoordError::UnknownMemberId;
            }
            if g.state == GroupState::Stable && generation >= 0 && generation != g.generation {
                return CoordError::IllegalGeneration;
            }
        }
        CoordError::None
    }

    /// 提交持久化（B2）：内部 topic append（acks=all 语义）——应答 Ok 即
    /// 已持久化。通道关闭/存储拒绝 = Err（调用方回 CoordinatorNotAvailable，
    /// 客户端重试；内存视图不更新 = 未提交）。
    async fn persist(&mut self, group: &str, offsets: &[CommittedOffset]) -> Result<(), ()> {
        let Some(sink) = self.state_out.clone() else { return Err(()) };
        let (tx, rx) = tokio::sync::oneshot::channel();
        sink.send((group.to_string(), offsets.to_vec(), tx))
            .await
            .map_err(|_| ())?;
        match rx.await.map_err(|_| ())? {
            Ok(()) => {
                self.apply_commits(group, offsets);
                Ok(())
            }
            Err(e) => {
                tracing::warn!(group=%group, error=%e, "group state sink rejected commit");
                Err(())
            }
        }
    }

    fn apply_commits(&mut self, group: &str, offsets: &[CommittedOffset]) {
        for o in offsets {
            self.offsets.insert((group.to_string(), o.topic.clone(), o.partition), o.clone());
        }
        tracing::info!(group=%group, accepted=offsets.len(), total=self.offsets.len(), "offsets committed");
    }
}
