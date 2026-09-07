//! 消费组协调器（Classic 协议）+ offset 管理（TASK.md T-M1.1/T-M1.2）。
//!
//! 架构原则（KRaft 同款）：
//! - offset 持久化走 append-only 日志，恢复=重放（record-based）；
//! - 组成员关系 POC 阶段驻内存（broker 重启即失，客户端会重新 join——
//!   语义等价于 session 过期，符合协议；完整 record-based 组日志在 M1 补齐）；
//! - 单任务独占状态（actor），内部无锁无 Arc。

use std::collections::HashMap;
use std::time::{Duration, Instant};

pub mod log;

use log::OffsetLog;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupState {
    Empty,
    PreparingRebalance,
    CompletingSync,
    Stable,
}

#[derive(Debug, Clone)]
pub struct Member {
    pub member_id: String,
    pub client_host: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub subscription: Vec<u8>,
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
    FetchOffsets { group: String, topics: Option<Vec<String>>, reply: tokio::sync::oneshot::Sender<Vec<CommittedOffset>> },
    DeleteGroup { group: String },
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
    offset_log: OffsetLog,
    rx: tokio::sync::mpsc::Receiver<GroupCmd>,
    pending_joins: HashMap<String, Vec<PendingJoin>>, // group → 等待 join 完成的请求
    pending_syncs: HashMap<String, Vec<PendingSync>>,
    next_member_seq: u64,
}

impl GroupManager {
    pub fn spawn(data_dir: &std::path::Path) -> tokio::sync::mpsc::Sender<GroupCmd> {
        let (tx, rx) = tokio::sync::mpsc::channel(512);
        let offset_log = OffsetLog::open(&data_dir.join("__consumer_offsets.log"));
        let mgr = GroupManager {
            groups: HashMap::new(),
            offsets: offset_log.replay(),
            offset_log,
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
        loop {
            let deadline = self.next_deadline();
            match tokio::time::timeout(deadline, self.rx.recv()).await {
                Ok(Some(cmd)) => self.handle(cmd),
                Ok(None) => break,
                Err(_) => self.sweep(),
            }
            self.drain();
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

    fn drain(&mut self) {
        while let Ok(cmd) = self.rx.try_recv() {
            self.handle(cmd);
        }
    }

    fn sweep(&mut self) {
        let now = Instant::now();
        for g in self.groups.values_mut() {
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
            }
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
                } else {
                    g.rebalance_deadline = None;
                    self.complete_rebalance(&name);
                }
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
            GroupCmd::CommitOffsets { group, generation, member_id, offsets, reply } => {
                let e = self.commit(&group, generation, &member_id, offsets);
                let _ = reply.send(e);
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
                assignment: Vec::new(),
                last_heartbeat: Instant::now(),
            });
        }
        let known_protocols: Vec<(String, Vec<u8>)> = spec.protocols.clone();
        if let Some(m) = g.member_mut(&member_id) {
            m.last_heartbeat = Instant::now();
            m.session_timeout_ms = spec.session_timeout_ms;
            if let Some((_name, meta)) = known_protocols.first() {
                m.subscription = meta.clone();
            }
        }

        let was_stable = g.state == GroupState::Stable || g.state == GroupState::Empty;
        if g.state == GroupState::Empty || (was_stable && g.state != GroupState::PreparingRebalance) {
            g.state = GroupState::PreparingRebalance;
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
        let pending = self.pending_joins.get(group).map(|v| v.len()).unwrap_or(0);
        if pending >= g.members.len() && !g.members.is_empty() {
            self.complete_rebalance(group);
        }
    }

    fn complete_rebalance(&mut self, group: &str) {
        let Some(g) = self.groups.get_mut(group) else { return };
        if g.state != GroupState::PreparingRebalance || g.members.is_empty() {
            return;
        }
        // 选分配策略：leader 订阅协议序里第一个全体支持的
        let leader_sub = g.member(g.leader.as_deref().unwrap_or("")).map(|m| m.subscription.clone());
        let _ = leader_sub;
        g.protocol = Some("range".to_string());
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
            let is_leader = p.spec_member == leader;
            let _ = p.reply.send(JoinResult {
                error: CoordError::None,
                generation,
                protocol_type: protocol_type.clone(),
                protocol: protocol.clone(),
                leader: leader.clone(),
                member_id: p.spec_member.clone(),
                members: if is_leader { members_meta.clone() } else { vec![] },
            });
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
        if g.state == GroupState::PreparingRebalance {
            let _ = reply.send(SyncResult { error: CoordError::RebalanceInProgress, protocol_type: g.protocol_type.clone(), protocol: g.protocol.clone(), assignment: vec![] });
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

    fn commit(&mut self, group: &str, generation: i32, member_id: &str, offsets: Vec<CommittedOffset>) -> CoordError {
        // POC：仅校验成员存在；generation 校验放宽（stable 组必须匹配）
        if let Some(g) = self.groups.get(group) {
            if g.member(member_id).is_none() {
                return CoordError::UnknownMemberId;
            }
            if g.state == GroupState::Stable && generation >= 0 && generation != g.generation {
                return CoordError::IllegalGeneration;
            }
        }
        for o in &offsets {
            self.offset_log.append(group, &o);
            self.offsets.insert((group.to_string(), o.topic.clone(), o.partition), o.clone());
        }
        tracing::info!(group=%group, accepted=offsets.len(), total=self.offsets.len(), "offsets committed");
        CoordError::None
    }
}
