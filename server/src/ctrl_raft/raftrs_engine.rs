//! raft-rs（TiKV）引擎——驱动式 RawNode（ADR-16）。
//!
//! 与 openraft（自驱事件循环）互补：本引擎由集成方驱动——
//! tick() 推进逻辑时钟、ready() 取出待持久化/待发送/已提交三队列、
//! advance() 确认。MemStorage 起步（v1：控制器重启后由 leader 日志重放
//! 追平；快照持久化为后续增强）。

use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use raft::prelude::*;
use raft::storage::MemStorage;
use raft::RawNode;

use basalt_metadata::cluster::ClusterRecord;
use basalt_metadata::cluster::ClusterState;
use slog::Drain;

fn logger() -> slog::Logger {
    slog::Logger::root(slog::Discard.fuse(), slog::o!())
}

pub enum EngineCmd {
    Propose(ClusterRecord, mpsc::Sender<Result<(), String>>),
    TriggerElect,
    /// 跨进程 MSG_RAFT：对端引擎的 raft 消息（prost 编码）
    RaftWire(Vec<u8>),
    Stop(mpsc::Sender<()>),
}

/// 本节点引擎的命令通道注册表（internal RPC MSG_RAFT 处理器经此投递）。
pub fn engine_cmd_tx() -> &'static std::sync::OnceLock<mpsc::Sender<EngineCmd>> {
    static TX: std::sync::OnceLock<mpsc::Sender<EngineCmd>> = std::sync::OnceLock::new();
    &TX
}

/// MSG_RAFT wire 帧投递入口（internal.rs 调用）。
pub fn deliver_wire(bytes: Vec<u8>) {
    if let Some(tx) = engine_cmd_tx().get() {
        let _ = tx.send(EngineCmd::RaftWire(bytes));
    }
}

/// 引擎是否启用（BASALT_CTRL_RAFT_ENGINE=raftrs）。
pub fn engine_enabled() -> bool {
    std::env::var("BASALT_CTRL_RAFT_ENGINE")
        .map(|v| v == "raftrs" || v == "1")
        .unwrap_or(false)
}

/// 共享句柄（调用面）：propose / 触发选举 / 只读状态克隆。
#[derive(Clone)]
pub struct RaftRsHandle {
    pub id: i32,
    tx: mpsc::Sender<EngineCmd>,
    pub shared: Arc<Mutex<ClusterState>>,
    pub is_leader: Arc<std::sync::atomic::AtomicBool>,
    pub leader_id: Arc<std::sync::atomic::AtomicI32>,
    pub applied: Arc<std::sync::atomic::AtomicU64>,
    /// 最近一次已知的 leader commit 水位（心跳/MsgApp 携带；leader 为自身
    /// committed）。applied >= leader_commit 即"日志已追平"——快照恢复的
    /// 节点在追平前对外服务会形成僵尸 leader（bounce r2c 实证）
    pub leader_commit: Arc<std::sync::atomic::AtomicU64>,
    /// raft 层是否已确认过 shared 状态（第一次 apply committed entry，含
    /// 当选 no-op）。启动时从 state.json 预加载的状态未经 raft 背书——
    /// 本节点死亡期间的 LeaderChange 不会重放（日志为空），未确认就对外
    /// 服务会形成僵尸 leader（bounce r1a 实证）
    pub raft_confirmed: Arc<std::sync::atomic::AtomicBool>,
}

impl RaftRsHandle {
    /// 提交元数据变更（非 leader 返回 Err，调用方重试路由到新主）。
    pub fn propose(&self, rec: ClusterRecord) -> Result<(), String> {
        let (tx, rx) = mpsc::channel();
        self.tx
            .send(EngineCmd::Propose(rec, tx))
            .map_err(|_| "engine stopped".to_string())?;
        rx.recv().map_err(|_| "engine dropped".to_string())?
    }

    pub fn trigger_elect(&self) {
        let _ = self.tx.send(EngineCmd::TriggerElect);
    }

    /// 真正停掉驱动线程（测试杀节点用）。仅 drop 句柄不够：
    /// engine_cmd_tx() OnceLock 持有同一通道的克隆，drop 不断开。
    pub fn stop(&self) {
        let (ack_tx, ack_rx) = mpsc::channel();
        if self.tx.send(EngineCmd::Stop(ack_tx)).is_ok() {
            let _ = ack_rx.recv_timeout(std::time::Duration::from_secs(2));
        }
    }

    pub fn applied_index(&self) -> u64 {
        self.applied.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// raft 消息路由：进程内通道（测试）或 TCP（跨进程，BASALT_CTRL_RAFT_ENGINE=raftrs）。
#[derive(Clone, Default)]
pub struct RaftRsRouter {
    inner: Arc<Mutex<std::collections::BTreeMap<i32, mpsc::Sender<Message>>>>,
    /// 跨进程模式：node_id → (host, internal_port)
    pub tcp_peers: Arc<Mutex<std::collections::BTreeMap<i32, String>>>,
}

impl RaftRsRouter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, id: i32, tx: mpsc::Sender<Message>) {
        self.inner.lock().unwrap().insert(id, tx);
    }

    fn route(&self, to: i32, msg: Message) {
        // 跨进程：TCP（connect 500ms 超时——防死节点阻塞驱动线程）
        if let Some(addr) = self.tcp_peers.lock().unwrap().get(&to) {
            let sa: std::net::SocketAddr = format!("{}:0", "127.0.0.1")
                .parse()
                .unwrap(); // placeholder; addr 已含 port
            let _ = sa;
            let addrs: Vec<_> = std::net::ToSocketAddrs::to_socket_addrs(addr.as_str())
                .map(|i| i.collect())
                .unwrap_or_default();
            for sa in &addrs {
                if let Ok(mut sock) = std::net::TcpStream::connect_timeout(sa, Duration::from_millis(300)) {
                    sock.set_write_timeout(Some(Duration::from_millis(300))).ok();
                    if let Ok(protobuf_bytes) = protobuf::Message::write_to_bytes(&msg) {
                        use std::io::Write;
                        let mut f = ((protobuf_bytes.len() + 1) as u32).to_be_bytes().to_vec();
                        f.push(crate::internal::MSG_RAFT);
                        f.extend_from_slice(&protobuf_bytes);
                        sock.write_all(&f).ok();
                    }
                    break;
                }
            }
            return;
        }
        // 进程内：通道
        if let Some(tx) = self.inner.lock().unwrap().get(&to).cloned() {
            let _ = tx.send(msg);
        }
    }
}

/// 单条对端 raft 消息的统一步进：日志回卷重同步预检（leader 侧）、
/// leader commit 水位跟踪（follower 侧）、catch_unwind 安全网。
/// 跨进程与进程内两条传输都必须经过这里。
fn step_incoming(
    node: &mut RawNode<MemStorage>,
    msg: Message,
    id: i32,
    leader_commit: &std::sync::atomic::AtomicU64,
) {
    use std::sync::atomic::Ordering;
    // 日志回卷重同步：无 WAL 的重启节点只恢复到快照点，而 leader 侧
    // Progress.matched 是单调的（update_committed 不下调）——残留旧值会
    // 让 leader 以为对端已追平，永远不重发缺失日志 → 对端元数据停在
    // 陈旧快照（bounce r1c 实证）。心跳响应的 commit 落后 matched =
    // 对端日志回卷的信号：下调 matched 并 probe（next_idx = matched+1）
    if node.raft.state == raft::StateRole::Leader
        && msg.get_msg_type() == MessageType::MsgHeartbeatResponse
        && msg.get_commit() > 0
    {
        if let Some(pr) = node.raft.mut_prs().get_mut(msg.get_from()) {
            if msg.get_commit() < pr.matched && pr.state != raft::ProgressState::Snapshot {
                eprintln!(
                    "RAFT-RESYNC id={} peer={} matched {} -> {}",
                    id,
                    msg.get_from(),
                    pr.matched,
                    msg.get_commit()
                );
                pr.matched = msg.get_commit();
                pr.become_probe();
            }
        }
    }
    // 水位跟踪：心跳/AppendEntries 携带 leader 的 committed，
    // 是"本节点日志应追平到哪里"的权威信号（追平门控依据）
    match msg.get_msg_type() {
        MessageType::MsgHeartbeat | MessageType::MsgAppend => {
            leader_commit.store(msg.get_commit(), Ordering::Relaxed);
        }
        _ => {}
    }
    // catch_unwind 安全网：重启节点日志落后而 leader 侧 Progress.matched
    // 残留旧值时，心跳的 commit 会越过本地 last_index 触发 raft-rs
    // commit_to panic（fatal!）。commit_to 在赋值前 panic，节点状态未被
    // 破坏——丢弃该消息即可，后续 MsgApp 补齐日志后自愈
    let stepped =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| node.step(msg)));
    if stepped.is_err() {
        eprintln!("RAFT-STEP-PANIC id={} dropped raft message", id);
    }
}

/// 单个 ready 周期的完整处理：propose 收割、ready 生成、持久化、
/// 消息三级路由、已提交应用、advance。独立成函数以便调用方整体
/// catch_unwind——raft-rs 在 follower→leader 迁移且仍有未持久化条目
/// 记录时会 fatal! 断言，panic 不允许杀死驱动线程。
#[allow(clippy::too_many_arguments)]
fn process_ready(
    node: &mut RawNode<MemStorage>,
    router: &RaftRsRouter,
    shared: &Arc<Mutex<ClusterState>>,
    applied: &std::sync::atomic::AtomicU64,
    applied_term: &std::sync::atomic::AtomicU64,
    raft_confirmed: &std::sync::atomic::AtomicBool,
    id: i32,
    pending: &mut Vec<(ClusterRecord, mpsc::Sender<Result<(), String>>)>,
) {
    use std::sync::atomic::Ordering;
    // leader 才能提交命令；follower 直接拒绝（调用方重试路由到新主）
    if node.raft.state == raft::StateRole::Leader {
        for (rec, reply) in pending.drain(..) {
            let data = serde_json::to_vec(&rec).unwrap();
            if let Err(e) = node.propose(b"ctrl".to_vec(), data) {
                let _ = reply.send(Err(format!("propose: {e}")));
            } else {
                let _ = reply.send(Ok(()));
            }
        }
    } else if !pending.is_empty() {
        for (_, reply) in pending.drain(..) {
            let _ = reply.send(Err("not leader".to_string()));
        }
    }

    let mut ready = node.ready();
    if std::env::var("BASALT_READY_PROBE").is_ok() {
        eprintln!(
            "PR id={} ce={} ents={} first={} committed={} persisted={} applied={}",
            id, ready.committed_entries().len(), ready.entries().len(),
            node.raft.raft_log.first_index(),
            node.raft.raft_log.committed,
            node.raft.raft_log.persisted,
            node.raft.raft_log.applied,
        );
    }
    {
        let n_msgs = ready.messages().len();
        let n_persisted = ready.persisted_messages().len();
        let n_entries = ready.entries().len();
        let n_committed = ready.committed_entries().len();
        if n_msgs + n_persisted + n_entries + n_committed > 0 {
            eprintln!("DRIVER id={} term={} role={:?} msgs={} persisted={} entries={} committed={}",
                id, node.raft.term, node.raft.state, n_msgs, n_persisted, n_entries, n_committed);
        }
    }
    // 注意：hs/entries 的持久化只在 ② 处执行一次——MemStorage::append
    // 无重叠检查，重复 append 会产生重复索引、破坏快照边界的日志定位

    // ① 非持久化消息先发（candidate 的投票请求等——不依赖落盘顺序）
    for msg in ready.take_messages() {
        router.route(msg.to as i32, msg);
    }

    // ② 持久化 hard state / entries（MemStorage 内存等价物）
    if let Some(hs) = ready.hs() {
        node.mut_store().wl().set_hardstate(hs.clone());
    }
    if !ready.entries().is_empty() {
        node.mut_store().wl().append(ready.entries());
    }

    // ③ 持久化后再发的消息（leader 的 append/heartbeat——依赖落盘顺序）
    for msg in ready.persisted_messages().to_vec() {
        router.route(msg.to as i32, msg);
    }

    // ④⑤ 应用已提交 + advance。raft-rs 0.7 契约：committed entries 分
    // 两批交付——Ready（persist 前）与 advance 的 LightReady（续批）。
    // 两批都必须应用；漏掉 LightReady 批会丢已提交条目（游标照常推进、
    // 状态机落后——restore_catches_up_missed_entries 实证）
    {
        let mut apply_entries = |entries: &[raft::eraftpb::Entry],
                                 shared: &Arc<Mutex<ClusterState>>,
                                 applied: &std::sync::atomic::AtomicU64,
                                 applied_term: &std::sync::atomic::AtomicU64,
                                 raft_confirmed: &std::sync::atomic::AtomicBool,
                                 id: i32| {
            for e in entries {
                raft_confirmed.store(true, Ordering::Relaxed);
                applied_term.store(e.get_term(), Ordering::Relaxed);
                applied.store(e.get_index(), Ordering::Relaxed);
                if e.data.is_empty() {
                    continue;
                }
                match serde_json::from_slice::<ClusterRecord>(&e.data) {
                    Ok(rec) => {
                        eprintln!("APPLY id={} idx={} rec={:?}", id, e.get_index(), rec);
                        let mut st = shared.lock().unwrap();
                        st.apply(&rec);
                        st.version = e.get_index();
                    }
                    Err(e2) => {
                        eprintln!("APPLY id={} idx={} PARSE-ERR {}", id, e.get_index(), e2);
                    }
                }
            }
        };
        apply_entries(
            ready.committed_entries(),
            shared, applied, applied_term, raft_confirmed, id,
        );
        let mut light = node.advance(ready);
        apply_entries(
            light.committed_entries(),
            shared, applied, applied_term, raft_confirmed, id,
        );
        for msg in light.take_messages() {
            router.route(msg.to as i32, msg);
        }
    }
}

fn driver(
    id: i32,
    mut node: RawNode<MemStorage>,
    router: RaftRsRouter,
    cmd_rx: mpsc::Receiver<EngineCmd>,
    mut msg_rx: mpsc::Receiver<Message>,
    shared: Arc<Mutex<ClusterState>>,
    is_leader: Arc<std::sync::atomic::AtomicBool>,
    leader_id: Arc<std::sync::atomic::AtomicI32>,
    applied: Arc<std::sync::atomic::AtomicU64>,
    applied_term: Arc<std::sync::atomic::AtomicU64>,
    leader_commit: Arc<std::sync::atomic::AtomicU64>,
    raft_confirmed: Arc<std::sync::atomic::AtomicBool>,
    snap_dir: PathBuf,
) {
    use std::sync::atomic::Ordering;
    let mut last_tick = Instant::now();
    let mut pending: Vec<(ClusterRecord, mpsc::Sender<Result<(), String>>)> = Vec::new();
    let mut leader_known: Option<u64> = None;
    let mut last_role = node.raft.state;
    let mut last_term = node.raft.term;
    let mut diag_every = 0u32;
    let mut wire_msgs: Vec<Message> = Vec::new();

    loop {
        // 命令（非阻塞收割）
        let mut stop = false;
        loop {
            match cmd_rx.try_recv() {
                Ok(EngineCmd::TriggerElect) => {
                    let _ = node.campaign();
                }
                Ok(EngineCmd::RaftWire(bytes)) => {
                    // protobuf-codec 原生编解码（raft-proto Message）。
                    // 跨进程消息与进程内 msg_rx 走同一步进路径（防护逻辑
                    // 必须对两条传输一致——此前只覆盖进程内通道）
                    if let Ok(msg) = protobuf::Message::parse_from_bytes(&bytes) {
                        wire_msgs.push(msg);
                    }
                }
                Ok(EngineCmd::Propose(rec, reply)) => pending.push((rec, reply)),
                Ok(EngineCmd::Stop(ack)) => {
                    let _ = ack.send(());
                    stop = true;
                    break;
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    stop = true;
                    break;
                }
                Err(mpsc::TryRecvError::Empty) => break,
            }
        }
        if stop {
            return;
        }

        // 对端 raft 消息（AppendEntries/Vote/...）步进——跨进程（RaftWire
        // 解析而来）与进程内（msg_rx）统一走 step_incoming
        for msg in wire_msgs.drain(..) {
            step_incoming(&mut node, msg, id, &leader_commit);
        }
        loop {
            match msg_rx.try_recv() {
                Ok(msg) => step_incoming(&mut node, msg, id, &leader_commit),
                Err(mpsc::TryRecvError::Empty) | Err(mpsc::TryRecvError::Disconnected) => break,
            }
        }

        // tick 节拍（50ms）
        let do_tick = last_tick.elapsed() >= Duration::from_millis(50);
        if do_tick {
            node.tick();
            last_tick = Instant::now();
        }
        // 诊断：非 Leader 节点的日志位置节流快照（~2s 一次）
        if node.raft.state != raft::StateRole::Leader {
            diag_every += 1;
            if diag_every >= 200 {
                diag_every = 0;
                eprintln!(
                    "FOLLOWER-DIAG id={id} term={term} leader={leader} last={last} committed={committed} first={first} persisted={persisted}",
                    id = id, term = node.raft.term, leader = node.raft.leader_id,
                    last = node.raft.raft_log.last_index(),
                    committed = node.raft.raft_log.committed,
                    first = node.raft.raft_log.first_index(),
                    persisted = node.raft.raft_log.persisted,
                );
            }
        }

        // ready 周期整体 catch_unwind：内部 panic 只丢弃本周期，不杀死驱动
        let processed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            process_ready(
                &mut node,
                &router,
                &shared,
                &applied,
                &applied_term,
                &raft_confirmed,
                id,
                &mut pending,
            );
        }));
        if processed.is_err() {
            eprintln!("READY-PANIC id={} recovered, continuing", id);
        }
        leader_known = Some(node.raft.leader_id);
        let im_leader = node.raft.state == raft::StateRole::Leader;
        if im_leader {
            leader_commit.store(node.raft.raft_log.committed, Ordering::Relaxed);
        }
        is_leader.store(im_leader, Ordering::Relaxed);
        if im_leader {
            leader_id.store(id, Ordering::Relaxed);
        } else if node.raft.leader_id != 0 {
            // raft.leader_id 是 raft id（=broker+1，INVALID_ID=0 偏移）——
            // 句柄对外统一 broker id，这里必须减回，否则 propose 转发指错节点
            leader_id.store(node.raft.leader_id as i32 - 1, Ordering::Relaxed);
        } else {
            leader_id.store(-1, Ordering::Relaxed);
        }
        if node.raft.state != last_role || node.raft.term != last_term {
            eprintln!("ENGINE id={id} role={:?} term={} leader={:?}", node.raft.state, node.raft.term, node.raft.leader_id);
            last_role = node.raft.state;
            last_term = node.raft.term;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// 启动引擎驱动线程。返回共享句柄（router 由调用方统一装配三节点）。
pub fn spawn(id: i32, peers: Vec<i32>, router: RaftRsRouter) -> RaftRsHandle {
    // 目录按进程隔离：state.json 残留会预加载旧状态（version>0），
    // 使测试的 leader 等待循环提前通过、propose 落空
    spawn_with_dir(
        id,
        peers,
        router,
        std::env::temp_dir().join(format!("basalt-ctrl-raftrs-{}-{id}", std::process::id())),
    )
}

/// 带快照目录的装配（data/ctrl-raftrs/）。
pub fn spawn_with_dir(id: i32, peers: Vec<i32>, router: RaftRsRouter, dir: PathBuf) -> RaftRsHandle {
    let (cmd_tx, cmd_rx) = mpsc::channel();
    let _ = engine_cmd_tx().set(cmd_tx.clone());
    let (msg_tx, msg_rx) = mpsc::channel();
    std::fs::create_dir_all(&dir).ok();
    // 重启恢复：state.json + state.meta.json（applied index/term）→ 灌回
    // MemStorage 快照（日志从快照点续传）。只恢复快照位置而不灌回会导致
    // leader 心跳的 commit 越过空日志（Progress.matched 残留旧值），
    // raft-rs commit_to 直接 panic 杀死驱动线程（bounce r1 实证）。
    // 顺序保证：先写 state.json 后写 meta——崩溃残留的中间态只会让
    // 重放多覆盖几条幂等记录（Register/CreateTopic 去重/LeaderChange 同值）
    let file_state: Option<ClusterState> = std::fs::read_to_string(dir.join("state.json"))
        .ok()
        .and_then(|d| serde_json::from_str(&d).ok());
    let file_meta: Option<(u64, u64)> = std::fs::read_to_string(dir.join("state.meta.json"))
        .ok()
        .and_then(|d| {
            serde_json::from_str::<serde_json::Value>(&d).ok().and_then(|v| {
                let applied = v.get("applied")?.as_u64()?;
                let term = v.get("term")?.as_u64()?;
                Some((applied, term))
            })
        });
    let shared = Arc::new(Mutex::new(file_state.clone().unwrap_or_default()));
    let is_leader = Arc::new(std::sync::atomic::AtomicBool::new(false));
    // leader_id 语义 = broker id；-1 = 未知（选举中）。broker 0 是合法值，
    // 不能作哨兵——否则"leader 是 node0"会被误判为未知（propose 永不转发）
    let leader_id = Arc::new(std::sync::atomic::AtomicI32::new(-1));
    let applied = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let applied_term = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let leader_commit = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let shared_clone = shared.clone();
    let is_leader_t = is_leader.clone();
    let leader_id_t = leader_id.clone();
    let applied_t = applied.clone();
    let applied_term_t = applied_term.clone();
    let leader_commit_t = leader_commit.clone();
    // 快照恢复 = 状态已经 raft 背书（正是 index=applied 处的已提交快照）
    let raft_confirmed = Arc::new(std::sync::atomic::AtomicBool::new(file_meta.is_some()));
    let raft_confirmed_t = raft_confirmed.clone();
    let restore_meta = file_meta.filter(|_| file_state.is_some());
    // applied 原子起点 = 快照 index（applied >= leader_commit 门控依赖
    // 此值的正确起点；快照恢复时 driver 尚未 apply 任何新条目）
    if let Some((idx, term)) = restore_meta {
        applied.store(idx, std::sync::atomic::Ordering::Relaxed);
        applied_term.store(term, std::sync::atomic::Ordering::Relaxed);
    }
    let snapshot_dir = dir.clone();

    // raft_id 偏移（raft-rs INVALID_ID=0）：raft_id = broker_id + 1
    let raft_id = id + 1;
    let raft_peers: Vec<i32> = peers.iter().map(|&p| p + 1).collect();
    router.register(raft_id, msg_tx);

    std::thread::spawn(move || {
        let mut cfg = Config::new(raft_id as u64);
        cfg.heartbeat_tick = 2;
        // 契约：快照恢复必须告知已 apply 的位置——ready 的 committed
        // 交付游标（commit_since_index）以它为初值，缺省会从 0 起步
        if let Some((applied_idx, _)) = restore_meta {
            cfg.applied = applied_idx;
        }
        // pre-vote（thesis §9.6）：重启/追平中的节点以空日志 campaign 时
        // 不抬 term——否则会把在位 leader 打下台而自己选不赢（disruption
        // 活锁，bounce 场景 r0c 实证）
        cfg.pre_vote = true;
        // election_tick 保持默认（ raft-rs 内部随机化 election_timeout =
        // rand(election_tick, 2 * election_tick)），不同节点自然去同步
        cfg.validate().unwrap();

        let mem_store = MemStorage::new();
        if let Some((applied_idx, term)) = restore_meta {
            if applied_idx > 0 {
                let mut snap = Snapshot::default();
                snap.mut_metadata().set_index(applied_idx);
                snap.mut_metadata().set_term(term);
                snap.mut_metadata().set_conf_state(ConfState {
                    voters: raft_peers.iter().map(|x| *x as u64).collect(),
                    ..Default::default()
                });
                if let Err(e) = mem_store.wl().apply_snapshot(snap) {
                    eprintln!("ENGINE id={} snapshot restore failed: {}", id, e);
                } else {
                    eprintln!("ENGINE id={} restored snapshot index={} term={}", id, applied_idx, term);
                }
            }
        }
        mem_store.wl().set_conf_state(ConfState {
            voters: raft_peers.iter().map(|x| *x as u64).collect(),
            ..Default::default()
        });
        let mut node = RawNode::new(&cfg, mem_store, &logger()).unwrap();

        // 启动选举触发：仅冷启动节点主动 campaign（去同步 jitter）。
        // 快照恢复的重新加入节点不得主动 campaign——它与 leader 日志等长
        // 时 pre-vote 会通过、打断在位 leader（bounce r2 扰动实证）；
        // 其选举需求由 tick 的 election timeout 自然触发兜底。
        if restore_meta.is_none() {
            let jitter = (std::process::id() % 300 + 50) as u64;
            std::thread::sleep(Duration::from_millis(jitter));
            let _ = node.campaign();
        }

        let shared_snap = shared_clone.clone();
        let last_ver = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let last_ver_t = last_ver.clone();
        let applied_term_s = applied_term_t.clone();
        let snap_dir = snapshot_dir.clone();
        let snap_dir_t = snap_dir.clone();
        std::thread::spawn(move || {
            // 快照落盘循环：状态 version 有变更才写（每 2s 检查）。
            // 先写 state.json 再写 meta（applied/term）——崩溃残留只会让
            // 恢复后多重放幂等记录，绝不丢记录
            loop {
                std::thread::sleep(Duration::from_secs(2));
                let st = shared_snap.lock().unwrap().clone();
                if st.version != last_ver_t.load(std::sync::atomic::Ordering::Relaxed) {
                    let _ = std::fs::write(
                        snap_dir_t.join("state.json"),
                        serde_json::to_string(&st).unwrap(),
                    );
                    let meta = serde_json::json!({
                        "applied": st.version,
                        "term": applied_term_s.load(std::sync::atomic::Ordering::Relaxed),
                    });
                    let _ = std::fs::write(snap_dir_t.join("state.meta.json"), meta.to_string());
                    last_ver_t.store(st.version, std::sync::atomic::Ordering::Relaxed);
                }
            }
        });
        driver(
            raft_id,
            node,
            router,
            cmd_rx,
            msg_rx,
            shared_clone,
            is_leader_t,
            leader_id_t,
            applied_t,
            applied_term_t,
            leader_commit_t,
            raft_confirmed_t,
            snap_dir.clone(),
        );
    });

    RaftRsHandle { id, tx: cmd_tx, shared, is_leader, leader_id, applied, leader_commit, raft_confirmed }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// ADR-16 双引擎对齐验收：raft-rs 三节点——campaign 选举、复制收敛、
    /// leader 死亡 → watchdog 选举 → 续写。
    #[test]
    fn three_node_campaign_replicates_and_recovers() {
        let router = RaftRsRouter::new();
        let ids = [1, 2, 3];
        let mut handles: BTreeMap<i32, RaftRsHandle> = BTreeMap::new();
        for &id in &ids {
            handles.insert(id, spawn(id, ids.to_vec(), router.clone()));
        }
        time::sleep(Duration::from_millis(200));

        // node1 发起选举
        handles[&1].trigger_elect();
        // 等 leader 稳定（当选 no-op 提交 = applied>=1，比 shared.version
        // 更及时：version 只在首个真实记录落状态机时推进）
        for _ in 0..100 {
            if handles[&1].applied_index() >= 1 {
                break;
            }
            time::sleep(Duration::from_millis(100));
        }

        // 经 leader 提交 5 条（propose 失败重试，容忍选举竞速）
        for i in 0..5 {
            let mut ok = false;
            for _ in 0..50 {
                for h in handles.values() {
                    if h.propose(ClusterRecord::LeaderChange {
                        topic: format!("t{i}"),
                        partition: 0,
                        leader: 1,
                        epoch: i + 1,
                    })
                    .is_ok()
                    {
                        ok = true;
                        break;
                    }
                }
                if ok {
                    break;
                }
                time::sleep(Duration::from_millis(100));
            }
            assert!(ok, "propose {i} 失败");
            // 等待复制收敛
            loop {
                let applied = handles.values().map(|h| h.applied_index()).min().unwrap();
                if applied >= (i + 1) as u64 {
                    break;
                }
                time::sleep(Duration::from_millis(50));
            }
        }

        // 杀 leader（node1）：显式停驱动线程（drop 句柄不断开 OnceLock 克隆）
        handles[&1].stop();
        handles.remove(&1);
        // 剩余节点 watchdog 选举 + 续写
        let mut ok = false;
        for h in handles.values() {
            h.trigger_elect();
        }
        for _ in 0..50 {
            for h in handles.values() {
                if h.propose(ClusterRecord::LeaderChange {
                    topic: "after".into(),
                    partition: 0,
                    leader: 2,
                    epoch: 99,
                })
                .is_ok()
                {
                    ok = true;
                    break;
                }
            }
            if ok {
                break;
            }
            time::sleep(Duration::from_millis(200));
        }
        assert!(ok, "10s 内多数派未恢复写入");
    }
}

// 测试用轮询辅助（无 tokio 依赖）
mod time {
    use std::thread;
    use std::time::Duration;
    pub fn sleep(ms: Duration) {
        thread::sleep(ms);
    }
}

#[cfg(test)]
mod restore_tests {
    use super::*;
    use std::collections::BTreeMap;

    /// 缺陷回归锁：无 WAL 快照恢复后的日志追赶——重启节点必须重放
    /// 死亡期间提交的全部条目（内容与存活节点一致），不允许跳条。
    #[test]
    fn restore_catches_up_missed_entries() {
        let dir = std::env::temp_dir().join(format!("basalt-restore-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).ok();
        let router = RaftRsRouter::new();
        let ids = [1, 2, 3];
        let mut handles: BTreeMap<i32, RaftRsHandle> = BTreeMap::new();
        for &id in &ids {
            let d = dir.join(format!("node{id}"));
            handles.insert(id, spawn_with_dir(id, ids.to_vec(), router.clone(), d));
        }
        handles[&1].trigger_elect();
        for _ in 0..50 {
            if handles.values().all(|h| h.applied_index() >= 1) {
                break;
            }
            time::sleep(Duration::from_millis(100));
        }
        // 提交 2 条（等快照线程落盘：2s 周期）。提案 election-agnostic
        for i in 0..2 {
            for _ in 0..50u32 {
                if handles.values().any(|h| h.propose(ClusterRecord::LeaderChange {
                    topic: "t".into(), partition: i, leader: 1, epoch: i + 1,
                }).is_ok()) {
                    break;
                }
                time::sleep(Duration::from_millis(100));
            }
        }
        for _ in 0..50 {
            if handles.values().all(|h| h.applied_index() >= 3) {
                break;
            }
            time::sleep(Duration::from_millis(100));
        }
        time::sleep(Duration::from_millis(2500)); // 快照落盘窗口
        let v_before: Vec<u64> = handles.values().map(|h| h.applied_index()).collect();

        // 杀 node3 → 多数派（1,2）继续提交 2 条（idx 4、5）
        handles[&3].stop();
        handles.remove(&3);
        time::sleep(Duration::from_millis(100));
        for i in 0..2 {
            // 选举竞速下 leader 可能换人（重启节点以同等日志参选合法）——
            // 向任一存活节点提案直至成功
            let mut ok = false;
            for _ in 0..80u32 {
                for h in handles.values() {
                    if h.propose(ClusterRecord::LeaderChange {
                        topic: "t".into(), partition: 9, leader: 1, epoch: 100 + i,
                    }).is_ok() {
                        ok = true;
                        break;
                    }
                }
                if ok { break; }
                time::sleep(Duration::from_millis(100));
            }
            assert!(ok, "多数派 propose {i} 失败");
        }
        for _ in 0..50 {
            if handles.values().all(|h| h.applied_index() >= 5) {
                break;
            }
            time::sleep(Duration::from_millis(100));
        }

        // 重启 node3（同目录 = 快照恢复）
        time::sleep(Duration::from_millis(200));
        let h3 = spawn_with_dir(3, ids.to_vec(), router.clone(), dir.join("node3"));
        let mut converged = false;
        for _ in 0..100 {
            let v3 = h3.applied_index();
            let want = handles.values().map(|h| h.applied_index()).max().unwrap_or(0);
            if v3 >= want && want >= 5 {
                converged = true;
                break;
            }
            time::sleep(Duration::from_millis(100));
        }
        let snap3 = h3.shared.lock().unwrap().clone();
        let snap1 = handles[&1].shared.lock().unwrap().clone();
        let vmax = handles.values().map(|h| h.applied_index()).max().unwrap_or(0);
        assert!(converged, "node3 未追平: v3={} vmax={} before={:?}", h3.applied_index(), vmax, v_before);
        assert_eq!(
            snap3.assignments.len(), snap1.assignments.len(),
            "恢复后 assignment 数不一致"
        );
        for a in &snap1.assignments {
            let b = snap3.assignments.iter()
                .find(|x| x.topic == a.topic && x.partition == a.partition)
                .unwrap_or_else(|| panic!("node3 缺 assignment {}#{}", a.topic, a.partition));
            assert_eq!((a.leader, a.epoch), (b.leader, b.epoch), "leader/epoch 不一致");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
