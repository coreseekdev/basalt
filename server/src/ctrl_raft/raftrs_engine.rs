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

    pub fn applied_index(&self) -> u64 {
        self.shared.lock().unwrap().version
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
    snap_dir: PathBuf,
) {
    use std::sync::atomic::Ordering;
    let mut last_tick = Instant::now();
    let mut pending: Vec<(ClusterRecord, mpsc::Sender<Result<(), String>>)> = Vec::new();
    let mut leader_known: Option<u64> = None;
    let mut last_role = node.raft.state;
    let mut last_term = node.raft.term;

    loop {
        // 命令（非阻塞收割）
        let mut stop = false;
        loop {
            match cmd_rx.try_recv() {
                Ok(EngineCmd::TriggerElect) => {
                    let _ = node.campaign();
                }
                Ok(EngineCmd::RaftWire(bytes)) => {
                    // protobuf-codec 原生编解码（raft-proto Message）
                    if let Ok(msg) = protobuf::Message::parse_from_bytes(&bytes) {
                        let _ = node.step(msg);
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

        // 对端 raft 消息（AppendEntries/Vote/...）步进
        loop {
            match msg_rx.try_recv() {
                Ok(msg) => {
                    if let Err(e) = node.step(msg) {
                        let _ = e; // 过期/非法消息（raft-rs 自身校验）
                    }
                }
                Err(mpsc::TryRecvError::Empty) | Err(mpsc::TryRecvError::Disconnected) => break,
            }
        }

        // tick 节拍（50ms）
        let do_tick = last_tick.elapsed() >= Duration::from_millis(50);
        if do_tick {
            node.tick();
            last_tick = Instant::now();
        }
        // 诊断：非 Leader 节点的选举计时器
        if node.raft.state != raft::StateRole::Leader {
            eprintln!(
                "TICK-DIAG id={id} term={term} role={role:?} leader={leader} msgs={msgs_len} tick_flag={do_tick}",
                id = id, term = node.raft.term, role = node.raft.state,
                leader = node.raft.leader_id, msgs_len = node.raft.msgs.len(),
                do_tick = do_tick,
            );
        }

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
                let _ = reply.send(Err(format!("not leader (leader={leader_known:?})")));
            }
        }

        let mut ready = node.ready();
        {
            let n_msgs = ready.messages().len();
            let n_persisted = ready.persisted_messages().len();
            let n_entries = ready.entries().len();
            let n_committed = ready.committed_entries().len();
            if id == 1 && n_msgs + n_persisted + n_entries + n_committed > 0 {
                eprintln!("DRIVER id={} term={} role={:?} msgs={} persisted={} entries={} committed={}",
                    id, node.raft.term, node.raft.state, n_msgs, n_persisted, n_entries, n_committed);
            }
        }
        if !ready.messages().is_empty() || !ready.entries().is_empty() {
        }

        // 持久化 hard state / entries（MemStorage 内存等价物）
        if let Some(hs) = ready.hs() {
            node.mut_store().wl().set_hardstate(hs.clone());
        }
        if !ready.entries().is_empty() {
            node.mut_store().wl().append(ready.entries());
        }

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

        // ④ 应用已提交
        if !ready.committed_entries().is_empty() {
            for e in ready.committed_entries().to_vec() {
                if e.data.is_empty() {
                    continue; // 空心跳占位
                }
                if let Ok(rec) = serde_json::from_slice::<ClusterRecord>(&e.data) {
                    let mut st = shared.lock().unwrap();
                    st.apply(&rec);
                    st.version = e.get_index();
                }
                applied.store(e.get_index(), Ordering::Relaxed);
            }
        }

        leader_known = Some(node.raft.leader_id);
        let im_leader = node.raft.state == raft::StateRole::Leader;
        is_leader.store(im_leader, Ordering::Relaxed);
        if im_leader {
            leader_id.store(id, Ordering::Relaxed);
        } else if node.raft.leader_id != 0 {
            leader_id.store(node.raft.leader_id as i32, Ordering::Relaxed);
        }
        if node.raft.state != last_role || node.raft.term != last_term {
            eprintln!("ENGINE id={id} role={:?} term={} leader={:?}", node.raft.state, node.raft.term, node.raft.leader_id);
            last_role = node.raft.state;
            last_term = node.raft.term;
        }
        // ⑤ advance 返回剩余 light（消息 + 提交增量）
        let light = node.advance(ready);
        for msg in light.messages() {
            router.route(msg.to as i32, msg.clone());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// 启动引擎驱动线程。返回共享句柄（router 由调用方统一装配三节点）。
pub fn spawn(id: i32, peers: Vec<i32>, router: RaftRsRouter) -> RaftRsHandle {
    spawn_with_dir(id, peers, router, std::env::temp_dir().join(format!("basalt-ctrl-raftrs-{id}")))
}

/// 带快照目录的装配（data/ctrl-raftrs/）。
pub fn spawn_with_dir(id: i32, peers: Vec<i32>, router: RaftRsRouter, dir: PathBuf) -> RaftRsHandle {
    let (cmd_tx, cmd_rx) = mpsc::channel();
    let _ = engine_cmd_tx().set(cmd_tx.clone());
    let (msg_tx, msg_rx) = mpsc::channel();
    std::fs::create_dir_all(&dir).ok();
    // 重启恢复：快照文件优先（applied 状态），否则空态由 leader 日志重放追平
    let shared = Arc::new(Mutex::new(
        std::fs::read_to_string(dir.join("state.json"))
            .ok()
            .and_then(|d| serde_json::from_str::<ClusterState>(&d).ok())
            .unwrap_or_default(),
    ));
    let is_leader = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let leader_id = Arc::new(std::sync::atomic::AtomicI32::new(0));
    let applied = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let shared_clone = shared.clone();
    let is_leader_t = is_leader.clone();
    let leader_id_t = leader_id.clone();
    let applied_t = applied.clone();
    let snapshot_dir = dir.clone();

    // raft_id 偏移（raft-rs INVALID_ID=0）：raft_id = broker_id + 1
    let raft_id = id + 1;
    let raft_peers: Vec<i32> = peers.iter().map(|&p| p + 1).collect();
    router.register(raft_id, msg_tx);

    std::thread::spawn(move || {
        let mut cfg = Config::new(raft_id as u64);
        cfg.heartbeat_tick = 2;
        // election_tick 保持默认（ raft-rs 内部随机化 election_timeout =
        // rand(election_tick, 2 * election_tick)），不同节点自然去同步
        cfg.validate().unwrap();

        let mem_store = MemStorage::new();
        mem_store.wl().set_conf_state(ConfState {
            voters: raft_peers.iter().map(|x| *x as u64).collect(),
            ..Default::default()
        });
        let mut node = RawNode::new(&cfg, mem_store, &logger()).unwrap();

        // 启动选举触发：raft-rs 0.7 的 tick_election 依赖 promotable
        // （初始 ConfState 投票者自动满足），但首次 tick 前需显式触发
        // 以避免所有节点同时 campaign（split vote）。随机延迟去同步。
        let jitter = (std::process::id() % 300 + 50) as u64;
        std::thread::sleep(Duration::from_millis(jitter));
        let _ = node.campaign();

        let shared_snap = shared_clone.clone();
        let last_ver = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let last_ver_t = last_ver.clone();
        let snap_dir = snapshot_dir.clone();
        let snap_dir_t = snap_dir.clone();
        std::thread::spawn(move || {
            // 快照落盘循环：状态 version 有变更才写（每 2s 检查）
            loop {
                std::thread::sleep(Duration::from_secs(2));
                let st = shared_snap.lock().unwrap().clone();
                if st.version != last_ver_t.load(std::sync::atomic::Ordering::Relaxed) {
                    let _ = std::fs::write(
                        snap_dir_t.join("state.json"),
                        serde_json::to_string(&st).unwrap(),
                    );
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
            snap_dir.clone(),
        );
    });

    RaftRsHandle { id, tx: cmd_tx, shared, is_leader, leader_id, applied }
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
        // 等 leader 稳定（node1 自我为胜）
        for _ in 0..50 {
            if handles[&1].shared.lock().unwrap().version >= 1 {
                break;
            }
            time::sleep(Duration::from_millis(100));
        }

        // 经 leader 提交 5 条
        for i in 0..5 {
            let mut ok = false;
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

        // 杀 leader（node1）：丢句柄（线程随通道断开退出）
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
