//! raft-rs（TiKV）引擎——驱动式 RawNode（ADR-16）。
//!
//! 与 openraft（自驱事件循环）互补：本引擎由集成方驱动——
//! tick() 推进逻辑时钟、ready() 取出待持久化/待发送/已提交三队列、
//! advance() 确认。MemStorage 起步（v1：控制器重启后由 leader 日志重放
//! 追平；快照持久化为后续增强）。

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
        .map(|v| v == "raftrs")
        .unwrap_or(false)
}

/// 共享句柄（调用面）：propose / 触发选举 / 只读状态克隆。
#[derive(Clone)]
pub struct RaftRsHandle {
    pub id: i32,
    tx: mpsc::Sender<EngineCmd>,
    pub shared: Arc<Mutex<ClusterState>>,
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

/// 进程内消息路由（raft-rs Message）。
#[derive(Clone, Default)]
pub struct RaftRsRouter {
    inner: Arc<Mutex<std::collections::BTreeMap<i32, mpsc::Sender<Message>>>>,
}

impl RaftRsRouter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, id: i32, tx: mpsc::Sender<Message>) {
        self.inner.lock().unwrap().insert(id, tx);
    }

    fn route(&self, to: i32, msg: Message) {
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
) {
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
        if last_tick.elapsed() >= Duration::from_millis(50) {
            node.tick();
            last_tick = Instant::now();
        }

        // 无主 watchdog：跟随者带待提交命令时发起选举
        if node.raft.leader_id == 0
            && node.raft.state == raft::StateRole::Follower
            && !pending.is_empty()
        {
            let _ = node.campaign();
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
        if !ready.messages().is_empty() || !ready.entries().is_empty() {
            eprintln!(
                "READY id={id} msgs={} entries={} committed={} hs={:?}",
                ready.messages().len(),
                ready.entries().len(),
                ready.committed_entries().len(),
                ready.hs().map(|h| (h.term, h.vote, h.commit))
            );
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
        for msg in ready.persisted_messages() {
            router.route(msg.to as i32, msg.clone());
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
            }
        }

        leader_known = Some(node.raft.leader_id);
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
    let (cmd_tx, cmd_rx) = mpsc::channel();
    let _ = engine_cmd_tx().set(cmd_tx.clone());
    let (msg_tx, msg_rx) = mpsc::channel();
    let shared = Arc::new(Mutex::new(ClusterState::default()));
    let shared_clone = shared.clone();

    router.register(id, msg_tx);

    std::thread::spawn(move || {
        let mut cfg = Config::new(id as u64);
        cfg.heartbeat_tick = 2;
        cfg.election_tick = 10;
        cfg.validate().unwrap();

        let mem_store = MemStorage::new();
        // 初始投票成员 = 全部 peers（v1 无成员变更）
        mem_store.wl().set_conf_state(ConfState {
            voters: peers.iter().map(|&x| x as u64).collect(),
            ..Default::default()
        });
        let node = RawNode::new(&cfg, mem_store, &logger()).unwrap();

        driver(id, node, router, cmd_rx, msg_rx, shared_clone);
    });

    RaftRsHandle { id, tx: cmd_tx, shared }
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
