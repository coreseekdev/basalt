//! 内部 RPC（多节点 POC）：
//! - 帧格式：[len:u32][type:u8][payload]，全大端；
//! - 消息：Register/Heartbeat/MetaSync/CreateTopic/FetchSlice；
//! - 控制器角色（最小 node id 承担）：独占 ClusterState + record 日志，
//!   心跳超时 → LeaderChange（epoch+1，副本轮转）。

use basalt_storage::pool::BufferPool;
use basalt_metadata::cluster::{BrokerInfo, ClusterRecord, ClusterState};
use bytes::{BufMut, Bytes, BytesMut};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub const MSG_REGISTER: u8 = 1;
pub const MSG_HEARTBEAT: u8 = 2;
pub const MSG_META_SYNC: u8 = 3;
pub const MSG_CREATE_TOPIC: u8 = 4;
pub const MSG_FETCH_SLICE: u8 = 5;
pub const MSG_TRANSFER: u8 = 6;
/// raft 引擎 wire 帧（prost/protobuf 编码的 raft::eraftpb::Message）
pub const MSG_RAFT: u8 = 7;
/// 引擎模式 propose 转发：payload = encode_record(rec)；应答 1B 状态（1=ok）
pub const MSG_CTRL_PROPOSE: u8 = 8;

/// 事务 marker 下发（ADR-18 §5，块 c）：coordinator 路由器 → 远端 leader。
pub const MSG_WRITE_TXN_MARKER: u8 = 9;
/// TxnOffsetCommit 代理（ADR-18 §7）：组协调器节点 → controller 事务协调器。
pub const MSG_TXN_OFFSET_COMMIT: u8 = 10;
/// B5 多节点 share：成员存在性校验（数据 leader → 组协调器）。
pub const MSG_SHARE_VALIDATE: u8 = 11;
/// B5 多节点 share：交付事件转发（数据 leader → share-state topic leader，
/// 落地节点经本地 sink 持久化）。
pub const MSG_SHARE_EVENT: u8 = 12;

/// 分区注入（T-M2.5）：本节点拒绝与之通信的对端集合（双向断边）。
/// BASALT_BLOCK_PEERS="1,2" —— 心跳/元数据/FetchSlice 全部断开。
pub fn blocked_peers() -> &'static std::collections::HashSet<i32> {
    static BLOCKED: std::sync::OnceLock<std::collections::HashSet<i32>> = std::sync::OnceLock::new();
    BLOCKED.get_or_init(|| {
        std::env::var("BASALT_BLOCK_PEERS")
            .map(|v| v.split(',').filter_map(|x| x.trim().parse().ok()).collect())
            .unwrap_or_default()
    })
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct FetchSliceResult {
    pub error: i16,
    pub high_watermark: i64,
    pub leader_next_offset: i64,
    pub data: Bytes,
}

/// 内部 RPC 客户端（到控制器 / 到 leader）。
pub struct InternalClient {
    addr: String,
}

impl InternalClient {
    pub fn new(addr: String) -> InternalClient {
        InternalClient { addr }
    }

    async fn call(&self, msg_type: u8, payload: &[u8]) -> std::io::Result<Bytes> {
        let sock = tokio::net::TcpStream::connect(&self.addr).await?;
        let mut sock = sock;
        // 连接级令牌（B1 v2）：HMAC 质询-应答——令牌不上线。
        // 服务端配置了令牌时必须完成握手；未配置时跳过。
        if let Ok(token) = std::env::var("BASALT_INTERNAL_TOKEN") {
            if !token.is_empty() {
                let mut auth_frame = BytesMut::with_capacity(1 + 5);
                auth_frame.put_u32(2u32);
                auth_frame.put_u8(MSG_AUTH);
                auth_frame.put_u8(AUTH_V2_REQUEST);
                sock.write_all(&auth_frame).await?;
                // 读挑战帧 [len][MSG_AUTH][0x00][challenge 32B]
                let mut len_buf = [0u8; 4];
                sock.read_exact(&mut len_buf).await?;
                let clen = u32::from_be_bytes(len_buf) as usize;
                if clen != 2 + AUTH_CHALLENGE_LEN {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "bad challenge frame",
                    ));
                }
                let mut cmsg = vec![0u8; clen];
                sock.read_exact(&mut cmsg).await?;
                if cmsg[0] != MSG_AUTH || cmsg[1] != 0x00 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        "internal auth rejected by peer",
                    ));
                }
                let mac = hmac_internal_token(&token, &cmsg[2..]);
                let mut verify = BytesMut::with_capacity(2 + 32 + 5);
                verify.put_u32((2 + AUTH_CHALLENGE_LEN) as u32);
                verify.put_u8(MSG_AUTH);
                verify.put_u8(AUTH_V2_VERIFY);
                verify.extend_from_slice(&mac);
                sock.write_all(&verify).await?;
                // 最终 ack（1 帧 + 2B 内容）
                let mut ack = [0u8; 4];
                sock.read_exact(&mut ack).await?;
                let alen = u32::from_be_bytes(ack) as usize;
                if alen == 0 || alen > 64 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        "internal auth failed",
                    ));
                }
                let mut abody = vec![0u8; alen];
                sock.read_exact(&mut abody).await?;
                if abody.first() != Some(&MSG_AUTH) || abody.get(1) != Some(&0x00) {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        "internal auth failed",
                    ));
                }
            }
        }
        let mut frame = BytesMut::with_capacity(payload.len() + 5);
        frame.put_u32((payload.len() + 1) as u32);
        frame.put_u8(msg_type);
        frame.extend_from_slice(payload);
        sock.write_all(&frame).await?;
        // 读响应帧
        let mut len_buf = [0u8; 4];
        sock.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut body = vec![0u8; len];
        sock.read_exact(&mut body).await?;
        Ok(Bytes::from(body))
    }

    pub async fn register(&self, node_id: i32, host: &str, port: u16) -> std::io::Result<()> {
        let mut p = Vec::new();
        p.extend_from_slice(&node_id.to_be_bytes());
        p.extend_from_slice(&(host.len() as i16).to_be_bytes());
        p.extend_from_slice(host.as_bytes());
        p.extend_from_slice(&(port as i32).to_be_bytes());
        self.call(MSG_REGISTER, &p).await.map(|_| ())
    }

    pub async fn heartbeat(&self, node_id: i32) -> std::io::Result<Bytes> {
        self.call(MSG_HEARTBEAT, &node_id.to_be_bytes()).await
    }

    pub async fn meta_sync(&self, version: u64) -> std::io::Result<Bytes> {
        self.call(MSG_META_SYNC, &version.to_be_bytes()).await
    }

    /// 事务 marker 跨节点下发（payload：[topic:S][part:i32][pid:i64]
    /// [epoch:i16][outcome:u8]；应答 [rc:i8][offset:i64]）。
    pub async fn write_txn_marker(
        &self,
        topic: &str,
        partition: i32,
        pid: i64,
        epoch: i16,
        outcome: basalt_record::ControlRecordType,
    ) -> Result<i64, basalt_storage::error::StorageError> {
        let mut p = Vec::new();
        p.extend_from_slice(&(topic.len() as i16).to_be_bytes());
        p.extend_from_slice(topic.as_bytes());
        p.extend_from_slice(&partition.to_be_bytes());
        p.extend_from_slice(&pid.to_be_bytes());
        p.extend_from_slice(&epoch.to_be_bytes());
        p.push(outcome as u8);
        let body = self
            .call(MSG_WRITE_TXN_MARKER, &p)
            .await
            .map_err(|e| basalt_storage::error::StorageError::Io(e))?;
        if body.len() < 9 || body[0] != 0 {
            return Err(basalt_storage::error::StorageError::Other(format!(
                "remote marker failed: {:?}",
                &body[..body.len().min(8)]
            )));
        }
        Ok(i64::from_be_bytes(body[1..9].try_into().unwrap()))
    }

    /// TxnOffsetCommit 代理到 controller（非 controller 节点的组协调器用）。
    pub async fn txn_offset_commit_proxy(
        &self,
        txn_id: &str,
        pid: i64,
        epoch: i16,
        offsets: &[crate::txn::PendingOffset],
    ) -> Result<(), basalt_storage::error::StorageError> {
        let mut p = Vec::new();
        p.extend_from_slice(&(txn_id.len() as i16).to_be_bytes());
        p.extend_from_slice(txn_id.as_bytes());
        p.extend_from_slice(&pid.to_be_bytes());
        p.extend_from_slice(&epoch.to_be_bytes());
        p.extend_from_slice(&(offsets.len() as i32).to_be_bytes());
        for o in offsets {
            p.extend_from_slice(&(o.group.len() as i16).to_be_bytes());
            p.extend_from_slice(o.group.as_bytes());
            p.extend_from_slice(&(o.topic.len() as i16).to_be_bytes());
            p.extend_from_slice(o.topic.as_bytes());
            p.extend_from_slice(&o.partition.to_be_bytes());
            p.extend_from_slice(&o.offset.to_be_bytes());
            p.extend_from_slice(&(o.metadata.len() as i16).to_be_bytes());
            p.extend_from_slice(o.metadata.as_bytes());
        }
        let body = self
            .call(MSG_TXN_OFFSET_COMMIT, &p)
            .await
            .map_err(|e| basalt_storage::error::StorageError::Io(e))?;
        if body.first().copied() == Some(0) {
            Ok(())
        } else {
            Err(basalt_storage::error::StorageError::InvalidTxnState)
        }
    }

    /// share 成员校验（B5）：问目标节点（组协调器）(group, member) 是否有效。
    pub async fn share_validate(&self, group: &str, member: &str) -> std::io::Result<bool> {
        let mut p = Vec::new();
        p.extend_from_slice(&(group.len() as i16).to_be_bytes());
        p.extend_from_slice(group.as_bytes());
        p.extend_from_slice(&(member.len() as i16).to_be_bytes());
        p.extend_from_slice(member.as_bytes());
        let body = self.call(MSG_SHARE_VALIDATE, &p).await?;
        Ok(body.first().copied().unwrap_or(0) == 1)
    }

    /// share 交付事件转发（B5）：目标节点（share-state topic leader）经本地
    /// sink 持久化 + 应用内存视图。
    pub async fn share_event(
        &self,
        group: &str,
        tid: u128,
        partition: i32,
        first: i64,
        last: i64,
        ty: i8,
    ) -> std::io::Result<()> {
        let mut p = Vec::new();
        p.extend_from_slice(&(group.len() as i16).to_be_bytes());
        p.extend_from_slice(group.as_bytes());
        p.extend_from_slice(&tid.to_be_bytes());
        p.extend_from_slice(&partition.to_be_bytes());
        p.extend_from_slice(&first.to_be_bytes());
        p.extend_from_slice(&last.to_be_bytes());
        p.push(ty as u8);
        self.call(MSG_SHARE_EVENT, &p).await.map(|_| ())
    }

    pub async fn create_topic(
        &self,
        name: &str,
        partitions: i32,
        rf: i32,
        tiered: bool,
        retention: basalt_metadata::TopicRetention,
    ) -> std::io::Result<Bytes> {
        let mut p = Vec::new();
        p.extend_from_slice(&(name.len() as i16).to_be_bytes());
        p.extend_from_slice(name.as_bytes());
        p.extend_from_slice(&partitions.to_be_bytes());
        p.extend_from_slice(&rf.to_be_bytes());
        p.push(tiered as u8);
        p.push(retention.ms.is_some() as u8);
        p.extend_from_slice(&retention.ms.unwrap_or(0).to_be_bytes());
        p.push(retention.bytes.is_some() as u8);
        p.extend_from_slice(&retention.bytes.unwrap_or(0).to_be_bytes());
        self.call(MSG_CREATE_TOPIC, &p).await
    }

    pub async fn fetch_slice(
        &self,
        topic: &str,
        partition: i32,
        follower_id: i32,
        from_offset: i64,
        max_bytes: usize,
    ) -> std::io::Result<FetchSliceResult> {
        let mut p = Vec::new();
        p.extend_from_slice(&(topic.len() as i16).to_be_bytes());
        p.extend_from_slice(topic.as_bytes());
        p.extend_from_slice(&partition.to_be_bytes());
        p.extend_from_slice(&follower_id.to_be_bytes());
        p.extend_from_slice(&from_offset.to_be_bytes());
        p.extend_from_slice(&(max_bytes as i32).to_be_bytes());
        let data = self.call(MSG_FETCH_SLICE, &p).await?;
        if data.len() < 20 {
            return Err(std::io::Error::other("short fetch slice response"));
        }
        let error = i16::from_be_bytes([data[0], data[1]]);
        let hw = i64::from_be_bytes(data[2..10].try_into().unwrap());
        let leader_next = i64::from_be_bytes(data[10..18].try_into().unwrap());
        let n = u32::from_be_bytes(data[18..22].try_into().unwrap()) as usize;
        let blob = data.slice(22..22 + n);
        Ok(FetchSliceResult { error, high_watermark: hw, leader_next_offset: leader_next, data: blob })
    }
}

/// 控制器命令（actor 模式）。
#[allow(dead_code)]
pub enum ControllerCmd {
    Register { info: BrokerInfo, reply: oneshot::Sender<()> },
    Heartbeat { node_id: i32 },
    Sync { version: u64, reply: oneshot::Sender<Option<Vec<u8>>> },
    CreateTopic {
        name: String,
        partitions: i32,
        rf: i32,
        tiered: bool,
        retention: basalt_metadata::TopicRetention,
        reply: oneshot::Sender<()>,
    },
    DeleteTopic { name: String, reply: oneshot::Sender<bool> },
    ApplySnapshot { state: ClusterState },
    /// 引擎模式跨节点 propose 转发（MSG_CTRL_PROPOSE 落地）：本节点为 raft
    /// leader 时本地 propose，否则返回错误（转发方重试）
    ProposeRemote { rec: ClusterRecord, reply: oneshot::Sender<Result<(), String>> },
    /// 计划内交接（M2 L1：目标为副本成员，epoch+1 走 LeaderChange——
    /// 数据已在副本集内同步，交接窗口 = 元数据广播周期，无数据迁移）
    TransferLeader { topic: String, partition: i32, to: i32, reply: oneshot::Sender<Result<(), String>> },
}

/// 控制器 actor（独占 ClusterState；最小 node id 的 broker 进程内运行）。
pub struct Controller {
    #[allow(dead_code)]
    pub node_id: i32,
    pub state: ClusterState,
    pub log_path: PathBuf,
    pub last_heartbeat: HashMap<i32, Instant>,
    pub heartbeat_timeout: Duration,
    pub rx: mpsc::Receiver<ControllerCmd>,
    /// ADR-15/16：raft 引擎运行时（BASALT_CTRL_RAFT_ENGINE=raftrs 时启用）。
    /// 启用后元数据变更经 raft propose 复制；本 actor 仅在 raft leader 上行使职权。
    pub engine: Option<crate::ctrl_raft::raftrs_engine::RaftRsHandle>,
    /// 全体节点内部端口地址（broker_id → "host:internal_port"），
    /// 引擎模式下 propose 转发给 raft leader 用。
    pub peers: HashMap<i32, String>,
}

impl Controller {
    pub fn spawn(
        node_id: i32,
        log_path: PathBuf,
        heartbeat_timeout: Duration,
        engine: Option<crate::ctrl_raft::raftrs_engine::RaftRsHandle>,
        peers: Vec<(i32, String)>,
    ) -> mpsc::Sender<ControllerCmd> {
        let (tx, rx) = mpsc::channel(256);
        let ctrl = Controller::open(node_id, log_path, heartbeat_timeout, rx, engine, peers);
        tokio::spawn(ctrl.run());
        tx
    }

    /// 引擎模式下经 raft propose 复制一条记录并等待本节点应用完成。
    /// 非 raft leader 的提案会被引擎拒绝——调用方（run 循环）仅在
    /// is_leader 时行使职权，此处有限重试即可。
    fn engine_propose(&self, rec: &ClusterRecord) -> Result<(), String> {
        tracing::debug!(node = self.node_id, "controller propose");
        let Some(engine) = &self.engine else {
            return Err("engine disabled".into());
        };
        for _ in 0..50 {
            match engine.propose(rec.clone()) {
                Ok(()) => {
                    // 等待本节点 apply 追上（本地 apply 与 commit 同步推进）
                    let want = engine.applied_index();
                    for _ in 0..100 {
                        if engine.applied_index() >= want {
                            return Ok(());
                        }
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    return Ok(());
                }
                Err(m) if m.contains("not leader") => {
                    // 非 leader：经内部 RPC 把记录转发给当前 raft leader
                    // （leader = -1 即选举中，原地等待下一轮；broker 0 合法）
                    let leader = engine.leader_id.load(std::sync::atomic::Ordering::Relaxed);
                    if leader >= 0 && leader != self.node_id {
                        match self.forward_propose(rec, leader) {
                            Ok(()) => {
                                // leader 已 propose 成功；等本地 apply（复制回来）
                                std::thread::sleep(Duration::from_millis(50));
                                return Ok(());
                            }
                            Err(m) => {
                                // 转发目标瞬态失败（对端选举中/未就绪）——可重试，
                                // 不能硬返回（否则注册类变更在启动窗口永久丢失）
                                tracing::warn!(node = self.node_id, to = leader, error = %m, "controller propose forward failed");
                                std::thread::sleep(Duration::from_millis(150));
                            }
                        }
                    } else {
                        std::thread::sleep(Duration::from_millis(150));
                    }
                }
                Err(m) => return Err(m),
            }
        }
        Err("raft proposal timeout".into())
    }

    /// 引擎模式 propose 转发：阻塞式内部 RPC（MSG_CTRL_PROPOSE）发往
    /// raft leader 节点，由其 controller 本地 propose（它是 leader，必成功）。
    fn forward_propose(&self, rec: &ClusterRecord, leader: i32) -> Result<(), String> {
        let Some(addr) = self.peers.get(&leader) else {
            return Err(format!("no peer addr for leader {leader}"));
        };
        tracing::debug!(node = self.node_id, leader, "controller propose forward");
        let payload = encode_record(rec);
        let mut frame = ((payload.len() + 1) as u32).to_be_bytes().to_vec();
        frame.push(MSG_CTRL_PROPOSE);
        frame.extend_from_slice(&payload);
        let addrs: Vec<_> = std::net::ToSocketAddrs::to_socket_addrs(addr.as_str())
            .map(|i| i.collect())
            .unwrap_or_default();
        for sa in addrs {
            if let Ok(mut sock) =
                std::net::TcpStream::connect_timeout(&sa, Duration::from_millis(500))
            {
                sock.set_write_timeout(Some(Duration::from_millis(500))).ok();
                sock.set_read_timeout(Some(Duration::from_millis(3000))).ok();
                use std::io::{Read, Write};
                if sock.write_all(&frame).is_ok() {
                    let mut len_buf = [0u8; 4];
                    if sock.read_exact(&mut len_buf).is_ok() {
                        let n = u32::from_be_bytes(len_buf) as usize;
                        let mut buf = vec![0u8; n];
                        if sock.read_exact(&mut buf).is_ok()
                            && !buf.is_empty()
                            && buf[0] == 1
                        {
                            return Ok(());
                        }
                        return Err("leader rejected propose".into());
                    }
                }
            }
            break;
        }
        Err("leader unreachable".into())
    }

    /// 引擎模式下的变更入口：propose 复制；本地 state 由引擎 apply 同步。
    fn apply_via_engine(&mut self, rec: &ClusterRecord) -> Result<(), String> {
        self.engine_propose(rec)
    }

    /// 引擎状态是否可对外服务：日志已追平（applied >= 最近已知 leader
    /// commit 水位）。无 leader 水位（0 = 未见任何心跳/尚未当选）时视为
    /// 未就绪——冷启动节点在当选/收到心跳前不服务陈旧/空状态
    fn engine_state_ready(&self) -> bool {
        match &self.engine {
            None => true,
            Some(e) => {
                let lc = e.leader_commit.load(std::sync::atomic::Ordering::Relaxed);
                if lc == 0 {
                    // 冷启动：尚未当选也未见任何 leader 心跳——无从证明状态新鲜
                    return false;
                }
                e.applied_index() >= lc
            }
        }
    }

    /// 引擎模式下本节点是否raft leader（failover/提案职权判定）。
    fn has_engine_authority(&self) -> bool {
        match &self.engine {
            Some(e) => e.is_leader.load(std::sync::atomic::Ordering::Relaxed),
            None => true, // 无引擎 = 传统单写者模式，恒有职权
        }
    }

    async fn run(mut self) {
        tracing::info!(node = self.node_id, engine = self.engine.is_some(), "controller loop started");
        loop {
            match tokio::time::timeout(Duration::from_millis(500), self.rx.recv()).await {
                Ok(Some(cmd)) => {
                    self.handle(cmd);
                    // 引擎模式：仅 raft leader 行使 failover 职权；
                    // 传统模式：全员检查（原有行为）
                    if self.has_engine_authority() {
                        self.failover_check();
                    }
                }
                Ok(None) => break,
                Err(_) => {
                    if self.has_engine_authority() {
                        self.failover_check();
                    }
                }
            }
        }
    }

    fn handle(&mut self, cmd: ControllerCmd) {
        // 引擎模式：所有 handler 统一读引擎 apply 后的状态（TransferLeader
        // 资格校验 / CreateTopic 幂等检查都依赖最新 assignment）。
        // 未追平时预加载快照不可信，按空态处理
        if self.engine.is_some() {
            if self.engine_state_ready() {
                let e = self.engine.as_ref().unwrap();
                self.state = e.shared.lock().unwrap().clone();
            } else {
                self.state = ClusterState::default();
            }
        }
        match cmd {
            ControllerCmd::Register { info, reply } => {
                if self.engine.is_some() {
                    let _ = self.apply_via_engine(&ClusterRecord::RegisterBroker(info.clone()));
                } else {
                    self.apply_and_persist(&ClusterRecord::RegisterBroker(info.clone()));
                }
                // 心跳键 = 本次注册的节点（而非"当前最大 id"——那会键错位）
                self.last_heartbeat.insert(info.node_id, Instant::now());
                let _ = reply.send(());
            }
            ControllerCmd::Heartbeat { node_id } => {
                self.last_heartbeat.insert(node_id, Instant::now());
            }
            ControllerCmd::Sync { version, reply } => {
                // 引擎模式：读引擎 apply 后的状态（raft 复制的一致性快照）。
                // 追平门控：applied < leader commit 水位 = 日志还在追赶，
                // 服务陈旧快照会形成僵尸 leader——返回 None 让调用方按
                // "无更新"处理，客户端重试其他节点
                let state_snap = match &self.engine {
                    Some(e) => {
                        if !self.engine_state_ready() {
                            let _ = reply.send(None);
                            return;
                        }
                        e.shared.lock().unwrap().clone()
                    }
                    None => self.state.clone(),
                };
                let _ = reply.send(if state_snap.version > version || state_snap.assignments.len() > 0 {
                    Some(state_snap.encode())
                } else {
                    None
                });
            }
            ControllerCmd::CreateTopic { name, partitions, rf, tiered, retention, reply } => {
                tracing::info!(node = self.node_id, topic = %name, "topic create via controller");
                let exists = self.state.assignments.iter().any(|a| a.topic == name);
                // rf 守卫：多副本建题时 broker 未注册齐会按不完整集群算
                // 副本（apply 里 rf 被 min 到 broker 数）——拒绝本次，
                // metadata 不含该 topic，客户端重试时集群视图已齐。
                // 只保护 rf>1：单节点传统模式无自注册路径（brokers 恒空、
                // apply 回退 vec![0]），rf=1 本无 failover 可言
                if rf > 1 && (self.state.brokers.len() as i32) < rf {
                    let _ = reply.send(());
                    return;
                }
                if self.engine.is_some() {
                    if let Err(m) = self.apply_via_engine(&ClusterRecord::CreateTopic { name: name.clone(), partitions, rf, tiered, retention }) {
                        tracing::error!(topic=%name, error=%m, "CreateTopic raft propose failed");
                    }
                } else {
                    if !exists {
                        self.apply_and_persist(&ClusterRecord::CreateTopic { name, partitions, rf, tiered, retention });
                    }
                }
                let _ = reply.send(());
            }
            ControllerCmd::ApplySnapshot { state } => {
                self.state = state;
            }
            ControllerCmd::DeleteTopic { name, reply } => {
                // 幂等：不存在时 no-op 返回 true（DeleteTopics 语义容忍）
                let exists = self.state.assignments.iter().any(|a| a.topic == name);
                if !exists {
                    let _ = reply.send(true);
                    return;
                }
                let r = if self.engine.is_some() {
                    self.apply_via_engine(&ClusterRecord::DeleteTopic { name: name.clone() })
                } else {
                    self.apply_and_persist(&ClusterRecord::DeleteTopic { name: name.clone() });
                    Ok(())
                };
                let _ = reply.send(r.is_ok());
            }
            ControllerCmd::ProposeRemote { rec, reply } => {
                // 仅 raft leader 受理转发提案；非 leader 立即拒绝——
                // 否则转发方每轮等待本节点完整重试周期（7.5s）造成级联阻塞
                let r = if self.has_engine_authority() {
                    self.apply_via_engine(&rec)
                } else {
                    Err("not leader (remote reject)".into())
                };
                let _ = reply.send(r);
            }
            ControllerCmd::TransferLeader { topic, partition, to, reply } => {
                // 先按不可变借用校验资格，再取独立信息调用 apply_and_persist
                let cur = self.state.assignments.iter().find(|a| a.topic == topic && a.partition == partition);
                let alive = self.last_heartbeat.get(&to)
                    .map(|t| t.elapsed() <= self.heartbeat_timeout)
                    .unwrap_or(to == self.node_id); // 控制器自身免死
                let r = match cur {
                    Some(a) if a.replicas.contains(&to) && a.leader != to && alive => {
                        let epoch = a.epoch + 1;
                        let rec = ClusterRecord::LeaderChange {
                            topic: topic.clone(),
                            partition,
                            leader: to,
                            epoch,
                        };
                        if self.engine.is_some() {
                            let _ = self.apply_via_engine(&rec);
                        } else {
                            self.apply_and_persist(&rec);
                        }
                        tracing::info!(topic=%topic, partition, to, epoch, "TRANSFER");
                        Ok(())
                    }
                    Some(a) => {
                        tracing::warn!(
                            node = self.node_id, topic = %topic, to, leader = a.leader,
                            replicas = ?a.replicas, alive, "leader transfer rejected: target not eligible");
                        Err("transfer target not eligible".into())
                    }
                    None => {
                        tracing::warn!(
                            node = self.node_id, topic = %topic,
                            assignments = self.state.assignments.len(),
                            "leader transfer rejected: assignment not found");
                        Err("assignment not found".into())
                    }
                };
                let _ = reply.send(r);
            }
        }
    }

    /// 心跳超时 → leader failover（epoch+1，副本轮转）。
    fn failover_check(&mut self) {
        // 引擎模式：failover 决策读引擎 apply 后的状态
        if let Some(e) = &self.engine {
            let st = e.shared.lock().unwrap().clone();
            self.state = st;
        }
        let now = Instant::now();
        if self.engine.is_some() {
            let alive: Vec<i32> = self.last_heartbeat.iter()
                .filter(|(_, t)| now.duration_since(**t) <= self.heartbeat_timeout)
                .map(|(id, _)| *id).collect();
            tracing::debug!(
                node = self.node_id, authority = self.has_engine_authority(),
                version = self.state.version, alive = ?alive,
                assignments = self.state.assignments.len(),
                "failover check");
        }
        // 控制器自身：免死 + 常驻 alive（无独立心跳线程，每次检查时刷新）
        self.last_heartbeat.insert(self.node_id, now);
        let stale: Vec<i32> = self
            .last_heartbeat
            .iter()
            .filter(|(_, t)| now.duration_since(**t) > self.heartbeat_timeout)
            .map(|(id, _)| *id)
            .collect();
        if !stale.is_empty() {
            tracing::info!(?stale, "failover check: stale brokers present");
        }
        let dead: Vec<i32> = self
            .state
            .brokers
            .keys()
            .copied()
            .filter(|id| {
                if *id == self.node_id {
                    return false; // 控制器自身永不死
                }
                self.last_heartbeat
                    .get(id)
                    .map(|t| now.duration_since(*t) > self.heartbeat_timeout)
                    .unwrap_or(true)
            })
            .collect();
        if dead.is_empty() {
            return;
        }
        let alive: Vec<i32> = self
            .state
            .brokers
            .keys()
            .copied()
            .filter(|id| {
                self.last_heartbeat
                    .get(id)
                    .map(|t| now.duration_since(*t) <= self.heartbeat_timeout)
                    .unwrap_or(false)
            })
            .collect();
        for a in self.state.assignments.clone() {
            if dead.contains(&a.leader) {
                if let Some(next) = a.replicas.iter().find(|r| alive.contains(r) && **r != a.leader) {
                    let rec = ClusterRecord::LeaderChange {
                        topic: a.topic.clone(),
                        partition: a.partition,
                        leader: *next,
                        epoch: a.epoch + 1,
                    };
                    tracing::info!(topic=%a.topic, partition=a.partition, old=a.leader, new=*next, epoch=a.epoch+1, "FAILOVER");
                    if self.engine.is_some() {
                        let _ = self.apply_via_engine(&rec);
                    } else {
                        self.apply_and_persist(&rec);
                    }
                }
            }
        }
    }
}

impl Controller {
    pub fn open(
        node_id: i32,
        log_path: PathBuf,
        heartbeat_timeout: Duration,
        rx: mpsc::Receiver<ControllerCmd>,
        engine: Option<crate::ctrl_raft::raftrs_engine::RaftRsHandle>,
        peers: Vec<(i32, String)>,
    ) -> Controller {
        // 恢复：重放 record 日志（自定义 record 编解码见 apply_and_persist）
        let mut state = ClusterState::default();
        if let Ok(data) = std::fs::read(&log_path) {
            let mut pos = 0usize;
            while pos + 4 <= data.len() {
                let len = u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;
                if len == 0 || pos + len > data.len() {
                    break;
                }
                if let Some(rec) = decode_record(&data[pos..pos + len]) {
                    state.apply(&rec);
                }
                pos += len;
            }
        }
        tracing::info!(version = state.version, brokers = state.brokers.len(), "controller recovered");
        let mut last_heartbeat = HashMap::new();
        // 控制器自身常驻存活（无独立心跳线程），failover 检查豁免自身
        last_heartbeat.insert(node_id, Instant::now());
        Controller {
            node_id,
            state,
            log_path,
            last_heartbeat,
            heartbeat_timeout,
            rx,
            engine,
            peers: peers.into_iter().collect(),
        }
    }

    fn apply_and_persist(&mut self, rec: &ClusterRecord) {
        let mut payload = encode_record(rec);
        let len = payload.len() as u32;
        let mut frame = len.to_be_bytes().to_vec();
        frame.append(&mut payload);
        if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(&self.log_path) {
            use std::io::Write;
            let _ = f.write_all(&frame);
            let _ = f.flush();
        }
        self.state.apply(rec);
    }
}

fn encode_record(rec: &ClusterRecord) -> Vec<u8> {
    let mut b = Vec::new();
    match rec {
        ClusterRecord::RegisterBroker(br) => {
            b.push(1);
            b.extend_from_slice(&br.node_id.to_be_bytes());
            b.extend_from_slice(&(br.host.len() as i16).to_be_bytes());
            b.extend_from_slice(br.host.as_bytes());
            b.extend_from_slice(&(br.port as i32).to_be_bytes());
        }
        ClusterRecord::CreateTopic { name, partitions, rf, tiered, retention } => {
            b.push(2);
            b.extend_from_slice(&(name.len() as i16).to_be_bytes());
            b.extend_from_slice(name.as_bytes());
            b.extend_from_slice(&partitions.to_be_bytes());
            b.extend_from_slice(&rf.to_be_bytes());
            b.push(*tiered as u8);
            b.push(retention.ms.is_some() as u8);
            b.extend_from_slice(&retention.ms.unwrap_or(0).to_be_bytes());
            b.push(retention.bytes.is_some() as u8);
            b.extend_from_slice(&retention.bytes.unwrap_or(0).to_be_bytes());
        }
        ClusterRecord::DeleteTopic { name } => {
            b.push(4);
            b.extend_from_slice(&(name.len() as i16).to_be_bytes());
            b.extend_from_slice(name.as_bytes());
        }
        ClusterRecord::LeaderChange { topic, partition, leader, epoch } => {
            b.push(3);
            b.extend_from_slice(&(topic.len() as i16).to_be_bytes());
            b.extend_from_slice(topic.as_bytes());
            b.extend_from_slice(&partition.to_be_bytes());
            b.extend_from_slice(&leader.to_be_bytes());
            b.extend_from_slice(&epoch.to_be_bytes());
        }
    }
    b
}

fn decode_record(data: &[u8]) -> Option<ClusterRecord> {
    let kind = *data.first()?;
    let mut p = 1usize;
    let g16 = |p: &mut usize| -> i16 {
        let v = i16::from_be_bytes(data[*p..*p + 2].try_into().unwrap());
        *p += 2;
        v
    };
    let g32 = |p: &mut usize| -> i32 {
        let v = i32::from_be_bytes(data[*p..*p + 4].try_into().unwrap());
        *p += 4;
        v
    };
    let gstr = |p: &mut usize| -> String {
        let n = g16(p) as usize;
        let s = String::from_utf8_lossy(&data[*p..*p + n]).into_owned();
        *p += n;
        s
    };
    Some(match kind {
        1 => {
            let node_id = g32(&mut p);
            let host = gstr(&mut p);
            let port = g32(&mut p) as u16;
            ClusterRecord::RegisterBroker(BrokerInfo { node_id, host, port })
        }
        2 => {
            let name = gstr(&mut p);
            let partitions = g32(&mut p);
            let rf = g32(&mut p);
            // tiered 尾字节：旧 WAL 记录缺失 → false（持久化面兼容）
            let tiered = p < data.len() && data[p] != 0;
            p += 1;
            // retention 尾缀：旧记录缺失 → None（B4 同款兼容）
            let (has_ms, has_bytes) = if p + 2 <= data.len() {
                let h1 = data[p] != 0;
                let h2 = data[p + 1] != 0;
                (h1, h2)
            } else {
                (false, false)
            };
            p += 2;
            let ms = if has_ms && p + 8 <= data.len() {
                let v = u64::from_be_bytes(data[p..p + 8].try_into().unwrap());
                p += 8;
                Some(v)
            } else {
                None
            };
            let bytes = if has_bytes && p + 8 <= data.len() {
                let v = u64::from_be_bytes(data[p..p + 8].try_into().unwrap());
                Some(v)
            } else {
                None
            };
            ClusterRecord::CreateTopic {
                name,
                partitions,
                rf,
                tiered,
                retention: basalt_metadata::TopicRetention { ms, bytes },
            }
        }
        3 => {
            let topic = gstr(&mut p);
            let partition = g32(&mut p);
            let leader = g32(&mut p);
            let epoch = g32(&mut p);
            ClusterRecord::LeaderChange { topic, partition, leader, epoch }
        }
        4 => {
            let name = gstr(&mut p);
            ClusterRecord::DeleteTopic { name }
        }
        _ => return None,
    })
}


/// 顶层便捷：write_txn_marker（构造一次性 client）。
pub async fn write_txn_marker(
    addr: &str,
    topic: &str,
    partition: i32,
    pid: i64,
    epoch: i16,
    outcome: basalt_record::ControlRecordType,
) -> Result<i64, basalt_storage::error::StorageError> {
    InternalClient::new(addr.to_string())
        .write_txn_marker(topic, partition, pid, epoch, outcome)
        .await
}

/// 顶层便捷：TxnOffsetCommit 代理。
pub async fn txn_offset_commit_proxy(
    addr: &str,
    txn_id: &str,
    pid: i64,
    epoch: i16,
    offsets: &[crate::txn::PendingOffset],
) -> Result<(), basalt_storage::error::StorageError> {
    InternalClient::new(addr.to_string())
        .txn_offset_commit_proxy(txn_id, pid, epoch, offsets)
        .await
}

// ---------- 内部服务端 ----------

#[derive(Clone)]
#[allow(dead_code)]
pub struct InternalCtx {
    // BufferPool 进程单例共享资源池：Arc 表达资源共享而非共享可变所有权（ADR-13）。
    #[allow(clippy::disallowed_types)]
    pub pool: std::sync::Arc<BufferPool>,
    #[allow(dead_code)]
    pub node_id: i32,
    #[allow(dead_code)]
    pub is_controller: bool,
    pub controller_tx: Option<mpsc::Sender<ControllerCmd>>,
    pub routes_rx: tokio::sync::watch::Receiver<crate::meta::RoutingTable>,
    /// 事务协调器句柄（仅 controller 节点；MSG_TXN_OFFSET_COMMIT 落点）。
    pub txn_tx: Option<mpsc::Sender<crate::txn::TxnCmd>>,
}

pub async fn serve(listener: tokio::net::TcpListener, ctx: InternalCtx) {
    loop {
        let Ok((sock, _)) = listener.accept().await else { break };
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let _ = handle_internal_conn(sock, ctx).await;
        });
    }
}

pub const MSG_AUTH: u8 = 0;

/// v2 质询-应答握手（B1）：MSG_AUTH payload[0] 作标记。
/// 0x02 = 请求质询；0x03 = HMAC-SHA256(token, challenge) 应答。
/// 旧明文握手 payload 首字节是 token 的 ASCII，不会撞 0x02。
const AUTH_V2_REQUEST: u8 = 0x02;
const AUTH_V2_VERIFY: u8 = 0x03;
const AUTH_CHALLENGE_LEN: usize = 32;

/// B1：内部口 HMAC（令牌只作对称密钥不上线；挑战一次性，安全性由
/// HMAC(token, challenge) 承担）。
fn hmac_internal_token(secret: &str, challenge: &[u8]) -> [u8; 32] {
    use hmac::Mac;
    type HmacSha256 = hmac::Hmac<sha2::Sha256>;
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .unwrap_or_else(|_| HmacSha256::new_from_slice(b"basalt-internal-fallback").unwrap());
    mac.update(challenge);
    mac.finalize().into_bytes().into()
}

/// v2 服务端侧：发挑战 → 验 HMAC。验收失败一律 PermissionDenied 断连
/// （fail-closed；旧明文握手同拒——令牌配置后明文即违规）。
async fn handshake_internal_v2(
    sock: &mut tokio::net::TcpStream,
    node: i32,
    expected: &str,
) -> std::io::Result<()> {
    let mut challenge = [0u8; AUTH_CHALLENGE_LEN];
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mut state = nanos ^ 0x9E37_79B9_7F4A_7C15;
    for b in challenge.iter_mut() {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *b = (state >> 33) as u8;
    }
    // S→C: [MSG_AUTH][0x00][challenge 32B]（framed）
    let mut reply = Vec::with_capacity(2 + AUTH_CHALLENGE_LEN);
    reply.push(MSG_AUTH);
    reply.push(0x00);
    reply.extend_from_slice(&challenge);
    let flen = (reply.len() as u32).to_be_bytes();
    sock.write_all(&flen).await?;
    sock.write_all(&reply).await?;

    // C→S: [MSG_AUTH][0x03][mac 32B]
    let mut len_buf = [0u8; 4];
    tokio::time::timeout(std::time::Duration::from_secs(5), sock.read_exact(&mut len_buf)).await
        .map_err(|_| std::io::Error::other("auth verify timeout"))??;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len != 2 + AUTH_CHALLENGE_LEN {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "bad verify frame"));
    }
    let mut msg = vec![0u8; len];
    tokio::time::timeout(std::time::Duration::from_secs(5), sock.read_exact(&mut msg)).await
        .map_err(|_| std::io::Error::other("auth verify read timeout"))??;
    if msg[0] != MSG_AUTH || msg[1] != AUTH_V2_VERIFY {
        return Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, "bad verify marker"));
    }
    let expected_mac = hmac_internal_token(expected, &challenge);
    if msg[2..] != expected_mac {
        tracing::warn!(node, "internal HMAC auth failed");
        return Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, "internal auth failed"));
    }
    // 成功 ack：framed [len=2][MSG_AUTH][0x00]
    let ack = (2u32).to_be_bytes();
    sock.write_all(&ack).await?;
    let ok = [MSG_AUTH, 0x00];
    sock.write_all(&ok).await?;
    Ok(())
}

#[cfg(test)]
mod hmac_auth_tests {
    //! B1：HMAC 质询-应答面（确定性 + 错误密钥判别）。

    use super::*;

    #[test]
    fn hmac_deterministic_and_secret_bound() {
        let c = [7u8; 32];
        let a = hmac_internal_token("secret-1", &c);
        let b = hmac_internal_token("secret-1", &c);
        let c2 = hmac_internal_token("secret-2", &c);
        let c3 = hmac_internal_token("secret-1", &[8u8; 32]);
        assert_eq!(a, b, "同密钥同挑战必须确定");
        assert_ne!(a, c2, "不同密钥必须不同");
        assert_ne!(a, c3, "不同挑战必须不同");
    }

    #[test]
    fn legacy_plaintext_payload_cannot_collide_with_v2_marker() {
        // 任意 ASCII token 的首字节不可能等于 0x02
        for t in ["x", "token-1", "0", "\u{7f}"] {
            assert_ne!(t.as_bytes()[0], AUTH_V2_REQUEST);
        }
    }
}

async fn handle_internal_conn(
    mut sock: tokio::net::TcpStream,
    ctx: InternalCtx,
) -> std::io::Result<()> {
    // 连接级令牌校验（B1 v2 质询-应答；明文握手 fail-closed 拒绝）
    if let Ok(expected) = std::env::var("BASALT_INTERNAL_TOKEN") {
        if !expected.is_empty() {
            let mut len_buf = [0u8; 4];
            tokio::time::timeout(std::time::Duration::from_secs(5), sock.read_exact(&mut len_buf)).await
                .map_err(|_| std::io::Error::other("auth timeout"))??;
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut msg = vec![0u8; len];
            tokio::time::timeout(std::time::Duration::from_secs(5), sock.read_exact(&mut msg)).await
                .map_err(|_| std::io::Error::other("auth read timeout"))??;
            if msg[0] != MSG_AUTH {
                return Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, "internal auth failed"));
            }
            let payload = &msg[1..];
            if payload == [AUTH_V2_REQUEST] {
                handshake_internal_v2(&mut sock, ctx.node_id, &expected).await?;
            } else {
                // 明文 token 上线 = 违规（B1 fail-closed）
                tracing::warn!(node = ctx.node_id, "plaintext internal auth rejected (use v2 HMAC handshake)");
                return Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, "plaintext internal auth rejected"));
            }
        }
    }
    loop {
        let mut len_buf = [0u8; 4];
        sock.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;
        if len == 0 || len > 64 * 1024 * 1024 {
            return Err(std::io::Error::other("bad internal frame"));
        }
        let mut msg = vec![0u8; len];
        sock.read_exact(&mut msg).await?;
        let msg_type = msg[0];
        let payload = &msg[1..];

        let resp: Bytes = match msg_type {
            MSG_REGISTER => {
                static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::debug!(node = ctx.node_id, brokers = n, "internal register");
                let Ok((node_id, host, port)) = parse_register(payload) else {
                    sock.write_all(&short_frame()).await?;
                    return Ok(());
                };
                if let Some(tx) = &ctx.controller_tx {
                    let (txr, rxr) = tokio::sync::oneshot::channel();
                    let _ = tx.send(ControllerCmd::Register {
                        info: BrokerInfo { node_id, host, port },
                        reply: txr,
                    }).await;
                    let _ = rxr.await;
                }
                Bytes::new()
            }
            MSG_HEARTBEAT => {
                static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if n % 20 == 0 {
                    tracing::debug!(node = ctx.node_id, brokers = n, "internal heartbeat");
                }
                if payload.len() < 4 {
                    sock.write_all(&short_frame()).await?;
                    return Ok(());
                }
                let node_id = i32::from_be_bytes(payload[0..4].try_into().unwrap());
                if let Some(tx) = &ctx.controller_tx {
                    let _ = tx.send(ControllerCmd::Heartbeat { node_id }).await;
                }
                Bytes::new()
            }
            MSG_META_SYNC => {
                if payload.len() < 8 {
                    sock.write_all(&short_frame()).await?;
                    return Ok(());
                }
                let version = u64::from_be_bytes(payload[0..8].try_into().unwrap());
                let mut out = Vec::new();
                if let Some(tx) = &ctx.controller_tx {
                    let (txr, rxr) = tokio::sync::oneshot::channel();
                    let _ = tx.send(ControllerCmd::Sync { version, reply: txr }).await;
                    if let Ok(Some(snap)) = rxr.await {
                        out.extend_from_slice(&1u8.to_be_bytes());
                        out.extend_from_slice(&snap);
                    } else {
                        out.extend_from_slice(&0u8.to_be_bytes());
                    }
                }
                Bytes::from(out)
            }
            MSG_TRANSFER => {
                // payload: u16 topic_len | topic | i32 partition | i32 to
                let bad = || Bytes::from(1i16.to_be_bytes().to_vec());
                if payload.len() < 2 {
                    sock.write_all(&bad()).await?;
                    return Ok(());
                }
                let n = i16::from_be_bytes(payload[0..2].try_into().unwrap()) as usize;
                if payload.len() < 2 + n + 8 {
                    sock.write_all(&bad()).await?;
                    return Ok(());
                }
                let topic = String::from_utf8_lossy(&payload[2..2 + n]).into_owned();
                let partition = i32::from_be_bytes(payload[2 + n..6 + n].try_into().unwrap());
                let to = i32::from_be_bytes(payload[6 + n..10 + n].try_into().unwrap());
                let r = if let Some(tx) = &ctx.controller_tx {
                    let (txr, rxr) = tokio::sync::oneshot::channel();
                    let _ = tx.send(ControllerCmd::TransferLeader { topic, partition, to, reply: txr }).await;
                    rxr.await.unwrap_or_else(|_| Err("controller dropped".into()))
                } else {
                    Err("not controller".into())
                };
                Bytes::from(r.map(|_| 0i16).unwrap_or(-1i16).to_be_bytes().to_vec())
            }
            MSG_WRITE_TXN_MARKER => {
                // payload: [topic:S][part:i32][pid:i64][epoch:i16][outcome:u8]
                // 应答: [rc:i8][offset:i64]
                let bad = || Bytes::from(vec![1u8, 0, 0, 0, 0, 0, 0, 0, 0]);
                let mut p = 0usize;
                let g16 = |p: &mut usize| -> Option<i16> {
                    if payload.len() < *p + 2 { return None; }
                    let v = i16::from_be_bytes(payload[*p..*p + 2].try_into().unwrap());
                    *p += 2;
                    Some(v)
                };
                let g32 = |p: &mut usize| -> Option<i32> {
                    if payload.len() < *p + 4 { return None; }
                    let v = i32::from_be_bytes(payload[*p..*p + 4].try_into().unwrap());
                    *p += 4;
                    Some(v)
                };
                let g64 = |p: &mut usize| -> Option<i64> {
                    if payload.len() < *p + 8 { return None; }
                    let v = i64::from_be_bytes(payload[*p..*p + 8].try_into().unwrap());
                    *p += 8;
                    Some(v)
                };
                let gstr = |p: &mut usize| -> Option<String> {
                    let n = g16(p)? as usize;
                    if payload.len() < *p + n { return None; }
                    let s = String::from_utf8_lossy(&payload[*p..*p + n]).into_owned();
                    *p += n;
                    Some(s)
                };
                let parsed = (|| {
                    let topic = gstr(&mut p)?;
                    let partition = g32(&mut p)?;
                    let pid = g64(&mut p)?;
                    let epoch = g16(&mut p)?;
                    if payload.len() < p + 1 { return None; }
                    let outcome = basalt_record::ControlRecordType::from_i16(payload[p] as i16)?;
                    Some((topic, partition, pid, epoch, outcome))
                })();
                let Some((topic, partition, pid, epoch, outcome)) = parsed else {
                    sock.write_all(&bad()).await?;
                    return Ok(());
                };
                let route = {
                    let g = ctx.routes_rx.borrow();
                    g.find(&topic, partition).cloned()
                };
                let res = if let Some(route) = route {
                    let (rtx, rrx) = tokio::sync::oneshot::channel();
                    match route.tx.send(crate::partition::PartitionCmd::WriteTxnMarker {
                        producer_id: pid,
                        producer_epoch: epoch,
                        outcome,
                        reply: rtx,
                    }).await {
                        Ok(()) => rrx.await.unwrap_or_else(|_| Err(basalt_storage::error::StorageError::Other("marker reply dropped".into()))),
                        Err(_) => Err(basalt_storage::error::StorageError::NotLeader),
                    }
                } else {
                    Err(basalt_storage::error::StorageError::NotLeader)
                };
                match res {
                    Ok(off) => {
                        let mut out = vec![0u8];
                        out.extend_from_slice(&off.to_be_bytes());
                        Bytes::from(out)
                    }
                    Err(_) => Bytes::from(vec![1u8, 0, 0, 0, 0, 0, 0, 0, 0]),
                }
            }
            MSG_TXN_OFFSET_COMMIT => {
                // 代理落点（controller）：payload 与 InternalClient::txn_offset_commit_proxy 对应
                // 应答: [rc:i8]
                let mut p = 0usize;
                let g16 = |p: &mut usize| -> Option<i16> {
                    if payload.len() < *p + 2 { return None; }
                    let v = i16::from_be_bytes(payload[*p..*p + 2].try_into().unwrap());
                    *p += 2;
                    Some(v)
                };
                let g32 = |p: &mut usize| -> Option<i32> {
                    if payload.len() < *p + 4 { return None; }
                    let v = i32::from_be_bytes(payload[*p..*p + 4].try_into().unwrap());
                    *p += 4;
                    Some(v)
                };
                let g64 = |p: &mut usize| -> Option<i64> {
                    if payload.len() < *p + 8 { return None; }
                    let v = i64::from_be_bytes(payload[*p..*p + 8].try_into().unwrap());
                    *p += 8;
                    Some(v)
                };
                let gstr = |p: &mut usize| -> Option<String> {
                    let n = g16(p)? as usize;
                    if payload.len() < *p + n { return None; }
                    let s = String::from_utf8_lossy(&payload[*p..*p + n]).into_owned();
                    *p += n;
                    Some(s)
                };
                let parsed = (|| {
                    let txn_id = gstr(&mut p)?;
                    let pid = g64(&mut p)?;
                    let epoch = g16(&mut p)?;
                    let n = g32(&mut p)? as usize;
                    let mut offs = Vec::with_capacity(n.min(1024));
                    for _ in 0..n {
                        let group = gstr(&mut p)?;
                        let topic = gstr(&mut p)?;
                        let partition = g32(&mut p)?;
                        let offset = g64(&mut p)?;
                        let metadata = gstr(&mut p)?;
                        offs.push(crate::txn::PendingOffset { group, topic, partition, offset, metadata });
                    }
                    Some((txn_id, pid, epoch, offs))
                })();
                let Some((txn_id, pid, epoch, offs)) = parsed else {
                    sock.write_all(&[1u8]).await?;
                    return Ok(());
                };
                let res = if let Some(tx) = &ctx.txn_tx {
                    let (rtx, rrx) = tokio::sync::oneshot::channel();
                    let sent = tx.send(crate::txn::TxnCmd::TxnOffsetCommit {
                        txn_id, pid, epoch, offsets: offs, reply: rtx,
                    }).await;
                    match sent {
                        Ok(()) => rrx.await.unwrap_or(Err(basalt_storage::error::StorageError::Other("reply dropped".into()))),
                        Err(_) => Err(basalt_storage::error::StorageError::Other("txn coordinator closed".into())),
                    }
                } else {
                    Err(basalt_storage::error::StorageError::Other("not controller".into()))
                };
                Bytes::from(vec![if res.is_ok() { 0u8 } else { 1u8 }])
            }
            MSG_RAFT => {
                // 接收探针：确认 wire 帧到达接收端
                tracing::debug!(payload_len = payload.len(), engine = crate::ctrl_raft::raftrs_engine::engine_enabled(), "raft payload received");
                if crate::ctrl_raft::raftrs_engine::engine_enabled() {
                    crate::ctrl_raft::raftrs_engine::deliver_wire(payload.to_vec());
                }
                Bytes::new()
            }
            MSG_CTRL_PROPOSE => {
                // 引擎模式 propose 转发落地：解码记录交本节点 controller propose
                tracing::debug!(node = ctx.node_id, payload_len = payload.len(), "raft propose forwarded");
                let ok = match decode_record(payload) {
                    Some(rec) => {
                        if let Some(tx) = &ctx.controller_tx {
                            let (txr, rxr) = tokio::sync::oneshot::channel();
                            let _ = tx
                                .send(ControllerCmd::ProposeRemote { rec, reply: txr })
                                .await;
                            rxr.await.is_ok()
                        } else {
                            false
                        }
                    }
                    None => false,
                };
                Bytes::from(vec![if ok { 1 } else { 0 }])
            }
            MSG_CREATE_TOPIC => {
                let Ok((name, partitions, rf, tiered, retention)) = parse_create(payload) else {
                    sock.write_all(&short_frame()).await?;
                    return Ok(());
                };
                if let Some(tx) = &ctx.controller_tx {
                    let (txr, rxr) = tokio::sync::oneshot::channel();
                    let _ = tx.send(ControllerCmd::CreateTopic { name, partitions, rf, tiered, retention, reply: txr }).await;
                    let _ = rxr.await;
                }
                Bytes::from(0i16.to_be_bytes().to_vec())
            }
            MSG_SHARE_VALIDATE => {
                let Ok(v) = parse_share_validate(payload) else {
                    sock.write_all(&short_frame()).await?;
                    return Ok(());
                };
                let ok = crate::share_group::validate_exists(&v.0, &v.1).is_ok();
                Bytes::from(vec![if ok { 1 } else { 0 }])
            }
            MSG_SHARE_EVENT => {
                let Ok((group, tid, partition, first, last, ty)) = parse_share_event_wire(payload) else {
                    sock.write_all(&short_frame()).await?;
                    return Ok(());
                };
                // 落地节点 = share-state topic leader：持久化（本地 sink 通道）
                // + 应用内存视图（本节点可能同为该数据分区的后任 leader）
                crate::share_group::ingest_event(&group, tid, partition, first, last, ty);
                if let Some(tx) = crate::share_group::sink_channel() {
                    let _ = tx.try_send((group, tid, partition, first, last, ty));
                }
                Bytes::from(vec![1])
            }
            MSG_FETCH_SLICE => {
                let Ok((topic, partition, follower_id, from_offset, max_bytes)) = parse_fetch_slice(payload) else {
                    sock.write_all(&short_frame()).await?;
                    return Ok(());
                };
                if blocked_peers().contains(&follower_id) {
                    // 分区注入：leader 拒绝为被断边的 follower 服务
                    sock.write_all(&short_frame()).await?;
                    return Ok(());
                }
                let route = {
                    let routes = ctx.routes_rx.borrow();
                    routes.find(&topic, partition).cloned()
                };
                let resp = match route {
                    None => Bytes::from(error_slice(6)),
                    // 副本集内互信：leader 服务常规拉取；副本间服务就任拉齐
                    // （reconciliation，账本 ㉟）——非副本集内一律拒绝
                    Some(route) if route.leader != ctx.node_id && !route.replicas.contains(&ctx.node_id) => Bytes::from(error_slice(6)),
                    Some(route) => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                if route
                    .tx
                    .send(crate::partition::PartitionCmd::FetchSlice {
                        follower: follower_id,
                        offset: from_offset,
                        max_bytes,
                        reply: tx,
                    })
                    .await
                    .is_err()
                {
                    Bytes::from(error_slice(8))
                } else {
                    let out = rx.await.map_err(|e| std::io::Error::other(e.to_string()))?;
                    let (err, hw, next, blob) = match &out.error {
                        Some(_) => (2i16, -1i64, -1i64, Bytes::new()),
                        None => (0i16, out.high_watermark, out.next_offset, out.data.clone()),
                    };
                        let mut b = BytesMut::new();
                        b.extend_from_slice(&err.to_be_bytes());
                        b.extend_from_slice(&hw.to_be_bytes());
                        b.extend_from_slice(&next.to_be_bytes());
                        b.extend_from_slice(&(blob.len() as u32).to_be_bytes());
                        b.extend_from_slice(&blob);
                        b.freeze()
                    }
                }
                };
                resp
            }
            _ => Bytes::new(),
        };
        let mut frame = BytesMut::new();
        frame.extend_from_slice(&(resp.len() as u32).to_be_bytes());
        frame.extend_from_slice(&resp);
        sock.write_all(&frame).await?;
    }
}

/// 短帧/坏帧的兜底应答（4B 长度 + 2B 错误码）。
fn short_frame() -> Vec<u8> {
    let body = 42i16.to_be_bytes();
    let mut b = Vec::with_capacity(6);
    b.extend_from_slice(&(body.len() as u32).to_be_bytes());
    b.extend_from_slice(&body);
    b
}

#[allow(dead_code)]
fn short_payload() -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&42i16.to_be_bytes()); // ILLEGAL_ARGUMENT 类
    b
}

fn error_slice(code: i16) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&code.to_be_bytes());
    b.extend_from_slice(&(-1i64).to_be_bytes());
    b.extend_from_slice(&(-1i64).to_be_bytes());
    b.extend_from_slice(&0u32.to_be_bytes());
    b
}

fn parse_register(p: &[u8]) -> std::io::Result<(i32, String, u16)> {
    if p.len() < 8 {
        return Err(std::io::Error::other("short register"));
    }
    let node_id = i32::from_be_bytes(p[0..4].try_into().unwrap());
    let n = i16::from_be_bytes(p[4..6].try_into().unwrap()) as usize;
    if p.len() < 6 + n + 4 {
        return Err(std::io::Error::other("short register host"));
    }
    let host = String::from_utf8_lossy(&p[6..6 + n]).into_owned();
    let port = i32::from_be_bytes(p[6 + n..10 + n].try_into().unwrap()) as u16;
    Ok((node_id, host, port))
}

fn parse_share_validate(p: &[u8]) -> std::io::Result<(String, String)> {
    if p.len() < 2 {
        return Err(std::io::Error::other("short validate"));
    }
    let n = i16::from_be_bytes(p[0..2].try_into().unwrap()) as usize;
    if p.len() < 2 + n + 2 {
        return Err(std::io::Error::other("short validate fields"));
    }
    let group = String::from_utf8_lossy(&p[2..2 + n]).into_owned();
    let rest = &p[2 + n..];
    let m = i16::from_be_bytes(rest[0..2].try_into().unwrap()) as usize;
    if rest.len() < 2 + m {
        return Err(std::io::Error::other("short validate member"));
    }
    let member = String::from_utf8_lossy(&rest[2..2 + m]).into_owned();
    Ok((group, member))
}

fn parse_share_event_wire(
    p: &[u8],
) -> std::io::Result<(String, u128, i32, i64, i64, i8)> {
    if p.len() < 2 {
        return Err(std::io::Error::other("short share event"));
    }
    let n = i16::from_be_bytes(p[0..2].try_into().unwrap()) as usize;
    if p.len() < 2 + n + 16 + 4 + 8 + 8 + 1 {
        return Err(std::io::Error::other("short share event fields"));
    }
    let group = String::from_utf8_lossy(&p[2..2 + n]).into_owned();
    let mut q = 2 + n;
    let tid = u128::from_be_bytes(p[q..q + 16].try_into().unwrap());
    q += 16;
    let partition = i32::from_be_bytes(p[q..q + 4].try_into().unwrap());
    q += 4;
    let first = i64::from_be_bytes(p[q..q + 8].try_into().unwrap());
    q += 8;
    let last = i64::from_be_bytes(p[q..q + 8].try_into().unwrap());
    q += 8;
    let ty = p[q] as i8;
    Ok((group, tid, partition, first, last, ty))
}

fn parse_create(
    p: &[u8],
) -> std::io::Result<(String, i32, i32, bool, basalt_metadata::TopicRetention)> {
    if p.len() < 2 {
        return Err(std::io::Error::other("short create"));
    }
    let n = i16::from_be_bytes(p[0..2].try_into().unwrap()) as usize;
    if p.len() < 2 + n + 8 {
        return Err(std::io::Error::other("short create fields"));
    }
    let name = String::from_utf8_lossy(&p[2..2 + n]).into_owned();
    let partitions = i32::from_be_bytes(p[2 + n..6 + n].try_into().unwrap());
    let rf = i32::from_be_bytes(p[6 + n..10 + n].try_into().unwrap());
    // tiered 尾字节（T-M4.3 v2）；旧客户端缺字节 → false
    let tiered = p.len() > 10 + n && p[10 + n] != 0;
    // retention 尾缀（per-topic retention）；旧客户端缺失 → None
    let mut retention = basalt_metadata::TopicRetention::default();
    let mut q = 10 + n + 1;
    if p.len() >= q + 10 {
        let has_ms = p[q] != 0;
        q += 1;
        if has_ms {
            retention.ms = Some(u64::from_be_bytes(p[q..q + 8].try_into().unwrap()));
        }
        q += 8;
        let has_bytes = p.get(q).copied().unwrap_or(0) != 0;
        q += 1;
        if has_bytes && p.len() >= q + 8 {
            retention.bytes = Some(u64::from_be_bytes(p[q..q + 8].try_into().unwrap()));
        }
    }
    Ok((name, partitions, rf, tiered, retention))
}

fn parse_fetch_slice(p: &[u8]) -> std::io::Result<(String, i32, i32, i64, usize)> {
    if p.len() < 2 {
        return Err(std::io::Error::other("short fetch slice"));
    }
    let n = i16::from_be_bytes(p[0..2].try_into().unwrap()) as usize;
    if p.len() < 2 + n + 20 {
        return Err(std::io::Error::other("short fetch slice fields"));
    }
    let topic = String::from_utf8_lossy(&p[2..2 + n]).into_owned();
    let mut o = 2 + n;
    let partition = i32::from_be_bytes(p[o..o + 4].try_into().unwrap());
    o += 4;
    let follower = i32::from_be_bytes(p[o..o + 4].try_into().unwrap());
    o += 4;
    let from = i64::from_be_bytes(p[o..o + 8].try_into().unwrap());
    o += 8;
    let max = u32::from_be_bytes(p[o..o + 4].try_into().unwrap()) as usize;
    Ok((topic, partition, follower, from, max))
}

#[cfg(test)]
mod failover_tests {
    use super::*;
    use basalt_metadata::cluster::BrokerInfo;

    #[test]
    fn failover_triggers_on_dead_leader() {
        let (_, rx) = mpsc::channel(16);
        let dir = std::env::temp_dir().join(format!("ctrl-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let log = dir.join("ctrl.log");
        let mut ctrl = Controller::open(0, log.clone(), Duration::from_millis(4000), rx, None, Vec::new());
        for i in 0..3 {
            ctrl.apply_and_persist(&ClusterRecord::RegisterBroker(BrokerInfo {
                node_id: i, host: "h".into(), port: 9000 + i as u16,
            }));
        }
        ctrl.apply_and_persist(&ClusterRecord::CreateTopic { name: "t".into(), partitions: 1, rf: 3, tiered: false, retention: Default::default() });
        // p0 的 leader 迁到 node1，随后 node1 死亡（无心跳）
        ctrl.apply_and_persist(&ClusterRecord::LeaderChange { topic: "t".into(), partition: 0, leader: 1, epoch: 1 });
        let now = Instant::now();
        ctrl.last_heartbeat.insert(0, now);
        ctrl.last_heartbeat.insert(2, now);
        // node1 不插入（从未心跳/已过期）
        ctrl.failover_check();
        let a = ctrl.state.assignment("t", 0).unwrap();
        assert_ne!(a.leader, 1, "leader must move off the dead node");
        assert!([0, 2].contains(&a.leader), "leader must be an alive replica");
        assert_eq!(a.epoch, 2);
        let _ = std::fs::remove_file(&log);
        let _ = std::fs::remove_dir(&dir);
    }
}
