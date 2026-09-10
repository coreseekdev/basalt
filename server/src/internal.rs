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

    pub async fn create_topic(&self, name: &str, partitions: i32, rf: i32) -> std::io::Result<Bytes> {
        let mut p = Vec::new();
        p.extend_from_slice(&(name.len() as i16).to_be_bytes());
        p.extend_from_slice(name.as_bytes());
        p.extend_from_slice(&partitions.to_be_bytes());
        p.extend_from_slice(&rf.to_be_bytes());
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
    CreateTopic { name: String, partitions: i32, rf: i32, reply: oneshot::Sender<()> },
    ApplySnapshot { state: ClusterState },
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
}

impl Controller {
    pub fn spawn(node_id: i32, log_path: PathBuf, heartbeat_timeout: Duration) -> mpsc::Sender<ControllerCmd> {
        let (tx, rx) = mpsc::channel(256);
        let ctrl = Controller::open(node_id, log_path, heartbeat_timeout, rx);
        tokio::spawn(ctrl.run());
        tx
    }

    async fn run(mut self) {
        loop {
            match tokio::time::timeout(Duration::from_millis(500), self.rx.recv()).await {
                Ok(Some(cmd)) => {
                    self.handle(cmd);
                    // 命令流量可能持续不断（MetaSync 轮询），failover 检查不能只依赖超时分支
                    self.failover_check();
                }
                Ok(None) => break,
                Err(_) => self.failover_check(),
            }
        }
    }

    fn handle(&mut self, cmd: ControllerCmd) {
        match cmd {
            ControllerCmd::Register { info, reply } => {
                self.apply_and_persist(&ClusterRecord::RegisterBroker(info.clone()));
                // 心跳键 = 本次注册的节点（而非"当前最大 id"——那会键错位）
                self.last_heartbeat.insert(info.node_id, Instant::now());
                let _ = reply.send(());
            }
            ControllerCmd::Heartbeat { node_id } => {
                self.last_heartbeat.insert(node_id, Instant::now());
            }
            ControllerCmd::Sync { version, reply } => {
                let _ = reply.send(if self.state.version > version {
                    Some(self.state.encode())
                } else {
                    None
                });
            }
            ControllerCmd::CreateTopic { name, partitions, rf, reply } => {
                let exists = self.state.assignments.iter().any(|a| a.topic == name);
                if !exists {
                    self.apply_and_persist(&ClusterRecord::CreateTopic { name, partitions, rf });
                }
                let _ = reply.send(());
            }
            ControllerCmd::ApplySnapshot { state } => {
                self.state = state;
            }
        }
    }

    /// 心跳超时 → leader failover（epoch+1，副本轮转）。
    fn failover_check(&mut self) {
        let now = Instant::now();
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
                    self.apply_and_persist(&rec);
                }
            }
        }
    }
}

impl Controller {
    pub fn open(node_id: i32, log_path: PathBuf, heartbeat_timeout: Duration, rx: mpsc::Receiver<ControllerCmd>) -> Controller {
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
        ClusterRecord::CreateTopic { name, partitions, rf } => {
            b.push(2);
            b.extend_from_slice(&(name.len() as i16).to_be_bytes());
            b.extend_from_slice(name.as_bytes());
            b.extend_from_slice(&partitions.to_be_bytes());
            b.extend_from_slice(&rf.to_be_bytes());
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
            ClusterRecord::CreateTopic { name, partitions, rf }
        }
        3 => {
            let topic = gstr(&mut p);
            let partition = g32(&mut p);
            let leader = g32(&mut p);
            let epoch = g32(&mut p);
            ClusterRecord::LeaderChange { topic, partition, leader, epoch }
        }
        _ => return None,
    })
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

async fn handle_internal_conn(
    mut sock: tokio::net::TcpStream,
    ctx: InternalCtx,
) -> std::io::Result<()> {
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
            MSG_CREATE_TOPIC => {
                let Ok((name, partitions, rf)) = parse_create(payload) else {
                    sock.write_all(&short_frame()).await?;
                    return Ok(());
                };
                if let Some(tx) = &ctx.controller_tx {
                    let (txr, rxr) = tokio::sync::oneshot::channel();
                    let _ = tx.send(ControllerCmd::CreateTopic { name, partitions, rf, reply: txr }).await;
                    let _ = rxr.await;
                }
                Bytes::from(0i16.to_be_bytes().to_vec())
            }
            MSG_FETCH_SLICE => {
                let Ok((topic, partition, follower_id, from_offset, max_bytes)) = parse_fetch_slice(payload) else {
                    sock.write_all(&short_frame()).await?;
                    return Ok(());
                };
                let route = {
                    let routes = ctx.routes_rx.borrow();
                    routes.find(&topic, partition).cloned()
                };
                let resp = match route {
                    None => Bytes::from(error_slice(6)),
                    Some(route) if route.leader != ctx.node_id => Bytes::from(error_slice(6)),
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

fn parse_create(p: &[u8]) -> std::io::Result<(String, i32, i32)> {
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
    Ok((name, partitions, rf))
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
        let mut ctrl = Controller::open(0, log.clone(), Duration::from_millis(4000), rx);
        for i in 0..3 {
            ctrl.apply_and_persist(&ClusterRecord::RegisterBroker(BrokerInfo {
                node_id: i, host: "h".into(), port: 9000 + i as u16,
            }));
        }
        ctrl.apply_and_persist(&ClusterRecord::CreateTopic { name: "t".into(), partitions: 1, rf: 3 });
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
