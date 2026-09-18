//! 连接任务：帧读取 → 头解析 → 版本协商 → 分发 handler → 帧回写。
//!
//! 每连接独占读/写缓冲（BytesMut 复用 = 连接级内存池）。

use crate::handlers::{self, Ctx, FetchTarget, ProduceTarget};
use crate::handlers_consumer;
use crate::handlers_groups;
use crate::handlers_txn;
use basalt_protocol::api::key;
use basalt_protocol::codec;
use basalt_protocol::error::ProtocolError;
use basalt_protocol::frame;
use basalt_protocol::registry::Registry;
use basalt_protocol::value::{s, Struct, Value};
use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MAX_FRAME: i32 = 100 * 1024 * 1024;

fn env_u64(name: &str) -> u64 {
    static CACHE: std::sync::OnceLock<(u64, u64)> = std::sync::OnceLock::new();
    let (p, f) = CACHE.get_or_init(|| {
        (
            std::env::var("BASALT_QUOTA_PRODUCER_BYTES").ok().and_then(|v| v.parse().ok()).unwrap_or(0),
            std::env::var("BASALT_QUOTA_FETCH_BYTES").ok().and_then(|v| v.parse().ok()).unwrap_or(0),
        )
    });
    match name {
        "p" => *p,
        _ => *f,
    }
}

/// per-user 配额覆盖（T-M4.2 尾项）：`BASALT_QUOTA_USER_BYTES="app:p=500000;f=1000000,admin:f=2000000"`
/// ——条目 `,` 分隔、维度 `;` 分隔；SASL 认证完成后按键覆盖全局默认；
/// 无条目回落全局。（有限兼容：Kafka 的 (user, client-id) 二维实体面
/// 不做，user 一维先行）
fn user_quotas() -> &'static std::collections::HashMap<String, (u64, u64)> {
    static U: std::sync::OnceLock<std::collections::HashMap<String, (u64, u64)>> = std::sync::OnceLock::new();
    U.get_or_init(|| parse_user_quotas(&std::env::var("BASALT_QUOTA_USER_BYTES").unwrap_or_default()))
}

/// 解析 `"user:p=1,f=2,user2:f=3"`（独立纯函数——env 缓存不可测试注入）
fn parse_user_quotas(raw: &str) -> std::collections::HashMap<String, (u64, u64)> {
    let mut m = std::collections::HashMap::new();
    for ent in raw.split(',') {
        let mut parts = ent.splitn(2, ':');
        let (Some(user), Some(rest)) = (parts.next(), parts.next()) else { continue };
        let (mut p, mut f) = (0u64, 0u64);
        for kv in rest.split(';') {
            let mut kv = kv.splitn(2, '=');
            match (kv.next(), kv.next().and_then(|v| v.parse::<u64>().ok())) {
                (Some("p"), Some(v)) => p = v,
                (Some("f"), Some(v)) => f = v,
                _ => {}
            }
        }
        if !user.is_empty() {
            m.insert(user.to_string(), (p, f));
        }
    }
    m
}

/// 连接级认证/配额共享状态（T-S1/S5）。请求任务并发处理 → Mutex 串行；
/// SASL 握手天然两轮串行，锁竞争可忽略。
pub struct ConnState {
    /// SASL 认证后的用户；None = 未认证（BASALT_AUTH 非 scram 时恒 Some("*")）
    pub user: Option<String>,
    /// SaslHandshake 完成标记（无握手直接 Authenticate = ILLEGAL_SASL_STATE）
    handshake_done: bool,
    /// 认证失败/非法请求 → 响应写出后断连（Kafka broker 同语义）
    pub close_after: bool,
    scram: crate::sasl::ScramSession,
    /// produce/fetch 字节率配额（bytes/sec；0 = 不限）
    quota_produce_rate: u64,
    quota_fetch_rate: u64,
    produce_tokens: f64,
    fetch_tokens: f64,
    last_replenish: std::time::Instant,
}

impl Default for ConnState {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnState {
    pub fn new() -> Self {
        let auth = crate::sasl::auth_enabled();
        ConnState {
            user: if auth { None } else { Some("*".into()) },
            handshake_done: false,
            close_after: false,
            scram: crate::sasl::ScramSession::default(),
            quota_produce_rate: env_u64("p"),
            quota_fetch_rate: env_u64("f"),
            // 桶初始 = 1 秒突发额度（空桶会惩罚连接后首个请求）
            produce_tokens: env_u64("p") as f64,
            fetch_tokens: env_u64("f") as f64,
            last_replenish: std::time::Instant::now(),
        }
    }
    /// 测试注入构造（绕过 env 读取——cargo test 并行线程下 env 竞争）
    #[cfg(test)]
    fn with_params(auth_enabled: bool, quota_produce: u64, quota_fetch: u64) -> Self {
        ConnState {
            user: if auth_enabled { None } else { Some("*".into()) },
            handshake_done: false,
            close_after: false,
            scram: crate::sasl::ScramSession::default(),
            quota_produce_rate: quota_produce,
            quota_fetch_rate: quota_fetch,
            produce_tokens: quota_produce as f64,
            fetch_tokens: quota_fetch as f64,
            last_replenish: std::time::Instant::now(),
        }
    }
    pub fn quota_enabled(&self) -> bool { self.quota_produce_rate > 0 || self.quota_fetch_rate > 0 }
    /// SASL 认证完成后套用 per-user 配额覆盖（无条目回落全局默认；
    /// 桶重置为新速率的一秒突发——切换前后速率语义自洽）
    pub fn apply_user_quota(&mut self, user: &str) {
        if let Some(&(p, f)) = user_quotas().get(user) {
            self.quota_produce_rate = p;
            self.quota_fetch_rate = f;
            self.produce_tokens = p as f64;
            self.fetch_tokens = f as f64;
        }
    }
    /// 节流原语（连接锁内调用）：持锁休眠 = 后续请求在锁上排队，
    /// 请求按配额速率串行化——pipeline 并发不能稀释节流率（否则并发
    /// 任务各自看到空桶各自 sleep，吞吐 = 配额 × 并发度）
    async fn throttled(conn: &tokio::sync::Mutex<ConnState>, is_produce: bool, bytes: u64) {
        let mut cs = conn.lock().await;
        let delay = cs.take_quota(is_produce, bytes);
        if delay > 0 {
            tracing::trace!(delay_ms = delay, bytes, produce = is_produce, "throttled");
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
        }
    }
    /// 未认证连接仅放行版本协商与 SASL 三 API；其余静默断连（认证门禁，
    /// Kafka 同语义——SASL 端口上的明文请求直接关闭）
    pub fn admits(&self, api_key: i16) -> bool {
        if self.user.is_some() {
            return true;
        }
        matches!(api_key, key::API_VERSIONS | key::SASL_HANDSHAKE | key::SASL_AUTHENTICATE)
    }
    /// 消费配额：纯计算返回节流毫秒（超出配额时 >0）；突发上限 2×rate。
    /// tokens 允许为负（欠账）——休眠本身不再产生可消费额度，否则睡醒的
    /// 请求把休眠期回补的额度瞬时吃掉，节流率 = 配额 × 并发（实证 2×）。
    fn take_quota(&mut self, is_produce: bool, bytes: u64) -> u64 {
        let (rate, tokens) = if is_produce {
            (self.quota_produce_rate, &mut self.produce_tokens)
        } else {
            (self.quota_fetch_rate, &mut self.fetch_tokens)
        };
        if rate == 0 { return 0; }
        let now = std::time::Instant::now();
        let elapsed = now.duration_since(self.last_replenish).as_secs_f64();
        self.last_replenish = now;
        *tokens = (*tokens + rate as f64 * elapsed).min(rate as f64 * 2.0);
        *tokens -= bytes as f64;
        if *tokens >= 0.0 {
            0
        } else {
            ((-*tokens / rate as f64) * 1000.0) as u64
        }
    }
}

// BufferPool 参数：进程单例共享资源池（内部 Mutex 串行化）——Arc 表达资源
// 共享而非共享可变所有权，与 Bytes/mpsc 内部引用计数同级豁免（ADR-13）。
/// 处理一连接：读半 + 写半分离；请求处理并发、写出按请求序重排。
///
/// 认证/配额状态为连接内共享（Arc<Mutex<ConnState>>，请求任务并发访问）；
/// 认证失败/未认证越权请求经 close watch 通知读侧断连（响应先行写出）。
// BufferPool 进程单例共享资源池（内部 Mutex 串行化）：Arc 表达资源共享而非
// 共享可变所有权，与 Bytes/mpsc 内部引用计数同级豁免（ADR-13）。
#[allow(clippy::disallowed_types)]
pub async fn serve_connection<S>(
    sock: S,
    peer: std::net::SocketAddr,
    ctx: Ctx,
    // BufferPool 进程单例共享资源池（内部 Mutex 串行化）：Arc 表达资源共享而非
    // 共享可变所有权，与 Bytes/mpsc 内部引用计数同级豁免（ADR-13）。
    pool: std::sync::Arc<basalt_storage::pool::BufferPool>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let conn = std::sync::Arc::new(tokio::sync::Mutex::new(ConnState::new()));
    if conn.lock().await.quota_enabled() {
        tracing::debug!(peer = %peer, "connection quotas active");
    }
    // 读半 + 写半分离：请求处理可并发（消除队头阻塞——长轮询 fetch 不再拖死同连接
    // 的 offset commit/heartbeat）。⚠ Kafka 线协议要求同连接响应按请求序返回
    // （correlation id 匹配的前提；Java kafka-clients 严格按序）——处理可以
    // 乱序完成，写出必须按请求序：写任务按 (seq, resp) 重排缓冲
    let (close_tx, mut close_rx) = tokio::sync::watch::channel(false);
    let (mut rd, mut wr) = tokio::io::split(sock);
    let (resp_tx, mut resp_rx) = tokio::sync::mpsc::channel::<(u64, Option<Bytes>)>(256);

    // 写任务：唯一写者 + 请求序重排；写完后归还读缓冲到池（perf #2）
    let writer_pool = pool.clone();
    let writer = tokio::spawn(async move {
        let mut pending: std::collections::BTreeMap<u64, Option<Bytes>> = Default::default();
        let mut write_seq: u64 = 1;   // 请求序号从 1 起（读侧先增再派发）
        while let Some((seq, resp)) = resp_rx.recv().await {
            pending.insert(seq, resp);
            while let Some(resp) = pending.remove(&write_seq) {
                write_seq += 1;
                let Some(resp) = resp else { continue }; // acks=0：占位无响应
                if wr.write_all(&resp).await.is_err() {
                    return;
                }
                // 归还唯一所有的读缓冲（Bytes 唯一 → BytesMut → 池）
                if let Ok(bm) = Bytes::try_into_mut(resp) {
                    use basalt_storage::pool::BufferPool;
                    BufferPool::release(&writer_pool, bm);
                }
            }
        }
    });

    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut req_seq: u64 = 0;
    loop {
        // 认证失败/越权断连：请求任务发完错误响应后置 close；读侧在 select
        // 中立即退出 → resp_tx drop → writer 冲刷完队列后关闭连接
        let len = tokio::select! {
            r = read_frame_len(&mut rd, &mut read_buf) => match r {
                Ok(l) => l,
                Err(FramedError::Io(_)) => break, // 连接关闭
                Err(FramedError::TooLarge) => break,
            },
            _ = close_rx.changed() => break,
        };
        if len == 0 {
            break;
        }
        let frame_bytes = match read_exact_bytes(&mut rd, &mut read_buf, len as usize).await {
            Ok(b) => b,
            Err(_) => break,
        };
        // 认证门禁（T-S1）：未认证连接只放行 ApiVersions/SaslHandshake/
        // SaslAuthenticate——先偷看 api_key，越权请求直接断连（Kafka 同语义：
        // SASL 端口上的明文请求不回错误、直接关闭）
        if frame_bytes.len() >= 2 {
            let peek = i16::from_be_bytes([frame_bytes[0], frame_bytes[1]]);
            if !conn.lock().await.admits(peek) {
                tracing::warn!(peer = %peer, api = peek, "request before SASL auth; closing");
                break;
            }
        }
        // 慢消费者背压：响应通道满时暂停读（客户端不读就不给它读下一请求）
        let ctx = ctx.clone_for_request();
        let resp_tx = resp_tx.clone();
        let conn = conn.clone();
        let close_tx = close_tx.clone();
        req_seq += 1;
        let seq = req_seq;
        // PRODUCE 内联处理（账本 58）：幂等生产者的 seq 序 = 连接请求序，
        // 分区 actor 必须按请求序收到 produce——per-request 并发派发下，同一
        // pipeline 的两个 produce 在 actor 邮箱上竞速，后发先至 → 服务端正确
        // 回 OOOSN，但客户端侧语义是整 PID reload（本地 epoch bump + 回卷重
        // 发 = 已落盘区间重复）+ 元数据 5s 停等 + produceTimeout 静默丢批的
        // 级联（Kafka：produce 按连接序应用是幂等 seq 的前提）。处理序 = 读
        // 序，零竞态。代价：produce 应答期间本连接暂停读——acks=all 停等
        // （RF≥2）最长 10s；生产者连接不同时做长轮询消费，队头阻塞面可忽略。
        let is_produce = frame_bytes.len() >= 8
            && i16::from_be_bytes([frame_bytes[0], frame_bytes[1]]) == key::PRODUCE;
        if is_produce {
            match dispatch(frame_bytes, &ctx, &conn).await {
                Ok(outcome) => {
                    let _ = resp_tx.send((seq, outcome.resp)).await;
                    if outcome.close_after {
                        let _ = close_tx.send(true);
                    }
                }
                Err(DispatchError::UnknownApi) | Err(DispatchError::Protocol(_)) => {
                    let _ = resp_tx.send((seq, None)).await;
                }
                Err(DispatchError::UnsupportedVersion(api_key, version)) => {
                    if let Some(b) = synth_unsupported(api_key, version).map(Bytes::from) {
                        let _ = resp_tx.send((seq, Some(Bytes::from(b)))).await;
                    } else {
                        let _ = resp_tx.send((seq, None)).await;
                    }
                }
            }
            continue;
        }
        tokio::spawn(async move {
            match dispatch(frame_bytes, &ctx, &conn).await {
                Ok(outcome) => {
                    match outcome.resp {
                        Some(resp) => { let _ = resp_tx.send((seq, Some(resp))).await; }
                        None => {
                            // acks=0：无响应，但序号必须占位推进（保持请求序）
                            let _ = resp_tx.send((seq, None)).await;
                        }
                    }
                    if outcome.close_after {
                        let _ = close_tx.send(true);
                    }
                }
                Err(DispatchError::UnknownApi) => {
                    tracing::warn!(peer = %peer, "unknown api key");
                    // 序号占位：写任务严格按请求序写出——任何分支不占位都会
                    // 在该连接上制造永久缺口，卡死其后全部响应（kafka-clients
                    // 档实证：一个解析失败请求卡死整连接）
                    let _ = resp_tx.send((seq, None)).await;
                }
                Err(DispatchError::UnsupportedVersion(api_key, version)) => {
                    if let Some(b) = synth_unsupported(api_key, version).map(Bytes::from) {
                        let _ = resp_tx.send((seq, Some(Bytes::from(b)))).await;
                    } else {
                        let _ = resp_tx.send((seq, None)).await;
                    }
                }
                Err(DispatchError::Protocol(e)) => {
                    tracing::warn!(peer = %peer, error = %e, "protocol error");
                    let _ = resp_tx.send((seq, None)).await;
                }
            }
        });
    }
    drop(resp_tx);
    let _ = writer.await;
}

enum FramedError {
    Io(#[allow(dead_code)] std::io::Error),
    TooLarge,
}

async fn read_frame_len<R: tokio::io::AsyncRead + Unpin>(sock: &mut R, buf: &mut BytesMut) -> Result<i32, FramedError> {
    while buf.len() < 4 {
        let n = sock.read_buf(buf).await.map_err(FramedError::Io)?;
        if n == 0 {
            // EOF：连接已关闭——必须退出，否则空转烧满一个核
            return Err(FramedError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed",
            )));
        }
    }
    let len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    buf.advance(4);
    if len < 0 || len > MAX_FRAME {
        return Err(FramedError::TooLarge);
    }
    Ok(len)
}

async fn read_exact_bytes<R: tokio::io::AsyncRead + Unpin>(
    sock: &mut R,
    buf: &mut BytesMut,
    len: usize,
) -> std::io::Result<Bytes> {
    buf.reserve(len);
    while buf.len() < len {
        let n = sock.read_buf(buf).await?;
        if n == 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "conn closed"));
        }
    }
    Ok(buf.split_to(len).freeze())
}

enum DispatchError {
    UnknownApi,
    UnsupportedVersion(i16, i16),
    Protocol(ProtocolError),
}

/// 单请求分派结果：resp = 响应帧（None = acks=0 占位）；close_after =
/// 响应写出后断连（SASL 失败/越权语义）
struct DispatchOutcome {
    resp: Option<Bytes>,
    close_after: bool,
}

impl From<ProtocolError> for DispatchError {
    fn from(e: ProtocolError) -> Self {
        DispatchError::Protocol(e)
    }
}

/// 处理一帧；返回完整响应帧（含长度前缀）。
async fn dispatch(
    frame_bytes: Bytes,
    ctx: &Ctx,
    conn: &tokio::sync::Mutex<ConnState>,
) -> Result<DispatchOutcome, DispatchError> {
    let reg = Registry::global();
    // 预读 api_key/version 以定头版本（先偷看前 4 字节，读头时再正式解析）
    if frame_bytes.len() < 8 {
        // 头部最少 8 字节（api_key+api_version+correlation_id）
        return Err(ProtocolError::UnexpectedEof { pos: 0, need: 8 }.into());
    }
    let api_key = i16::from_be_bytes([frame_bytes[0], frame_bytes[1]]);
    let _api_version = i16::from_be_bytes([frame_bytes[2], frame_bytes[3]]);

    let Some(entry) = reg.api(api_key) else {
        return Err(DispatchError::UnknownApi);
    };
    let (head, body_start) = match frame::read_request_header(&frame_bytes, entry.flexible_from) {
        Ok(x) => x,
        Err(e) => {
            // KIP-511 探测语义：协商前的 ApiVersions 可能用旧头/无 tag section，
            // 解析失败也要以 v0 语义回 UNSUPPORTED_VERSION（corr 在固定偏移 4..8）
            if api_key == key::API_VERSIONS {
                return Ok(DispatchOutcome { resp: Some(Bytes::from(synth_apiversions_unsupported(&frame_bytes))), close_after: false });
            }
            return Err(DispatchError::Protocol(e));
        }
    };
    let (api_key, api_version) = (head.api_key, head.api_version);

    let flexible = frame::is_flexible(api_version, entry.flexible_from);
    let supported = reg
        .advertised()
        .iter()
        .find(|(k, _, _, _)| *k == api_key)
        .map(|(_, lo, hi, _)| api_version >= *lo && api_version <= *hi)
        .unwrap_or(false);

    if !supported {
        return Err(DispatchError::UnsupportedVersion(api_key, api_version));
    }

    let body = match frame::split_body(&frame_bytes, body_start) {
        Ok(b) => b,
        Err(e) => return Err(DispatchError::Protocol(e)),
    };
    let req = match codec::decode(&entry.request.fields, api_version, flexible, &body) {
        Ok(r) => r,
        Err(e) => {
            // KIP-511：客户端在版本协商前可能用旧头发 ApiVersions 探测。
            // Kafka broker 语义：该请求解析失败 → 以 v0 格式回 UNSUPPORTED_VERSION，连接保活。
            if api_key == key::API_VERSIONS {
                return Ok(DispatchOutcome { resp: Some(Bytes::from(synth_apiversions_unsupported(&frame_bytes))), close_after: false });
            }
            return Err(DispatchError::Protocol(e));
        }
    };

    tracing::trace!(api = api_key, version = api_version, corr = head.correlation_id, "request");

    let (resp_value, acks_none, mut close_after) = match api_key {
        key::API_VERSIONS => {
            let v = handlers::api_versions(&req, api_version);
            (v, false, false)
        }
        key::SASL_HANDSHAKE => sasl_handshake(&req, conn).await,
        key::SASL_AUTHENTICATE => sasl_authenticate(&req, conn).await,
        key::METADATA => (handlers::metadata(&req, api_version, ctx).await, false, false),
        key::PRODUCE => {
            let (v, acks_none) = handle_produce(&req, api_version, ctx, conn).await?;
            (v, acks_none, false)
        }
        key::FETCH => {
            let parsed = parse_fetch(&req, api_version)?;
            let (v, close) = handle_fetch(parsed, ctx).await?;
            (v, false, close)
        }
        key::LIST_OFFSETS => (handlers::list_offsets(&req, ctx).await, false, false),
        key::FIND_COORDINATOR => (handlers_groups::find_coordinator(api_version, &req, ctx).await, false, false),
        key::JOIN_GROUP => (handlers_groups::join_group(&req, api_version, ctx).await, false, false),
        key::SYNC_GROUP => (handlers_groups::sync_group(&req, ctx).await, false, false),
        key::HEARTBEAT => (handlers_groups::heartbeat(&req, ctx).await, false, false),
        key::LEAVE_GROUP => (handlers_groups::leave_group(&req, ctx).await, false, false),
        key::OFFSET_COMMIT => (handlers_groups::offset_commit(&req, ctx).await, false, false),
        key::OFFSET_FETCH => (handlers_groups::offset_fetch(&req, api_version, ctx).await, false, false),
        key::CREATE_TOPICS => (handlers_groups::create_topics(&req, ctx).await, false, false),
        key::DELETE_TOPICS => (handlers_groups::delete_topics(&req, ctx).await, false, false),
        key::DELETE_RECORDS => (handlers_groups::delete_records_handler(&req, ctx).await, false, false),
        key::INIT_PRODUCER_ID => (handlers_groups::init_producer_id(&req, ctx).await, false, false),
        key::ADD_PARTITIONS_TO_TXN => (handlers_txn::add_partitions_to_txn(api_version, &req, ctx).await, false, false),
        key::ADD_OFFSETS_TO_TXN => (handlers_txn::add_offsets_to_txn(&req, ctx).await, false, false),
        key::END_TXN => (handlers_txn::end_txn(&req, ctx).await, false, false),
        key::TXN_OFFSET_COMMIT => (handlers_txn::txn_offset_commit(api_version, &req, ctx).await, false, false),
        key::DESCRIBE_TRANSACTIONS => (handlers_txn::describe_transactions(&req, ctx).await, false, false),
        key::LIST_TRANSACTIONS => (handlers_txn::list_transactions(&req, ctx).await, false, false),
        key::OFFSET_FOR_LEADER_EPOCH => (handlers::offset_for_leader_epoch(&req, ctx).await, false, false),
        key::CONSUMER_GROUP_HEARTBEAT => (handlers_consumer::consumer_group_heartbeat(&req, ctx).await, false, false),
        key::CONSUMER_GROUP_DESCRIBE => (handlers_consumer::consumer_group_describe(&req, ctx).await, false, false),
        key::DESCRIBE_GROUPS => (handlers_groups::describe_groups(&req, ctx).await, false, false),
        key::LIST_GROUPS => (handlers_groups::list_groups(&req, ctx).await, false, false),
        key::DESCRIBE_CLUSTER => (handlers::describe_cluster(&req, ctx), false, false),
        key::DESCRIBE_CONFIGS => (handlers::describe_configs(&req, ctx).await, false, false),
        _ => {
            return Err(DispatchError::UnsupportedVersion(api_key, api_version));
        }
    };

    // 响应编码（perf #3）：预留 4B 长度前缀占位，编码后回填——消除
    // framed 中间缓冲与 to_vec 的两次整响应拷贝；freeze 后零拷贝进写通道。
    // 缓冲取自共享池：writer 发送后按唯一所有权归还（review 四轮 P2-1
    // ——此前 with_capacity 新建、writer 归还的是从未入池的缓冲，闭环空转）
    let mut out = ctx.pool.acquire(4 + 512);
    out.extend_from_slice(&[0u8; 4]); // 长度前缀占位
    let resp_header_v = frame::response_header_version(api_key, flexible);
    frame::write_response_header(&mut out, head.correlation_id, resp_header_v);
    codec::encode_struct_fields(&entry.response.fields, api_version, flexible, &resp_value_value(&resp_value), &mut out)?;
    let total = (out.len() - 4) as i32;
    out[0..4].copy_from_slice(&total.to_be_bytes());
    // fetch 字节率配额（S5）：按响应字节数节流（produce 在 handle_produce
    // 按请求批字节）。节流以延迟响应实现（v0 语义；效果等同配额约束速率）
    if api_key == key::FETCH {
        ConnState::throttled(conn, false, total as u64).await;
    }
    Ok(DispatchOutcome {
        resp: if acks_none { None } else { Some(out.freeze()) },
        close_after,
    })
}

fn resp_value_value(v: &Value) -> &Struct {
    match v {
        Value::Struct(s) => s,
        _ => unreachable!("responses are structs"),
    }
}

// ---------- SASL（T-S1：SaslHandshake v0-1 / SaslAuthenticate v0-2）----------

/// 机制协商：仅 SCRAM-SHA-256。错误机制回 33 + 机制表后断连（Kafka 同语义）
async fn sasl_handshake(req: &Struct, conn: &tokio::sync::Mutex<ConnState>) -> (Value, bool, bool) {
    let mech = req.get("Mechanism").map(|v| v.as_str().to_string()).unwrap_or_default();
    let mechanisms = Value::Array(
        crate::sasl::MECHANISMS.iter().map(|m| Value::str(*m)).collect(),
    );
    if !crate::sasl::MECHANISMS.contains(&mech.as_str()) {
        tracing::warn!(mechanism = %mech, "sasl handshake: unsupported mechanism");
        let mut cs = conn.lock().await;
        cs.handshake_done = false;
        cs.close_after = true;
        return (
            s([
                ("ErrorCode", Value::I16(basalt_protocol::api::ErrorCode::UnsupportedSaslMechanism as i16)),
                ("Mechanisms", mechanisms),
            ]),
            false,
            true,
        );
    }
    let mut cs = conn.lock().await;
    cs.scram = crate::sasl::ScramSession::default(); // 重复握手重开会话
    cs.handshake_done = true;
    (
        s([
            ("ErrorCode", Value::I16(0)),
            ("Mechanisms", mechanisms),
        ]),
        false,
        false,
    )
}

/// SCRAM 两轮认证。Continue → server-first（err 0）；Authenticated → 置
/// 认证态 + 回 server-final（`v=<sig>`，kafka-python/franz-go 均校验签名）；
/// Failed → 58 后断连。无握手直接 Authenticate → 34 后断连。
async fn sasl_authenticate(req: &Struct, conn: &tokio::sync::Mutex<ConnState>) -> (Value, bool, bool) {
    let auth_bytes = match req.get("AuthBytes") {
        Some(Value::Bytes(b)) => b.clone(),
        _ => Bytes::new(),
    };
    let mut cs = conn.lock().await;
    if !cs.handshake_done {
        tracing::warn!("sasl authenticate before handshake");
        cs.close_after = true;
        return (
            s([
                ("ErrorCode", Value::I16(basalt_protocol::api::ErrorCode::IllegalSaslState as i16)),
                ("ErrorMessage", Value::str("SaslAuthenticate request received before SaslHandshake")),
                ("AuthBytes", Value::Bytes(Bytes::new())),
                ("SessionLifetimeMs", Value::I64(0)),
            ]),
            false,
            true,
        );
    }
    use crate::sasl::{scram_authenticate, ScramOutcome};
    match scram_authenticate(&mut cs.scram, crate::sasl::global_users(), &auth_bytes) {
        ScramOutcome::Continue { data } => (
            s([
                ("ErrorCode", Value::I16(0)),
                ("ErrorMessage", Value::Null),
                ("AuthBytes", Value::Bytes(Bytes::from(data))),
                ("SessionLifetimeMs", Value::I64(0)),
            ]),
            false,
            false,
        ),
        ScramOutcome::Authenticated { user, server_final } => {
            tracing::info!(user = %user, "sasl authenticated");
            cs.apply_user_quota(&user);
            cs.user = Some(user);
            cs.handshake_done = false; // 会话终结；重认证须重新握手
            (
                s([
                    ("ErrorCode", Value::I16(0)),
                    ("ErrorMessage", Value::Null),
                    ("AuthBytes", Value::Bytes(Bytes::from(server_final))),
                    ("SessionLifetimeMs", Value::I64(0)),
                ]),
                false,
                false,
            )
        }
        ScramOutcome::Failed { reason } => {
            tracing::warn!(reason = %reason, "sasl authentication failed");
            crate::sasl::reset_session(&mut cs.scram);
            cs.handshake_done = false;
            cs.close_after = true;
            (
                s([
                    ("ErrorCode", Value::I16(basalt_protocol::api::ErrorCode::SaslAuthenticationFailed as i16)),
                    ("ErrorMessage", Value::str(format!(
                        "SASL authentication failed: {reason}"
                    ))),
                    ("AuthBytes", Value::Bytes(Bytes::new())),
                    ("SessionLifetimeMs", Value::I64(0)),
                ]),
                false,
                true,
            )
        }
    }
}

async fn handle_produce(
    req: &Struct,
    version: i16,
    ctx: &Ctx,
    conn: &tokio::sync::Mutex<ConnState>,
) -> Result<(Value, bool), DispatchError> {
    let acks = req.get("Acks").map(|v| v.as_i16()).unwrap_or(1);
    let mut targets = Vec::new();
    let mut batch_bytes: u64 = 0;
    if let Some(Value::Array(topics)) = req.get("TopicData") {
        for t in topics {
            let Value::Struct(ts) = t else { continue };
            let name = ts.get("Name").map(|x| x.as_str().to_string()).unwrap_or_default();
            let topic_id = ts.get("TopicId").map(|x| x.as_uuid()).unwrap_or(0);
            if let Some(Value::Array(parts)) = ts.get("PartitionData") {
                for p in parts {
                    let Value::Struct(ps) = p else { continue };
                    let index = ps.get("Index").map(|v| v.as_i32()).unwrap_or(0);
                    let records = ps.get("Records").cloned().unwrap_or(Value::Null);
                    let batches = match records {
                        Value::Bytes(b) => b,
                        Value::Null => Bytes::new(),
                        _ => Bytes::new(),
                    };
                    targets.push(ProduceTarget { topic: name.clone(), topic_id, partition: index, batches: batches.clone() });
                    batch_bytes += batches.len() as u64;
                }
            }
        }
    }
    let _ = version;
    // produce 字节率配额（S5）：按请求批字节节流（token bucket）
    ConnState::throttled(conn, true, batch_bytes).await;
    let v = handlers::produce(version, acks, targets, ctx).await;
    Ok((v, acks == 0))
}

/// Fetch 请求解析产物：分区目标 + KIP-227 会话面（v7+）
pub(crate) struct ParsedFetch {
    pub targets: Vec<FetchTarget>,
    pub session_id: i32,
    pub epoch: i32,
    pub forgotten: Vec<crate::fetch_session::Forgotten>,
}

fn parse_fetch(req: &Struct, version: i16) -> Result<ParsedFetch, DispatchError> {
    let max_wait = req.get("MaxWaitMs").map(|v| v.as_i32()).unwrap_or(500);
    let min_bytes = req.get("MinBytes").map(|v| v.as_i32()).unwrap_or(1);
    // Fetch v4+ IsolationLevel（v4 之前无字段 = read_uncommitted）。
    // int8 字段必须 as_i8——as_i32 对 Value::I8 静默返 0（read_committed
    // 被静默降级为 read_uncommitted，Java 事务 e2e 实证）
    let isolation = match req.get("IsolationLevel").map(|v| v.as_i8()) {
        Some(1) if version >= 4 => crate::partition::Isolation::ReadCommitted,
        _ => crate::partition::Isolation::ReadUncommitted,
    };
    let top_max = req.get("MaxBytes").map(|v| v.as_i32()).unwrap_or(i32::MAX).max(0) as usize;
    let mut out = Vec::new();
    if let Some(Value::Array(topics)) = req.get("Topics") {
        for t in topics {
            let Value::Struct(ts) = t else { continue };
            let name = ts.get("Topic").map(|x| x.as_str().to_string()).unwrap_or_default();
            let topic_id = ts.get("TopicId").map(|x| x.as_uuid()).unwrap_or(0);
            if let Some(Value::Array(parts)) = ts.get("Partitions") {
                for p in parts {
                    let Value::Struct(ps) = p else { continue };
                    let index = ps.get("Partition").map(|v| v.as_i32()).unwrap_or(0);
                    let offset = ps.get("FetchOffset").map(|v| v.as_i64()).unwrap_or(0);
                    let pmax = ps.get("PartitionMaxBytes").map(|v| v.as_i32()).unwrap_or(1024).max(0) as usize;
                    out.push(FetchTarget {
                        topic: name.clone(),
                        topic_id,
                        partition: index,
                        offset,
                        max_bytes: pmax.min(top_max.max(1)),
                        max_wait_ms: max_wait,
                        min_bytes,
                        isolation,
                    });
                }
            }
        }
    }
    // KIP-227 会话面（v7+）
    let (session_id, epoch, forgotten) = if version >= 7 {
        let sid = req.get("SessionId").map(|v| v.as_i32()).unwrap_or(0);
        let ep = req.get("Epoch").map(|v| v.as_i32()).unwrap_or(0);
        let mut fg = Vec::new();
        if let Some(Value::Array(ft)) = req.get("ForgottenTopicsData") {
            for t in ft {
                let Value::Struct(ts) = t else { continue };
                let name = ts.get("Topic").map(|x| x.as_str().to_string()).filter(|s| !s.is_empty());
                let topic_id = ts.get("TopicId").map(|x| x.as_uuid()).filter(|u| *u != 0);
                let mut parts = Vec::new();
                if let Some(Value::Array(ps)) = ts.get("Partitions") {
                    for p in ps {
                        parts.push(p.as_i32());
                    }
                }
                fg.push(crate::fetch_session::Forgotten { topic: name, topic_id, partitions: parts });
            }
        }
        (sid, ep, fg)
    } else {
        (0, 0, Vec::new())
    };
    Ok(ParsedFetch { targets: out, session_id, epoch, forgotten })
}

/// fetch 分发：会话面结算 + 全量/增量服务（KIP-227，T-M4.2 尾项）
async fn handle_fetch(parsed: ParsedFetch, ctx: &Ctx) -> Result<(Value, bool), DispatchError> {
    let ParsedFetch { targets, session_id, epoch, forgotten } = parsed;
    // 会话记账（错误路径响应级 70/71 + 空Responses，客户端按 KIP-227 重建）
    let outcome = if epoch == 0 {
        let reg: Vec<(String, u128, i32, i64, usize)> = targets
            .iter()
            .map(|t| (t.topic.clone(), t.topic_id, t.partition, t.offset, t.max_bytes))
            .collect();
        let id = crate::fetch_session::begin(&reg);
        crate::fetch_session::SessionOutcome::Ok { session_id: id }
    } else if epoch == -1 {
        crate::fetch_session::close(session_id);
        crate::fetch_session::SessionOutcome::Ok { session_id: 0 }
    } else {
        let reg: Vec<(String, u128, i32, i64, usize)> = targets
            .iter()
            .map(|t| (t.topic.clone(), t.topic_id, t.partition, t.offset, t.max_bytes))
            .collect();
        crate::fetch_session::incremental(session_id, epoch, &reg, &forgotten)
    };
    let sess_id_for_resp = match &outcome {
        crate::fetch_session::SessionOutcome::Ok { session_id } => *session_id,
        _ => 0,
    };
    if let (crate::fetch_session::SessionOutcome::Unknown, true) =
        (&outcome, epoch >= 1)
    {
        let v = s([
            ("ThrottleTimeMs", Value::I32(0)),
            ("ErrorCode", Value::I16(70)), // UNKNOWN_FETCH_SESSION_ID
            ("SessionId", Value::I32(0)),
            ("Responses", Value::Array(vec![])),
            ("NodeEndpoints", Value::Array(vec![])),
        ]);
        return Ok((v, false));
    }
    if let crate::fetch_session::SessionOutcome::InvalidEpoch = outcome {
        let v = s([
            ("ThrottleTimeMs", Value::I32(0)),
            ("ErrorCode", Value::I16(71)), // INVALID_FETCH_SESSION_EPOCH
            ("SessionId", Value::I32(0)),
            ("Responses", Value::Array(vec![])),
            ("NodeEndpoints", Value::Array(vec![])),
        ]);
        return Ok((v, false));
    }
    // 全量/增量服务：增量时省略「空且无错」分区（长轮询空闲分区不占字节）
    let incremental = epoch >= 1;
    let v = handlers::fetch(targets, ctx, incremental, sess_id_for_resp).await;
    Ok((v, false))
}

/// 版本不支持时的兜底响应（ApiVersions 走 v0 特例语义）。
fn synth_unsupported(api_key: i16, version: i16) -> Option<Vec<u8>> {
    let reg = Registry::global();
    let entry = reg.api(api_key)?;
    let _flexible = frame::is_flexible(version, entry.flexible_from);
    let resp_version = if api_key == key::API_VERSIONS && version > entry.stable_max {
        0 // KIP-511：以 v0 回 UNSUPPORTED_VERSION
    } else {
        version.clamp(entry.valid.0, entry.stable_max)
    };
    let resp_flexible = frame::is_flexible(resp_version, entry.flexible_from);
    let mut st = Struct::new();
    st.set("ErrorCode", Value::I16(basalt_protocol::api::ErrorCode::UnsupportedVersion as i16));
    st.set("ThrottleTimeMs", Value::I32(0));
    let mut body = BytesMut::new();
    codec::encode_struct_fields(&entry.response.fields, resp_version, resp_flexible, &st, &mut body)
        .ok()?;
    let mut out = BytesMut::new();
    frame::write_response_header(&mut out, 0, frame::response_header_version(api_key, resp_flexible));
    out.extend_from_slice(&body);
    let total = out.len() as i32;
    let mut framed = BytesMut::with_capacity(out.len() + 4);
    framed.extend_from_slice(&total.to_be_bytes());
    framed.extend_from_slice(&out);
    Some(framed.to_vec())
}

/// ApiVersions 协商失败的固定应答：v0 格式 UNSUPPORTED_VERSION（KIP-511）。
fn synth_apiversions_unsupported(frame_bytes: &Bytes) -> Vec<u8> {
    let corr = i32::from_be_bytes([frame_bytes[4], frame_bytes[5], frame_bytes[6], frame_bytes[7]]);
    let mut out = BytesMut::new();
    frame::write_response_header(&mut out, corr, 0);
    out.extend_from_slice(&(basalt_protocol::api::ErrorCode::UnsupportedVersion as i16).to_be_bytes());
    out.extend_from_slice(&0i32.to_be_bytes()); // 空 ApiKeys
    let total = out.len() as i32;
    let mut framed = BytesMut::with_capacity(out.len() + 4);
    framed.extend_from_slice(&total.to_be_bytes());
    framed.extend_from_slice(&out);
    framed.to_vec()
}

#[cfg(test)]
mod conn_tests {
    use super::*;

    /// 认证门禁（T-S1）：未认证仅放行 ApiVersions/SaslHandshake/SaslAuthenticate；
    /// 认证后全放行；无鉴权模式恒全放行。
    #[test]
    fn gate_admits_only_sasl_apis_before_auth() {
        let cs = ConnState::with_params(true, 0, 0);
        assert!(cs.admits(key::API_VERSIONS));
        assert!(cs.admits(key::SASL_HANDSHAKE));
        assert!(cs.admits(key::SASL_AUTHENTICATE));
        assert!(!cs.admits(key::PRODUCE));
        assert!(!cs.admits(key::METADATA));
        assert!(!cs.admits(key::FETCH));

        let mut cs = ConnState::with_params(true, 0, 0);
        cs.user = Some("admin".into());
        assert!(cs.admits(key::PRODUCE), "认证后全放行");

        let cs = ConnState::with_params(false, 0, 0);
        assert!(cs.admits(key::PRODUCE), "无鉴权模式恒放行");
        assert_eq!(cs.user.as_deref(), Some("*"));
    }

    /// per-user 配额解析（T-M4.2）：键值面、部分字段、坏项容忍
    #[test]
    fn parse_user_quotas_matrix() {
        let m = parse_user_quotas("app:p=500000;f=1000000,admin:f=2000000,bad-entry,,ghost:p=abc");
        assert_eq!(m.get("app"), Some(&(500000, 1000000)));
        assert_eq!(m.get("admin"), Some(&(0, 2000000)), "缺省维度=0（不限）");
        assert_eq!(m.get("ghost"), Some(&(0, 0)), "坏值容忍为不限");
        assert_eq!(m.len(), 3, "空项剔除、坏值项保留");
    }

    /// 配额 token bucket（S5）：额度内零延迟、耗尽后按缺口比例节流、
    /// 时间流逝回补、突发上限 2×rate。
    #[test]
    fn quota_token_bucket_throttles_and_recovers() {
        let mut cs = ConnState::with_params(false, 1000, 0); // produce 1KB/s
        assert_eq!(cs.take_quota(false, 999), 0, "fetch 未配额恒 0");
        assert_eq!(cs.take_quota(true, 400), 0); // 初始 1s 突发额度
        assert_eq!(cs.take_quota(true, 600), 0); // 恰好耗尽（负债制不截断为 0）
        // 缺口节流：500B @1KB/s → ~500ms
        let d = cs.take_quota(true, 500);
        assert!(d >= 450 && d <= 550, "缺口节流 ~500ms，得 {d}");
        // 回补：sleep 320ms → 欠账 -500+320 = -180；再扣 500 → -680 → ~680ms
        std::thread::sleep(std::time::Duration::from_millis(320));
        let d2 = cs.take_quota(true, 500);
        assert!(d2 >= 620 && d2 <= 740, "欠账累积（-180-500 → ~680ms），得 {d2}");
        // 长回补：sleep 1s → 欠账 -680+1000 = +320；扣 500 → -180 → ~180ms
        std::thread::sleep(std::time::Duration::from_millis(1000));
        let d3 = cs.take_quota(true, 500);
        assert!(d3 >= 120 && d3 <= 280, "部分回补后节流 ~180ms，得 {d3}");
        // 突发上限 2×rate 由 take_quota 内 min() 结构性保证（真实时钟驱动，
        // 时序断言在此粒度下易碎，不单测）
    }
}
