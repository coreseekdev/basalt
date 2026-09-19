//! Basalt broker 进程壳（多节点 POC）：
//! - 控制器角色（最小 node id）：ClusterState + record 日志 + failover watch；
//! - 内部 RPC（client port + 1）：Register/Heartbeat/MetaSync/CreateTopic/FetchSlice；
//! - 元数据同步循环：拉快照 → ApplyCluster（spawn actor / SetRole / 路由）；
//! - follower 拉取：非 leader 副本持续从 leader 拉切片（Absolute 追加），
//!   拉取即 LEO 上报 → leader HW 推进 → acks=all 停等放行。

mod acl;
mod config;
mod conn;
mod fetch_session;
mod handlers;
mod sasl;
mod handlers_consumer;
mod handlers_groups;
mod ctrl_raft;
mod internal;
mod meta;
mod share_group;
mod partition;
mod group_state_store;
mod handlers_share;
mod handlers_txn;
mod tls;
mod txn;

use basalt_storage::pool::BufferPool;
use config::Config;
use std::sync::OnceLock;

static CTX: OnceLock<handlers::Ctx> = OnceLock::new();

fn main() {
    let cfg = config::Config::from_env();
    let filter = tracing_subscriber::EnvFilter::try_new(&cfg.log_level)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(async_main(cfg));
}

fn cluster_peers(cfg: &Config) -> Vec<(i32, String, u16)> {
    cfg.nodes.clone()
}

fn controller_peer(cfg: &Config) -> Option<(i32, String, u16)> {
    cluster_peers(cfg).into_iter().min_by_key(|(id, _, _)| *id)
}

/// 每连接 Ctx 快照（明文/TLS listener 共用）
fn ctx_for(c: &handlers::Ctx, pool: std::sync::Arc<BufferPool>) -> handlers::Ctx {
    handlers::Ctx {
        principal: "ANONYMOUS".into(),
        node_id: c.node_id,
        controller_id: c.controller_id,
        host: c.host.clone(),
        port: c.port,
        all_brokers: Vec::new(),
        meta_tx: c.meta_tx.clone(),
        group_tx: c.group_tx.clone(),
        cg_tx: c.cg_tx.clone(),
        routes_rx: c.routes_rx.clone(),
        txn_tx: c.txn_tx.clone(),
        brokers_cache: std::sync::Mutex::new(c.brokers_cache.lock().unwrap().clone()),
        pool,
        cluster_id: c.cluster_id.clone(),
        segment_max_bytes: c.segment_max_bytes,
        num_partitions: c.num_partitions,
        min_isr: c.min_isr,
        txn_timeout_ms: c.txn_timeout_ms,
    }
}

/// TLS accept loop（'static：Ctx 经 CTX 全局取，pool 所有权移交）
async fn tls_accept_loop(
    l: tokio::net::TcpListener,
    tls_cfg: std::sync::Arc<rustls::ServerConfig>,
    pool: std::sync::Arc<BufferPool>,
) {
    let acceptor = tokio_rustls::TlsAcceptor::from(tls_cfg);
    let ctx_ref = CTX.get().expect("ctx");
    // 通告地址跟随客户端所连 listener（Kafka 语义：metadata 按请求到达口
    // 通告对应 listener 地址）——否则 SSL 客户端 bootstrap 后被重定向到
    // 明文口，TLS 面形同虚设
    let adv_port = l.local_addr().map(|a| a.port()).unwrap_or(0);
    loop {
        match l.accept().await {
            Ok((sock, peer)) => {
                let mut ctx = ctx_for(ctx_ref, pool.clone());
                if adv_port != 0 {
                    ctx.port = adv_port;
                }
                let pool = pool.clone();
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    match acceptor.accept(sock).await {
                        Ok(tls_sock) => conn::serve_connection(tls_sock, peer, ctx, pool).await,
                        Err(e) => tracing::warn!(peer = %peer, error = %e, "tls handshake failed"),
                    }
                });
            }
            Err(e) => tracing::warn!(error = %e, "tls accept failed"),
        }
    }
}
/// （重启一致——客户端按 ClusterId 缓存/校验，逐次漂移会导致重连风暴）
fn ensure_cluster_id(data_dir: &str) -> String {
    if let Ok(id) = std::env::var("BASALT_CLUSTER_ID") {
        if !id.trim().is_empty() {
            return id.trim().to_string();
        }
    }
    let path = std::path::Path::new(data_dir).join("cluster_id");
    if let Ok(s) = std::fs::read_to_string(&path) {
        let t = s.trim();
        if !t.is_empty() {
            return t.to_string();
        }
    }
    // 熵源：RandomState 每实例随机键 × 纳秒时钟（无 rand 依赖）
    use std::hash::{BuildHasher as _, Hasher as _};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0),
    );
    let id = format!("basalt-{:016x}", h.finish());
    let _ = std::fs::write(&path, &id);
    id
}

async fn async_main(cfg: Config) {
    std::fs::create_dir_all(&cfg.data_dir).expect("data dir");
    let _peers = cluster_peers(&cfg);
    let ctrl = controller_peer(&cfg);
    let is_controller = ctrl.as_ref().map(|(id, _, _)| *id == cfg.node_id).unwrap_or(true);
    let controller_id = ctrl.as_ref().map(|(id, _, _)| *id).unwrap_or(cfg.node_id);
    tracing::info!(
        node_id = cfg.node_id, port = cfg.port, data = %cfg.data_dir,
        controller = ?ctrl.as_ref().map(|(id, _, _)| *id), is_controller, "basalt starting"
    );
    tracing::info!(
        fsync = ?crate::meta::fsync_schedule_from(std::env::var("BASALT_FSYNC").ok().as_deref()),
        "fsync schedule resolved"
    );
    if crate::sasl::auth_enabled() {
        // 空用户表 = 所有认证必败：scram 模式下的配置疏漏信号
        if std::env::var("BASALT_SASL_USERS").map(|v| v.trim().is_empty()).unwrap_or(true) {
            tracing::warn!("BASALT_AUTH=scram but BASALT_SASL_USERS is empty; all authentications will fail");
        } else {
            tracing::info!("SASL/SCRAM-SHA-256 authentication enabled");
        }
    }
    let cluster_id = ensure_cluster_id(&cfg.data_dir);
    tracing::info!(cluster_id = %cluster_id, "cluster id resolved");
    // ACL 骨架（T-M4.1 尾）：装载持久化 ACL + 数据目录注册（变更即落盘）
    acl::set_data_dir(&cfg.data_dir);
    acl::load_from_dir(std::path::Path::new(&cfg.data_dir));
    // share groups（T-M3.6 块 c）：装载持久化交付状态（cursor/archived/counts）
    share_group::set_data_dir(&cfg.data_dir);
    share_group::load_from_dir(&cfg.data_dir);

    // 内部端口：client port + 1
    let internal_port = cfg.port + 1;

    // 控制器 actor：传统模式仅控制器节点；引擎模式全节点各启
    // （职权由 raft leader 门控 has_engine_authority——控制器 kill 后
    //  新 raft leader 的 controller 自动接管）
    let controller_tx = if is_controller || crate::ctrl_raft::raftrs_engine::engine_enabled() {
        // ADR-15/16：引擎运行时（BASALT_CTRL_RAFT_ENGINE=raftrs）下，
        // 先装配本节点 raft 引擎（voters = 全部 nodes），Controller 经
        // engine propose 复制元数据变更；仅 raft leader 行使职权。
        let engine_handle = if crate::ctrl_raft::raftrs_engine::engine_enabled() {
            let peers: Vec<i32> = cfg.nodes.iter().map(|(id, _, _)| *id).collect();
            let router = crate::ctrl_raft::raftrs_engine::RaftRsRouter::new();
            // TCP peer 地址表（跨进程 raft 消息路由）
            for (id, host, port) in &cfg.nodes {
                let raft_id = id + 1; // broker→raft id 偏移（INVALID_ID=0）
                router.tcp_peers.lock().unwrap().insert(
                    raft_id,
                    format!("{host}:{}", port + 1),
                );
            }
            Some(crate::ctrl_raft::raftrs_engine::spawn_with_dir(
                cfg.node_id,
                peers,
                router,
                std::path::Path::new(&cfg.data_dir).join("ctrl-raftrs"),
            ))
        } else {
            None
        };
        Some(internal::Controller::spawn(
            cfg.node_id,
            std::path::Path::new(&cfg.data_dir).join("__controller.log"),
            std::time::Duration::from_millis(
                std::env::var("BASALT_HEARTBEAT_TIMEOUT_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(1000),
            ),
            engine_handle,
            // 全体节点内部端口地址（引擎模式 propose 转发用）
            cfg.nodes.iter()
                .map(|(id, h, p)| (*id, format!("{h}:{}", p + 1)))
                .collect(),
        ))
    } else {
        None
    };

    let controller_addr = ctrl.as_ref().map(|(_, h, p)| format!("{h}:{}", p + 1));

    // BufferPool 进程单例共享资源池：Arc 表达资源共享而非共享可变所有权，
    // 与 Bytes/mpsc 内部引用计数同级豁免（ADR-13）。
    #[allow(clippy::disallowed_types)]
    let pool: std::sync::Arc<BufferPool> = std::sync::Arc::new(BufferPool::new());
    // 单节点（BASALT_NODES 未配置）：无 controller_peer/自注册路径——
    // cluster.brokers 恒空 → metadata 响应 Brokers=[] → kafka-clients 严格
    // 要求分区 leader 可映射到 Brokers 列表，视整个 metadata 为不完整
    // （"Topic not present"）。补自注册使 broker 列表恒含自身
    if cfg.nodes.is_empty() {
        if let Some(tx) = &controller_tx {
            let (rtx, rrx) = tokio::sync::oneshot::channel();
            let _ = tx
                .send(internal::ControllerCmd::Register {
                    info: basalt_metadata::cluster::BrokerInfo {
                        node_id: cfg.node_id,
                        host: cfg.host.clone(),
                        port: cfg.port,
                    },
                    reply: rtx,
                })
                .await;
            let _ = rrx.await;
        }
    }

    let (meta_tx, routes_rx) = meta::MetaService::spawn(cfg.clone(), controller_addr, controller_tx.clone(), pool.clone());
    let group_tx = basalt_coordinator::GroupManager::spawn(std::path::Path::new(&cfg.data_dir));

    // 内部 topic 自动创建（方案 B 块 b1）：组状态持久化面
    {
        let itx = meta_tx.clone();
        tokio::spawn(async move {
            for _ in 0..20 {
                if CTX.get().is_some() { break; }
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            let (txr, rxr) = tokio::sync::oneshot::channel();
            let _ = itx.send(meta::MetaCmd::EnsureTopic {
                name: "__basalt_group_state".to_string(),
                partitions: 1,
                rf: 1,
                tiered: false,
                reply: txr,
            }).await;
            let _ = rxr.await;
            tracing::info!("internal group state topic ensured");
        });
    }

    // KIP-848 consumer 组 actor（每节点一个；FindCoordinator 组路径回自身——
    // 与 classic 同拓扑，ADR-19 §4）
    let cg_tx = basalt_coordinator::ConsumerGroups::spawn();
    let routes_rx_internal = routes_rx.clone();

    // 事务协调器（ADR-18 §9：单实例驻 controller 节点）+ marker 路由器。
    // 非 controller 节点 txn_tx = None——事务 API 回 NotCoordinator(16)，
    // 客户端经 FindCoordinator(Type=Transaction) 重路由（协议自愈）。
    let txn_tx = if is_controller {
        let (marker_tx, marker_rx) = tokio::sync::mpsc::channel::<txn::MarkerJob>(256);
        let router_routes = routes_rx.clone();
        let router_meta = meta_tx.clone();
        let router_node = cfg.node_id;
        tokio::spawn(handlers_txn::marker_router(marker_rx, router_routes, router_meta, router_node));
        let log_path = std::path::Path::new(&cfg.data_dir).join("txn").join("txn.log");
        if let Some(parent) = log_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        // 提升桥接（ADR-18 §7）：EndTxn(commit) 的 pending 生效点 = 组
        // 协调器 OffsetLog（PromoteTxnOffsets 免成员校验——fence 已在
        // 协调器 TxnLog 的 epoch 校验完成）
        let (promote_tx, mut promote_rx) = tokio::sync::mpsc::channel::<txn::PromotedOffsets>(64);
        let bridge_group = group_tx.clone();
        tokio::spawn(async move {
            while let Some(p) = promote_rx.recv().await {
                // 按 PendingOffset 自带的消费组分组（提升目标是消费组，非事务 ID）
                let mut by_group: std::collections::HashMap<String, Vec<basalt_coordinator::CommittedOffset>> =
                    std::collections::HashMap::new();
                for o in p.offsets {
                    by_group.entry(o.group.clone()).or_default().push(basalt_coordinator::CommittedOffset {
                        topic: o.topic,
                        partition: o.partition,
                        offset: o.offset,
                        metadata: o.metadata,
                        commit_ts: crate::partition::now_ms(),
                    });
                }
                for (group, offsets) in by_group {
                    let (rtx, rrx) = tokio::sync::oneshot::channel();
                    let _ = bridge_group
                        .send(basalt_coordinator::GroupCmd::PromoteTxnOffsets {
                            group,
                            offsets,
                            reply: rtx,
                        })
                        .await;
                    let _ = rrx.await;
                }
            }
        });
        let coord = txn::TxnCoordinator::spawn(
            &log_path,
            cfg.node_id,
            marker_tx,
            Some(promote_tx),
            std::time::Duration::from_millis(cfg.txn_timeout_ms),
        );
        Some(coord)
    } else {
        None
    };
    let txn_tx_internal = txn_tx.clone();

    // 内部服务
    // 内部口绑定（P0-1）：默认 localhost（单节点/本机进程间），多节点经
    // BASALT_INTERNAL_BIND 显式放开；生产部署配防火墙隔离此端口
    let internal_bind = std::env::var("BASALT_INTERNAL_BIND")
        .unwrap_or_else(|_| "127.0.0.1".to_string());
    if internal_bind != "127.0.0.1" {
        tracing::warn!(bind = %internal_bind, port = internal_port,
            "internal RPC on non-localhost: ensure network isolation (firewall/VPC)");
    }
    let internal_listener = tokio::net::TcpListener::bind(format!("{internal_bind}:{internal_port}"))
        .await
        .expect("bind internal");
    let controller_tx_internal = controller_tx.clone();
    let pool_internal = pool.clone();
    {
        let cfg = cfg.clone();
        tokio::spawn(async move {
            // 控制器节点：让内部服务能访问控制器 actor
            // （内部连接处理器经由 ctx.controller_tx 转发）
            let is_controller = std::env::var("BASALT_NODE_ID")
                .ok()
                .and_then(|v| v.parse::<i32>().ok())
                .map(|id| {
                    controller_peer(&Config::from_env())
                        .map(|(cid, _, _)| cid == id)
                        .unwrap_or(true)
                })
                .unwrap_or(true);
            // 内部服务与客户端路径共享同一 BufferPool（性能 #2 闭环前提；
            // 四轮 review P0-1：7d5f11d 曾在此丢失 serve 接线，多节点内部
            // RPC 全部静默挂死）
            let ctx = internal::InternalCtx {
                node_id: cfg.node_id,
                pool: pool_internal,
                is_controller,
                controller_tx: controller_tx_internal,
                routes_rx: routes_rx_internal,
                txn_tx: txn_tx_internal,
            };
            internal::serve(internal_listener, ctx).await;
        });
    }

    // Metrics HTTP 端点（Prometheus 格式）；BASALT_METRICS_PORT=0 = 关闭
    // （此前 0 语义为绑随机端口——e2e 全部传 0 期望关闭，实际每次起一个
    // 无人访问的 listener，可观测速赢）
    let metrics_port: u16 = std::env::var("BASALT_METRICS_PORT").ok().and_then(|v| v.parse().ok()).unwrap_or(9094);
    if metrics_port == 0 {
        tracing::info!("metrics endpoint disabled (BASALT_METRICS_PORT=0)");
    }
    let metrics_listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{metrics_port}")).await.expect("bind metrics");
    tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        loop {
            let Ok((mut sock, _)) = metrics_listener.accept().await else { break };
            tokio::spawn(async move {
                use tokio::io::AsyncReadExt;
                let mut buf = [0u8; 1024];
                let path = match sock.peek(&mut buf).await {
                    _ if false => String::new(),
                    Ok(n) => String::from_utf8_lossy(&buf[..n]).to_string(),
                    Err(_) => String::new(),
                };
                // 健康面：GET /health = 进程活着（liveness）；GET /ready =
                // 路由非空（readiness——控制器快照已应用，可服务客户端）
                if path.starts_with("GET /health") {
                    let resp = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok";
                    let _ = sock.write_all(resp.as_bytes()).await;
                    let _ = sock.shutdown().await;
                    return;
                }
                if path.starts_with("GET /ready") {
                    let ready = CTX.get().map(|c| !c.routes().is_empty()).unwrap_or(false);
                    let (code, body) = if ready { ("200", "ready") } else { ("503", "empty-routing") };
                    let resp = format!("HTTP/1.1 {code} OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                    let _ = sock.write_all(resp.as_bytes()).await;
                    let _ = sock.shutdown().await;
                    return;
                }
                let m = crate::partition::metrics();
                let produced = m.messages_produced.load(std::sync::atomic::Ordering::Relaxed);
                let bytes_p = m.bytes_produced.load(std::sync::atomic::Ordering::Relaxed);
                let consumed = m.messages_consumed.load(std::sync::atomic::Ordering::Relaxed);
                let errors = m.produce_errors.load(std::sync::atomic::Ordering::Relaxed);
                let fetches = m.fetch_requests.load(std::sync::atomic::Ordering::Relaxed);
                let produces = m.produce_requests.load(std::sync::atomic::Ordering::Relaxed);
                let compressed = m.compressed_batches.load(std::sync::atomic::Ordering::Relaxed);
                let connections = m.connections_total.load(std::sync::atomic::Ordering::Relaxed);
                let authz = m.authz_rejections_total.load(std::sync::atomic::Ordering::Relaxed);
                let gauge_lines = if let Ok(map) = crate::partition::PARTITION_GAUGES.lock() {
                    let mut lines = String::from("# TYPE basalt_partition_hw gauge\n# TYPE basalt_partition_lso gauge\n# TYPE basalt_partition_log_start gauge\n");
                    for (key, (hw, lso, start)) in map.iter() {
                        lines.push_str(&format!("basalt_partition_hw{{partition=\"{}\"}} {}\n", key, hw));
                        lines.push_str(&format!("basalt_partition_lso{{partition=\"{}\"}} {}\n", key, lso));
                        lines.push_str(&format!("basalt_partition_log_start{{partition=\"{}\"}} {}\n", key, start));
                    }
                    lines
                } else {
                    String::new()
                };
                let body = format!(
                    "{}# TYPE basalt_messages_produced_total counter\nbasalt_messages_produced_total {produced}\n# TYPE basalt_bytes_produced_total counter\nbasalt_bytes_produced_total {bytes_p}\n# TYPE basalt_messages_consumed_total counter\nbasalt_messages_consumed_total {consumed}\n# TYPE basalt_produce_errors_total counter\nbasalt_produce_errors_total {errors}\n# TYPE basalt_fetch_requests_total counter\nbasalt_fetch_requests_total {fetches}\n# TYPE basalt_produce_requests_total counter\nbasalt_produce_requests_total {produces}\n# TYPE basalt_compressed_batches_total counter\nbasalt_compressed_batches_total {compressed}\n# TYPE basalt_connections_total counter\nbasalt_connections_total {connections}\n# TYPE basalt_authz_rejections_total counter\nbasalt_authz_rejections_total {authz}\n",
                    gauge_lines
                );
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.shutdown().await;
            });
        }
    });

    // 客户端监听
    let listener = tokio::net::TcpListener::bind(cfg.listen_addr()).await.expect("bind");
    tracing::info!(addr = %cfg.listen_addr(), "basalt listening");

    // 元数据同步循环 + 心跳 + follower 拉取编排
    let sync_cfg = cfg.clone();
    let meta_tx_sync = meta_tx.clone();
    let controller_tx_sync = controller_tx.clone();
    tokio::spawn(async move {
        let engine_mode = crate::ctrl_raft::raftrs_engine::engine_enabled();
        let ctrl = controller_peer(&sync_cfg);
        let Some((ctrl_id, ctrl_host, ctrl_port)) = ctrl else { return };
        let ctrl_addr = format!("{ctrl_host}:{}", ctrl_port + 1);
        let client = internal::InternalClient::new(ctrl_addr.clone());

        if engine_mode {
            // 引擎模式：直接轮询本地 Controller（raft 复制后的状态），
            // 无需网络跳——节点间一致性由 raft 保证
            let local_tx = controller_tx_sync.clone().unwrap();
            // 自注册：经 raft 复制（非 leader 节点由 controller 转发给 raft leader）。
            // fire-and-forget：选举窗口内的转发重试不应阻塞心跳/同步循环的启动
            // （state.brokers 经 MetaSync 轮询最终一致收敛）
            let reg_tx = local_tx.clone();
            let reg_cfg = sync_cfg.clone();
            tokio::spawn(async move {
                let (txr, rxr) = tokio::sync::oneshot::channel();
                let _ = reg_tx
                    .send(internal::ControllerCmd::Register {
                        info: basalt_metadata::cluster::BrokerInfo {
                            node_id: reg_cfg.node_id,
                            host: reg_cfg.host.clone(),
                            port: reg_cfg.port,
                        },
                        reply: txr,
                    })
                    .await;
                let _ = rxr.await;
            });
            // 心跳广播到全部 peers（每节点 controller 本地记账，
            // 仅 raft leader 行使 failover 职权）
            let hb_ms: u64 = std::env::var("BASALT_HEARTBEAT_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(300);
            let hb_node = sync_cfg.node_id;
            let hb_peers: Vec<(i32, String)> = sync_cfg.nodes.iter()
                .filter(|(id, _, _)| *id != hb_node)
                .map(|(id, h, p)| (*id, format!("{h}:{}", p + 1)))
                .collect();
            let hb_clients: Vec<(i32, internal::InternalClient)> = hb_peers.iter()
                .map(|(id, addr)| (*id, internal::InternalClient::new(addr.clone())))
                .collect();
            tokio::spawn(async move {
                tracing::debug!(node = hb_node, peers = ?hb_peers, "heartbeat loop started");
                loop {
                    for (pid, pc) in &hb_clients {
                        if !internal::blocked_peers().contains(pid) {
                            if let Err(e) = pc.heartbeat(hb_node).await {
                                tracing::warn!(node = hb_node, to = pid, error = %e, "heartbeat failed");
                            }
                        }
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(hb_ms)).await;
                }
            });
            let mut last_version = 0u64;
            loop {
                let (txr, rxr) = tokio::sync::oneshot::channel();
                if local_tx.send(internal::ControllerCmd::Sync { version: last_version, reply: txr }).await.is_err() {
                    break;
                }
                if let Ok(Some(data)) = rxr.await {
                    // Controller::Sync 回复 = state.encode() 纯编码字节（无 marker）
                    if let Some(state) = basalt_metadata::cluster::ClusterState::decode(&data) {
                        if state.version > last_version || state.assignments.len() > 0 {
                            if state.version != last_version {
                                tracing::debug!(node = sync_cfg.node_id, version = state.version, "cluster snapshot applied");
                            }
                            last_version = state.version;
                            let _ = meta_tx_sync.send(meta::MetaCmd::ApplyCluster(Box::new(state))).await;
                        }
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        } else if sync_cfg.node_id != ctrl_id {
            // 注册（非控制器节点 → 静态控制器）
            let _ = client.register(sync_cfg.node_id, &sync_cfg.host, sync_cfg.port).await;
            // 心跳：传统模式发静态控制器；引擎模式向全部 peers 广播
            // （每节点 controller 本地记账，仅 raft leader 行使 failover 职权）
            let hb_ms: u64 = std::env::var("BASALT_HEARTBEAT_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(300);
            let engine_mode = crate::ctrl_raft::raftrs_engine::engine_enabled();
            let hb_node = sync_cfg.node_id;
            // 引擎模式：心跳广播到全部 peers（每节点 controller 本地记账，
            // 仅 raft leader 行使 failover 职权）
            let hb_peers: Vec<(i32, String)> = if engine_mode {
                sync_cfg.nodes.iter()
                    .filter(|(id, _, _)| *id != hb_node)
                    .map(|(id, h, p)| (*id, format!("{h}:{}", p + 1)))
                    .collect()
            } else {
                vec![(ctrl_id, ctrl_addr.clone())]
            };
            let hb_clients: Vec<(i32, internal::InternalClient)> = hb_peers.iter()
                .map(|(id, addr)| (*id, internal::InternalClient::new(addr.clone())))
                .collect();
            tokio::spawn(async move {
                loop {
                    for (pid, pc) in &hb_clients {
                        // 分区注入：对端在断边集内则不发送
                        if !internal::blocked_peers().contains(pid) {
                            let _ = pc.heartbeat(hb_node).await;
                        }
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(hb_ms)).await;
                }
            });
        } else if let Some(tx) = &controller_tx_sync {
            // 控制器节点自登记
            let (txr, rxr) = tokio::sync::oneshot::channel();
            let _ = tx
                .send(internal::ControllerCmd::Register {
                    info: basalt_metadata::cluster::BrokerInfo {
                        node_id: sync_cfg.node_id,
                        host: sync_cfg.host.clone(),
                        port: sync_cfg.port,
                    },
                    reply: txr,
                })
                .await;
            let _ = rxr.await;
        }

        // 元数据轮询
        let mut last_version = 0u64;
        let meta_tx = meta_tx_sync;
        loop {
            // 分区注入：控制器在断边集内则跳过元数据轮询
            if internal::blocked_peers().contains(&ctrl_id) {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                continue;
            }
            if let Ok(resp) = client.meta_sync(last_version).await {
                if resp.len() >= 1 && resp[0] == 1 {
                    if let Some(state) = basalt_metadata::cluster::ClusterState::decode(&resp[1..]) {
                        last_version = state.version;
                        let _ = meta_tx.send(meta::MetaCmd::ApplyCluster(Box::new(state))).await;
                    }
                }
            }
            // L1：元数据收敛 ≤200ms（failover 检出 1.3s + 0.2s ≈ 1.5s < 2s）
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    });

    let ctx = handlers::Ctx {
        node_id: cfg.node_id,
        controller_id,
        host: cfg.host.clone(),
        port: cfg.port,
        all_brokers: Vec::new(),
        meta_tx,
        group_tx: group_tx.clone(),
        cg_tx: cg_tx.clone(),
        routes_rx: routes_rx.clone(),
        brokers_cache: std::sync::Mutex::new(None),
        pool: pool.clone(),
        txn_tx: txn_tx.clone(),
        principal: "ANONYMOUS".into(),
        cluster_id: ensure_cluster_id(&cfg.data_dir),
        segment_max_bytes: cfg.segment_max_bytes,
        num_partitions: cfg.num_partitions,
        min_isr: cfg.min_isr,
        txn_timeout_ms: cfg.txn_timeout_ms,
    };
    CTX.set(ctx).ok();

    let ctx_ref = CTX.get().expect("ctx");
    let mut accept_loop = Box::pin(async {
        loop {
            match listener.accept().await {
                Ok((sock, peer)) => {
                    let ctx = ctx_for(ctx_ref, pool.clone());
                    tokio::spawn(conn::serve_connection(sock, peer, ctx, pool.clone()));
                }
                Err(e) => tracing::warn!(error = %e, "accept failed"),
            }
        }
    });

    // TLS listener（T-S1 加密面）：配置 BASALT_TLS_CERT/KEY 后于独立端口
    // （默认 client+2）提供加密面——主口保持明文（Kafka 多 listener 语义）。
    // TLS 握手在 serve_connection 之外完成，失败仅断该连接。
    let tls_loop = match tls::listener_port(cfg.port) {
        Some(tls_port) => match tls::server_config() {
            None => {
                tracing::warn!("TLS port configured but cert/key load failed; TLS disabled");
                None
            }
            Some(tls_cfg) => match tokio::net::TcpListener::bind((cfg.host.as_str(), tls_port)).await {
                Ok(l) => {
                    tracing::info!(port = tls_port, "TLS listener bound");
                    Some(Box::pin(tls_accept_loop(l, tls_cfg, pool.clone())))
                }
                Err(e) => {
                    tracing::error!(error = %e, port = tls_port, "TLS listener bind failed");
                    None
                }
            },
        },
        None => None,
    };

    // 优雅停机：SIGTERM/SIGINT → 停 accept → 短暂 drain → sync
    tokio::select! {
        _ = &mut accept_loop => {},
        // TLS 未配置时该臂必须永久挂起——空臂瞬间就绪会让服务启动即停机
        _ = async {
            match tls_loop {
                Some(mut tls) => tls.await,
                None => std::future::pending::<()>().await,
            }
        } => {},
        _ = async {
            let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = term.recv() => {},
            }
        } => {
            tracing::info!("SIGTERM/SIGINT: shutting down gracefully");
        }
    }
    // 给 in-flight 请求 2s drain 窗口
    tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
    // 数据落盘由 FsyncSchedule 决定（默认 always：每批 fsync；进程退出无需显式 sync）
    tracing::info!("basalt shutdown complete");
}
