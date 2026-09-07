//! Basalt broker 进程壳（多节点 POC）：
//! - 控制器角色（最小 node id）：ClusterState + record 日志 + failover watch；
//! - 内部 RPC（client port + 1）：Register/Heartbeat/MetaSync/CreateTopic/FetchSlice；
//! - 元数据同步循环：拉快照 → ApplyCluster（spawn actor / SetRole / 路由）；
//! - follower 拉取：非 leader 副本持续从 leader 拉切片（Absolute 追加），
//!   拉取即 LEO 上报 → leader HW 推进 → acks=all 停等放行。

mod config;
mod conn;
mod handlers;
mod handlers_groups;
mod internal;
mod meta;
mod partition;

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

async fn async_main(cfg: Config) {
    std::fs::create_dir_all(&cfg.data_dir).expect("data dir");
    let _peers = cluster_peers(&cfg);
    let ctrl = controller_peer(&cfg);
    let is_controller = ctrl.as_ref().map(|(id, _, _)| *id == cfg.node_id).unwrap_or(true);
    tracing::info!(
        node_id = cfg.node_id, port = cfg.port, data = %cfg.data_dir,
        controller = ?ctrl.as_ref().map(|(id, _, _)| *id), is_controller, "basalt starting"
    );

    // 内部端口：client port + 1
    let internal_port = cfg.port + 1;

    // 控制器 actor（仅控制器节点）
    let controller_tx = if is_controller {
        Some(internal::Controller::spawn(
            cfg.node_id,
            std::path::Path::new(&cfg.data_dir).join("__controller.log"),
            std::time::Duration::from_millis(
                std::env::var("BASALT_HEARTBEAT_TIMEOUT_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(4000),
            ),
        ))
    } else {
        None
    };

    let controller_addr = ctrl.as_ref().map(|(_, h, p)| format!("{h}:{}", p + 1));

    let (meta_tx, routes_rx) = meta::MetaService::spawn(cfg.clone(), controller_addr, controller_tx.clone());
    let group_tx = basalt_coordinator::GroupManager::spawn(std::path::Path::new(&cfg.data_dir));
    let routes_rx_internal = routes_rx.clone();

    // 内部服务
    let internal_listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{internal_port}"))
        .await
        .expect("bind internal");
    let controller_tx_internal = controller_tx.clone();
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
            let _ = is_controller;
            // controller_tx 在下方通过闭包传递有所有权问题 —— POC 简化：
            // 内部服务的 controller 命令经 meta 通道回环由 sync 任务代为转发
            let ctx = internal::InternalCtx {
                node_id: cfg.node_id,
                is_controller,
                controller_tx: controller_tx_internal,
                routes_rx: routes_rx_internal,
            };
            internal::serve(internal_listener, ctx).await;
        });
    }

    // 客户端监听
    let listener = tokio::net::TcpListener::bind(cfg.listen_addr()).await.expect("bind");
    tracing::info!(addr = %cfg.listen_addr(), "basalt listening");

    // 元数据同步循环 + 心跳 + follower 拉取编排
    let sync_cfg = cfg.clone();
    let meta_tx_sync = meta_tx.clone();
    let controller_tx_sync = controller_tx.clone();
    tokio::spawn(async move {
        let ctrl = controller_peer(&sync_cfg);
        let Some((ctrl_id, ctrl_host, ctrl_port)) = ctrl else { return };
        let ctrl_addr = format!("{ctrl_host}:{}", ctrl_port + 1);
        let client = internal::InternalClient::new(ctrl_addr.clone());

        if sync_cfg.node_id != ctrl_id {
            // 注册 + 心跳（非控制器节点）
            let _ = client.register(sync_cfg.node_id, &sync_cfg.host, sync_cfg.port).await;
            let hb_client = internal::InternalClient::new(ctrl_addr.clone());
            let hb_node = sync_cfg.node_id;
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
                    let _ = hb_client.heartbeat(hb_node).await;
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
            if let Ok(resp) = client.meta_sync(last_version).await {
                if resp.len() >= 1 && resp[0] == 1 {
                    if let Some(state) = basalt_metadata::cluster::ClusterState::decode(&resp[1..]) {
                        last_version = state.version;
                        let _ = meta_tx.send(meta::MetaCmd::ApplyCluster(Box::new(state))).await;
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        }
    });

    let ctx = handlers::Ctx {
        node_id: cfg.node_id,
        host: cfg.host.clone(),
        port: cfg.port,
        all_brokers: Vec::new(),
        meta_tx,
        group_tx: group_tx.clone(),
        routes_rx: routes_rx.clone(),
        brokers_cache: std::sync::Mutex::new(None),
    };
    CTX.set(ctx).ok();

    let ctx_ref = CTX.get().expect("ctx");
    loop {
        match listener.accept().await {
            Ok((sock, peer)) => {
                let ctx = handlers::Ctx {
                    node_id: ctx_ref.node_id,
                    host: ctx_ref.host.clone(),
                    port: ctx_ref.port,
                    all_brokers: Vec::new(),
                    meta_tx: ctx_ref.meta_tx.clone(),
                    group_tx: ctx_ref.group_tx.clone(),
                    routes_rx: ctx_ref.routes_rx.clone(),
                    brokers_cache: std::sync::Mutex::new(ctx_ref.brokers_cache.lock().unwrap().clone()),
                };
                tokio::spawn(conn::serve_connection(sock, peer, ctx));
            }
            Err(e) => tracing::warn!(error = %e, "accept failed"),
        }
    }
}
