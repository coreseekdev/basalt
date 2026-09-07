//! Basalt broker 进程壳：启动元数据服务 → 恢复本地日志 → 监听端口。

mod config;
mod conn;
mod handlers;
mod handlers_groups;
mod meta;
mod partition;

use config::Config;
use std::sync::OnceLock;

static CTX: OnceLock<handlers::Ctx> = OnceLock::new();

fn main() {
    let cfg = Config::from_env();
    let filter = tracing_subscriber::EnvFilter::try_new(&cfg.log_level)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(async_main(cfg));
}

async fn async_main(cfg: Config) {
    std::fs::create_dir_all(&cfg.data_dir).expect("data dir");
    tracing::info!(node_id = cfg.node_id, port = cfg.port, data = %cfg.data_dir, "basalt starting");

    let (meta_tx, routes_rx) = meta::MetaService::spawn(cfg.clone());
    let group_tx = basalt_coordinator::GroupManager::spawn(std::path::Path::new(&cfg.data_dir));

    // 恢复既有 topic：目录先于元数据存在 → 用恢复通道逐个注册
    recover_existing(&cfg, &meta_tx).await;

    let ctx = handlers::Ctx {
        node_id: cfg.node_id,
        host: cfg.host.clone(),
        port: cfg.port,
        meta_tx,
        group_tx: group_tx.clone(),
        routes_rx,
    };
    CTX.set(ctx).ok();

    let ctx_ref = CTX.get().expect("ctx");
    let listener = tokio::net::TcpListener::bind(cfg.listen_addr()).await.expect("bind");
    tracing::info!(addr = %cfg.listen_addr(), "basalt listening");

    loop {
        match listener.accept().await {
            Ok((sock, peer)) => {
                let ctx = handlers::Ctx {
                    node_id: ctx_ref.node_id,
                    host: ctx_ref.host.clone(),
                    port: ctx_ref.port,
                    meta_tx: ctx_ref.meta_tx.clone(),
                    group_tx: ctx_ref.group_tx.clone(),
                    routes_rx: ctx_ref.routes_rx.clone(),
                };
                tokio::spawn(conn::serve_connection(sock, peer, ctx));
            }
            Err(e) => tracing::warn!(error = %e, "accept failed"),
        }
    }
}

/// 目录扫描恢复：data/<topic>/p<i> 存在但元数据未登记 → 触发自动建题登记。
async fn recover_existing(cfg: &Config, meta_tx: &tokio::sync::mpsc::Sender<meta::MetaCmd>) {
    let Ok(entries) = std::fs::read_dir(&cfg.data_dir) else { return };
    for e in entries.flatten() {
        if !e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let topic = e.file_name().to_string_lossy().into_owned();
        let Ok(parts) = std::fs::read_dir(e.path()) else { continue };
        let has_parts = parts
            .flatten()
            .any(|p| p.file_name().to_string_lossy().starts_with('p'));
        if !has_parts {
            continue;
        }
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        if meta_tx
            .send(meta::MetaCmd::Lookup {
                names: Some(vec![topic]),
                allow_create: true,
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            return;
        }
        let _ = reply_rx.await;
    }
}
