//! TLS listener（T-S1 加密面）：BASALT_TLS_CERT/BASALT_TLS_KEY（PEM）配置后
//! 于 BASALT_TLS_PORT（默认 client port+2，与内部口 +1 同风格）另起 TLS
//! listener——Kafka 多 listener 语义：主口保持明文兼容，加密是独立端口。
//! 证书由部署侧提供（自签/CA 均可；e2e 用 openssl 现生成）。

use rustls_pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
use std::sync::Arc;

/// TLS 端口（主口 +2 或 BASALT_TLS_PORT 显式覆盖）；未配置证书 → None
pub fn listener_port(client_port: u16) -> Option<u16> {
    server_config()?; // 无证书即无 TLS 口
    let p = std::env::var("BASALT_TLS_PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok());
    Some(p.unwrap_or(client_port + 2))
}

/// 服务端证书/私钥装载（PEM；失败返回 None 并由调用方告警降级为无 TLS 口）
pub fn server_config() -> Option<Arc<rustls::ServerConfig>> {
    let cert_path = std::env::var("BASALT_TLS_CERT").ok()?;
    let key_path = std::env::var("BASALT_TLS_KEY").ok()?;
    if cert_path.is_empty() || key_path.is_empty() {
        return None;
    }
    let certs: Vec<_> = match CertificateDer::pem_file_iter(&cert_path) {
        Ok(c) => c.filter_map(|r| r.ok()).collect(),
        Err(e) => {
            tracing::error!(path = %cert_path, error = %e, "TLS cert load failed");
            return None;
        }
    };
    if certs.is_empty() {
        tracing::error!(path = %cert_path, "TLS cert file has no PEM certificates");
        return None;
    }
    let key = match PrivateKeyDer::from_pem_file(&key_path) {
        Ok(k) => k,
        Err(e) => {
            tracing::error!(path = %key_path, error = %e, "TLS key load failed");
            return None;
        }
    };
    match rustls::ServerConfig::builder().with_no_client_auth().with_single_cert(certs, key) {
        Ok(cfg) => {
            tracing::info!("TLS listener enabled");
            Some(Arc::new(cfg))
        }
        Err(e) => {
            tracing::error!(error = %e, "TLS ServerConfig build failed");
            None
        }
    }
}
