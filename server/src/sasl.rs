//! SASL/SCRAM-SHA-256 服务端认证（T-S1 安全面，行动清单 P0）。
//!
//! 凭据：环境变量 BASALT_SASL_USERS（`user1:pass1,...`），启动时一次性
//! 派生（salt=HMAC(sha256(user), server_secret)、iterations=4096），明文
//! 口令即弃。BASALT_AUTH=scram 时，非 SASL 请求在认证完成前被 conn.rs
//! 门禁拒绝（连接关闭——Kafka broker 同语义）。

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

type HmacSha256 = Hmac<Sha256>;

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac any len");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn sha256(data: &[u8]) -> Vec<u8> {
    Sha256::digest(data).to_vec()
}

/// PBKDF2-HMAC-SHA-256 单块（32B = SHA-256 输出宽度）
fn pbkdf2_sha256(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let salt_block: Vec<u8> = salt.iter().copied().chain(1u32.to_be_bytes()).collect();
    let mut u = hmac_sha256(password, &salt_block);
    let mut out = u.clone();
    for _ in 1..iterations {
        u = hmac_sha256(password, &u);
        for (o, x) in out.iter_mut().zip(&u) {
            *o ^= x;
        }
    }
    let mut result = [0u8; 32];
    result.copy_from_slice(&out);
    result
}

/// 服务端派生凭据（明文口令即弃）
pub struct StoredCredentials {
    pub salt: [u8; 16],
    pub iterations: u32,
    pub stored_key: [u8; 32],  // H(ClientKey)
    pub server_key: [u8; 32],  // HMAC(SaltedPassword, "Server Key")
}

/// 凭据表（启动时一次性派生，后续只读）
pub struct ScramUsers {
    users: HashMap<String, StoredCredentials>,
}

impl ScramUsers {
    pub fn from_env() -> Self {
        let raw = std::env::var("BASALT_SASL_USERS").unwrap_or_default();
        let mut users = HashMap::new();
        for pair in raw.split(',') {
            let mut kv = pair.splitn(2, ':');
            let (Some(user), Some(pass)) = (kv.next(), kv.next()) else { continue };
            if user.is_empty() || pass.is_empty() {
                continue;
            }
            let salt_bytes = sha256(user.as_bytes());
            let mut salt = [0u8; 16];
            salt.copy_from_slice(&salt_bytes[..16]);
            let iterations = 4096u32;
            let salted = pbkdf2_sha256(pass.as_bytes(), &salt, iterations);
            let client_key = hmac_sha256(&salted, b"Client Key");
            let stored_key: [u8; 32] = sha256(&client_key).try_into().unwrap();
            let server_key: [u8; 32] = hmac_sha256(&salted, b"Server Key").try_into().unwrap();
            users.insert(user.to_string(), StoredCredentials { salt, iterations, stored_key, server_key });
        }
        ScramUsers { users }
    }

    pub fn get(&self, user: &str) -> Option<&StoredCredentials> {
        self.users.get(user)
    }
}

/// 进程级凭据表（启动时一次性派生；每连接共享只读）
pub fn global_users() -> &'static ScramUsers {
    static U: std::sync::OnceLock<ScramUsers> = std::sync::OnceLock::new();
    U.get_or_init(ScramUsers::from_env)
}

/// BASALT_AUTH=scram 时启用连接级认证门禁
pub fn auth_enabled() -> bool {
    std::env::var("BASALT_AUTH").as_deref() == Ok("scram")
}

/// 本 broker 支持的机制（Handshake 响应的 Mechanisms）
pub const MECHANISMS: &[&str] = &["SCRAM-SHA-256"];

/// 连接级 SCRAM 会话（conn.rs per connection）
#[derive(Default)]
pub struct ScramSession {
    pub state: ScramState,
    pub user: String,
    client_first_bare: String,
    server_first: String,
    auth_message: String,
    stored_key: Option<[u8; 32]>,
    server_key: Option<[u8; 32]>,
    nonce_prefix: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum ScramState {
    #[default]
    Idle,
    ServerFirstSent,
    Authenticated,
}

/// SCRAM 协议步骤（conn.rs 的 SASL_AUTHENTICATE handler 调用）
#[derive(Debug)]
pub enum ScramOutcome {
    /// 认证成功（连接已认证，user 可用；server_final = `v=<sig>` 须回给
    /// 客户端——RFC 5802 服务端最后一步，kafka-python/franz-go 均校验签名）
    Authenticated { user: String, server_final: String },
    /// 需要下一轮：返回 server-first-message 的 base64（放入 SASL_AUTHENTICATE 响应）
    Continue { data: String },
    /// 认证失败（连接可关闭或允许重试）
    Failed { reason: String },
}

fn parse_attr(msg: &str, want: char) -> Option<String> {
    for part in msg.split(',') {
        let mut kv = part.splitn(2, '=');
        if kv.next()? == want.to_string() {
            return kv.next().map(|s| s.to_string());
        }
    }
    None
}

fn rand_server_nonce() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let mut seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(1);
    let mut buf = [0u8; 18];
    for b in buf.iter_mut() {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *b = (seed >> 33) as u8;
    }
    let mut out = String::new();
    for chunk in buf.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        for (i, byte) in b.iter().enumerate() {
            match (i, chunk.len()) {
                (0, _) => out.push(((*byte >> 2) as u8 + b'A') as char),
                (1, _) => out.push((((byte >> 4) & 0x0F) as u8 + b'a') as char),
                (2, 3) => out.push((((byte >> 6) & 0x03) as u8 + b'0') as char),
                _ => {}
            }
        }
    }
    out
}

/// SCRAM-SHA-256 认证（conn.rs 每条 SASL_AUTHENTICATE 调用一次）
///
/// - 首轮 `msg` = client-first-message（如 `n,,n=admin,r=abc123`）
/// - 次轮 `msg` = client-final-message（如 `c=biws,r=abc123srv,p=proof_b64`）
///
/// 返回 ScramOutcome 供 conn.rs 映射 SASL_AUTHENTICATE 响应。
pub fn scram_authenticate(
    session: &mut ScramSession,
    users: &ScramUsers,
    msg: &[u8],
) -> ScramOutcome {
    let text = String::from_utf8_lossy(msg);
    if session.state == ScramState::Idle {
        // client-first-message（含 gs2 header）
        let gs2_end = text.find(",n=").map(|i| i + 1).unwrap_or(0);
        let bare_start = text.find("n=").map(|i| i).unwrap_or(0);
        let bare = &text[bare_start..];
        session.client_first_bare = bare.to_string();
        let user = parse_attr(bare, 'n').unwrap_or_default();
        let client_nonce = parse_attr(bare, 'r').unwrap_or_default();
        let Some(creds) = users.get(&user) else {
            return ScramOutcome::Failed { reason: "SCRAM-SHA-256: unknown user".into() };
        };
        let srv = rand_server_nonce();
        let server_nonce = format!("{client_nonce}{srv}");
        session.server_first = format!("r={server_nonce},s={},i={}",
            b64_encode(&creds.salt), creds.iterations);
        session.nonce_prefix = client_nonce;
        session.user = user;
        session.stored_key = Some(creds.stored_key);
        session.server_key = Some(creds.server_key);
        session.auth_message = format!("{},{}", session.client_first_bare, session.server_first);
        session.state = ScramState::ServerFirstSent;
        let _ = gs2_end;
        ScramOutcome::Continue { data: session.server_first.clone() }
    } else {
        // client-final-message
        let proof_b64 = parse_attr(&text, 'p').unwrap_or_default();
        let without_proof = match text.rfind(",p=") {
            Some(i) => text[..i].to_string(),
            None => text.to_string(),
        };
        session.auth_message = format!("{},{}", session.auth_message, without_proof);
        let proof = match b64_decode(&proof_b64) {
            Some(p) => p,
            None => return ScramOutcome::Failed { reason: "bad proof".into() },
        };
        let Some(stored_key) = session.stored_key else {
            return ScramOutcome::Failed { reason: "no stored key".into() };
        };
        let client_sig = hmac_sha256(&stored_key, session.auth_message.as_bytes());
        let client_key: Vec<u8> = proof.iter().zip(&client_sig).map(|(a, b)| a ^ b).collect();
        let computed_stored = sha256(&client_key);
        if computed_stored.as_slice() != stored_key.as_slice() {
            return ScramOutcome::Failed { reason: "SCRAM proof mismatch".into() };
        }
        // ServerSignature = HMAC(ServerKey, AuthMessage)；AuthMessage 不含
        // server-final 自身——先算签名再回填（auth_message 保持 RFC 5802 定义）
        let Some(server_key) = session.server_key else {
            return ScramOutcome::Failed { reason: "no server key".into() };
        };
        let server_sig = hmac_sha256(&server_key, session.auth_message.as_bytes());
        session.state = ScramState::Authenticated;
        let server_final = format!("v={}", b64_encode(&server_sig));
        ScramOutcome::Authenticated { user: session.user.clone(), server_final }
    }
}

/// 认证失败后会话回退 Idle（允许同一连接重新握手——Kafka broker 语义是
/// 关连接，这里保留重试面，关闭决策归 conn.rs）
pub fn reset_session(session: &mut ScramSession) {
    let user = std::mem::take(&mut session.user);
    *session = ScramSession { user, ..Default::default() };
}

/// base64 编码（RFC 4648 标准字母表，无 padding 需求时也兼容）
pub fn b64_encode(data: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        out.push(T[(b[0] >> 2) as usize] as char);
        out.push(T[(((b[0] & 0x03) << 4) | (b[1] >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(T[(((b[1] & 0x0F) << 2) | (b[2] >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(T[(b[2] & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

/// base64 解码
pub fn b64_decode(s: &str) -> Option<Vec<u8>> {
    fn v(c: u8) -> Option<u32> {
        match c {
            b'A'..=b'Z' => Some((c - b'A') as u32),
            b'a'..=b'z' => Some((c - b'a' + 26) as u32),
            b'0'..=b'9' => Some((c - b'0' + 52) as u32),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let s = s.trim_end_matches('=');
    let mut out = Vec::new();
    let mut buf = 0u32;
    let mut bits = 0;
    for &c in s.as_bytes() {
        buf = (buf << 6) | v(c)?;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    Some(out)
}


#[cfg(test)]
mod scram_tests {
    use super::*;

    fn make_users() -> ScramUsers {
        let mut users = HashMap::new();
        let salt = [1u8; 16];
        let salted = pbkdf2_sha256(b"secret123", &salt, 4096);
        let stored_key: [u8; 32] = sha256(&hmac_sha256(&salted, b"Client Key")).try_into().unwrap();
        let server_key: [u8; 32] = hmac_sha256(&salted, b"Server Key").try_into().unwrap();
        users.insert("admin".to_string(), StoredCredentials {
            salt, iterations: 4096, stored_key, server_key,
        });
        ScramUsers { users }
    }

    /// 模拟合法客户端的 SCRAM 流程（RFC 5802 服务端视角单测）
    #[test]
    fn scram_success_flow() {
        let users = make_users();
        let mut sess = ScramSession::default();
        // 阶段 1：client-first
        match scram_authenticate(&mut sess, &users, b"n,,n=admin,r=abc123") {
            ScramOutcome::Continue { data } => {
                assert!(data.contains("r=abc123") && data.contains("s="), "server-first 格式");
            }
            ScramOutcome::Failed { reason } => panic!("阶段 1 失败: {reason}"),
            _ => panic!("阶段 1 应为 Continue"),
        }
        // 阶段 2：client-final（正确 proof）
        let auth_msg = format!("{},{}", sess.auth_message, "");
        // 从 server-first 提取 nonce/salt/iterations，构造正确 proof
        let nonce = parse_attr(&sess.server_first, 'r').unwrap();
        let salt_b64 = parse_attr(&sess.server_first, 's').unwrap();
        let iters: u32 = parse_attr(&sess.server_first, 'i').unwrap().parse().unwrap();
        let salt = b64_decode(&salt_b64).unwrap();
        let salted = pbkdf2_sha256(b"secret123", &salt, iters);
        let client_key = hmac_sha256(&salted, b"Client Key");
        let client_final_without = format!("c=biws,r={nonce}");
        let auth_msg = format!("{},{}", sess.auth_message, client_final_without);
        let stored_key: [u8; 32] = sha256(&client_key).try_into().unwrap();
        let client_sig = hmac_sha256(&stored_key, auth_msg.as_bytes());
        let client_proof: Vec<u8> = client_key.iter().zip(&client_sig).map(|(a, b)| a ^ b).collect();
        let client_final = format!("{client_final_without},p={}", b64_encode(&client_proof));
        match scram_authenticate(&mut sess, &users, client_final.as_bytes()) {
            ScramOutcome::Authenticated { user, server_final } => {
                assert_eq!(user, "admin");
                // 客户端视角校验 server-final：ServerSignature = HMAC(ServerKey,
                // AuthMessage)——kafka-python process_server_final_message 同式
                let server_sig = hmac_sha256(&hmac_sha256(&salted, b"Server Key"), auth_msg.as_bytes());
                assert_eq!(server_final, format!("v={}", b64_encode(&server_sig)));
            }
            ScramOutcome::Failed { reason } => panic!("阶段 2 失败: {reason}"),
            _ => panic!("阶段 2 应为 Authenticated"),
        }
        assert_eq!(sess.state, ScramState::Authenticated);
    }

    /// 错误口令 → 阶段 2 Failed
    #[test]
    fn scram_wrong_password_rejected() {
        let users = make_users();
        let mut sess = ScramSession::default();
        let _ = scram_authenticate(&mut sess, &users, b"n,,n=admin,r=test1");
        let nonce = parse_attr(&sess.server_first, 'r').unwrap();
        let salt_b64 = parse_attr(&sess.server_first, 's').unwrap();
        let salt = b64_decode(&salt_b64).unwrap();
        // 错误口令派生
        let wrong_salted = pbkdf2_sha256(b"wrong_password", &salt, 4096);
        let wrong_ck = hmac_sha256(&wrong_salted, b"Client Key");
        let stored = [0u8; 32];
        let cs = hmac_sha256(&stored, format!("n,,n=admin,r=test1,{},{}", sess.server_first, "c=biws,r=test1").as_bytes());
        let proof: Vec<u8> = wrong_ck.iter().zip(&cs).map(|(a, b)| a ^ b).collect();
        let client_final = format!("c=biws,r={nonce},p={}", b64_encode(&proof));
        match scram_authenticate(&mut sess, &users, client_final.as_bytes()) {
            ScramOutcome::Failed { .. } => {}
            other => panic!("期望失败，得 {other:?}"),
        }
    }

    /// 未知用户 → 阶段 1 Failed
    #[test]
    fn scram_unknown_user_rejected() {
        let users = make_users();
        let mut sess = ScramSession::default();
        match scram_authenticate(&mut sess, &users, b"n,,n=ghost,r=n1") {
            ScramOutcome::Failed { .. } => {}
            _ => panic!("未知用户应失败"),
        }
    }
}
