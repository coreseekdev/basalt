//! ACL 骨架（T-M4.1 尾项）：存储 + 授权决策面 + 三 Admin API 支撑。
//!
//! 模型对齐 Kafka ACL（枚举值双源核对：kafka-clients Java 线协议值 ⊕
//! kafka-python acl_resource；10-12 号操作两者分歧，骨架只用无争议操作
//! READ/WRITE/CREATE/DELETE/DESCRIBE/ALL）。
//!
//! 边界（骨架）：host 仅 "*" 生效（特定 host 规则不匹配）；principal 格式
//! "User:<name>"；存储 = data_dir/acls.json（单机面，多节点复制留后续）；
//! authorizer 关闭（默认）时全放行——既有行为零变化。

use serde::{Deserialize, Serialize};
use std::sync::RwLock;

// ---- 线上枚举（Kafka AclBinding 面）----
pub const RT_UNKNOWN: i8 = 0;
pub const RT_ANY: i8 = 1;
pub const RT_TOPIC: i8 = 2;
pub const RT_GROUP: i8 = 3;
pub const RT_CLUSTER: i8 = 4;
pub const RT_TRANSACTIONAL_ID: i8 = 5;

pub const PT_LITERAL: i8 = 3;
pub const PT_PREFIXED: i8 = 4;

pub const OP_ANY: i8 = 1;
pub const OP_ALL: i8 = 2;
pub const OP_READ: i8 = 3;
pub const OP_WRITE: i8 = 4;
pub const OP_CREATE: i8 = 5;
pub const OP_DELETE: i8 = 6;
pub const OP_DESCRIBE: i8 = 8;

pub const PERM_DENY: i8 = 2;
pub const PERM_ALLOW: i8 = 3;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Acl {
    pub resource_type: i8,
    pub resource_name: String,
    pub pattern_type: i8,
    pub principal: String, // "User:<name>"
    pub host: String,      // 骨架仅 "*" 生效
    pub operation: i8,
    pub permission: i8,
}

static ACLS: RwLock<Option<Vec<Acl>>> = RwLock::new(None);

/// 启动装载（data_dir/acls.json；文件缺失 = 空表）
pub fn load_from_dir(dir: &std::path::Path) {
    let path = dir.join("acls.json");
    let acls: Vec<Acl> = std::fs::read_to_string(&path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default();
    tracing::info!(count = acls.len(), path = %path.display(), "ACLs loaded");
    *ACLS.write().unwrap() = Some(acls);
}

fn save_to_dir(dir: &std::path::Path, acls: &[Acl]) {
    let path = dir.join("acls.json");
    if let Ok(json) = serde_json::to_string_pretty(acls) {
        if let Err(e) = std::fs::write(&path, json) {
            tracing::warn!(error = %e, "ACL persist failed");
        }
    }
}

/// 数据目录注册（main.rs 启动调用后，变更即落盘）
static DATA_DIR: std::sync::OnceLock<String> = std::sync::OnceLock::new();
pub fn set_data_dir(dir: &str) {
    let _ = DATA_DIR.set(dir.to_string());
}

pub fn list() -> Vec<Acl> {
    ACLS.read().unwrap().as_ref().cloned().unwrap_or_default()
}

pub fn add(acls: Vec<Acl>) {
    let mut g = ACLS.write().unwrap();
    let v = g.get_or_insert_with(Vec::new);
    for a in acls {
        if !v.contains(&a) {
            v.push(a);
        }
    }
    if let Some(dir) = DATA_DIR.get() {
        save_to_dir(std::path::Path::new(dir), v);
    }
}

/// 按 filter 删除（ANY/空字段 = 通配），返回删除的条目
pub fn delete_by(filter: &Acl) -> Vec<Acl> {
    let mut g = ACLS.write().unwrap();
    let v = g.get_or_insert_with(Vec::new);
    let (removed, kept): (Vec<Acl>, Vec<Acl>) = v.drain(..).partition(|a| {
        matches_name(filter, a)
            && matches_i8_opt(filter.operation, a.operation, OP_ANY)
            && matches_i8_opt(filter.permission, a.permission, 1 /* PERM_ANY */)
    });
    if !removed.is_empty() {
        if let Some(dir) = DATA_DIR.get() {
            save_to_dir(std::path::Path::new(dir), &kept);
        }
    }
    *g = Some(kept);
    removed
}

fn matches_i8_opt(filter: i8, actual: i8, any: i8) -> bool {
    filter == any || filter == actual
}

/// filter 谓词（describe/delete 共用；ANY/空 = 通配）
pub fn filter_matches(filter: &Acl, a: &Acl) -> bool {
    matches_name(filter, a) && matches_i8_opt(filter.operation, a.operation, OP_ANY) && matches_i8_opt(filter.permission, a.permission, 1)
}

fn matches_name(filter: &Acl, a: &Acl) -> bool {
    if filter.resource_type != RT_ANY && filter.resource_type != a.resource_type {
        return false;
    }
    if !filter.resource_name.is_empty() && filter.resource_name != a.resource_name {
        return false;
    }
    if !filter.principal.is_empty() && filter.principal != a.principal {
        return false;
    }
    if !filter.host.is_empty() && filter.host != a.host {
        return false;
    }
    true
}

/// 授权决策：authorizer 关闭 → 恒 true；超级用户旁路；DENY 优先于 ALLOW；
/// LITERAL "*" 通配全部名字；PREFIXED 前缀匹配；无匹配 → 默认拒绝。
pub fn authorize(principal: &str, operation: i8, resource_type: i8, resource_name: &str) -> bool {
    authorize_with(
        principal,
        operation,
        resource_type,
        resource_name,
        authorizer_enabled(),
        superusers(),
    )
}

/// 决策纯函数（env 解耦——可测试注入）
fn authorize_with(
    principal: &str,
    operation: i8,
    resource_type: i8,
    resource_name: &str,
    enabled: bool,
    superusers: &[String],
) -> bool {
    if !enabled {
        return true;
    }
    if superusers.iter().any(|u| u == principal) {
        return true;
    }
    let prin = format!("User:{principal}");
    let acls = list();
    let mut allowed = false;
    for a in &acls {
        // principal 侧匹配（规则存 "User:<name>" 全格式）
        if a.principal != prin {
            continue;
        }
        // host 骨架：仅 "*" 规则生效
        if a.host != "*" {
            continue;
        }
        // 资源匹配
        if a.resource_type != resource_type && a.resource_type != RT_ANY {
            continue;
        }
        let name_match = if a.pattern_type == PT_PREFIXED {
            resource_name.starts_with(&a.resource_name)
        } else {
            a.resource_name == "*" || a.resource_name == resource_name
        };
        if !name_match {
            continue;
        }
        if a.operation != OP_ALL && a.operation != operation && a.operation != OP_ANY {
            continue;
        }
        if a.permission == PERM_DENY {
            return false;
        }
        if a.permission == PERM_ALLOW {
            allowed = true;
        }
    }
    allowed
}

fn authorizer_enabled() -> bool {
    static E: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *E.get_or_init(|| std::env::var("BASALT_AUTHORIZER_ENABLED").as_deref() == Ok("true"))
}

fn superusers() -> &'static Vec<String> {
    static S: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
    S.get_or_init(|| {
        std::env::var("BASALT_SUPER_USERS")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    })
}

#[cfg(test)]
mod acl_tests {
    use super::*;

    fn acl(rt: i8, name: &str, pt: i8, principal: &str, op: i8, perm: i8) -> Acl {
        Acl {
            resource_type: rt,
            resource_name: name.into(),
            pattern_type: pt,
            principal: principal.into(),
            host: "*".into(),
            operation: op,
            permission: perm,
        }
    }

    /// 决策矩阵：DENY 优先 / LITERAL 通配 / PREFIXED 前缀 / 默认拒绝 /
    /// 无关 principal 与操作不匹配
    #[test]
    fn authorize_decision_matrix() {
        // 注入式决策（enabled=true）——全局 ACLS 作为规则载体（决策逻辑与
        // 载体无差）；authorizer 关闭面由 authorizer_disabled_allows_all 覆盖
        *ACLS.write().unwrap() = Some(vec![
            acl(RT_TOPIC, "app-", PT_PREFIXED, "User:app", OP_WRITE, PERM_ALLOW),
            acl(RT_TOPIC, "*", PT_LITERAL, "User:app", OP_READ, PERM_ALLOW),
            acl(RT_TOPIC, "secret", PT_LITERAL, "User:app", OP_READ, PERM_DENY),
            acl(RT_GROUP, "g1", PT_LITERAL, "User:app", OP_READ, PERM_ALLOW),
        ]);
        let dec = |p: &str, op: i8, rt: i8, n: &str| authorize_with(p, op, rt, n, true, &[]);
        // PREFIXED 前缀匹配 + 对应操作
        assert!(dec("app", OP_WRITE, RT_TOPIC, "app-events"));
        assert!(!dec("app", OP_WRITE, RT_TOPIC, "other"), "前缀不匹配默认拒绝");
        // LITERAL "*" 通配读
        assert!(dec("app", OP_READ, RT_TOPIC, "anything"));
        // DENY 优先于同资源其他 ALLOW
        assert!(!dec("app", OP_READ, RT_TOPIC, "secret"));
        // 操作不匹配 → 默认拒绝（有 READ 不等于可 DELETE）
        assert!(!dec("app", OP_DELETE, RT_TOPIC, "anything"));
        // group 面
        assert!(dec("app", OP_READ, RT_GROUP, "g1"));
        assert!(!dec("app", OP_READ, RT_GROUP, "g2"));
        // 无关 principal
        assert!(!dec("mallory", OP_READ, RT_TOPIC, "anything"));
        // 超级用户旁路（注入式）
        assert!(authorize_with("root", OP_DELETE, RT_TOPIC, "anything", true, &["root".into()]));
        // 关闭 authorizer：恒放行（超级用户列表不参与）
        assert!(authorize_with("nobody", OP_DELETE, RT_TOPIC, "anything", false, &[]));
        *ACLS.write().unwrap() = Some(vec![]);
    }

    #[test]
    fn authorizer_disabled_allows_all() {
        // 默认（未设 BASALT_AUTHORIZER_ENABLED）恒放行——既有行为零变化
        assert!(authorize("nobody", OP_DELETE, RT_TOPIC, "anything"));
    }
}
