//! 配置：env 优先（T-0.5 的最小先行版），类型化字段。

#[derive(Debug, Clone)]
pub struct Config {
    pub node_id: i32,
    pub host: String,
    pub port: u16,
    pub data_dir: String,
    /// 自动建题默认分区数 / 副本数。
    pub num_partitions: i32,
    pub default_rf: i32,
    pub segment_max_bytes: u64,
    pub log_level: String,
    /// 集群成员（多节点 POC）：`0=host:port,1=host:port`。
    pub nodes: Vec<(i32, String, u16)>,
}

impl Config {
    pub fn from_env() -> Config {
        let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
        let port: u16 = env("BASALT_PORT", "9092").parse().unwrap_or(9092);
        let node_id: i32 = env("BASALT_NODE_ID", "0").parse().unwrap_or(0);
        let nodes = parse_nodes(&env("BASALT_NODES", ""));
        Config {
            node_id,
            host: env("BASALT_HOST", "localhost"),
            port,
            data_dir: env("BASALT_DATA_DIR", "./data"),
            num_partitions: env("BASALT_NUM_PARTITIONS", "1").parse().unwrap_or(1),
            default_rf: env("BASALT_RF", "1").parse().unwrap_or(1),
            segment_max_bytes: env("BASALT_SEGMENT_MAX_BYTES", "1073741824")
                .parse()
                .unwrap_or(1 << 30),
            log_level: env("BASALT_LOG_LEVEL", "info"),
            nodes,
        }
    }

    pub fn listen_addr(&self) -> String {
        format!("0.0.0.0:{}", self.port)
    }

    pub fn broker_ids(&self) -> Vec<i32> {
        if self.nodes.is_empty() {
            vec![self.node_id]
        } else {
            let mut ids: Vec<i32> = self.nodes.iter().map(|(id, _, _)| *id).collect();
            if !ids.contains(&self.node_id) {
                ids.push(self.node_id);
            }
            ids.sort_unstable();
            ids
        }
    }
}

fn parse_nodes(s: &str) -> Vec<(i32, String, u16)> {
    let mut out = Vec::new();
    for part in s.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let Some((id, addr)) = part.split_once('=') else { continue };
        let Ok(id) = id.parse() else { continue };
        if let Some((h, p)) = addr.rsplit_once(':') {
            if let Ok(p) = p.parse() {
                out.push((id, h.to_string(), p));
            }
        }
    }
    out
}
