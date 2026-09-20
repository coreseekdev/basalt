//! basalt-cli：basalt 运维 CLI（A5 最小集）。
//!
//! 直连 Kafka 线协议（复用 basalt-protocol 数据驱动编解码），无外部客户端依赖。
//! 子命令面 = broker 已实现的 Admin API：
//!   topics  list / describe / create / delete
//!   groups  list / describe / offsets
//! （DeleteGroups API broker 未实现，无 groups delete——见 conn.rs dispatch 表）
//!
//! 用法：
//!   basalt-cli [--bootstrap HOST:PORT] topics list
//!   basalt-cli topics describe NAME
//!   basalt-cli topics create NAME [--partitions N] [--replication N] [--config k=v]...
//!   basalt-cli topics delete NAME
//!   basalt-cli groups list
//!   basalt-cli groups describe GROUP
//!   basalt-cli groups offsets GROUP        # 各分区 committed/end/lag
//!
//! 环境变量 BASALT_BOOTSTRAP 为 --bootstrap 缺省；BASALT_CLI_DEBUG=1 打印
//! 原始响应树（排障用）。

use basalt_protocol::api::key;
use basalt_protocol::codec;
use basalt_protocol::frame;
use basalt_protocol::primitives::Reader;
use basalt_protocol::registry::Registry;
use basalt_protocol::value::{s, Struct, Value};
use bytes::{BufMut, Bytes, BytesMut};

type Result<T> = std::result::Result<T, String>;

// ---------- 协议客户端 ----------

struct Client {
    stream: tokio::net::TcpStream,
    correlation: i32,
}

impl Client {
    async fn connect(addr: &str) -> Result<Self> {
        let stream = tokio::net::TcpStream::connect(addr)
            .await
            .map_err(|e| format!("connect {addr}: {e}"))?;
        Ok(Client { stream, correlation: 0 })
    }

    /// 单次请求-响应（CLI 串行使用，无并发面）。body = Value::Struct。
    async fn call(&mut self, api_key: i16, version: i16, body: Value) -> Result<Struct> {
        let entry = Registry::global().api(api_key)
            .ok_or_else(|| format!("api key {api_key} not in protocol registry"))?;
        let flexible = frame::is_flexible(version, entry.flexible_from);
        self.correlation += 1;
        let corr = self.correlation;

        // 请求体：计划驱动编码
        let Value::Struct(body) = body else { return Err("request body must be a struct".into()) };
        let mut body_bytes = BytesMut::new();
        codec::encode_struct_fields(&entry.request.fields, version, flexible, &body, &mut body_bytes)
            .map_err(|e| format!("encode request: {e}"))?;
        if std::env::var("BASALT_CLI_DEBUG").as_deref() == Ok("2") {
            eprintln!("api={api_key} v={version} flexible={flexible} body={:02x?}", &body_bytes[..]);
        }

        // 请求头（手写，镜像 frame::read_request_header 的解析规则：
        // client_id 恒为 nullable string，不随消息 flexible 变 compact）
        let mut head = BytesMut::new();
        head.put_i16(api_key);
        head.put_i16(version);
        head.put_i32(corr);
        let cid = b"basalt-cli";
        head.put_i16(cid.len() as i16);
        head.extend_from_slice(cid);
        if flexible {
            head.put_u8(0); // 空 tag section
        }

        let mut out = BytesMut::new();
        out.put_i32((head.len() + body_bytes.len()) as i32);
        out.extend_from_slice(&head);
        out.extend_from_slice(&body_bytes);

        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        self.stream.write_all(&out).await.map_err(|e| format!("send: {e}"))?;

        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf).await.map_err(|e| format!("recv frame: {e}"))?;
        let len = i32::from_be_bytes(len_buf);
        if !(0..64 * 1024 * 1024).contains(&len) {
            return Err(format!("bad frame length {len}"));
        }
        let mut buf = vec![0u8; len as usize];
        self.stream.read_exact(&mut buf).await.map_err(|e| format!("recv body: {e}"))?;
        let bytes = Bytes::from(buf);

        // 响应头：correlation 校验 + flexible 时跳过 tag section
        let rh_v = frame::response_header_version(api_key, flexible);
        let mut r = Reader::new(&bytes);
        let resp_corr = r.i32().map_err(|e| format!("response header: {e}"))?;
        if resp_corr != corr {
            return Err(format!("correlation mismatch: sent {corr}, got {resp_corr}"));
        }
        if rh_v == 1 {
            let n = r.u8().map_err(|e| format!("response header: {e}"))?;
            for _ in 0..n {
                let _tag = r.uvarint().map_err(|e| format!("response header: {e}"))?;
                let sz = r.uvarint().map_err(|e| format!("response header: {e}"))?;
                r.skip(sz as usize).map_err(|e| format!("response header: {e}"))?;
            }
        }
        let body = bytes.slice(r.pos()..);
        codec::decode(&entry.response.fields, version, flexible, &body)
            .map_err(|e| format!("decode response: {e}"))
    }
}

fn dump(st: &Struct) {
    if std::env::var("BASALT_CLI_DEBUG").as_deref() == Ok("1") {
        eprintln!("{st:#?}");
    }
}

fn get_i16(st: &Struct, name: &str) -> i16 {
    st.get(name).map(|v| v.as_i16()).unwrap_or(0)
}
fn get_i32(st: &Struct, name: &str) -> i32 {
    st.get(name).map(|v| v.as_i32()).unwrap_or(-1)
}
fn get_i64(st: &Struct, name: &str) -> i64 {
    st.get(name).map(|v| v.as_i64()).unwrap_or(-1)
}
fn get_str(st: &Struct, name: &str) -> String {
    st.get(name).map(|v| v.as_str().to_string()).unwrap_or_default()
}
fn get_bool(st: &Struct, name: &str) -> bool {
    st.get(name).map(|v| v.as_bool()).unwrap_or(false)
}

// ---------- 错误码 ----------

fn err_name(code: i16) -> String {
    use basalt_protocol::api::ErrorCode as E;
    let known: &[(i16, &str)] = &[
        (E::None as i16, "NONE"),
        (E::OffsetOutOfRange as i16, "OFFSET_OUT_OF_RANGE"),
        (E::CorruptMessage as i16, "CORRUPT_MESSAGE"),
        (E::UnknownTopicOrPartition as i16, "UNKNOWN_TOPIC_OR_PARTITION"),
        (E::LeaderNotAvailable as i16, "LEADER_NOT_AVAILABLE"),
        (E::NotLeaderOrFollower as i16, "NOT_LEADER_OR_FOLLOWER"),
        (E::RequestTimedOut as i16, "REQUEST_TIMED_OUT"),
        (E::NetworkException as i16, "NETWORK_EXCEPTION"),
        (E::CoordinatorNotAvailable as i16, "COORDINATOR_NOT_AVAILABLE"),
        (E::NotCoordinator as i16, "NOT_COORDINATOR"),
        (E::InvalidTopicException as i16, "INVALID_TOPIC_EXCEPTION"),
        (E::TopicAlreadyExists as i16, "TOPIC_ALREADY_EXISTS"),
        (E::InvalidPartitions as i16, "INVALID_PARTITIONS"),
        (E::InvalidReplicationFactor as i16, "INVALID_REPLICATION_FACTOR"),
        (E::CoordinatorNotAvailable as i16, "GROUP_COORDINATOR_NOT_AVAILABLE"), // 15：Kafka 旧名
        (E::NotCoordinator as i16, "NOT_COORDINATOR_FOR_GROUP"), // 16：Kafka 旧名
        (E::GroupIdNotFound as i16, "GROUP_ID_NOT_FOUND"),
        (E::UnsupportedVersion as i16, "UNSUPPORTED_VERSION"),
        (E::TopicAlreadyExists as i16, "TOPIC_ALREADY_EXISTS"),
        (E::InvalidGroupId as i16, "INVALID_GROUP_ID"),
        (E::UnknownMemberId as i16, "UNKNOWN_MEMBER_ID"),
    ];
    known.iter()
        .find(|(c, _)| *c == code)
        .map(|(_, n)| (*n).to_string())
        .unwrap_or_else(|| format!("ERROR_{code}"))
}

fn check(st: &Struct, what: &str) -> Result<()> {
    let code = get_i16(st, "ErrorCode");
    if code != 0 {
        return Err(format!("{what}: {} ({code})", err_name(code)));
    }
    Ok(())
}

// ---------- 命令实现 ----------

const V_METADATA: i16 = 12;
const V_CREATE_TOPICS: i16 = 5;
const V_DELETE_TOPICS: i16 = 6;
const V_LIST_GROUPS: i16 = 4;
const V_DESCRIBE_GROUPS: i16 = 1;
const V_OFFSET_FETCH: i16 = 5;
const V_LIST_OFFSETS: i16 = 1;

fn struct_of<'a>(arr: &'a [Value], what: &str) -> Result<&'a Struct> {
    match arr.first() {
        Some(Value::Struct(s)) => Ok(s),
        _ => Err(format!("{what}: empty response")),
    }
}

async fn topic_shape(c: &mut Client, name: &str) -> Result<(usize, usize)> {
    let resp = c.call(key::METADATA, V_METADATA, s([
        ("Topics", Value::Array(vec![s([("Name", Value::str(name))])])),
    ])).await?;
    let ts = resp.get_array("Topics").ok_or("metadata: no Topics")?;
    let t = struct_of(ts, "metadata")?;
    let parts = t.get_array("Partitions").map(|p| p.len()).unwrap_or(0);
    let rf = t.get_array("Partitions")
        .and_then(|p| p.first())
        .and_then(|p| match p {
            Value::Struct(s) => s.get_array("ReplicaNodes").map(|r| r.len()),
            _ => None,
        })
        .unwrap_or(0);
    Ok((parts, rf))
}

async fn topics_list(c: &mut Client) -> Result<()> {
    // Metadata v1：Topics=null → 全量
    let resp = c.call(key::METADATA, V_METADATA, s([
        ("Topics", Value::Null),
    ])).await?;
    dump(&resp);
    let empty: &[Value] = &[];
    let mut names: Vec<(String, bool)> = Vec::new();
    for t in resp.get_array("Topics").unwrap_or(empty) {
        let Value::Struct(st) = t else { continue };
        check(st, "metadata")?;
        names.push((get_str(st, "Name"), get_bool(st, "IsInternal")));
    }
    names.sort();
    println!("{:<40} {:>6} {:>4} {:>8}", "TOPIC", "PARTS", "RF", "INTERNAL");
    for (name, internal) in &names {
        let (parts, rf) = topic_shape(c, name).await?;
        println!("{:<40} {:>6} {:>4} {:>8}", name, parts, rf, if *internal { "yes" } else { "-" });
    }
    Ok(())
}

async fn topics_describe(c: &mut Client, name: &str) -> Result<()> {
    let resp = c.call(key::METADATA, V_METADATA, s([
        ("Topics", Value::Array(vec![s([("Name", Value::str(name))])])),
    ])).await?;
    dump(&resp);
    let ts = resp.get_array("Topics").ok_or("metadata: no Topics")?;
    let t = struct_of(ts, "metadata")?;
    let code = get_i16(t, "ErrorCode");
    if code == 3 {
        return Err(format!("topic {name}: UNKNOWN_TOPIC_OR_PARTITION"));
    }
    check(t, name)?;
    let brokers: std::collections::HashMap<i32, String> = resp
        .get_array("Brokers")
        .map(|bs| {
            bs.iter()
                .filter_map(|b| match b {
                    Value::Struct(s) => {
                        let id = get_i32(s, "NodeId");
                        Some((id, format!("{}:{}", get_str(s, "Host"), get_i32(s, "Port"))))
                    }
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();
    println!("topic: {name}");
    println!("{:<6} {:<24} {:<14} ISR", "PART", "LEADER", "REPLICAS");
    let empty: &[Value] = &[];
    for p in t.get_array("Partitions").unwrap_or(empty) {
        let Value::Struct(s) = p else { continue };
        check(s, "partition")?;
        let fmt = |arr: Option<&[Value]>| -> String {
            arr.map(|a| a.iter().map(|v| v.as_i32().to_string()).collect::<Vec<_>>().join(","))
                .unwrap_or_default()
        };
        let leader = get_i32(s, "LeaderId");
        let leader_disp = brokers
            .get(&leader)
            .map(|a| format!("{leader} ({a})"))
            .unwrap_or_else(|| leader.to_string());
        println!(
            "{:<6} {:<24} {:<14} {}",
            get_i32(s, "PartitionIndex"),
            leader_disp,
            fmt(s.get_array("ReplicaNodes")),
            fmt(s.get_array("IsrNodes")),
        );
    }
    Ok(())
}

async fn topics_create(
    c: &mut Client,
    name: &str,
    partitions: i32,
    rf: i16,
    configs: &[(String, String)],
) -> Result<()> {
    let cfg_vals: Vec<Value> = configs
        .iter()
        .map(|(k, v)| s([("Name", Value::str(k.clone())), ("Value", Value::str(v.clone()))]))
        .collect();
    let resp = c.call(key::CREATE_TOPICS, V_CREATE_TOPICS, s([
        ("Topics", Value::Array(vec![s([
            ("Name", Value::str(name)),
            ("NumPartitions", Value::I32(partitions)),
            ("ReplicationFactor", Value::I16(rf)),
            ("Assignments", Value::Array(vec![])),
            ("Configs", Value::Array(cfg_vals)),
        ])])),
        ("TimeoutMs", Value::I32(30_000)),
        ("ValidateOnly", Value::Bool(false)),
    ])).await?;
    dump(&resp);
    let ts = resp.get_array("Topics").ok_or("create: no Topics")?;
    let t = struct_of(ts, "create")?;
    check(t, "create")?;
    let msg = get_str(t, "ErrorMessage");
    if msg.is_empty() {
        println!("{name}: created (partitions={partitions}, replication_factor={rf})");
    } else {
        println!("{name}: {msg}");
    }
    Ok(())
}

async fn topics_delete(c: &mut Client, name: &str) -> Result<()> {
    // v6（flexible）：请求 Topics[] {Name, TopicId}，响应 Responses 带逐
    // topic 错误码（v0-5 响应为空，错误不可见）
    let resp = c.call(key::DELETE_TOPICS, V_DELETE_TOPICS, s([
        ("Topics", Value::Array(vec![s([
            ("Name", Value::str(name)),
            ("TopicId", Value::Uuid(0)),
        ])])),
    ])).await?;
    dump(&resp);
    let empty: &[Value] = &[];
    let ts = resp.get_array("Responses").unwrap_or(empty);
    let Some(t) = ts.first() else {
        println!("{name}: delete request accepted (no per-topic response)");
        return Ok(());
    };
    let Value::Struct(t) = t else { return Err("delete: bad response".into()) };
    check(t, "delete")?;
    println!("{name}: deleted");
    Ok(())
}

async fn groups_list(c: &mut Client) -> Result<()> {
    // v4：GroupState 字段自 v4 起才有（v0-3 响应无状态列）；请求 StatesFilter
    // = Null → 全量
    let resp = c.call(key::LIST_GROUPS, V_LIST_GROUPS, s([
        ("StatesFilter", Value::Null),
    ])).await?;
    dump(&resp);
    let empty: &[Value] = &[];
    let gs = resp.get_array("Groups").unwrap_or(empty);
    println!("{:<32} {:<14} {}", "GROUP", "PROTOCOL_TYPE", "STATE");
    for g in gs {
        let Value::Struct(s) = g else { continue };
        check(s, "list groups")?;
        println!(
            "{:<32} {:<14} {}",
            get_str(s, "GroupId"),
            get_str(s, "ProtocolType"),
            get_str(s, "GroupState"),
        );
    }
    Ok(())
}

async fn groups_describe(c: &mut Client, group: &str) -> Result<()> {
    // Groups 是扁平 string 数组（[]string，非结构体数组）
    let resp = c.call(key::DESCRIBE_GROUPS, V_DESCRIBE_GROUPS, s([
        ("Groups", Value::Array(vec![Value::str(group)])),
    ])).await?;
    dump(&resp);
    let empty: &[Value] = &[];
    let gs = resp.get_array("Groups").unwrap_or(empty);
    let g = struct_of(gs, "describe group")?;
    let code = get_i16(g, "ErrorCode");
    if code == 69 {
        return Err(format!("group {group}: GROUP_ID_NOT_FOUND"));
    }
    check(g, "describe group")?;
    println!("group:  {}", get_str(g, "GroupId"));
    println!("state:  {}", get_str(g, "GroupState"));
    println!("type:   {}", get_str(g, "ProtocolType"));
    println!("proto:  {}", get_str(g, "ProtocolData"));
    println!("{:<36} {:<20} {}", "MEMBER", "CLIENT", "HOST");
    for m in g.get_array("Members").unwrap_or(empty) {
        let Value::Struct(s) = m else { continue };
        println!(
            "{:<36} {:<20} {}",
            get_str(s, "MemberId"),
            get_str(s, "ClientId"),
            get_str(s, "ClientHost"),
        );
    }
    Ok(())
}

async fn list_offsets_latest(c: &mut Client, topic: &str, partitions: &[i32]) -> Result<Vec<(i32, i64)>> {
    let resp = c.call(key::LIST_OFFSETS, V_LIST_OFFSETS, s([
        ("ReplicaId", Value::I32(-1)),
        ("Topics", Value::Array(vec![s([
            ("Name", Value::str(topic)),
            ("Partitions", Value::Array(
                partitions.iter().map(|i| s([
                    ("PartitionIndex", Value::I32(*i)),
                    ("Timestamp", Value::I64(-1)),
                ])).collect(),
            )),
        ])])),
    ])).await?;
    let empty: &[Value] = &[];
    let mut out = Vec::new();
    for t in resp.get_array("Topics").unwrap_or(empty) {
        let Value::Struct(t) = t else { continue };
        for p in t.get_array("Partitions").unwrap_or(empty) {
            let Value::Struct(s) = p else { continue };
            check(s, "list offsets")?;
            out.push((get_i32(s, "PartitionIndex"), get_i64(s, "Offset")));
        }
    }
    Ok(out)
}

async fn groups_offsets(c: &mut Client, group: &str) -> Result<()> {
    // OffsetFetch v5：Topics=null → 该组全部已提交 topic
    let resp = c.call(key::OFFSET_FETCH, V_OFFSET_FETCH, s([
        ("GroupId", Value::str(group)),
        ("Topics", Value::Null),
    ])).await?;
    dump(&resp);
    let empty: &[Value] = &[];
    // 收集 (topic, partition → committed)
    let mut per_topic: Vec<(String, Vec<(i32, i64)>)> = Vec::new();
    for t in resp.get_array("Topics").unwrap_or(empty) {
        let Value::Struct(t) = t else { continue };
        let Some(v) = t.get("Name").or_else(|| t.get("Topic")) else { continue };
        let name = v.as_str().to_string();
        let mut parts: Vec<(i32, i64)> = Vec::new();
        for p in t.get_array("Partitions").unwrap_or(empty) {
            let Value::Struct(s) = p else { continue };
            check(s, "offset fetch")?;
            parts.push((get_i32(s, "PartitionIndex"), get_i64(s, "CommittedOffset")));
        }
        if !parts.is_empty() {
            per_topic.push((name, parts));
        }
    }
    println!("{:<32} {:>5} {:>12} {:>12} {:>10}", "TOPIC", "PART", "COMMITTED", "END", "LAG");
    let mut total_lag = 0i64;
    let mut any = false;
    for (name, parts) in &mut per_topic {
        let idxs: Vec<i32> = parts.iter().map(|(i, _)| *i).collect();
        let ends = list_offsets_latest(c, name, &idxs).await?;
        parts.sort();
        for (idx, committed) in parts.iter() {
            let end = ends.iter().find(|(i, _)| i == idx).map(|(_, v)| *v).unwrap_or(-1);
            let lag = if end >= 0 && *committed >= 0 { end - committed } else { -1 };
            if lag > 0 {
                total_lag += lag;
            }
            println!("{:<32} {:>5} {:>12} {:>12} {:>10}", name, idx, committed, end, lag);
            any = true;
        }
    }
    if !any {
        println!("(no committed offsets for group {group})");
    } else {
        println!("\ntotal lag: {total_lag}");
    }
    Ok(())
}

// ---------- 参数解析 ----------

fn usage() -> ! {
    eprintln!(
        "basalt-cli — basalt 运维 CLI\n\n\
        USAGE:\n  \
        basalt-cli [--bootstrap HOST:PORT] <command>\n\n\
        COMMANDS:\n  \
        topics list\n  \
        topics describe NAME\n  \
        topics create NAME [--partitions N] [--replication N] [--config K=V]...\n  \
        topics delete NAME\n  \
        groups list\n  \
        groups describe GROUP\n  \
        groups offsets GROUP\n\n\
        ENV:\n  \
        BASALT_BOOTSTRAP    缺省 bootstrap 地址（默认 127.0.0.1:9092）\n  \
        BASALT_CLI_DEBUG=1  打印原始协议响应树"
    );
    std::process::exit(2);
}

struct Args {
    bootstrap: String,
    positional: Vec<String>,
    partitions: i32,
    replication: i16,
    configs: Vec<(String, String)>,
}

fn parse_args() -> Args {
    let mut bootstrap = std::env::var("BASALT_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let mut positional = Vec::new();
    let mut partitions: Option<i32> = None;
    let mut replication: Option<i16> = None;
    let mut configs: Vec<(String, String)> = Vec::new();
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < argv.len() {
        let a = argv[i].as_str();
        let next = || -> String { argv.get(i + 1).cloned().unwrap_or_default() };
        match a {
            "--bootstrap" if i + 1 < argv.len() => {
                bootstrap = next();
                i += 1;
            }
            "--partitions" if i + 1 < argv.len() => {
                partitions = Some(parse_num(&next(), "--partitions"));
                i += 1;
            }
            "--replication" if i + 1 < argv.len() => {
                replication = Some(parse_num(&next(), "--replication"));
                i += 1;
            }
            "--config" if i + 1 < argv.len() => {
                let kv = next();
                if let Some((k, v)) = kv.split_once('=') {
                    configs.push((k.to_string(), v.to_string()));
                } else {
                    eprintln!("error: --config expects K=V, got {kv:?}");
                    std::process::exit(2);
                }
                i += 1;
            }
            _ if a.starts_with('-') => usage(),
            _ => positional.push(a.to_string()),
        }
        i += 1;
    }
    if positional.is_empty() {
        usage();
    }
    Args {
        bootstrap,
        positional,
        partitions: partitions.unwrap_or(1),
        replication: replication.unwrap_or(1),
        configs,
    }
}

fn parse_num<T: std::str::FromStr>(s: &str, what: &str) -> T {
    match s.parse() {
        Ok(v) => v,
        Err(_) => {
            eprintln!("error: invalid {what}: {s:?}");
            std::process::exit(2);
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut args = parse_args();
    let positional = std::mem::take(&mut args.positional);
    let mut pos = positional.into_iter();
    let domain = pos.next().unwrap_or_else(|| usage());
    let action = pos.next().unwrap_or_else(|| usage());
    let rest: Vec<String> = pos.collect();

    if let Err(e) = run_command(args, &domain, &action, &rest).await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn run_command(mut args: Args, domain: &str, action: &str, rest: &[String]) -> Result<()> {
    match (domain, action, rest) {
        ("topics", "list", []) => {
            let mut c = Client::connect(&args.bootstrap).await?;
            topics_list(&mut c).await
        }
        ("topics", "describe", [name]) => {
            let mut c = Client::connect(&args.bootstrap).await?;
            topics_describe(&mut c, name).await
        }
        ("topics", "create", [name]) => {
            let mut c = Client::connect(&args.bootstrap).await?;
            let configs = std::mem::take(&mut args.configs);
            topics_create(&mut c, name, args.partitions, args.replication, &configs).await
        }
        ("topics", "delete", [name]) => {
            let mut c = Client::connect(&args.bootstrap).await?;
            topics_delete(&mut c, name).await
        }
        ("groups", "list", []) => {
            let mut c = Client::connect(&args.bootstrap).await?;
            groups_list(&mut c).await
        }
        ("groups", "describe", [g]) => {
            let mut c = Client::connect(&args.bootstrap).await?;
            groups_describe(&mut c, g).await
        }
        ("groups", "offsets", [g]) => {
            let mut c = Client::connect(&args.bootstrap).await?;
            groups_offsets(&mut c, g).await
        }
        _ => Err("unknown command (see --help)".into()),
    }
}
