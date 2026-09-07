//! 协议注册表：构建期内嵌的 203 个 JSON → 启动时一次性解析编译。
//!
//! 全局只读（OnceLock），无锁访问；进程内唯一实例。

use crate::api::key;
use crate::error::{ProtocolError, Result};
use crate::plan::Plan;
use crate::schema::MessageSpec;
use std::collections::HashMap;
use std::sync::OnceLock;

include!(concat!(env!("OUT_DIR"), "/definitions.rs"));

#[derive(Debug)]
pub struct ApiEntry {
    pub api_key: i16,
    pub name: String,
    pub valid: (i16, i16),
    pub stable_max: i16,
    pub flexible_from: Option<i16>,
    pub request: Plan,
    pub response: Plan,
}

#[derive(Debug)]
pub struct Registry {
    by_key: HashMap<i16, ApiEntry>,
    headers: HashMap<String, Plan>,
}

impl Registry {
    pub fn global() -> &'static Registry {
        static REG: OnceLock<Registry> = OnceLock::new();
        REG.get_or_init(|| Registry::build().expect("embedded protocol definitions must compile"))
    }

    pub fn build() -> Result<Registry> {
        let mut specs: HashMap<String, MessageSpec> = HashMap::new();
        for (name, json) in DEFINITIONS {
            let spec = MessageSpec::parse(json).map_err(|e| {
                ProtocolError::Schema(format!("definition {name}: {e}"))
            })?;
            specs.insert((*name).to_string(), spec);
        }

        let mut by_key: HashMap<i16, ApiEntry> = HashMap::new();
        for spec in specs.values() {
            if spec.msg_type != "request" || spec.api_key.is_none() {
                continue;
            }
            let api_key = spec.api_key.unwrap();
            let resp_name = spec.name.replace("Request", "Response");
            let Some(resp) = specs.get(&resp_name) else {
                continue; // 无对应响应的请求（内部 API），跳过
            };
            let request = Plan::compile(spec)?;
            let response = Plan::compile(resp)?;
            by_key.insert(
                api_key,
                ApiEntry {
                    api_key,
                    name: spec.name.trim_end_matches("Request").to_string(),
                    valid: (spec.valid.min, spec.valid.max),
                    stable_max: spec.stable_max().min(resp.stable_max()),
                    flexible_from: spec.flexible_from,
                    request,
                    response,
                },
            );
        }

        let mut headers = HashMap::new();
        for hname in ["RequestHeader", "ResponseHeader"] {
            if let Some(h) = specs.get(hname) {
                headers.insert(hname.to_string(), Plan::compile(h)?);
            }
        }

        tracing::debug!(apis = by_key.len(), "protocol registry compiled");
        Ok(Registry { by_key, headers })
    }

    pub fn api(&self, api_key: i16) -> Option<&ApiEntry> {
        self.by_key.get(&api_key)
    }

    pub fn header(&self, name: &str) -> Option<&Plan> {
        self.headers.get(name)
    }

    /// 我们宣告支持的版本区间（api.rs 声明 ∩ schema 稳定版本）。
    pub fn advertised(&self) -> Vec<(i16, i16, i16, &'static str)> {
        crate::api::supported_versions()
            .iter()
            .filter_map(|&(k, lo, hi)| {
                let e = self.by_key.get(&k)?;
                let name: &'static str = match k {
                    key::PRODUCE => "Produce",
                    key::FETCH => "Fetch",
                    key::LIST_OFFSETS => "ListOffsets",
                    key::METADATA => "Metadata",
                    key::OFFSET_COMMIT => "OffsetCommit",
                    key::OFFSET_FETCH => "OffsetFetch",
                    key::FIND_COORDINATOR => "FindCoordinator",
                    key::JOIN_GROUP => "JoinGroup",
                    key::HEARTBEAT => "Heartbeat",
                    key::LEAVE_GROUP => "LeaveGroup",
                    key::SYNC_GROUP => "SyncGroup",
                    key::DESCRIBE_GROUPS => "DescribeGroups",
                    key::LIST_GROUPS => "ListGroups",
                    key::API_VERSIONS => "ApiVersions",
                    key::CREATE_TOPICS => "CreateTopics",
                    key::DELETE_TOPICS => "DeleteTopics",
                    key::INIT_PRODUCER_ID => "InitProducerId",
                    key::OFFSET_FOR_LEADER_EPOCH => "OffsetForLeaderEpoch",
                    key::DELETE_RECORDS => "DeleteRecords",
                    key::DESCRIBE_CONFIGS => "DescribeConfigs",
                    key::ALTER_CONFIGS => "AlterConfigs",
                    key::DESCRIBE_CLUSTER => "DescribeCluster",
                    _ => "Unknown",
                };
                let hi = hi.min(e.stable_max);
                let lo = lo.max(e.valid.0);
                if lo > hi {
                    return None;
                }
                Some((k, lo, hi, name))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_compiles_all() {
        let reg = Registry::build().unwrap();
        assert!(reg.api(0).is_some(), "Produce");
        assert!(reg.api(1).is_some(), "Fetch");
        assert!(reg.api(3).is_some(), "Metadata");
        assert!(reg.api(18).is_some(), "ApiVersions");
        assert_eq!(reg.api(18).unwrap().flexible_from, Some(3));

        let adv = reg.advertised();
        assert!(adv.iter().any(|&(k, _, _, _)| k == 0));
        // Fetch 在 4.5 已删 v0-3，宣告区间须与 schema 相交合理
        let fetch = adv.iter().find(|&&(k, _, _, _)| k == 1).unwrap();
        assert!(fetch.1 >= 4);
    }
}

#[cfg(test)]
mod gating_tests {
    use crate::codec;
    use crate::value::{s, Value, Struct};
    use bytes::BytesMut;

    fn encode_metadata_v(version: i16) -> usize {
        let reg = crate::registry::Registry::global();
        let entry = reg.api(3).unwrap();
        let part = s([
            ("ErrorCode", Value::I16(0)),
            ("PartitionIndex", Value::I32(0)),
            ("LeaderId", Value::I32(0)),
            ("LeaderEpoch", Value::I32(0)),
            ("ReplicaNodes", Value::Array(vec![Value::I32(0)])),
            ("IsrNodes", Value::Array(vec![Value::I32(0)])),
            ("OfflineReplicas", Value::Array(vec![])),
        ]);
        let topic = s([
            ("ErrorCode", Value::I16(0)),
            ("Name", Value::str("t1")),
            ("TopicId", Value::Uuid(0)),
            ("IsInternal", Value::Bool(false)),
            ("Partitions", Value::Array(vec![part])),
            ("TopicAuthorizedOperations", Value::I32(-2147483648)),
        ]);
        let mut st = Struct::new();
        st.set("ThrottleTimeMs", Value::I32(0));
        st.set("Brokers", Value::Array(vec![s([
            ("NodeId", Value::I32(0)),
            ("Host", Value::str("localhost")),
            ("Port", Value::I32(9092)),
            ("Rack", Value::Null),
        ])]));
        st.set("ClusterId", Value::Null);
        st.set("ControllerId", Value::I32(0));
        st.set("Topics", Value::Array(vec![topic]));
        st.set("ClusterAuthorizedOperations", Value::I32(-2147483648));
        let mut out = BytesMut::new();
        let flex = version >= 9;
        codec::encode_struct_fields(&entry.response.fields, version, flex, &st, &mut out).unwrap();
        if version == 1 {
            let hexs: String = out.iter().map(|x| format!("{x:02x}")).collect();
            println!("v1hex={hexs}");
        }
        out.len()
    }

    #[test]
    fn version_gating_sizes() {
        let v1 = encode_metadata_v(1);
        let v7 = encode_metadata_v(7);
        println!("v1={v1} v7={v7}");
        // v7 相对 v1 增量：ClusterId(2) + ThrottleTimeMs(4) + LeaderEpoch(4) + OfflineReplicas(4)
        assert_eq!(v7 - v1, 14, "v7 adds ClusterId/Throttle/LeaderEpoch/OfflineReplicas");
    }
}
