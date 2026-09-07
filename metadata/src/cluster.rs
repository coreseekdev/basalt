//! 集群元数据（多节点 POC，ADR-10 形态）：
//! - 控制器（最小 node id）独占 ClusterState，变更为 record 追加日志（恢复=重放）；
//! - broker 持快照副本（经内部 RPC 同步），快照可编解码；
//! - leader 指派 / epoch fencing：epoch 单调，failover = epoch+1 + 副本轮转。

use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub struct ReplicaAssignment {
    pub topic: String,
    pub partition: i32,
    pub replicas: Vec<i32>,
    pub leader: i32,
    pub epoch: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BrokerInfo {
    pub node_id: i32,
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ClusterState {
    pub brokers: HashMap<i32, BrokerInfo>,
    pub assignments: Vec<ReplicaAssignment>,
    pub version: u64,
}

#[derive(Debug, Clone)]
pub enum ClusterRecord {
    RegisterBroker(BrokerInfo),
    CreateTopic { name: String, partitions: i32, rf: i32 },
    LeaderChange { topic: String, partition: i32, leader: i32, epoch: i32 },
}

impl ClusterState {
    pub fn apply(&mut self, rec: &ClusterRecord) {
        match rec {
            ClusterRecord::RegisterBroker(b) => {
                self.brokers.insert(b.node_id, b.clone());
            }
            ClusterRecord::CreateTopic { name, partitions, rf } => {
                let mut brokers: Vec<i32> = self.brokers.keys().copied().collect();
                brokers.sort_unstable();
                if brokers.is_empty() {
                    brokers = vec![0];
                }
                let rf = (*rf).max(1).min(brokers.len() as i32);
                for p in 0..*partitions {
                    let replicas: Vec<i32> = brokers
                        .iter()
                        .cycle()
                        .skip(p as usize)
                        .take(rf as usize)
                        .copied()
                        .collect();
                    let leader = replicas[0];
                    self.assignments.push(ReplicaAssignment {
                        topic: name.clone(),
                        partition: p,
                        replicas: replicas.clone(),
                        leader,
                        epoch: 0,
                    });
                }
            }
            ClusterRecord::LeaderChange { topic, partition, leader, epoch } => {
                if let Some(a) = self
                    .assignments
                    .iter_mut()
                    .find(|a| a.topic == *topic && a.partition == *partition)
                {
                    a.leader = *leader;
                    a.epoch = *epoch;
                }
            }
        }
        self.version += 1;
    }

    pub fn assignment(&self, topic: &str, partition: i32) -> Option<&ReplicaAssignment> {
        self.assignments
            .iter()
            .find(|a| a.topic == topic && a.partition == partition)
    }

    // ---------- 快照编解码（BE + len 前缀 string；无外部依赖） ----------

    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&self.version.to_be_bytes());
        b.extend_from_slice(&(self.brokers.len() as u32).to_be_bytes());
        for br in self.brokers.values() {
            b.extend_from_slice(&br.node_id.to_be_bytes());
            put_str(&mut b, &br.host);
            b.extend_from_slice(&(br.port as i32).to_be_bytes());
        }
        b.extend_from_slice(&(self.assignments.len() as u32).to_be_bytes());
        for a in &self.assignments {
            put_str(&mut b, &a.topic);
            b.extend_from_slice(&a.partition.to_be_bytes());
            b.extend_from_slice(&(a.replicas.len() as u32).to_be_bytes());
            for r in &a.replicas {
                b.extend_from_slice(&r.to_be_bytes());
            }
            b.extend_from_slice(&a.leader.to_be_bytes());
            b.extend_from_slice(&a.epoch.to_be_bytes());
        }
        b
    }

    pub fn decode(data: &[u8]) -> Option<ClusterState> {
        struct R<'a> {
            b: &'a [u8],
            p: usize,
        }
        impl<'a> R<'a> {
            fn g16(&mut self) -> i16 {
                let v = i16::from_be_bytes(self.b[self.p..self.p + 2].try_into().unwrap());
                self.p += 2;
                v
            }
            fn g32(&mut self) -> i32 {
                let v = i32::from_be_bytes(self.b[self.p..self.p + 4].try_into().unwrap());
                self.p += 4;
                v
            }
            fn g64(&mut self) -> u64 {
                let v = u64::from_be_bytes(self.b[self.p..self.p + 8].try_into().unwrap());
                self.p += 8;
                v
            }
            fn gstr(&mut self) -> String {
                let n = self.g16();
                let s = String::from_utf8_lossy(&self.b[self.p..self.p + n as usize]).into_owned();
                self.p += n as usize;
                s
            }
        }
        let mut r = R { b: data, p: 0 };
        let mut st = ClusterState::default();
        st.version = r.g64();
        let nb = r.g32();
        for _ in 0..nb {
            let node_id = r.g32();
            let host = r.gstr();
            let port = r.g32() as u16;
            st.brokers.insert(node_id, BrokerInfo { node_id, host, port });
        }
        let na = r.g32();
        for _ in 0..na {
            let topic = r.gstr();
            let partition = r.g32();
            let nr = r.g32();
            let mut replicas = Vec::with_capacity(nr as usize);
            for _ in 0..nr {
                replicas.push(r.g32());
            }
            let leader = r.g32();
            let epoch = r.g32();
            st.assignments.push(ReplicaAssignment { topic, partition, replicas, leader, epoch });
        }
        Some(st)
    }
}

fn put_str(b: &mut Vec<u8>, s: &str) {
    b.extend_from_slice(&(s.len() as i16).to_be_bytes());
    b.extend_from_slice(s.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_and_snapshot_roundtrip() {
        let mut st = ClusterState::default();
        st.apply(&ClusterRecord::RegisterBroker(BrokerInfo { node_id: 0, host: "h0".into(), port: 9092 }));
        st.apply(&ClusterRecord::RegisterBroker(BrokerInfo { node_id: 1, host: "h1".into(), port: 9093 }));
        st.apply(&ClusterRecord::RegisterBroker(BrokerInfo { node_id: 2, host: "h2".into(), port: 9094 }));
        st.apply(&ClusterRecord::CreateTopic { name: "t".into(), partitions: 2, rf: 3 });
        assert_eq!(st.assignment("t", 0).unwrap().replicas.len(), 3);
        st.apply(&ClusterRecord::LeaderChange { topic: "t".into(), partition: 0, leader: 1, epoch: 1 });
        let enc = st.encode();
        let dec = ClusterState::decode(&enc).unwrap();
        assert_eq!(dec, st);
        let a = dec.assignment("t", 0).unwrap();
        assert_eq!((a.leader, a.epoch), (1, 1));
    }
}
