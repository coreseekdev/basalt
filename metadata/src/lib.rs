//! 控制面元数据状态机（sans-I/O；TASK.md T-M2.1/T-M2.2 的 M0 边界版）。
//!
//! KRaft 范式（ADR-2）：元数据变更是 record，apply 到内存 image；
//! broker 侧只持只读快照。M0 单机形态：TopicTable 由 metadata actor 独占，
//! 路由表经 watch 广播 owned 快照（无 Arc，见根 Cargo.toml 性能纪律）。

use std::collections::HashMap;

pub const TOPIC_ID_UNSET: u128 = 0;

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionMeta {
    pub index: i32,
    pub leader: i32,
    pub leader_epoch: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopicMeta {
    pub name: String,
    pub topic_id: u128,
    pub internal: bool,
    pub partitions: Vec<PartitionMeta>,
}

impl TopicMeta {
    pub fn partition(&self, index: i32) -> Option<&PartitionMeta> {
        self.partitions.iter().find(|p| p.index == index)
    }
}

#[derive(Debug, Default)]
pub struct TopicTable {
    topics: HashMap<String, TopicMeta>,
    by_id: HashMap<u128, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateError {
    AlreadyExists,
    InvalidPartitions,
    InvalidReplicationFactor,
    InvalidTopic,
}

impl TopicTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, name: &str) -> Option<&TopicMeta> {
        self.topics.get(name)
    }

    pub fn get_by_id(&self, id: u128) -> Option<&TopicMeta> {
        self.by_id.get(&id).and_then(|n| self.topics.get(n))
    }

    pub fn topics(&self) -> impl Iterator<Item = &TopicMeta> {
        self.topics.values()
    }

    /// 创建 topic：副本按 broker 列表轮转放置（POC：leader=首个副本）。
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        &mut self,
        name: &str,
        partitions: i32,
        replication_factor: i32,
        brokers: &[i32],
    ) -> Result<&TopicMeta, CreateError> {
        if name.is_empty() || name.len() > 249 || name == "." || name == ".." {
            return Err(CreateError::InvalidTopic);
        }
        if partitions <= 0 {
            return Err(CreateError::InvalidPartitions);
        }
        if replication_factor <= 0 || brokers.len() < replication_factor as usize {
            return Err(CreateError::InvalidReplicationFactor);
        }
        if self.topics.contains_key(name) {
            return Err(CreateError::AlreadyExists);
        }
        let topic_id = new_topic_id(name);
        let mut parts = Vec::with_capacity(partitions as usize);
        for i in 0..partitions {
            let mut assignment: Vec<i32> = brokers
                .iter()
                .cycle()
                .skip(i as usize)
                .take(replication_factor as usize)
                .copied()
                .collect();
            assignment.sort_unstable();
            let leader = assignment[0];
            parts.push(PartitionMeta {
                index: i,
                leader,
                leader_epoch: 0,
                replicas: assignment.clone(),
                isr: assignment,
            });
        }
        let meta = TopicMeta { name: name.to_string(), topic_id, internal: false, partitions: parts };
        self.by_id.insert(topic_id, meta.name.clone());
        self.topics.insert(name.to_string(), meta);
        Ok(self.topics.get(name).expect("just inserted"))
    }

    pub fn delete(&mut self, name: &str) -> bool {
        if let Some(t) = self.topics.remove(name) {
            self.by_id.remove(&t.topic_id);
            true
        } else {
            false
        }
    }

    /// leader 变更（failover 路径）：epoch 单调 +1。
    pub fn reassign_leader(&mut self, topic: &str, partition: i32, new_leader: i32) -> bool {
        let Some(t) = self.topics.get_mut(topic) else { return false };
        let Some(p) = t.partitions.iter_mut().find(|p| p.index == partition) else {
            return false;
        };
        if p.leader == new_leader {
            return false;
        }
        p.leader = new_leader;
        p.leader_epoch += 1;
        p.isr = p.replicas.clone();
        true
    }
}

/// 主题 id：非加密场景的确定性 128bit（名字哈希 + 计数盐，删除重建必然变化——
/// franz-go #676 防线：id 随重建刷新）。
fn new_topic_id(name: &str) -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    let mut h: u128 = 0xcbf29ce484222325;
    for b in name.bytes() {
        h ^= u128::from(b);
        h = h.wrapping_mul(0x100000001b3);
    }
    h ^ (nanos << 32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_round_robin_assignment() {
        let mut t = TopicTable::new();
        let m = t.create("orders", 3, 2, &[0, 1, 2]).unwrap();
        assert_eq!(m.partitions.len(), 3);
        for (i, p) in m.partitions.iter().enumerate() {
            assert_eq!(p.index, i as i32);
            assert_eq!(p.replicas.len(), 2);
            assert_eq!(p.leader, p.replicas[0]);
            assert_eq!(p.isr, p.replicas);
        }
        assert_ne!(m.topic_id, TOPIC_ID_UNSET);
    }

    #[test]
    fn create_errors() {
        let mut t = TopicTable::new();
        assert!(matches!(t.create("x", 0, 1, &[0]), Err(CreateError::InvalidPartitions)));
        assert!(matches!(t.create("x", 1, 3, &[0]), Err(CreateError::InvalidReplicationFactor)));
        t.create("x", 1, 1, &[0]).unwrap();
        assert!(matches!(t.create("x", 1, 1, &[0]), Err(CreateError::AlreadyExists)));
        assert!(matches!(t.create("", 1, 1, &[0]), Err(CreateError::InvalidTopic)));
    }

    #[test]
    fn delete_recreate_refreshes_id() {
        // franz-go #676 语义：删除重建后 topic id 必须刷新
        let mut t = TopicTable::new();
        let id1 = t.create("t", 1, 1, &[0]).unwrap().topic_id;
        assert!(t.delete("t"));
        let id2 = t.create("t", 1, 1, &[0]).unwrap().topic_id;
        assert_ne!(id1, id2);
        assert!(t.get_by_id(id1).is_none());
        assert!(t.get_by_id(id2).is_some());
    }

    #[test]
    fn reassign_leader_bumps_epoch() {
        let mut t = TopicTable::new();
        t.create("t", 1, 2, &[0, 1]).unwrap();
        assert!(t.reassign_leader("t", 0, 1));
        let p = t.get("t").unwrap().partition(0).unwrap();
        assert_eq!(p.leader, 1);
        assert_eq!(p.leader_epoch, 1);
        assert!(!t.reassign_leader("t", 0, 1), "same leader = no-op");
    }
}
