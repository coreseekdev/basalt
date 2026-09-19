//! GroupStateStore trait（方案 B 块 b1）：组状态持久化的存储抽象面。
//!
//! 设计决策（TLA+ GroupStateHA 确认模型，b97b032）：
//! - append = durable（复用 produce 路径，acks=all = ISR 确认即持久化）
//! - 无独立 sync 步骤（TLA+ v1 violation 是建模 artifact，非协议缺陷）
//! - 状态重放 = read_from(0) 全量消费
//!
//! 实现：
//! - `MemoryStateStore`：测试/开发用内存假体
//! - `TopicStateStore`：生产实现，走分区 actor produce 路径（块 b2-b3）


/// 组状态变更消息（内部 topic 的 value 载荷）
#[derive(Debug, Clone, PartialEq)]
pub enum GroupStateMessage {
    /// offset commit：group 在 partition 上提交了 offset
    OffsetCommit {
        group: String,
        topic: String,
        partition: i32,
        offset: i64,
    },
    /// 成员加入（share groups / classic groups）
    MemberJoin { group: String, member_id: String },
    /// 成员离开
    MemberLeave { group: String, member_id: String },
    /// share group 确认（accept/release/reject）
    ShareAck {
        group: String,
        topic_id: u128,
        partition: i32,
        first_offset: i64,
        last_offset: i64,
        ack_type: i8, // 1=accept 2=release 3=reject
    },
}

/// 组状态持久化存储抽象（append = durable，同 produce acks=all 语义）
pub trait GroupStateStore: Send {
    /// 追加状态消息。返回 Ok 即持久化（acks=all 语义）。
    fn append(&mut self, msg: &GroupStateMessage) -> Result<u64, String>;

    /// 从指定位置读取所有后续消息（重放面）
    fn read_from(&self, pos: u64) -> Result<Vec<(u64, GroupStateMessage)>, String>;

    /// 当前 log 末尾位置
    fn position(&self) -> u64;
}

/// 内存假体（测试/开发）
#[derive(Default)]
pub struct MemoryStateStore {
    log: Vec<GroupStateMessage>,
}

impl GroupStateStore for MemoryStateStore {
    fn append(&mut self, msg: &GroupStateMessage) -> Result<u64, String> {
        self.log.push(msg.clone());
        Ok(self.log.len() as u64 - 1)
    }

    fn read_from(&self, pos: u64) -> Result<Vec<(u64, GroupStateMessage)>, String> {
        Ok(self
            .log
            .iter()
            .skip(pos as usize)
            .enumerate()
            .map(|(i, m)| ((pos as usize + i) as u64, m.clone()))
            .collect())
    }

    fn position(&self) -> u64 {
        self.log.len() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_store_roundtrip() {
        let mut store = MemoryStateStore::default();
        let msg = GroupStateMessage::OffsetCommit {
            group: "g1".into(),
            topic: "t1".into(),
            partition: 0,
            offset: 42,
        };
        let pos = store.append(&msg).unwrap();
        assert_eq!(pos, 0);
        assert_eq!(store.position(), 1);

        let read = store.read_from(0).unwrap();
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].0, 0);
        assert_eq!(read[0].1, msg);
    }

    #[test]
    fn memory_store_replay_from_middle() {
        let mut store = MemoryStateStore::default();
        for i in 0..5 {
            let msg = GroupStateMessage::MemberJoin {
                group: "g".into(),
                member_id: format!("m{}", i),
            };
            store.append(&msg).unwrap();
        }
        // 从 pos 2 重放：应得 3 条
        let read = store.read_from(2).unwrap();
        assert_eq!(read.len(), 3);
        assert_eq!(read[0].0, 2);
    }

    #[test]
    fn memory_store_empty_replay() {
        let store = MemoryStateStore::default();
        let read = store.read_from(0).unwrap();
        assert!(read.is_empty());
    }
}
