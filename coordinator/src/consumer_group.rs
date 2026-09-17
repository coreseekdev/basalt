//! KIP-848 consumer 组状态机（T-M3.3 块 a，ADR-19 §2/§3）：心跳单循环、
//! 服务端 Range 分配、member-epoch fence。纯同步结构体（无 I/O）——
//! actor 壳与协议面在块 b 接线；确定性单测直驱。

use std::collections::{BTreeMap, HashMap};

/// 单成员视图：epoch 从 1 起（0 = 注册中）；owned 为成员上报的当前持有。
#[derive(Debug, Clone)]
pub struct MemberState {
    pub member_id: String,
    pub epoch: i32,
    pub subscribed: Vec<String>,
    pub assignment: BTreeMap<String, Vec<i32>>,
}

#[derive(Debug, Default)]
pub struct ConsumerGroup {
    /// topic → 分区数（服务端元数据面，块 b 经 meta 接线更新）
    pub partition_counts: HashMap<String, i32>,
    members: BTreeMap<String, MemberState>,
    /// 成员集+订阅集签名：变化才重算 target 并全员 epoch+1（幂等续租不 bump）
    subsig: u64,
    next_ordinal: u64,
    /// 重算次数（Describe 的 Group/AssignmentEpoch 同源——membership 变化
    /// 即重算，无栅栏相位）
    rebalances: u64,
}

/// ConsumerGroupDescribe 的组快照（块 b）。
#[derive(Debug, Clone)]
pub struct GroupDescribe {
    pub state: String,
    pub group_epoch: i32,
    pub assignment_epoch: i32,
    pub assignor: String,
    pub members: Vec<MemberDescribe>,
}

#[derive(Debug, Clone)]
pub struct MemberDescribe {
    pub member_id: String,
    pub member_epoch: i32,
    pub subscribed: Vec<String>,
    pub assignment: BTreeMap<String, Vec<i32>>,
}

/// 心跳结果：成员态 + 当前 target assignment。
/// fenced = member-epoch 与服务端不符（C9 令牌面下沉），fenced 心跳不带
/// assignment 且不产生任何状态变化。fenced 二分（协议面错误码映射，
/// ADR-19 §6③）：unknown_member = 未知成员/僵尸（UNKNOWN_MEMBER_ID），
/// 否则已知成员 epoch 陈旧（FENCED_MEMBER_EPOCH）。
#[derive(Debug, Default)]
pub struct HeartbeatResult {
    pub member_id: String,
    pub member_epoch: i32,
    pub assignment: BTreeMap<String, Vec<i32>>,
    pub fenced: bool,
    pub unknown_member: bool,
}

impl ConsumerGroup {
    pub fn heartbeat(
        &mut self,
        member_id: &str,
        member_epoch: i32,
        subscribed: Vec<String>,
        _owned: BTreeMap<String, Vec<i32>>,
    ) -> HeartbeatResult {
        // 离开：epoch = -1 → 注销 + 重算（剩余成员接管）
        if member_epoch < 0 {
            self.members.remove(member_id);
            self.rebalance();
            return HeartbeatResult { member_id: member_id.into(), member_epoch: -1, ..Default::default() };
        }
        // member-epoch fence：已知成员任何不符（含归零）一律拒；未知成员带
        // 非零 epoch = 已被清除的僵尸。fenced 心跳零状态变化。
        if let Some(m) = self.members.get(member_id) {
            if m.epoch != member_epoch {
                return HeartbeatResult {
                    member_id: member_id.into(),
                    member_epoch: m.epoch,
                    fenced: true,
                    ..Default::default()
                };
            }
        } else if !member_id.is_empty() && member_epoch != 0 {
            return HeartbeatResult { fenced: true, unknown_member: true, ..Default::default() };
        }

        // 注册（空 MemberId）/ 续租
        let id = if member_id.is_empty() {
            self.next_ordinal += 1;
            format!("consumer-{}", self.next_ordinal)
        } else {
            member_id.to_string()
        };
        match self.members.get_mut(&id) {
            Some(m) => m.subscribed = subscribed.clone(),
            None => {
                self.members.insert(id.clone(), MemberState {
                    member_id: id.clone(), epoch: 0, subscribed: subscribed.clone(), assignment: BTreeMap::new(),
                });
            }
        }

        // 成员集/订阅集变化 → 重算 target 并全员 epoch+1；否则幂等续租
        let subsig = self.signature();
        if subsig != self.subsig {
            self.subsig = subsig;
            self.rebalance();
        } else if self.members[&id].epoch == 0 {
            // 新成员注册后的首算（rebalance 已在签名变化时跑过）
            self.rebalance();
        }
        let m = &self.members[&id];
        HeartbeatResult {
            member_id: id,
            member_epoch: m.epoch,
            assignment: m.assignment.clone(),
            fenced: false,
            unknown_member: false,
        }
    }

    /// DescribeConsumerGroup 快照（ConsumerGroupDescribe v0）。空组成员集
    /// = Empty（组存在但无成员）；Assignor 固定 range（§3 POC 边界）。
    pub fn describe(&self) -> GroupDescribe {
        GroupDescribe {
            state: if self.members.is_empty() { "Empty" } else { "Stable" }.to_string(),
            group_epoch: self.rebalances as i32,
            assignment_epoch: self.rebalances as i32,
            assignor: "range".to_string(),
            members: self
                .members
                .values()
                .map(|m| MemberDescribe {
                    member_id: m.member_id.clone(),
                    member_epoch: m.epoch,
                    subscribed: m.subscribed.clone(),
                    assignment: m.assignment.clone(),
                })
                .collect(),
        }
    }

    /// 成员集 + 订阅集签名（BTreeMap 稳定序）。
    fn signature(&self) -> u64 {
        let mut h: u64 = 1469598103934665603;
        for (id, m) in &self.members {
            for b in id.bytes().chain(m.subscribed.iter().flat_map(|s| s.bytes())) {
                h = (h ^ b as u64).wrapping_mul(0x100000001b3);
            }
            h = (h ^ 0xff).wrapping_mul(0x100000001b3);
        }
        h
    }

    /// 服务端 Range 分配（java RangeAssignor 对齐）：订阅并集内每个 topic，
    /// 分区按成员序连续切块（余数给前面的成员）；未订阅的 topic 不分配。
    fn rebalance(&mut self) {
        self.rebalances += 1;
        let ids: Vec<String> = self.members.keys().cloned().collect();
        for (id, m) in self.members.iter_mut() {
            let mut target = BTreeMap::new();
            let subscribed: std::collections::BTreeSet<&String> = m.subscribed.iter().collect();
            for topic in &subscribed {
                let total = *self.partition_counts.get(*topic).unwrap_or(&0) as usize;
                if total == 0 || ids.is_empty() {
                    continue;
                }
                let idx = ids.iter().position(|x| x == id).unwrap_or(0);
                let base = total / ids.len();
                let rem = total % ids.len();
                let start = idx * base + idx.min(rem);
                let count = base + if idx < rem { 1 } else { 0 };
                target.insert((*topic).clone(), (start as i32..(start + count) as i32).collect());
            }
            m.assignment = target;
            if m.epoch == 0 {
                m.epoch = 1;
            } else {
                m.epoch += 1;
            }
        }
    }
}

#[cfg(test)]
mod consumer_group_tests {
    //! T-M3.3 块 a：Range 分配（java RangeAssignor 对齐）/ member-epoch
    //! fence / 幂等续租不 bump / 成员离开接管 / 退订收回。

    use super::*;

    fn hb(
        cg: &mut ConsumerGroup,
        id: &str,
        epoch: i32,
        sub: Vec<&str>,
        owned: Vec<(&str, Vec<i32>)>,
    ) -> HeartbeatResult {
        cg.heartbeat(
            id,
            epoch,
            sub.into_iter().map(String::from).collect(),
            owned.into_iter().map(|(t, ps)| (t.to_string(), ps)).collect(),
        )
    }

    /// Range 分配（java RangeAssignor 对齐）：首成员先独占，b 加入重算后
    /// 连续切块（余数给前位成员）——[0,1] / [2,3]。
    #[test]
    fn range_assignment_java_aligned() {
        let mut cg = ConsumerGroup::default();
        cg.partition_counts.insert("t".into(), 4);
        let a = hb(&mut cg, "", 0, vec!["t"], vec![]);
        assert_eq!(a.assignment[&"t".to_string()], vec![0, 1, 2, 3], "首成员先独占（无栅栏）");
        let b = hb(&mut cg, "", 0, vec!["t"], vec![]);
        assert_eq!(a.member_id, "consumer-1");
        assert_eq!(b.member_id, "consumer-2");
        // b 加入后 a 旧 epoch 被 fence（应答携带服务端当前 epoch）→ 重同步
        let af = hb(&mut cg, &a.member_id, a.member_epoch, vec!["t"], vec![("t", vec![0, 1, 2, 3])]);
        assert!(af.fenced, "成员集变化后旧 epoch 必须 fence");
        let a2 = hb(&mut cg, &a.member_id, af.member_epoch, vec!["t"], vec![("t", vec![0, 1, 2, 3])]);
        assert_eq!(a2.assignment[&"t".to_string()], vec![0, 1], "连续切块，余数给前位");
        let b2 = hb(&mut cg, &b.member_id, b.member_epoch, vec!["t"], vec![("t", vec![])]);
        assert_eq!(b2.assignment[&"t".to_string()], vec![2, 3]);
    }

    /// 幂等续租不 bump epoch；成员加入 → a 的旧 epoch 被 fence（应答携带
    /// 服务端当前 epoch，客户端据此重同步）→ 重算后各持 1；离开（-1）→
    /// 剩余成员接管全部。
    #[test]
    fn join_bumps_epoch_and_leave_yields_all() {
        let mut cg = ConsumerGroup::default();
        cg.partition_counts.insert("t".into(), 2);
        let a0 = hb(&mut cg, "", 0, vec!["t"], vec![]);
        let a1 = hb(&mut cg, &a0.member_id, a0.member_epoch, vec!["t"], vec![("t", vec![0, 1])]);
        assert_eq!(a1.member_epoch, a0.member_epoch, "幂等续租不得 bump");
        assert_eq!(a1.assignment[&"t".to_string()], vec![0, 1]);

        let b0 = hb(&mut cg, "", 0, vec!["t"], vec![]);
        assert!(b0.member_epoch >= 1);
        // b 加入后 a 的旧 epoch 心跳被 fence，应答携带服务端当前 epoch
        let fenced = hb(&mut cg, &a0.member_id, a0.member_epoch, vec!["t"], vec![("t", vec![0])]);
        assert!(fenced.fenced, "成员集变化后旧 epoch 必须 fence");
        let a_epoch = fenced.member_epoch;
        let a2 = hb(&mut cg, &a0.member_id, a_epoch, vec!["t"], vec![("t", vec![0])]);
        assert_eq!(a2.member_epoch, a_epoch, "重同步后续租");
        assert_eq!(a2.assignment[&"t".to_string()].len(), 1, "2 成员 2 分区各持 1");

        hb(&mut cg, &b0.member_id, -1, vec![], vec![]);
        let leave = hb(&mut cg, &a0.member_id, a2.member_epoch, vec!["t"], vec![("t", vec![0])]);
        assert!(leave.fenced, "b 离开触发重算 → a 旧 epoch 再次 fence（应答带新 epoch）");
        let a3 = hb(&mut cg, &a0.member_id, leave.member_epoch, vec!["t"], vec![("t", vec![0])]);
        assert_eq!(a3.assignment[&"t".to_string()], vec![0, 1], "离开后接管全部");
    }

    /// member-epoch fence：落后/归零/未知僵尸心跳一律被拒且零状态变化。
    #[test]
    fn stale_epoch_heartbeat_fenced() {
        let mut cg = ConsumerGroup::default();
        cg.partition_counts.insert("t".into(), 1);
        let a = hb(&mut cg, "", 0, vec!["t"], vec![]);
        hb(&mut cg, "", 0, vec!["t"], vec![]); // 第二成员加入 → a epoch bump
        let stale = hb(&mut cg, &a.member_id, a.member_epoch.saturating_sub(1), vec!["t"], vec![]);
        assert!(stale.fenced, "落后 epoch 必须 fence");
        assert_eq!(stale.assignment.len(), 0, "fenced 心跳不带 assignment");
        let zero = hb(&mut cg, &a.member_id, 0, vec!["t"], vec![]);
        assert!(zero.fenced, "已知成员归零 epoch = 陈旧心跳，不得落注册路径");
        assert!(!zero.unknown_member, "已知成员 fence = 82 面（非未知成员）");
        let zombie = hb(&mut cg, "consumer-99", 7, vec!["t"], vec![]);
        assert!(zombie.fenced, "未知成员带非零 epoch = 僵尸");
        assert!(zombie.unknown_member, "僵尸必须标记 unknown_member（协议面映射 25）");
    }

    /// Describe 快照（块 b ConsumerGroupDescribe 数据面）：成员明细/订阅/
    /// 分配与 epoch 同源；空组 = Empty；不存在组由 actor 层回 None。
    #[test]
    fn describe_reflects_membership() {
        let mut cg = ConsumerGroup::default();
        cg.partition_counts.insert("t".into(), 2);
        let d0 = cg.describe();
        assert_eq!(d0.state, "Empty");
        let a = hb(&mut cg, "", 0, vec!["t"], vec![]);
        hb(&mut cg, &a.member_id, a.member_epoch, vec!["t"], vec![("t", vec![0, 1])]);
        let d = cg.describe();
        assert_eq!(d.state, "Stable");
        assert_eq!(d.assignor, "range");
        assert!(d.group_epoch >= 1 && d.assignment_epoch == d.group_epoch, "epoch 同源");
        assert_eq!(d.members.len(), 1);
        let m = &d.members[0];
        assert_eq!(m.member_id, a.member_id);
        assert_eq!(m.subscribed, vec!["t".to_string()]);
        assert_eq!(m.assignment[&"t".to_string()], vec![0, 1]);
    }

    /// 退订：订阅集变化 → 重算后该 topic 不在 target。
    #[test]
    fn unsubscribe_releases_partitions() {
        let mut cg = ConsumerGroup::default();
        cg.partition_counts.insert("t".into(), 2);
        let a = hb(&mut cg, "", 0, vec!["t"], vec![]);
        let u = hb(&mut cg, &a.member_id, a.member_epoch, vec![], vec![]);
        assert!(!u.assignment.contains_key("t"), "退订后收回分配");
    }
}

// ---------- 多组管理 actor（块 b 协议面接线用） ----------

pub struct CGHeartbeat {
    pub group: String,
    pub member_id: String,
    pub member_epoch: i32,
    pub subscribed: Vec<String>,
    pub owned: BTreeMap<String, Vec<i32>>,
    /// 订阅 topic 的分区数快照（handler 经 meta 查询后携带）
    pub counts: Vec<(String, i32)>,
    pub reply: tokio::sync::oneshot::Sender<HeartbeatResult>,
}

pub enum CGCmd {
    Heartbeat(CGHeartbeat),
    /// ConsumerGroupDescribe：组不存在回 None（协议面映射 GROUP_ID_NOT_FOUND）。
    Describe {
        group: String,
        reply: tokio::sync::oneshot::Sender<Option<GroupDescribe>>,
    },
}

/// 每节点一个（组协调器 POC 全节点，FindCoordinator Type=0 回自身——与
/// classic 同拓扑）。
pub struct ConsumerGroups {
    groups: HashMap<String, ConsumerGroup>,
}

impl ConsumerGroups {
    pub fn spawn() -> tokio::sync::mpsc::Sender<CGCmd> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(256);
        tokio::spawn(async move {
            let mut groups: HashMap<String, ConsumerGroup> = HashMap::new();
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    CGCmd::Heartbeat(hb) => {
                        let cg = groups.entry(hb.group.clone()).or_default();
                        for (t, c) in hb.counts {
                            cg.partition_counts.insert(t, c);
                        }
                        let res = cg.heartbeat(
                            &hb.member_id,
                            hb.member_epoch,
                            hb.subscribed,
                            hb.owned,
                        );
                        let _ = hb.reply.send(res);
                    }
                    CGCmd::Describe { group, reply } => {
                        let _ = reply.send(groups.get(&group).map(|cg| cg.describe()));
                    }
                }
            }
        });
        tx
    }
}
