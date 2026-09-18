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

#[derive(Debug)]
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
    /// 组分配器（T-M3.4，ADR-20 §3）：组创建时首个非空 ServerAssignor
    /// 定名（first-wins）；"range"（per-topic 连续切块，默认）/"uniform"
    /// （全局份额 + movement-minimizing）
    assignor: String,
}

impl Default for ConsumerGroup {
    fn default() -> Self {
        Self {
            partition_counts: HashMap::new(),
            members: BTreeMap::new(),
            subsig: 0,
            next_ordinal: 0,
            rebalances: 0,
            assignor: "range".into(),
        }
    }
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
    /// `subscribed: None` = 订阅未变（KIP-848 心跳 keepalive：客户端对未变
    /// 字段置 null；franz-go consumer_group_848 的 topicsMatch 路径实证）。
    /// 新成员带 None 视为空订阅（无分配，等下个显式订阅）。
    pub fn heartbeat(
        &mut self,
        member_id: &str,
        member_epoch: i32,
        subscribed: Option<Vec<String>>,
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

        // 注册（空 MemberId = 服务端分配 / 非空 = 客户端生成，KIP-1082 v1
        // 语义）/ 续租。None 订阅 = keepalive 不动现有订阅。
        let id = if member_id.is_empty() {
            self.next_ordinal += 1;
            format!("consumer-{}", self.next_ordinal)
        } else {
            member_id.to_string()
        };
        match self.members.get_mut(&id) {
            Some(m) => {
                if let Some(subs) = subscribed.clone() {
                    m.subscribed = subs;
                }
            }
            None => {
                self.members.insert(id.clone(), MemberState {
                    member_id: id.clone(), epoch: 0, subscribed: subscribed.unwrap_or_default(), assignment: BTreeMap::new(),
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
    /// = Empty（组存在但无成员）；Assignor = 组级分配器名。
    pub fn describe(&self) -> GroupDescribe {
        GroupDescribe {
            state: if self.members.is_empty() { "Empty" } else { "Stable" }.to_string(),
            group_epoch: self.rebalances as i32,
            assignment_epoch: self.rebalances as i32,
            assignor: self.assignor.clone(),
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

    /// 重算分派：range（per-topic 连续切块，默认）/ uniform（全局份额 +
    /// movement-minimizing，T-M3.4）。epoch 推进共用。
    fn rebalance(&mut self) {
        self.rebalances += 1;
        let ids: Vec<String> = self.members.keys().cloned().collect();
        let targets = if self.assignor == "uniform" {
            self.uniform_targets(&ids)
        } else {
            self.range_targets(&ids)
        };
        for (id, m) in self.members.iter_mut() {
            m.assignment = targets.get(id).cloned().unwrap_or_default();
            if m.epoch == 0 {
                m.epoch = 1;
            } else {
                m.epoch += 1;
            }
        }
    }

    /// 服务端 Range 分配（java RangeAssignor 对齐）：订阅并集内每个 topic，
    /// 分区按成员序连续切块（余数给前面的成员）；未订阅的 topic 不分配。
    fn range_targets(&self, ids: &[String]) -> BTreeMap<String, BTreeMap<String, Vec<i32>>> {
        let mut out: BTreeMap<String, BTreeMap<String, Vec<i32>>> =
            ids.iter().map(|id| (id.clone(), BTreeMap::new())).collect();
        let topics: std::collections::BTreeSet<&String> = self
            .members
            .values()
            .flat_map(|m| m.subscribed.iter())
            .collect();
        for topic in topics {
            let total = *self.partition_counts.get(topic).unwrap_or(&0) as usize;
            if total == 0 || ids.is_empty() {
                continue;
            }
            let base = total / ids.len();
            let rem = total % ids.len();
            for (i, id) in ids.iter().enumerate() {
                if !self.members[id].subscribed.iter().any(|s| s == topic) {
                    continue;
                }
                let start = i * base + i.min(rem);
                let count = base + if i < rem { 1 } else { 0 };
                if count > 0 {
                    out.get_mut(id).unwrap().insert(
                        (*topic).clone(),
                        (start as i32..(start + count) as i32).collect(),
                    );
                }
            }
        }
        out
    }

    /// uniform（movement-minimizing，ADR-20 §3）：全局分区清单 = 订阅
    /// topic 并集 × 分区（(topic, partition) 字典序）；份额 n_i = total/M
    /// 前 r 位成员 +1（成员按 id 稳定序）。**保留段**——各成员按序在份额
    /// 内保留现有仍订阅的分区，保留序按「topic 订阅者数升序」优先（唯一
    /// 订阅的 topic 先保——防 constrained topic 被泛订阅 topic 挤出而饿死
    /// 唯一订阅者）；**补派段**——余下分区按序轮派给未满份额的订阅者，
    /// 无未满订阅者时由任一订阅者兜底（订阅约束优先于份额）。
    fn uniform_targets(&self, ids: &[String]) -> BTreeMap<String, BTreeMap<String, Vec<i32>>> {
        let n = ids.len();
        let mut out: BTreeMap<String, BTreeMap<String, Vec<i32>>> =
            ids.iter().map(|id| (id.clone(), BTreeMap::new())).collect();
        if n == 0 {
            return out;
        }
        // 全局分区清单（订阅 topic 并集）
        let topics: std::collections::BTreeSet<&String> = self
            .members
            .values()
            .flat_map(|m| m.subscribed.iter())
            .collect();
        let mut all: Vec<(String, i32)> = Vec::new();
        for t in &topics {
            let total = *self.partition_counts.get(*t).unwrap_or(&0);
            for p in 0..total {
                all.push(((*t).clone(), p));
            }
        }
        let total = all.len();
        let fair = total / n;
        let rem = total % n;
        let size_of = |i: usize| fair + if i < rem { 1 } else { 0 };
        let subscriber_count =
            |t: &String| ids.iter().filter(|id| self.members[*id].subscribed.iter().any(|s| s == t)).count();

        // 保留段：现 assignment 摊平、过滤已退订、按（订阅者数升序，topic，
        // 分区）排序后截到份额
        let mut kept: Vec<Vec<(String, i32)>> = ids
            .iter()
            .map(|id| {
                let m = &self.members[id];
                let sub: std::collections::BTreeSet<&String> = m.subscribed.iter().collect();
                let mut cur: Vec<(String, i32)> = m
                    .assignment
                    .iter()
                    .flat_map(|(t, ps)| ps.iter().map(move |p| ((*t).clone(), *p)))
                    .filter(|(t, _)| sub.contains(t))
                    .collect();
                cur.sort_by_key(|(t, p)| (subscriber_count(t), t.clone(), *p));
                let cap = size_of(ids.iter().position(|x| x == id).unwrap_or(0));
                cur.truncate(cap);
                cur
            })
            .collect();

        // 补派段：未保留分区按（成员序 cyclic）派给未满份额的订阅者；
        // 全员满份额时由任一订阅者兜底（唯一订阅 topic 不得悬空）
        let mut taken: std::collections::BTreeSet<(String, i32)> =
            kept.iter().flatten().cloned().collect();
        let pool: Vec<(String, i32)> = all.into_iter().filter(|k| !taken.contains(k)).collect();
        let mut j = 0usize;
        for kv in pool {
            let subs: Vec<usize> = (0..n)
                .filter(|&i| self.members[&ids[i]].subscribed.iter().any(|s| s == &kv.0))
                .collect();
            if subs.is_empty() {
                continue; // 不可达（all 来自当前订阅并集），防御
            }
            let mut placed = false;
            for k in 0..n {
                let i = (j + k) % n;
                if subs.contains(&i) && kept[i].len() < size_of(i) {
                    kept[i].push(kv.clone());
                    j = i + 1;
                    placed = true;
                    break;
                }
            }
            if !placed {
                for k in 0..n {
                    let i = (j + k) % n;
                    if subs.contains(&i) {
                        kept[i].push(kv.clone());
                        j = i + 1;
                        break;
                    }
                }
            }
        }
        for (i, id) in ids.iter().enumerate() {
            let mut by_topic: BTreeMap<String, Vec<i32>> = BTreeMap::new();
            for (t, p) in &kept[i] {
                by_topic.entry(t.clone()).or_default().push(*p);
            }
            out.insert(id.clone(), by_topic);
        }
        taken.clear();
        out
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
            Some(sub.into_iter().map(String::from).collect()),
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

    /// uniform 分配器（T-M3.4 块 a，ADR-20 §3）movement-minimizing 金样：
    /// 3 分区 [A 全占] → B 加入 [A 留 2、B 得 1] → C 加入 [A 留 p0、B 的
    /// p2 不动、C 得 p1——range 在此把 B 挪到 p1，分叉点即金样] → C 离开
    /// 回归 [A 0,1 / B 2]，零不必要移动。
    #[test]
    fn uniform_assignment_minimizes_movement() {
        let mut cg = ConsumerGroup { assignor: "uniform".into(), ..Default::default() };
        cg.partition_counts.insert("t".into(), 3);
        let a = hb(&mut cg, "", 0, vec!["t"], vec![]);
        assert_eq!(a.assignment[&"t".to_string()], vec![0, 1, 2]);
        let a_id = a.member_id.clone();
        let b = hb(&mut cg, "", 0, vec!["t"], vec![]);
        let b_id = b.member_id.clone();
        // HeartbeatResult 是应答时刻的快照——断言服务端现状须读 cg.members
        assert_eq!(cg.members[&a_id].assignment[&"t".to_string()], vec![0, 1], "A 保留段零移动");
        assert_eq!(cg.members[&b_id].assignment[&"t".to_string()], vec![2]);
        let c = hb(&mut cg, "", 0, vec!["t"], vec![]);
        let c_id = c.member_id.clone();
        let (a1, b1, c1) = (
            &cg.members[&a_id].assignment,
            &cg.members[&b_id].assignment,
            &cg.members[&c_id].assignment,
        );
        assert_eq!(a1[&"t".to_string()], vec![0], "A 保留 p0");
        assert_eq!(b1[&"t".to_string()], vec![2], "B 的 p2 不动（range 会挪——分叉点）");
        assert_eq!(c1[&"t".to_string()], vec![1], "C 补派 p1");
        // C 离开：份额回归，A 拿回 p1，B 全程不动
        hb(&mut cg, &c_id, -1, vec![], vec![]);
        assert_eq!(cg.members[&a_id].assignment[&"t".to_string()], vec![0, 1]);
        assert_eq!(cg.members[&b_id].assignment[&"t".to_string()], vec![2]);
        assert!(!cg.members.contains_key(&c_id));
    }

    /// uniform 退订收回 + 多 topic 全局份额（vs range 的 per-topic 切块）：
    /// 保留序按「topic 订阅者数升序」——A 先保唯一可订阅的 t2，t1 全归 B
    /// （B 的唯一可订阅 topic），订阅约束优先于份额。
    #[test]
    fn uniform_unsubscribe_and_multi_topic() {
        let mut cg = ConsumerGroup { assignor: "uniform".into(), ..Default::default() };
        cg.partition_counts.insert("t1".into(), 2);
        cg.partition_counts.insert("t2".into(), 2);
        let a = hb(&mut cg, "", 0, vec!["t1", "t2"], vec![]);
        assert_eq!(a.assignment[&"t1".to_string()], vec![0, 1]);
        assert_eq!(a.assignment[&"t2".to_string()], vec![0, 1]);
        let a_id = a.member_id.clone();
        let b = hb(&mut cg, "", 0, vec!["t1"], vec![]);
        let b_id = b.member_id.clone();
        // 全局 4 分区 2 成员 → 各 2：A 保 t2（唯一订阅者），B 得 t1
        let a_now = &cg.members[&a_id].assignment;
        assert_eq!(a_now.get("t1"), None, "t1 让给唯一订阅 t1 的 B");
        assert_eq!(a_now[&"t2".to_string()], vec![0, 1], "A 保唯一可订阅的 t2");
        assert_eq!(cg.members[&b_id].assignment[&"t1".to_string()], vec![0, 1]);
        // 订阅约束优先于份额——B 不得拿 t2
        assert_eq!(cg.members[&b_id].assignment.get("t2"), None);
        // A 退订 t1（已不在手上）→ 双方份额不变，B 零扰动。
        // 真实客户端语义：B 加入曾令 A 被 fence，退订前先按服务端当前
        // epoch 重同步（直接用旧快照的心跳会被拒）
        let a_epoch = cg.members[&a_id].epoch;
        let u = hb(&mut cg, &a_id, a_epoch, vec!["t2"], vec![]);
        assert!(!u.assignment.contains_key("t1"));
        assert_eq!(u.assignment[&"t2".to_string()], vec![0, 1]);
        assert_eq!(cg.members[&b_id].assignment[&"t1".to_string()], vec![0, 1]);
    }

    /// keepalive（KIP-848：未变字段置 null）：订阅保持、分配保留、epoch
    /// 不 bump——不得把 null 订阅当退订（franz-go topicsMatch 路径每拍都
    /// 发 null，误判会让成员每拍丢分配）。
    #[test]
    fn keepalive_null_subscription_is_noop() {
        let mut cg = ConsumerGroup::default();
        cg.partition_counts.insert("t".into(), 2);
        let a = hb(&mut cg, "", 0, vec!["t"], vec![]);
        let ka = cg.heartbeat(
            &a.member_id,
            a.member_epoch,
            None,
            BTreeMap::new(),
        );
        assert!(!ka.fenced);
        assert_eq!(ka.member_epoch, a.member_epoch, "keepalive 不 bump");
        assert_eq!(ka.assignment[&"t".to_string()], vec![0, 1], "分配保留");
        // 新成员带 None 订阅 = 空订阅（无分配但不挂）
        let b = cg.heartbeat("", 0, None, BTreeMap::new());
        assert!(!b.fenced);
        assert!(b.assignment.is_empty());
    }
}

// ---------- 多组管理 actor（块 b 协议面接线用） ----------

pub struct CGHeartbeat {
    pub group: String,
    pub member_id: String,
    pub member_epoch: i32,
    /// None = 订阅未变（KIP-848 keepalive：未变字段置 null）
    pub subscribed: Option<Vec<String>>,
    /// ServerAssignor（协议面已校验 ∈ {range, uniform}）；None = 未变。
    /// 组创建时首个非空请求定组分配器（first-wins，ADR-20 §3）
    pub assignor: Option<String>,
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
                        let cg = groups.entry(hb.group.clone()).or_insert_with(|| ConsumerGroup {
                            assignor: hb.assignor.clone().unwrap_or_else(|| "range".into()),
                            ..Default::default()
                        });
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
