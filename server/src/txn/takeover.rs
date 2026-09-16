//! 接管恢复判定纯函数（ADR-18 §6）：输入全部来自 TxnLog 重放结果，无 I/O、
//! 无时钟——可表驱动单测、可进 TLA+（arroyo §11 derive_checkpoint_state /
//! resolve_candidate 的同构物）。
//!
//! 三态映射（arroyo 对照）：`Committing → committed.json → ReplayCommit` ↔
//! `Prepare → Complete → ReplayCommit`；`Orphaned`（所有权已易主即出局）↔
//! epoch 被超越的 Ongoing。注意与 arroyo 的同名异义：arroyo 的 Orphaned 是
//! 终态停机（StopOrphaned = leader 自杀退休），basalt 用作「强制 abort」
//! 行动态——abort 是安全方向，落地后转 Ready。

use super::{TxnOutcome, TxnPhase, TxnState};

/// 接管判定结果。ReplayCommit 携带补发 marker 所需的全部信息（outcome +
/// 分区清单）；pending offsets 生效在 ReplayCommit{Commit} 时附带重跑
/// （证据在 TxnLog，重放幂等，ADR-18 §7）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Takeover {
    /// 无需动作（无事务 / 上一事务已终结）。
    Ready,
    /// Prepare 已落盘但 Complete 未落：补发 marker（幂等由分区侧 §4.3 保证）。
    ReplayCommit { outcome: super::TxnOutcome, parts: Vec<(String, i32)> },
    /// 恢复到的 Ongoing 一律转强制 abort（安全方向，落地后转 Ready）——
    /// epoch 被新 InitProducerId 超越 / deadline 已过只是其成因分类。
    Orphaned { pid: i64, epoch: i16, parts: Vec<(String, i32)> },
}

/// 纯判定：coordinator 接管（进程重启 / failover 换主后首次见到该事务）
/// 时，按 TxnLog 折叠出的状态决定动作。
pub fn resolve_takeover(rec: &TxnState) -> Takeover {
    match rec.phase {
        TxnPhase::Empty | TxnPhase::Complete { .. } => Takeover::Ready,
        TxnPhase::Prepare { outcome } => Takeover::ReplayCommit {
            outcome,
            parts: rec.parts.clone(),
        },
        TxnPhase::Ongoing => Takeover::Orphaned {
            pid: rec.pid,
            epoch: rec.epoch,
            parts: rec.parts.clone(),
        },
    }
}

#[cfg(test)]
mod takeover_tests {
    //! 表驱动：ADR-18 §6 三态全分支 + 边界（空分区清单的 Prepare 仍要
    //! 补发——补发零 marker 即完成，Complete 落盘保证状态机闭合）。

    use super::*;
    use basalt_record::ControlRecordType;

    fn state(phase: TxnPhase, parts: Vec<(String, i32)>) -> TxnState {
        TxnState {
            txn_id: "t1".into(),
            pid: 42,
            epoch: 3,
            prepare_epoch: None,
            phase,
            parts,
            pending: vec![],
        }
    }

    #[test]
    fn empty_is_ready() {
        assert_eq!(resolve_takeover(&state(TxnPhase::Empty, vec![])), Takeover::Ready);
    }

    #[test]
    fn complete_is_ready_with_any_outcome() {
        for outcome in [TxnOutcome::Commit, TxnOutcome::Abort] {
            let s = state(TxnPhase::Complete { outcome }, vec![("a".into(), 0)]);
            assert_eq!(resolve_takeover(&s), Takeover::Ready);
        }
    }

    #[test]
    fn prepare_replays_with_outcome_and_parts() {
        let s = state(
            TxnPhase::Prepare { outcome: TxnOutcome::Commit },
            vec![("a".into(), 0), ("b".into(), 2)],
        );
        assert_eq!(
            resolve_takeover(&s),
            Takeover::ReplayCommit {
                outcome: TxnOutcome::Commit,
                parts: vec![("a".into(), 0), ("b".into(), 2)],
            }
        );
        // 空 parts 也走 ReplayCommit（零 marker + Complete 落盘闭合状态机）
        let s0 = state(TxnPhase::Prepare { outcome: TxnOutcome::Abort }, vec![]);
        assert_eq!(
            resolve_takeover(&s0),
            Takeover::ReplayCommit { outcome: TxnOutcome::Abort, parts: vec![] }
        );
    }

    #[test]
    fn ongoing_is_orphaned_forced_abort() {
        let s = state(TxnPhase::Ongoing, vec![("a".into(), 1)]);
        assert_eq!(
            resolve_takeover(&s),
            Takeover::Orphaned { pid: 42, epoch: 3, parts: vec![("a".into(), 1)] }
        );
    }
}
