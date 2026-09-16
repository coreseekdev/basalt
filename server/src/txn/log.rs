//! TxnLog：事务元数据 append-only 日志（ADR-18 §2，`__transaction_state`
//! 的 basalt 对应物；OffsetLog/ADR-17 WAL 同族——恢复 = 重放折叠）。
//!
//! 记录布局（全 BE + 长度前缀，`[len:i32][T:u8][payload…]`，S = [len:i16+bytes]）：
//! - T=1 Init    `[txn:S][pid:i64][epoch:i16]` —— PID/epoch 分配（每事务 init bump）
//! - T=2 Begin   `[txn:S][pid:i64][epoch:i16][parts]` —— Empty/Complete → Ongoing
//! - T=3 Prepare `[txn:S][pid:i64][epoch:i16][outcome:i8][parts]` —— 两段
//!   落盘点（fsync 后才发 marker）；携带 (pid,epoch) 使接管重驱钉住 Prepare
//!   时历元（re-init bump 后重驱不再发错 epoch，review P2 根因修法）
//! - T=4 Complete`[txn:S][outcome:i8]` —— 终结
//! - T=5 Pending `[txn:S][pid:i64][epoch:i16][offs]` —— TxnOffsetCommit 落盘（§7）
//! `[parts]` = [n:i32] + n×([topic:S][partition:i32])；
//! `[offs]`  = [n:i32] + n×([group:S][topic:S][partition:i32][offset:i64][meta:S])
//!
//! Prepare/Complete 的 outcome：1=Commit、2=Abort。

use super::{PendingOffset, TxnOutcome, TxnPhase, TxnState};
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;

pub struct TxnLog {
    path: PathBuf,
    file: std::sync::Mutex<Option<std::fs::File>>,
}

fn put_str(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as i16).to_be_bytes());
    buf.extend_from_slice(s.as_bytes());
}

fn put_parts(buf: &mut Vec<u8>, parts: &[(String, i32)]) {
    buf.extend_from_slice(&(parts.len() as i32).to_be_bytes());
    for (t, p) in parts {
        put_str(buf, t);
        buf.extend_from_slice(&p.to_be_bytes());
    }
}

fn outcome_byte(o: TxnOutcome) -> u8 {
    match o {
        TxnOutcome::Commit => 1,
        TxnOutcome::Abort => 2,
    }
}

fn outcome_of(b: u8) -> Option<TxnOutcome> {
    match b {
        1 => Some(TxnOutcome::Commit),
        2 => Some(TxnOutcome::Abort),
        _ => None,
    }
}

struct Reader<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn i8(&mut self) -> i8 {
        let v = self.b[self.pos] as i8;
        self.pos += 1;
        v
    }
    fn i16(&mut self) -> i16 {
        let v = i16::from_be_bytes(self.b[self.pos..self.pos + 2].try_into().unwrap());
        self.pos += 2;
        v
    }
    fn i32(&mut self) -> i32 {
        let v = i32::from_be_bytes(self.b[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        v
    }
    fn i64(&mut self) -> i64 {
        let v = i64::from_be_bytes(self.b[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        v
    }
    fn string(&mut self) -> String {
        let n = self.i16();
        let s = String::from_utf8_lossy(&self.b[self.pos..self.pos + n.max(0) as usize]).into_owned();
        self.pos += n.max(0) as usize;
        s
    }
    fn parts(&mut self) -> Vec<(String, i32)> {
        let n = self.i32();
        let mut v = Vec::new();
        for _ in 0..n.max(0) {
            let t = self.string();
            let p = self.i32();
            v.push((t, p));
        }
        v
    }
    fn offsets(&mut self) -> Vec<PendingOffset> {
        let n = self.i32();
        let mut v = Vec::new();
        for _ in 0..n.max(0) {
            let group = self.string();
            let topic = self.string();
            let partition = self.i32();
            let offset = self.i64();
            let metadata = self.string();
            v.push(PendingOffset { group, topic, partition, offset, metadata });
        }
        v
    }
    /// 容错推进到记录尾（重放折叠容忍个别半记录——坏尾截断语义）。
    fn seek(&mut self, end: usize) {
        self.pos = end;
    }
}

impl TxnLog {
    pub fn open(path: &std::path::Path) -> TxnLog {
        TxnLog { path: path.to_path_buf(), file: std::sync::Mutex::new(None) }
    }

    /// 追加一条记录并 fsync（Prepare 落盘点契约：ADR-18 §5——Prepare 先于
    /// 一切 marker；掉电后重放必须见到它）。事务元数据低频，不做组提交。
    fn append(&self, rec_ty: u8, mut payload: Vec<u8>) -> std::io::Result<()> {
        let mut frame = (payload.len() as i32 + 1).to_be_bytes().to_vec();
        frame.push(rec_ty);
        frame.append(&mut payload);
        let mut f = self.file.lock().unwrap();
        let file = f.get_or_insert_with(|| {
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
                .expect("open txn log")
        });
        file.write_all(&frame)?;
        file.flush()?;
        file.sync_data()
    }

    pub fn append_init(&self, txn_id: &str, pid: i64, epoch: i16) -> std::io::Result<()> {
        let mut p = Vec::with_capacity(64);
        put_str(&mut p, txn_id);
        p.extend_from_slice(&pid.to_be_bytes());
        p.extend_from_slice(&epoch.to_be_bytes());
        self.append(1, p)
    }

    pub fn append_begin(&self, txn_id: &str, pid: i64, epoch: i16, parts: &[(String, i32)]) -> std::io::Result<()> {
        let mut p = Vec::with_capacity(64);
        put_str(&mut p, txn_id);
        p.extend_from_slice(&pid.to_be_bytes());
        p.extend_from_slice(&epoch.to_be_bytes());
        put_parts(&mut p, parts);
        self.append(2, p)
    }

    pub fn append_prepare(
        &self,
        txn_id: &str,
        pid: i64,
        epoch: i16,
        outcome: TxnOutcome,
        parts: &[(String, i32)],
    ) -> std::io::Result<()> {
        let mut p = Vec::with_capacity(64);
        put_str(&mut p, txn_id);
        p.extend_from_slice(&pid.to_be_bytes());
        p.extend_from_slice(&epoch.to_be_bytes());
        p.push(outcome_byte(outcome));
        put_parts(&mut p, parts);
        self.append(3, p)
    }

    pub fn append_complete(&self, txn_id: &str, outcome: TxnOutcome) -> std::io::Result<()> {
        let mut p = Vec::with_capacity(32);
        put_str(&mut p, txn_id);
        p.push(outcome_byte(outcome));
        self.append(4, p)
    }

    pub fn append_pending(&self, txn_id: &str, pid: i64, epoch: i16, offs: &[PendingOffset]) -> std::io::Result<()> {
        let mut p = Vec::with_capacity(128);
        put_str(&mut p, txn_id);
        p.extend_from_slice(&pid.to_be_bytes());
        p.extend_from_slice(&epoch.to_be_bytes());
        p.extend_from_slice(&(offs.len() as i32).to_be_bytes());
        for o in offs {
            put_str(&mut p, &o.group);
            put_str(&mut p, &o.topic);
            p.extend_from_slice(&o.partition.to_be_bytes());
            p.extend_from_slice(&o.offset.to_be_bytes());
            put_str(&mut p, &o.metadata);
        }
        self.append(5, p)
    }

    /// 重放折叠（last-wins per txn_id）：坏尾截断（撕裂容忍，OffsetLog 同
    /// 语义）；PendingOffsets 记录在 Complete 之前累积、Complete 后清空
    /// （重放折叠与运行期语义一致：pending 属于 Prepare→Complete 窗口）。
    pub fn replay(&self) -> HashMap<String, TxnState> {
        let mut map: HashMap<String, TxnState> = HashMap::new();
        let Ok(data) = std::fs::read(&self.path) else { return map };
        let mut r = Reader { b: &data, pos: 0 };
        while r.pos + 5 <= data.len() {
            let len_i = r.i32();
            if len_i <= 0 || r.pos + len_i as usize > data.len() {
                break; // 坏尾截断（负帧长：crafted/腐坏，防 usize 回绕）
            }
            let len = len_i as usize;
            let rec_end = r.pos + len;
            let ty = r.i8() as u8;
            match ty {
                1 => {
                    let txn = r.string();
                    let pid = r.i64();
                    let epoch = r.i16();
                    let e = map.entry(txn.clone()).or_insert_with(|| TxnState {
                        txn_id: txn,
                        pid,
                        epoch,
                        prepare_epoch: None,
                        phase: TxnPhase::Empty,
                        parts: vec![],
                        pending: vec![],
                    });
                    e.pid = pid;
                    e.epoch = epoch;
                }
                2 => {
                    let txn = r.string();
                    let pid = r.i64();
                    let epoch = r.i16();
                    let parts = r.parts();
                    let e = map.entry(txn.clone()).or_insert_with(|| TxnState {
                        txn_id: txn,
                        pid,
                        epoch,
                        prepare_epoch: None,
                        phase: TxnPhase::Empty,
                        parts: vec![],
                        pending: vec![],
                    });
                    e.pid = pid;
                    e.epoch = epoch;
                    e.parts = parts;
                    e.phase = TxnPhase::Ongoing;
                }
                3 => {
                    let txn = r.string();
                    let pid = r.i64();
                    let epoch = r.i16();
                    let outcome = outcome_of(r.i8() as u8);
                    let parts = r.parts();
                    if let (Some(e), Some(outcome)) = (map.get_mut(&txn), outcome) {
                        e.phase = TxnPhase::Prepare { outcome };
                        e.parts = parts;
                        // 钉住 Prepare 时历元（重驱 marker 用它，非当前折叠值）
                        e.prepare_epoch = Some(epoch);
                        let _ = pid;
                    }
                }
                4 => {
                    let txn = r.string();
                    let outcome = outcome_of(r.i8() as u8);
                    if let (Some(e), Some(outcome)) = (map.get_mut(&txn), outcome) {
                        e.phase = TxnPhase::Complete { outcome };
                        // commit 的 pending 保留（review P1-b）：Complete 先于
                        // group 侧落盘的窄缝内崩溃时，接管对它幂等重提升
                        if outcome == TxnOutcome::Abort {
                            e.pending.clear();
                        }
                    }
                }
                5 => {
                    let txn = r.string();
                    let pid = r.i64();
                    let epoch = r.i16();
                    let offs = r.offsets();
                    if let Some(e) = map.get_mut(&txn) {
                        // epoch 校验：陈旧会话的 pending 丢弃（fence 面）
                        if e.pid == pid && e.epoch == epoch {
                            e.pending = offs;
                        }
                    }
                }
                _ => {}
            }
            r.seek(rec_end);
        }
        tracing::info!(txns = map.len(), path = %self.path.display(), "txn log replayed");
        map
    }
}
