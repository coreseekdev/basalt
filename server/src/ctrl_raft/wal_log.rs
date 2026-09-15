//! 控制器 raft WAL：写穿持久化（ADR-17 delta-A；dendro SPEC 02 移植子集）。
//!
//! 设计（docs/research/dendro-WAL与TiDBX对照调研.md §4-A）：
//! - 帧头 32B（LE）：magic("BRWL") + version + ftype + term + index + len +
//!   crc32c（只覆盖 payload——头与 payload 独立失效可分别处理，dendro 同款）；
//! - 段 = 追加文件 `{dir}/wal/{seg:020}.wal`，按字节阈值轮转，sync_data 每
//!   批一次（控制器元数据低频，SyncEach 语义即可，不需要组提交）；
//! - 恢复 = 段序升序重放进 MemStorageCore：`append` 的冲突裁剪语义使跨
//!   生命周期混段重放安全（新生命周期首帧 index=1 会截掉旧前缀）；
//!   **末段撕裂容忍**（截到最后合法帧边界后续写，SQLite/PG 同语义）；
//!   非末段腐坏 = 放弃重放、内存起步靠 leader 重发（与无 WAL 现状等同，
//!   不拒绝启动——文件保留供取证）；
//! - 毒化：IO 失败置位后 persist 跳过 WAL（内存降级 + ERROR 一次）；恢复 =
//!   进程重启。POC 取舍：可用性优先于"ack 即 durable"的严格性（生产答案 =
//!   dendro 式拒绝写，ADR-17 记录）。
//!
//! 不做的事（v1 边界）：组提交（元数据低频无意义）、WAL GC/压缩（快照
//! 截断基点留待后续）、CONF 帧持久化（静态成员每 boot 从 cfg 重设）。

use raft::eraftpb::{Entry, HardState};
use raft::storage::{MemStorage, MemStorageCore};
use raft::{GetEntriesContext, RaftState, Storage};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub const FRAME_MAGIC: u32 = 0x4C575242; // "BRWL" LE 视觉可辨
pub const FRAME_VERSION: u16 = 1;
pub const HEADER_LEN: usize = 32;
const SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

/// ftype：1 = HARDSTATE（term/vote/commit），2 = ENTRY（protobuf）
pub const FT_HARDSTATE: u16 = 1;
pub const FT_ENTRY: u16 = 2;

// ---------------------------------------------------------------------------
// 帧编解码
// ---------------------------------------------------------------------------

fn encode_header(ftype: u16, term: u64, index: u64, payload: &[u8]) -> Vec<u8> {
    let mut f = Vec::with_capacity(HEADER_LEN + payload.len());
    f.extend_from_slice(&FRAME_MAGIC.to_le_bytes());
    f.extend_from_slice(&FRAME_VERSION.to_le_bytes());
    f.extend_from_slice(&ftype.to_le_bytes());
    f.extend_from_slice(&term.to_le_bytes());
    f.extend_from_slice(&index.to_le_bytes());
    f.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    f.extend_from_slice(&crc32c::crc32c(payload).to_le_bytes());
    f
}

/// 单帧解析：返回 (ftype, term, index, payload, 帧总长)。
/// Err = 帧腐坏（magic/version/len/crc 任一）；None(&[]) = 需要更多字节（撕尾）。
fn parse_frame(data: &[u8]) -> Option<Result<(u16, u64, u64, &[u8], usize), ()>> {
    if data.len() < HEADER_LEN {
        return None; // 半帧头：撕尾
    }
    let magic = u32::from_le_bytes(data[..4].try_into().unwrap());
    if magic != FRAME_MAGIC {
        return Some(Err(()));
    }
    let ver = u16::from_le_bytes(data[4..6].try_into().unwrap());
    if ver != FRAME_VERSION {
        return Some(Err(()));
    }
    let ftype = u16::from_le_bytes(data[6..8].try_into().unwrap());
    let term = u64::from_le_bytes(data[8..16].try_into().unwrap());
    let index = u64::from_le_bytes(data[16..24].try_into().unwrap());
    let len = u32::from_le_bytes(data[24..28].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(data[28..32].try_into().unwrap());
    if HEADER_LEN + len > data.len() {
        return None; // 半帧体：撕尾
    }
    let payload = &data[HEADER_LEN..HEADER_LEN + len];
    if crc32c::crc32c(payload) != crc {
        return Some(Err(()));
    }
    Some(Ok((ftype, term, index, payload, HEADER_LEN + len)))
}

// ---------------------------------------------------------------------------
// WalStorage：MemStorage（服务层）+ 写穿 WAL（持久层）
// ---------------------------------------------------------------------------

pub struct WalStorage {
    mem: MemStorage,
    sink: Mutex<WalSink>,
}

struct WalSink {
    dir: PathBuf,
    file: Option<File>,
    seg: u64,
    written: u64,
    poisoned: bool,
    poisoned_logged: bool,
}

impl WalStorage {
    /// 打开并恢复：段序重放进内存核心；末段撕裂截到合法边界。
    pub fn open(dir: &Path) -> WalStorage {
        let mem = MemStorage::new();
        let wal_dir = dir.join("wal");
        let sink = match Self::replay(&wal_dir, &mem) {
            Ok((seg, written)) => WalSink {
                dir: wal_dir,
                file: None,
                seg,
                written,
                poisoned: false,
                poisoned_logged: false,
            },
            Err(e) => {
                // 非末段腐坏：放弃重放（内存起步靠 leader 重发——与无 WAL
                // 现状等同），文件保留供取证
                eprintln!("RAFT-WAL replay failed ({e}); starting with empty raft log");
                WalSink {
                    dir: wal_dir,
                    file: None,
                    seg: 0,
                    written: 0,
                    poisoned: false,
                    poisoned_logged: false,
                }
            }
        };
        WalStorage { mem, sink: Mutex::new(sink) }
    }

    /// 静态成员：每 boot 从 cfg 重设（不持久化）。
    pub fn set_confstate(&self, cs: &raft::prelude::ConfState) {
        self.mem.wl().set_conf_state(cs.clone());
    }

    /// 是否有恢复出的 raft 历史（campaign 门控：有历史 = 重 join 节点，
    /// 不得主动 campaign 打断在位 leader）。
    pub fn has_history(&self) -> bool {
        self.mem.last_index().map(|i| i > 0).unwrap_or(false)
    }

    /// 写穿：帧落 WAL（+sync_data）后应用到内存核心。
    /// IO 失败 = 毒化（后续 persist 内存降级，ERROR 一次）。
    pub fn persist_ready(&self, hs: Option<&HardState>, ents: &[Entry]) {
        let mut sink = self.sink.lock().unwrap();
        if !sink.poisoned {
            let mut batch: Vec<u8> = Vec::new();
            if let Some(h) = hs {
                let mut p = Vec::with_capacity(24);
                p.extend_from_slice(&h.term.to_le_bytes());
                p.extend_from_slice(&h.vote.to_le_bytes());
                p.extend_from_slice(&h.commit.to_le_bytes());
                batch.extend_from_slice(&encode_header(FT_HARDSTATE, h.term, 0, &p));
                batch.extend_from_slice(&p);
            }
            for e in ents {
                let p = protobuf::Message::write_to_bytes(e).unwrap_or_default();
                batch.extend_from_slice(&encode_header(FT_ENTRY, e.get_term(), e.get_index(), &p));
                batch.extend_from_slice(&p);
            }
            if !batch.is_empty() {
                if let Err(e) = sink.write_sync(&batch) {
                    sink.poisoned = true;
                    eprintln!("RAFT-WAL persist failed, degrading to memory-only: {e}");
                }
            }
        }
        // 内存核心（服务层）无论 WAL 成败都推进——可用性优先，
        // 毒化期间丢的是"重启恢复点"，不是运行中状态
        let mut core = self.mem.wl();
        if let Some(h) = hs {
            core.set_hardstate(h.clone());
        }
        if !ents.is_empty() {
            let _ = core.append(ents);
        }
    }

    /// 段序重放。返回 (最后段号, 末段合法前缀字节数)。
    fn replay(wal_dir: &Path, mem: &MemStorage) -> std::io::Result<(u64, u64)> {
        let read_dir = match std::fs::read_dir(wal_dir) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((0, 0)),
            Err(e) => return Err(e),
        };
        let mut segs: Vec<PathBuf> = read_dir
            .filter_map(|r| r.ok())
            .map(|r| r.path())
            .filter(|p| p.extension().map(|x| x == "wal").unwrap_or(false))
            .collect();
        segs.sort();
        if segs.is_empty() {
            return Ok((0, 0));
        }
        let mut guard = mem.wl();
        let core: &mut MemStorageCore = &mut guard;
        let n = segs.len();
        for (i, path) in segs.iter().enumerate() {
            let data = std::fs::read(path)?;
            let last = i + 1 == n;
            let mut off = 0usize;
            let mut err: Option<String> = None;
            while off < data.len() {
                match parse_frame(&data[off..]) {
                    None => {
                        // 半帧 = 撕尾；末段截掉，非末段视为腐坏
                        if !last {
                            err = Some(format!("torn frame mid-file at {off}"));
                        }
                        break;
                    }
                    Some(Err(())) => {
                        if !last {
                            err = Some(format!("corrupt frame at {off}"));
                        }
                        break;
                    }
                    Some(Ok((ftype, term, index, payload, flen))) => {
                        match ftype {
                            FT_HARDSTATE if payload.len() >= 24 => {
                                let mut hs = HardState::default();
                                hs.term = u64::from_le_bytes(payload[0..8].try_into().unwrap());
                                hs.vote = u64::from_le_bytes(payload[8..16].try_into().unwrap());
                                hs.commit = u64::from_le_bytes(payload[16..24].try_into().unwrap());
                                core.set_hardstate(hs);
                            }
                            FT_ENTRY => {
                                if let Ok(e) =
                                    protobuf::Message::parse_from_bytes(payload)
                                        .map(|e: Entry| e)
                                {
                                    core.append(&[e]).map_err(|e| {
                                        std::io::Error::other(format!("replay append: {e}"))
                                    })?;
                                }
                                let _ = index;
                            }
                            _ => {}
                        }
                        off += flen;
                    }
                }
            }
            if let Some(e) = err {
                return Err(std::io::Error::other(format!(
                    "{}: {e}",
                    path.display()
                )));
            }
            if i + 1 == n {
                // 末段撕裂截尾：后续 append 从合法边界续写
                if (off as u64) < data.len() as u64 {
                    let f = OpenOptions::new().write(true).open(path)?;
                    f.set_len(off as u64)?;
                    f.sync_data()?;
                }
                return Ok((parse_seg_no(path), off as u64));
            }
        }
        unreachable!("segs non-empty")
    }
}

fn parse_seg_no(path: &Path) -> u64 {
    path.file_stem()
        .and_then(|s| s.to_str())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
}

impl WalSink {
    /// 追加字节 + sync_data；跨段轮转（写前判定，失败路径由毒化兜底）。
    fn write_sync(&mut self, batch: &[u8]) -> std::io::Result<()> {
        if self.file.is_none() {
            std::fs::create_dir_all(&self.dir)?;
            self.seg += 1;
            let path = self.dir.join(format!("{:020}.wal", self.seg));
            let mut f = OpenOptions::new().create(true).append(true).open(&path)?;
            self.written = f.metadata()?.len();
            f.write_all(&batch)?;
            f.sync_data()?;
            self.file = Some(f);
            self.written += batch.len() as u64;
            return Ok(());
        }
        let f = self.file.as_mut().unwrap();
        f.write_all(&batch)?;
        f.sync_data()?;
        self.written += batch.len() as u64;
        if self.written >= SEGMENT_BYTES {
            // 轮转：关闭当前句柄，下一批开新段
            self.file = None;
        }
        Ok(())
    }
}

impl Storage for WalStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        self.mem.initial_state()
    }
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        self.mem.entries(low, high, max_size, context)
    }
    fn term(&self, idx: u64) -> raft::Result<u64> {
        self.mem.term(idx)
    }
    fn first_index(&self) -> raft::Result<u64> {
        self.mem.first_index()
    }
    fn last_index(&self) -> raft::Result<u64> {
        self.mem.last_index()
    }
    fn snapshot(&self, request_index: u64, to: u64) -> raft::Result<raft::prelude::Snapshot> {
        self.mem.snapshot(request_index, to)
    }
}

/// ready 周期的持久化接口：MemStorage（纯内存，现状语义）与
/// WalStorage（写穿）共用 driver 泛型路径。
pub trait ReadyPersist {
    fn persist_ready(&self, hs: Option<&HardState>, ents: &[Entry]);
}

impl ReadyPersist for MemStorage {
    fn persist_ready(&self, hs: Option<&HardState>, ents: &[Entry]) {
        let mut core = self.wl();
        if let Some(h) = hs {
            core.set_hardstate(h.clone());
        }
        if !ents.is_empty() {
            let _ = core.append(ents);
        }
    }
}

impl ReadyPersist for WalStorage {
    fn persist_ready(&self, hs: Option<&HardState>, ents: &[Entry]) {
        WalStorage::persist_ready(self, hs, ents)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use raft::prelude::ConfState;

    fn mk_entry(idx: u64, term: u64, tag: u8) -> Entry {
        let mut e = Entry::default();
        e.set_index(idx);
        e.set_term(term);
        e.set_data(bytes::Bytes::from(vec![tag]));
        e
    }

    fn hs(term: u64, vote: u64, commit: u64) -> HardState {
        let mut h = HardState::default();
        h.term = term;
        h.vote = vote;
        h.commit = commit;
        h
    }

    fn temp_dir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("basalt-wal-test-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// roundtrip：写穿 + 重开重放恢复 hardstate/entries
    #[test]
    fn wal_roundtrip_replay() {
        let dir = temp_dir("roundtrip");
        {
            let st = WalStorage::open(&dir);
            st.persist_ready(Some(&hs(2, 1, 3)), &[mk_entry(1, 1, 1), mk_entry(2, 2, 2)]);
            st.set_confstate(&ConfState { voters: vec![1, 2, 3], ..Default::default() });
        }
        let st = WalStorage::open(&dir);
        let init = st.initial_state().unwrap();
        assert_eq!(init.hard_state.term, 2);
        assert_eq!(init.hard_state.vote, 1);
        assert_eq!(init.hard_state.commit, 3);
        assert_eq!(st.last_index().unwrap(), 2);
        let ents = st.entries(1, 3, None, GetEntriesContext::empty(false)).unwrap();
        assert_eq!(ents.len(), 2);
        assert_eq!(ents[1].get_term(), 2);
        // 重放后 term 查询（含 first-1 语义：term(1)=1）
        assert_eq!(st.term(2).unwrap(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 撕裂容忍：末段半帧/坏帧截到合法边界，重开可续写
    #[test]
    fn wal_torn_tail_tolerance() {
        let dir = temp_dir("torn");
        {
            let st = WalStorage::open(&dir);
            st.persist_ready(Some(&hs(1, 0, 1)), &[mk_entry(1, 1, 1)]);
        }
        // 追加撕尾（半帧头 + 垃圾）
        let seg = dir.join("wal").join(format!("{:020}.wal", 1));
        let mut f = OpenOptions::new().append(true).open(&seg).unwrap();
        f.write_all(&[0x42, 0x42, 0x42]).unwrap();
        f.sync_data().unwrap();
        drop(f);

        let st = WalStorage::open(&dir);
        assert_eq!(st.last_index().unwrap(), 1);
        // 截断后续写不污染：新帧正常恢复
        st.persist_ready(Some(&hs(1, 0, 2)), &[mk_entry(2, 1, 2)]);
        let seg_len = std::fs::metadata(&seg).unwrap().len();
        drop(st);
        let st = WalStorage::open(&dir);
        assert_eq!(st.last_index().unwrap(), 2);
        assert_eq!(st.term(2).unwrap(), 1);
        assert!(seg_len > 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 跨生命周期混段重放安全：新生命周期 index=1 首帧截掉旧前缀
    /// （append 冲突裁剪语义），重放终态 = 新生命周期终态
    #[test]
    fn wal_new_life_truncates_old_prefix() {
        let dir = temp_dir("newlife");
        {
            let st = WalStorage::open(&dir);
            st.persist_ready(Some(&hs(1, 0, 3)), &[mk_entry(1, 1, 1), mk_entry(2, 1, 2), mk_entry(3, 1, 3)]);
        }
        // 新生命周期：内存起步后从 index 1 重新接收（term 2）
        let st = WalStorage::open(&dir);
        st.persist_ready(Some(&hs(2, 1, 2)), &[mk_entry(1, 2, 9), mk_entry(2, 2, 8)]);
        drop(st);
        let st = WalStorage::open(&dir);
        assert_eq!(st.last_index().unwrap(), 2);
        let ents = st.entries(1, 3, None, GetEntriesContext::empty(false)).unwrap();
        assert_eq!(ents[0].get_data(), vec![9u8]); // 新生命周期的条目
        assert_eq!(st.term(2).unwrap(), 2);
        assert_eq!(st.initial_state().unwrap().hard_state.commit, 2);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
