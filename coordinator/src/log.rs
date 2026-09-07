//! offset 持久化：append-only 日志（record-based，恢复=重放）。
//!
//! 记录布局（全 BE + 长度前缀）：
//! [len:i32][group:S][topic:S][partition:i32][offset:i64][meta:S][ts:i64]
//! S = [len:i16 + bytes]；len<0 表示空。
//! 打开时全量重放去重构建 map（即紧凑化）；后续分段压缩由 M1 补齐。

use crate::CommittedOffset;
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;

pub struct OffsetLog {
    path: PathBuf,
    file: std::sync::Mutex<Option<std::fs::File>>,
}

fn put_str(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as i16).to_be_bytes());
    buf.extend_from_slice(s.as_bytes());
}

struct Reader<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn i16(&mut self) -> i16 {
        let v = i16::from_be_bytes([self.b[self.pos], self.b[self.pos + 1]]);
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
}

impl OffsetLog {
    pub fn open(path: &std::path::Path) -> OffsetLog {
        OffsetLog { path: path.to_path_buf(), file: std::sync::Mutex::new(None) }
    }

    pub fn append(&self, group: &str, o: &CommittedOffset) {
        let _ = group;
        let mut rec = Vec::with_capacity(64);
        let _ = group;
        put_str(&mut rec, group);
        put_str(&mut rec, &o.topic);
        rec.extend_from_slice(&o.partition.to_be_bytes());
        rec.extend_from_slice(&o.offset.to_be_bytes());
        put_str(&mut rec, &o.metadata);
        rec.extend_from_slice(&o.commit_ts.to_be_bytes());
        let len = rec.len() as i32;
        let mut frame = len.to_be_bytes().to_vec();
        frame.extend_from_slice(&rec);
        let mut f = self.file.lock().unwrap();
        let file = f.get_or_insert_with(|| {
            std::fs::OpenOptions::new().create(true).append(true).open(&self.path).expect("open offset log")
        });
        let _ = file.write_all(&frame);
        let _ = file.flush();
    }

    /// 重放构建（含 group 维度）：文件布局逐记录 [len][group][topic][part][offset][meta][ts]。
    pub fn replay(&self) -> HashMap<(String, String, i32), CommittedOffset> {
        let mut map = HashMap::new();
        let Ok(data) = std::fs::read(&self.path) else { return map };
        let mut r = Reader { b: &data, pos: 0 };
        while r.pos + 4 <= data.len() {
            let len = r.i32() as usize;
            if len == 0 || r.pos + len > data.len() {
                break; // 坏尾截断
            }
            let rec_end = r.pos + len;
            // 记录内：group:S topic:S part:i32 offset:i64 meta:S ts:i64
            let group = r.string();
            let topic = r.string();
            let part = r.i32();
            let offset = r.i64();
            let meta = r.string();
            let ts = r.i64();
            map.insert(
                (group, topic.clone(), part),
                CommittedOffset { topic, partition: part, offset, metadata: meta, commit_ts: ts },
            );
            if r.pos != rec_end {
                r.pos = rec_end; // 容错推进
            }
        }
        tracing::info!(entries = map.len(), path = %self.path.display(), "offset log replayed");
        map
    }
}

impl Drop for OffsetLog {
    fn drop(&mut self) {
        // flush 由 append 内处理
    }
}
