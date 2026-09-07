//! 稀疏索引（内存结构；段密封时落盘 .index/.timeindex，Kafka 同布局）。
//!
//! - offset 索引项：(rel_offset:u32, physical_pos:u32)——每 INDEX_INTERVAL 字节一项；
//! - time 索引项：(timestamp:i64, rel_offset:u32)——与 offset 索引同节奏。
//! 查询：二分定位 ≤ 目标的最近项，再顺序扫描批头。

#[derive(Debug, Clone, Default)]
pub struct OffsetIndex {
    pub entries: Vec<(u32, u32)>, // (rel_offset, pos)
}

impl OffsetIndex {
    pub fn push(&mut self, rel_offset: u32, pos: u32) {
        if self.entries.last().is_none_or(|&(o, _)| o < rel_offset) {
            self.entries.push((rel_offset, pos));
        }
    }

    /// ≤ target 的最近项位置（无则 0）。
    pub fn lookup(&self, target: i64) -> u32 {
        let rel = target.max(0) as u32;
        match self.entries.binary_search_by(|&(o, _)| o.cmp(&rel)) {
            Ok(i) => self.entries[i].1,
            Err(0) => 0,
            Err(i) => self.entries[i - 1].1,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.entries.len() * 8);
        for &(o, p) in &self.entries {
            out.extend_from_slice(&o.to_be_bytes());
            out.extend_from_slice(&p.to_be_bytes());
        }
        out
    }

    pub fn decode(data: &[u8]) -> OffsetIndex {
        let mut entries = Vec::with_capacity(data.len() / 8);
        for ch in data.chunks_exact(8) {
            entries.push((
                u32::from_be_bytes([ch[0], ch[1], ch[2], ch[3]]),
                u32::from_be_bytes([ch[4], ch[5], ch[6], ch[7]]),
            ));
        }
        OffsetIndex { entries }
    }
}

#[derive(Debug, Clone, Default)]
pub struct TimeIndex {
    pub entries: Vec<(i64, u32)>, // (max_timestamp, rel_offset)
}

impl TimeIndex {
    pub fn push(&mut self, ts: i64, rel_offset: u32) {
        if self.entries.last().is_none_or(|&(t, _)| t < ts) {
            self.entries.push((ts, rel_offset));
        }
    }

    /// 第一个 max_timestamp >= target 的项的 rel_offset（时间升序批流）。
    pub fn lookup(&self, target: i64) -> Option<(i64, u32)> {
        let i = self.entries.partition_point(|&(t, _)| t < target);
        self.entries.get(i).copied()
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.entries.len() * 12);
        for &(t, o) in &self.entries {
            out.extend_from_slice(&t.to_be_bytes());
            out.extend_from_slice(&o.to_be_bytes());
        }
        out
    }

    pub fn decode(data: &[u8]) -> TimeIndex {
        let mut entries = Vec::with_capacity(data.len() / 12);
        for ch in data.chunks_exact(12) {
            entries.push((
                i64::from_be_bytes([
                    ch[0], ch[1], ch[2], ch[3], ch[4], ch[5], ch[6], ch[7],
                ]),
                u32::from_be_bytes([ch[8], ch[9], ch[10], ch[11]]),
            ));
        }
        TimeIndex { entries }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_index_lookup() {
        let mut ix = OffsetIndex::default();
        ix.push(0, 0);
        ix.push(10, 4096);
        ix.push(20, 8192);
        assert_eq!(ix.lookup(0), 0);
        assert_eq!(ix.lookup(9), 0);
        assert_eq!(ix.lookup(10), 4096);
        assert_eq!(ix.lookup(15), 4096);
        assert_eq!(ix.lookup(25), 8192);
    }

    #[test]
    fn time_index_lookup() {
        let mut tx = TimeIndex::default();
        tx.push(100, 0);
        tx.push(200, 4096);
        assert_eq!(tx.lookup(50), Some((100, 0)));
        assert_eq!(tx.lookup(150), Some((200, 4096)));
        assert_eq!(tx.lookup(201), None);
        assert_eq!(tx.lookup(200), Some((200, 4096)));
    }

    #[test]
    fn encode_decode() {
        let mut ix = OffsetIndex::default();
        ix.push(5, 1024);
        let bytes = ix.encode();
        assert_eq!(OffsetIndex::decode(&bytes).entries, ix.entries);
        let mut tx = TimeIndex::default();
        tx.push(-5, 8);
        let bytes = tx.encode();
        assert_eq!(TimeIndex::decode(&bytes).entries, tx.entries);
    }
}
