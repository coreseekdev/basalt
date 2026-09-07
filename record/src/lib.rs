//! RecordBatch v2 编解码（TASK.md T-M0.1）。
//!
//! 范围：batch/record 头、varint/zigzag delta 编码（offsetDelta/timestampDelta）、
//! 压缩（none + lz4 起步，gzip/snappy/zstd 后补）、CRC32C 校验。
//! 只支持 magic v2（ADR-4）。
//!
//! 参考：Kafka `DefaultRecordBatch`/`DefaultRecord` 字段布局（docs/01 §7）。

/// RecordBatch v2 的魔术字节。
pub const MAGIC_V2: i8 = 2;

#[cfg(test)]
mod tests {
    #[test]
    fn magic_is_v2() {
        assert_eq!(super::MAGIC_V2, 2);
    }
}
