//! 尺寸类内存池（性能纪律：热路径缓冲复用）。
//!
//! acquire 按请求尺寸向上取 2 的幂（4KiB..16MiB）分配/复用；release 归还。
//! 内部 Mutex 仅保护空闲链表，不属于「共享可变所有权」禁令范畴（见根 Cargo.toml）。

use bytes::BytesMut;
use std::collections::HashMap;
use std::sync::Mutex;

const MIN_CLASS: usize = 4 * 1024;
const MAX_CLASS: usize = 16 * 1024 * 1024;

#[derive(Default)]
pub struct BufferPool {
    free: Mutex<HashMap<usize, Vec<BytesMut>>>,
}

impl BufferPool {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn class_of(size: usize) -> usize {
        let mut c = MIN_CLASS;
        while c < size && c < MAX_CLASS {
            c <<= 1;
        }
        c
    }

    pub fn acquire(&self, size: usize) -> BytesMut {
        let class = Self::class_of(size);
        let mut free = self.free.lock().unwrap();
        match free.get_mut(&class).and_then(|v| v.pop()) {
            Some(mut buf) => {
                buf.clear();
                buf
            }
            None => BytesMut::with_capacity(class),
        }
    }

    pub fn release(&self, buf: BytesMut) {
        let cap = buf.capacity();
        if cap < MIN_CLASS || cap > MAX_CLASS {
            return;
        }
        let class = Self::class_of(cap);
        let mut free = self.free.lock().unwrap();
        let bucket = free.entry(class).or_default();
        if bucket.len() < 64 {
            bucket.push(buf);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reuse_roundtrip() {
        let pool = BufferPool::new();
        let mut b = pool.acquire(100);
        assert!(b.capacity() >= MIN_CLASS);
        b.extend_from_slice(b"hello");
        pool.release(b);
        let c = pool.acquire(1);
        assert_eq!(c.len(), 0);
        assert!(c.capacity() >= MIN_CLASS);
    }
}
