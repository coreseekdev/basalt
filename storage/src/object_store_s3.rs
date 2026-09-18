//! S3 兼容对象存储适配器（T-M4.3 v1.1，行动清单 P2）：按 basalt 四原语
//! 契约封装 arrow-rs `object_store`。目标端点 = 任意 S3 兼容服务（自研
//! duramen 已实现 If-None-Match:* 条件写——PutMode::Create 直接对应
//! basalt create-CAS 的回读契约）。
//!
//! 环境变量（from_env）：BASALT_S3_ENDPOINT / BASALT_S3_BUCKET /
//! BASALT_S3_KEY / BASALT_S3_SECRET / BASALT_S3_REGION(默认 us-east-1)。
//! trait 为同步接口：内嵌 current-thread runtime 承载 arrow-rs 的异步
//! future（对象存储调用都在 offloader 专用上下文，不占 actor 线程）。

use crate::error::{Result, StorageError};
use crate::object_store::CreateOutcome;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore as OsObjectStore, ObjectStoreExt as _, PutMode, PutOptions};
use std::sync::Arc;

pub struct S3ObjectStore {
    inner: Arc<dyn OsObjectStore>,
    rt: tokio::runtime::Runtime,
}

impl S3ObjectStore {
    /// 统一执行入口：tokio worker 内用 block_in_place（否则嵌套 block_on
    /// panic，fetch 任务静默死亡）；纯线程上下文（启动 GC/offloader 变体）
    /// 直接 block_on。
    fn block<F, T>(&self, fut: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| self.rt.block_on(fut))
        } else {
            self.rt.block_on(fut)
        }
    }
}

impl S3ObjectStore {
    pub fn from_env() -> Result<Self> {
        let env = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        let endpoint = env("BASALT_S3_ENDPOINT")
            .ok_or_else(|| StorageError::Other("BASALT_S3_ENDPOINT missing".into()))?;
        let bucket = env("BASALT_S3_BUCKET")
            .ok_or_else(|| StorageError::Other("BASALT_S3_BUCKET missing".into()))?;
        let key = env("BASALT_S3_KEY")
            .ok_or_else(|| StorageError::Other("BASALT_S3_KEY missing".into()))?;
        let secret = env("BASALT_S3_SECRET")
            .ok_or_else(|| StorageError::Other("BASALT_S3_SECRET missing".into()))?;
        let region = env("BASALT_S3_REGION").unwrap_or_else(|| "us-east-1".into());
        let inner: Arc<dyn OsObjectStore> = Arc::new(
            object_store::aws::AmazonS3Builder::new()
                .with_endpoint(&endpoint)
                .with_bucket_name(&bucket)
                .with_access_key_id(&key)
                .with_secret_access_key(&secret)
                .with_region(&region)
                .with_allow_http(true)
                .build()
                .map_err(|e| StorageError::Other(format!("s3 build: {e}")))?,
        );
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| StorageError::Other(format!("s3 runtime: {e}")))?;
        Ok(S3ObjectStore { inner, rt })
    }

    fn path(&self, key: &str) -> OsPath {
        OsPath::from(key.trim_start_matches('/'))
    }

    fn not_found(e: &object_store::Error) -> bool {
        matches!(e, object_store::Error::NotFound { .. })
            // S3 端点把 NoSuchBucket 包成 Generic 错误（404 + Code 标签）——
            // list 语义要求映射为空集（首次 PUT 隐式建桶）
            || e.to_string().contains("NoSuchBucket")
            || e.to_string().contains("404")
    }

    fn precondition(e: &object_store::Error) -> bool {
        matches!(e, object_store::Error::AlreadyExists { .. })
            || e.to_string().to_lowercase().contains("precondition")
    }
}

impl crate::object_store::ObjectStore for S3ObjectStore {
    fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.block(async {
            match self.inner.as_ref().get(&self.path(key)).await {
                Ok(resp) => {
                    let bytes = resp
                        .bytes()
                        .await
                        .map_err(|e| StorageError::Other(format!("s3 get body {key}: {e}")))?;
                    Ok(bytes.to_vec())
                }
                Err(e) if Self::not_found(&e) => Err(StorageError::OffsetOutOfRange(-1)),
                Err(e) => Err(StorageError::Other(format!("s3 get {key}: {e}"))),
            }
        })
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<()> {
        self.block(async {
            self.inner
                .as_ref()
                .put(&self.path(key), bytes.to_vec().into())
                .await
                .map_err(|e| StorageError::Other(format!("s3 put {key}: {e}")))?;
            Ok(())
        })
    }

    fn create(&self, key: &str, bytes: &[u8]) -> Result<CreateOutcome> {
        self.block(async {
            let opts = PutOptions::from(PutMode::Create);
            match self.inner.as_ref().put_opts(&self.path(key), bytes.to_vec().into(), opts).await {
                Ok(_) => Ok(CreateOutcome::Stored),
                Err(e) if Self::precondition(&e) => {
                    // 条件写撞车：回读已有字节（幂等成功 vs 冲突由调用方
                    // 比对判定——basalt create 契约）
                    let existing = self.get(key)?;
                    Ok(CreateOutcome::Existed(existing))
                }
                Err(e) => Err(StorageError::Other(format!("s3 create {key}: {e}"))),
            }
        })
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.block(async {
            match self.inner.as_ref().delete(&self.path(key)).await {
                Ok(()) => Ok(()),
                Err(e) if Self::not_found(&e) => Ok(()),
                Err(e) => Err(StorageError::Other(format!("s3 delete {key}: {e}"))),
            }
        })
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.block(async {
            // bucket 尚不存在（S3 语义：首次 PUT 隐式建桶）→ 空列表而非错
            match self
                .inner
                .as_ref()
                .list_with_delimiter(Some(&self.path(prefix)))
                .await
            {
                Ok(result) => Ok(result
                    .objects
                    .into_iter()
                    .map(|m| format!("/{}", m.location))
                    .collect()),
                Err(e) if Self::not_found(&e) => Ok(vec![]),
                Err(e) => Err(StorageError::Other(format!("s3 list {prefix}: {e}"))),
            }
        })
    }

    fn get_range(&self, key: &str, start: u64, len: usize) -> Result<Vec<u8>> {
        self.block(async {
            let loc = self.path(key);
            let flen = self
                .inner
                .as_ref()
                .head(&loc)
                .await
                .map_err(|e| StorageError::Other(format!("s3 head {key}: {e}")))?
                .size;
            let start = start.min(flen);
            let end = start + (len as u64).min(flen - start);
            let resp = self
                .inner
                .as_ref()
                .get_range(&loc, start..end)
                .await
                .map_err(|e| StorageError::Other(format!("s3 get_range {key}: {e}")))?;
            Ok(resp.to_vec())
        })
    }

    fn mtime_ms(&self, key: &str) -> Option<u64> {
        self.block(async {
            let meta = self.inner.as_ref().head(&self.path(key)).await.ok()?;
            Some(meta.last_modified.timestamp_millis() as u64)
        })
    }
}
