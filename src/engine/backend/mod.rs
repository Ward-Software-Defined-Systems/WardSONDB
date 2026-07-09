//! Storage backend abstraction.
//!
//! WardSONDB supports two pluggable KV engines: fjall and RocksDB. This module
//! defines the [`StorageBackend`] trait plus the enum-dispatched [`Engine`] type
//! that the rest of the codebase talks to. Enum dispatch avoids propagating a
//! generic parameter through every module while still monomorphizing calls.

pub mod fjall_backend;
pub mod rocksdb_backend;

use std::path::Path;

use crate::error::AppError;

pub use fjall_backend::FjallBackend;
pub use rocksdb_backend::RocksDbBackend;

/// Tunables passed to a backend on open. Sizes are in bytes.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub cache_size_bytes: u64,
    pub write_buffer_bytes: u64,
    pub memtable_bytes: u32,
    pub flush_workers: usize,
    pub compaction_workers: usize,
}

/// Errors returned by the storage backend.
#[derive(Debug)]
pub enum BackendError {
    /// Generic I/O or internal engine error.
    Internal(String),
    /// Engine has entered a fatal state — no more writes should be accepted.
    Poisoned(String),
}

impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackendError::Internal(m) => write!(f, "backend internal: {m}"),
            BackendError::Poisoned(m) => write!(f, "backend poisoned: {m}"),
        }
    }
}

impl std::error::Error for BackendError {}

impl From<BackendError> for AppError {
    fn from(e: BackendError) -> Self {
        match e {
            BackendError::Poisoned(msg) => {
                tracing::error!(
                    error = %msg,
                    "FATAL: Storage engine poisoned — background worker failed. \
                     All writes will be rejected. Restart required."
                );
                AppError::StoragePoisoned
            }
            BackendError::Internal(msg) => {
                tracing::error!(error = %msg, "Storage engine error");
                AppError::Internal(format!("Storage error: {msg}"))
            }
        }
    }
}

pub type BackendResult<T> = Result<T, BackendError>;
pub type KvPair = (Vec<u8>, Vec<u8>);

/// Opaque handle to a partition (fjall partition / RocksDB column family).
///
/// Clone is cheap — both variants wrap an Arc-like internal handle.
#[derive(Clone)]
pub enum PartitionId {
    Fjall(::fjall::TxPartitionHandle),
    RocksDb {
        db: std::sync::Arc<::rust_rocksdb::DB>,
        cf_name: std::sync::Arc<str>,
    },
}

/// Atomic write batch. All staged mutations commit together.
///
/// For fjall we use `Batch` (non-serialized but atomic on commit) rather than
/// `WriteTransaction` — the transaction type borrows a `MutexGuard` from the
/// keyspace, which would force a lifetime parameter all the way up through
/// `Storage`. WardSONDB never uses read-modify-write inside a transaction, so
/// the weaker atomicity of `Batch` is sufficient.
pub enum WriteBatchWrapper {
    Fjall(::fjall::Batch),
    RocksDb {
        batch: ::rust_rocksdb::WriteBatch,
        db: std::sync::Arc<::rust_rocksdb::DB>,
    },
}

impl WriteBatchWrapper {
    pub fn insert(&mut self, partition: &PartitionId, key: &[u8], value: &[u8]) {
        match (self, partition) {
            (WriteBatchWrapper::Fjall(batch), PartitionId::Fjall(handle)) => {
                batch.insert(handle.inner(), key, value);
            }
            (WriteBatchWrapper::RocksDb { batch, db }, PartitionId::RocksDb { cf_name, .. }) => {
                let cf = db.cf_handle(cf_name).expect("CF must exist at insert time");
                batch.put_cf(&cf, key, value);
            }
            _ => panic!("WriteBatchWrapper / PartitionId backend mismatch"),
        }
    }

    pub fn remove(&mut self, partition: &PartitionId, key: &[u8]) {
        match (self, partition) {
            (WriteBatchWrapper::Fjall(batch), PartitionId::Fjall(handle)) => {
                batch.remove(handle.inner(), key);
            }
            (WriteBatchWrapper::RocksDb { batch, db }, PartitionId::RocksDb { cf_name, .. }) => {
                let cf = db.cf_handle(cf_name).expect("CF must exist at remove time");
                batch.delete_cf(&cf, key);
            }
            _ => panic!("WriteBatchWrapper / PartitionId backend mismatch"),
        }
    }
}

/// Streaming iterator over owned key-value pairs. Owning the bytes decouples
/// the iterator from any read transaction / snapshot lifetime, which lets
/// callers break early (critical for IndexSorted and count_only paths).
pub struct BackendIterator {
    inner: BackendIteratorInner,
}

enum BackendIteratorInner {
    /// fjall iterators borrow from a read transaction, so we collect up front.
    Fjall(std::vec::IntoIter<BackendResult<KvPair>>),
    /// RocksDB iterators also buffer before returning: DBIterator borrows the
    /// DB, but this type must be 'static. Callers that only need a page pass
    /// `max_results` to the range methods to bound the buffering.
    RocksDb(Box<dyn Iterator<Item = BackendResult<KvPair>> + Send>),
}

impl BackendIterator {
    pub(crate) fn from_fjall(items: Vec<BackendResult<KvPair>>) -> Self {
        BackendIterator {
            inner: BackendIteratorInner::Fjall(items.into_iter()),
        }
    }

    pub(crate) fn from_rocksdb(
        iter: Box<dyn Iterator<Item = BackendResult<KvPair>> + Send>,
    ) -> Self {
        BackendIterator {
            inner: BackendIteratorInner::RocksDb(iter),
        }
    }
}

impl Iterator for BackendIterator {
    type Item = BackendResult<KvPair>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            BackendIteratorInner::Fjall(it) => it.next(),
            BackendIteratorInner::RocksDb(it) => it.next(),
        }
    }
}

// ─── Trait ───────────────────────────────────────────────────────────

pub trait StorageBackend: Send + Sync {
    fn create_or_open_partition(&self, name: &str) -> BackendResult<PartitionId>;
    fn get(&self, partition: &PartitionId, key: &[u8]) -> BackendResult<Option<Vec<u8>>>;
    fn prefix_iterator(
        &self,
        partition: &PartitionId,
        prefix: &[u8],
    ) -> BackendResult<BackendIterator>;
    /// Iterate keys `k` with `start <= k < end` in ASCENDING byte order.
    /// `max_results: Some(n)` reads and buffers at most n pairs (for
    /// limit+1-style page probes); `None` is unbounded.
    fn range_iterator(
        &self,
        partition: &PartitionId,
        start: &[u8],
        end: &[u8],
        max_results: Option<usize>,
    ) -> BackendResult<BackendIterator>;
    /// Iterate the same half-open key set `start <= k < end` in DESCENDING
    /// byte order, i.e. starting from the largest key strictly below `end`.
    /// Used for sorted descending scans with early termination and for
    /// descending cursor seeks. `max_results` as on `range_iterator`.
    fn range_iterator_rev(
        &self,
        partition: &PartitionId,
        start: &[u8],
        end: &[u8],
        max_results: Option<usize>,
    ) -> BackendResult<BackendIterator>;
    fn full_iterator(&self, partition: &PartitionId) -> BackendResult<BackendIterator>;
    fn first_key(&self, partition: &PartitionId) -> BackendResult<Option<Vec<u8>>>;
    fn last_key(&self, partition: &PartitionId) -> BackendResult<Option<Vec<u8>>>;
    fn write_batch(&self) -> WriteBatchWrapper;
    fn commit_batch(&self, batch: WriteBatchWrapper) -> BackendResult<()>;
    fn is_poisoned(&self) -> bool;
    fn flush(&self) -> BackendResult<()>;
    fn engine_name(&self) -> &'static str;
}

// ─── Engine enum (delegating dispatch) ───────────────────────────────

pub enum Engine {
    Fjall(FjallBackend),
    RocksDb(RocksDbBackend),
}

impl Engine {
    pub fn open(engine_type: &str, path: &Path, config: &EngineConfig) -> BackendResult<Self> {
        match engine_type {
            "fjall" => Ok(Engine::Fjall(FjallBackend::open(path, config)?)),
            "rocksdb" => Ok(Engine::RocksDb(RocksDbBackend::open(path, config)?)),
            other => Err(BackendError::Internal(format!(
                "Unknown storage engine '{other}' (expected 'rocksdb' or 'fjall')"
            ))),
        }
    }
}

impl StorageBackend for Engine {
    fn create_or_open_partition(&self, name: &str) -> BackendResult<PartitionId> {
        match self {
            Engine::Fjall(b) => b.create_or_open_partition(name),
            Engine::RocksDb(b) => b.create_or_open_partition(name),
        }
    }
    fn get(&self, partition: &PartitionId, key: &[u8]) -> BackendResult<Option<Vec<u8>>> {
        match self {
            Engine::Fjall(b) => b.get(partition, key),
            Engine::RocksDb(b) => b.get(partition, key),
        }
    }
    fn prefix_iterator(
        &self,
        partition: &PartitionId,
        prefix: &[u8],
    ) -> BackendResult<BackendIterator> {
        match self {
            Engine::Fjall(b) => b.prefix_iterator(partition, prefix),
            Engine::RocksDb(b) => b.prefix_iterator(partition, prefix),
        }
    }
    fn range_iterator(
        &self,
        partition: &PartitionId,
        start: &[u8],
        end: &[u8],
        max_results: Option<usize>,
    ) -> BackendResult<BackendIterator> {
        match self {
            Engine::Fjall(b) => b.range_iterator(partition, start, end, max_results),
            Engine::RocksDb(b) => b.range_iterator(partition, start, end, max_results),
        }
    }
    fn range_iterator_rev(
        &self,
        partition: &PartitionId,
        start: &[u8],
        end: &[u8],
        max_results: Option<usize>,
    ) -> BackendResult<BackendIterator> {
        match self {
            Engine::Fjall(b) => b.range_iterator_rev(partition, start, end, max_results),
            Engine::RocksDb(b) => b.range_iterator_rev(partition, start, end, max_results),
        }
    }
    fn full_iterator(&self, partition: &PartitionId) -> BackendResult<BackendIterator> {
        match self {
            Engine::Fjall(b) => b.full_iterator(partition),
            Engine::RocksDb(b) => b.full_iterator(partition),
        }
    }
    fn first_key(&self, partition: &PartitionId) -> BackendResult<Option<Vec<u8>>> {
        match self {
            Engine::Fjall(b) => b.first_key(partition),
            Engine::RocksDb(b) => b.first_key(partition),
        }
    }
    fn last_key(&self, partition: &PartitionId) -> BackendResult<Option<Vec<u8>>> {
        match self {
            Engine::Fjall(b) => b.last_key(partition),
            Engine::RocksDb(b) => b.last_key(partition),
        }
    }
    fn write_batch(&self) -> WriteBatchWrapper {
        match self {
            Engine::Fjall(b) => b.write_batch(),
            Engine::RocksDb(b) => b.write_batch(),
        }
    }
    fn commit_batch(&self, batch: WriteBatchWrapper) -> BackendResult<()> {
        match self {
            Engine::Fjall(b) => b.commit_batch(batch),
            Engine::RocksDb(b) => b.commit_batch(batch),
        }
    }
    fn is_poisoned(&self) -> bool {
        match self {
            Engine::Fjall(b) => b.is_poisoned(),
            Engine::RocksDb(b) => b.is_poisoned(),
        }
    }
    fn flush(&self) -> BackendResult<()> {
        match self {
            Engine::Fjall(b) => b.flush(),
            Engine::RocksDb(b) => b.flush(),
        }
    }
    fn engine_name(&self) -> &'static str {
        match self {
            Engine::Fjall(b) => b.engine_name(),
            Engine::RocksDb(b) => b.engine_name(),
        }
    }
}
