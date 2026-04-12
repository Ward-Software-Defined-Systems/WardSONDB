//! Fjall backend implementation.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use fjall::{Batch, Config, PartitionCreateOptions, PersistMode, TxKeyspace};

use super::{
    BackendError, BackendIterator, BackendResult, EngineConfig, KvPair, PartitionId,
    StorageBackend, WriteBatchWrapper,
};

pub struct FjallBackend {
    db: TxKeyspace,
    partition_opts: PartitionCreateOptions,
    poisoned: AtomicBool,
}

impl FjallBackend {
    pub fn open(path: &Path, config: &EngineConfig) -> BackendResult<Self> {
        let db = Config::new(path)
            .cache_size(config.cache_size_bytes)
            .max_write_buffer_size(config.write_buffer_bytes)
            .flush_workers(config.flush_workers)
            .compaction_workers(config.compaction_workers)
            .open_transactional()
            .map_err(|e| BackendError::Internal(format!("fjall open failed: {e}")))?;

        let partition_opts =
            PartitionCreateOptions::default().max_memtable_size(config.memtable_bytes);

        Ok(FjallBackend {
            db,
            partition_opts,
            poisoned: AtomicBool::new(false),
        })
    }

    fn check_poison<T>(&self, r: Result<T, fjall::Error>) -> BackendResult<T> {
        match r {
            Ok(v) => Ok(v),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("oison") {
                    self.poisoned.store(true, Ordering::Relaxed);
                    return Err(BackendError::Poisoned(msg));
                }
                Err(BackendError::Internal(msg))
            }
        }
    }
}

fn to_kv(r: Result<(fjall::Slice, fjall::Slice), fjall::Error>) -> BackendResult<KvPair> {
    match r {
        Ok((k, v)) => Ok((k.to_vec(), v.to_vec())),
        Err(e) => Err(BackendError::Internal(e.to_string())),
    }
}

impl StorageBackend for FjallBackend {
    fn create_or_open_partition(&self, name: &str) -> BackendResult<PartitionId> {
        let handle = self
            .db
            .open_partition(name, self.partition_opts.clone())
            .map_err(|e| BackendError::Internal(format!("open partition '{name}': {e}")))?;
        Ok(PartitionId::Fjall(handle))
    }

    fn get(&self, partition: &PartitionId, key: &[u8]) -> BackendResult<Option<Vec<u8>>> {
        let PartitionId::Fjall(handle) = partition else {
            return Err(BackendError::Internal(
                "PartitionId/backend mismatch".into(),
            ));
        };
        match handle.get(key) {
            Ok(Some(v)) => Ok(Some(v.to_vec())),
            Ok(None) => Ok(None),
            Err(e) => Err(BackendError::Internal(e.to_string())),
        }
    }

    fn prefix_iterator(
        &self,
        partition: &PartitionId,
        prefix: &[u8],
    ) -> BackendResult<BackendIterator> {
        let PartitionId::Fjall(handle) = partition else {
            return Err(BackendError::Internal(
                "PartitionId/backend mismatch".into(),
            ));
        };
        let rtx = self.db.read_tx();
        let items: Vec<BackendResult<KvPair>> = rtx.prefix(handle, prefix).map(to_kv).collect();
        Ok(BackendIterator::from_fjall(items))
    }

    fn prefix_iterator_rev(
        &self,
        partition: &PartitionId,
        prefix: &[u8],
    ) -> BackendResult<BackendIterator> {
        let PartitionId::Fjall(handle) = partition else {
            return Err(BackendError::Internal(
                "PartitionId/backend mismatch".into(),
            ));
        };
        let rtx = self.db.read_tx();
        let items: Vec<BackendResult<KvPair>> =
            rtx.prefix(handle, prefix).rev().map(to_kv).collect();
        Ok(BackendIterator::from_fjall(items))
    }

    fn range_iterator(
        &self,
        partition: &PartitionId,
        start: &[u8],
        end: &[u8],
    ) -> BackendResult<BackendIterator> {
        let PartitionId::Fjall(handle) = partition else {
            return Err(BackendError::Internal(
                "PartitionId/backend mismatch".into(),
            ));
        };
        let rtx = self.db.read_tx();
        let start_v = start.to_vec();
        let end_v = end.to_vec();
        let items: Vec<BackendResult<KvPair>> =
            rtx.range(handle, start_v..end_v).map(to_kv).collect();
        Ok(BackendIterator::from_fjall(items))
    }

    fn full_iterator(&self, partition: &PartitionId) -> BackendResult<BackendIterator> {
        let PartitionId::Fjall(handle) = partition else {
            return Err(BackendError::Internal(
                "PartitionId/backend mismatch".into(),
            ));
        };
        let rtx = self.db.read_tx();
        let items: Vec<BackendResult<KvPair>> = rtx.iter(handle).map(to_kv).collect();
        Ok(BackendIterator::from_fjall(items))
    }

    fn first_key(&self, partition: &PartitionId) -> BackendResult<Option<Vec<u8>>> {
        let PartitionId::Fjall(handle) = partition else {
            return Err(BackendError::Internal(
                "PartitionId/backend mismatch".into(),
            ));
        };
        let rtx = self.db.read_tx();
        match rtx.iter(handle).next() {
            Some(Ok((k, _))) => Ok(Some(k.to_vec())),
            Some(Err(e)) => Err(BackendError::Internal(e.to_string())),
            None => Ok(None),
        }
    }

    fn last_key(&self, partition: &PartitionId) -> BackendResult<Option<Vec<u8>>> {
        let PartitionId::Fjall(handle) = partition else {
            return Err(BackendError::Internal(
                "PartitionId/backend mismatch".into(),
            ));
        };
        let rtx = self.db.read_tx();
        match rtx.iter(handle).next_back() {
            Some(Ok((k, _))) => Ok(Some(k.to_vec())),
            Some(Err(e)) => Err(BackendError::Internal(e.to_string())),
            None => Ok(None),
        }
    }

    fn write_batch(&self) -> WriteBatchWrapper {
        let batch: Batch = self
            .db
            .inner()
            .batch()
            .durability(Some(PersistMode::Buffer));
        WriteBatchWrapper::Fjall(batch)
    }

    fn commit_batch(&self, batch: WriteBatchWrapper) -> BackendResult<()> {
        let WriteBatchWrapper::Fjall(b) = batch else {
            return Err(BackendError::Internal(
                "WriteBatchWrapper/backend mismatch".into(),
            ));
        };
        self.check_poison(b.commit())
    }

    fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Relaxed)
    }

    fn flush(&self) -> BackendResult<()> {
        self.db
            .persist(PersistMode::Buffer)
            .map_err(|e| BackendError::Internal(format!("persist failed: {e}")))
    }

    fn engine_name(&self) -> &'static str {
        "fjall"
    }
}
