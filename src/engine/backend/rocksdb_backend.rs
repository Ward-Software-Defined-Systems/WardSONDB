//! RocksDB backend implementation.
//!
//! Uses `rust-rocksdb` with the `multi-threaded-cf` feature so column families
//! can be created / dropped concurrently with reads and writes. Memory is
//! bounded by a shared `LruCache` + `WriteBufferManager` (global memtable
//! budget), plus `cache_index_and_filter_blocks` so index/filter blocks are
//! charged against the cache instead of living outside it.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use rust_rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DB, DBCompressionType, Direction,
    IteratorMode, Options, WriteBatch, WriteBufferManager,
};

use super::{
    BackendError, BackendIterator, BackendResult, EngineConfig, KvPair, PartitionId,
    StorageBackend, WriteBatchWrapper,
};

pub struct RocksDbBackend {
    db: Arc<DB>,
    cache: Cache,
    // Kept alive for DB lifetime so the global memtable budget stays enforced.
    _wbm: WriteBufferManager,
    memtable_bytes: usize,
    poisoned: AtomicBool,
}

impl RocksDbBackend {
    pub fn open(path: &Path, config: &EngineConfig) -> BackendResult<Self> {
        let cache = Cache::new_lru_cache(config.cache_size_bytes as usize);
        let wbm = WriteBufferManager::new_write_buffer_manager_with_cache(
            config.write_buffer_bytes as usize,
            false,
            cache.clone(),
        );

        let memtable_bytes = config.memtable_bytes as usize;
        let bg_jobs = (config.flush_workers + config.compaction_workers) as i32;

        let build_cf_opts = || {
            let mut cf_opts = Options::default();
            cf_opts.set_write_buffer_size(memtable_bytes);
            cf_opts.set_max_write_buffer_number(2);

            let mut table_opts = BlockBasedOptions::default();
            table_opts.set_block_cache(&cache);
            table_opts.set_cache_index_and_filter_blocks(true);
            table_opts.set_bloom_filter(10.0, false);
            table_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
            cf_opts.set_block_based_table_factory(&table_opts);

            cf_opts.set_compression_per_level(&[
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
            ]);
            cf_opts
        };

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_write_buffer_manager(&wbm);
        db_opts.set_max_background_jobs(bg_jobs.max(2));
        // Apply table/compression to default CF as well.
        let mut table_opts = BlockBasedOptions::default();
        table_opts.set_block_cache(&cache);
        table_opts.set_cache_index_and_filter_blocks(true);
        table_opts.set_bloom_filter(10.0, false);
        table_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
        db_opts.set_block_based_table_factory(&table_opts);
        db_opts.set_compression_per_level(&[
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
        ]);

        // RocksDB requires all existing CFs to be listed on open.
        let existing_cfs = DB::list_cf(&db_opts, path).unwrap_or_default();
        let cf_descriptors: Vec<ColumnFamilyDescriptor> = if existing_cfs.is_empty() {
            vec![ColumnFamilyDescriptor::new("default", build_cf_opts())]
        } else {
            existing_cfs
                .into_iter()
                .map(|name| ColumnFamilyDescriptor::new(name, build_cf_opts()))
                .collect()
        };

        let db = DB::open_cf_descriptors(&db_opts, path, cf_descriptors)
            .map_err(|e| BackendError::Internal(format!("RocksDB open failed: {e}")))?;

        Ok(RocksDbBackend {
            db: Arc::new(db),
            cache,
            _wbm: wbm,
            memtable_bytes,
            poisoned: AtomicBool::new(false),
        })
    }

    fn cf_opts(&self) -> Options {
        let mut cf_opts = Options::default();
        cf_opts.set_write_buffer_size(self.memtable_bytes);
        cf_opts.set_max_write_buffer_number(2);

        let mut table_opts = BlockBasedOptions::default();
        table_opts.set_block_cache(&self.cache);
        table_opts.set_cache_index_and_filter_blocks(true);
        table_opts.set_bloom_filter(10.0, false);
        table_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
        cf_opts.set_block_based_table_factory(&table_opts);

        cf_opts.set_compression_per_level(&[
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
        ]);
        cf_opts
    }
}

fn rocks_err<E: std::fmt::Display>(e: E) -> BackendError {
    BackendError::Internal(e.to_string())
}

fn unwrap_rocks(partition: &PartitionId) -> BackendResult<(&Arc<DB>, &Arc<str>)> {
    match partition {
        PartitionId::RocksDb { db, cf_name } => Ok((db, cf_name)),
        _ => Err(BackendError::Internal(
            "PartitionId/backend mismatch".into(),
        )),
    }
}

impl StorageBackend for RocksDbBackend {
    fn create_or_open_partition(&self, name: &str) -> BackendResult<PartitionId> {
        if self.db.cf_handle(name).is_none() {
            self.db
                .create_cf(name, &self.cf_opts())
                .map_err(|e| BackendError::Internal(format!("create_cf '{name}': {e}")))?;
        }
        Ok(PartitionId::RocksDb {
            db: self.db.clone(),
            cf_name: Arc::from(name),
        })
    }

    fn get(&self, partition: &PartitionId, key: &[u8]) -> BackendResult<Option<Vec<u8>>> {
        let (db, cf_name) = unwrap_rocks(partition)?;
        let cf = db
            .cf_handle(cf_name)
            .ok_or_else(|| BackendError::Internal(format!("CF not found: {cf_name}")))?;
        db.get_cf(&cf, key).map_err(rocks_err)
    }

    fn prefix_iterator(
        &self,
        partition: &PartitionId,
        prefix: &[u8],
    ) -> BackendResult<BackendIterator> {
        let (db, cf_name) = unwrap_rocks(partition)?;
        let cf = db
            .cf_handle(cf_name)
            .ok_or_else(|| BackendError::Internal(format!("CF not found: {cf_name}")))?;
        let raw = db.iterator_cf(&cf, IteratorMode::From(prefix, Direction::Forward));
        let prefix_owned = prefix.to_vec();
        let mut materialized: Vec<BackendResult<KvPair>> = Vec::new();
        for item in raw {
            match item {
                Ok((k, v)) => {
                    if !k.starts_with(&prefix_owned) {
                        break;
                    }
                    materialized.push(Ok((k.to_vec(), v.to_vec())));
                }
                Err(e) => {
                    materialized.push(Err(rocks_err(e)));
                    break;
                }
            }
        }
        Ok(BackendIterator::from_rocksdb(Box::new(
            materialized.into_iter(),
        )))
    }

    fn range_iterator(
        &self,
        partition: &PartitionId,
        start: &[u8],
        end: &[u8],
        max_results: Option<usize>,
    ) -> BackendResult<BackendIterator> {
        let (db, cf_name) = unwrap_rocks(partition)?;
        let cf = db
            .cf_handle(cf_name)
            .ok_or_else(|| BackendError::Internal(format!("CF not found: {cf_name}")))?;
        let raw = db.iterator_cf(&cf, IteratorMode::From(start, Direction::Forward));
        let end_owned = end.to_vec();
        let cap = max_results.unwrap_or(usize::MAX);
        let mut materialized: Vec<BackendResult<KvPair>> = Vec::new();
        for item in raw {
            match item {
                Ok((k, v)) => {
                    if k.as_ref() >= end_owned.as_slice() || materialized.len() >= cap {
                        break;
                    }
                    materialized.push(Ok((k.to_vec(), v.to_vec())));
                }
                Err(e) => {
                    materialized.push(Err(rocks_err(e)));
                    break;
                }
            }
        }
        Ok(BackendIterator::from_rocksdb(Box::new(
            materialized.into_iter(),
        )))
    }

    fn range_iterator_rev(
        &self,
        partition: &PartitionId,
        start: &[u8],
        end: &[u8],
        max_results: Option<usize>,
    ) -> BackendResult<BackendIterator> {
        let (db, cf_name) = unwrap_rocks(partition)?;
        let cf = db
            .cf_handle(cf_name)
            .ok_or_else(|| BackendError::Internal(format!("CF not found: {cf_name}")))?;
        // From(end, Reverse) positions at the largest key <= end; `end` is
        // exclusive, so a key equal to it is skipped by the bound check.
        let raw = db.iterator_cf(&cf, IteratorMode::From(end, Direction::Reverse));
        let start_owned = start.to_vec();
        let end_owned = end.to_vec();
        let cap = max_results.unwrap_or(usize::MAX);
        let mut materialized: Vec<BackendResult<KvPair>> = Vec::new();
        for item in raw {
            match item {
                Ok((k, v)) => {
                    if k.as_ref() >= end_owned.as_slice() {
                        continue; // only the seek key itself can land here
                    }
                    if k.as_ref() < start_owned.as_slice() || materialized.len() >= cap {
                        break;
                    }
                    materialized.push(Ok((k.to_vec(), v.to_vec())));
                }
                Err(e) => {
                    materialized.push(Err(rocks_err(e)));
                    break;
                }
            }
        }
        Ok(BackendIterator::from_rocksdb(Box::new(
            materialized.into_iter(),
        )))
    }

    fn full_iterator(&self, partition: &PartitionId) -> BackendResult<BackendIterator> {
        let (db, cf_name) = unwrap_rocks(partition)?;
        let cf = db
            .cf_handle(cf_name)
            .ok_or_else(|| BackendError::Internal(format!("CF not found: {cf_name}")))?;
        let raw = db.iterator_cf(&cf, IteratorMode::Start);
        let mut materialized: Vec<BackendResult<KvPair>> = Vec::new();
        for item in raw {
            match item {
                Ok((k, v)) => materialized.push(Ok((k.to_vec(), v.to_vec()))),
                Err(e) => {
                    materialized.push(Err(rocks_err(e)));
                    break;
                }
            }
        }
        Ok(BackendIterator::from_rocksdb(Box::new(
            materialized.into_iter(),
        )))
    }

    fn count_prefix(&self, partition: &PartitionId, prefix: &[u8]) -> BackendResult<u64> {
        let (db, cf_name) = unwrap_rocks(partition)?;
        let cf = db
            .cf_handle(cf_name)
            .ok_or_else(|| BackendError::Internal(format!("CF not found: {cf_name}")))?;
        // Raw iterator: keys are only borrowed to test the prefix, values are
        // never copied. status() after the loop surfaces iteration errors.
        let mut iter = db.raw_iterator_cf(&cf);
        iter.seek(prefix);
        let mut count = 0u64;
        while let Some(k) = iter.key() {
            if !k.starts_with(prefix) {
                break;
            }
            count += 1;
            iter.next();
        }
        iter.status().map_err(rocks_err)?;
        Ok(count)
    }

    fn count_range(&self, partition: &PartitionId, start: &[u8], end: &[u8]) -> BackendResult<u64> {
        if start >= end {
            return Ok(0);
        }
        let (db, cf_name) = unwrap_rocks(partition)?;
        let cf = db
            .cf_handle(cf_name)
            .ok_or_else(|| BackendError::Internal(format!("CF not found: {cf_name}")))?;
        let mut iter = db.raw_iterator_cf(&cf);
        iter.seek(start);
        let mut count = 0u64;
        while let Some(k) = iter.key() {
            if k >= end {
                break;
            }
            count += 1;
            iter.next();
        }
        iter.status().map_err(rocks_err)?;
        Ok(count)
    }

    fn first_key(&self, partition: &PartitionId) -> BackendResult<Option<Vec<u8>>> {
        let (db, cf_name) = unwrap_rocks(partition)?;
        let cf = db
            .cf_handle(cf_name)
            .ok_or_else(|| BackendError::Internal(format!("CF not found: {cf_name}")))?;
        let mut iter = db.iterator_cf(&cf, IteratorMode::Start);
        match iter.next() {
            Some(Ok((k, _))) => Ok(Some(k.to_vec())),
            Some(Err(e)) => Err(rocks_err(e)),
            None => Ok(None),
        }
    }

    fn last_key(&self, partition: &PartitionId) -> BackendResult<Option<Vec<u8>>> {
        let (db, cf_name) = unwrap_rocks(partition)?;
        let cf = db
            .cf_handle(cf_name)
            .ok_or_else(|| BackendError::Internal(format!("CF not found: {cf_name}")))?;
        let mut iter = db.iterator_cf(&cf, IteratorMode::End);
        match iter.next() {
            Some(Ok((k, _))) => Ok(Some(k.to_vec())),
            Some(Err(e)) => Err(rocks_err(e)),
            None => Ok(None),
        }
    }

    fn write_batch(&self) -> WriteBatchWrapper {
        WriteBatchWrapper::RocksDb {
            batch: WriteBatch::default(),
            db: self.db.clone(),
        }
    }

    fn commit_batch(&self, batch: WriteBatchWrapper) -> BackendResult<()> {
        let WriteBatchWrapper::RocksDb { batch, db } = batch else {
            return Err(BackendError::Internal(
                "WriteBatchWrapper/backend mismatch".into(),
            ));
        };
        match db.write(&batch) {
            Ok(()) => Ok(()),
            Err(e) => {
                let msg = e.to_string();
                let lower = msg.to_lowercase();
                if lower.contains("corruption") || lower.contains("io error") {
                    self.poisoned.store(true, Ordering::Relaxed);
                    return Err(BackendError::Poisoned(msg));
                }
                Err(BackendError::Internal(msg))
            }
        }
    }

    fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Relaxed)
    }

    fn flush(&self) -> BackendResult<()> {
        // flush_wal(false) syncs the WAL buffer; equivalent to fjall's
        // PersistMode::Buffer — durability up to this point, without fsync cost.
        self.db.flush_wal(false).map_err(rocks_err)
    }

    fn engine_name(&self) -> &'static str {
        "rocksdb"
    }
}
