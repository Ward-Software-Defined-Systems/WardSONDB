use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use parking_lot::RwLock;

use fjall::{Config, PartitionCreateOptions, PersistMode, TxKeyspace, TxPartitionHandle};
use tracing::{error, info};

use uuid::Uuid;

use crate::error::AppError;
use crate::index::IndexManager;

/// Memory configuration for fjall storage engine.
/// Prevents unbounded memory growth under sustained write load.
pub struct MemoryConfig {
    /// Unified block + blob cache size (default: 64 MiB)
    pub cache_size: u64,
    /// Total memory cap for all write buffers across all partitions (default: 64 MiB)
    pub max_write_buffer_size: u64,
    /// Per-partition memtable size limit (default: 8 MiB)
    pub max_memtable_size: u32,
    /// Number of background flush worker threads (default: 2)
    pub flush_workers: usize,
    /// Number of background compaction worker threads (default: 2)
    pub compaction_workers: usize,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        MemoryConfig {
            cache_size: 64 * 1024 * 1024,            // 64 MiB
            max_write_buffer_size: 64 * 1024 * 1024, // 64 MiB
            max_memtable_size: 8 * 1024 * 1024,      // 8 MiB
            flush_workers: 2,
            compaction_workers: 2,
        }
    }
}

/// Atomic document counters per collection — O(1) doc count reads.
pub struct DocCounters {
    counts: RwLock<HashMap<String, AtomicI64>>,
}

impl Default for DocCounters {
    fn default() -> Self {
        Self::new()
    }
}

impl DocCounters {
    pub fn new() -> Self {
        DocCounters {
            counts: RwLock::new(HashMap::new()),
        }
    }

    pub fn initialize(&self, collection: &str, count: i64) {
        let mut map = self.counts.write();
        map.insert(collection.to_string(), AtomicI64::new(count));
    }

    pub fn increment(&self, collection: &str, delta: i64) {
        let map = self.counts.read();
        if let Some(counter) = map.get(collection) {
            counter.fetch_add(delta, Ordering::Relaxed);
        }
    }

    pub fn get(&self, collection: &str) -> i64 {
        let map = self.counts.read();
        map.get(collection)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    pub fn remove(&self, collection: &str) {
        let mut map = self.counts.write();
        map.remove(collection);
    }
}

pub struct Storage {
    pub db: TxKeyspace,
    pub meta: TxPartitionHandle,
    pub index_manager: IndexManager,
    pub doc_counts: DocCounters,
    /// Set to true when fjall reports a poisoned keyspace (fatal flush/compaction failure).
    pub poisoned: AtomicBool,
    /// Memory config used for this instance (for stats reporting).
    pub memory_config: MemoryConfig,
    /// Default partition options with memory limits applied.
    partition_opts: PartitionCreateOptions,
}

impl Storage {
    pub fn open(data_dir: &Path) -> Result<Self, AppError> {
        Self::open_with_config(data_dir, MemoryConfig::default())
    }

    pub fn open_with_config(data_dir: &Path, mem: MemoryConfig) -> Result<Self, AppError> {
        info!(
            cache_mb = mem.cache_size / (1024 * 1024),
            write_buffer_mb = mem.max_write_buffer_size / (1024 * 1024),
            memtable_mb = mem.max_memtable_size / (1024 * 1024),
            flush_workers = mem.flush_workers,
            compaction_workers = mem.compaction_workers,
            "Opening database with memory limits"
        );

        let db = Config::new(data_dir)
            .cache_size(mem.cache_size)
            .max_write_buffer_size(mem.max_write_buffer_size)
            .flush_workers(mem.flush_workers)
            .compaction_workers(mem.compaction_workers)
            .open_transactional()
            .map_err(|e| AppError::Internal(format!("Failed to open database: {e}")))?;

        let partition_opts =
            PartitionCreateOptions::default().max_memtable_size(mem.max_memtable_size);

        let meta = db
            .open_partition("_meta", partition_opts.clone())
            .map_err(|e| AppError::Internal(format!("Failed to open meta partition: {e}")))?;

        let index_manager = IndexManager::new();
        let doc_counts = DocCounters::new();

        let storage = Storage {
            db,
            meta,
            index_manager,
            doc_counts,
            poisoned: AtomicBool::new(false),
            memory_config: mem,
            partition_opts,
        };

        // Load indexes from meta
        storage
            .index_manager
            .load_indexes(&storage.db, &storage.meta)?;

        // Initialize doc counters for all existing collections
        storage.initialize_doc_counts()?;

        Ok(storage)
    }

    /// Check if the storage engine is in a poisoned state.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Relaxed)
    }

    fn initialize_doc_counts(&self) -> Result<(), AppError> {
        let read_tx = self.db.read_tx();
        for kv in read_tx.prefix(&self.meta, "collection:") {
            let (key_bytes, _) = kv?;
            let key_str = std::str::from_utf8(key_bytes.as_ref())
                .map_err(|e| AppError::Internal(format!("Invalid key: {e}")))?;
            let col_name = key_str.strip_prefix("collection:").unwrap_or(key_str);

            let docs_partition = self.create_partition(&format!("{col_name}#docs"))?;
            let count_tx = self.db.read_tx();
            let count = count_tx.iter(&docs_partition).count() as i64;
            self.doc_counts.initialize(col_name, count);
        }
        Ok(())
    }

    pub fn create_partition(&self, name: &str) -> Result<TxPartitionHandle, AppError> {
        self.db
            .open_partition(name, self.partition_opts.clone())
            .map_err(|e| AppError::Internal(format!("Failed to create partition '{name}': {e}")))
    }

    /// Check a fjall result and detect poisoning. Sets the poisoned flag
    /// so the health endpoint can report degraded state.
    pub fn check_fjall_result<T>(&self, result: Result<T, fjall::Error>) -> Result<T, AppError> {
        match result {
            Ok(v) => Ok(v),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("oison") {
                    self.poisoned.store(true, Ordering::Relaxed);
                    error!(
                        "Storage engine POISONED — fatal background worker failure. \
                         Writes rejected, reads may continue. Restart required."
                    );
                }
                Err(e.into())
            }
        }
    }

    /// Guard that rejects writes immediately if the storage is already poisoned.
    pub fn check_not_poisoned(&self) -> Result<(), AppError> {
        if self.is_poisoned() {
            return Err(AppError::StoragePoisoned);
        }
        Ok(())
    }

    /// Get the oldest and newest document timestamps from UUIDv7 keys.
    pub fn get_doc_time_range(
        &self,
        collection: &str,
    ) -> Result<(Option<String>, Option<String>), AppError> {
        let docs_partition = self.get_docs_partition(collection)?;
        let read_tx = self.db.read_tx();

        let oldest = read_tx
            .iter(&docs_partition)
            .next()
            .and_then(|kv| kv.ok())
            .and_then(|(k, _)| uuid_key_to_timestamp(k.as_ref()));

        let newest = read_tx
            .iter(&docs_partition)
            .next_back()
            .and_then(|kv| kv.ok())
            .and_then(|(k, _)| uuid_key_to_timestamp(k.as_ref()));

        Ok((oldest, newest))
    }

    pub fn persist(&self) -> Result<(), AppError> {
        self.db
            .persist(PersistMode::Buffer)
            .map_err(|e| AppError::Internal(format!("Failed to persist: {e}")))
    }
}

/// Extract ISO 8601 timestamp from a UUIDv7 string key.
fn uuid_key_to_timestamp(key_bytes: &[u8]) -> Option<String> {
    let key_str = std::str::from_utf8(key_bytes).ok()?;
    let uuid = Uuid::parse_str(key_str).ok()?;
    let ts = uuid.get_timestamp()?;
    let (secs, _nanos) = ts.to_unix();
    let dt = chrono::DateTime::from_timestamp(secs as i64, 0)?;
    Some(dt.to_rfc3339())
}
