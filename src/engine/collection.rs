use fjall::TxPartitionHandle;
use serde::{Deserialize, Serialize};

use crate::error::AppError;

use super::storage::Storage;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionInfo {
    pub name: String,
    pub doc_count: u64,
    pub indexes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CollectionMeta {
    name: String,
    created_at: String,
}

impl Storage {
    pub fn create_collection(&self, name: &str) -> Result<CollectionInfo, AppError> {
        self.check_not_poisoned()?;
        let meta_key = format!("collection:{name}");
        if self.meta.get(&meta_key)?.is_some() {
            return Err(AppError::CollectionExists(name.to_string()));
        }

        // Create the docs partition
        let ks_name = format!("{name}#docs");
        self.create_partition(&ks_name)?;

        // Store collection metadata
        let meta = CollectionMeta {
            name: name.to_string(),
            created_at: chrono::Utc::now().to_rfc3339(),
        };
        let meta_bytes = serde_json::to_vec(&meta)?;

        let mut tx = self.db.write_tx();
        tx.insert(&self.meta, &meta_key, meta_bytes);
        self.check_fjall_result(tx.commit())?;

        self.persist()?;

        // Initialize doc counter
        self.doc_counts.initialize(name, 0);

        Ok(CollectionInfo {
            name: name.to_string(),
            doc_count: 0,
            indexes: vec![],
        })
    }

    pub fn list_collections(&self) -> Result<Vec<CollectionInfo>, AppError> {
        let mut collections = Vec::new();
        let read_tx = self.db.read_tx();
        for kv in read_tx.prefix(&self.meta, "collection:") {
            let (key, _value) = kv?;
            let key_str = std::str::from_utf8(key.as_ref())
                .map_err(|e| AppError::Internal(format!("Invalid key: {e}")))?;
            let col_name = key_str
                .strip_prefix("collection:")
                .unwrap_or(key_str)
                .to_string();

            let doc_count = self.doc_counts.get(&col_name).max(0) as u64;
            let indexes = self
                .index_manager
                .get_indexes_for_collection(&col_name)
                .iter()
                .map(|d| d.name.clone())
                .collect();

            collections.push(CollectionInfo {
                name: col_name,
                doc_count,
                indexes,
            });
        }
        Ok(collections)
    }

    pub fn get_collection_info(&self, name: &str) -> Result<CollectionInfo, AppError> {
        let meta_key = format!("collection:{name}");
        if self.meta.get(&meta_key)?.is_none() {
            return Err(AppError::CollectionNotFound(name.to_string()));
        }

        let doc_count = self.doc_counts.get(name).max(0) as u64;
        let indexes = self
            .index_manager
            .get_indexes_for_collection(name)
            .iter()
            .map(|d| d.name.clone())
            .collect();

        Ok(CollectionInfo {
            name: name.to_string(),
            doc_count,
            indexes,
        })
    }

    pub fn drop_collection(&self, name: &str) -> Result<(), AppError> {
        self.check_not_poisoned()?;
        let meta_key = format!("collection:{name}");
        if self.meta.get(&meta_key)?.is_none() {
            return Err(AppError::CollectionNotFound(name.to_string()));
        }

        let docs_partition = self.get_docs_partition(name)?;

        // Collect all doc keys
        let read_tx = self.db.read_tx();
        let keys: Vec<Vec<u8>> = read_tx
            .iter(&docs_partition)
            .filter_map(|kv| kv.ok().map(|(k, _)| k.as_ref().to_vec()))
            .collect();
        drop(read_tx);

        // Also drop all indexes for this collection
        let index_defs = self.index_manager.get_indexes_for_collection(name);

        let mut tx = self.db.write_tx();

        // Remove all documents
        for key in &keys {
            tx.remove(&docs_partition, key.as_slice());
        }

        // Remove all index entries and meta keys
        for idx_def in &index_defs {
            if let Some(idx_partition) = self.index_manager.get_index_partition(name, &idx_def.name)
            {
                let rtx = self.db.read_tx();
                let idx_keys: Vec<Vec<u8>> = rtx
                    .iter(&idx_partition)
                    .filter_map(|kv| kv.ok().map(|(k, _)| k.as_ref().to_vec()))
                    .collect();
                drop(rtx);
                for key in &idx_keys {
                    tx.remove(&idx_partition, key.as_slice());
                }
            }
            let idx_meta_key = format!("index:{}:{}", name, idx_def.name);
            tx.remove(&self.meta, idx_meta_key.as_str());
        }

        tx.remove(&self.meta, meta_key.as_str());
        self.check_fjall_result(tx.commit())?;

        // Unregister indexes from cache
        for idx_def in &index_defs {
            self.index_manager.unregister(name, &idx_def.name);
        }

        // Remove doc counter
        self.doc_counts.remove(name);

        self.persist()?;
        Ok(())
    }

    pub fn collection_exists(&self, name: &str) -> Result<bool, AppError> {
        let meta_key = format!("collection:{name}");
        Ok(self.meta.get(&meta_key)?.is_some())
    }

    pub fn get_docs_partition(&self, collection: &str) -> Result<TxPartitionHandle, AppError> {
        let ks_name = format!("{collection}#docs");
        self.create_partition(&ks_name)
    }

    #[allow(dead_code)]
    pub fn collection_doc_count(&self, name: &str) -> Result<u64, AppError> {
        Ok(self.doc_counts.get(name).max(0) as u64)
    }
}
