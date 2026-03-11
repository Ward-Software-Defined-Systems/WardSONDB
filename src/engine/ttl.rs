use serde::{Deserialize, Serialize};
use tracing::info;

use crate::error::AppError;
use crate::query::filter::parse_filter;

use super::storage::Storage;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TtlConfig {
    pub retention_days: u64,
    pub field: String,
    pub enabled: bool,
}

impl Storage {
    pub fn set_ttl(
        &self,
        collection: &str,
        retention_days: u64,
        field: &str,
    ) -> Result<TtlConfig, AppError> {
        self.check_not_poisoned()?;
        self.ensure_collection_exists(collection)?;

        let config = TtlConfig {
            retention_days,
            field: field.to_string(),
            enabled: true,
        };

        let meta_key = format!("ttl:{collection}");
        let bytes = serde_json::to_vec(&config)?;
        let mut tx = self.db.write_tx();
        tx.insert(&self.meta, &meta_key, bytes);
        self.check_fjall_result(tx.commit())?;

        Ok(config)
    }

    pub fn get_ttl(&self, collection: &str) -> Result<Option<TtlConfig>, AppError> {
        self.ensure_collection_exists(collection)?;
        let meta_key = format!("ttl:{collection}");
        match self.meta.get(&meta_key)? {
            Some(bytes) => {
                let config: TtlConfig = serde_json::from_slice(bytes.as_ref())?;
                Ok(Some(config))
            }
            None => Ok(None),
        }
    }

    pub fn delete_ttl(&self, collection: &str) -> Result<(), AppError> {
        self.check_not_poisoned()?;
        self.ensure_collection_exists(collection)?;
        let meta_key = format!("ttl:{collection}");
        let mut tx = self.db.write_tx();
        tx.remove(&self.meta, meta_key.as_str());
        self.check_fjall_result(tx.commit())?;
        Ok(())
    }

    /// Get all collections that have a TTL policy.
    pub fn get_all_ttl_configs(&self) -> Result<Vec<(String, TtlConfig)>, AppError> {
        let mut results = Vec::new();
        let read_tx = self.db.read_tx();
        for kv in read_tx.prefix(&self.meta, "ttl:") {
            let (key_bytes, val_bytes) = kv?;
            let key_str = std::str::from_utf8(key_bytes.as_ref())
                .map_err(|e| AppError::Internal(format!("Invalid key: {e}")))?;
            let collection = key_str.strip_prefix("ttl:").unwrap_or(key_str).to_string();
            let config: TtlConfig = serde_json::from_slice(val_bytes.as_ref())?;
            results.push((collection, config));
        }
        Ok(results)
    }

    /// Run TTL cleanup for a single collection. Returns the number of documents deleted.
    pub fn run_ttl_cleanup(&self, collection: &str, config: &TtlConfig) -> Result<u64, AppError> {
        if !config.enabled {
            return Ok(0);
        }

        let cutoff = chrono::Utc::now() - chrono::Duration::days(config.retention_days as i64);
        let cutoff_str = cutoff.to_rfc3339();

        // Build a filter: {field: {"$lt": cutoff_str}}
        let filter_json = serde_json::json!({
            &config.field: {"$lt": cutoff_str}
        });
        let filter = parse_filter(&filter_json)?;

        let deleted = self.delete_by_query(collection, &filter)?;
        if deleted > 0 {
            info!(
                collection = collection,
                deleted = deleted,
                field = %config.field,
                retention_days = config.retention_days,
                "TTL cleanup completed"
            );
        }
        Ok(deleted)
    }
}
