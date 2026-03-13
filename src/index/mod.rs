pub mod primary;
pub mod secondary;

use std::collections::HashMap;

use parking_lot::RwLock;

use fjall::{PartitionCreateOptions, TxKeyspace, TxPartitionHandle};
use serde_json::Value;

use crate::error::AppError;
use crate::query::filter::resolve_json_path;

use self::secondary::{
    IndexDef, extract_doc_id_from_key, make_compound_index_key, make_index_key,
    value_to_sortable_bytes,
};

/// Cached index: definition + open partition handle.
struct IndexEntry {
    def: IndexDef,
    partition: TxPartitionHandle,
}

pub struct IndexManager {
    /// (collection, index_name) → IndexEntry
    indexes: RwLock<HashMap<(String, String), IndexEntry>>,
}

impl Default for IndexManager {
    fn default() -> Self {
        Self::new()
    }
}

impl IndexManager {
    pub fn new() -> Self {
        IndexManager {
            indexes: RwLock::new(HashMap::new()),
        }
    }

    /// Load all index definitions from _meta on startup.
    pub fn load_indexes(&self, db: &TxKeyspace, meta: &TxPartitionHandle) -> Result<(), AppError> {
        let read_tx = db.read_tx();
        let mut indexes = self.indexes.write();

        for kv in read_tx.prefix(meta, "index:") {
            let (key_bytes, value_bytes) = kv?;
            let _key_str = std::str::from_utf8(key_bytes.as_ref())
                .map_err(|e| AppError::Internal(format!("Invalid index meta key: {e}")))?;

            let mut def: IndexDef = serde_json::from_slice(value_bytes.as_ref())
                .map_err(|e| AppError::Internal(format!("Invalid index meta value: {e}")))?;

            // Backward compat: old indexes stored `field` but not `fields`
            if def.fields.is_empty() && !def.field.is_empty() {
                def.fields = vec![def.field.clone()];
            }

            let partition_name = format!("{}#idx#{}", def.collection, def.name);
            let partition = db
                .open_partition(&partition_name, PartitionCreateOptions::default())
                .map_err(|e| AppError::Internal(format!("Failed to open index partition: {e}")))?;

            indexes.insert(
                (def.collection.clone(), def.name.clone()),
                IndexEntry { def, partition },
            );
        }

        Ok(())
    }

    /// Register an index (called after backfill + meta write).
    pub fn register(&self, def: IndexDef, partition: TxPartitionHandle) {
        let mut indexes = self.indexes.write();
        indexes.insert(
            (def.collection.clone(), def.name.clone()),
            IndexEntry { def, partition },
        );
    }

    /// Remove an index from the cache.
    pub fn unregister(&self, collection: &str, name: &str) {
        let mut indexes = self.indexes.write();
        indexes.remove(&(collection.to_string(), name.to_string()));
    }

    /// Get all index definitions for a collection.
    pub fn get_indexes_for_collection(&self, collection: &str) -> Vec<IndexDef> {
        let indexes = self.indexes.read();
        indexes
            .iter()
            .filter(|((col, _), _)| col == collection)
            .map(|(_, entry)| entry.def.clone())
            .collect()
    }

    /// Get a single-field index (or the first field of a compound index) by field path.
    /// Prefers exact single-field indexes over compound indexes.
    pub fn get_index_for_field(
        &self,
        collection: &str,
        field: &str,
    ) -> Option<(IndexDef, TxPartitionHandle)> {
        let indexes = self.indexes.read();

        // First try exact single-field index
        let single = indexes
            .iter()
            .find(|((col, _), entry)| {
                col == collection && entry.def.fields.len() == 1 && entry.def.fields[0] == field
            })
            .map(|(_, entry)| (entry.def.clone(), entry.partition.clone()));

        if single.is_some() {
            return single;
        }

        // Fall back to first field of a compound index
        indexes
            .iter()
            .find(|((col, _), entry)| {
                col == collection && !entry.def.fields.is_empty() && entry.def.fields[0] == field
            })
            .map(|(_, entry)| (entry.def.clone(), entry.partition.clone()))
    }

    /// Find a compound index whose leading fields match `eq_fields` (in order),
    /// optionally followed by `sort_field`.
    ///
    /// For sorted scan (Opt 1): pass `sort_field = Some(field)`.
    /// For compound equality (Opt 3): pass `sort_field = None`, requires >= 2 matched fields.
    ///
    /// Returns (IndexDef, partition, number of eq fields matched).
    pub fn find_compound_index(
        &self,
        collection: &str,
        eq_field_names: &[&str],
        sort_field: Option<&str>,
    ) -> Option<(IndexDef, TxPartitionHandle, usize)> {
        let indexes = self.indexes.read();
        let eq_set: std::collections::HashSet<&str> = eq_field_names.iter().copied().collect();

        let mut best: Option<(IndexDef, TxPartitionHandle, usize)> = None;

        for ((col, _), entry) in indexes.iter() {
            if col != collection || !entry.def.is_compound() {
                continue;
            }

            let idx_fields = &entry.def.fields;

            // Count consecutive leading fields that appear in eq_set
            let mut matched = 0;
            for f in idx_fields {
                if eq_set.contains(f.as_str()) {
                    matched += 1;
                } else {
                    break;
                }
            }

            if matched == 0 {
                continue;
            }

            if let Some(sf) = sort_field {
                // For IndexSorted: need the field at position `matched` to be the sort field
                if matched < idx_fields.len()
                    && idx_fields[matched] == sf
                    && best.as_ref().is_none_or(|(_, _, bm)| matched > *bm)
                {
                    best = Some((entry.def.clone(), entry.partition.clone(), matched));
                }
            } else {
                // For CompoundEq: need at least 2 matched fields
                if matched >= 2 && best.as_ref().is_none_or(|(_, _, bm)| matched > *bm) {
                    best = Some((entry.def.clone(), entry.partition.clone(), matched));
                }
            }
        }

        best
    }

    /// Find a compound index where leading N fields match equality conditions
    /// and the field at position N matches the range field.
    /// Returns (IndexDef, partition, number of eq fields matched).
    pub fn find_compound_range_index(
        &self,
        collection: &str,
        eq_field_names: &[&str],
        range_field: &str,
    ) -> Option<(IndexDef, TxPartitionHandle, usize)> {
        let indexes = self.indexes.read();
        let eq_set: std::collections::HashSet<&str> = eq_field_names.iter().copied().collect();

        let mut best: Option<(IndexDef, TxPartitionHandle, usize)> = None;

        for ((col, _), entry) in indexes.iter() {
            if col != collection || !entry.def.is_compound() {
                continue;
            }

            let idx_fields = &entry.def.fields;

            // Count consecutive leading fields that appear in eq_set
            let mut matched = 0;
            for f in idx_fields {
                if eq_set.contains(f.as_str()) {
                    matched += 1;
                } else {
                    break;
                }
            }

            if matched == 0 {
                continue;
            }

            // The field at position `matched` must be the range field
            if matched < idx_fields.len()
                && idx_fields[matched] == range_field
                && best.as_ref().is_none_or(|(_, _, bm)| matched > *bm)
            {
                best = Some((entry.def.clone(), entry.partition.clone(), matched));
            }
        }

        best
    }

    /// Get the partition handle for a specific index by name.
    pub fn get_index_partition(&self, collection: &str, name: &str) -> Option<TxPartitionHandle> {
        let indexes = self.indexes.read();
        indexes
            .get(&(collection.to_string(), name.to_string()))
            .map(|entry| entry.partition.clone())
    }

    /// Write index entries for a newly inserted document.
    pub fn add_index_entries_to_tx(
        &self,
        tx: &mut fjall::WriteTransaction,
        collection: &str,
        doc_id: &str,
        doc: &Value,
    ) {
        let indexes = self.indexes.read();
        for ((col, _), entry) in indexes.iter() {
            if col != collection {
                continue;
            }
            if entry.def.is_compound() {
                // Compound index: all fields must be present
                let values: Vec<&Value> = entry
                    .def
                    .fields
                    .iter()
                    .filter_map(|f| resolve_json_path(doc, f))
                    .collect();
                if values.len() == entry.def.fields.len() {
                    let key = make_compound_index_key(&values, doc_id);
                    tx.insert(&entry.partition, key, b"");
                }
            } else if let Some(field_val) = resolve_json_path(doc, &entry.def.fields[0]) {
                let key = make_index_key(field_val, doc_id);
                tx.insert(&entry.partition, key, b"");
            }
        }
    }

    /// Remove index entries for a document being deleted/updated.
    pub fn remove_index_entries_from_tx(
        &self,
        tx: &mut fjall::WriteTransaction,
        collection: &str,
        doc_id: &str,
        doc: &Value,
    ) {
        let indexes = self.indexes.read();
        for ((col, _), entry) in indexes.iter() {
            if col != collection {
                continue;
            }
            if entry.def.is_compound() {
                let values: Vec<&Value> = entry
                    .def
                    .fields
                    .iter()
                    .filter_map(|f| resolve_json_path(doc, f))
                    .collect();
                if values.len() == entry.def.fields.len() {
                    let key = make_compound_index_key(&values, doc_id);
                    tx.remove(&entry.partition, key);
                }
            } else if let Some(field_val) = resolve_json_path(doc, &entry.def.fields[0]) {
                let key = make_index_key(field_val, doc_id);
                tx.remove(&entry.partition, key);
            }
        }
    }

    /// Equality lookup: get all doc IDs where field == value.
    pub fn lookup_eq(
        &self,
        db: &TxKeyspace,
        collection: &str,
        field: &str,
        value: &Value,
    ) -> Option<Vec<String>> {
        let (def, partition) = self.get_index_for_field(collection, field)?;

        // For compound indexes querying only the first field, use \x01 separator
        // (field separator) instead of \x00 (doc_id separator)
        let separator = if def.is_compound() { 0x01 } else { 0x00 };
        let prefix = {
            let mut p = value_to_sortable_bytes(value);
            p.push(separator);
            p
        };

        let read_tx = db.read_tx();
        let mut doc_ids = Vec::new();
        for (key, _) in read_tx.prefix(&partition, prefix).flatten() {
            if let Some(id) = extract_doc_id_from_key(key.as_ref()) {
                doc_ids.push(id);
            }
        }
        Some(doc_ids)
    }

    /// Range lookup: get all doc IDs where field is in the given range.
    pub fn lookup_range(
        &self,
        db: &TxKeyspace,
        collection: &str,
        field: &str,
        lower: Option<(&Value, bool)>,
        upper: Option<(&Value, bool)>,
    ) -> Option<Vec<String>> {
        let (_def, partition) = self.get_index_for_field(collection, field)?;

        let lower_bytes = lower.map(|(v, _)| value_to_sortable_bytes(v));
        let upper_bytes = upper.map(|(v, inclusive)| {
            let mut b = value_to_sortable_bytes(v);
            if inclusive {
                b.push(0x00);
                b.extend_from_slice(&[0xFF; 37]);
            }
            b
        });

        let read_tx = db.read_tx();

        let start = lower_bytes.as_deref().unwrap_or(&[]);
        let end = upper_bytes
            .as_deref()
            .unwrap_or(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);

        // Build a set of excluded doc_ids for non-inclusive lower bound
        let lower_exact_prefix = if let Some((lower_val, false)) = lower {
            let mut p = value_to_sortable_bytes(lower_val);
            p.push(0x00);
            Some(p)
        } else {
            None
        };

        let mut doc_ids = Vec::new();
        for (key, _) in read_tx.range(&partition, start..end).flatten() {
            let key_ref = key.as_ref();

            // Skip entries that match the lower bound exactly when not inclusive
            if let Some(ref prefix) = lower_exact_prefix
                && key_ref.starts_with(prefix)
            {
                continue;
            }

            if let Some(id) = extract_doc_id_from_key(key_ref) {
                doc_ids.push(id);
            }
        }

        Some(doc_ids)
    }

    /// Count index entries for an equality match (optimized count_only).
    pub fn count_eq(
        &self,
        db: &TxKeyspace,
        collection: &str,
        field: &str,
        value: &Value,
    ) -> Option<u64> {
        let (def, partition) = self.get_index_for_field(collection, field)?;
        let separator = if def.is_compound() { 0x01 } else { 0x00 };
        let prefix = {
            let mut p = value_to_sortable_bytes(value);
            p.push(separator);
            p
        };

        let read_tx = db.read_tx();
        let count = read_tx.prefix(&partition, prefix).flatten().count() as u64;
        Some(count)
    }

    /// Count all index entries in a range.
    pub fn count_range(
        &self,
        db: &TxKeyspace,
        collection: &str,
        field: &str,
        lower: Option<(&Value, bool)>,
        upper: Option<(&Value, bool)>,
    ) -> Option<u64> {
        self.lookup_range(db, collection, field, lower, upper)
            .map(|ids| ids.len() as u64)
    }

    /// Get doc IDs for $in operator: union of equality lookups.
    pub fn lookup_in(
        &self,
        db: &TxKeyspace,
        collection: &str,
        field: &str,
        values: &[Value],
    ) -> Option<Vec<String>> {
        self.get_index_for_field(collection, field)?;

        let mut all_ids = Vec::new();
        for value in values {
            if let Some(ids) = self.lookup_eq(db, collection, field, value) {
                all_ids.extend(ids);
            }
        }
        let mut seen = std::collections::HashSet::new();
        all_ids.retain(|id| seen.insert(id.clone()));
        Some(all_ids)
    }
}
