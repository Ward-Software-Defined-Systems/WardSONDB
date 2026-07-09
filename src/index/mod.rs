pub mod primary;
pub mod secondary;

use std::collections::HashMap;

use parking_lot::RwLock;

use serde_json::Value;

use crate::engine::backend::{Engine, PartitionId, StorageBackend, WriteBatchWrapper};
use crate::error::AppError;
use crate::query::filter::resolve_json_path;

use self::secondary::{
    IndexDef, extract_doc_id_from_key, make_compound_index_key, make_index_key,
    value_to_sortable_bytes,
};

/// Cached index: definition + opaque partition handle.
struct IndexEntry {
    def: IndexDef,
    partition: PartitionId,
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
    pub fn load_indexes(&self, engine: &Engine, meta: &PartitionId) -> Result<(), AppError> {
        let mut indexes = self.indexes.write();

        for kv in engine.prefix_iterator(meta, b"index:")? {
            let (key_bytes, value_bytes) = kv?;
            let _key_str = std::str::from_utf8(&key_bytes)
                .map_err(|e| AppError::Internal(format!("Invalid index meta key: {e}")))?;

            let mut def: IndexDef = serde_json::from_slice(&value_bytes)
                .map_err(|e| AppError::Internal(format!("Invalid index meta value: {e}")))?;

            // Backward compat: old indexes stored `field` but not `fields`
            if def.fields.is_empty() && !def.field.is_empty() {
                def.fields = vec![def.field.clone()];
            }

            let partition_name = format!("{}#idx#{}", def.collection, def.name);
            let partition = engine.create_or_open_partition(&partition_name)?;

            indexes.insert(
                (def.collection.clone(), def.name.clone()),
                IndexEntry { def, partition },
            );
        }

        Ok(())
    }

    /// Register an index (called after backfill + meta write).
    pub fn register(&self, def: IndexDef, partition: PartitionId) {
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
    ) -> Option<(IndexDef, PartitionId)> {
        let indexes = self.indexes.read();

        let single = indexes
            .iter()
            .find(|((col, _), entry)| {
                col == collection && entry.def.fields.len() == 1 && entry.def.fields[0] == field
            })
            .map(|(_, entry)| (entry.def.clone(), entry.partition.clone()));

        if single.is_some() {
            return single;
        }

        indexes
            .iter()
            .find(|((col, _), entry)| {
                col == collection && !entry.def.fields.is_empty() && entry.def.fields[0] == field
            })
            .map(|(_, entry)| (entry.def.clone(), entry.partition.clone()))
    }

    /// Find a compound index whose leading fields match `eq_fields`, optionally followed by `sort_field`.
    pub fn find_compound_index(
        &self,
        collection: &str,
        eq_field_names: &[&str],
        sort_fields: &[&str],
    ) -> Option<(IndexDef, PartitionId, usize)> {
        let indexes = self.indexes.read();
        let eq_set: std::collections::HashSet<&str> = eq_field_names.iter().copied().collect();

        let mut best: Option<(IndexDef, PartitionId, usize)> = None;

        for ((col, _), entry) in indexes.iter() {
            if col != collection || !entry.def.is_compound() {
                continue;
            }

            let idx_fields = &entry.def.fields;

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

            if !sort_fields.is_empty() {
                // The index fields right after the matched eq prefix must be
                // exactly the sort fields, in order (extra trailing index
                // fields are allowed — they only affect within-tie order).
                let need = matched + sort_fields.len();
                if need <= idx_fields.len()
                    && idx_fields[matched..need]
                        .iter()
                        .map(String::as_str)
                        .eq(sort_fields.iter().copied())
                    && best.as_ref().is_none_or(|(_, _, bm)| matched > *bm)
                {
                    best = Some((entry.def.clone(), entry.partition.clone(), matched));
                }
            } else if matched >= 2 && best.as_ref().is_none_or(|(_, _, bm)| matched > *bm) {
                best = Some((entry.def.clone(), entry.partition.clone(), matched));
            }
        }

        best
    }

    pub fn find_compound_range_index(
        &self,
        collection: &str,
        eq_field_names: &[&str],
        range_field: &str,
    ) -> Option<(IndexDef, PartitionId, usize)> {
        let indexes = self.indexes.read();
        let eq_set: std::collections::HashSet<&str> = eq_field_names.iter().copied().collect();

        let mut best: Option<(IndexDef, PartitionId, usize)> = None;

        for ((col, _), entry) in indexes.iter() {
            if col != collection || !entry.def.is_compound() {
                continue;
            }

            let idx_fields = &entry.def.fields;

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
    pub fn get_index_partition(&self, collection: &str, name: &str) -> Option<PartitionId> {
        let indexes = self.indexes.read();
        indexes
            .get(&(collection.to_string(), name.to_string()))
            .map(|entry| entry.partition.clone())
    }

    /// Stage index inserts for a newly written document into the given batch.
    pub fn add_index_entries_to_batch(
        &self,
        batch: &mut WriteBatchWrapper,
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
                    batch.insert(&entry.partition, &key, b"");
                }
            } else if let Some(field_val) = resolve_json_path(doc, &entry.def.fields[0]) {
                let key = make_index_key(field_val, doc_id);
                batch.insert(&entry.partition, &key, b"");
            }
        }
    }

    /// Stage index removes for a document being deleted/updated into the given batch.
    pub fn remove_index_entries_from_batch(
        &self,
        batch: &mut WriteBatchWrapper,
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
                    batch.remove(&entry.partition, &key);
                }
            } else if let Some(field_val) = resolve_json_path(doc, &entry.def.fields[0]) {
                let key = make_index_key(field_val, doc_id);
                batch.remove(&entry.partition, &key);
            }
        }
    }

    /// Equality lookup: get all doc IDs where field == value.
    pub fn lookup_eq(
        &self,
        engine: &Engine,
        collection: &str,
        field: &str,
        value: &Value,
    ) -> Option<Vec<String>> {
        let (def, partition) = self.get_index_for_field(collection, field)?;

        let separator = if def.is_compound() { 0x01 } else { 0x00 };
        let prefix = {
            let mut p = value_to_sortable_bytes(value);
            p.push(separator);
            p
        };

        let iter = engine.prefix_iterator(&partition, &prefix).ok()?;
        let mut doc_ids = Vec::new();
        for item in iter.flatten() {
            let (key, _) = item;
            if let Some(id) = extract_doc_id_from_key(&key) {
                doc_ids.push(id);
            }
        }
        Some(doc_ids)
    }

    /// Range lookup: get all doc IDs where field is in the given range.
    pub fn lookup_range(
        &self,
        engine: &Engine,
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

        let start: &[u8] = lower_bytes.as_deref().unwrap_or(&[]);
        let default_end: [u8; 10] = [0xFF; 10];
        let end: &[u8] = upper_bytes.as_deref().unwrap_or(&default_end);

        let lower_exact_prefix = if let Some((lower_val, false)) = lower {
            let mut p = value_to_sortable_bytes(lower_val);
            p.push(0x00);
            Some(p)
        } else {
            None
        };

        let mut doc_ids = Vec::new();
        let iter = engine.range_iterator(&partition, start, end).ok()?;
        for item in iter.flatten() {
            let (key, _) = item;

            if let Some(ref prefix) = lower_exact_prefix
                && key.starts_with(prefix)
            {
                continue;
            }

            if let Some(id) = extract_doc_id_from_key(&key) {
                doc_ids.push(id);
            }
        }

        Some(doc_ids)
    }

    /// Count index entries for an equality match (optimized count_only).
    pub fn count_eq(
        &self,
        engine: &Engine,
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

        let iter = engine.prefix_iterator(&partition, &prefix).ok()?;
        let count = iter.flatten().count() as u64;
        Some(count)
    }

    /// Count all index entries in a range.
    pub fn count_range(
        &self,
        engine: &Engine,
        collection: &str,
        field: &str,
        lower: Option<(&Value, bool)>,
        upper: Option<(&Value, bool)>,
    ) -> Option<u64> {
        self.lookup_range(engine, collection, field, lower, upper)
            .map(|ids| ids.len() as u64)
    }

    /// $in: union of equality lookups.
    pub fn lookup_in(
        &self,
        engine: &Engine,
        collection: &str,
        field: &str,
        values: &[Value],
    ) -> Option<Vec<String>> {
        self.get_index_for_field(collection, field)?;

        let mut all_ids = Vec::new();
        for value in values {
            if let Some(ids) = self.lookup_eq(engine, collection, field, value) {
                all_ids.extend(ids);
            }
        }
        let mut seen = std::collections::HashSet::new();
        all_ids.retain(|id| seen.insert(id.clone()));
        Some(all_ids)
    }
}
