use chrono::Utc;
use serde_json::Value;
use uuid::Uuid;

use crate::error::AppError;
use crate::query::filter::FilterNode;

use super::storage::Storage;

const MAX_DOCUMENT_SIZE: usize = 16 * 1024 * 1024; // 16 MB
const MAX_BULK_INSERT: usize = 10_000;

impl Storage {
    pub fn insert_document(&self, collection: &str, mut doc: Value) -> Result<Value, AppError> {
        self.check_not_poisoned()?;
        self.ensure_collection_exists(collection)?;

        let id = Uuid::now_v7().to_string();
        let now = Utc::now().to_rfc3339();

        let obj = doc
            .as_object_mut()
            .ok_or_else(|| AppError::InvalidDocument("Document must be a JSON object".into()))?;
        obj.insert("_id".to_string(), Value::String(id.clone()));
        obj.insert("_rev".to_string(), Value::Number(1.into()));
        obj.insert("_created_at".to_string(), Value::String(now.clone()));
        obj.insert("_updated_at".to_string(), Value::String(now.clone()));
        obj.insert("_received_at".to_string(), Value::String(now));

        let bytes = serde_json::to_vec(&doc)?;
        if bytes.len() > MAX_DOCUMENT_SIZE {
            return Err(AppError::DocumentTooLarge);
        }

        let docs_partition = self.get_docs_partition(collection)?;
        let mut tx = self.db.write_tx();
        tx.insert(&docs_partition, id.as_str(), &bytes);
        self.index_manager
            .add_index_entries_to_tx(&mut tx, collection, &id, &doc);
        self.check_fjall_result(tx.commit())?;

        self.doc_counts.increment(collection, 1);

        Ok(doc)
    }

    pub fn get_document(&self, collection: &str, id: &str) -> Result<Value, AppError> {
        self.ensure_collection_exists(collection)?;

        let docs_partition = self.get_docs_partition(collection)?;
        match docs_partition.get(id)? {
            Some(bytes) => {
                let doc: Value = serde_json::from_slice(bytes.as_ref())?;
                Ok(doc)
            }
            None => Err(AppError::DocumentNotFound(id.to_string())),
        }
    }

    pub fn replace_document(
        &self,
        collection: &str,
        id: &str,
        mut doc: Value,
    ) -> Result<Value, AppError> {
        self.check_not_poisoned()?;
        self.ensure_collection_exists(collection)?;

        let docs_partition = self.get_docs_partition(collection)?;
        let existing_bytes = docs_partition
            .get(id)?
            .ok_or_else(|| AppError::DocumentNotFound(id.to_string()))?;
        let existing_doc: Value = serde_json::from_slice(existing_bytes.as_ref())?;

        let old_rev = existing_doc
            .get("_rev")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        // Check for revision conflict if client sends _rev
        if let Some(client_rev) = doc.get("_rev").and_then(|v| v.as_u64())
            && client_rev != old_rev
        {
            return Err(AppError::DocumentConflict(format!(
                "Expected rev {client_rev}, current is {old_rev}"
            )));
        }

        let now = Utc::now().to_rfc3339();
        let created_at = existing_doc
            .get("_created_at")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let received_at = existing_doc
            .get("_received_at")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let obj = doc
            .as_object_mut()
            .ok_or_else(|| AppError::InvalidDocument("Document must be a JSON object".into()))?;
        obj.insert("_id".to_string(), Value::String(id.to_string()));
        obj.insert("_rev".to_string(), Value::Number((old_rev + 1).into()));
        obj.insert("_created_at".to_string(), Value::String(created_at));
        obj.insert("_updated_at".to_string(), Value::String(now));
        obj.insert("_received_at".to_string(), Value::String(received_at));

        let bytes = serde_json::to_vec(&doc)?;
        if bytes.len() > MAX_DOCUMENT_SIZE {
            return Err(AppError::DocumentTooLarge);
        }

        let mut tx = self.db.write_tx();
        // Remove old index entries, write new ones
        self.index_manager
            .remove_index_entries_from_tx(&mut tx, collection, id, &existing_doc);
        tx.insert(&docs_partition, id, &bytes);
        self.index_manager
            .add_index_entries_to_tx(&mut tx, collection, id, &doc);
        self.check_fjall_result(tx.commit())?;

        Ok(doc)
    }

    pub fn partial_update_document(
        &self,
        collection: &str,
        id: &str,
        patch: Value,
    ) -> Result<Value, AppError> {
        let mut existing = self.get_document(collection, id)?;
        json_merge_patch(&mut existing, &patch);
        self.replace_document(collection, id, existing)
    }

    pub fn delete_document(&self, collection: &str, id: &str) -> Result<(), AppError> {
        self.check_not_poisoned()?;
        self.ensure_collection_exists(collection)?;

        let docs_partition = self.get_docs_partition(collection)?;
        let existing_bytes = docs_partition
            .get(id)?
            .ok_or_else(|| AppError::DocumentNotFound(id.to_string()))?;
        let existing_doc: Value = serde_json::from_slice(existing_bytes.as_ref())?;

        let mut tx = self.db.write_tx();
        self.index_manager
            .remove_index_entries_from_tx(&mut tx, collection, id, &existing_doc);
        tx.remove(&docs_partition, id);
        self.check_fjall_result(tx.commit())?;

        self.doc_counts.increment(collection, -1);

        Ok(())
    }

    pub fn bulk_insert_documents(
        &self,
        collection: &str,
        documents: Vec<Value>,
    ) -> Result<(u64, Vec<String>), AppError> {
        if documents.len() > MAX_BULK_INSERT {
            return Err(AppError::InvalidDocument(format!(
                "Bulk insert limited to {MAX_BULK_INSERT} documents per request"
            )));
        }
        self.check_not_poisoned()?;
        self.ensure_collection_exists(collection)?;

        let docs_partition = self.get_docs_partition(collection)?;
        let mut inserted = 0u64;
        let mut errors = Vec::new();
        // (id, bytes, doc) — keep doc around for index writes
        let mut to_write: Vec<(String, Vec<u8>, Value)> = Vec::new();

        for (i, mut doc) in documents.into_iter().enumerate() {
            let id = Uuid::now_v7().to_string();
            let now = Utc::now().to_rfc3339();

            match doc.as_object_mut() {
                Some(obj) => {
                    obj.insert("_id".to_string(), Value::String(id.clone()));
                    obj.insert("_rev".to_string(), Value::Number(1.into()));
                    obj.insert("_created_at".to_string(), Value::String(now.clone()));
                    obj.insert("_updated_at".to_string(), Value::String(now.clone()));
                    obj.insert("_received_at".to_string(), Value::String(now));

                    match serde_json::to_vec(&doc) {
                        Ok(bytes) => {
                            if bytes.len() > MAX_DOCUMENT_SIZE {
                                errors.push(format!("Document {i}: exceeds 16 MB size limit"));
                            } else {
                                to_write.push((id, bytes, doc));
                                inserted += 1;
                            }
                        }
                        Err(e) => errors.push(format!("Document {i}: {e}")),
                    }
                }
                None => errors.push(format!("Document {i}: must be a JSON object")),
            }
        }

        if !to_write.is_empty() {
            let mut tx = self.db.write_tx();
            for (id, bytes, doc) in &to_write {
                tx.insert(&docs_partition, id.as_str(), bytes.as_slice());
                self.index_manager
                    .add_index_entries_to_tx(&mut tx, collection, id, doc);
            }
            self.check_fjall_result(tx.commit())?;
            self.doc_counts.increment(collection, inserted as i64);
        }

        Ok((inserted, errors))
    }

    pub fn scan_all_documents(&self, collection: &str) -> Result<Vec<Value>, AppError> {
        self.ensure_collection_exists(collection)?;

        let docs_partition = self.get_docs_partition(collection)?;
        let read_tx = self.db.read_tx();
        let mut docs = Vec::new();
        for kv in read_tx.iter(&docs_partition) {
            let (_, value) = kv?;
            let doc: Value = serde_json::from_slice(value.as_ref())?;
            docs.push(doc);
        }
        Ok(docs)
    }

    /// Delete all documents matching a filter. Returns the count of deleted documents.
    pub fn delete_by_query(&self, collection: &str, filter: &FilterNode) -> Result<u64, AppError> {
        self.check_not_poisoned()?;
        self.ensure_collection_exists(collection)?;

        // Scan and filter to find matching docs
        let all_docs = self.scan_all_documents(collection)?;
        let matching: Vec<Value> = all_docs
            .into_iter()
            .filter(|doc| filter.matches(doc))
            .collect();

        let count = matching.len() as u64;
        if count == 0 {
            return Ok(0);
        }

        let docs_partition = self.get_docs_partition(collection)?;
        let mut tx = self.db.write_tx();

        for doc in &matching {
            if let Some(id) = doc.get("_id").and_then(|v| v.as_str()) {
                self.index_manager
                    .remove_index_entries_from_tx(&mut tx, collection, id, doc);
                tx.remove(&docs_partition, id);
            }
        }

        self.check_fjall_result(tx.commit())?;
        self.doc_counts.increment(collection, -(count as i64));

        Ok(count)
    }

    /// Update all documents matching a filter with $set operations.
    /// Returns the count of updated documents.
    pub fn update_by_query(
        &self,
        collection: &str,
        filter: &FilterNode,
        update: &Value,
    ) -> Result<u64, AppError> {
        self.check_not_poisoned()?;
        self.ensure_collection_exists(collection)?;

        let set_fields = parse_set_updates(update)?;

        // Scan and filter to find matching docs
        let all_docs = self.scan_all_documents(collection)?;
        let matching: Vec<Value> = all_docs
            .into_iter()
            .filter(|doc| filter.matches(doc))
            .collect();

        let count = matching.len() as u64;
        if count == 0 {
            return Ok(0);
        }

        let docs_partition = self.get_docs_partition(collection)?;
        let now = Utc::now().to_rfc3339();
        let mut tx = self.db.write_tx();

        for mut doc in matching {
            let id = doc
                .get("_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let old_rev = doc.get("_rev").and_then(|v| v.as_u64()).unwrap_or(0);

            // Remove old index entries
            self.index_manager
                .remove_index_entries_from_tx(&mut tx, collection, &id, &doc);

            // Apply $set fields
            if let Some(obj) = doc.as_object_mut() {
                for (path, value) in &set_fields {
                    set_nested_field(obj, path, value.clone());
                }
                obj.insert("_rev".to_string(), Value::Number((old_rev + 1).into()));
                obj.insert("_updated_at".to_string(), Value::String(now.clone()));
            }

            let bytes = serde_json::to_vec(&doc)?;
            tx.insert(&docs_partition, id.as_str(), bytes.as_slice());

            // Write new index entries
            self.index_manager
                .add_index_entries_to_tx(&mut tx, collection, &id, &doc);
        }

        self.check_fjall_result(tx.commit())?;

        Ok(count)
    }

    /// Create a secondary index on one or more fields, backfilling existing documents.
    pub fn create_index(
        &self,
        collection: &str,
        name: &str,
        fields: &[String],
    ) -> Result<crate::index::secondary::IndexDef, AppError> {
        self.check_not_poisoned()?;
        self.ensure_collection_exists(collection)?;

        if fields.is_empty() {
            return Err(AppError::InvalidIndex(
                "Index must have at least one field".into(),
            ));
        }

        // Check for duplicate name
        let meta_key = format!("index:{collection}:{name}");
        if self.meta.get(&meta_key)?.is_some() {
            return Err(AppError::IndexExists(name.to_string()));
        }

        // For single-field indexes, check if an index on this field already exists
        if fields.len() == 1
            && self
                .index_manager
                .get_index_for_field(collection, &fields[0])
                .is_some()
        {
            return Err(AppError::IndexExists(format!(
                "An index on field '{}' already exists",
                fields[0]
            )));
        }

        let def = crate::index::secondary::IndexDef::new(
            name.to_string(),
            collection.to_string(),
            fields.to_vec(),
            Utc::now().to_rfc3339(),
        );

        // Create the index partition
        let partition_name = format!("{collection}#idx#{name}");
        let partition = self.create_partition(&partition_name)?;

        // Backfill: scan all existing documents and index them
        let docs_partition = self.get_docs_partition(collection)?;
        let read_tx = self.db.read_tx();
        let mut entries: Vec<Vec<u8>> = Vec::new();
        let is_compound = fields.len() > 1;

        for kv in read_tx.iter(&docs_partition) {
            let (_, value_bytes) = kv?;
            let doc: Value = serde_json::from_slice(value_bytes.as_ref())?;
            if let Some(doc_id) = doc.get("_id").and_then(|v| v.as_str()) {
                if is_compound {
                    // All fields must be present for compound index
                    let values: Vec<&Value> = fields
                        .iter()
                        .filter_map(|f| crate::query::filter::resolve_json_path(&doc, f))
                        .collect();
                    if values.len() == fields.len() {
                        let key = crate::index::secondary::make_compound_index_key(&values, doc_id);
                        entries.push(key);
                    }
                } else if let Some(field_val) =
                    crate::query::filter::resolve_json_path(&doc, &fields[0])
                {
                    let key = crate::index::secondary::make_index_key(field_val, doc_id);
                    entries.push(key);
                }
            }
        }
        drop(read_tx);

        // Write all index entries + meta in one transaction
        let meta_bytes = serde_json::to_vec(&def)?;
        let mut tx = self.db.write_tx();
        for key in &entries {
            tx.insert(&partition, key.as_slice(), b"");
        }
        tx.insert(&self.meta, &meta_key, meta_bytes);
        self.check_fjall_result(tx.commit())?;

        // Register in cache
        self.index_manager.register(def.clone(), partition);

        self.persist()?;

        Ok(def)
    }

    /// Drop a secondary index.
    pub fn drop_index(&self, collection: &str, name: &str) -> Result<(), AppError> {
        self.check_not_poisoned()?;
        self.ensure_collection_exists(collection)?;

        let meta_key = format!("index:{collection}:{name}");
        if self.meta.get(&meta_key)?.is_none() {
            return Err(AppError::IndexNotFound(format!("{collection}/{name}")));
        }

        let partition = self
            .index_manager
            .get_index_partition(collection, name)
            .ok_or_else(|| AppError::IndexNotFound(format!("{collection}/{name}")))?;

        // Clear all entries from the partition
        let read_tx = self.db.read_tx();
        let keys: Vec<Vec<u8>> = read_tx
            .iter(&partition)
            .filter_map(|kv| kv.ok().map(|(k, _)| k.as_ref().to_vec()))
            .collect();
        drop(read_tx);

        let mut tx = self.db.write_tx();
        for key in &keys {
            tx.remove(&partition, key.as_slice());
        }
        tx.remove(&self.meta, meta_key.as_str());
        self.check_fjall_result(tx.commit())?;

        self.index_manager.unregister(collection, name);
        self.persist()?;

        Ok(())
    }

    /// List all indexes for a collection.
    pub fn list_indexes(
        &self,
        collection: &str,
    ) -> Result<Vec<crate::index::secondary::IndexDef>, AppError> {
        self.ensure_collection_exists(collection)?;
        Ok(self.index_manager.get_indexes_for_collection(collection))
    }

    pub fn ensure_collection_exists(&self, collection: &str) -> Result<(), AppError> {
        if !self.collection_exists(collection)? {
            return Err(AppError::CollectionNotFound(collection.to_string()));
        }
        Ok(())
    }
}

/// Parse $set updates from the update spec: {"$set": {"field": value, ...}}
fn parse_set_updates(update: &Value) -> Result<Vec<(String, Value)>, AppError> {
    let obj = update
        .as_object()
        .ok_or_else(|| AppError::InvalidQuery("Update must be a JSON object".into()))?;

    let set_obj = obj
        .get("$set")
        .and_then(|v| v.as_object())
        .ok_or_else(|| AppError::InvalidQuery("Update must contain a '$set' object".into()))?;

    Ok(set_obj
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect())
}

/// Set a nested field using dot-notation path, creating intermediate objects as needed.
fn set_nested_field(obj: &mut serde_json::Map<String, Value>, path: &str, value: Value) {
    // Depth limit: silently skip if too deep (validation happens at parse layer)
    if path.matches('.').count() >= 20 {
        return;
    }
    let parts: Vec<&str> = path.split('.').collect();
    if parts.len() == 1 {
        obj.insert(path.to_string(), value);
        return;
    }

    let mut current = obj;
    for part in &parts[..parts.len() - 1] {
        let entry = current
            .entry(part.to_string())
            .or_insert_with(|| Value::Object(serde_json::Map::new()));
        if let Value::Object(inner) = entry {
            current = inner;
        } else {
            // Overwrite non-object with an object
            *entry = Value::Object(serde_json::Map::new());
            if let Value::Object(inner) = entry {
                current = inner;
            } else {
                return;
            }
        }
    }

    let last = parts.last().unwrap();
    current.insert(last.to_string(), value);
}

fn json_merge_patch(target: &mut Value, patch: &Value) {
    if let Value::Object(patch_obj) = patch
        && let Value::Object(target_obj) = target
    {
        for (key, value) in patch_obj {
            if value.is_null() {
                target_obj.remove(key);
            } else {
                let entry = target_obj.entry(key.clone()).or_insert(Value::Null);
                json_merge_patch(entry, value);
            }
        }
        return;
    }
    *target = patch.clone();
}
