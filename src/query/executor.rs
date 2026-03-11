use serde_json::Value;

use crate::engine::storage::Storage;
use crate::error::AppError;
use crate::index::secondary::extract_doc_id_from_key;

use super::filter::resolve_json_path;
use super::parser::ParsedQuery;
use super::planner::{QueryPlan, ScanPlan, plan_query};
use super::sort::sort_documents;

#[derive(Debug)]
pub struct QueryResult {
    pub docs: Vec<Value>,
    pub total_count: Option<u64>,
    pub docs_scanned: u64,
    pub index_used: Option<String>,
    pub scan_strategy: Option<String>,
    pub has_more: bool,
}

pub fn execute_query(
    storage: &Storage,
    collection: &str,
    query: &ParsedQuery,
) -> Result<QueryResult, AppError> {
    let plan = plan_query(
        &query.filter,
        &storage.index_manager,
        collection,
        &query.sort,
        query.limit,
        query.count_only,
    );

    match &plan.scan {
        ScanPlan::FullScan => execute_full_scan(storage, collection, query, &plan),
        ScanPlan::IndexEq { .. }
        | ScanPlan::IndexIn { .. }
        | ScanPlan::IndexRange { .. }
        | ScanPlan::CompoundEq { .. } => execute_index_scan(storage, collection, query, &plan),
        ScanPlan::IndexSorted { .. } => execute_index_sorted(storage, collection, query, &plan),
    }
}

fn execute_full_scan(
    storage: &Storage,
    collection: &str,
    query: &ParsedQuery,
    plan: &QueryPlan,
) -> Result<QueryResult, AppError> {
    let all_docs = storage.scan_all_documents(collection)?;
    let docs_scanned = all_docs.len() as u64;

    let filter = plan.original_filter.as_ref();
    let mut matching: Vec<Value> = if let Some(filter) = filter {
        all_docs
            .into_iter()
            .filter(|doc| filter.matches(doc))
            .collect()
    } else {
        all_docs
    };

    let total_count = matching.len() as u64;

    if query.count_only {
        return Ok(QueryResult {
            docs: vec![],
            total_count: Some(total_count),
            docs_scanned,
            index_used: None,
            scan_strategy: None,
            has_more: false,
        });
    }

    if !query.sort.is_empty() {
        sort_documents(&mut matching, &query.sort);
    }

    let offset = query.offset as usize;
    let limit = query.limit as usize;
    let paginated: Vec<Value> = matching.into_iter().skip(offset).take(limit).collect();

    let docs = if let Some(ref fields) = query.fields {
        paginated
            .into_iter()
            .map(|doc| project_fields(&doc, fields))
            .collect()
    } else {
        paginated
    };

    Ok(QueryResult {
        docs,
        total_count: Some(total_count),
        docs_scanned,
        index_used: None,
        scan_strategy: None,
        has_more: false,
    })
}

fn execute_index_scan(
    storage: &Storage,
    collection: &str,
    query: &ParsedQuery,
    plan: &QueryPlan,
) -> Result<QueryResult, AppError> {
    let (index_name, candidate_ids) = match &plan.scan {
        ScanPlan::IndexEq {
            index_name,
            field,
            value,
        } => {
            // Optimized count_only: count index keys without loading docs
            if query.count_only
                && plan.post_filter.is_none()
                && let Some(count) =
                    storage
                        .index_manager
                        .count_eq(&storage.db, collection, field, value)
            {
                return Ok(QueryResult {
                    docs: vec![],
                    total_count: Some(count),
                    docs_scanned: 0,
                    index_used: Some(index_name.clone()),
                    scan_strategy: None,
                    has_more: false,
                });
            }

            let ids = storage
                .index_manager
                .lookup_eq(&storage.db, collection, field, value)
                .unwrap_or_default();
            (index_name.clone(), ids)
        }
        ScanPlan::IndexIn {
            index_name,
            field,
            values,
        } => {
            // Optimized count_only for $in
            if query.count_only && plan.post_filter.is_none() {
                let mut total = 0u64;
                let has_index = storage
                    .index_manager
                    .get_index_for_field(collection, field)
                    .is_some();
                if has_index {
                    for value in values {
                        if let Some(count) =
                            storage
                                .index_manager
                                .count_eq(&storage.db, collection, field, value)
                        {
                            total += count;
                        }
                    }
                    return Ok(QueryResult {
                        docs: vec![],
                        total_count: Some(total),
                        docs_scanned: 0,
                        index_used: Some(index_name.clone()),
                        scan_strategy: None,
                        has_more: false,
                    });
                }
            }

            let ids = storage
                .index_manager
                .lookup_in(&storage.db, collection, field, values)
                .unwrap_or_default();
            (index_name.clone(), ids)
        }
        ScanPlan::IndexRange {
            index_name,
            field,
            lower,
            upper,
        } => {
            // Optimized count_only for range
            if query.count_only && plan.post_filter.is_none() {
                let lower_ref = lower.as_ref().map(|(v, i)| (v, *i));
                let upper_ref = upper.as_ref().map(|(v, i)| (v, *i));
                if let Some(count) = storage.index_manager.count_range(
                    &storage.db,
                    collection,
                    field,
                    lower_ref,
                    upper_ref,
                ) {
                    return Ok(QueryResult {
                        docs: vec![],
                        total_count: Some(count),
                        docs_scanned: 0,
                        index_used: Some(index_name.clone()),
                        scan_strategy: None,
                        has_more: false,
                    });
                }
            }

            let lower_ref = lower.as_ref().map(|(v, i)| (v, *i));
            let upper_ref = upper.as_ref().map(|(v, i)| (v, *i));
            let ids = storage
                .index_manager
                .lookup_range(&storage.db, collection, field, lower_ref, upper_ref)
                .unwrap_or_default();
            (index_name.clone(), ids)
        }
        ScanPlan::CompoundEq { index_name, prefix } => {
            // Compound equality: prefix scan on compound index
            if query.count_only && plan.post_filter.is_none() {
                let partition = storage
                    .index_manager
                    .get_index_partition(collection, index_name)
                    .ok_or_else(|| {
                        AppError::Internal(format!("Index partition not found: {index_name}"))
                    })?;
                let read_tx = storage.db.read_tx();
                let count = read_tx.prefix(&partition, prefix).flatten().count() as u64;
                return Ok(QueryResult {
                    docs: vec![],
                    total_count: Some(count),
                    docs_scanned: 0,
                    index_used: Some(index_name.clone()),
                    scan_strategy: Some("compound_eq".to_string()),
                    has_more: false,
                });
            }

            let partition = storage
                .index_manager
                .get_index_partition(collection, index_name)
                .ok_or_else(|| {
                    AppError::Internal(format!("Index partition not found: {index_name}"))
                })?;
            let read_tx = storage.db.read_tx();
            let mut ids = Vec::new();
            for (key, _) in read_tx.prefix(&partition, prefix).flatten() {
                if let Some(id) = extract_doc_id_from_key(key.as_ref()) {
                    ids.push(id);
                }
            }
            (index_name.clone(), ids)
        }
        ScanPlan::FullScan | ScanPlan::IndexSorted { .. } => unreachable!(),
    };

    let docs_scanned = candidate_ids.len() as u64;

    // Load documents by ID
    let docs_partition = storage.get_docs_partition(collection)?;
    let mut loaded_docs = Vec::with_capacity(candidate_ids.len());
    for id in &candidate_ids {
        if let Ok(Some(bytes)) = docs_partition.get(id.as_str())
            && let Ok(doc) = serde_json::from_slice::<Value>(bytes.as_ref())
        {
            loaded_docs.push(doc);
        }
    }

    // Apply post-filter (residual conditions not covered by the index)
    let mut matching = if let Some(ref post_filter) = plan.post_filter {
        loaded_docs
            .into_iter()
            .filter(|doc| post_filter.matches(doc))
            .collect()
    } else {
        loaded_docs
    };

    let total_count = matching.len() as u64;

    if query.count_only {
        return Ok(QueryResult {
            docs: vec![],
            total_count: Some(total_count),
            docs_scanned,
            index_used: Some(index_name),
            scan_strategy: None,
            has_more: false,
        });
    }

    if !query.sort.is_empty() {
        sort_documents(&mut matching, &query.sort);
    }

    let offset = query.offset as usize;
    let limit = query.limit as usize;
    let paginated: Vec<Value> = matching.into_iter().skip(offset).take(limit).collect();

    let docs = if let Some(ref fields) = query.fields {
        paginated
            .into_iter()
            .map(|doc| project_fields(&doc, fields))
            .collect()
    } else {
        paginated
    };

    Ok(QueryResult {
        docs,
        total_count: Some(total_count),
        docs_scanned,
        index_used: Some(index_name),
        scan_strategy: None,
        has_more: false,
    })
}

/// Execute a sorted index scan with early termination.
/// Uses a compound index that covers both filter and sort fields.
fn execute_index_sorted(
    storage: &Storage,
    collection: &str,
    query: &ParsedQuery,
    plan: &QueryPlan,
) -> Result<QueryResult, AppError> {
    let (index_name, prefix, reverse) = match &plan.scan {
        ScanPlan::IndexSorted {
            index_name,
            prefix,
            reverse,
        } => (index_name.as_str(), prefix.as_slice(), *reverse),
        _ => unreachable!(),
    };

    let partition = storage
        .index_manager
        .get_index_partition(collection, index_name)
        .ok_or_else(|| AppError::Internal(format!("Index partition not found: {index_name}")))?;

    let docs_partition = storage.get_docs_partition(collection)?;

    let offset = query.offset as usize;
    let limit = query.limit as usize;

    let mut results = Vec::with_capacity(limit);
    let mut skipped = 0usize;
    let mut docs_scanned = 0u64;
    let mut has_more = false;

    // Helper closure to process a single index key
    let mut process_key = |key_bytes: &[u8]| -> Result<bool, AppError> {
        let doc_id = match extract_doc_id_from_key(key_bytes) {
            Some(id) => id,
            None => return Ok(false),
        };

        let doc = match docs_partition.get(doc_id.as_str()) {
            Ok(Some(bytes)) => match serde_json::from_slice::<Value>(bytes.as_ref()) {
                Ok(doc) => doc,
                Err(_) => return Ok(false),
            },
            _ => return Ok(false),
        };
        docs_scanned += 1;

        // Apply post-filter
        if let Some(ref pf) = plan.post_filter
            && !pf.matches(&doc)
        {
            return Ok(false);
        }

        // Handle offset
        if skipped < offset {
            skipped += 1;
            return Ok(false);
        }

        // Apply projection
        let doc = if let Some(ref fields) = query.fields {
            project_fields(&doc, fields)
        } else {
            doc
        };

        results.push(doc);
        if results.len() >= limit {
            has_more = true;
            return Ok(true); // signal: stop
        }
        Ok(false)
    };

    // Scan in the requested direction
    let read_tx = storage.db.read_tx();
    if reverse {
        for kv in read_tx.prefix(&partition, prefix).rev() {
            let (key, _) = kv?;
            if process_key(key.as_ref())? {
                break;
            }
        }
    } else {
        for kv in read_tx.prefix(&partition, prefix) {
            let (key, _) = kv?;
            if process_key(key.as_ref())? {
                break;
            }
        }
    }

    Ok(QueryResult {
        docs: results,
        total_count: None, // Unknown with early termination
        docs_scanned,
        index_used: Some(index_name.to_string()),
        scan_strategy: Some("index_sorted".to_string()),
        has_more,
    })
}

fn project_fields(doc: &Value, fields: &[String]) -> Value {
    let mut result = serde_json::Map::new();

    // Always include _id
    if let Some(id) = doc.get("_id") {
        result.insert("_id".to_string(), id.clone());
    }

    for field in fields {
        if let Some(val) = resolve_json_path(doc, field) {
            result.insert(field.clone(), val.clone());
        }
    }

    Value::Object(result)
}
