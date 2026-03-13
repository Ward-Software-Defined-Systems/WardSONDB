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
        &storage.scan_accelerator,
    );

    match &plan.scan {
        ScanPlan::FullScan => execute_full_scan(storage, collection, query, &plan),
        ScanPlan::IndexEq { .. }
        | ScanPlan::IndexIn { .. }
        | ScanPlan::IndexRange { .. }
        | ScanPlan::CompoundEq { .. } => execute_index_scan(storage, collection, query, &plan),
        ScanPlan::IndexSorted { .. } => execute_index_sorted(storage, collection, query, &plan),
        ScanPlan::CompoundRange { .. } => execute_compound_range(storage, collection, query, &plan),
        ScanPlan::BitmapScan => execute_bitmap_scan(storage, collection, query, &plan),
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
        ScanPlan::FullScan
        | ScanPlan::IndexSorted { .. }
        | ScanPlan::CompoundRange { .. }
        | ScanPlan::BitmapScan => unreachable!(),
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

/// Execute a compound range scan: equality prefix + range on next field.
fn execute_compound_range(
    storage: &Storage,
    collection: &str,
    query: &ParsedQuery,
    plan: &QueryPlan,
) -> Result<QueryResult, AppError> {
    let (index_name, eq_prefix, lower, upper) = match &plan.scan {
        ScanPlan::CompoundRange {
            index_name,
            eq_prefix,
            lower,
            upper,
        } => (
            index_name.as_str(),
            eq_prefix.as_slice(),
            lower.as_ref(),
            upper.as_ref(),
        ),
        _ => unreachable!(),
    };

    let partition = storage
        .index_manager
        .get_index_partition(collection, index_name)
        .ok_or_else(|| AppError::Internal(format!("Index partition not found: {index_name}")))?;

    // Build range start key: eq_prefix already has trailing 0x01
    let start_key: Vec<u8> = if let Some((lower_bytes, _inclusive)) = lower {
        let mut k = eq_prefix.to_vec();
        k.extend_from_slice(lower_bytes);
        k
    } else {
        eq_prefix.to_vec()
    };

    // Build range end key
    let end_key: Vec<u8> = if let Some((upper_bytes, inclusive)) = upper {
        let mut k = eq_prefix.to_vec();
        k.extend_from_slice(upper_bytes);
        if *inclusive {
            // Include all entries with this prefix (append high bytes)
            k.push(0x00);
            k.extend_from_slice(&[0xFF; 37]);
        }
        k
    } else {
        // End of this eq_prefix range: 0x01 → 0x02 bumps past separator
        let mut k = eq_prefix.to_vec();
        // Replace trailing 0x01 separator with 0x02 to get end of prefix range
        if let Some(last) = k.last_mut() {
            *last = 0x02;
        }
        k
    };

    let read_tx = storage.db.read_tx();

    // For non-inclusive lower bound, build the exact prefix to skip
    let lower_exact_prefix = if let Some((lower_bytes, false)) = lower {
        let mut p = eq_prefix.to_vec();
        p.extend_from_slice(lower_bytes);
        p.push(0x00); // separator before doc_id
        Some(p)
    } else {
        None
    };

    // count_only optimization: count index keys without loading docs
    if query.count_only && plan.post_filter.is_none() {
        let mut count = 0u64;
        for kv in read_tx.range(&partition, start_key.as_slice()..end_key.as_slice()) {
            let (key, _) = kv?;
            if let Some(ref prefix) = lower_exact_prefix
                && key.as_ref().starts_with(prefix)
            {
                continue;
            }
            count += 1;
        }
        return Ok(QueryResult {
            docs: vec![],
            total_count: Some(count),
            docs_scanned: 0,
            index_used: Some(index_name.to_string()),
            scan_strategy: Some("compound_range".to_string()),
            has_more: false,
        });
    }

    // Collect candidate doc IDs from the range scan
    let mut candidate_ids = Vec::new();
    for kv in read_tx.range(&partition, start_key.as_slice()..end_key.as_slice()) {
        let (key, _) = kv?;
        let key_ref = key.as_ref();

        if let Some(ref prefix) = lower_exact_prefix
            && key_ref.starts_with(prefix)
        {
            continue;
        }

        if let Some(id) = extract_doc_id_from_key(key_ref) {
            candidate_ids.push(id);
        }
    }

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

    // Apply post-filter
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
            index_used: Some(index_name.to_string()),
            scan_strategy: Some("compound_range".to_string()),
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
        index_used: Some(index_name.to_string()),
        scan_strategy: Some("compound_range".to_string()),
        has_more: false,
    })
}

/// Execute a query using the bitmap scan accelerator.
fn execute_bitmap_scan(
    storage: &Storage,
    collection: &str,
    query: &ParsedQuery,
    plan: &QueryPlan,
) -> Result<QueryResult, AppError> {
    let filter = plan.original_filter.as_ref().unwrap();
    let bitmap_result = match storage.scan_accelerator.bitmap_scan(filter) {
        Some(r) => r,
        None => {
            // Fallback to full scan if bitmap scan fails at execution time
            return execute_full_scan(storage, collection, query, plan);
        }
    };

    let bitmap = bitmap_result.bitmap;

    // count_only optimization — zero doc reads when no residual filter
    if query.count_only && bitmap_result.residual_filter.is_none() {
        return Ok(QueryResult {
            docs: vec![],
            total_count: Some(bitmap.len()),
            docs_scanned: 0,
            index_used: None,
            scan_strategy: Some("bitmap".to_string()),
            has_more: false,
        });
    }

    // Load documents by position
    let docs_partition = storage.get_docs_partition(collection)?;
    let mut loaded_docs = Vec::new();
    let mut docs_scanned = 0u64;

    for pos in bitmap.iter() {
        if let Some(doc_id) = storage.scan_accelerator.positions.get_doc_id(pos)
            && let Ok(Some(bytes)) = docs_partition.get(doc_id.as_str())
            && let Ok(doc) = serde_json::from_slice::<Value>(bytes.as_ref())
        {
            docs_scanned += 1;
            loaded_docs.push(doc);
        }
    }

    // Apply residual post-filter if any
    let mut matching = if let Some(ref residual) = bitmap_result.residual_filter {
        loaded_docs
            .into_iter()
            .filter(|doc| residual.matches(doc))
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
            index_used: None,
            scan_strategy: Some("bitmap".to_string()),
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
        scan_strategy: Some("bitmap".to_string()),
        has_more: false,
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
