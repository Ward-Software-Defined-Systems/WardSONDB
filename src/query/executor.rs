use serde_json::Value;

use crate::engine::backend::StorageBackend;
use crate::engine::storage::Storage;
use crate::error::AppError;
use crate::index::secondary::{extract_doc_id_from_key, value_to_sortable_bytes};

use super::cursor::{Cursor, CursorValue, compare_doc_to_cursor, encode_cursor};
use super::filter::resolve_json_path;
use super::parser::ParsedQuery;
use super::planner::{QueryPlan, ScanPlan, plan_query};
use super::sort::compare_docs;

#[derive(Debug)]
pub struct QueryResult {
    pub docs: Vec<Value>,
    pub total_count: Option<u64>,
    pub docs_scanned: u64,
    pub index_used: Option<String>,
    pub scan_strategy: Option<String>,
    pub has_more: bool,
    pub next_cursor: Option<String>,
}

pub fn execute_query(
    storage: &Storage,
    collection: &str,
    query: &ParsedQuery,
) -> Result<QueryResult, AppError> {
    // Unfiltered count: DocCounters is authoritative (seeded by a full count
    // at startup, maintained on every insert/delete path including bulk,
    // delete_by_query, and TTL cleanup), so the O(n) scan-and-parse the full
    // scan would do is pure waste — ~335 ms on a 100k-doc collection.
    if query.count_only && query.filter.is_none() {
        storage.ensure_collection_exists(collection)?;
        return Ok(QueryResult {
            docs: vec![],
            total_count: Some(storage.doc_counts.get(collection).max(0) as u64),
            docs_scanned: 0,
            index_used: None,
            scan_strategy: Some("doc_counter".to_string()),
            has_more: false,
            next_cursor: None,
        });
    }

    let plan = plan_query(
        query,
        &storage.index_manager,
        collection,
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

/// Final ordering, pagination, and projection for strategies that materialize
/// the full match set. Sorts with the `_id`-tiebreak comparator whenever a
/// sort or a cursor is present (a cursor with no sort needs the deterministic
/// `_id` order — this is also what makes bitmap scans, whose natural order is
/// insertion position, cursor-safe). Resolves the page window from the cursor
/// position or the offset, computes an exact `has_more`, and builds
/// `next_cursor` from the last page document BEFORE projection strips fields.
fn paginate_materialized(
    mut matching: Vec<Value>,
    query: &ParsedQuery,
    collection: &str,
) -> (Vec<Value>, bool, Option<String>) {
    use std::cmp::Ordering;

    if query.cursor.is_some() || !query.sort.is_empty() {
        matching.sort_by(|a, b| compare_docs(a, b, &query.sort));
    }

    let start = match &query.cursor {
        // Sorted by the same total order the cursor encodes, so the page
        // starts exactly where docs stop comparing at-or-before the cursor.
        Some(cursor) => matching.partition_point(|doc| {
            compare_doc_to_cursor(doc, cursor, &query.sort) != Ordering::Greater
        }),
        None => (query.offset as usize).min(matching.len()),
    };
    let end = start
        .saturating_add(query.limit as usize)
        .min(matching.len());
    let has_more = matching.len() > end;

    let next_cursor =
        if has_more && end > start && (query.cursor.is_some() || !query.sort.is_empty()) {
            encode_cursor(&matching[end - 1], &query.sort, collection)
        } else {
            None
        };

    matching.truncate(end);
    let page = matching.split_off(start);

    let docs = if let Some(ref fields) = query.fields {
        page.iter().map(|doc| project_fields(doc, fields)).collect()
    } else {
        page
    };

    (docs, has_more, next_cursor)
}

fn execute_full_scan(
    storage: &Storage,
    collection: &str,
    query: &ParsedQuery,
    plan: &QueryPlan,
) -> Result<QueryResult, AppError> {
    // Cursor + no sort: the total order is _id ascending, which is exactly
    // the docs partition's key order — seek straight to the position instead
    // of materializing everything before it.
    if let Some(cursor) = &query.cursor
        && query.sort.is_empty()
        && !query.count_only
    {
        return execute_full_scan_id_seek(storage, collection, query, plan, cursor);
    }

    let all_docs = storage.scan_all_documents(collection)?;
    let docs_scanned = all_docs.len() as u64;

    let filter = plan.original_filter.as_ref();
    let matching: Vec<Value> = if let Some(filter) = filter {
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
            next_cursor: None,
        });
    }

    let (docs, has_more, mut next_cursor) = paginate_materialized(matching, query, collection);

    // Bootstrap cursor for no-sort walks: a full scan streams in _id order
    // (the docs partition key), so the position is sound without a sort spec.
    // Only _id is needed here, which projection always preserves. Index and
    // bitmap scans must NOT do this — their no-sort order isn't _id.
    if next_cursor.is_none() && has_more && query.sort.is_empty() {
        next_cursor = docs
            .last()
            .and_then(|doc| encode_cursor(doc, &[], collection));
    }

    Ok(QueryResult {
        docs,
        total_count: Some(total_count),
        docs_scanned,
        index_used: None,
        scan_strategy: None,
        has_more,
        next_cursor,
    })
}

/// Cursor-resumed full scan with an empty sort: seek the docs partition
/// (key = `_id`) to just after the cursor's id and stream forward with a
/// limit+1 probe.
fn execute_full_scan_id_seek(
    storage: &Storage,
    collection: &str,
    query: &ParsedQuery,
    plan: &QueryPlan,
    cursor: &Cursor,
) -> Result<QueryResult, AppError> {
    let docs_partition = storage.get_docs_partition(collection)?;
    let limit = query.limit as usize;

    // Strictly after last_id: ids are NUL-free, so last_id ++ 0x00 is the
    // smallest key greater than last_id.
    let mut lo = cursor.last_id.clone().into_bytes();
    lo.push(0x00);
    // _ids are UTF-8 strings and 0xFF never occurs in UTF-8, so a single
    // 0xFF byte sorts above every doc key.
    let hi = [0xFFu8];

    let max_results = plan.original_filter.is_none().then_some(limit + 1);

    let mut results: Vec<Value> = Vec::new();
    let mut docs_scanned = 0u64;
    for kv in storage
        .engine
        .range_iterator(&docs_partition, &lo, &hi, max_results)?
    {
        let (_, value_bytes) = kv?;
        let Ok(doc) = serde_json::from_slice::<Value>(&value_bytes) else {
            continue;
        };
        docs_scanned += 1;
        if let Some(filter) = &plan.original_filter
            && !filter.matches(&doc)
        {
            continue;
        }
        results.push(doc);
        if results.len() > limit {
            break;
        }
    }

    let has_more = results.len() > limit;
    results.truncate(limit);
    let next_cursor = if has_more {
        results
            .last()
            .and_then(|doc| encode_cursor(doc, &[], collection))
    } else {
        None
    };

    let docs = if let Some(ref fields) = query.fields {
        results
            .iter()
            .map(|doc| project_fields(doc, fields))
            .collect()
    } else {
        results
    };

    Ok(QueryResult {
        docs,
        total_count: None, // the seek never sees the full match set
        docs_scanned,
        index_used: None,
        scan_strategy: None,
        has_more,
        next_cursor,
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
                        .count_eq(&storage.engine, collection, field, value)
            {
                return Ok(QueryResult {
                    docs: vec![],
                    total_count: Some(count),
                    docs_scanned: 0,
                    index_used: Some(index_name.clone()),
                    scan_strategy: None,
                    has_more: false,
                    next_cursor: None,
                });
            }

            let ids = storage
                .index_manager
                .lookup_eq(&storage.engine, collection, field, value)
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
                        if let Some(count) = storage.index_manager.count_eq(
                            &storage.engine,
                            collection,
                            field,
                            value,
                        ) {
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
                        next_cursor: None,
                    });
                }
            }

            let ids = storage
                .index_manager
                .lookup_in(&storage.engine, collection, field, values)
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
                    &storage.engine,
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
                        next_cursor: None,
                    });
                }
            }

            let lower_ref = lower.as_ref().map(|(v, i)| (v, *i));
            let upper_ref = upper.as_ref().map(|(v, i)| (v, *i));
            let ids = storage
                .index_manager
                .lookup_range(&storage.engine, collection, field, lower_ref, upper_ref)
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
                let count = storage
                    .engine
                    .prefix_iterator(&partition, prefix)?
                    .flatten()
                    .count() as u64;
                return Ok(QueryResult {
                    docs: vec![],
                    total_count: Some(count),
                    docs_scanned: 0,
                    index_used: Some(index_name.clone()),
                    scan_strategy: Some("compound_eq".to_string()),
                    has_more: false,
                    next_cursor: None,
                });
            }

            let partition = storage
                .index_manager
                .get_index_partition(collection, index_name)
                .ok_or_else(|| {
                    AppError::Internal(format!("Index partition not found: {index_name}"))
                })?;
            let mut ids = Vec::new();
            for (key, _) in storage
                .engine
                .prefix_iterator(&partition, prefix)?
                .flatten()
            {
                if let Some(id) = extract_doc_id_from_key(&key) {
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
        if let Ok(Some(bytes)) = storage.engine.get(&docs_partition, id.as_bytes())
            && let Ok(doc) = serde_json::from_slice::<Value>(&bytes)
        {
            loaded_docs.push(doc);
        }
    }

    // Apply post-filter (residual conditions not covered by the index)
    let matching: Vec<Value> = if let Some(ref post_filter) = plan.post_filter {
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
            next_cursor: None,
        });
    }

    let (docs, has_more, next_cursor) = paginate_materialized(matching, query, collection);

    Ok(QueryResult {
        docs,
        total_count: Some(total_count),
        docs_scanned,
        index_used: Some(index_name),
        scan_strategy: None,
        has_more,
        next_cursor,
    })
}

/// Smallest byte string greater than every key that has `prefix` as a
/// prefix — the exclusive upper bound of the prefix window. IndexSorted
/// prefixes always end with the 0x01 field separator, so in practice this
/// bumps that byte; the loop handles trailing 0xFF for generality. An
/// all-0xFF prefix has no finite successor (unreachable here), so a maximal
/// sentinel keeps the function total.
fn prefix_successor(prefix: &[u8]) -> Vec<u8> {
    let mut p = prefix.to_vec();
    while let Some(&last) = p.last() {
        if last == 0xFF {
            p.pop();
        } else {
            *p.last_mut().unwrap() = last + 1;
            return p;
        }
    }
    vec![0xFF; prefix.len() + 1]
}

/// Rebuild the exact index key for a cursor position under this plan's
/// prefix: `prefix ++ join(0x01, sortable(values)) ++ 0x00 ++ last_id` —
/// byte-identical to `make_compound_index_key` for the eq+sort fields (the
/// planner prefix already carries its trailing 0x01 separator).
fn index_cursor_key(prefix: &[u8], cursor: &Cursor) -> Vec<u8> {
    let mut key = prefix.to_vec();
    for (i, cv) in cursor.sort_values.iter().enumerate() {
        if i > 0 {
            key.push(0x01);
        }
        // Missing is unreachable on this path (the planner rejects such
        // cursors); encoding nothing keeps the function total.
        if let CursorValue::Present(v) = cv {
            key.extend_from_slice(&value_to_sortable_bytes(v));
        }
    }
    key.push(0x00);
    key.extend_from_slice(cursor.last_id.as_bytes());
    key
}

/// Execute a sorted index scan with early termination.
/// Uses a compound index that covers both filter and sort fields.
fn execute_index_sorted(
    storage: &Storage,
    collection: &str,
    query: &ParsedQuery,
    plan: &QueryPlan,
) -> Result<QueryResult, AppError> {
    let (index_name, prefix, reverse, exact_tail) = match &plan.scan {
        ScanPlan::IndexSorted {
            index_name,
            prefix,
            reverse,
            exact_tail,
        } => (
            index_name.as_str(),
            prefix.as_slice(),
            *reverse,
            *exact_tail,
        ),
        _ => unreachable!(),
    };

    let partition = storage
        .index_manager
        .get_index_partition(collection, index_name)
        .ok_or_else(|| AppError::Internal(format!("Index partition not found: {index_name}")))?;

    let docs_partition = storage.get_docs_partition(collection)?;

    let offset = query.offset as usize;
    let limit = query.limit as usize;

    // Scan bounds: the whole prefix window, tightened to strictly after
    // (forward) or strictly before (reverse) the cursor position. The planner
    // only routes cursors here when exact_tail holds and no sort value is
    // Missing, so the cursor maps onto an exact index key.
    let prefix_end = prefix_successor(prefix);
    let (lo, hi) = match &query.cursor {
        Some(cursor) => {
            let cursor_key = index_cursor_key(prefix, cursor);
            if reverse {
                (prefix.to_vec(), cursor_key) // end-exclusive = strictly below
            } else {
                // Doc ids are NUL-free, so no real key equals cursor_key ++
                // 0x00 — appending it yields the smallest strictly-greater key.
                let mut lo = cursor_key;
                lo.push(0x00);
                (lo, prefix_end)
            }
        }
        None => (prefix.to_vec(), prefix_end),
    };

    // Bound the backend read to the page probe when nothing gets post-
    // filtered away. An index entry whose doc vanished between iterator
    // creation and the `get` consumes a slot and could understate has_more,
    // but doc + index entries are deleted in one atomic batch, so the window
    // is only the snapshot gap.
    let max_results = plan.post_filter.is_none().then_some(offset + limit + 1);

    let iter = if reverse {
        storage
            .engine
            .range_iterator_rev(&partition, &lo, &hi, max_results)?
    } else {
        storage
            .engine
            .range_iterator(&partition, &lo, &hi, max_results)?
    };

    // Collect UNPROJECTED docs up to limit+1: the extra probe row makes
    // has_more exact, and the cursor must see sort-field values that a
    // projection might strip.
    let mut results: Vec<Value> = Vec::new();
    let mut skipped = 0usize;
    let mut docs_scanned = 0u64;

    for kv in iter {
        let (key, _) = kv?;
        let Some(doc_id) = extract_doc_id_from_key(&key) else {
            continue;
        };
        let doc = match storage.engine.get(&docs_partition, doc_id.as_bytes()) {
            Ok(Some(bytes)) => match serde_json::from_slice::<Value>(&bytes) {
                Ok(doc) => doc,
                Err(_) => continue,
            },
            _ => continue,
        };
        docs_scanned += 1;

        if let Some(ref pf) = plan.post_filter
            && !pf.matches(&doc)
        {
            continue;
        }

        if skipped < offset {
            skipped += 1;
            continue;
        }

        results.push(doc);
        if results.len() > limit {
            break;
        }
    }

    let has_more = results.len() > limit;
    results.truncate(limit);

    let next_cursor = if has_more && exact_tail {
        results
            .last()
            .and_then(|doc| encode_cursor(doc, &query.sort, collection))
    } else {
        None
    };

    let docs = if let Some(ref fields) = query.fields {
        results
            .iter()
            .map(|doc| project_fields(doc, fields))
            .collect()
    } else {
        results
    };

    Ok(QueryResult {
        docs,
        total_count: None, // Unknown with early termination
        docs_scanned,
        index_used: Some(index_name.to_string()),
        scan_strategy: Some("index_sorted".to_string()),
        has_more,
        next_cursor,
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
        for kv in storage.engine.range_iterator(
            &partition,
            start_key.as_slice(),
            end_key.as_slice(),
            None,
        )? {
            let (key, _) = kv?;
            if let Some(ref prefix) = lower_exact_prefix
                && key.starts_with(prefix)
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
            next_cursor: None,
        });
    }

    // Collect candidate doc IDs from the range scan
    let mut candidate_ids = Vec::new();
    for kv in
        storage
            .engine
            .range_iterator(&partition, start_key.as_slice(), end_key.as_slice(), None)?
    {
        let (key, _) = kv?;

        if let Some(ref prefix) = lower_exact_prefix
            && key.starts_with(prefix)
        {
            continue;
        }

        if let Some(id) = extract_doc_id_from_key(&key) {
            candidate_ids.push(id);
        }
    }

    let docs_scanned = candidate_ids.len() as u64;

    // Load documents by ID
    let docs_partition = storage.get_docs_partition(collection)?;
    let mut loaded_docs = Vec::with_capacity(candidate_ids.len());
    for id in &candidate_ids {
        if let Ok(Some(bytes)) = storage.engine.get(&docs_partition, id.as_bytes())
            && let Ok(doc) = serde_json::from_slice::<Value>(&bytes)
        {
            loaded_docs.push(doc);
        }
    }

    // Apply post-filter
    let matching: Vec<Value> = if let Some(ref post_filter) = plan.post_filter {
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
            next_cursor: None,
        });
    }

    let (docs, has_more, next_cursor) = paginate_materialized(matching, query, collection);

    Ok(QueryResult {
        docs,
        total_count: Some(total_count),
        docs_scanned,
        index_used: Some(index_name.to_string()),
        scan_strategy: Some("compound_range".to_string()),
        has_more,
        next_cursor,
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
            next_cursor: None,
        });
    }

    // Load documents by position
    let docs_partition = storage.get_docs_partition(collection)?;
    let mut loaded_docs = Vec::new();
    let mut docs_scanned = 0u64;

    for pos in bitmap.iter() {
        if let Some(doc_id) = storage.scan_accelerator.positions.get_doc_id(pos)
            && let Ok(Some(bytes)) = storage.engine.get(&docs_partition, doc_id.as_bytes())
            && let Ok(doc) = serde_json::from_slice::<Value>(&bytes)
        {
            docs_scanned += 1;
            loaded_docs.push(doc);
        }
    }

    // Apply residual post-filter if any
    let matching: Vec<Value> = if let Some(ref residual) = bitmap_result.residual_filter {
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
            next_cursor: None,
        });
    }

    let (docs, has_more, next_cursor) = paginate_materialized(matching, query, collection);

    Ok(QueryResult {
        docs,
        total_count: Some(total_count),
        docs_scanned,
        index_used: None,
        scan_strategy: Some("bitmap".to_string()),
        has_more,
        next_cursor,
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
