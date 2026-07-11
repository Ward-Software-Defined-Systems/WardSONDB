use serde_json::Value;

use crate::engine::backend::StorageBackend;
use crate::engine::storage::Storage;
use crate::error::AppError;
use crate::index::secondary::{
    RangeScanBounds, extract_doc_id_from_key, make_compound_index_key, prefix_successor,
    range_scan_bounds, value_to_sortable_bytes,
};

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
        ScanPlan::BitmapScan { .. } => execute_bitmap_scan(storage, collection, query, &plan),
        ScanPlan::OrUnion { .. } => execute_or_union(storage, collection, query, &plan),
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
            scan_strategy: Some(plan.scan.name().to_string()),
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

/// Load only the `offset..offset+limit` window of an ordered candidate id
/// list — the bare-page fast path where no post-filter, sort, or cursor can
/// change which ids form the page. Framing matches `paginate_materialized`
/// exactly (`start = offset.min(len)`, `end = (start+limit).min(len)`,
/// `has_more = len > end`). Ids whose doc vanished in the index-read→get gap
/// shorten the page rather than shifting it — the same snapshot-gap semantic
/// as the count fast paths. Returns `(docs, docs_scanned, has_more)`;
/// `docs_scanned` counts the gets performed (the window), not the candidates.
fn load_id_window(
    storage: &Storage,
    collection: &str,
    query: &ParsedQuery,
    candidate_ids: &[String],
) -> Result<(Vec<Value>, u64, bool), AppError> {
    let total = candidate_ids.len();
    let start = (query.offset as usize).min(total);
    let end = start.saturating_add(query.limit as usize).min(total);

    let docs_partition = storage.get_docs_partition(collection)?;
    let mut docs = Vec::with_capacity(end - start);
    for id in &candidate_ids[start..end] {
        if let Ok(Some(bytes)) = storage.engine.get(&docs_partition, id.as_bytes())
            && let Ok(doc) = serde_json::from_slice::<Value>(&bytes)
        {
            docs.push(doc);
        }
    }
    let docs_scanned = (end - start) as u64;
    let has_more = total > end;

    let docs = if let Some(ref fields) = query.fields {
        docs.iter().map(|doc| project_fields(doc, fields)).collect()
    } else {
        docs
    };
    Ok((docs, docs_scanned, has_more))
}

/// True when nothing after the scan can change which candidates form the
/// page: no residual filter, no sort, no cursor, and docs are wanted.
fn bare_page(query: &ParsedQuery, post_filter: &Option<crate::query::filter::FilterNode>) -> bool {
    !query.count_only && post_filter.is_none() && query.sort.is_empty() && query.cursor.is_none()
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
                    scan_strategy: Some(plan.scan.name().to_string()),
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
                    // Dedup by encoded value — the identity the index prefix
                    // uses — or $in: ["a","a"] double-counts (the non-count
                    // path dedups by doc id and never had this).
                    let mut seen = std::collections::HashSet::new();
                    for value in values {
                        if !seen.insert(value_to_sortable_bytes(value)) {
                            continue;
                        }
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
                        scan_strategy: Some(plan.scan.name().to_string()),
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
                        scan_strategy: Some(plan.scan.name().to_string()),
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
                // Keys-only backend count; errors propagate instead of the
                // old flatten().count() silently undercounting on them.
                let count = storage.engine.count_prefix(&partition, prefix)?;
                return Ok(QueryResult {
                    docs: vec![],
                    total_count: Some(count),
                    docs_scanned: 0,
                    index_used: Some(index_name.clone()),
                    scan_strategy: Some(plan.scan.name().to_string()),
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
        | ScanPlan::BitmapScan { .. }
        | ScanPlan::OrUnion { .. } => unreachable!(),
    };

    // Bare page: the page is exactly candidate_ids[offset..offset+limit] in
    // index order — load only that window instead of every candidate (an eq
    // filter matching 50k docs with limit 10 was doing 50k gets + parses).
    if bare_page(query, &plan.post_filter) {
        let total = candidate_ids.len() as u64;
        let (docs, docs_scanned, has_more) =
            load_id_window(storage, collection, query, &candidate_ids)?;
        return Ok(QueryResult {
            docs,
            total_count: Some(total),
            docs_scanned,
            index_used: Some(index_name),
            scan_strategy: None,
            has_more,
            next_cursor: None,
        });
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
        // The materialized count (post-filter present, or a fast path that
        // didn't apply) — label it with the strategy that produced it.
        return Ok(QueryResult {
            docs: vec![],
            total_count: Some(total_count),
            docs_scanned,
            index_used: Some(index_name),
            scan_strategy: Some(plan.scan.name().to_string()),
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

/// Rebuild the exact index key for a cursor position under this plan's
/// prefix: the cursor's (sort values, last_id) tail IS a compound index key,
/// so reuse `make_compound_index_key` — the planner prefix already carries
/// its trailing 0x01 separator.
fn index_cursor_key(prefix: &[u8], cursor: &Cursor) -> Vec<u8> {
    // Missing is unreachable on this path (the planner rejects such cursors
    // before choosing an index seek); filter_map keeps the function total and
    // the debug_assert pins the invariant.
    let values: Vec<&Value> = cursor
        .sort_values
        .iter()
        .filter_map(|cv| match cv {
            CursorValue::Present(v) => Some(v),
            CursorValue::Missing => None,
        })
        .collect();
    debug_assert_eq!(
        values.len(),
        cursor.sort_values.len(),
        "cursor with Missing sort values must not reach the index seek path"
    );
    let mut key = prefix.to_vec();
    key.extend_from_slice(&make_compound_index_key(&values, &cursor.last_id));
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
    // is only the snapshot gap. offset is unclamped user input — saturate
    // (usize::MAX degrades to an unbounded read, which is the correct
    // semantic for an offset past the end).
    let max_results = plan
        .post_filter
        .is_none()
        .then_some(offset.saturating_add(limit).saturating_add(1));

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
        scan_strategy: Some(plan.scan.name().to_string()),
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

    // Shared bounds builder: eq_prefix (with its trailing 0x01) glued to the
    // range-field bounds, open ends closed over the operand's type bracket,
    // exclusive lower folded into the start key (no skip loop needed).
    let (start_key, end_key) = match range_scan_bounds(
        eq_prefix,
        lower.map(|(b, i)| (b.as_slice(), *i)),
        upper.map(|(b, i)| (b.as_slice(), *i)),
    ) {
        RangeScanBounds::Empty => {
            // The range predicate can never match (null/array/object operand
            // or cross-bucket bounds) — serve every path without a scan.
            return Ok(QueryResult {
                docs: vec![],
                total_count: Some(0),
                docs_scanned: 0,
                index_used: Some(index_name.to_string()),
                scan_strategy: Some(plan.scan.name().to_string()),
                has_more: false,
                next_cursor: None,
            });
        }
        RangeScanBounds::Span { start, end } => (start, end),
    };

    // count_only optimization: count index keys without loading docs.
    if query.count_only && plan.post_filter.is_none() {
        let count = storage
            .engine
            .count_range(&partition, &start_key, &end_key)?;
        return Ok(QueryResult {
            docs: vec![],
            total_count: Some(count),
            docs_scanned: 0,
            index_used: Some(index_name.to_string()),
            scan_strategy: Some(plan.scan.name().to_string()),
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
        if let Some(id) = extract_doc_id_from_key(&key) {
            candidate_ids.push(id);
        }
    }

    // Bare page: window the range-ordered candidates (same rationale and
    // semantics as the index-scan fast path above).
    if bare_page(query, &plan.post_filter) {
        let total = candidate_ids.len() as u64;
        let (docs, docs_scanned, has_more) =
            load_id_window(storage, collection, query, &candidate_ids)?;
        return Ok(QueryResult {
            docs,
            total_count: Some(total),
            docs_scanned,
            index_used: Some(index_name.to_string()),
            scan_strategy: Some(plan.scan.name().to_string()),
            has_more,
            next_cursor: None,
        });
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
            scan_strategy: Some(plan.scan.name().to_string()),
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
        scan_strategy: Some(plan.scan.name().to_string()),
        has_more,
        next_cursor,
    })
}

/// Candidate doc ids for one `$or` arm, plus the arm's index name. Arms are
/// only ever the index-servable variants (`plan_or_arm` builds them).
fn or_arm_ids<'a>(
    storage: &Storage,
    collection: &str,
    arm: &'a ScanPlan,
) -> Result<(&'a str, Vec<String>), AppError> {
    match arm {
        ScanPlan::IndexEq {
            index_name,
            field,
            value,
        } => {
            let ids = storage
                .index_manager
                .lookup_eq(&storage.engine, collection, field, value)
                .unwrap_or_default();
            Ok((index_name, ids))
        }
        ScanPlan::IndexIn {
            index_name,
            field,
            values,
        } => {
            let ids = storage
                .index_manager
                .lookup_in(&storage.engine, collection, field, values)
                .unwrap_or_default();
            Ok((index_name, ids))
        }
        ScanPlan::IndexRange {
            index_name,
            field,
            lower,
            upper,
        } => {
            let lower_ref = lower.as_ref().map(|(v, i)| (v, *i));
            let upper_ref = upper.as_ref().map(|(v, i)| (v, *i));
            let ids = storage
                .index_manager
                .lookup_range(&storage.engine, collection, field, lower_ref, upper_ref)
                .unwrap_or_default();
            Ok((index_name, ids))
        }
        ScanPlan::CompoundEq { index_name, prefix } => {
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
            Ok((index_name, ids))
        }
        ScanPlan::CompoundRange {
            index_name,
            eq_prefix,
            lower,
            upper,
        } => {
            let bounds = range_scan_bounds(
                eq_prefix,
                lower.as_ref().map(|(b, i)| (b.as_slice(), *i)),
                upper.as_ref().map(|(b, i)| (b.as_slice(), *i)),
            );
            let (start, end) = match bounds {
                // This arm alone matches nothing; other arms still contribute.
                RangeScanBounds::Empty => return Ok((index_name, Vec::new())),
                RangeScanBounds::Span { start, end } => (start, end),
            };
            let partition = storage
                .index_manager
                .get_index_partition(collection, index_name)
                .ok_or_else(|| {
                    AppError::Internal(format!("Index partition not found: {index_name}"))
                })?;
            let mut ids = Vec::new();
            for kv in storage
                .engine
                .range_iterator(&partition, &start, &end, None)?
            {
                let (key, _) = kv?;
                if let Some(id) = extract_doc_id_from_key(&key) {
                    ids.push(id);
                }
            }
            Ok((index_name, ids))
        }
        ScanPlan::FullScan
        | ScanPlan::IndexSorted { .. }
        | ScanPlan::BitmapScan { .. }
        | ScanPlan::OrUnion { .. } => unreachable!("or_union arms are index scans"),
    }
}

/// Execute a `$or` union of per-arm index lookups. Ids are unioned across
/// arms (deduped) and re-sorted to `_id` order — the docs-partition order a
/// full scan yields — so pages, counts, and offset tiling are byte-identical
/// to the full-scan path this replaces; only `docs_scanned`, `index_used`,
/// and `scan_strategy` differ. When any arm over-approximates, the plan's
/// post-filter is the original `$or` (see `ScanPlan::OrUnion`).
fn execute_or_union(
    storage: &Storage,
    collection: &str,
    query: &ParsedQuery,
    plan: &QueryPlan,
) -> Result<QueryResult, AppError> {
    let ScanPlan::OrUnion { arms } = &plan.scan else {
        unreachable!()
    };

    let mut seen = std::collections::HashSet::new();
    let mut ids: Vec<String> = Vec::new();
    let mut index_names: Vec<&str> = Vec::new();
    for arm in arms {
        let (index_name, arm_ids) = or_arm_ids(storage, collection, arm)?;
        if !index_names.contains(&index_name) {
            index_names.push(index_name);
        }
        for id in arm_ids {
            if seen.insert(id.clone()) {
                ids.push(id);
            }
        }
    }
    // _id order == docs-partition key order == full-scan output order.
    ids.sort_unstable();
    let index_used = Some(index_names.join("+"));

    // Exact arms: the union IS the match set — count without loading a doc.
    if query.count_only && plan.post_filter.is_none() {
        return Ok(QueryResult {
            docs: vec![],
            total_count: Some(ids.len() as u64),
            docs_scanned: 0,
            index_used,
            scan_strategy: Some(plan.scan.name().to_string()),
            has_more: false,
            next_cursor: None,
        });
    }

    // Bare page: window the _id-ordered union (same semantics as the other
    // windowed index paths — total_count is the full union size).
    if bare_page(query, &plan.post_filter) {
        let total = ids.len() as u64;
        let (docs, docs_scanned, has_more) = load_id_window(storage, collection, query, &ids)?;
        return Ok(QueryResult {
            docs,
            total_count: Some(total),
            docs_scanned,
            index_used,
            scan_strategy: Some(plan.scan.name().to_string()),
            has_more,
            next_cursor: None,
        });
    }

    let docs_scanned = ids.len() as u64;

    let docs_partition = storage.get_docs_partition(collection)?;
    let mut loaded_docs = Vec::with_capacity(ids.len());
    for id in &ids {
        if let Ok(Some(bytes)) = storage.engine.get(&docs_partition, id.as_bytes())
            && let Ok(doc) = serde_json::from_slice::<Value>(&bytes)
        {
            loaded_docs.push(doc);
        }
    }

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
            index_used,
            scan_strategy: Some(plan.scan.name().to_string()),
            has_more: false,
            next_cursor: None,
        });
    }

    let (docs, has_more, next_cursor) = paginate_materialized(matching, query, collection);

    Ok(QueryResult {
        docs,
        total_count: Some(total_count),
        docs_scanned,
        index_used,
        scan_strategy: Some(plan.scan.name().to_string()),
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
    // The plan carries the bitmap computed during planning (Roaring AND/OR
    // over per-value bitmaps is not free — recomputing it here doubled that
    // work) and its residual lives in plan.post_filter. Position resolution
    // below tolerates staleness relative to concurrent writes the same way
    // the old plan-then-recompute window did.
    let ScanPlan::BitmapScan { bitmap } = &plan.scan else {
        unreachable!()
    };

    // count_only optimization — zero doc reads when no residual filter
    if query.count_only && plan.post_filter.is_none() {
        return Ok(QueryResult {
            docs: vec![],
            total_count: Some(bitmap.len()),
            docs_scanned: 0,
            index_used: None,
            scan_strategy: Some(plan.scan.name().to_string()),
            has_more: false,
            next_cursor: None,
        });
    }

    // Bare page: the page is the offset..offset+limit window of the
    // ascending-position order — resolve and load only that window, with a
    // +1 probe id so has_more stays exact even across transient holes.
    // total_count is the bitmap cardinality, matching the count fast path
    // above (same snapshot-gap semantic as the index windows).
    if bare_page(query, &plan.post_filter) {
        let total = bitmap.len();
        let offset = query.offset as usize;
        let limit = query.limit as usize;
        let mut ids = storage.scan_accelerator.positions.resolve_window(
            bitmap,
            offset,
            limit.saturating_add(1),
        );
        let has_more = ids.len() > limit;
        ids.truncate(limit);

        let docs_partition = storage.get_docs_partition(collection)?;
        let mut docs = Vec::with_capacity(ids.len());
        for doc_id in &ids {
            if let Ok(Some(bytes)) = storage.engine.get(&docs_partition, doc_id.as_bytes())
                && let Ok(doc) = serde_json::from_slice::<Value>(&bytes)
            {
                docs.push(doc);
            }
        }
        let docs_scanned = ids.len() as u64;
        let docs = if let Some(ref fields) = query.fields {
            docs.iter().map(|doc| project_fields(doc, fields)).collect()
        } else {
            docs
        };
        return Ok(QueryResult {
            docs,
            total_count: Some(total),
            docs_scanned,
            index_used: None,
            scan_strategy: Some(plan.scan.name().to_string()),
            has_more,
            next_cursor: None,
        });
    }

    // Resolve every matching position to its id under ONE short guard —
    // the per-position lookup took the position read lock once per doc, and
    // holding any single guard across the get() IO loop is the b965de5
    // deadlock pattern.
    let ids = storage
        .scan_accelerator
        .positions
        .resolve_window(bitmap, 0, usize::MAX);

    // Load documents by id
    let docs_partition = storage.get_docs_partition(collection)?;
    let mut loaded_docs = Vec::with_capacity(ids.len());
    let mut docs_scanned = 0u64;

    for doc_id in &ids {
        if let Ok(Some(bytes)) = storage.engine.get(&docs_partition, doc_id.as_bytes())
            && let Ok(doc) = serde_json::from_slice::<Value>(&bytes)
        {
            docs_scanned += 1;
            loaded_docs.push(doc);
        }
    }

    // Apply residual post-filter if any
    let matching: Vec<Value> = if let Some(ref residual) = plan.post_filter {
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
            scan_strategy: Some(plan.scan.name().to_string()),
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
        scan_strategy: Some(plan.scan.name().to_string()),
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
