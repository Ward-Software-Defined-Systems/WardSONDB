use std::collections::HashSet;

use serde_json::Value;

use crate::index::IndexManager;
use crate::index::secondary::value_to_sortable_bytes;

use super::filter::{FilterNode, FilterOp};
use super::sort::SortField;

/// What scan strategy to use for a query.
#[derive(Debug)]
pub enum ScanPlan {
    /// Full collection scan — no usable index.
    FullScan,

    /// Index equality scan — one or more exact values.
    IndexEq {
        index_name: String,
        field: String,
        value: Value,
    },

    /// Index $in scan — union of equality lookups.
    IndexIn {
        index_name: String,
        field: String,
        values: Vec<Value>,
    },

    /// Index range scan.
    IndexRange {
        index_name: String,
        field: String,
        lower: Option<(Value, bool)>, // (value, inclusive)
        upper: Option<(Value, bool)>,
    },

    /// Compound equality scan — multiple equality fields covered by one compound index.
    CompoundEq { index_name: String, prefix: Vec<u8> },

    /// Sorted index scan with early termination.
    /// A compound index covers both the filter field(s) and the sort field,
    /// allowing us to iterate in sort order and stop after offset+limit docs.
    IndexSorted {
        index_name: String,
        prefix: Vec<u8>,
        reverse: bool,
    },
}

/// The result of planning: a scan strategy + optional residual filter.
pub struct QueryPlan {
    pub scan: ScanPlan,
    /// Residual filter to apply to documents after the index scan.
    pub post_filter: Option<FilterNode>,
    /// Full original filter (used for full-scan path).
    pub original_filter: Option<FilterNode>,
}

/// Plan the best scan strategy for a filter, given available indexes.
pub fn plan_query(
    filter: &Option<FilterNode>,
    index_manager: &IndexManager,
    collection: &str,
    sort: &[SortField],
    limit: u64,
    count_only: bool,
) -> QueryPlan {
    let Some(filter) = filter else {
        return QueryPlan {
            scan: ScanPlan::FullScan,
            post_filter: None,
            original_filter: None,
        };
    };

    // Try IndexSorted first (highest priority — enables early termination)
    // Only when: has sort, finite limit, not count_only
    if !sort.is_empty()
        && limit < u64::MAX
        && !count_only
        && let Some(plan) = try_index_sorted(index_manager, collection, filter, sort)
    {
        return plan;
    }

    match filter {
        // Simple comparison — check if the field is indexed
        FilterNode::Comparison { field, op, value } => {
            if let Some(plan) = try_index_comparison(index_manager, collection, field, op, value) {
                return QueryPlan {
                    scan: plan,
                    post_filter: None,
                    original_filter: Some(filter.clone()),
                };
            }
            QueryPlan {
                scan: ScanPlan::FullScan,
                post_filter: None,
                original_filter: Some(filter.clone()),
            }
        }

        // AND — try compound multi-field eq, then single-field index
        FilterNode::And(children) => {
            // Try compound multi-field equality (Opt 3)
            if let Some(plan) = try_compound_eq(index_manager, collection, children, filter) {
                return plan;
            }

            // Try each child to see if it can use a single-field index
            for (i, child) in children.iter().enumerate() {
                if let FilterNode::Comparison { field, op, value } = child
                    && let Some(plan) =
                        try_index_comparison(index_manager, collection, field, op, value)
                {
                    let remaining: Vec<FilterNode> = children
                        .iter()
                        .enumerate()
                        .filter(|(j, _)| *j != i)
                        .map(|(_, c)| c.clone())
                        .collect();

                    let post = if remaining.is_empty() {
                        None
                    } else if remaining.len() == 1 {
                        Some(remaining.into_iter().next().unwrap())
                    } else {
                        Some(FilterNode::And(remaining))
                    };

                    return QueryPlan {
                        scan: plan,
                        post_filter: post,
                        original_filter: Some(filter.clone()),
                    };
                }
            }

            // Also try range combination: if we have both $gte and $lte on the same indexed field
            if let Some(plan) = try_range_from_and(index_manager, collection, children) {
                // Build post-filter excluding the range components
                let range_field = match &plan {
                    ScanPlan::IndexRange { field, .. } => field.clone(),
                    _ => String::new(),
                };
                let remaining: Vec<FilterNode> = children
                    .iter()
                    .filter(|c| {
                        if let FilterNode::Comparison { field, op, .. } = c {
                            !(field == &range_field
                                && matches!(
                                    op,
                                    FilterOp::Gt | FilterOp::Gte | FilterOp::Lt | FilterOp::Lte
                                ))
                        } else {
                            true
                        }
                    })
                    .cloned()
                    .collect();

                let post = if remaining.is_empty() {
                    None
                } else if remaining.len() == 1 {
                    Some(remaining.into_iter().next().unwrap())
                } else {
                    Some(FilterNode::And(remaining))
                };

                return QueryPlan {
                    scan: plan,
                    post_filter: post,
                    original_filter: Some(filter.clone()),
                };
            }

            QueryPlan {
                scan: ScanPlan::FullScan,
                post_filter: None,
                original_filter: Some(filter.clone()),
            }
        }

        // OR and NOT cannot efficiently use indexes — fall back to full scan
        _ => QueryPlan {
            scan: ScanPlan::FullScan,
            post_filter: None,
            original_filter: Some(filter.clone()),
        },
    }
}

/// Try to plan an IndexSorted scan using a compound index that covers
/// equality filter fields + the sort field.
fn try_index_sorted(
    index_manager: &IndexManager,
    collection: &str,
    filter: &FilterNode,
    sort: &[SortField],
) -> Option<QueryPlan> {
    if sort.is_empty() {
        return None;
    }
    let sort_field = &sort[0].field;

    // Extract equality conditions from the filter
    let eq_pairs = extract_eq_pairs(filter);
    if eq_pairs.is_empty() {
        return None;
    }

    let eq_field_names: Vec<&str> = eq_pairs.iter().map(|(f, _)| f.as_str()).collect();

    let (idx_def, _, n_matched) =
        index_manager.find_compound_index(collection, &eq_field_names, Some(sort_field))?;

    // Build prefix from matched eq values in index field order
    let mut prefix = Vec::new();
    for i in 0..n_matched {
        let idx_field = &idx_def.fields[i];
        let (_, val) = eq_pairs.iter().find(|(f, _)| f == idx_field)?;
        if i > 0 {
            prefix.push(0x01); // field separator
        }
        prefix.extend_from_slice(&value_to_sortable_bytes(val));
    }
    prefix.push(0x01); // separator before sort field values

    let reverse = !sort[0].ascending;

    // Build post-filter from conditions not covered by the compound index eq fields
    let covered: HashSet<&str> = idx_def.fields[..n_matched]
        .iter()
        .map(|s| s.as_str())
        .collect();
    let remaining = build_remaining_filter(filter, &covered);

    Some(QueryPlan {
        scan: ScanPlan::IndexSorted {
            index_name: idx_def.name,
            prefix,
            reverse,
        },
        post_filter: remaining,
        original_filter: Some(filter.clone()),
    })
}

/// Try to use a compound index for multi-field AND equality (Opt 3).
fn try_compound_eq(
    index_manager: &IndexManager,
    collection: &str,
    children: &[FilterNode],
    original: &FilterNode,
) -> Option<QueryPlan> {
    let eq_pairs: Vec<(String, Value)> = children
        .iter()
        .filter_map(|c| {
            if let FilterNode::Comparison {
                field,
                op: FilterOp::Eq,
                value,
            } = c
            {
                Some((field.clone(), value.clone()))
            } else {
                None
            }
        })
        .collect();

    if eq_pairs.len() < 2 {
        return None;
    }

    let eq_field_names: Vec<&str> = eq_pairs.iter().map(|(f, _)| f.as_str()).collect();
    let (idx_def, _, n_matched) =
        index_manager.find_compound_index(collection, &eq_field_names, None)?;

    // Build compound prefix
    let all_matched = n_matched == idx_def.fields.len();
    let mut prefix = Vec::new();
    for i in 0..n_matched {
        let idx_field = &idx_def.fields[i];
        let (_, val) = eq_pairs.iter().find(|(f, _)| f == idx_field)?;
        if i > 0 {
            prefix.push(0x01); // field separator
        }
        prefix.extend_from_slice(&value_to_sortable_bytes(val));
    }
    prefix.push(if all_matched { 0x00 } else { 0x01 });

    // Build remaining filter
    let covered: HashSet<&str> = idx_def.fields[..n_matched]
        .iter()
        .map(|s| s.as_str())
        .collect();
    let remaining: Vec<FilterNode> = children
        .iter()
        .filter(|c| {
            if let FilterNode::Comparison {
                field,
                op: FilterOp::Eq,
                ..
            } = c
            {
                !covered.contains(field.as_str())
            } else {
                true
            }
        })
        .cloned()
        .collect();

    let post_filter = match remaining.len() {
        0 => None,
        1 => Some(remaining.into_iter().next().unwrap()),
        _ => Some(FilterNode::And(remaining)),
    };

    Some(QueryPlan {
        scan: ScanPlan::CompoundEq {
            index_name: idx_def.name,
            prefix,
        },
        post_filter,
        original_filter: Some(original.clone()),
    })
}

/// Extract (field, value) pairs from equality conditions in a filter.
fn extract_eq_pairs(filter: &FilterNode) -> Vec<(String, Value)> {
    match filter {
        FilterNode::Comparison {
            field,
            op: FilterOp::Eq,
            value,
        } => {
            vec![(field.clone(), value.clone())]
        }
        FilterNode::And(children) => children
            .iter()
            .filter_map(|c| {
                if let FilterNode::Comparison {
                    field,
                    op: FilterOp::Eq,
                    value,
                } = c
                {
                    Some((field.clone(), value.clone()))
                } else {
                    None
                }
            })
            .collect(),
        _ => vec![],
    }
}

/// Build a post-filter from conditions not covered by the given set of fields.
fn build_remaining_filter(filter: &FilterNode, covered: &HashSet<&str>) -> Option<FilterNode> {
    match filter {
        FilterNode::Comparison {
            field,
            op: FilterOp::Eq,
            ..
        } if covered.contains(field.as_str()) => None,
        FilterNode::And(children) => {
            let remaining: Vec<FilterNode> = children
                .iter()
                .filter(|c| {
                    if let FilterNode::Comparison {
                        field,
                        op: FilterOp::Eq,
                        ..
                    } = c
                    {
                        !covered.contains(field.as_str())
                    } else {
                        true
                    }
                })
                .cloned()
                .collect();
            match remaining.len() {
                0 => None,
                1 => Some(remaining.into_iter().next().unwrap()),
                _ => Some(FilterNode::And(remaining)),
            }
        }
        other => Some(other.clone()),
    }
}

fn try_index_comparison(
    index_manager: &IndexManager,
    collection: &str,
    field: &str,
    op: &FilterOp,
    value: &Value,
) -> Option<ScanPlan> {
    let (def, _) = index_manager.get_index_for_field(collection, field)?;

    match op {
        FilterOp::Eq => Some(ScanPlan::IndexEq {
            index_name: def.name,
            field: field.to_string(),
            value: value.clone(),
        }),
        FilterOp::In => {
            if let Value::Array(values) = value {
                Some(ScanPlan::IndexIn {
                    index_name: def.name,
                    field: field.to_string(),
                    values: values.clone(),
                })
            } else {
                None
            }
        }
        FilterOp::Gt => Some(ScanPlan::IndexRange {
            index_name: def.name,
            field: field.to_string(),
            lower: Some((value.clone(), false)),
            upper: None,
        }),
        FilterOp::Gte => Some(ScanPlan::IndexRange {
            index_name: def.name,
            field: field.to_string(),
            lower: Some((value.clone(), true)),
            upper: None,
        }),
        FilterOp::Lt => Some(ScanPlan::IndexRange {
            index_name: def.name,
            field: field.to_string(),
            lower: None,
            upper: Some((value.clone(), false)),
        }),
        FilterOp::Lte => Some(ScanPlan::IndexRange {
            index_name: def.name,
            field: field.to_string(),
            lower: None,
            upper: Some((value.clone(), true)),
        }),
        _ => None,
    }
}

/// Try to combine $gte/$gt + $lte/$lt on the same indexed field into a single range scan.
fn try_range_from_and(
    index_manager: &IndexManager,
    collection: &str,
    children: &[FilterNode],
) -> Option<ScanPlan> {
    type Bound = Option<(Value, bool)>;
    let mut field_bounds: std::collections::HashMap<String, (Bound, Bound)> =
        std::collections::HashMap::new();

    for child in children {
        if let FilterNode::Comparison { field, op, value } = child {
            match op {
                FilterOp::Gt | FilterOp::Gte => {
                    let inclusive = matches!(op, FilterOp::Gte);
                    field_bounds.entry(field.clone()).or_insert((None, None)).0 =
                        Some((value.clone(), inclusive));
                }
                FilterOp::Lt | FilterOp::Lte => {
                    let inclusive = matches!(op, FilterOp::Lte);
                    field_bounds.entry(field.clone()).or_insert((None, None)).1 =
                        Some((value.clone(), inclusive));
                }
                _ => {}
            }
        }
    }

    // Find a field with both lower and upper bounds that has an index
    for (field, (lower, upper)) in &field_bounds {
        if lower.is_some()
            && upper.is_some()
            && let Some((def, _)) = index_manager.get_index_for_field(collection, field)
        {
            return Some(ScanPlan::IndexRange {
                index_name: def.name,
                field: field.clone(),
                lower: lower.clone(),
                upper: upper.clone(),
            });
        }
    }

    None
}
