use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::{Path, State};
use serde::Deserialize;
use serde_json::Value;

use crate::error::AppError;
use crate::query::aggregate::{AggregateRequest, execute_aggregate};
use crate::query::executor::execute_query;
use crate::query::filter::parse_filter;
use crate::query::parser::{QueryRequest, parse_query};
use crate::server::AppState;
use crate::server::middleware::error_handler::JsonBody;
use crate::server::response::{ApiResponse, ResponseMeta};

/// Run a blocking closure with an optional timeout.
async fn with_query_timeout<F, R>(timeout_secs: u64, f: F) -> Result<R, AppError>
where
    F: FnOnce() -> Result<R, AppError> + Send + 'static,
    R: Send + 'static,
{
    if timeout_secs == 0 {
        return tokio::task::spawn_blocking(f)
            .await
            .map_err(|e| AppError::Internal(format!("Task join error: {e}")))?;
    }

    let timeout = Duration::from_secs(timeout_secs);
    match tokio::time::timeout(timeout, tokio::task::spawn_blocking(f)).await {
        Ok(Ok(result)) => result,
        Ok(Err(e)) => Err(AppError::Internal(format!("Task join error: {e}"))),
        Err(_) => Err(AppError::QueryTimeout(timeout_secs)),
    }
}

pub async fn search(
    State(state): State<Arc<AppState>>,
    Path(collection): Path<String>,
    JsonBody(body): JsonBody<QueryRequest>,
) -> Result<Json<ApiResponse>, AppError> {
    let start = Instant::now();
    let query = parse_query(body)?;
    let count_only = query.count_only;
    let timeout_secs = state.config.query_timeout;

    let st = state.clone();
    let coll = collection.clone();
    let result = with_query_timeout(timeout_secs, move || {
        execute_query(&st.storage, &coll, &query)
    })
    .await?;

    state.metrics.record_query();
    let duration_ms = (start.elapsed().as_secs_f64() * 1_000_000.0).round() / 1000.0;

    let returned_count = result.docs.len() as u64;

    if count_only {
        let count = result.total_count.unwrap_or(0);
        let data = serde_json::json!({ "count": count });
        Ok(Json(ApiResponse::success_with_meta(
            data,
            ResponseMeta {
                duration_ms: Some(duration_ms),
                total_count: Some(count),
                docs_scanned: Some(result.docs_scanned),
                index_used: result.index_used,
                scan_strategy: result.scan_strategy,
                ..Default::default()
            },
        )))
    } else {
        let data = serde_json::to_value(&result.docs)?;
        Ok(Json(ApiResponse::success_with_meta(
            data,
            ResponseMeta {
                duration_ms: Some(duration_ms),
                total_count: result.total_count,
                returned_count: Some(returned_count),
                docs_scanned: Some(result.docs_scanned),
                index_used: result.index_used,
                scan_strategy: result.scan_strategy,
                has_more: if result.has_more { Some(true) } else { None },
                ..Default::default()
            },
        )))
    }
}

pub async fn aggregate(
    State(state): State<Arc<AppState>>,
    Path(collection): Path<String>,
    JsonBody(body): JsonBody<AggregateRequest>,
) -> Result<Json<ApiResponse>, AppError> {
    let start = Instant::now();
    let timeout_secs = state.config.query_timeout;

    let st = state.clone();
    let coll = collection.clone();
    let result = with_query_timeout(timeout_secs, move || {
        execute_aggregate(&st.storage, &coll, &body)
    })
    .await?;

    state.metrics.record_query();
    let duration_ms = (start.elapsed().as_secs_f64() * 1_000_000.0).round() / 1000.0;

    let data = serde_json::to_value(&result.docs)?;
    Ok(Json(ApiResponse::success_with_meta(
        data,
        ResponseMeta {
            duration_ms: Some(duration_ms),
            docs_scanned: Some(result.docs_scanned),
            groups: Some(result.groups),
            index_used: result.index_used,
            scan_strategy: result.scan_strategy,
            ..Default::default()
        },
    )))
}

#[derive(Deserialize)]
pub struct DeleteByQueryRequest {
    pub filter: Value,
}

pub async fn delete_by_query(
    State(state): State<Arc<AppState>>,
    Path(collection): Path<String>,
    JsonBody(body): JsonBody<DeleteByQueryRequest>,
) -> Result<Json<ApiResponse>, AppError> {
    let start = Instant::now();
    let filter = parse_filter(&body.filter)?;
    let deleted = state.storage.delete_by_query(&collection, &filter)?;
    state
        .metrics
        .lifetime_deletes
        .fetch_add(deleted, std::sync::atomic::Ordering::Relaxed);
    let duration_ms = (start.elapsed().as_secs_f64() * 1_000_000.0).round() / 1000.0;

    let data = serde_json::json!({ "deleted": deleted });
    Ok(Json(ApiResponse::success_with_meta(
        data,
        ResponseMeta {
            duration_ms: Some(duration_ms),
            ..Default::default()
        },
    )))
}

#[derive(Deserialize)]
pub struct DistinctRequest {
    pub field: String,
    pub filter: Option<Value>,
    #[serde(default = "default_distinct_limit")]
    pub limit: usize,
}

fn default_distinct_limit() -> usize {
    1000
}

pub async fn distinct(
    State(state): State<Arc<AppState>>,
    Path(collection): Path<String>,
    JsonBody(body): JsonBody<DistinctRequest>,
) -> Result<Json<ApiResponse>, AppError> {
    let start = Instant::now();

    let filter = match &body.filter {
        Some(f) => Some(parse_filter(f)?),
        None => None,
    };

    let result = crate::query::distinct::execute_distinct(
        &state.storage,
        &collection,
        &body.field,
        filter.as_ref(),
        body.limit,
    )?;

    state.metrics.record_query();
    let duration_ms = (start.elapsed().as_secs_f64() * 1_000_000.0).round() / 1000.0;

    let data = serde_json::json!({
        "field": body.field,
        "values": result.values,
        "count": result.count,
        "truncated": result.truncated,
    });

    Ok(Json(ApiResponse::success_with_meta(
        data,
        ResponseMeta {
            duration_ms: Some(duration_ms),
            docs_scanned: Some(result.docs_scanned),
            index_used: result.index_used,
            ..Default::default()
        },
    )))
}

#[derive(Deserialize)]
pub struct UpdateByQueryRequest {
    pub filter: Value,
    pub update: Value,
}

pub async fn update_by_query(
    State(state): State<Arc<AppState>>,
    Path(collection): Path<String>,
    JsonBody(body): JsonBody<UpdateByQueryRequest>,
) -> Result<Json<ApiResponse>, AppError> {
    let start = Instant::now();
    let filter = parse_filter(&body.filter)?;
    let updated = state
        .storage
        .update_by_query(&collection, &filter, &body.update)?;
    let duration_ms = (start.elapsed().as_secs_f64() * 1_000_000.0).round() / 1000.0;

    let data = serde_json::json!({ "updated": updated });
    Ok(Json(ApiResponse::success_with_meta(
        data,
        ResponseMeta {
            duration_ms: Some(duration_ms),
            ..Default::default()
        },
    )))
}
