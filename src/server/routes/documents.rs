use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Deserialize;
use serde_json::Value;

use crate::error::AppError;
use crate::server::AppState;
use crate::server::middleware::error_handler::JsonBody;
use crate::server::response::{ApiResponse, ApiResponseWithStatus};

pub async fn create(
    State(state): State<Arc<AppState>>,
    Path(collection): Path<String>,
    JsonBody(body): JsonBody<Value>,
) -> Result<impl IntoResponse, AppError> {
    let doc = state.storage.insert_document(&collection, body)?;
    state.metrics.record_insert();
    Ok(ApiResponseWithStatus {
        status: StatusCode::CREATED,
        response: ApiResponse::success(doc),
    })
}

#[derive(Deserialize)]
pub struct BulkInsertRequest {
    pub documents: Vec<Value>,
}

pub async fn bulk_create(
    State(state): State<Arc<AppState>>,
    Path(collection): Path<String>,
    JsonBody(body): JsonBody<BulkInsertRequest>,
) -> Result<impl IntoResponse, AppError> {
    let (inserted, errors) = state
        .storage
        .bulk_insert_documents(&collection, body.documents)?;
    state.metrics.record_bulk_insert(inserted);
    let data = serde_json::json!({
        "inserted": inserted,
        "errors": errors,
    });
    Ok(ApiResponseWithStatus {
        status: StatusCode::CREATED,
        response: ApiResponse::success(data),
    })
}

pub async fn get_by_id(
    State(state): State<Arc<AppState>>,
    Path((collection, id)): Path<(String, String)>,
) -> Result<Json<ApiResponse>, AppError> {
    let doc = state.storage.get_document(&collection, &id)?;
    Ok(Json(ApiResponse::success(doc)))
}

pub async fn replace(
    State(state): State<Arc<AppState>>,
    Path((collection, id)): Path<(String, String)>,
    JsonBody(body): JsonBody<Value>,
) -> Result<Json<ApiResponse>, AppError> {
    let doc = state.storage.replace_document(&collection, &id, body)?;
    Ok(Json(ApiResponse::success(doc)))
}

pub async fn partial_update(
    State(state): State<Arc<AppState>>,
    Path((collection, id)): Path<(String, String)>,
    JsonBody(body): JsonBody<Value>,
) -> Result<Json<ApiResponse>, AppError> {
    let doc = state
        .storage
        .partial_update_document(&collection, &id, body)?;
    Ok(Json(ApiResponse::success(doc)))
}

pub async fn delete(
    State(state): State<Arc<AppState>>,
    Path((collection, id)): Path<(String, String)>,
) -> Result<Json<ApiResponse>, AppError> {
    state.storage.delete_document(&collection, &id)?;
    state.metrics.record_delete();
    let data = serde_json::json!({ "deleted": true });
    Ok(Json(ApiResponse::success(data)))
}
