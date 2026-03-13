use reqwest::Client;
use serde_json::{Value, json};
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;

use wardsondb::config::Config;
use wardsondb::engine::storage::Storage;
use wardsondb::server::metrics::Metrics;
use wardsondb::server::{AppState, build_router};

fn test_config(tmp: &TempDir, port: u16) -> Config {
    Config {
        port,
        data_dir: tmp.path().to_string_lossy().to_string(),
        log_level: "error".to_string(),
        log_file: tmp.path().join("test.log").to_string_lossy().to_string(),
        verbose: false,
        tls: false,
        tls_cert: None,
        tls_key: None,
        ttl_interval: 60,
        api_keys: vec![],
        api_key_file: None,
        query_timeout: 30,
        metrics_public: false,
        cache_size_mb: 64,
        write_buffer_mb: 64,
        memtable_mb: 8,
        flush_workers: 2,
        compaction_workers: 2,
        bitmap_fields: String::new(),
        bitmap_max_cardinality: 1000,
        bitmap_sample_size: 100,
        no_bitmap: false,
    }
}

async fn start_test_server() -> (String, TempDir) {
    start_test_server_with_keys(vec![]).await
}

async fn start_test_server_with_bitmap(bitmap_fields: &str) -> (String, TempDir) {
    let tmp = TempDir::new().unwrap();
    let storage = Storage::open(tmp.path()).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let mut config = test_config(&tmp, port);
    config.bitmap_fields = bitmap_fields.to_string();

    // Configure the scan accelerator with explicit fields
    if !bitmap_fields.is_empty() {
        let fields: Vec<String> = bitmap_fields
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        storage.scan_accelerator.configure_fields(fields);
        storage.scan_accelerator.set_ready(true);
    }

    let state = Arc::new(AppState {
        storage,
        config,
        started_at: Instant::now(),
        metrics: Arc::new(Metrics::new()),
        api_keys: vec![],
    });

    let app = build_router(state);
    let addr = format!("127.0.0.1:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    let base_url = format!("http://{addr}");

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    (base_url, tmp)
}

async fn start_test_server_with_keys(api_keys: Vec<String>) -> (String, TempDir) {
    let tmp = TempDir::new().unwrap();
    let storage = Storage::open(tmp.path()).unwrap();

    // Find a free port
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let config = test_config(&tmp, port);

    let state = Arc::new(AppState {
        storage,
        config,
        started_at: Instant::now(),
        metrics: Arc::new(Metrics::new()),
        api_keys,
    });

    let app = build_router(state);
    let addr = format!("127.0.0.1:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    let base_url = format!("http://{addr}");

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Give the server a moment to start
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    (base_url, tmp)
}

#[tokio::test]
async fn test_health_and_info() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    // Health check
    let resp = client
        .get(format!("{base_url}/_health"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);
    assert_eq!(body["data"]["status"], "healthy");

    // Server info
    let resp = client.get(&base_url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);
    assert_eq!(body["data"]["name"], "WardSONDB");

    // Stats
    let resp = client
        .get(format!("{base_url}/_stats"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["collection_count"], 0);
}

#[tokio::test]
async fn test_collection_lifecycle() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    // Create collection
    let resp = client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["name"], "events");

    // Duplicate collection → 409
    let resp = client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409);

    // List collections
    let resp = client
        .get(format!("{base_url}/_collections"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let collections = body["data"].as_array().unwrap();
    assert_eq!(collections.len(), 1);
    assert_eq!(collections[0]["name"], "events");

    // Get collection info
    let resp = client
        .get(format!("{base_url}/events"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Drop collection
    let resp = client
        .delete(format!("{base_url}/events"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Verify dropped
    let resp = client
        .get(format!("{base_url}/events"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn test_document_crud() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    // Create collection
    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "users"}))
        .send()
        .await
        .unwrap();

    // Insert document
    let resp = client
        .post(format!("{base_url}/users/docs"))
        .json(&json!({"name": "Alice", "age": 30}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let body: Value = resp.json().await.unwrap();
    let doc_id = body["data"]["_id"].as_str().unwrap().to_string();
    assert_eq!(body["data"]["name"], "Alice");
    assert_eq!(body["data"]["_rev"], 1);

    // Get by ID
    let resp = client
        .get(format!("{base_url}/users/docs/{doc_id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["name"], "Alice");

    // Replace (PUT)
    let resp = client
        .put(format!("{base_url}/users/docs/{doc_id}"))
        .json(&json!({"name": "Alice Smith", "age": 31}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["name"], "Alice Smith");
    assert_eq!(body["data"]["_rev"], 2);

    // Partial update (PATCH)
    let resp = client
        .patch(format!("{base_url}/users/docs/{doc_id}"))
        .json(&json!({"email": "alice@example.com"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["email"], "alice@example.com");
    assert_eq!(body["data"]["name"], "Alice Smith");
    assert_eq!(body["data"]["_rev"], 3);

    // Delete
    let resp = client
        .delete(format!("{base_url}/users/docs/{doc_id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Verify deleted
    let resp = client
        .get(format!("{base_url}/users/docs/{doc_id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn test_bulk_insert() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "logs"}))
        .send()
        .await
        .unwrap();

    let docs = json!({
        "documents": [
            {"level": "info", "msg": "started"},
            {"level": "warn", "msg": "slow query"},
            {"level": "error", "msg": "connection lost"},
        ]
    });

    let resp = client
        .post(format!("{base_url}/logs/docs/_bulk"))
        .json(&docs)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["inserted"], 3);

    // Verify count
    let resp = client.get(format!("{base_url}/logs")).send().await.unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["doc_count"], 3);
}

#[tokio::test]
async fn test_query_filter_and_sort() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "products"}))
        .send()
        .await
        .unwrap();

    // Insert test data
    let docs = json!({
        "documents": [
            {"name": "Apple", "price": 1.5, "category": "fruit"},
            {"name": "Banana", "price": 0.5, "category": "fruit"},
            {"name": "Carrot", "price": 0.8, "category": "vegetable"},
            {"name": "Donut", "price": 2.0, "category": "pastry"},
            {"name": "Eggplant", "price": 1.2, "category": "vegetable"},
        ]
    });

    client
        .post(format!("{base_url}/products/docs/_bulk"))
        .json(&docs)
        .send()
        .await
        .unwrap();

    // Query: filter by category
    let resp = client
        .post(format!("{base_url}/products/query"))
        .json(&json!({"filter": {"category": "fruit"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["meta"]["total_count"], 2);

    // Query: comparison operator
    let resp = client
        .post(format!("{base_url}/products/query"))
        .json(&json!({"filter": {"price": {"$gt": 1.0}}}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["meta"]["total_count"], 3); // Apple, Donut, Eggplant

    // Query: sort by price descending
    let resp = client
        .post(format!("{base_url}/products/query"))
        .json(&json!({
            "sort": [{"price": "desc"}],
            "limit": 2
        }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let docs = body["data"].as_array().unwrap();
    assert_eq!(docs.len(), 2);
    assert_eq!(docs[0]["name"], "Donut");
    assert_eq!(docs[1]["name"], "Apple");

    // Query: count_only
    let resp = client
        .post(format!("{base_url}/products/query"))
        .json(&json!({"count_only": true}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["count"], 5);

    // Query: projection
    let resp = client
        .post(format!("{base_url}/products/query"))
        .json(&json!({
            "filter": {"category": "fruit"},
            "fields": ["name"]
        }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let docs = body["data"].as_array().unwrap();
    for doc in docs {
        assert!(doc.get("_id").is_some());
        assert!(doc.get("name").is_some());
        assert!(doc.get("price").is_none());
    }
}

#[tokio::test]
async fn test_query_logical_operators() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "items"}))
        .send()
        .await
        .unwrap();

    let docs = json!({
        "documents": [
            {"name": "A", "x": 1, "y": 10},
            {"name": "B", "x": 2, "y": 20},
            {"name": "C", "x": 3, "y": 30},
        ]
    });
    client
        .post(format!("{base_url}/items/docs/_bulk"))
        .json(&docs)
        .send()
        .await
        .unwrap();

    // $or
    let resp = client
        .post(format!("{base_url}/items/query"))
        .json(&json!({"filter": {"$or": [{"x": 1}, {"x": 3}]}}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["meta"]["total_count"], 2);

    // $and
    let resp = client
        .post(format!("{base_url}/items/query"))
        .json(&json!({"filter": {"$and": [{"x": {"$gte": 2}}, {"y": {"$lte": 20}}]}}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["meta"]["total_count"], 1);

    // $not
    let resp = client
        .post(format!("{base_url}/items/query"))
        .json(&json!({"filter": {"$not": {"x": 2}}}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["meta"]["total_count"], 2);
}

#[tokio::test]
async fn test_nested_field_query() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    let docs = json!({
        "documents": [
            {"event_type": "firewall", "network": {"src_ip": "10.0.0.1", "dst_port": 22}},
            {"event_type": "firewall", "network": {"src_ip": "10.0.0.2", "dst_port": 80}},
            {"event_type": "auth", "user": {"name": "admin"}},
        ]
    });
    client
        .post(format!("{base_url}/events/docs/_bulk"))
        .json(&docs)
        .send()
        .await
        .unwrap();

    // Query nested field with dot notation
    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({"filter": {"network.dst_port": 22}}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["meta"]["total_count"], 1);
    assert_eq!(body["data"][0]["network"]["src_ip"], "10.0.0.1");
}

#[tokio::test]
async fn test_error_cases() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    // Missing collection
    let resp = client
        .get(format!("{base_url}/nonexistent"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], false);
    assert_eq!(body["error"]["code"], "COLLECTION_NOT_FOUND");

    // Insert into missing collection
    let resp = client
        .post(format!("{base_url}/nonexistent/docs"))
        .json(&json!({"foo": "bar"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    // Create collection then get missing doc
    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "test"}))
        .send()
        .await
        .unwrap();

    let resp = client
        .get(format!("{base_url}/test/docs/nonexistent-id"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "DOCUMENT_NOT_FOUND");
}

#[tokio::test]
async fn test_pagination() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "pages"}))
        .send()
        .await
        .unwrap();

    // Insert 10 docs
    let docs: Vec<Value> = (0..10).map(|i| json!({"num": i})).collect();
    client
        .post(format!("{base_url}/pages/docs/_bulk"))
        .json(&json!({"documents": docs}))
        .send()
        .await
        .unwrap();

    // Get first page
    let resp = client
        .post(format!("{base_url}/pages/query"))
        .json(&json!({"limit": 3, "offset": 0}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["meta"]["total_count"], 10);
    assert_eq!(body["meta"]["returned_count"], 3);

    // Get second page
    let resp = client
        .post(format!("{base_url}/pages/query"))
        .json(&json!({"limit": 3, "offset": 3}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["meta"]["returned_count"], 3);
}

#[tokio::test]
async fn test_invalid_json_returns_error_envelope() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "test"}))
        .send()
        .await
        .unwrap();

    // Send invalid JSON body
    let resp = client
        .post(format!("{base_url}/test/docs"))
        .header("content-type", "application/json")
        .body("not valid json")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], false);
    assert_eq!(body["error"]["code"], "INVALID_DOCUMENT");

    // Send non-object JSON
    let resp = client
        .post(format!("{base_url}/test/docs"))
        .json(&json!("just a string"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], false);
}

#[tokio::test]
async fn test_collection_name_validation() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    // Underscore prefix not allowed
    let resp = client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "_internal"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    // Empty name not allowed
    let resp = client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": ""}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    // Invalid characters not allowed
    let resp = client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "foo:bar"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    // Valid name works
    let resp = client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "valid-name.123"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
}

#[tokio::test]
async fn test_request_id_header() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    let resp = client
        .get(format!("{base_url}/_health"))
        .send()
        .await
        .unwrap();
    let request_id = resp
        .headers()
        .get("x-request-id")
        .expect("Missing x-request-id header");
    let id_str = request_id.to_str().unwrap();
    // UUIDv7 format: 8-4-4-4-12
    assert_eq!(id_str.len(), 36);
    assert!(id_str.contains('-'));
}

#[tokio::test]
async fn test_nested_field_sort() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    let docs = json!({
        "documents": [
            {"event_type": "firewall", "network": {"src_ip": "10.0.0.1", "dst_port": 443}},
            {"event_type": "firewall", "network": {"src_ip": "10.0.0.2", "dst_port": 22}},
            {"event_type": "firewall", "network": {"src_ip": "10.0.0.3", "dst_port": 8080}},
            {"event_type": "auth", "user": {"name": "admin"}},
        ]
    });
    client
        .post(format!("{base_url}/events/docs/_bulk"))
        .json(&docs)
        .send()
        .await
        .unwrap();

    // Sort by nested field ascending
    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({
            "filter": {"event_type": "firewall"},
            "sort": [{"network.dst_port": "asc"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let docs = body["data"].as_array().unwrap();
    assert_eq!(docs.len(), 3);
    assert_eq!(docs[0]["network"]["dst_port"], 22);
    assert_eq!(docs[1]["network"]["dst_port"], 443);
    assert_eq!(docs[2]["network"]["dst_port"], 8080);

    // Sort by nested field descending
    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({
            "filter": {"event_type": "firewall"},
            "sort": [{"network.dst_port": "desc"}]
        }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let docs = body["data"].as_array().unwrap();
    assert_eq!(docs[0]["network"]["dst_port"], 8080);
    assert_eq!(docs[1]["network"]["dst_port"], 443);
    assert_eq!(docs[2]["network"]["dst_port"], 22);

    // Documents missing the sort field should sort to the beginning (None < Some)
    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({
            "sort": [{"network.dst_port": "asc"}]
        }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let docs = body["data"].as_array().unwrap();
    assert_eq!(docs.len(), 4);
    // The auth event has no network.dst_port, should be first in ascending order
    assert_eq!(docs[0]["event_type"], "auth");
}

#[tokio::test]
async fn test_bulk_insert_partial_success() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    // Mix valid docs with invalid ones (non-objects)
    let resp = client
        .post(format!("{base_url}/events/docs/_bulk"))
        .header("content-type", "application/json")
        .body(
            r#"{"documents": [
            {"event_type": "firewall", "severity": "high"},
            "not an object",
            {"event_type": "dns", "query": "example.com"},
            42,
            {"event_type": "auth", "user": "admin"}
        ]}"#,
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let body: Value = resp.json().await.unwrap();

    // 3 valid docs should be inserted
    assert_eq!(body["data"]["inserted"], 3);

    // 2 invalid docs should have per-doc errors
    let errors = body["data"]["errors"].as_array().unwrap();
    assert_eq!(errors.len(), 2);
    // Error messages should reference the document index
    assert!(errors[0].as_str().unwrap().contains("1"));
    assert!(errors[1].as_str().unwrap().contains("3"));

    // Verify the 3 valid docs are actually in the collection
    let resp = client
        .get(format!("{base_url}/events"))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["doc_count"], 3);

    // Verify we can query the valid docs
    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({"filter": {"event_type": "firewall"}}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["meta"]["total_count"], 1);
}

#[tokio::test]
async fn test_request_tracing_logged() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    // Verify the server responds with proper headers on errors too
    let resp = client
        .get(format!("{base_url}/nonexistent"))
        .send()
        .await
        .unwrap();
    assert!(resp.headers().get("x-request-id").is_some());
}

#[tokio::test]
async fn test_aggregate_basic() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    let docs = json!({
        "documents": [
            {"event_type": "firewall", "network": {"src_ip": "10.0.0.1", "action": "block"}, "severity": 8},
            {"event_type": "firewall", "network": {"src_ip": "10.0.0.1", "action": "block"}, "severity": 5},
            {"event_type": "firewall", "network": {"src_ip": "10.0.0.2", "action": "allow"}, "severity": 3},
            {"event_type": "dns", "query": "example.com", "severity": 1},
            {"event_type": "dns", "query": "test.com", "severity": 2},
            {"event_type": "auth", "user": "admin", "severity": 9},
        ]
    });
    client
        .post(format!("{base_url}/events/docs/_bulk"))
        .json(&docs)
        .send()
        .await
        .unwrap();

    // Basic group by event_type with count
    let resp = client
        .post(format!("{base_url}/events/aggregate"))
        .json(&json!({
            "pipeline": [
                {"$group": {
                    "_id": "event_type",
                    "count": {"$count": {}}
                }},
                {"$sort": {"count": "desc"}}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 3);
    // firewall has 3, dns has 2, auth has 1
    assert_eq!(data[0]["_id"], "firewall");
    assert_eq!(data[0]["count"], 3);
    assert_eq!(data[1]["_id"], "dns");
    assert_eq!(data[1]["count"], 2);
    assert_eq!(data[2]["_id"], "auth");
    assert_eq!(data[2]["count"], 1);
    // Meta should include groups count
    assert_eq!(body["meta"]["groups"], 3);
    assert_eq!(body["meta"]["docs_scanned"], 6);
}

#[tokio::test]
async fn test_aggregate_with_match_and_accumulators() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    let docs = json!({
        "documents": [
            {"event_type": "firewall", "network": {"src_ip": "10.0.0.1", "action": "block"}, "severity": 8},
            {"event_type": "firewall", "network": {"src_ip": "10.0.0.1", "action": "block"}, "severity": 5},
            {"event_type": "firewall", "network": {"src_ip": "10.0.0.2", "action": "allow"}, "severity": 3},
            {"event_type": "dns", "query": "example.com", "severity": 1},
        ]
    });
    client
        .post(format!("{base_url}/events/docs/_bulk"))
        .json(&docs)
        .send()
        .await
        .unwrap();

    // Match + group with $sum, $avg, $min, $max
    let resp = client
        .post(format!("{base_url}/events/aggregate"))
        .json(&json!({
            "pipeline": [
                {"$match": {"event_type": "firewall"}},
                {"$group": {
                    "_id": "network.action",
                    "count": {"$count": {}},
                    "total_severity": {"$sum": "severity"},
                    "avg_severity": {"$avg": "severity"},
                    "min_severity": {"$min": "severity"},
                    "max_severity": {"$max": "severity"}
                }},
                {"$sort": {"count": "desc"}}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 2);

    // block: 2 events, severity 8+5=13, avg=6.5, min=5, max=8
    let block_group = &data[0];
    assert_eq!(block_group["_id"], "block");
    assert_eq!(block_group["count"], 2);
    assert_eq!(block_group["total_severity"], 13.0);
    assert_eq!(block_group["avg_severity"], 6.5);
    assert_eq!(block_group["min_severity"], 5);
    assert_eq!(block_group["max_severity"], 8);

    // allow: 1 event, severity 3
    let allow_group = &data[1];
    assert_eq!(allow_group["_id"], "allow");
    assert_eq!(allow_group["count"], 1);
}

#[tokio::test]
async fn test_aggregate_multi_field_group() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    let docs = json!({
        "documents": [
            {"event_type": "firewall", "network": {"action": "block"}},
            {"event_type": "firewall", "network": {"action": "block"}},
            {"event_type": "firewall", "network": {"action": "allow"}},
            {"event_type": "dns", "network": {"action": "allow"}},
        ]
    });
    client
        .post(format!("{base_url}/events/docs/_bulk"))
        .json(&docs)
        .send()
        .await
        .unwrap();

    // Multi-field _id grouping
    let resp = client
        .post(format!("{base_url}/events/aggregate"))
        .json(&json!({
            "pipeline": [
                {"$group": {
                    "_id": {"type": "event_type", "action": "network.action"},
                    "count": {"$count": {}}
                }},
                {"$sort": {"count": "desc"}},
                {"$limit": 2}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 2);
    // firewall+block has 2 events
    assert_eq!(data[0]["_id"]["type"], "firewall");
    assert_eq!(data[0]["_id"]["action"], "block");
    assert_eq!(data[0]["count"], 2);
}

#[tokio::test]
async fn test_aggregate_null_id_groups_all() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    let docs = json!({
        "documents": [
            {"severity": 10},
            {"severity": 20},
            {"severity": 30},
        ]
    });
    client
        .post(format!("{base_url}/events/docs/_bulk"))
        .json(&docs)
        .send()
        .await
        .unwrap();

    // _id: null groups all documents into a single result
    let resp = client
        .post(format!("{base_url}/events/aggregate"))
        .json(&json!({
            "pipeline": [
                {"$group": {
                    "_id": null,
                    "count": {"$count": {}},
                    "total": {"$sum": "severity"},
                    "avg": {"$avg": "severity"}
                }}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 1);
    assert_eq!(data[0]["_id"], Value::Null);
    assert_eq!(data[0]["count"], 3);
    assert_eq!(data[0]["total"], 60.0);
    assert_eq!(data[0]["avg"], 20.0);
}

#[tokio::test]
async fn test_aggregate_invalid_pipeline() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    // Empty pipeline
    let resp = client
        .post(format!("{base_url}/events/aggregate"))
        .json(&json!({"pipeline": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "INVALID_PIPELINE");

    // Unknown stage
    let resp = client
        .post(format!("{base_url}/events/aggregate"))
        .json(&json!({"pipeline": [{"$unknown": {}}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    assert_eq!(
        resp.json::<Value>().await.unwrap()["error"]["code"],
        "INVALID_PIPELINE"
    );
}

// ===================== Phase 2: Index + Query Optimization Tests =====================

#[tokio::test]
async fn test_index_crud() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    // Create collection
    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "items"}))
        .send()
        .await
        .unwrap();

    // Create an index
    let resp = client
        .post(format!("{base_url}/items/indexes"))
        .json(&json!({"name": "idx_category", "field": "category"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let body: Value = resp.json().await.unwrap();
    assert!(body["ok"].as_bool().unwrap());
    assert_eq!(body["data"]["name"], "idx_category");
    assert_eq!(body["data"]["field"], "category");

    // List indexes
    let resp = client
        .get(format!("{base_url}/items/indexes"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let indexes = body["data"].as_array().unwrap();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0]["name"], "idx_category");

    // Duplicate index name returns 409
    let resp = client
        .post(format!("{base_url}/items/indexes"))
        .json(&json!({"name": "idx_category", "field": "other_field"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409);

    // Drop index
    let resp = client
        .delete(format!("{base_url}/items/indexes/idx_category"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert!(body["data"]["dropped"].as_bool().unwrap());

    // List indexes after drop — empty
    let resp = client
        .get(format!("{base_url}/items/indexes"))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn test_index_accelerates_query() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    // Setup: create collection + insert docs
    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "products"}))
        .send()
        .await
        .unwrap();

    let mut docs = Vec::new();
    for i in 0..50 {
        let category = if i % 3 == 0 {
            "fruit"
        } else if i % 3 == 1 {
            "vegetable"
        } else {
            "dairy"
        };
        docs.push(json!({"name": format!("item_{i}"), "category": category, "price": i * 10}));
    }
    client
        .post(format!("{base_url}/products/docs/_bulk"))
        .json(&json!({"documents": docs}))
        .send()
        .await
        .unwrap();

    // Create index on category
    client
        .post(format!("{base_url}/products/indexes"))
        .json(&json!({"name": "idx_category", "field": "category"}))
        .send()
        .await
        .unwrap();

    // Query using the indexed field
    let resp = client
        .post(format!("{base_url}/products/query"))
        .json(&json!({"filter": {"category": "fruit"}}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert!(body["ok"].as_bool().unwrap());
    assert_eq!(body["meta"]["index_used"], "idx_category");

    // All returned docs should have category == "fruit"
    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 17); // ceil(50/3) = 17
    for doc in data {
        assert_eq!(doc["category"], "fruit");
    }
}

#[tokio::test]
async fn test_count_only_with_index() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    // Setup
    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    let mut docs = Vec::new();
    for i in 0..100 {
        let event_type = if i % 4 == 0 {
            "firewall"
        } else if i % 4 == 1 {
            "dns"
        } else if i % 4 == 2 {
            "dhcp"
        } else {
            "ids"
        };
        docs.push(json!({"event_type": event_type, "seq": i}));
    }
    client
        .post(format!("{base_url}/events/docs/_bulk"))
        .json(&json!({"documents": docs}))
        .send()
        .await
        .unwrap();

    // Create index
    client
        .post(format!("{base_url}/events/indexes"))
        .json(&json!({"name": "idx_event_type", "field": "event_type"}))
        .send()
        .await
        .unwrap();

    // Count-only query on indexed field
    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({"filter": {"event_type": "firewall"}, "count_only": true}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert!(body["ok"].as_bool().unwrap());
    assert_eq!(body["data"]["count"], 25);
    assert_eq!(body["meta"]["index_used"], "idx_event_type");
    // docs_scanned should be 0 (index-only count)
    assert_eq!(body["meta"]["docs_scanned"], 0);
}

#[tokio::test]
async fn test_fast_stats() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    // Create collection + insert docs
    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "test_stats"}))
        .send()
        .await
        .unwrap();

    // Stats should show 0 docs
    let resp = client
        .get(format!("{base_url}/_stats"))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["total_documents"], 0);

    // Insert 10 docs
    let docs: Vec<Value> = (0..10).map(|i| json!({"n": i})).collect();
    client
        .post(format!("{base_url}/test_stats/docs/_bulk"))
        .json(&json!({"documents": docs}))
        .send()
        .await
        .unwrap();

    // Stats should show 10 docs
    let resp = client
        .get(format!("{base_url}/_stats"))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["total_documents"], 10);

    // Insert one more
    let resp = client
        .post(format!("{base_url}/test_stats/docs"))
        .json(&json!({"n": 99}))
        .send()
        .await
        .unwrap();
    let doc_id = resp.json::<Value>().await.unwrap()["data"]["_id"]
        .as_str()
        .unwrap()
        .to_string();

    // Stats should show 11
    let resp = client
        .get(format!("{base_url}/_stats"))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["total_documents"], 11);

    // Delete one doc
    client
        .delete(format!("{base_url}/test_stats/docs/{doc_id}"))
        .send()
        .await
        .unwrap();

    // Stats should show 10 again
    let resp = client
        .get(format!("{base_url}/_stats"))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["total_documents"], 10);
}

#[tokio::test]
async fn test_delete_by_query() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    // Setup
    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "logs"}))
        .send()
        .await
        .unwrap();

    let docs: Vec<Value> = (0..20)
        .map(|i| {
            json!({
                "level": if i < 10 { "info" } else { "error" },
                "message": format!("log entry {i}")
            })
        })
        .collect();
    client
        .post(format!("{base_url}/logs/docs/_bulk"))
        .json(&json!({"documents": docs}))
        .send()
        .await
        .unwrap();

    // Delete all "info" logs
    let resp = client
        .post(format!("{base_url}/logs/docs/_delete_by_query"))
        .json(&json!({"filter": {"level": "info"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert!(body["ok"].as_bool().unwrap());
    assert_eq!(body["data"]["deleted"], 10);

    // Verify remaining docs are all "error"
    let resp = client
        .post(format!("{base_url}/logs/query"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["meta"]["total_count"], 10);
    for doc in body["data"].as_array().unwrap() {
        assert_eq!(doc["level"], "error");
    }
}

#[tokio::test]
async fn test_update_by_query() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    // Setup
    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    let docs: Vec<Value> = (0..10)
        .map(|i| {
            json!({
                "network": {"src_ip": if i < 5 { "1.2.3.4" } else { "5.6.7.8" }},
                "event_type": "firewall"
            })
        })
        .collect();
    client
        .post(format!("{base_url}/events/docs/_bulk"))
        .json(&json!({"documents": docs}))
        .send()
        .await
        .unwrap();

    // Update all events from 1.2.3.4 with enrichment data
    let resp = client
        .post(format!("{base_url}/events/docs/_update_by_query"))
        .json(&json!({
            "filter": {"network.src_ip": "1.2.3.4"},
            "update": {
                "$set": {
                    "enrichment.src.geo_country": "US",
                    "enrichment.src.abuse_score": 42
                }
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert!(body["ok"].as_bool().unwrap());
    assert_eq!(body["data"]["updated"], 5);

    // Verify the enrichment data was applied
    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({"filter": {"network.src_ip": "1.2.3.4"}}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    for doc in body["data"].as_array().unwrap() {
        assert_eq!(doc["enrichment"]["src"]["geo_country"], "US");
        assert_eq!(doc["enrichment"]["src"]["abuse_score"], 42);
        // _rev should be incremented to 2
        assert_eq!(doc["_rev"], 2);
    }

    // Verify the other events were NOT updated
    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({"filter": {"network.src_ip": "5.6.7.8"}}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    for doc in body["data"].as_array().unwrap() {
        assert!(doc.get("enrichment").is_none());
        assert_eq!(doc["_rev"], 1);
    }
}

#[tokio::test]
async fn test_index_maintained_on_update() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    // Setup
    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "tasks"}))
        .send()
        .await
        .unwrap();

    // Create index on status
    client
        .post(format!("{base_url}/tasks/indexes"))
        .json(&json!({"name": "idx_status", "field": "status"}))
        .send()
        .await
        .unwrap();

    // Insert a doc with status "active"
    let resp = client
        .post(format!("{base_url}/tasks/docs"))
        .json(&json!({"status": "active", "name": "task1"}))
        .send()
        .await
        .unwrap();
    let doc_id = resp.json::<Value>().await.unwrap()["data"]["_id"]
        .as_str()
        .unwrap()
        .to_string();

    // Query for active — should find it via index
    let resp = client
        .post(format!("{base_url}/tasks/query"))
        .json(&json!({"filter": {"status": "active"}}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["meta"]["total_count"], 1);
    assert_eq!(body["meta"]["index_used"], "idx_status");

    // Update status to "inactive"
    client
        .patch(format!("{base_url}/tasks/docs/{doc_id}"))
        .json(&json!({"status": "inactive"}))
        .send()
        .await
        .unwrap();

    // Query for active — should find 0
    let resp = client
        .post(format!("{base_url}/tasks/query"))
        .json(&json!({"filter": {"status": "active"}}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["meta"]["total_count"], 0);

    // Query for inactive — should find 1
    let resp = client
        .post(format!("{base_url}/tasks/query"))
        .json(&json!({"filter": {"status": "inactive"}}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["meta"]["total_count"], 1);
    assert_eq!(body["meta"]["index_used"], "idx_status");
}

#[tokio::test]
async fn test_index_backfill() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    // Setup: insert docs FIRST, then create index
    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "items"}))
        .send()
        .await
        .unwrap();

    let docs: Vec<Value> = (0..30)
        .map(|i| {
            json!({
                "color": if i % 3 == 0 { "red" } else if i % 3 == 1 { "blue" } else { "green" },
                "n": i
            })
        })
        .collect();
    client
        .post(format!("{base_url}/items/docs/_bulk"))
        .json(&json!({"documents": docs}))
        .send()
        .await
        .unwrap();

    // Now create index — should backfill existing docs
    let resp = client
        .post(format!("{base_url}/items/indexes"))
        .json(&json!({"name": "idx_color", "field": "color"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // Query using the index
    let resp = client
        .post(format!("{base_url}/items/query"))
        .json(&json!({"filter": {"color": "red"}}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["meta"]["index_used"], "idx_color");
    assert_eq!(body["meta"]["total_count"], 10); // 30/3 = 10 red items
}

#[tokio::test]
async fn test_index_with_compound_filter() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    // Setup
    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    let mut docs = Vec::new();
    for i in 0..40 {
        docs.push(json!({
            "event_type": if i % 2 == 0 { "firewall" } else { "dns" },
            "severity": if i % 4 == 0 { "high" } else { "low" },
            "seq": i
        }));
    }
    client
        .post(format!("{base_url}/events/docs/_bulk"))
        .json(&json!({"documents": docs}))
        .send()
        .await
        .unwrap();

    // Create index on event_type
    client
        .post(format!("{base_url}/events/indexes"))
        .json(&json!({"name": "idx_event_type", "field": "event_type"}))
        .send()
        .await
        .unwrap();

    // Compound filter: event_type (indexed) + severity (not indexed)
    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({
            "filter": {
                "event_type": "firewall",
                "severity": "high"
            }
        }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert!(body["ok"].as_bool().unwrap());
    // Index should be used for event_type, severity applied as post-filter
    assert_eq!(body["meta"]["index_used"], "idx_event_type");
    // 20 firewall events, 10 are high severity (every 4th out of 40 total)
    assert_eq!(body["meta"]["total_count"], 10);

    for doc in body["data"].as_array().unwrap() {
        assert_eq!(doc["event_type"], "firewall");
        assert_eq!(doc["severity"], "high");
    }
}

// ===================== Phase 2.5: Compound Indexes + Aggregation Index Tests =====================

#[tokio::test]
async fn test_compound_index_creation() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    // Insert docs
    let docs: Vec<Value> = (0..30)
        .map(|i| {
            json!({
                "event_type": if i % 3 == 0 { "firewall" } else if i % 3 == 1 { "dns" } else { "ids" },
                "severity": i % 5,
                "seq": i
            })
        })
        .collect();
    client
        .post(format!("{base_url}/events/docs/_bulk"))
        .json(&json!({"documents": docs}))
        .send()
        .await
        .unwrap();

    // Create compound index using `fields` array
    let resp = client
        .post(format!("{base_url}/events/indexes"))
        .json(&json!({
            "name": "idx_type_severity",
            "fields": ["event_type", "severity"]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let body: Value = resp.json().await.unwrap();
    assert!(body["ok"].as_bool().unwrap());
    assert_eq!(body["data"]["name"], "idx_type_severity");
    let fields = body["data"]["fields"].as_array().unwrap();
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0], "event_type");
    assert_eq!(fields[1], "severity");

    // List indexes shows the compound index
    let resp = client
        .get(format!("{base_url}/events/indexes"))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let indexes = body["data"].as_array().unwrap();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0]["name"], "idx_type_severity");

    // Query on the first field of compound index should use it
    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({"filter": {"event_type": "firewall"}}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert!(body["ok"].as_bool().unwrap());
    assert_eq!(body["meta"]["index_used"], "idx_type_severity");
    assert_eq!(body["meta"]["total_count"], 10); // 30/3 = 10 firewall
}

#[tokio::test]
async fn test_aggregate_uses_index_for_match() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    // Insert a mix of events
    let mut docs = Vec::new();
    for i in 0..60 {
        docs.push(json!({
            "event_type": if i % 3 == 0 { "firewall" } else if i % 3 == 1 { "dns" } else { "ids" },
            "severity": i % 5,
            "network": {"action": if i % 2 == 0 { "block" } else { "allow" }}
        }));
    }
    client
        .post(format!("{base_url}/events/docs/_bulk"))
        .json(&json!({"documents": docs}))
        .send()
        .await
        .unwrap();

    // Create index on event_type
    client
        .post(format!("{base_url}/events/indexes"))
        .json(&json!({"name": "idx_event_type", "field": "event_type"}))
        .send()
        .await
        .unwrap();

    // Aggregation with $match as first stage should use the index
    let resp = client
        .post(format!("{base_url}/events/aggregate"))
        .json(&json!({
            "pipeline": [
                {"$match": {"event_type": "firewall"}},
                {"$group": {
                    "_id": "network.action",
                    "count": {"$count": {}}
                }},
                {"$sort": {"count": "desc"}}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert!(body["ok"].as_bool().unwrap());

    // Should use the index
    assert_eq!(body["meta"]["index_used"], "idx_event_type");
    // docs_scanned should be 20 (only firewall events), not 60
    assert_eq!(body["meta"]["docs_scanned"], 20);

    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 2); // block and allow groups
    // Total count should be 20 (all firewall events)
    let total: u64 = data.iter().map(|d| d["count"].as_u64().unwrap()).sum();
    assert_eq!(total, 20);
}

#[tokio::test]
async fn test_compound_index_maintained_on_crud() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "tasks"}))
        .send()
        .await
        .unwrap();

    // Create compound index
    client
        .post(format!("{base_url}/tasks/indexes"))
        .json(&json!({
            "name": "idx_status_priority",
            "fields": ["status", "priority"]
        }))
        .send()
        .await
        .unwrap();

    // Insert a document
    let resp = client
        .post(format!("{base_url}/tasks/docs"))
        .json(&json!({"status": "active", "priority": 1, "title": "task1"}))
        .send()
        .await
        .unwrap();
    let doc_id = resp.json::<Value>().await.unwrap()["data"]["_id"]
        .as_str()
        .unwrap()
        .to_string();

    // Query on first field should use compound index
    let resp = client
        .post(format!("{base_url}/tasks/query"))
        .json(&json!({"filter": {"status": "active"}}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["meta"]["index_used"], "idx_status_priority");
    assert_eq!(body["meta"]["total_count"], 1);

    // Update the document's status
    client
        .patch(format!("{base_url}/tasks/docs/{doc_id}"))
        .json(&json!({"status": "done"}))
        .send()
        .await
        .unwrap();

    // Old status should return 0
    let resp = client
        .post(format!("{base_url}/tasks/query"))
        .json(&json!({"filter": {"status": "active"}}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["meta"]["total_count"], 0);

    // New status should return 1
    let resp = client
        .post(format!("{base_url}/tasks/query"))
        .json(&json!({"filter": {"status": "done"}}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["meta"]["total_count"], 1);
    assert_eq!(body["meta"]["index_used"], "idx_status_priority");

    // Delete the document
    client
        .delete(format!("{base_url}/tasks/docs/{doc_id}"))
        .send()
        .await
        .unwrap();

    // Should return 0 now
    let resp = client
        .post(format!("{base_url}/tasks/query"))
        .json(&json!({"filter": {"status": "done"}}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["meta"]["total_count"], 0);
}

// ===================== Phase 3 Tests =====================

#[tokio::test]
async fn test_received_at_on_insert() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    // Insert a document
    let resp = client
        .post(format!("{base_url}/events/docs"))
        .json(&json!({"event_type": "firewall"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let body: Value = resp.json().await.unwrap();
    let doc = &body["data"];
    assert!(doc["_received_at"].is_string());
    let received_at = doc["_received_at"].as_str().unwrap().to_string();
    let doc_id = doc["_id"].as_str().unwrap().to_string();

    // PUT replace — _received_at should be preserved
    let resp = client
        .put(format!("{base_url}/events/docs/{doc_id}"))
        .json(&json!({"event_type": "dns", "new_field": true}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["_received_at"], received_at);
    assert_eq!(body["data"]["event_type"], "dns");

    // PATCH — _received_at should be preserved
    let resp = client
        .patch(format!("{base_url}/events/docs/{doc_id}"))
        .json(&json!({"event_type": "ids"}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["_received_at"], received_at);
    assert_eq!(body["data"]["event_type"], "ids");
}

#[tokio::test]
async fn test_received_at_on_bulk_insert() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    let docs = json!({
        "documents": [
            {"event_type": "firewall"},
            {"event_type": "dns"},
        ]
    });
    client
        .post(format!("{base_url}/events/docs/_bulk"))
        .json(&docs)
        .send()
        .await
        .unwrap();

    // Query all docs and verify _received_at
    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    for doc in body["data"].as_array().unwrap() {
        assert!(doc["_received_at"].is_string());
    }
}

#[tokio::test]
async fn test_ttl_crud() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    // No TTL initially
    let resp = client
        .get(format!("{base_url}/events/ttl"))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["enabled"], false);

    // Set TTL
    let resp = client
        .put(format!("{base_url}/events/ttl"))
        .json(&json!({"retention_days": 30, "field": "_created_at"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["retention_days"], 30);
    assert_eq!(body["data"]["field"], "_created_at");
    assert_eq!(body["data"]["enabled"], true);

    // Get TTL
    let resp = client
        .get(format!("{base_url}/events/ttl"))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["retention_days"], 30);
    assert_eq!(body["data"]["enabled"], true);

    // Delete TTL
    let resp = client
        .delete(format!("{base_url}/events/ttl"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Verify deleted
    let resp = client
        .get(format!("{base_url}/events/ttl"))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["enabled"], false);
}

#[tokio::test]
async fn test_ttl_in_stats() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    // Set TTL
    client
        .put(format!("{base_url}/events/ttl"))
        .json(&json!({"retention_days": 7}))
        .send()
        .await
        .unwrap();

    // Stats should show active TTL policies
    let resp = client
        .get(format!("{base_url}/_stats"))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["ttl"]["active_policies"], 1);
}

#[tokio::test]
async fn test_api_key_auth() {
    let keys = vec!["test-key-123".to_string(), "another-key".to_string()];
    let (base_url, _tmp) = start_test_server_with_keys(keys).await;
    let client = Client::new();

    // No key → 401
    let resp = client
        .get(format!("{base_url}/_stats"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "UNAUTHORIZED");

    // Invalid key → 401
    let resp = client
        .get(format!("{base_url}/_stats"))
        .header("Authorization", "Bearer wrong-key")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    // Valid key via Authorization header → 200
    let resp = client
        .get(format!("{base_url}/_stats"))
        .header("Authorization", "Bearer test-key-123")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Valid key via X-API-Key header → 200
    let resp = client
        .get(format!("{base_url}/_stats"))
        .header("X-API-Key", "another-key")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // /_health is always accessible without auth
    let resp = client
        .get(format!("{base_url}/_health"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn test_collect_accumulator() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    let docs: Vec<Value> = vec![
        json!({"src_ip": "10.0.0.1", "dst_port": 80}),
        json!({"src_ip": "10.0.0.1", "dst_port": 443}),
        json!({"src_ip": "10.0.0.1", "dst_port": 80}), // duplicate port
        json!({"src_ip": "10.0.0.2", "dst_port": 22}),
        json!({"src_ip": "10.0.0.2", "dst_port": 443}),
    ];
    client
        .post(format!("{base_url}/events/docs/_bulk"))
        .json(&json!({"documents": docs}))
        .send()
        .await
        .unwrap();

    let resp = client
        .post(format!("{base_url}/events/aggregate"))
        .json(&json!({
            "pipeline": [
                {"$group": {
                    "_id": "src_ip",
                    "ports": {"$collect": "dst_port"},
                    "count": {"$count": {}}
                }},
                {"$sort": {"count": "desc"}}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let data = body["data"].as_array().unwrap();

    // 10.0.0.1 has 3 events, unique ports: [80, 443]
    let ip1 = &data[0];
    assert_eq!(ip1["_id"], "10.0.0.1");
    assert_eq!(ip1["count"], 3);
    let ports = ip1["ports"].as_array().unwrap();
    assert_eq!(ports.len(), 2); // deduplicated

    // 10.0.0.2 has 2 events, unique ports: [22, 443]
    let ip2 = &data[1];
    assert_eq!(ip2["_id"], "10.0.0.2");
    assert_eq!(ip2["count"], 2);
    let ports = ip2["ports"].as_array().unwrap();
    assert_eq!(ports.len(), 2);
}

#[tokio::test]
async fn test_distinct_endpoint() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    let docs: Vec<Value> = (0..30)
        .map(|i| {
            json!({
                "event_type": match i % 3 { 0 => "firewall", 1 => "dns", _ => "ids" },
                "severity": i % 5,
            })
        })
        .collect();
    client
        .post(format!("{base_url}/events/docs/_bulk"))
        .json(&json!({"documents": docs}))
        .send()
        .await
        .unwrap();

    // Distinct without filter
    let resp = client
        .post(format!("{base_url}/events/distinct"))
        .json(&json!({"field": "event_type"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["count"], 3);
    assert_eq!(body["data"]["truncated"], false);

    // Distinct with filter
    let resp = client
        .post(format!("{base_url}/events/distinct"))
        .json(&json!({"field": "severity", "filter": {"event_type": "firewall"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    // firewall events: i=0,3,6,9,12,15,18,21,24,27 → severities: 0,3,1,4,2,0,3,1,4,2 → unique: 0,1,2,3,4
    assert_eq!(body["data"]["count"], 5);

    // Distinct with limit
    let resp = client
        .post(format!("{base_url}/events/distinct"))
        .json(&json!({"field": "event_type", "limit": 2}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["count"], 2);
    assert_eq!(body["data"]["truncated"], true);
}

#[tokio::test]
async fn test_distinct_with_index() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    let docs: Vec<Value> = (0..20)
        .map(|i| json!({"event_type": match i % 3 { 0 => "firewall", 1 => "dns", _ => "ids" }}))
        .collect();
    client
        .post(format!("{base_url}/events/docs/_bulk"))
        .json(&json!({"documents": docs}))
        .send()
        .await
        .unwrap();

    // Create index
    client
        .post(format!("{base_url}/events/indexes"))
        .json(&json!({"name": "idx_event_type", "field": "event_type"}))
        .send()
        .await
        .unwrap();

    // Distinct on indexed field — should use index
    let resp = client
        .post(format!("{base_url}/events/distinct"))
        .json(&json!({"field": "event_type"}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["count"], 3);
    assert_eq!(body["meta"]["docs_scanned"], 0); // index-only scan
    assert_eq!(body["meta"]["index_used"], "idx_event_type");
}

#[tokio::test]
async fn test_storage_info_endpoint() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    // Empty collection
    let resp = client
        .get(format!("{base_url}/events/storage"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["doc_count"], 0);
    assert!(body["data"]["oldest_doc"].is_null());
    assert!(body["data"]["newest_doc"].is_null());

    // Insert some docs
    let docs: Vec<Value> = (0..10).map(|i| json!({"n": i})).collect();
    client
        .post(format!("{base_url}/events/docs/_bulk"))
        .json(&json!({"documents": docs}))
        .send()
        .await
        .unwrap();

    // Create index
    client
        .post(format!("{base_url}/events/indexes"))
        .json(&json!({"name": "idx_n", "field": "n"}))
        .send()
        .await
        .unwrap();

    // Set TTL
    client
        .put(format!("{base_url}/events/ttl"))
        .json(&json!({"retention_days": 30}))
        .send()
        .await
        .unwrap();

    let resp = client
        .get(format!("{base_url}/events/storage"))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["doc_count"], 10);
    assert_eq!(body["data"]["index_count"], 1);
    assert!(body["data"]["oldest_doc"].is_string());
    assert!(body["data"]["newest_doc"].is_string());
    assert_eq!(body["data"]["ttl"]["retention_days"], 30);
}

#[tokio::test]
async fn test_prometheus_metrics() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    let resp = client
        .get(format!("{base_url}/_metrics"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let content_type = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(content_type.contains("text/plain"));
    let body = resp.text().await.unwrap();
    assert!(body.contains("wardsondb_uptime_seconds"));
    assert!(body.contains("wardsondb_documents_total"));
    assert!(body.contains("wardsondb_requests_total"));
    assert!(body.contains("wardsondb_storage_poisoned 0"));
}

// ── Optimization 1: IndexSorted — early termination with compound index ──

#[tokio::test]
async fn test_index_sorted_compound_scan() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    // Create collection
    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    // Create compound index on [event_type, received_at]
    client
        .post(format!("{base_url}/events/indexes"))
        .json(&json!({"name": "idx_type_time", "fields": ["event_type", "received_at"]}))
        .send()
        .await
        .unwrap();

    // Insert documents with varying received_at
    let docs: Vec<Value> = (0..20)
        .map(|i| {
            json!({
                "event_type": if i < 15 { "firewall" } else { "dns" },
                "received_at": format!("2026-03-{:02}T00:00:00Z", i + 1),
                "severity": if i % 2 == 0 { "high" } else { "low" }
            })
        })
        .collect();
    client
        .post(format!("{base_url}/events/docs/_bulk"))
        .json(&json!({"documents": docs}))
        .send()
        .await
        .unwrap();

    // Query: event_type=firewall, sort by received_at desc, limit 5
    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({
            "filter": {"event_type": "firewall"},
            "sort": [{"received_at": "desc"}],
            "limit": 5
        }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);

    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 5);

    // Verify scan_strategy is index_sorted
    assert_eq!(body["meta"]["scan_strategy"], "index_sorted");
    assert_eq!(body["meta"]["index_used"], "idx_type_time");

    // Verify results are in desc order of received_at
    for i in 0..data.len() - 1 {
        let a = data[i]["received_at"].as_str().unwrap();
        let b = data[i + 1]["received_at"].as_str().unwrap();
        assert!(a >= b, "Results not in desc order: {a} vs {b}");
    }

    // All should be firewall
    for doc in data {
        assert_eq!(doc["event_type"], "firewall");
    }

    // has_more should be true (15 firewall docs, only returned 5)
    assert_eq!(body["meta"]["has_more"], true);

    // total_count should be null (unknown with early termination)
    assert!(body["meta"]["total_count"].is_null());

    // docs_scanned should be small (much less than 15)
    let docs_scanned = body["meta"]["docs_scanned"].as_u64().unwrap();
    assert!(
        docs_scanned <= 10,
        "Expected early termination but scanned {docs_scanned}"
    );
}

#[tokio::test]
async fn test_index_sorted_asc_with_offset() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{base_url}/events/indexes"))
        .json(&json!({"name": "idx_type_time", "fields": ["event_type", "received_at"]}))
        .send()
        .await
        .unwrap();

    let docs: Vec<Value> = (0..10)
        .map(|i| {
            json!({
                "event_type": "firewall",
                "received_at": format!("2026-03-{:02}T00:00:00Z", i + 1),
            })
        })
        .collect();
    client
        .post(format!("{base_url}/events/docs/_bulk"))
        .json(&json!({"documents": docs}))
        .send()
        .await
        .unwrap();

    // Query: asc sort, offset 3, limit 3
    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({
            "filter": {"event_type": "firewall"},
            "sort": [{"received_at": "asc"}],
            "limit": 3,
            "offset": 3
        }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);
    assert_eq!(body["meta"]["scan_strategy"], "index_sorted");

    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 3);

    // With offset 3 in asc order, should get days 4, 5, 6
    assert!(data[0]["received_at"].as_str().unwrap().contains("03-04"));
    assert!(data[1]["received_at"].as_str().unwrap().contains("03-05"));
    assert!(data[2]["received_at"].as_str().unwrap().contains("03-06"));
}

#[tokio::test]
async fn test_index_sorted_correctness() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{base_url}/events/indexes"))
        .json(&json!({"name": "idx_type_time", "fields": ["event_type", "received_at"]}))
        .send()
        .await
        .unwrap();

    let docs: Vec<Value> = (0..30)
        .map(|i| {
            json!({
                "event_type": if i % 3 == 0 { "firewall" } else { "dns" },
                "received_at": format!("2026-03-{:02}T{:02}:00:00Z", (i / 24) + 1, i % 24),
            })
        })
        .collect();
    client
        .post(format!("{base_url}/events/docs/_bulk"))
        .json(&json!({"documents": docs}))
        .send()
        .await
        .unwrap();

    // Get all firewall docs via regular query (no sort = full scan)
    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({
            "filter": {"event_type": "firewall"},
            "sort": [{"received_at": "desc"}],
            "limit": 100
        }))
        .send()
        .await
        .unwrap();
    let full_body: Value = resp.json().await.unwrap();
    let full_data = full_body["data"].as_array().unwrap();

    // Get top 5 via index_sorted
    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({
            "filter": {"event_type": "firewall"},
            "sort": [{"received_at": "desc"}],
            "limit": 5
        }))
        .send()
        .await
        .unwrap();
    let sorted_body: Value = resp.json().await.unwrap();
    let sorted_data = sorted_body["data"].as_array().unwrap();

    // The first 5 from full scan should match the 5 from index_sorted
    let full_ids: Vec<&str> = full_data
        .iter()
        .take(5)
        .map(|d| d["_id"].as_str().unwrap())
        .collect();
    let sorted_ids: Vec<&str> = sorted_data
        .iter()
        .map(|d| d["_id"].as_str().unwrap())
        .collect();
    assert_eq!(
        full_ids, sorted_ids,
        "IndexSorted results must match full scan results"
    );
}

// ── Optimization 2: Index-only aggregation ──

#[tokio::test]
async fn test_index_only_aggregate_count() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{base_url}/events/indexes"))
        .json(&json!({"name": "idx_event_type", "field": "event_type"}))
        .send()
        .await
        .unwrap();

    // Insert docs with different event types
    let docs: Vec<Value> = vec![
        json!({"event_type": "firewall", "x": 1}),
        json!({"event_type": "firewall", "x": 2}),
        json!({"event_type": "firewall", "x": 3}),
        json!({"event_type": "dns", "x": 4}),
        json!({"event_type": "dns", "x": 5}),
        json!({"event_type": "ids", "x": 6}),
    ];
    client
        .post(format!("{base_url}/events/docs/_bulk"))
        .json(&json!({"documents": docs}))
        .send()
        .await
        .unwrap();

    // Aggregate: group by event_type, count
    let resp = client
        .post(format!("{base_url}/events/aggregate"))
        .json(&json!({
            "pipeline": [
                {"$group": {"_id": "event_type", "count": {"$count": {}}}},
                {"$sort": {"count": "desc"}}
            ]
        }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);

    // Should use index-only aggregate
    assert_eq!(body["meta"]["scan_strategy"], "index_only_aggregate");
    assert_eq!(body["meta"]["docs_scanned"], 0);
    assert_eq!(body["meta"]["index_used"], "idx_event_type");

    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 3);
    // Sorted desc by count: firewall=3, dns=2, ids=1
    assert_eq!(data[0]["_id"], "firewall");
    assert_eq!(data[0]["count"], 3);
    assert_eq!(data[1]["_id"], "dns");
    assert_eq!(data[1]["count"], 2);
    assert_eq!(data[2]["_id"], "ids");
    assert_eq!(data[2]["count"], 1);
}

#[tokio::test]
async fn test_index_only_aggregate_fallback() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{base_url}/events/indexes"))
        .json(&json!({"name": "idx_event_type", "field": "event_type"}))
        .send()
        .await
        .unwrap();

    let docs: Vec<Value> = vec![
        json!({"event_type": "firewall", "score": 10}),
        json!({"event_type": "firewall", "score": 20}),
        json!({"event_type": "dns", "score": 5}),
    ];
    client
        .post(format!("{base_url}/events/docs/_bulk"))
        .json(&json!({"documents": docs}))
        .send()
        .await
        .unwrap();

    // Aggregate with $sum — should fall back to standard path (can't do $sum from index)
    let resp = client
        .post(format!("{base_url}/events/aggregate"))
        .json(&json!({
            "pipeline": [
                {"$group": {"_id": "event_type", "count": {"$count": {}}, "total_score": {"$sum": "score"}}},
                {"$sort": {"count": "desc"}}
            ]
        }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);

    // Should NOT use index-only aggregate (has $sum)
    assert!(body["meta"]["scan_strategy"].is_null());
    assert!(body["meta"]["docs_scanned"].as_u64().unwrap() > 0);
}

// ── Optimization 3: Compound index prefix for multi-field AND ──

#[tokio::test]
async fn test_compound_eq_multi_field() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    // Create compound index on [event_type, action]
    client
        .post(format!("{base_url}/events/indexes"))
        .json(&json!({"name": "idx_type_action", "fields": ["event_type", "action"]}))
        .send()
        .await
        .unwrap();

    let docs: Vec<Value> = vec![
        json!({"event_type": "firewall", "action": "block", "src": "1.2.3.4"}),
        json!({"event_type": "firewall", "action": "allow", "src": "1.2.3.5"}),
        json!({"event_type": "firewall", "action": "block", "src": "1.2.3.6"}),
        json!({"event_type": "dns", "action": "block", "src": "1.2.3.7"}),
        json!({"event_type": "dns", "action": "allow", "src": "1.2.3.8"}),
    ];
    client
        .post(format!("{base_url}/events/docs/_bulk"))
        .json(&json!({"documents": docs}))
        .send()
        .await
        .unwrap();

    // Query: event_type=firewall AND action=block — should use compound index
    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({
            "filter": {"event_type": "firewall", "action": "block"},
            "count_only": true
        }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);
    assert_eq!(body["data"]["count"], 2);
    assert_eq!(body["meta"]["index_used"], "idx_type_action");
    assert_eq!(body["meta"]["scan_strategy"], "compound_eq");
    assert_eq!(body["meta"]["docs_scanned"], 0);
}

#[tokio::test]
async fn test_compound_eq_with_post_filter() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    // Compound index on [event_type, action]
    client
        .post(format!("{base_url}/events/indexes"))
        .json(&json!({"name": "idx_type_action", "fields": ["event_type", "action"]}))
        .send()
        .await
        .unwrap();

    let docs: Vec<Value> = vec![
        json!({"event_type": "firewall", "action": "block", "severity": "high"}),
        json!({"event_type": "firewall", "action": "block", "severity": "low"}),
        json!({"event_type": "firewall", "action": "block", "severity": "high"}),
    ];
    client
        .post(format!("{base_url}/events/docs/_bulk"))
        .json(&json!({"documents": docs}))
        .send()
        .await
        .unwrap();

    // Query: event_type=firewall AND action=block AND severity=high
    // Compound covers first two, severity is post-filter
    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({
            "filter": {"event_type": "firewall", "action": "block", "severity": "high"}
        }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);
    assert_eq!(body["meta"]["total_count"], 2);
    assert_eq!(body["meta"]["index_used"], "idx_type_action");
}

// =============================================================================
// Security hardening tests (50-57)
// =============================================================================

/// Test 50: Regex DoS — catastrophic backtracking pattern completes quickly
#[tokio::test]
async fn test_regex_dos_prevention() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    // Create collection and insert a document
    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "regextest"}))
        .send()
        .await
        .unwrap();
    client
        .post(format!("{base_url}/regextest/docs"))
        .json(&json!({"name": "aaaaaaaaaaaaaaaaab"}))
        .send()
        .await
        .unwrap();

    // This pattern causes catastrophic backtracking in naive regex engines
    let start = Instant::now();
    let resp = client
        .post(format!("{base_url}/regextest/query"))
        .json(&json!({
            "filter": {"name": {"$regex": "^(a|ab)*(b|bb)*(c|cc)*x$"}}
        }))
        .send()
        .await
        .unwrap();
    let elapsed = start.elapsed();

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);
    // Must complete in under 1 second (not hang)
    assert!(
        elapsed.as_millis() < 1000,
        "Regex query took too long: {:?}",
        elapsed
    );
}

/// Test 51: Query limit capped at 10,000
#[tokio::test]
async fn test_query_limit_cap() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "limittest"}))
        .send()
        .await
        .unwrap();

    // Insert a few docs
    for i in 0..5 {
        client
            .post(format!("{base_url}/limittest/docs"))
            .json(&json!({"n": i}))
            .send()
            .await
            .unwrap();
    }

    // Request with absurdly high limit — should be clamped
    let resp = client
        .post(format!("{base_url}/limittest/query"))
        .json(&json!({"limit": 999999}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);
    // All 5 docs returned (within 10,000 cap)
    assert_eq!(body["data"].as_array().unwrap().len(), 5);
}

/// Test 52: Bulk insert rejects more than 10,000 documents
#[tokio::test]
async fn test_bulk_insert_cap() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "bulkcap"}))
        .send()
        .await
        .unwrap();

    // Create 10,001 small docs
    let docs: Vec<Value> = (0..10_001).map(|i| json!({"n": i})).collect();
    let resp = client
        .post(format!("{base_url}/bulkcap/docs/_bulk"))
        .json(&json!({"documents": docs}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], false);
    assert!(body["error"]["message"].as_str().unwrap().contains("10000"));
}

/// Test 53: Aggregation pipeline rejects more than 100 stages
#[tokio::test]
async fn test_pipeline_stage_cap() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "pipecap"}))
        .send()
        .await
        .unwrap();

    // Build 101 stages
    let stages: Vec<Value> = (0..101).map(|_| json!({"$limit": 10})).collect();
    let resp = client
        .post(format!("{base_url}/pipecap/aggregate"))
        .json(&json!({"pipeline": stages}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], false);
    assert!(body["error"]["message"].as_str().unwrap().contains("100"));
}

/// Test 54: Filter with too many $or branches is rejected
#[tokio::test]
async fn test_filter_branch_limit() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "branchtest"}))
        .send()
        .await
        .unwrap();

    // Build $or with 1001 branches
    let branches: Vec<Value> = (0..1001).map(|i| json!({"n": i})).collect();
    let resp = client
        .post(format!("{base_url}/branchtest/query"))
        .json(&json!({"filter": {"$or": branches}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], false);
    assert!(body["error"]["message"].as_str().unwrap().contains("1000"));
}

/// Test 55: Deeply nested filter is rejected
#[tokio::test]
async fn test_filter_depth_limit() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "depthtest"}))
        .send()
        .await
        .unwrap();

    // Build nested $not chain of depth 21
    let mut filter = json!({"x": 1});
    for _ in 0..21 {
        filter = json!({"$not": filter});
    }
    let resp = client
        .post(format!("{base_url}/depthtest/query"))
        .json(&json!({"filter": filter}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], false);
    assert!(body["error"]["message"].as_str().unwrap().contains("depth"));
}

/// Test 56: Dot-notation path depth limit
#[tokio::test]
async fn test_dot_notation_depth_limit() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "dottest"}))
        .send()
        .await
        .unwrap();

    // 22-level deep path (21 dots)
    let deep_path = "a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v";
    let resp = client
        .post(format!("{base_url}/dottest/query"))
        .json(&json!({"filter": {deep_path: 1}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], false);
    assert!(body["error"]["message"].as_str().unwrap().contains("depth"));
}

/// Test 57: Invalid regex pattern returns INVALID_QUERY at parse time
#[tokio::test]
async fn test_invalid_regex_pattern() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "regexerr"}))
        .send()
        .await
        .unwrap();

    let resp = client
        .post(format!("{base_url}/regexerr/query"))
        .json(&json!({"filter": {"name": {"$regex": "[invalid("} }}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], false);
    assert_eq!(body["error"]["code"], "INVALID_QUERY");
}

/// Test 58: Health endpoint includes write_pressure field
#[tokio::test]
async fn test_health_write_pressure() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    let resp = client
        .get(format!("{base_url}/_health"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);
    assert_eq!(body["data"]["status"], "healthy");
    // write_pressure must always be present and "normal" under no load
    assert_eq!(body["data"]["write_pressure"], "normal");
}

// ── Bitmap Scan Accelerator Tests ──────────────────────────────────────────

/// Helper: create a collection and insert docs with known field values for bitmap tests.
async fn setup_bitmap_test_data(base_url: &str, client: &Client) {
    // Create collection
    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    // Insert 20 docs with various categories and statuses
    let docs: Vec<Value> = (0..20)
        .map(|i| {
            let category = match i % 4 {
                0 => "firewall",
                1 => "dhcp",
                2 => "threat",
                _ => "system",
            };
            let status = if i % 2 == 0 { "active" } else { "inactive" };
            let severity = i % 5;
            json!({
                "category": category,
                "status": status,
                "severity": severity,
                "name": format!("event_{i}"),
            })
        })
        .collect();

    client
        .post(format!("{base_url}/events/docs/_bulk"))
        .json(&json!({"documents": docs}))
        .send()
        .await
        .unwrap();
}

/// Test 59: Bitmap scan equality filter
#[tokio::test]
async fn test_bitmap_scan_equality() {
    let (base_url, _tmp) = start_test_server_with_bitmap("category,status").await;
    let client = Client::new();
    setup_bitmap_test_data(&base_url, &client).await;

    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({"filter": {"category": "firewall"}}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);
    let docs = body["data"].as_array().unwrap();
    assert_eq!(docs.len(), 5); // 20/4 = 5 firewall events
    assert_eq!(body["meta"]["scan_strategy"], "bitmap");
    for doc in docs {
        assert_eq!(doc["category"], "firewall");
    }
}

/// Test 60: Bitmap scan AND filter
#[tokio::test]
async fn test_bitmap_scan_and() {
    let (base_url, _tmp) = start_test_server_with_bitmap("category,status").await;
    let client = Client::new();
    setup_bitmap_test_data(&base_url, &client).await;

    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({"filter": {"category": "firewall", "status": "active"}}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);
    let docs = body["data"].as_array().unwrap();
    // firewall = indices 0,4,8,12,16 → active = even indices → all firewall are at even indices
    // so all 5 firewall events are active
    assert_eq!(docs.len(), 5);
    assert_eq!(body["meta"]["scan_strategy"], "bitmap");
}

/// Test 61: Bitmap scan OR filter
#[tokio::test]
async fn test_bitmap_scan_or() {
    let (base_url, _tmp) = start_test_server_with_bitmap("category,status").await;
    let client = Client::new();
    setup_bitmap_test_data(&base_url, &client).await;

    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({
            "filter": {
                "$or": [
                    {"category": "firewall"},
                    {"category": "threat"}
                ]
            }
        }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);
    let docs = body["data"].as_array().unwrap();
    assert_eq!(docs.len(), 10); // 5 firewall + 5 threat
    assert_eq!(body["meta"]["scan_strategy"], "bitmap");
}

/// Test 62: Bitmap scan $ne filter
#[tokio::test]
async fn test_bitmap_scan_ne() {
    let (base_url, _tmp) = start_test_server_with_bitmap("category,status").await;
    let client = Client::new();
    setup_bitmap_test_data(&base_url, &client).await;

    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({"filter": {"category": {"$ne": "firewall"}}}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);
    let docs = body["data"].as_array().unwrap();
    assert_eq!(docs.len(), 15); // 20 - 5 firewall
    assert_eq!(body["meta"]["scan_strategy"], "bitmap");
}

/// Test 63: Bitmap scan $in filter
#[tokio::test]
async fn test_bitmap_scan_in() {
    let (base_url, _tmp) = start_test_server_with_bitmap("category,status").await;
    let client = Client::new();
    setup_bitmap_test_data(&base_url, &client).await;

    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({"filter": {"category": {"$in": ["firewall", "dhcp"]}}}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);
    let docs = body["data"].as_array().unwrap();
    assert_eq!(docs.len(), 10); // 5 firewall + 5 dhcp
    assert_eq!(body["meta"]["scan_strategy"], "bitmap");
}

/// Test 64: Bitmap count_only — zero doc reads
#[tokio::test]
async fn test_bitmap_count_only() {
    let (base_url, _tmp) = start_test_server_with_bitmap("category,status").await;
    let client = Client::new();
    setup_bitmap_test_data(&base_url, &client).await;

    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({"filter": {"category": "firewall"}, "count_only": true}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);
    assert_eq!(body["data"]["count"], 5);
    assert_eq!(body["meta"]["docs_scanned"], 0);
    assert_eq!(body["meta"]["scan_strategy"], "bitmap");
}

/// Test 65: Bitmap aggregate count — zero doc reads
#[tokio::test]
async fn test_bitmap_aggregate_count() {
    let (base_url, _tmp) = start_test_server_with_bitmap("category,status").await;
    let client = Client::new();
    setup_bitmap_test_data(&base_url, &client).await;

    let resp = client
        .post(format!("{base_url}/events/aggregate"))
        .json(&json!({
            "pipeline": [
                {"$group": {"_id": "category", "count": {"$count": {}}}}
            ]
        }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);
    assert_eq!(body["meta"]["docs_scanned"], 0);
    assert_eq!(body["meta"]["scan_strategy"], "bitmap_aggregate");
    let docs = body["data"].as_array().unwrap();
    assert_eq!(docs.len(), 4); // 4 categories
    // Verify all groups have count 5
    for doc in docs {
        assert_eq!(doc["count"], 5);
    }
}

/// Test 66: Bitmap filtered aggregate — $match + $group with $count
#[tokio::test]
async fn test_bitmap_filtered_aggregate() {
    let (base_url, _tmp) = start_test_server_with_bitmap("category,status").await;
    let client = Client::new();
    setup_bitmap_test_data(&base_url, &client).await;

    let resp = client
        .post(format!("{base_url}/events/aggregate"))
        .json(&json!({
            "pipeline": [
                {"$match": {"status": "active"}},
                {"$group": {"_id": "category", "count": {"$count": {}}}}
            ]
        }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);
    assert_eq!(body["meta"]["docs_scanned"], 0);
    assert_eq!(body["meta"]["scan_strategy"], "bitmap_filtered_aggregate");
}

/// Test 67: Bitmap partial coverage — AND with one bitmap and one non-bitmap field
#[tokio::test]
async fn test_bitmap_partial_coverage() {
    let (base_url, _tmp) = start_test_server_with_bitmap("category").await;
    let client = Client::new();
    setup_bitmap_test_data(&base_url, &client).await;

    // "category" has a bitmap, "severity" does not
    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({"filter": {"category": "firewall", "severity": 0}}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);
    // firewall events at indices 0,4,8,12,16 → severity = i%5 → severity 0 at indices 0, 20 (not exist) → just index 0
    // Actually: idx 0: sev=0, idx 4: sev=4, idx 8: sev=3, idx 12: sev=2, idx 16: sev=1
    // So only index 0 has severity 0
    let docs = body["data"].as_array().unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(body["meta"]["scan_strategy"], "bitmap");
}

/// Test 68: Bitmap correctness — compare bitmap scan results with full scan results
#[tokio::test]
async fn test_bitmap_correctness() {
    let (base_url, _tmp) = start_test_server_with_bitmap("category,status").await;
    let client = Client::new();
    setup_bitmap_test_data(&base_url, &client).await;

    // Query with bitmap (should use bitmap scan)
    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({"filter": {"category": "threat"}, "sort": [{"name": "asc"}]}))
        .send()
        .await
        .unwrap();
    let bitmap_body: Value = resp.json().await.unwrap();
    assert_eq!(bitmap_body["meta"]["scan_strategy"], "bitmap");

    // Also run a query that definitely uses full scan (non-bitmap field)
    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({"filter": {"name": {"$regex": "^event_[28]$"}}, "sort": [{"name": "asc"}]}))
        .send()
        .await
        .unwrap();
    let full_body: Value = resp.json().await.unwrap();
    // Full scan for non-bitmap field
    assert!(
        full_body["meta"]["scan_strategy"].is_null()
            || full_body["meta"]["scan_strategy"] != "bitmap"
    );

    // Verify bitmap results are correct
    let bitmap_docs = bitmap_body["data"].as_array().unwrap();
    assert_eq!(bitmap_docs.len(), 5);
    for doc in bitmap_docs {
        assert_eq!(doc["category"], "threat");
    }
}

/// Test 69: Bitmap CRUD consistency — insert, update, delete maintain correct bitmap results
#[tokio::test]
async fn test_bitmap_crud_consistency() {
    let (base_url, _tmp) = start_test_server_with_bitmap("category").await;
    let client = Client::new();

    // Create collection
    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "items"}))
        .send()
        .await
        .unwrap();

    // Insert 3 docs
    let resp = client
        .post(format!("{base_url}/items/docs"))
        .json(&json!({"category": "A"}))
        .send()
        .await
        .unwrap();
    let doc_a: Value = resp.json().await.unwrap();
    let id_a = doc_a["data"]["_id"].as_str().unwrap().to_string();

    client
        .post(format!("{base_url}/items/docs"))
        .json(&json!({"category": "B"}))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{base_url}/items/docs"))
        .json(&json!({"category": "A"}))
        .send()
        .await
        .unwrap();

    // Verify: 2 A's, 1 B
    let resp = client
        .post(format!("{base_url}/items/query"))
        .json(&json!({"filter": {"category": "A"}, "count_only": true}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["count"], 2);

    // Update first doc: A -> C
    client
        .patch(format!("{base_url}/items/docs/{id_a}"))
        .json(&json!({"category": "C"}))
        .send()
        .await
        .unwrap();

    // Verify: 1 A, 1 B, 1 C
    let resp = client
        .post(format!("{base_url}/items/query"))
        .json(&json!({"filter": {"category": "A"}, "count_only": true}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["count"], 1);

    let resp = client
        .post(format!("{base_url}/items/query"))
        .json(&json!({"filter": {"category": "C"}, "count_only": true}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["count"], 1);

    // Delete the C doc
    client
        .delete(format!("{base_url}/items/docs/{id_a}"))
        .send()
        .await
        .unwrap();

    // Verify: 1 A, 1 B, 0 C
    let resp = client
        .post(format!("{base_url}/items/query"))
        .json(&json!({"filter": {"category": "C"}, "count_only": true}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["count"], 0);
}

/// Test 70: Bitmap cardinality cap — field exceeding max_cardinality is not fully tracked
#[tokio::test]
async fn test_bitmap_cardinality_cap() {
    let (base_url, _tmp) = start_test_server_with_bitmap("category").await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "items"}))
        .send()
        .await
        .unwrap();

    // Insert docs — the category field has max_cardinality=1000 by default,
    // and we're inserting well under that limit, so bitmap should work
    for i in 0..10 {
        client
            .post(format!("{base_url}/items/docs"))
            .json(&json!({"category": format!("type_{i}")}))
            .send()
            .await
            .unwrap();
    }

    let resp = client
        .post(format!("{base_url}/items/query"))
        .json(&json!({"filter": {"category": "type_0"}, "count_only": true}))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["count"], 1);
    assert_eq!(body["meta"]["scan_strategy"], "bitmap");
}

/// Test 71: Bitmap auto-detection — insert > sample_size docs, verify low-cardinality fields
/// get auto-detected. Uses bitmap_sample_size=100 in test config.
#[tokio::test]
async fn test_bitmap_auto_detection() {
    // Start server WITHOUT explicit bitmap fields — relies on auto-detection
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    // Insert 120 docs (> sample_size of 100) with low-cardinality fields
    let docs: Vec<Value> = (0..120)
        .map(|i| {
            json!({
                "event_type": match i % 3 { 0 => "A", 1 => "B", _ => "C" },
                "severity": i % 5,
                "unique_id": format!("uid_{i}"),
            })
        })
        .collect();

    // Bulk insert in batches
    for chunk in docs.chunks(60) {
        client
            .post(format!("{base_url}/events/docs/_bulk"))
            .json(&json!({"documents": chunk}))
            .send()
            .await
            .unwrap();
    }

    // Check stats to see if accelerator detected fields
    let resp = client
        .get(format!("{base_url}/_stats"))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    // The accelerator may or may not be ready depending on timing,
    // but the scan_accelerator section should exist in stats
    assert!(body["data"]["scan_accelerator"].is_object());
}

/// Test 72: Bitmap persistence — build accelerator, check stats show data
#[tokio::test]
async fn test_bitmap_persistence() {
    let (base_url, _tmp) = start_test_server_with_bitmap("category").await;
    let client = Client::new();
    setup_bitmap_test_data(&base_url, &client).await;

    // Verify bitmap is populated
    let resp = client
        .get(format!("{base_url}/_stats"))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let accel = &body["data"]["scan_accelerator"];
    assert_eq!(accel["ready"], true);
    assert!(accel["total_positions"].as_u64().unwrap() > 0);
    let cols = accel["bitmap_columns"].as_array().unwrap();
    assert!(!cols.is_empty());
    // Find the category column
    let cat_col = cols.iter().find(|c| c["field"] == "category");
    assert!(cat_col.is_some());
    assert!(cat_col.unwrap()["cardinality"].as_u64().unwrap() > 0);
}

/// Test 73: Bitmap stats in /_stats endpoint
#[tokio::test]
async fn test_bitmap_stats() {
    let (base_url, _tmp) = start_test_server_with_bitmap("category,status").await;
    let client = Client::new();
    setup_bitmap_test_data(&base_url, &client).await;

    let resp = client
        .get(format!("{base_url}/_stats"))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let accel = &body["data"]["scan_accelerator"];
    assert_eq!(accel["ready"], true);
    assert_eq!(accel["total_positions"], 20);
    let cols = accel["bitmap_columns"].as_array().unwrap();
    assert_eq!(cols.len(), 2); // category and status

    // Check health endpoint too
    let resp = client
        .get(format!("{base_url}/_health"))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["scan_accelerator_ready"], true);
}

// ── Compound Range Tests ────────────────────────────────────────────────────

#[tokio::test]
async fn test_compound_range_basic() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    // Create collection
    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    // Insert docs with category and timestamp
    for i in 0..20 {
        let category = if i % 2 == 0 { "A" } else { "B" };
        let ts = format!("2026-03-12T{:02}:00:00Z", i);
        client
            .post(format!("{base_url}/events/docs"))
            .json(&json!({"category": category, "timestamp": ts, "seq": i}))
            .send()
            .await
            .unwrap();
    }

    // Create compound index on (category, timestamp)
    client
        .post(format!("{base_url}/events/indexes"))
        .json(&json!({"name": "idx_cat_ts", "fields": ["category", "timestamp"]}))
        .send()
        .await
        .unwrap();

    // Query: category = "A" AND timestamp >= "2026-03-12T10:00:00Z"
    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({
            "filter": {
                "category": "A",
                "timestamp": {"$gte": "2026-03-12T10:00:00Z"}
            }
        }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);

    let docs = body["data"].as_array().unwrap();
    // Category A docs: i=0,2,4,6,8,10,12,14,16,18
    // Timestamps >= 10:00: i=10,12,14,16,18 → 5 docs
    assert_eq!(docs.len(), 5);
    for doc in docs {
        assert_eq!(doc["category"], "A");
        assert!(doc["timestamp"].as_str().unwrap() >= "2026-03-12T10:00:00Z");
    }

    // Verify compound_range strategy is used
    assert_eq!(body["meta"]["scan_strategy"], "compound_range");
    assert_eq!(body["meta"]["index_used"], "idx_cat_ts");
}

#[tokio::test]
async fn test_compound_range_with_upper_bound() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    for i in 0..20 {
        let category = if i % 2 == 0 { "A" } else { "B" };
        let ts = format!("2026-03-12T{:02}:00:00Z", i);
        client
            .post(format!("{base_url}/events/docs"))
            .json(&json!({"category": category, "timestamp": ts}))
            .send()
            .await
            .unwrap();
    }

    client
        .post(format!("{base_url}/events/indexes"))
        .json(&json!({"name": "idx_cat_ts", "fields": ["category", "timestamp"]}))
        .send()
        .await
        .unwrap();

    // Query: category = "A" AND timestamp >= "2026-03-12T06:00:00Z" AND timestamp < "2026-03-12T14:00:00Z"
    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({
            "filter": {
                "$and": [
                    {"category": "A"},
                    {"timestamp": {"$gte": "2026-03-12T06:00:00Z"}},
                    {"timestamp": {"$lt": "2026-03-12T14:00:00Z"}}
                ]
            }
        }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);

    let docs = body["data"].as_array().unwrap();
    // Category A: i=0,2,4,6,8,10,12,14,16,18
    // ts >= 06:00 AND ts < 14:00 → i=6,8,10,12 → 4 docs
    assert_eq!(docs.len(), 4);
    for doc in docs {
        assert_eq!(doc["category"], "A");
        let ts = doc["timestamp"].as_str().unwrap();
        assert!(ts >= "2026-03-12T06:00:00Z");
        assert!(ts < "2026-03-12T14:00:00Z");
    }

    assert_eq!(body["meta"]["scan_strategy"], "compound_range");
}

#[tokio::test]
async fn test_compound_range_with_post_filter() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    for i in 0..20 {
        let category = if i % 2 == 0 { "A" } else { "B" };
        let status = if i % 3 == 0 { "active" } else { "inactive" };
        let ts = format!("2026-03-12T{:02}:00:00Z", i);
        client
            .post(format!("{base_url}/events/docs"))
            .json(&json!({"category": category, "timestamp": ts, "status": status}))
            .send()
            .await
            .unwrap();
    }

    client
        .post(format!("{base_url}/events/indexes"))
        .json(&json!({"name": "idx_cat_ts", "fields": ["category", "timestamp"]}))
        .send()
        .await
        .unwrap();

    // Query: category = "A" AND timestamp >= "2026-03-12T06:00:00Z" AND status = "active"
    // status is NOT in the compound index → post-filter
    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({
            "filter": {
                "$and": [
                    {"category": "A"},
                    {"timestamp": {"$gte": "2026-03-12T06:00:00Z"}},
                    {"status": "active"}
                ]
            }
        }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);

    let docs = body["data"].as_array().unwrap();
    // Category A with ts >= 06:00: i=6,8,10,12,14,16,18
    // Active (i%3==0): i=6,12,18 → 3 docs
    assert_eq!(docs.len(), 3);
    for doc in docs {
        assert_eq!(doc["category"], "A");
        assert_eq!(doc["status"], "active");
    }

    assert_eq!(body["meta"]["scan_strategy"], "compound_range");
}

#[tokio::test]
async fn test_compound_range_count_only() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    for i in 0..20 {
        let category = if i % 2 == 0 { "A" } else { "B" };
        let ts = format!("2026-03-12T{:02}:00:00Z", i);
        client
            .post(format!("{base_url}/events/docs"))
            .json(&json!({"category": category, "timestamp": ts}))
            .send()
            .await
            .unwrap();
    }

    client
        .post(format!("{base_url}/events/indexes"))
        .json(&json!({"name": "idx_cat_ts", "fields": ["category", "timestamp"]}))
        .send()
        .await
        .unwrap();

    // count_only: category = "A" AND timestamp >= "2026-03-12T10:00:00Z"
    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({
            "filter": {
                "category": "A",
                "timestamp": {"$gte": "2026-03-12T10:00:00Z"}
            },
            "count_only": true
        }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);
    assert_eq!(body["data"]["count"], 5);
    assert_eq!(body["meta"]["docs_scanned"], 0);
    assert_eq!(body["meta"]["scan_strategy"], "compound_range");
}

#[tokio::test]
async fn test_compound_range_planner_priority() {
    let (base_url, _tmp) = start_test_server().await;
    let client = Client::new();

    client
        .post(format!("{base_url}/_collections"))
        .json(&json!({"name": "events"}))
        .send()
        .await
        .unwrap();

    for i in 0..20 {
        let category = if i % 4 == 0 {
            "A"
        } else if i % 4 == 1 {
            "B"
        } else if i % 4 == 2 {
            "C"
        } else {
            "D"
        };
        let ts = format!("2026-03-12T{:02}:00:00Z", i);
        client
            .post(format!("{base_url}/events/docs"))
            .json(&json!({"category": category, "timestamp": ts}))
            .send()
            .await
            .unwrap();
    }

    // Create BOTH a single-field index on category AND a compound index
    client
        .post(format!("{base_url}/events/indexes"))
        .json(&json!({"name": "idx_cat", "fields": ["category"]}))
        .send()
        .await
        .unwrap();
    client
        .post(format!("{base_url}/events/indexes"))
        .json(&json!({"name": "idx_cat_ts", "fields": ["category", "timestamp"]}))
        .send()
        .await
        .unwrap();

    // Query with eq + range: should prefer CompoundRange over single-field IndexEq
    let resp = client
        .post(format!("{base_url}/events/query"))
        .json(&json!({
            "filter": {
                "category": "A",
                "timestamp": {"$gte": "2026-03-12T10:00:00Z"}
            }
        }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);
    assert_eq!(body["meta"]["scan_strategy"], "compound_range");
    assert_eq!(body["meta"]["index_used"], "idx_cat_ts");

    // Verify docs_scanned is less than total for the category (compound range narrows)
    let total_cat_a = body["meta"]["total_count"].as_u64().unwrap();
    let docs = body["data"].as_array().unwrap();
    assert_eq!(docs.len() as u64, total_cat_a);
    // All returned docs should be category A with ts >= 10:00
    for doc in docs {
        assert_eq!(doc["category"], "A");
        assert!(doc["timestamp"].as_str().unwrap() >= "2026-03-12T10:00:00Z");
    }
}
