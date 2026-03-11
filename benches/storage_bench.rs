use criterion::{Criterion, criterion_group, criterion_main};
use serde_json::{Value, json};
use tempfile::TempDir;

use wardsondb::engine::storage::Storage;
use wardsondb::query::executor::execute_query;
use wardsondb::query::parser::{QueryRequest, parse_query};

fn create_siem_event(i: u64) -> Value {
    let event_types = ["firewall", "dns", "dhcp", "auth", "vpn"];
    let severities = ["low", "medium", "high", "critical"];
    let actions = ["allow", "block", "drop", "reject"];
    let ports = [22, 80, 443, 8080, 3306];

    let idx = i as usize;

    json!({
        "event_type": event_types[idx % event_types.len()],
        "severity": severities[idx % severities.len()],
        "network": {
            "src_ip": format!("192.168.{}.{}", (i / 256) % 256, i % 256),
            "dst_ip": format!("10.0.{}.{}", (i / 256) % 256, i % 256),
            "dst_port": ports[idx % ports.len()],
            "action": actions[idx % actions.len()],
        },
        "received_at": format!("2026-03-09T{:02}:{:02}:{:02}Z", (i / 3600) % 24, (i / 60) % 60, i % 60),
        "message": format!("Event number {i}"),
    })
}

fn setup_storage_with_docs(n: u64) -> (Storage, TempDir) {
    let tmp = TempDir::new().unwrap();
    let storage = Storage::open(tmp.path()).unwrap();
    storage.create_collection("events").unwrap();

    // Insert in batches of 500
    let batch_size = 500;
    let mut i = 0u64;
    while i < n {
        let end = std::cmp::min(i + batch_size, n);
        let docs: Vec<Value> = (i..end).map(create_siem_event).collect();
        storage.bulk_insert_documents("events", docs).unwrap();
        i = end;
    }

    (storage, tmp)
}

fn bench_single_insert(c: &mut Criterion) {
    let tmp = TempDir::new().unwrap();
    let storage = Storage::open(tmp.path()).unwrap();
    storage.create_collection("events").unwrap();

    let mut i = 0u64;
    c.bench_function("single_insert", |b| {
        b.iter(|| {
            let doc = create_siem_event(i);
            storage.insert_document("events", doc).unwrap();
            i += 1;
        });
    });
}

fn bench_bulk_insert_500(c: &mut Criterion) {
    let tmp = TempDir::new().unwrap();
    let storage = Storage::open(tmp.path()).unwrap();
    storage.create_collection("events").unwrap();

    let mut i = 0u64;
    c.bench_function("bulk_insert_500", |b| {
        b.iter(|| {
            let docs: Vec<Value> = (i..i + 500).map(create_siem_event).collect();
            storage.bulk_insert_documents("events", docs).unwrap();
            i += 500;
        });
    });
}

fn bench_get_by_id(c: &mut Criterion) {
    let tmp = TempDir::new().unwrap();
    let storage = Storage::open(tmp.path()).unwrap();
    storage.create_collection("events").unwrap();

    // Insert 1000 docs and collect their IDs
    let mut ids = Vec::new();
    for i in 0..1000 {
        let doc = storage
            .insert_document("events", create_siem_event(i))
            .unwrap();
        ids.push(doc["_id"].as_str().unwrap().to_string());
    }

    let mut idx = 0;
    c.bench_function("get_by_id", |b| {
        b.iter(|| {
            let id = &ids[idx % ids.len()];
            storage.get_document("events", id).unwrap();
            idx += 1;
        });
    });
}

fn bench_query_10k(c: &mut Criterion) {
    let (storage, _tmp) = setup_storage_with_docs(10_000);

    c.bench_function("query_eq_filter_10k", |b| {
        b.iter(|| {
            let query = parse_query(QueryRequest {
                filter: Some(json!({"event_type": "firewall"})),
                sort: None,
                limit: Some(50),
                offset: Some(0),
                fields: None,
                count_only: None,
            })
            .unwrap();
            execute_query(&storage, "events", &query).unwrap();
        });
    });

    c.bench_function("query_nested_filter_10k", |b| {
        b.iter(|| {
            let query = parse_query(QueryRequest {
                filter: Some(json!({"network.dst_port": 443})),
                sort: None,
                limit: Some(50),
                offset: Some(0),
                fields: None,
                count_only: None,
            })
            .unwrap();
            execute_query(&storage, "events", &query).unwrap();
        });
    });

    c.bench_function("query_with_sort_10k", |b| {
        b.iter(|| {
            let query = parse_query(QueryRequest {
                filter: Some(json!({"event_type": "firewall"})),
                sort: Some(vec![json!({"network.dst_port": "desc"})]),
                limit: Some(50),
                offset: Some(0),
                fields: None,
                count_only: None,
            })
            .unwrap();
            execute_query(&storage, "events", &query).unwrap();
        });
    });

    c.bench_function("query_count_only_10k", |b| {
        b.iter(|| {
            let query = parse_query(QueryRequest {
                filter: Some(json!({"severity": "high"})),
                sort: None,
                limit: None,
                offset: None,
                fields: None,
                count_only: Some(true),
            })
            .unwrap();
            execute_query(&storage, "events", &query).unwrap();
        });
    });
}

fn bench_query_100k(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_100k");
    group.sample_size(10); // Fewer samples for expensive benchmarks

    let (storage, _tmp) = setup_storage_with_docs(100_000);

    group.bench_function("eq_filter", |b| {
        b.iter(|| {
            let query = parse_query(QueryRequest {
                filter: Some(json!({"event_type": "firewall"})),
                sort: None,
                limit: Some(50),
                offset: Some(0),
                fields: None,
                count_only: None,
            })
            .unwrap();
            execute_query(&storage, "events", &query).unwrap();
        });
    });

    group.bench_function("nested_eq_filter", |b| {
        b.iter(|| {
            let query = parse_query(QueryRequest {
                filter: Some(json!({"network.dst_port": 443})),
                sort: None,
                limit: Some(50),
                offset: Some(0),
                fields: None,
                count_only: None,
            })
            .unwrap();
            execute_query(&storage, "events", &query).unwrap();
        });
    });

    group.bench_function("complex_filter_sort", |b| {
        b.iter(|| {
            let query = parse_query(QueryRequest {
                filter: Some(json!({
                    "$and": [
                        {"event_type": "firewall"},
                        {"network.action": "block"},
                        {"severity": {"$in": ["high", "critical"]}}
                    ]
                })),
                sort: Some(vec![json!({"received_at": "desc"})]),
                limit: Some(100),
                offset: Some(0),
                fields: None,
                count_only: None,
            })
            .unwrap();
            execute_query(&storage, "events", &query).unwrap();
        });
    });

    group.bench_function("count_only", |b| {
        b.iter(|| {
            let query = parse_query(QueryRequest {
                filter: Some(json!({"severity": "high"})),
                sort: None,
                limit: None,
                offset: None,
                fields: None,
                count_only: Some(true),
            })
            .unwrap();
            execute_query(&storage, "events", &query).unwrap();
        });
    });

    group.bench_function("full_scan_no_filter", |b| {
        b.iter(|| {
            let query = parse_query(QueryRequest {
                filter: None,
                sort: None,
                limit: Some(50),
                offset: Some(0),
                fields: None,
                count_only: None,
            })
            .unwrap();
            execute_query(&storage, "events", &query).unwrap();
        });
    });

    group.bench_function("projection", |b| {
        b.iter(|| {
            let query = parse_query(QueryRequest {
                filter: Some(json!({"event_type": "firewall"})),
                sort: None,
                limit: Some(50),
                offset: Some(0),
                fields: Some(vec![
                    "event_type".into(),
                    "network.src_ip".into(),
                    "severity".into(),
                ]),
                count_only: None,
            })
            .unwrap();
            execute_query(&storage, "events", &query).unwrap();
        });
    });

    group.finish();
}

fn bench_scan_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan_all");
    group.sample_size(10);

    let (storage, _tmp) = setup_storage_with_docs(100_000);

    group.bench_function("scan_100k_docs", |b| {
        b.iter(|| {
            storage.scan_all_documents("events").unwrap();
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_single_insert,
    bench_bulk_insert_500,
    bench_get_by_id,
    bench_query_10k,
    bench_query_100k,
    bench_scan_all,
);
criterion_main!(benches);
