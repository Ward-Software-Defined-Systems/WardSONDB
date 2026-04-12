#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod config;
mod engine;
mod error;
mod index;
mod query;
mod schema;
mod server;

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use clap::Parser;
use tracing::{info, warn};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, fmt};

use config::Config;
use engine::storage::Storage;
use server::metrics::Metrics;
use server::{AppState, build_router};

#[tokio::main]
async fn main() {
    let config = Config::parse();

    // Build logging layers:
    //   - Terminal: shows everything EXCEPT per-request logs (unless --verbose)
    //   - File: shows everything including per-request logs
    let base_filter = &config.log_level;

    // Terminal filter: suppress wardsondb::requests unless verbose
    let terminal_filter = if config.verbose {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(base_filter))
    } else {
        EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new(format!("{base_filter},wardsondb::requests=off")))
    };

    let terminal_layer = fmt::layer()
        .with_writer(std::io::stderr)
        .with_filter(terminal_filter);

    // File layer: always logs everything including per-request logs
    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&config.log_file)
        .expect("Failed to open log file");

    let file_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(base_filter));

    let file_layer = fmt::layer()
        .with_writer(log_file)
        .with_ansi(false)
        .with_filter(file_filter);

    tracing_subscriber::registry()
        .with(terminal_layer)
        .with(file_layer)
        .init();

    // Check file descriptor limits
    check_file_descriptor_limit();

    // Open storage
    let data_dir = Path::new(&config.data_dir);
    std::fs::create_dir_all(data_dir).expect("Failed to create data directory");
    let mem_config = engine::storage::MemoryConfig {
        cache_size: config.cache_size_mb * 1024 * 1024,
        max_write_buffer_size: config.write_buffer_mb * 1024 * 1024,
        max_memtable_size: config.memtable_mb * 1024 * 1024,
        flush_workers: config.flush_workers,
        compaction_workers: config.compaction_workers,
    };
    let storage = Storage::open_with_config(data_dir, &config.storage_engine, mem_config)
        .expect("Failed to open database");
    info!(data_dir = %config.data_dir, engine = storage.engine_name, "Database opened");

    // Configure scan accelerator
    if !config.no_bitmap {
        let bitmap_fields: Vec<String> = config
            .bitmap_fields
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if !bitmap_fields.is_empty() {
            storage
                .scan_accelerator
                .configure_fields(bitmap_fields.clone());
            {
                let mut cfg = storage.scan_accelerator.config_mut();
                cfg.max_cardinality = config.bitmap_max_cardinality;
                cfg.max_memory_bytes = resolve_bitmap_memory_limit(config.bitmap_memory_mb);
            }

            // Try loading from disk first, then rebuild from storage
            let loaded = storage.scan_accelerator.load_from_disk(data_dir, "_all");
            if !loaded {
                rebuild_all_accelerators(&storage);
            }
            storage.scan_accelerator.set_ready(true);
            info!(fields = ?bitmap_fields, "Scan accelerator configured");
        }
        // If no explicit fields, auto-detection happens during inserts
    } else {
        info!("Scan accelerator disabled (--no-bitmap)");
    }

    let metrics = Arc::new(Metrics::new());

    // Load API keys from CLI flags and key file
    let api_keys = load_api_keys(&config);
    if !api_keys.is_empty() {
        info!(count = api_keys.len(), "API key authentication enabled");
    }

    let state = Arc::new(AppState {
        storage,
        config: config.clone(),
        started_at: Instant::now(),
        metrics: metrics.clone(),
        api_keys,
    });

    // Spawn periodic stats reporter (every 10 seconds)
    server::metrics::spawn_stats_reporter(metrics.clone(), 10);

    // Spawn bitmap persistence + compaction task (every 60 seconds)
    if !config.no_bitmap {
        let persist_state = state.clone();
        let data_dir_owned = config.data_dir.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(tokio::time::Duration::from_secs(60));
            tick.tick().await; // Skip first immediate tick
            loop {
                tick.tick().await;
                if persist_state.storage.scan_accelerator.is_ready() {
                    let dir = std::path::Path::new(&data_dir_owned);
                    if let Err(e) = persist_state
                        .storage
                        .scan_accelerator
                        .persist_to_disk(dir, "_all")
                    {
                        warn!(error = %e, "Failed to persist scan accelerator");
                    }

                    // Compact if >25% holes from TTL deletes
                    if persist_state.storage.scan_accelerator.needs_compaction() {
                        info!("Bitmap position map has >25% holes, triggering compaction rebuild");
                        let compact_state = persist_state.clone();
                        let _ = tokio::task::spawn_blocking(move || {
                            rebuild_all_accelerators(&compact_state.storage);
                        })
                        .await;
                    }
                }
            }
        });
    }

    // Spawn TTL cleanup worker
    {
        let state_clone = state.clone();
        let ttl_interval = config.ttl_interval;
        tokio::spawn(async move {
            server::ttl_worker::run_ttl_loop(state_clone, ttl_interval).await;
        });
    }

    let app = build_router(state);

    let addr = format!("0.0.0.0:{}", config.port);

    if config.verbose {
        info!("Verbose mode: per-request logs shown in terminal");
    } else {
        info!(log_file = %config.log_file, "Per-request logs written to file (use --verbose to show in terminal)");
    }

    if config.tls {
        let (cert_path, key_path) = resolve_tls_paths(&config);
        let scheme = "https";
        info!(addr = %addr, scheme = scheme, cert = %cert_path, "Starting WardSONDB with TLS");

        let tls_config =
            axum_server::tls_rustls::RustlsConfig::from_pem_file(&cert_path, &key_path)
                .await
                .expect("Failed to load TLS certificate/key");

        let bind_addr: std::net::SocketAddr = addr.parse().expect("Invalid bind address");
        axum_server::bind_rustls(bind_addr, tls_config)
            .serve(app.into_make_service())
            .await
            .expect("Server error");
    } else {
        info!(addr = %addr, "Starting WardSONDB");

        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .expect("Failed to bind address");

        axum::serve(listener, app).await.expect("Server error");
    }
}

fn load_api_keys(config: &Config) -> Vec<String> {
    let mut keys: Vec<String> = config.api_keys.clone();

    if let Some(path) = &config.api_key_file {
        match std::fs::read_to_string(path) {
            Ok(contents) => {
                for line in contents.lines() {
                    let trimmed = line.trim();
                    if !trimmed.is_empty()
                        && !trimmed.starts_with('#')
                        && !keys.contains(&trimmed.to_string())
                    {
                        keys.push(trimmed.to_string());
                    }
                }
            }
            Err(e) => {
                warn!(path = path, error = %e, "Failed to read API key file");
            }
        }
    }

    keys
}

fn resolve_tls_paths(config: &Config) -> (String, String) {
    if let (Some(cert), Some(key)) = (&config.tls_cert, &config.tls_key) {
        return (cert.clone(), key.clone());
    }

    // Auto-generate self-signed certificate
    let tls_dir = Path::new(&config.data_dir).join("tls");
    let cert_path = tls_dir.join("cert.pem");
    let key_path = tls_dir.join("key.pem");

    // Reuse existing certs if already generated
    if cert_path.exists() && key_path.exists() {
        info!("Reusing existing self-signed certificate");
        return (
            cert_path.to_string_lossy().to_string(),
            key_path.to_string_lossy().to_string(),
        );
    }

    info!("Generating self-signed TLS certificate");
    std::fs::create_dir_all(&tls_dir).expect("Failed to create TLS directory");

    let mut params = rcgen::CertificateParams::new(vec!["localhost".to_string()])
        .expect("Failed to create certificate params");
    params
        .subject_alt_names
        .push(rcgen::SanType::IpAddress(std::net::IpAddr::V4(
            std::net::Ipv4Addr::new(0, 0, 0, 0),
        )));
    params
        .subject_alt_names
        .push(rcgen::SanType::IpAddress(std::net::IpAddr::V4(
            std::net::Ipv4Addr::new(127, 0, 0, 1),
        )));
    // Valid for 365 days
    params.not_after = rcgen::date_time_ymd(2027, 3, 9);

    let key_pair = rcgen::KeyPair::generate().expect("Failed to generate key pair");
    let cert = params
        .self_signed(&key_pair)
        .expect("Failed to generate self-signed certificate");

    std::fs::write(&cert_path, cert.pem()).expect("Failed to write certificate");
    std::fs::write(&key_path, key_pair.serialize_pem()).expect("Failed to write private key");

    info!(
        cert = %cert_path.display(),
        key = %key_path.display(),
        "Self-signed certificate generated (valid 365 days)"
    );

    (
        cert_path.to_string_lossy().to_string(),
        key_path.to_string_lossy().to_string(),
    )
}

/// Rebuild scan accelerator from all existing collections using batched iteration.
/// Peak memory: ~BATCH_SIZE documents instead of all documents.
fn rebuild_all_accelerators(storage: &Storage) {
    const BATCH_SIZE: usize = 10_000;

    let collections = match storage.list_collections() {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "Failed to list collections for accelerator rebuild");
            return;
        }
    };

    storage.scan_accelerator.set_ready(false);
    storage.scan_accelerator.clear();

    // Re-create columns after clear
    let fields = storage.scan_accelerator.config_read().bitmap_fields.clone();
    if !fields.is_empty() {
        storage.scan_accelerator.configure_fields(fields);
    }

    let start = std::time::Instant::now();
    let mut total_docs: usize = 0;

    for col in &collections {
        let docs_partition = match storage.get_docs_partition(&col.name) {
            Ok(p) => p,
            Err(e) => {
                warn!(collection = col.name, error = %e, "Skipping collection for rebuild");
                continue;
            }
        };
        use engine::backend::StorageBackend;
        let iter = match storage.engine.full_iterator(&docs_partition) {
            Ok(it) => it,
            Err(e) => {
                warn!(collection = col.name, error = ?e, "Skipping collection for rebuild");
                continue;
            }
        };
        let mut batch: Vec<(String, serde_json::Value)> = Vec::with_capacity(BATCH_SIZE);

        for kv in iter {
            if let Ok((_, value)) = kv
                && let Ok(doc) = serde_json::from_slice::<serde_json::Value>(&value)
                && let Some(id) = doc.get("_id").and_then(|v| v.as_str())
            {
                batch.push((id.to_string(), doc));
            }
            if batch.len() >= BATCH_SIZE {
                total_docs += batch.len();
                storage.scan_accelerator.rebuild_batch(&batch);
                batch.clear();
                if storage.scan_accelerator.is_over_budget() {
                    info!(
                        docs_indexed = total_docs,
                        "Bitmap rebuild stopped early: memory budget exceeded"
                    );
                    break;
                }
            }
        }
        if !batch.is_empty() {
            total_docs += batch.len();
            storage.scan_accelerator.rebuild_batch(&batch);
        }
    }

    info!(
        docs = total_docs,
        elapsed_ms = start.elapsed().as_millis(),
        "Scan accelerator rebuilt (batched)"
    );
    storage.scan_accelerator.set_ready(true);
}

/// Check the OS file descriptor limit and warn if too low.
/// fjall opens file handles for each SST segment and will hit "Too many open files"
/// past ~900K documents if the limit is too low (macOS default: 256, Linux: 1024).
fn check_file_descriptor_limit() {
    const MIN_RECOMMENDED: u64 = 4096;

    let mut rlim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };

    let ret = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) };
    if ret != 0 {
        warn!("Could not check file descriptor limit (getrlimit failed)");
        return;
    }

    let current = rlim.rlim_cur;
    let max = rlim.rlim_max;

    info!(
        current = current,
        max = max,
        "File descriptor limit (ulimit -n)"
    );

    if current < MIN_RECOMMENDED {
        // Attempt to raise to min(max, MIN_RECOMMENDED)
        let target = if max >= MIN_RECOMMENDED || max == libc::RLIM_INFINITY {
            MIN_RECOMMENDED
        } else {
            max
        };

        let new_rlim = libc::rlimit {
            rlim_cur: target,
            rlim_max: max,
        };
        let raise_ret = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &new_rlim) };
        if raise_ret == 0 && target >= MIN_RECOMMENDED {
            info!(
                from = current,
                to = target,
                "Raised file descriptor limit automatically"
            );
        } else {
            warn!(
                current = current,
                recommended = MIN_RECOMMENDED,
                "Low file descriptor limit — fjall may crash with 'Too many open files' \
                 at large document counts. Fix: ulimit -n 65536"
            );
        }
    }
}

/// Resolve the bitmap memory limit from the CLI flag.
/// 0 = auto: min(4GB, 10% of system RAM).
fn resolve_bitmap_memory_limit(configured_mb: u64) -> u64 {
    if configured_mb > 0 {
        let bytes = configured_mb * 1024 * 1024;
        info!(
            bitmap_memory_mb = configured_mb,
            "Bitmap memory budget set (explicit)"
        );
        return bytes;
    }
    let four_gb: u64 = 4 * 1024 * 1024 * 1024;
    let budget = match system_ram_bytes() {
        Some(ram) => std::cmp::min(four_gb, ram / 10),
        None => four_gb,
    };
    info!(
        bitmap_memory_mb = budget / (1024 * 1024),
        "Bitmap memory budget set (auto)"
    );
    budget
}

/// Detect total system RAM in bytes.
fn system_ram_bytes() -> Option<u64> {
    #[cfg(target_os = "macos")]
    {
        use std::mem;
        let mut size: u64 = 0;
        let mut len = mem::size_of::<u64>();
        let mut mib: [libc::c_int; 2] = [libc::CTL_HW, libc::HW_MEMSIZE];
        let ret = unsafe {
            libc::sysctl(
                mib.as_mut_ptr(),
                2,
                &mut size as *mut u64 as *mut libc::c_void,
                &mut len,
                std::ptr::null_mut(),
                0,
            )
        };
        if ret == 0 && size > 0 {
            return Some(size);
        }
    }

    #[cfg(target_os = "linux")]
    {
        if let Ok(contents) = std::fs::read_to_string("/proc/meminfo") {
            for line in contents.lines() {
                if let Some(rest) = line.strip_prefix("MemTotal:") {
                    let rest = rest.trim();
                    if let Some(kb_str) = rest.strip_suffix("kB") {
                        if let Ok(kb) = kb_str.trim().parse::<u64>() {
                            return Some(kb * 1024);
                        }
                    }
                }
            }
        }
    }

    None
}
