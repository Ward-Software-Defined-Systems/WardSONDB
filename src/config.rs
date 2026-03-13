use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "wardsondb",
    version,
    about = "A high-performance JSON document database",
    after_help = "FILE DESCRIPTORS:\n  \
        WardSONDB requires a high file descriptor limit for production use.\n  \
        macOS defaults to 256, Linux to 1024 — both are too low.\n  \
        Set before launching: ulimit -n 65536\n  \
        Example: ulimit -n 65536 && wardsondb --tls"
)]
pub struct Config {
    /// Listen port
    #[arg(short, long, default_value = "8080")]
    pub port: u16,

    /// Data directory
    #[arg(short, long, default_value = "./data")]
    pub data_dir: String,

    /// Log level: trace|debug|info|warn|error
    #[arg(short, long, default_value = "info")]
    pub log_level: String,

    /// Log file path (per-request logs always go here)
    #[arg(long, default_value = "wardsondb.log")]
    pub log_file: String,

    /// Print per-request logs to the terminal (verbose mode)
    #[arg(short, long, default_value_t = false)]
    pub verbose: bool,

    /// Enable TLS (HTTPS)
    #[arg(long, default_value_t = false)]
    pub tls: bool,

    /// Path to PEM certificate file (auto-generated if --tls without this)
    #[arg(long)]
    pub tls_cert: Option<String>,

    /// Path to PEM private key file (auto-generated if --tls without this)
    #[arg(long)]
    pub tls_key: Option<String>,

    /// TTL cleanup interval in seconds
    #[arg(long, default_value = "60")]
    pub ttl_interval: u64,

    /// API key for authentication (can be specified multiple times)
    #[arg(long = "api-key")]
    pub api_keys: Vec<String>,

    /// File containing API keys, one per line (lines starting with # are comments)
    #[arg(long = "api-key-file")]
    pub api_key_file: Option<String>,

    /// Query timeout in seconds (0 = no timeout)
    #[arg(long, default_value = "30")]
    pub query_timeout: u64,

    /// Make /_metrics endpoint publicly accessible (bypasses auth)
    #[arg(long, default_value_t = false)]
    pub metrics_public: bool,

    /// Cache size in MiB (block + blob cache shared across all partitions)
    #[arg(long, default_value = "64")]
    pub cache_size_mb: u64,

    /// Max write buffer size in MiB (total across all partitions)
    #[arg(long, default_value = "64")]
    pub write_buffer_mb: u64,

    /// Max memtable size in MiB (per partition, triggers flush when exceeded)
    #[arg(long, default_value = "8")]
    pub memtable_mb: u32,

    /// Number of background flush worker threads
    #[arg(long, default_value = "2")]
    pub flush_workers: usize,

    /// Number of background compaction worker threads
    #[arg(long, default_value = "2")]
    pub compaction_workers: usize,

    /// Comma-separated list of fields to track with bitmap indexes (auto-detected if empty)
    #[arg(long, default_value = "")]
    pub bitmap_fields: String,

    /// Maximum distinct values per bitmap column before disabling (default: 1000)
    #[arg(long, default_value = "1000")]
    pub bitmap_max_cardinality: u32,

    /// Number of inserts to sample for auto-detection of bitmap fields (default: 10000)
    #[arg(long, default_value = "10000")]
    pub bitmap_sample_size: u32,

    /// Disable the scan accelerator entirely
    #[arg(long, default_value_t = false)]
    pub no_bitmap: bool,
}
