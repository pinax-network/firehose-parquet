use std::path::PathBuf;

/// Partitioning strategy for output files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Partition {
    /// No partitioning — all data in a flat directory per table.
    None,
    /// Partition by block number ranges of the given size.
    BlockRange(u64),
    /// Partition by date (YYYY-MM-DD).
    Date,
    /// Partition by hour (YYYY-MM-DD/HH).
    Hour,
}

/// Compression codec for Parquet files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Snappy,
    Gzip,
    Zstd,
}

/// Metadata about a batch of blocks, used for partitioning decisions.
#[derive(Debug, Clone)]
pub struct BlockMetadata {
    pub min_block_number: u64,
    pub max_block_number: u64,
    /// Unix timestamp of the first block in the batch (seconds)
    pub min_timestamp: Option<i64>,
    /// Unix timestamp of the last block in the batch (seconds)
    pub max_timestamp: Option<i64>,
}

/// Pipeline configuration.
#[derive(Debug, Clone)]
pub struct Config {
    pub endpoint: String,
    pub api_key: Option<String>,
    pub jwt_token: Option<String>,
    pub start_block: Option<u64>,
    pub stop_block: Option<u64>,
    pub cursor_path: Option<PathBuf>,
    pub insecure: bool,
    pub plaintext: bool,
    pub output: PathBuf,
    pub partition: Partition,
    pub flush_rows: u32,
    pub flush_bytes: u64,
    pub flush_interval_secs: Option<u64>,
    pub compression: Compression,
    pub final_blocks_only: bool,
    pub dry_run: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            endpoint: "https://mainnet.sol.streamingfast.io:443".to_string(),
            api_key: None,
            jwt_token: None,
            start_block: None,
            stop_block: None,
            cursor_path: None,
            insecure: false,
            plaintext: false,
            output: PathBuf::from("output"),
            partition: Partition::None,
            flush_rows: 50_000,
            flush_bytes: 128 * 1024 * 1024, // 128 MB
            flush_interval_secs: None,
            compression: Compression::Zstd,
            final_blocks_only: true,
            dry_run: false,
        }
    }
}
