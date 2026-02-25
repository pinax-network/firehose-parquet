use std::path::PathBuf;

/// Partitioning strategy for output files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Partition {
    /// No partitioning — all data in a flat directory per table.
    None,
    /// Partition by block number ranges of the given size.
    BlockRange(u64),
}

/// Compression codec for Parquet files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Snappy,
    Gzip,
    Zstd,
}

/// Pipeline configuration.
#[derive(Debug, Clone)]
pub struct Config {
    pub endpoint: String,
    pub api_token: Option<String>,
    pub start_block: Option<u64>,
    pub stop_block: Option<u64>,
    pub cursor: Option<String>,
    pub output: PathBuf,
    pub partition: Partition,
    pub flush_rows: u32,
    pub flush_bytes: u64,
    pub compression: Compression,
    pub final_blocks_only: bool,
    pub dry_run: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            endpoint: "https://mainnet.sol.streamingfast.io:443".to_string(),
            api_token: None,
            start_block: None,
            stop_block: None,
            cursor: None,
            output: PathBuf::from("output"),
            partition: Partition::None,
            flush_rows: 50_000,
            flush_bytes: 128 * 1024 * 1024, // 128 MB
            compression: Compression::Zstd,
            final_blocks_only: true,
            dry_run: false,
        }
    }
}
