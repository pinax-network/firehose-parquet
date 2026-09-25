use std::path::PathBuf;

/// Hive prefix of the day-of-month directory written by time-based partitioning
/// (`year=YYYY/month=MM/day=DD/...`).
pub const DAY_PARTITION_PREFIX: &str = "day=";

/// Day-of-month prefix written by earlier releases (`date=DD`). Under hive
/// partitioning it collided with the canonical `date` column, so new output uses
/// [`DAY_PARTITION_PREFIX`]. Readers accept both.
pub const LEGACY_DAY_PARTITION_PREFIX: &str = "date=";

/// Partitioning strategy for output files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Partition {
    /// No partitioning — all data in a flat directory per table.
    None,
    /// Partition by block number ranges of the given size.
    BlockRange { size: u64, start_block: Option<u64> },
    /// Partition by date (`year=YYYY/month=MM/day=DD`).
    Date,
    /// Partition by hour (`.../day=DD/hour=HH`).
    Hour,
    /// Partition by minute (`.../hour=HH/minute=MM`).
    Minute,
    /// Partition by second (`.../minute=MM/second=SS`).
    Second,
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

impl BlockMetadata {
    /// Merge another metadata range into this one, expanding min/max bounds.
    pub fn merge(&mut self, other: &BlockMetadata) {
        self.min_block_number = self.min_block_number.min(other.min_block_number);
        self.max_block_number = self.max_block_number.max(other.max_block_number);
        self.min_timestamp = match (self.min_timestamp, other.min_timestamp) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        self.max_timestamp = match (self.max_timestamp, other.max_timestamp) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
    }
}

/// Pipeline configuration.
#[derive(Debug, Clone)]
pub struct Config {
    pub endpoint: String,
    pub api_key: Option<String>,
    pub jwt_token: Option<String>,
    pub start_block: Option<u64>,
    pub stop_block: Option<u64>,
    pub skip_missing_blocks: bool,
    /// Resolved cursor location — either a local path or an `s3://` URI.
    /// Must have a `.parquet` extension.
    pub cursor_path: Option<String>,
    pub output: PathBuf,
    pub partition: Partition,
    pub flush_rows: Option<u32>,
    pub flush_blocks: Option<u64>,
    pub flush_bytes: u64,
    pub flush_interval_secs: Option<u64>,
    pub compression: Compression,
    pub final_blocks_only: bool,
    pub dry_run: bool,
    // AWS S3 credentials (used when output is an s3:// URL)
    pub aws_access_key_id: Option<String>,
    pub aws_secret_access_key: Option<String>,
    pub aws_session_token: Option<String>,
    pub aws_region: Option<String>,
    pub aws_endpoint_url: Option<String>,
    pub s3_bucket: Option<String>,
    pub cache_control: Option<String>,
    pub metrics_port: Option<u16>,
    pub stream_idle_timeout_secs: Option<u64>,
    pub reconnect_stall_timeout_secs: Option<u64>,
}

impl Partition {
    pub fn block_range(size: u64) -> Self {
        Self::BlockRange {
            size,
            start_block: None,
        }
    }

    pub fn set_block_range_start(&mut self, start_block: Option<u64>) {
        if let Self::BlockRange {
            start_block: anchor,
            ..
        } = self
        {
            *anchor = start_block;
        }
    }

    pub fn block_range_bounds(&self, block_number: u64) -> Option<(u64, u64)> {
        let Self::BlockRange { size, start_block } = self else {
            return None;
        };

        let anchor = start_block.unwrap_or(0);
        if block_number < anchor {
            debug_assert!(
                block_number >= anchor,
                "block number {block_number} precedes anchored block-range start {anchor}"
            );
            let stop = anchor.saturating_add(*size);
            return Some((anchor, stop));
        }

        let relative_block = block_number - anchor;
        let partition_index = relative_block / *size;
        let start = anchor.saturating_add(partition_index.saturating_mul(*size));
        let stop = start.saturating_add(*size);
        Some((start, stop))
    }

    /// Compute the partition key for a block given its number and timestamp.
    ///
    /// Returns a string that uniquely identifies the partition bucket this block
    /// belongs to. Two blocks in the same partition return the same key.
    /// Returns `None` for `Partition::None` (no partitioning).
    pub fn partition_key(&self, block_number: u64, timestamp: i64) -> Option<String> {
        match self {
            Partition::None => None,
            Partition::BlockRange { .. } => self
                .block_range_bounds(block_number)
                .map(|(start, stop)| format!("block_range={start}-{stop}")),
            Partition::Date => {
                let dt = time::OffsetDateTime::from_unix_timestamp(timestamp)
                    .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
                Some(format!(
                    "year={:04}/month={:02}/day={:02}",
                    dt.year(),
                    dt.month() as u8,
                    dt.day()
                ))
            }
            Partition::Hour => {
                let dt = time::OffsetDateTime::from_unix_timestamp(timestamp)
                    .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
                Some(format!(
                    "year={:04}/month={:02}/day={:02}/hour={:02}",
                    dt.year(),
                    dt.month() as u8,
                    dt.day(),
                    dt.hour()
                ))
            }
            Partition::Minute => {
                let dt = time::OffsetDateTime::from_unix_timestamp(timestamp)
                    .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
                Some(format!(
                    "year={:04}/month={:02}/day={:02}/hour={:02}/minute={:02}",
                    dt.year(),
                    dt.month() as u8,
                    dt.day(),
                    dt.hour(),
                    dt.minute()
                ))
            }
            Partition::Second => {
                let dt = time::OffsetDateTime::from_unix_timestamp(timestamp)
                    .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
                Some(format!(
                    "year={:04}/month={:02}/day={:02}/hour={:02}/minute={:02}/second={:02}",
                    dt.year(),
                    dt.month() as u8,
                    dt.day(),
                    dt.hour(),
                    dt.minute(),
                    dt.second()
                ))
            }
        }
    }
}

impl std::fmt::Display for Partition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Partition::None => write!(f, "none"),
            Partition::BlockRange { size, .. } => write!(f, "block_range({size})"),
            Partition::Date => write!(f, "date"),
            Partition::Hour => write!(f, "hour"),
            Partition::Minute => write!(f, "minute"),
            Partition::Second => write!(f, "second"),
        }
    }
}

impl std::fmt::Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Compression::None => write!(f, "none"),
            Compression::Snappy => write!(f, "snappy"),
            Compression::Gzip => write!(f, "gzip"),
            Compression::Zstd => write!(f, "zstd"),
        }
    }
}

impl std::fmt::Display for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let auth = if self.api_key.is_some() {
            "api_key (***)"
        } else if self.jwt_token.is_some() {
            "bearer (***)"
        } else {
            "none"
        };

        let start = self
            .start_block
            .map_or("N/A".to_string(), |n| n.to_string());
        let stop = self.stop_block.map_or("stream forever".to_string(), |n| {
            if n == 0 {
                "stream forever".to_string()
            } else {
                n.to_string()
            }
        });

        let flush_bytes = if self.flush_bytes == 0 {
            "disabled".to_string()
        } else if self.flush_bytes >= 1024 * 1024 * 1024 {
            format!("{} GiB", self.flush_bytes / (1024 * 1024 * 1024))
        } else if self.flush_bytes >= 1024 * 1024 {
            format!("{} MiB", self.flush_bytes / (1024 * 1024))
        } else if self.flush_bytes >= 1024 {
            format!("{} KiB", self.flush_bytes / 1024)
        } else {
            format!("{} B", self.flush_bytes)
        };

        writeln!(f, "  endpoint           {}", self.endpoint)?;
        writeln!(f, "  auth               {auth}")?;
        writeln!(f, "  start_block        {start}")?;
        writeln!(f, "  stop_block         {stop} (exclusive)")?;
        writeln!(f, "  missing_blocks     skip after probe retries")?;
        if let Some(ref path) = self.cursor_path {
            writeln!(f, "  cursor             {}", path)?;
        }
        writeln!(f, "  output             {}", self.output.display())?;
        writeln!(f, "  partition          {}", self.partition)?;
        writeln!(f, "  compression        {}", self.compression)?;
        if let Some(rows) = self.flush_rows {
            writeln!(f, "  flush_rows         {rows}")?;
        }
        if let Some(blocks) = self.flush_blocks {
            writeln!(f, "  flush_blocks       {blocks}")?;
        }
        writeln!(f, "  flush_bytes        {flush_bytes}")?;
        if let Some(secs) = self.flush_interval_secs {
            writeln!(f, "  flush_interval     {secs}s")?;
        }
        writeln!(f, "  final_blocks_only  {}", self.final_blocks_only)?;
        if self.dry_run {
            writeln!(f, "  dry_run            true")?;
        }
        if let Some(port) = self.metrics_port {
            writeln!(f, "  metrics_port       {port}")?;
        }
        if let Some(secs) = self.stream_idle_timeout_secs {
            writeln!(f, "  stream_idle_timeout {secs}s")?;
        }
        if let Some(secs) = self.reconnect_stall_timeout_secs {
            writeln!(f, "  reconnect_stall_timeout {secs}s")?;
        }
        // AWS / S3 section — only shown when output is actually targeting S3.
        if self.output.to_string_lossy().starts_with("s3://")
            && (self.aws_access_key_id.is_some()
                || self.aws_secret_access_key.is_some()
                || self.aws_endpoint_url.is_some()
                || self.aws_region.is_some()
                || self.s3_bucket.is_some())
        {
            if let Some(ref bucket) = self.s3_bucket {
                writeln!(f, "  s3_bucket          {bucket}")?;
            }
            if let Some(ref region) = self.aws_region {
                writeln!(f, "  aws_region         {region}")?;
            }
            if let Some(ref endpoint) = self.aws_endpoint_url {
                writeln!(f, "  aws_endpoint       {endpoint}")?;
            }
            if self.aws_access_key_id.is_some() {
                writeln!(f, "  aws_access_key     ***")?;
            }
            if self.aws_secret_access_key.is_some() {
                writeln!(f, "  aws_secret_key     ***")?;
            }
            if self.aws_session_token.is_some() {
                writeln!(f, "  aws_session_token  ***")?;
            }
        }
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            endpoint: "https://mainnet.sol.streamingfast.io:443".to_string(),
            api_key: None,
            jwt_token: None,
            start_block: None,
            stop_block: None,
            skip_missing_blocks: true,
            cursor_path: None,
            output: PathBuf::from("."),
            partition: Partition::None,
            flush_rows: None,
            flush_blocks: None,
            flush_bytes: 128 * 1024 * 1024, // 128 MiB; set to 0 to disable size-based rollover
            flush_interval_secs: None,
            compression: Compression::Zstd,
            final_blocks_only: true,
            dry_run: false,
            aws_access_key_id: None,
            aws_secret_access_key: None,
            aws_session_token: None,
            aws_region: None,
            aws_endpoint_url: None,
            s3_bucket: None,
            cache_control: None,
            metrics_port: None,
            stream_idle_timeout_secs: Some(120),
            reconnect_stall_timeout_secs: Some(900),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_display() {
        assert_eq!(Partition::None.to_string(), "none");
        assert_eq!(
            Partition::block_range(10000).to_string(),
            "block_range(10000)"
        );
        assert_eq!(Partition::Date.to_string(), "date");
        assert_eq!(Partition::Hour.to_string(), "hour");
    }

    #[test]
    fn test_compression_display() {
        assert_eq!(Compression::None.to_string(), "none");
        assert_eq!(Compression::Snappy.to_string(), "snappy");
        assert_eq!(Compression::Gzip.to_string(), "gzip");
        assert_eq!(Compression::Zstd.to_string(), "zstd");
    }

    #[test]
    fn test_config_display_default() {
        let config = Config::default();
        let display = config.to_string();
        assert!(display.contains("endpoint"));
        assert!(display.contains("auth               none"));
        assert!(display.contains("start_block        N/A"));
        assert!(display.contains("stop_block         stream forever (exclusive)"));
        assert!(display.contains("missing_blocks     skip after probe retries"));
        assert!(display.contains("partition          none"));
        assert!(display.contains("compression        zstd"));
        assert!(!display.contains("flush_rows"));
        assert!(!display.contains("flush_blocks"));
        assert!(display.contains("128 MiB"));
        assert!(display.contains("final_blocks_only  true"));
        // dry_run defaults to false, so it should not appear
        assert!(!display.contains("dry_run"));
    }

    #[test]
    fn test_config_display_masks_api_key() {
        let config = Config {
            api_key: Some("super-secret-key".to_string()),
            ..Config::default()
        };
        let display = config.to_string();
        assert!(display.contains("api_key (***)"));
        assert!(!display.contains("super-secret-key"));
    }

    #[test]
    fn test_config_display_masks_jwt_token() {
        let config = Config {
            jwt_token: Some("super-secret-token".to_string()),
            ..Config::default()
        };
        let display = config.to_string();
        assert!(display.contains("bearer (***)"));
        assert!(!display.contains("super-secret-token"));
    }

    #[test]
    fn test_config_display_with_blocks() {
        let config = Config {
            start_block: Some(100),
            stop_block: Some(200),
            ..Config::default()
        };
        let display = config.to_string();
        assert!(display.contains("start_block        100"));
        assert!(display.contains("stop_block         200 (exclusive)"));
    }

    #[test]
    fn test_config_default_skips_missing_blocks() {
        assert!(Config::default().skip_missing_blocks);
        assert!(Config::default()
            .to_string()
            .contains("missing_blocks     skip after probe retries"));
    }

    #[test]
    fn test_config_display_stop_block_zero() {
        let config = Config {
            stop_block: Some(0),
            ..Config::default()
        };
        let display = config.to_string();
        assert!(display.contains("stop_block         stream forever (exclusive)"));
    }

    #[test]
    fn test_config_display_with_cursor() {
        let config = Config {
            cursor_path: Some("cursor.parquet".to_string()),
            ..Config::default()
        };
        let display = config.to_string();
        assert!(display.contains("cursor             cursor.parquet"));
    }

    #[test]
    fn test_config_display_with_flush_interval() {
        let config = Config {
            flush_interval_secs: Some(60),
            ..Config::default()
        };
        let display = config.to_string();
        assert!(display.contains("flush_interval     60s"));
    }

    #[test]
    fn test_config_display_with_flush_blocks() {
        let config = Config {
            flush_blocks: Some(25),
            ..Config::default()
        };
        let display = config.to_string();
        assert!(display.contains("flush_blocks       25"));
    }

    #[test]
    fn test_config_display_dry_run() {
        let config = Config {
            dry_run: true,
            ..Config::default()
        };
        let display = config.to_string();
        assert!(display.contains("dry_run            true"));
    }

    #[test]
    fn test_config_display_aws_credentials() {
        let config = Config {
            output: PathBuf::from("s3://my-bucket/output"),
            s3_bucket: Some("my-bucket".to_string()),
            aws_region: Some("us-east-1".to_string()),
            aws_endpoint_url: Some("https://t3.storage.dev".to_string()),
            aws_access_key_id: Some("AKID123456".to_string()),
            aws_secret_access_key: Some("super-secret".to_string()),
            aws_session_token: Some("tok-secret".to_string()),
            ..Config::default()
        };
        let display = config.to_string();
        // Non-secrets shown in clear
        assert!(display.contains("s3_bucket          my-bucket"));
        assert!(display.contains("aws_region         us-east-1"));
        assert!(display.contains("aws_endpoint       https://t3.storage.dev"));
        // Secrets masked
        assert!(display.contains("aws_access_key     ***"));
        assert!(display.contains("aws_secret_key     ***"));
        assert!(display.contains("aws_session_token  ***"));
        // Actual secret values never appear
        assert!(!display.contains("AKID123456"));
        assert!(!display.contains("super-secret"));
        assert!(!display.contains("tok-secret"));
    }

    #[test]
    fn test_config_display_no_aws_when_unset() {
        let config = Config::default();
        let display = config.to_string();
        assert!(!display.contains("s3_bucket"));
        assert!(!display.contains("aws_region"));
        assert!(!display.contains("aws_endpoint"));
        assert!(!display.contains("aws_access_key"));
        assert!(!display.contains("aws_secret_key"));
        assert!(!display.contains("aws_session_token"));
    }

    #[test]
    fn test_config_display_omits_aws_for_local_output_even_when_configured() {
        let config = Config {
            output: PathBuf::from("./output"),
            s3_bucket: Some("my-bucket".to_string()),
            aws_region: Some("us-east-1".to_string()),
            aws_endpoint_url: Some("https://t3.storage.dev".to_string()),
            aws_access_key_id: Some("AKID123456".to_string()),
            aws_secret_access_key: Some("super-secret".to_string()),
            aws_session_token: Some("tok-secret".to_string()),
            ..Config::default()
        };
        let display = config.to_string();
        assert!(display.contains("output             ./output"));
        assert!(!display.contains("s3_bucket"));
        assert!(!display.contains("aws_region"));
        assert!(!display.contains("aws_endpoint"));
        assert!(!display.contains("aws_access_key"));
        assert!(!display.contains("aws_secret_key"));
        assert!(!display.contains("aws_session_token"));
    }

    #[test]
    fn test_config_display_flush_bytes_formats() {
        // KiB range
        let config = Config {
            flush_bytes: 512 * 1024,
            ..Config::default()
        };
        assert!(config.to_string().contains("512 KiB"));

        // GiB range
        let config = Config {
            flush_bytes: 2 * 1024 * 1024 * 1024,
            ..Config::default()
        };
        assert!(config.to_string().contains("2 GiB"));

        // Bytes range
        let config = Config {
            flush_bytes: 500,
            ..Config::default()
        };
        assert!(config.to_string().contains("500 B"));

        // Disabled (0)
        let config = Config {
            flush_bytes: 0,
            ..Config::default()
        };
        assert!(config.to_string().contains("disabled"));
    }

    #[test]
    fn test_partition_key_none() {
        assert_eq!(Partition::None.partition_key(100, 1000), None);
    }

    #[test]
    fn test_partition_key_block_range() {
        assert_eq!(
            Partition::block_range(1000).partition_key(1000, 0),
            Some("block_range=1000-2000".to_string())
        );
        assert_eq!(
            Partition::block_range(1000).partition_key(1500, 0),
            Some("block_range=1000-2000".to_string())
        );
        assert_eq!(
            Partition::block_range(1000).partition_key(1999, 0),
            Some("block_range=1000-2000".to_string())
        );
        assert_eq!(
            Partition::block_range(1000).partition_key(2000, 0),
            Some("block_range=2000-3000".to_string())
        );
    }

    #[test]
    fn test_partition_key_block_range_uses_start_block_anchor() {
        let mut partition = Partition::block_range(100);
        partition.set_block_range_start(Some(9_820_210));

        assert_eq!(
            partition.partition_key(9_820_210, 0),
            Some("block_range=9820210-9820310".to_string())
        );
        assert_eq!(
            partition.partition_key(9_820_309, 0),
            Some("block_range=9820210-9820310".to_string())
        );
        assert_eq!(
            partition.partition_key(9_820_310, 0),
            Some("block_range=9820310-9820410".to_string())
        );
    }

    #[test]
    fn test_partition_key_date() {
        // 2024-01-15 12:00:00 UTC = 1705320000
        assert_eq!(
            Partition::Date.partition_key(100, 1705320000),
            Some("year=2024/month=01/day=15".to_string())
        );
        // 2024-01-16 00:00:00 UTC = 1705363200
        assert_eq!(
            Partition::Date.partition_key(200, 1705363200),
            Some("year=2024/month=01/day=16".to_string())
        );
    }

    #[test]
    fn test_partition_key_hour() {
        // 2024-01-15 14:30:00 UTC = 1705329000
        assert_eq!(
            Partition::Hour.partition_key(100, 1705329000),
            Some("year=2024/month=01/day=15/hour=14".to_string())
        );
    }

    #[test]
    fn test_partition_key_detects_boundary() {
        // Last second of 2024-01-15 vs first second of 2024-01-16
        let key1 = Partition::Date.partition_key(100, 1705363199); // 23:59:59
        let key2 = Partition::Date.partition_key(101, 1705363200); // 00:00:00
        assert_ne!(key1, key2);
    }
}
