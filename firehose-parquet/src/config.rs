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
    pub public: bool,
    pub output: PathBuf,
    pub partition: Partition,
    pub flush_rows: Option<u32>,
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
}

impl std::fmt::Display for Partition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Partition::None => write!(f, "none"),
            Partition::BlockRange(size) => write!(f, "block_range({size})"),
            Partition::Date => write!(f, "date"),
            Partition::Hour => write!(f, "hour"),
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
        let auth = if self.public {
            "public"
        } else if self.api_key.is_some() {
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

        let flush_bytes = if self.flush_bytes >= 1024 * 1024 * 1024 {
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
        writeln!(f, "  stop_block         {stop}")?;
        if let Some(ref path) = self.cursor_path {
            writeln!(f, "  cursor             {}", path.display())?;
        }
        writeln!(f, "  output             {}", self.output.display())?;
        writeln!(f, "  partition          {}", self.partition)?;
        writeln!(f, "  compression        {}", self.compression)?;
        if let Some(rows) = self.flush_rows {
            writeln!(f, "  flush_rows         {rows}")?;
        }
        writeln!(f, "  flush_bytes        {flush_bytes}")?;
        if let Some(secs) = self.flush_interval_secs {
            writeln!(f, "  flush_interval     {secs}s")?;
        }
        writeln!(f, "  final_blocks_only  {}", self.final_blocks_only)?;
        if self.dry_run {
            writeln!(f, "  dry_run            true")?;
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
            cursor_path: None,
            public: false,
            output: PathBuf::from("output"),
            partition: Partition::None,
            flush_rows: None,
            flush_bytes: 128 * 1024 * 1024, // 128 MB
            flush_interval_secs: None,
            compression: Compression::Zstd,
            final_blocks_only: true,
            dry_run: false,
            aws_access_key_id: None,
            aws_secret_access_key: None,
            aws_session_token: None,
            aws_region: None,
            aws_endpoint_url: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_display() {
        assert_eq!(Partition::None.to_string(), "none");
        assert_eq!(Partition::BlockRange(10000).to_string(), "block_range(10000)");
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
        assert!(display.contains("stop_block         stream forever"));
        assert!(display.contains("partition          none"));
        assert!(display.contains("compression        zstd"));
        assert!(!display.contains("flush_rows"));
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
    fn test_config_display_public_auth() {
        let config = Config {
            public: true,
            ..Config::default()
        };
        let display = config.to_string();
        assert!(display.contains("auth               public"));
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
        assert!(display.contains("stop_block         200"));
    }

    #[test]
    fn test_config_display_stop_block_zero() {
        let config = Config {
            stop_block: Some(0),
            ..Config::default()
        };
        let display = config.to_string();
        assert!(display.contains("stop_block         stream forever"));
    }

    #[test]
    fn test_config_display_with_cursor() {
        let config = Config {
            cursor_path: Some(PathBuf::from("cursor.txt")),
            ..Config::default()
        };
        let display = config.to_string();
        assert!(display.contains("cursor             cursor.txt"));
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
    fn test_config_display_dry_run() {
        let config = Config {
            dry_run: true,
            ..Config::default()
        };
        let display = config.to_string();
        assert!(display.contains("dry_run            true"));
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
    }
}
