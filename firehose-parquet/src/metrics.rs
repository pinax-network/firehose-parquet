use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tracing::{info, warn};

/// Labels for metrics that are grouped by table name.
#[derive(Clone, Debug, Hash, PartialEq, Eq, prometheus_client::encoding::EncodeLabelSet)]
pub struct TableLabels {
    pub table: String,
}

/// Labels for metrics that are grouped by table and partition.
#[derive(Clone, Debug, Hash, PartialEq, Eq, prometheus_client::encoding::EncodeLabelSet)]
pub struct TablePartitionLabels {
    pub table: String,
    pub partition: String,
}

/// Labels for flush trigger types.
#[derive(Clone, Debug, Hash, PartialEq, Eq, prometheus_client::encoding::EncodeLabelSet)]
pub struct FlushLabels {
    pub trigger: String,
}

/// Labels for error kinds.
#[derive(Clone, Debug, Hash, PartialEq, Eq, prometheus_client::encoding::EncodeLabelSet)]
pub struct ErrorLabels {
    pub kind: String,
}

/// All Prometheus metrics for the streaming pipeline.
#[derive(Clone)]
pub struct PipelineMetrics {
    /// Total blocks processed since start.
    pub blocks_processed_total: Counter,
    /// Blocks received below the effective start block and skipped.
    pub blocks_skipped_below_start_total: Counter,
    /// Total protobuf bytes consumed from Firehose stream.
    pub bytes_read_total: Counter,
    /// Rows written per table name.
    pub rows_written_total: Family<TableLabels, Counter>,
    /// Block number of the most recently processed block.
    pub current_block_number: Gauge,
    /// Minimum block number seen (global).
    pub min_block_number: Gauge,
    /// Maximum block number seen (global).
    pub max_block_number: Gauge,
    /// Rolling blocks/sec throughput.
    pub blocks_per_second: Gauge<f64, AtomicU64>,
    /// Rolling bytes/sec throughput.
    pub bytes_per_second: Gauge<f64, AtomicU64>,
    /// Seconds since pipeline start.
    pub elapsed_seconds: Gauge<f64, AtomicU64>,

    /// Parquet files written (labels: table, partition).
    pub files_written_total: Family<TablePartitionLabels, Counter>,
    /// Total compressed parquet bytes written to disk/S3 (labels: table).
    pub file_bytes_total: Family<TableLabels, Counter>,
    /// Flush count by trigger type.
    pub flushes_total: Family<FlushLabels, Counter>,
    /// Current in-memory buffer size (estimated compressed).
    pub buffer_estimated_bytes: Gauge,
    /// Current unresolved Solana timestamp backfill buffer size.
    pub backfill_buffer_estimated_bytes: Gauge,
    /// Current unresolved Solana timestamp backfill block count.
    pub backfill_buffered_blocks: Gauge,
    /// Current buffered row count per table.
    pub buffer_rows: Family<TableLabels, Gauge>,

    /// Number of times the cursor was persisted.
    pub cursor_saves_total: Counter,
    /// Block number from the last saved cursor.
    pub cursor_last_block_num: Gauge,

    /// Errors by kind.
    pub errors_total: Family<ErrorLabels, Counter>,
    /// Number of gRPC stream reconnections.
    pub grpc_reconnects_total: Counter,
}

impl PipelineMetrics {
    /// Create a new set of metrics and register them in the given registry.
    pub fn new(registry: &mut Registry) -> Self {
        let metrics = Self {
            blocks_processed_total: Counter::default(),
            blocks_skipped_below_start_total: Counter::default(),
            bytes_read_total: Counter::default(),
            rows_written_total: Family::default(),
            current_block_number: Gauge::default(),
            min_block_number: Gauge::default(),
            max_block_number: Gauge::default(),
            blocks_per_second: Gauge::default(),
            bytes_per_second: Gauge::default(),
            elapsed_seconds: Gauge::default(),

            files_written_total: Family::default(),
            file_bytes_total: Family::default(),
            flushes_total: Family::default(),
            buffer_estimated_bytes: Gauge::default(),
            backfill_buffer_estimated_bytes: Gauge::default(),
            backfill_buffered_blocks: Gauge::default(),
            buffer_rows: Family::default(),

            cursor_saves_total: Counter::default(),
            cursor_last_block_num: Gauge::default(),

            errors_total: Family::default(),
            grpc_reconnects_total: Counter::default(),
        };

        registry.register(
            "firehose_parquet_blocks_processed_total",
            "Total blocks processed since start",
            metrics.blocks_processed_total.clone(),
        );
        registry.register(
            "firehose_parquet_blocks_skipped_below_start_total",
            "Blocks received below the effective start block and skipped",
            metrics.blocks_skipped_below_start_total.clone(),
        );
        registry.register(
            "firehose_parquet_bytes_read_total",
            "Total protobuf bytes consumed from Firehose stream",
            metrics.bytes_read_total.clone(),
        );
        registry.register(
            "firehose_parquet_rows_written_total",
            "Rows written per table name",
            metrics.rows_written_total.clone(),
        );
        registry.register(
            "firehose_parquet_current_block_number",
            "Block number of the most recently processed block",
            metrics.current_block_number.clone(),
        );
        registry.register(
            "firehose_parquet_min_block_number",
            "Minimum block number seen (global)",
            metrics.min_block_number.clone(),
        );
        registry.register(
            "firehose_parquet_max_block_number",
            "Maximum block number seen (global)",
            metrics.max_block_number.clone(),
        );
        registry.register(
            "firehose_parquet_blocks_per_second",
            "Rolling blocks/sec throughput",
            metrics.blocks_per_second.clone(),
        );
        registry.register(
            "firehose_parquet_bytes_per_second",
            "Rolling bytes/sec throughput",
            metrics.bytes_per_second.clone(),
        );
        registry.register(
            "firehose_parquet_elapsed_seconds",
            "Seconds since pipeline start",
            metrics.elapsed_seconds.clone(),
        );

        registry.register(
            "firehose_parquet_files_written_total",
            "Parquet files written",
            metrics.files_written_total.clone(),
        );
        registry.register(
            "firehose_parquet_file_bytes_total",
            "Total compressed parquet bytes written to disk/S3",
            metrics.file_bytes_total.clone(),
        );
        registry.register(
            "firehose_parquet_flushes_total",
            "Flush count by trigger type",
            metrics.flushes_total.clone(),
        );
        registry.register(
            "firehose_parquet_buffer_estimated_bytes",
            "Current in-memory buffer size (estimated compressed)",
            metrics.buffer_estimated_bytes.clone(),
        );
        registry.register(
            "firehose_parquet_backfill_buffer_estimated_bytes",
            "Current unresolved Solana timestamp backfill buffer size (estimated)",
            metrics.backfill_buffer_estimated_bytes.clone(),
        );
        registry.register(
            "firehose_parquet_backfill_buffered_blocks",
            "Current unresolved Solana timestamp backfill block count",
            metrics.backfill_buffered_blocks.clone(),
        );
        registry.register(
            "firehose_parquet_buffer_rows",
            "Current buffered row count per table",
            metrics.buffer_rows.clone(),
        );

        registry.register(
            "firehose_parquet_cursor_saves_total",
            "Number of times the cursor was persisted",
            metrics.cursor_saves_total.clone(),
        );
        registry.register(
            "firehose_parquet_cursor_last_block_num",
            "Block number from the last saved cursor",
            metrics.cursor_last_block_num.clone(),
        );

        registry.register(
            "firehose_parquet_errors_total",
            "Errors by kind",
            metrics.errors_total.clone(),
        );
        registry.register(
            "firehose_parquet_grpc_reconnects_total",
            "Number of gRPC stream reconnections",
            metrics.grpc_reconnects_total.clone(),
        );

        metrics
    }
}

/// Register the `firehose_parquet_info` gauge with endpoint metadata labels.
pub fn register_info_metric(registry: &mut Registry, labels: Vec<(String, String)>) {
    let info = prometheus_client::metrics::info::Info::new(labels);
    registry.register("firehose_parquet", "Pipeline metadata", info);
}

/// Initialize a Prometheus metrics registry and pipeline metrics.
pub fn init() -> (Registry, PipelineMetrics) {
    let mut registry = Registry::default();
    let metrics = PipelineMetrics::new(&mut registry);
    (registry, metrics)
}

/// Spawn a lightweight HTTP server that serves `/metrics` on the given port.
///
/// Binds to `0.0.0.0:<port>` and returns immediately. The server runs in the
/// background as a tokio task until the runtime shuts down.
pub fn serve(registry: Arc<Registry>, port: u16) {
    tokio::spawn(async move {
        let addr = format!("0.0.0.0:{port}");
        let listener = match TcpListener::bind(&addr).await {
            Ok(l) => {
                info!(addr = %addr, "Prometheus metrics server listening");
                l
            }
            Err(e) => {
                warn!(error = %e, addr = %addr, "failed to bind metrics server");
                return;
            }
        };

        loop {
            let (mut stream, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    warn!(error = %e, "metrics server accept error");
                    continue;
                }
            };

            let registry = Arc::clone(&registry);
            tokio::spawn(async move {
                // Apply a timeout to the entire request handling to prevent
                // slow/idle connections from holding resources.
                let result = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    handle_request(&mut stream, &registry),
                )
                .await;
                if result.is_err() {
                    let _ = stream.shutdown().await;
                }
            });
        }
    });
}

async fn handle_request(stream: &mut tokio::net::TcpStream, registry: &Registry) {
    // Read the request (we don't parse it fully — just drain input).
    let mut buf = [0u8; 4096];
    let request_line = match tokio::io::AsyncReadExt::read(stream, &mut buf).await {
        Ok(n) if n > 0 => String::from_utf8_lossy(&buf[..n.min(256)]).to_string(),
        _ => String::new(),
    };

    // Check if the request is for /metrics or /health.
    let path = request_line
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/");

    let response = if path == "/metrics" {
        let mut body = String::new();
        if encode(&mut body, registry).is_err() {
            let error_body = "# error encoding metrics\n";
            format!(
                "HTTP/1.1 500 Internal Server Error\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
                error_body.len(),
                error_body,
            )
        } else {
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body,
            )
        }
    } else if path == "/health" || path == "/ready" {
        let body = "OK\n";
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body,
        )
    } else {
        let body = "Not Found\n";
        format!(
            "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body,
        )
    };

    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_creates_metrics() {
        let (_, metrics) = init();
        // Verify counters start at 0.
        assert_eq!(metrics.blocks_processed_total.get(), 0);
        assert_eq!(metrics.bytes_read_total.get(), 0);
        assert_eq!(metrics.grpc_reconnects_total.get(), 0);
        assert_eq!(metrics.cursor_saves_total.get(), 0);
    }

    #[test]
    fn test_counter_increment() {
        let (_, metrics) = init();
        metrics.blocks_processed_total.inc();
        metrics.blocks_processed_total.inc();
        assert_eq!(metrics.blocks_processed_total.get(), 2);
    }

    #[test]
    fn test_gauge_set() {
        let (_, metrics) = init();
        metrics.current_block_number.set(42);
        assert_eq!(metrics.current_block_number.get(), 42);
    }

    #[test]
    fn test_family_counter() {
        let (_, metrics) = init();
        let labels = TableLabels {
            table: "blocks".to_string(),
        };
        metrics
            .rows_written_total
            .get_or_create(&labels)
            .inc_by(100);
        assert_eq!(metrics.rows_written_total.get_or_create(&labels).get(), 100);
    }

    #[test]
    fn test_flush_labels() {
        let (_, metrics) = init();
        let labels = FlushLabels {
            trigger: "bytes".to_string(),
        };
        metrics.flushes_total.get_or_create(&labels).inc();
        assert_eq!(metrics.flushes_total.get_or_create(&labels).get(), 1);
    }

    #[test]
    fn test_encode_metrics() {
        let (registry, metrics) = init();
        metrics.blocks_processed_total.inc_by(42);
        metrics.current_block_number.set(1000);

        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();

        assert!(buf.contains("firehose_parquet_blocks_processed_total"));
        assert!(buf.contains("firehose_parquet_current_block_number"));
    }

    #[test]
    fn test_info_metric() {
        let mut registry = Registry::default();
        let labels = vec![
            ("chain_name".to_string(), "eth-mainnet".to_string()),
            ("version".to_string(), "0.2.5".to_string()),
        ];
        register_info_metric(&mut registry, labels);

        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();

        assert!(buf.contains("firehose_parquet_info"));
        assert!(buf.contains("eth-mainnet"));
    }

    #[tokio::test]
    async fn test_serve_and_fetch_metrics() {
        let (registry, metrics) = init();
        metrics.blocks_processed_total.inc_by(123);

        let registry = Arc::new(registry);

        // Use port 0 to pick a random available port.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        serve(Arc::clone(&registry), port);

        // Give the server a moment to start.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Connect and make a request.
        let mut stream = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();

        let mut response = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut response)
            .await
            .unwrap();
        let response_str = String::from_utf8_lossy(&response);

        assert!(response_str.contains("200 OK"));
        assert!(response_str.contains("firehose_parquet_blocks_processed_total"));
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let (registry, _) = init();
        let registry = Arc::new(registry);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        serve(Arc::clone(&registry), port);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let mut stream = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        stream
            .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();

        let mut response = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut response)
            .await
            .unwrap();
        let response_str = String::from_utf8_lossy(&response);

        assert!(response_str.contains("200 OK"));
        assert!(response_str.contains("OK"));
    }
}
