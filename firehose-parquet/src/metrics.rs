use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tracing::{info, warn};

/// Labels for metrics that are grouped by table name.
#[derive(Clone, Debug, Hash, PartialEq, Eq, prometheus_client::encoding::EncodeLabelSet)]
pub struct TableLabels {
    pub table: String,
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

struct StreamActivity {
    started: Instant,
    stale_after: Duration,
    connected: bool,
    last_message: Option<Instant>,
    finished: bool,
}

impl StreamActivity {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            stale_after: Duration::from_secs(120),
            connected: false,
            last_message: None,
            finished: false,
        }
    }

    fn readiness_error(&self, now: Instant) -> Option<&'static str> {
        if self.finished {
            Some("stream stopped")
        } else if !self.connected {
            Some("stream disconnected or starting")
        } else if let Some(last) = self.last_message {
            (now.saturating_duration_since(last) >= self.stale_after)
                .then_some("stream message freshness threshold exceeded")
        } else {
            Some("waiting for first stream message")
        }
    }
}

/// Marks a stream disconnected on every return path, including cancellation.
/// A pipeline may still be committing its final output after this guard drops.
pub struct StreamActivityGuard(PipelineMetrics);

impl Drop for StreamActivityGuard {
    fn drop(&mut self) {
        let mut state = self.0.activity.lock().unwrap_or_else(|e| e.into_inner());
        state.connected = false;
    }
}

/// Holds liveness through the pipeline's final output and cursor commit.
pub struct PipelineActivityGuard(PipelineMetrics);

impl Drop for PipelineActivityGuard {
    fn drop(&mut self) {
        let mut state = self.0.activity.lock().unwrap_or_else(|e| e.into_inner());
        state.connected = false;
        state.finished = true;
    }
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
    /// Seconds since pipeline start.
    pub elapsed_seconds: Gauge<f64, AtomicU64>,
    /// Unix timestamp of the last valid streamed block; NaN when absent.
    pub last_block_timestamp_seconds: Gauge<f64, AtomicU64>,
    /// Wall-clock age of that timestamp, not a measured remote chain head lag.
    pub block_time_lag_seconds: Gauge<f64, AtomicU64>,
    /// Monotonic seconds since the last valid stream message; NaN before one.
    pub last_message_age_seconds: Gauge<f64, AtomicU64>,
    activity: Arc<Mutex<StreamActivity>>,

    /// Parquet files written (labels: table only).
    pub files_written_total: Family<TableLabels, Counter>,
    /// Total compressed parquet bytes written to disk/S3 (labels: table).
    pub file_bytes_total: Family<TableLabels, Counter>,
    /// Flush count by trigger type.
    pub flushes_total: Family<FlushLabels, Counter>,
    /// Current writer-owned buffer size (estimated compressed).
    pub buffer_estimated_bytes: Gauge,
    /// Current writer-owned row count per table.
    pub buffer_rows: Family<TableLabels, Gauge>,
    /// Current mapper-owned row count, summed across tables.
    pub mapper_buffer_rows: Gauge,
    /// Estimated Arrow bytes in the largest mapper table, used by flush limits.
    pub mapper_largest_table_estimated_bytes: Gauge,
    /// Raw protobuf blocks awaiting a genesis timestamp anchor.
    pub bootstrap_buffered_blocks: Gauge,
    pub bootstrap_buffered_bytes: Gauge,

    /// Number of times the cursor was persisted.
    pub cursor_saves_total: Counter,
    /// Failed cursor persistence attempts, including failures recovered by retry.
    pub cursor_save_failures_total: Counter,
    /// Unix timestamp of the last successful save in this process, or zero.
    pub cursor_last_success_timestamp_seconds: Gauge,
    /// Block number from the last saved cursor.
    pub cursor_last_block_num: Gauge,

    /// Errors by kind.
    pub errors_total: Family<ErrorLabels, Counter>,
    /// Number of gRPC stream reconnections.
    pub grpc_reconnects_total: Counter,
}

impl PipelineMetrics {
    pub fn begin_pipeline(&self) -> PipelineActivityGuard {
        self.activity
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .finished = false;
        PipelineActivityGuard(self.clone())
    }
    pub fn set_readiness_timeout(&self, timeout: Duration) {
        self.activity
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .stale_after = timeout;
    }

    pub fn begin_stream(&self) -> StreamActivityGuard {
        let mut state = self.activity.lock().unwrap_or_else(|e| e.into_inner());
        state.connected = false;
        state.last_message = None;
        StreamActivityGuard(self.clone())
    }

    pub fn stream_disconnected(&self) {
        self.activity
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .connected = false;
    }

    pub fn record_stream_message(&self, timestamp: Option<f64>) {
        let mut state = self.activity.lock().unwrap_or_else(|e| e.into_inner());
        state.connected = true;
        state.last_message = Some(Instant::now());
        self.last_block_timestamp_seconds
            .set(timestamp.unwrap_or(f64::NAN));
    }

    pub fn is_ready(&self) -> bool {
        self.activity
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .readiness_error(Instant::now())
            .is_none()
    }

    pub fn record_mapper_buffer(&self, rows: usize, largest_table_bytes: usize) {
        self.mapper_buffer_rows
            .set(i64::try_from(rows).unwrap_or(i64::MAX));
        self.mapper_largest_table_estimated_bytes
            .set(i64::try_from(largest_table_bytes).unwrap_or(i64::MAX));
    }

    fn refresh(&self, now: Instant, unix_seconds: f64) {
        let state = self.activity.lock().unwrap_or_else(|e| e.into_inner());
        self.elapsed_seconds
            .set(now.saturating_duration_since(state.started).as_secs_f64());
        self.last_message_age_seconds
            .set(state.last_message.map_or(f64::NAN, |last| {
                now.saturating_duration_since(last).as_secs_f64()
            }));
        let timestamp = self.last_block_timestamp_seconds.get();
        self.block_time_lag_seconds.set(if timestamp.is_finite() {
            (unix_seconds - timestamp).max(0.0)
        } else {
            f64::NAN
        });
    }

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
            elapsed_seconds: Gauge::default(),
            last_block_timestamp_seconds: Gauge::default(),
            block_time_lag_seconds: Gauge::default(),
            last_message_age_seconds: Gauge::default(),
            activity: Arc::new(Mutex::new(StreamActivity::new())),

            files_written_total: Family::default(),
            file_bytes_total: Family::default(),
            flushes_total: Family::default(),
            buffer_estimated_bytes: Gauge::default(),
            buffer_rows: Family::default(),
            mapper_buffer_rows: Gauge::default(),
            mapper_largest_table_estimated_bytes: Gauge::default(),
            bootstrap_buffered_blocks: Gauge::default(),
            bootstrap_buffered_bytes: Gauge::default(),

            cursor_saves_total: Counter::default(),
            cursor_save_failures_total: Counter::default(),
            cursor_last_success_timestamp_seconds: Gauge::default(),
            cursor_last_block_num: Gauge::default(),

            errors_total: Family::default(),
            grpc_reconnects_total: Counter::default(),
        };

        registry.register(
            "firehose_parquet_blocks_processed",
            "Total blocks processed since start",
            metrics.blocks_processed_total.clone(),
        );
        registry.register(
            "firehose_parquet_blocks_skipped_below_start",
            "Blocks received below the effective start block and skipped",
            metrics.blocks_skipped_below_start_total.clone(),
        );
        registry.register(
            "firehose_parquet_bytes_read",
            "Total protobuf bytes consumed from Firehose stream",
            metrics.bytes_read_total.clone(),
        );
        registry.register(
            "firehose_parquet_rows_written",
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
            "firehose_parquet_elapsed_seconds",
            "Seconds since pipeline start",
            metrics.elapsed_seconds.clone(),
        );
        for (name, help, gauge) in [
            ("firehose_parquet_last_block_timestamp_seconds", "Unix timestamp of the last valid streamed block; NaN when its timestamp is absent", &metrics.last_block_timestamp_seconds),
            ("firehose_parquet_block_time_lag_seconds", "Wall-clock age of the last streamed block timestamp, clamped at zero; not measured remote head lag", &metrics.block_time_lag_seconds),
            ("firehose_parquet_last_message_age_seconds", "Monotonic seconds since the last valid stream message; NaN before the first message", &metrics.last_message_age_seconds),
        ] {
            gauge.set(f64::NAN);
            registry.register(name, help, gauge.clone());
        }

        registry.register(
            "firehose_parquet_files_written",
            "Parquet files written",
            metrics.files_written_total.clone(),
        );
        registry.register(
            "firehose_parquet_file_bytes",
            "Total compressed parquet bytes written to disk/S3",
            metrics.file_bytes_total.clone(),
        );
        registry.register(
            "firehose_parquet_flushes",
            "Flush count by trigger type",
            metrics.flushes_total.clone(),
        );
        registry.register(
            "firehose_parquet_buffer_estimated_bytes",
            "Current writer-owned buffer size (estimated compressed)",
            metrics.buffer_estimated_bytes.clone(),
        );
        registry.register(
            "firehose_parquet_buffer_rows",
            "Current writer-owned buffered row count per table",
            metrics.buffer_rows.clone(),
        );
        for (name, help, gauge) in [
            (
                "firehose_parquet_mapper_buffer_rows",
                "Current mapper-owned rows summed across all tables",
                &metrics.mapper_buffer_rows,
            ),
            (
                "firehose_parquet_mapper_largest_table_estimated_bytes",
                "Estimated Arrow bytes in the largest mapper table; not total process memory",
                &metrics.mapper_largest_table_estimated_bytes,
            ),
            (
                "firehose_parquet_bootstrap_buffered_blocks",
                "Raw blocks awaiting a genesis timestamp anchor",
                &metrics.bootstrap_buffered_blocks,
            ),
            (
                "firehose_parquet_bootstrap_buffered_bytes",
                "Raw protobuf bytes awaiting a genesis timestamp anchor",
                &metrics.bootstrap_buffered_bytes,
            ),
        ] {
            registry.register(name, help, gauge.clone());
        }

        registry.register(
            "firehose_parquet_cursor_saves",
            "Number of times the cursor was persisted",
            metrics.cursor_saves_total.clone(),
        );
        registry.register(
            "firehose_parquet_cursor_save_failures",
            "Failed cursor persistence attempts, including failures recovered by retry",
            metrics.cursor_save_failures_total.clone(),
        );
        registry.register(
            "firehose_parquet_cursor_last_success_timestamp_seconds",
            "Unix timestamp of the last successful cursor save in this process, or zero before the first save",
            metrics.cursor_last_success_timestamp_seconds.clone(),
        );
        registry.register(
            "firehose_parquet_cursor_last_block_num",
            "Block number from the last saved cursor",
            metrics.cursor_last_block_num.clone(),
        );

        registry.register(
            "firehose_parquet_errors",
            "Errors by kind",
            metrics.errors_total.clone(),
        );
        registry.register(
            "firehose_parquet_grpc_reconnects",
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
pub fn serve(registry: Arc<Registry>, metrics: PipelineMetrics, port: u16) {
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
            let metrics = metrics.clone();
            tokio::spawn(async move {
                // Apply a timeout to the entire request handling to prevent
                // slow/idle connections from holding resources.
                let result = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    handle_request(&mut stream, &registry, &metrics),
                )
                .await;
                if result.is_err() {
                    let _ = stream.shutdown().await;
                }
            });
        }
    });
}

async fn handle_request(
    stream: &mut tokio::net::TcpStream,
    registry: &Registry,
    metrics: &PipelineMetrics,
) {
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
        metrics.refresh(
            Instant::now(),
            time::OffsetDateTime::now_utc().unix_timestamp_nanos() as f64 / 1_000_000_000.0,
        );
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
        let state = metrics.activity.lock().unwrap_or_else(|e| e.into_inner());
        let error = if path == "/ready" {
            state.readiness_error(Instant::now())
        } else {
            state.finished.then_some("stream stopped")
        };
        let status = if error.is_some() {
            "503 Service Unavailable"
        } else {
            "200 OK"
        };
        let body = format!("{}\n", error.unwrap_or("OK"));
        format!(
            "HTTP/1.1 {}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
            status,
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
        assert!(buf.contains("firehose_parquet_blocks_processed_total 42\n"));
        assert!(!buf.contains("_total_total"));
        for removed in ["blocks_per_second", "bytes_per_second", "backfill_buffer"] {
            assert!(!buf.contains(removed));
        }
    }

    #[test]
    fn activity_uses_monotonic_freshness_and_preserves_unknown_block_time() {
        let (_, metrics) = init();
        let activity = metrics.begin_stream();
        let now = Instant::now();
        metrics.refresh(now, 10.0);
        assert!(metrics.last_message_age_seconds.get().is_nan());
        assert!(metrics.block_time_lag_seconds.get().is_nan());
        metrics.record_stream_message(Some(-0.5));
        metrics.refresh(Instant::now(), 10.0);
        assert_eq!(metrics.last_block_timestamp_seconds.get(), -0.5);
        assert_eq!(metrics.block_time_lag_seconds.get(), 10.5);
        metrics.refresh(Instant::now(), -10.0);
        assert_eq!(metrics.block_time_lag_seconds.get(), 0.0);
        metrics.record_stream_message(None);
        assert!(metrics.is_ready());
        metrics.refresh(Instant::now(), 10.0);
        assert!(metrics.last_block_timestamp_seconds.get().is_nan());
        assert!(metrics.block_time_lag_seconds.get().is_nan());
        {
            let mut state = metrics.activity.lock().unwrap();
            state.last_message = Some(now);
            state.stale_after = Duration::from_secs(2);
            assert!(state
                .readiness_error(now + Duration::from_secs(1))
                .is_none());
            assert!(state
                .readiness_error(now + Duration::from_secs(2))
                .is_some());
        }
        drop(activity);
        assert!(!metrics.is_ready());
    }

    async fn http_response(
        registry: Arc<Registry>,
        metrics: PipelineMetrics,
        path: &str,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handler = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handle_request(&mut stream, &registry, &metrics).await;
        });
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream
            .write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut response = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut stream, &mut response)
            .await
            .unwrap();
        handler.await.unwrap();
        response
    }

    #[tokio::test]
    async fn readiness_http_reports_startup_fresh_stale_reconnect_and_stopped_states() {
        let (registry, metrics) = init();
        let registry = Arc::new(registry);
        let pipeline = metrics.begin_pipeline();
        assert!(http_response(registry.clone(), metrics.clone(), "/ready")
            .await
            .starts_with("HTTP/1.1 503"));
        assert!(http_response(registry.clone(), metrics.clone(), "/health")
            .await
            .starts_with("HTTP/1.1 200"));
        let active = metrics.begin_stream();
        metrics.record_stream_message(Some(1.0));
        assert!(http_response(registry.clone(), metrics.clone(), "/ready")
            .await
            .starts_with("HTTP/1.1 200"));
        metrics.stream_disconnected();
        assert!(http_response(registry.clone(), metrics.clone(), "/ready")
            .await
            .starts_with("HTTP/1.1 503"));
        metrics.record_stream_message(None);
        assert!(http_response(registry.clone(), metrics.clone(), "/ready")
            .await
            .starts_with("HTTP/1.1 200"));
        {
            let mut state = metrics.activity.lock().unwrap();
            state.last_message = Some(Instant::now() - Duration::from_secs(121));
        }
        assert!(http_response(registry.clone(), metrics.clone(), "/ready")
            .await
            .contains("503 Service Unavailable"));
        assert!(http_response(registry.clone(), metrics.clone(), "/health")
            .await
            .starts_with("HTTP/1.1 200"));
        let scrape = http_response(registry.clone(), metrics.clone(), "/metrics").await;
        assert!(scrape.starts_with("HTTP/1.1 200"));
        assert!(metrics.last_message_age_seconds.get() >= 121.0);
        drop(active);
        assert!(http_response(registry.clone(), metrics.clone(), "/ready")
            .await
            .contains("503 Service Unavailable"));
        assert!(http_response(registry.clone(), metrics.clone(), "/health")
            .await
            .starts_with("HTTP/1.1 200"));
        drop(pipeline);
        for path in ["/ready", "/health"] {
            assert!(http_response(registry.clone(), metrics.clone(), path)
                .await
                .contains("503 Service Unavailable"));
        }
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

        serve(Arc::clone(&registry), metrics.clone(), port);

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
        let (registry, metrics) = init();
        let registry = Arc::new(registry);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        serve(Arc::clone(&registry), metrics.clone(), port);
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
