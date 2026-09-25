use crate::config::Config;
use crate::metrics::PipelineMetrics;
use crate::traits::BlockIdentity;
use anyhow::{Context, Result};
use backoff::backoff::Backoff;
use backoff::ExponentialBackoffBuilder;
use firehose_protos::firehose;
use std::time::{Duration, Instant};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tracing::{debug, info, warn};

const FIREHOSE_TCP_KEEPALIVE: Duration = Duration::from_secs(30);
const FIREHOSE_HTTP2_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const FIREHOSE_HTTP2_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EndpointKeepaliveSettings {
    tcp_keepalive: Duration,
    http2_keep_alive_interval: Duration,
    keep_alive_timeout: Duration,
    keep_alive_while_idle: bool,
}

/// Information about the Firehose endpoint, returned by the `EndpointInfo/Info` RPC.
#[derive(Debug, Clone)]
pub struct EndpointInfo {
    pub chain_name: String,
    pub chain_name_aliases: Vec<String>,
    pub first_streamable_block_num: u64,
    pub first_streamable_block_id: String,
    pub block_id_encoding: i32,
    pub block_features: Vec<String>,
}

/// A thin wrapper around the Firehose v2 gRPC `Stream` client that handles
/// connection, authentication, and streaming with automatic retry/resume.
pub struct FirehoseClient {
    config: Config,
    auth: AuthMetadata,
    metrics: Option<PipelineMetrics>,
}

/// gRPC authentication metadata, parsed once from the configured API key and
/// JWT token.
#[derive(Clone, Default)]
struct AuthMetadata {
    api_key: Option<MetadataValue<Ascii>>,
    bearer: Option<MetadataValue<Ascii>>,
}

impl AuthMetadata {
    /// Trim the credentials (a secret file often ends with a newline) and
    /// parse them into header values. Blank credentials are ignored.
    fn from_config(config: &Config) -> Result<Self> {
        let parse = |value: &Option<String>, format: fn(&str) -> String, what: &str| {
            value
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| {
                    MetadataValue::try_from(format(value)).map_err(|_| {
                        anyhow::anyhow!(
                            "{what} contains characters that are not allowed in a gRPC header (control characters or line breaks inside the value)"
                        )
                    })
                })
                .transpose()
        };
        Ok(Self {
            api_key: parse(&config.api_key, str::to_string, "the API key")?,
            bearer: parse(
                &config.jwt_token,
                |token| format!("Bearer {token}"),
                "the JWT token",
            )?,
        })
    }

    fn apply<T>(&self, request: &mut tonic::Request<T>) {
        if let Some(ref key) = self.api_key {
            request.metadata_mut().insert("x-api-key", key.clone());
        }
        if let Some(ref bearer) = self.bearer {
            request
                .metadata_mut()
                .insert("authorization", bearer.clone());
        }
    }
}

impl FirehoseClient {
    /// Create a client. Fails if the API key or JWT token cannot be sent as a
    /// gRPC header.
    pub fn new(config: Config) -> Result<Self> {
        let auth = AuthMetadata::from_config(&config)?;
        Ok(Self {
            config,
            auth,
            metrics: None,
        })
    }

    /// Set the pipeline metrics for Prometheus instrumentation.
    pub fn set_metrics(&mut self, metrics: PipelineMetrics) {
        self.metrics = Some(metrics);
    }

    fn endpoint_keepalive_settings() -> EndpointKeepaliveSettings {
        EndpointKeepaliveSettings {
            tcp_keepalive: FIREHOSE_TCP_KEEPALIVE,
            http2_keep_alive_interval: FIREHOSE_HTTP2_KEEPALIVE_INTERVAL,
            keep_alive_timeout: FIREHOSE_HTTP2_KEEPALIVE_TIMEOUT,
            keep_alive_while_idle: true,
        }
    }

    fn endpoint(&self) -> Result<Endpoint> {
        let uri = self.config.endpoint.clone();
        let keepalive = Self::endpoint_keepalive_settings();
        Endpoint::from_shared(uri.clone())
            .with_context(|| format!("invalid endpoint URI: {uri}"))
            .map(|endpoint| {
                endpoint
                    .timeout(Duration::from_secs(300))
                    .connect_timeout(Duration::from_secs(30))
                    .tcp_keepalive(Some(keepalive.tcp_keepalive))
                    .http2_keep_alive_interval(keepalive.http2_keep_alive_interval)
                    .keep_alive_timeout(keepalive.keep_alive_timeout)
                    .keep_alive_while_idle(keepalive.keep_alive_while_idle)
            })
    }

    /// Build a tonic channel to the configured endpoint.
    async fn connect_with_log(&self, log_connect: bool) -> Result<Channel> {
        let uri = self.config.endpoint.clone();
        let mut endpoint = self.endpoint()?;

        // Determine if TLS should be used based on the URL scheme.
        // https:// → TLS enabled; http:// → plaintext (no TLS).
        let use_tls = self.config.endpoint.starts_with("https");

        if use_tls {
            endpoint = endpoint.tls_config(ClientTlsConfig::new().with_native_roots())?;
        }

        let channel = endpoint
            .connect()
            .await
            .with_context(|| format!("connecting to {uri}"))?;
        if log_connect {
            info!(endpoint = %uri, tls = use_tls, "connected to Firehose");
        } else {
            debug!(endpoint = %uri, tls = use_tls, "connected to Firehose for sparse probe");
        }
        Ok(channel)
    }

    async fn connect(&self) -> Result<Channel> {
        self.connect_with_log(true).await
    }

    /// Verify that the configured Firehose endpoint is reachable before
    /// startup relies on endpoint metadata or begins streaming.
    pub async fn healthcheck(&self) -> Result<()> {
        let uri = self.config.endpoint.clone();
        self.connect_with_log(false)
            .await
            .with_context(|| format!("Firehose endpoint `{uri}` is unavailable or unhealthy"))?;
        Ok(())
    }

    /// Fetch endpoint information from the `EndpointInfo/Info` RPC.
    ///
    /// Returns `None` if the endpoint does not support this RPC
    /// (e.g. older servers), logging a warning instead of failing.
    pub async fn info(&self) -> Option<EndpointInfo> {
        let channel = match self.connect().await {
            Ok(ch) => ch,
            Err(e) => {
                warn!(error = %e, "failed to connect for EndpointInfo; skipping");
                return None;
            }
        };

        let mut client = firehose::endpoint_info_client::EndpointInfoClient::new(channel)
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
            .max_decoding_message_size(128 * 1024 * 1024);

        let mut request = tonic::Request::new(firehose::InfoRequest {});
        self.auth.apply(&mut request);

        match client.info(request).await {
            Ok(resp) => {
                let r = resp.into_inner();
                let ei = EndpointInfo {
                    chain_name: r.chain_name,
                    chain_name_aliases: r.chain_name_aliases,
                    first_streamable_block_num: r.first_streamable_block_num,
                    first_streamable_block_id: r.first_streamable_block_id,
                    block_id_encoding: r.block_id_encoding,
                    block_features: r.block_features,
                };
                info!(
                    chain_name = %ei.chain_name,
                    block_id_encoding = ei.block_id_encoding,
                    block_features = ?ei.block_features,
                    "received endpoint info"
                );
                Some(ei)
            }
            Err(e) => {
                warn!(error = %e, "EndpointInfo/Info RPC not available; skipping");
                None
            }
        }
    }

    pub async fn fetch_block_identity(
        &self,
        block_num: u64,
        wait_timeout: Option<Duration>,
    ) -> Result<Option<BlockIdentity>> {
        let fetch = async {
            let channel = self.connect_with_log(false).await?;
            let mut client = firehose::fetch_client::FetchClient::new(channel)
                .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                .max_decoding_message_size(128 * 1024 * 1024);

            let req = firehose::SingleBlockRequest {
                transforms: vec![],
                reference: Some(firehose::single_block_request::Reference::BlockNumber(
                    firehose::single_block_request::BlockNumber { num: block_num },
                )),
            };

            let mut request = tonic::Request::new(req);
            self.auth.apply(&mut request);

            let response = client.block(request).await?.into_inner();
            Ok(response.metadata.as_ref().map(|m| BlockIdentity {
                block_num: m.num,
                block_id: m.id.clone(),
                parent_num: m.parent_num,
                parent_id: m.parent_id.clone(),
                lib_num: m.lib_num,
                timestamp: m.time.as_ref().map_or(0, |t| t.seconds),
                timestamp_nanos: m.time.as_ref().map_or(0, |t| t.nanos),
                fork_step: None,
            }))
        };

        match wait_timeout {
            Some(timeout) => match tokio::time::timeout(timeout, fetch).await {
                Ok(result) => result,
                Err(_) => Ok(None),
            },
            None => fetch.await,
        }
    }

    /// Start streaming blocks. Calls `handler` for every block received as
    /// raw bytes (the `Any.value` field). The handler receives the raw bytes
    /// and the cursor string. When `initial_cursor` is provided, the stream
    /// resumes from that cursor; otherwise it starts fresh from
    /// `self.config.start_block`.
    ///
    /// On stream errors the client will retry with exponential back-off
    /// and resume from the last cursor. A clean end of stream is handled by
    /// [`clean_end_action`]: live streams reconnect, and a bounded stream that
    /// ends before its last requested block is resumed to confirm the range
    /// has no more blocks.
    ///
    /// Returns when a bounded stream is exhausted or an unrecoverable error
    /// occurs. Callers should still check which blocks were received, since
    /// an exhausted range can end below `stop_block - 1` (e.g. skipped slots).
    pub async fn stream_blocks<F>(
        &self,
        initial_cursor: Option<String>,
        mut handler: F,
    ) -> Result<()>
    where
        F: FnMut(Vec<u8>, String, String, BlockIdentity, i32) -> Result<()>,
    {
        let mut cursor = initial_cursor;
        let mut reconnect_backoff = ExponentialBackoffBuilder::default()
            .with_initial_interval(Duration::from_secs(1))
            .with_max_interval(Duration::from_secs(60))
            .with_max_elapsed_time(None) // retry forever
            .build();
        // A timeout of 0 means disabled.
        let stream_idle_timeout = self
            .config
            .stream_idle_timeout_secs
            .filter(|secs| *secs > 0)
            .map(Duration::from_secs);
        let reconnect_stall_timeout = self
            .config
            .reconnect_stall_timeout_secs
            .filter(|secs| *secs > 0)
            .map(Duration::from_secs);

        let mut attempt = 0u64;
        let mut reconnect_stall_started_at: Option<Instant> = None;
        // Highest block number received so far, across reconnects.
        let mut last_block_num: Option<u64> = None;
        // Set once a bounded stream ended before its last requested block.
        let mut resumed_after_early_end = false;

        loop {
            attempt += 1;
            let mut blocks_this_connection = 0u64;
            let channel = match self.connect().await {
                Ok(ch) => {
                    attempt = 0;
                    ch
                }
                Err(e) => {
                    let stall_elapsed = reconnect_stall_started_at
                        .get_or_insert_with(Instant::now)
                        .elapsed();
                    if let Some(max_stall) = reconnect_stall_timeout {
                        if stall_elapsed >= max_stall {
                            return Err(anyhow::anyhow!(
                                "reconnect stalled for {:?} (limit {:?}) after {} attempts; last error: {}",
                                stall_elapsed,
                                max_stall,
                                attempt,
                                e
                            ));
                        }
                    }
                    let wait = reconnect_backoff
                        .next_backoff()
                        .unwrap_or(Duration::from_secs(60));
                    warn!(
                        attempt,
                        error = %e,
                        retry_in = ?wait,
                        reconnect_stall_elapsed = ?stall_elapsed,
                        "connection failed, retrying"
                    );
                    if let Some(ref m) = self.metrics {
                        m.grpc_reconnects_total.inc();
                        m.errors_total
                            .get_or_create(&crate::metrics::ErrorLabels {
                                kind: "grpc_reconnect".to_string(),
                            })
                            .inc();
                    }
                    tokio::time::sleep(wait).await;
                    continue;
                }
            };

            let mut client = firehose::stream_client::StreamClient::new(channel)
                .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                .max_decoding_message_size(128 * 1024 * 1024);

            let start_block_num = match &cursor {
                Some(_) => self.config.start_block.unwrap_or(0) as i64,
                None => self.config.start_block.unwrap_or(0) as i64,
            };

            let req = firehose::Request {
                start_block_num,
                cursor: cursor.clone().unwrap_or_default(),
                stop_block_num: firehose_stop_block_num(self.config.stop_block),
                final_blocks_only: self.config.final_blocks_only,
                transforms: vec![],
            };

            let mut request = tonic::Request::new(req);
            self.auth.apply(&mut request);

            let stream = match client.blocks(request).await {
                Ok(resp) => {
                    reconnect_backoff.reset();
                    reconnect_stall_started_at = None;
                    resp.into_inner()
                }
                Err(e) => {
                    let stall_elapsed = reconnect_stall_started_at
                        .get_or_insert_with(Instant::now)
                        .elapsed();
                    if let Some(max_stall) = reconnect_stall_timeout {
                        if stall_elapsed >= max_stall {
                            return Err(anyhow::anyhow!(
                                "reconnect stalled for {:?} (limit {:?}) after {} attempts; last error: {}",
                                stall_elapsed,
                                max_stall,
                                attempt,
                                e
                            ));
                        }
                    }
                    let wait = reconnect_backoff
                        .next_backoff()
                        .unwrap_or(Duration::from_secs(60));
                    warn!(
                        attempt,
                        error = %e,
                        retry_in = ?wait,
                        reconnect_stall_elapsed = ?stall_elapsed,
                        "Blocks RPC failed, will retry"
                    );
                    if let Some(ref m) = self.metrics {
                        m.grpc_reconnects_total.inc();
                        m.errors_total
                            .get_or_create(&crate::metrics::ErrorLabels {
                                kind: "grpc_reconnect".to_string(),
                            })
                            .inc();
                    }
                    tokio::time::sleep(wait).await;
                    continue;
                }
            };

            let mut stream = stream;

            loop {
                let next_message = if let Some(timeout) = stream_idle_timeout {
                    match tokio::time::timeout(timeout, stream.message()).await {
                        Ok(msg) => msg,
                        Err(_) => {
                            warn!(idle_for = ?timeout, "stream idle timeout reached, will reconnect");
                            if let Some(ref m) = self.metrics {
                                m.grpc_reconnects_total.inc();
                                m.errors_total
                                    .get_or_create(&crate::metrics::ErrorLabels {
                                        kind: "grpc_reconnect".to_string(),
                                    })
                                    .inc();
                            }
                            break;
                        }
                    }
                } else {
                    stream.message().await
                };

                match next_message {
                    Ok(Some(resp)) => {
                        let new_cursor = resp.cursor.clone();
                        debug!(cursor = %new_cursor, step = ?resp.step, "received response");

                        let fork_step = if self.config.final_blocks_only {
                            None
                        } else {
                            Some(match resp.step {
                                1 => "NEW".to_string(),
                                2 => "UNDO".to_string(),
                                3 => "FINAL".to_string(),
                                other => format!("UNKNOWN_{other}"),
                            })
                        };

                        let identity = resp
                            .metadata
                            .as_ref()
                            .map(|m| BlockIdentity {
                                block_num: m.num,
                                block_id: m.id.clone(),
                                parent_num: m.parent_num,
                                parent_id: m.parent_id.clone(),
                                lib_num: m.lib_num,
                                timestamp: m.time.as_ref().map_or(0, |t| t.seconds),
                                timestamp_nanos: m.time.as_ref().map_or(0, |t| t.nanos),
                                fork_step: fork_step.clone(),
                            })
                            .unwrap_or_default();

                        if let Some(any) = resp.block {
                            blocks_this_connection += 1;
                            last_block_num =
                                Some(last_block_num.map_or(identity.block_num, |last| {
                                    last.max(identity.block_num)
                                }));
                            // `--stop-block` is exclusive. The request may ask
                            // for one block more than wanted (see
                            // `firehose_stop_block_num`), so never pass blocks
                            // at or above the stop block to the handler.
                            let past_stop_block = self
                                .config
                                .stop_block
                                .is_some_and(|stop_block| identity.block_num >= stop_block);
                            if past_stop_block {
                                debug!(
                                    block_num = identity.block_num,
                                    "dropping block at or above the exclusive stop block"
                                );
                            } else {
                                handler(
                                    any.value,
                                    any.type_url,
                                    new_cursor.clone(),
                                    identity,
                                    resp.step,
                                )?;
                            }
                        }

                        cursor = Some(new_cursor.clone());
                    }
                    Ok(None) => {
                        let stop_block = self.config.stop_block;
                        match clean_end_action(
                            stop_block,
                            last_block_num,
                            blocks_this_connection,
                            resumed_after_early_end,
                        ) {
                            CleanEndAction::Complete => {
                                info!("stream ended (stop block reached)");
                                return Ok(());
                            }
                            CleanEndAction::Exhausted => {
                                // The caller checks whether the range is complete.
                                info!(
                                    last_block_num = ?last_block_num,
                                    stop_block = ?stop_block,
                                    "resumed stream ended without new blocks; the server has no more blocks before the stop block"
                                );
                                return Ok(());
                            }
                            CleanEndAction::Reconnect => {
                                if stop_block.is_some() {
                                    resumed_after_early_end = true;
                                    if last_block_num.is_some() {
                                        warn!(
                                            last_block_num = ?last_block_num,
                                            stop_block = ?stop_block,
                                            "stream ended before the last requested block, resuming from the last cursor"
                                        );
                                    } else {
                                        // Typical when rerunning a finished range.
                                        info!(
                                            stop_block = ?stop_block,
                                            "stream ended without blocks, resuming once to confirm the range is exhausted"
                                        );
                                    }
                                } else {
                                    warn!(
                                        last_block_num = ?last_block_num,
                                        "live stream closed by the server, will reconnect"
                                    );
                                }
                                if let Some(ref m) = self.metrics {
                                    m.grpc_reconnects_total.inc();
                                    m.errors_total
                                        .get_or_create(&crate::metrics::ErrorLabels {
                                            kind: "grpc_reconnect".to_string(),
                                        })
                                        .inc();
                                }
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "stream error, will reconnect");
                        if let Some(ref m) = self.metrics {
                            m.grpc_reconnects_total.inc();
                            m.errors_total
                                .get_or_create(&crate::metrics::ErrorLabels {
                                    kind: "grpc_reconnect".to_string(),
                                })
                                .inc();
                        }
                        break;
                    }
                }
            }

            let wait = reconnect_backoff
                .next_backoff()
                .unwrap_or(Duration::from_secs(60));
            tokio::time::sleep(wait).await;
        }
    }
}

/// Convert the exclusive `--stop-block` into Firehose's inclusive
/// `stop_block_num`, where 0 means "stream forever".
///
/// A bounded run never sends 0: `--stop-block 1` (only block 0) asks for
/// blocks up to 1, and the stream loop drops block 1.
fn firehose_stop_block_num(stop_block: Option<u64>) -> u64 {
    match stop_block {
        None => 0,
        Some(stop_block) => stop_block.saturating_sub(1).max(1),
    }
}

/// What the stream loop does when the server ends a stream cleanly (`Ok(None)`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CleanEndAction {
    /// The last requested block (`stop_block - 1`) was received.
    Complete,
    /// A resumed bounded stream ended without delivering any block, so the
    /// server has no more blocks in the range (e.g. skipped slots at the end).
    Exhausted,
    /// Resume from the last cursor after the usual back-off.
    Reconnect,
}

/// Decide what a clean end of stream means.
///
/// - Live runs (no stop block) never end on their own, so a clean close came
///   from the server or a proxy: reconnect.
/// - Bounded runs complete once `stop_block - 1` was received.
/// - A bounded stream that ends earlier is resumed from the cursor. If that
///   resumed stream ends without delivering a block, the range is exhausted.
fn clean_end_action(
    stop_block: Option<u64>,
    last_block_num: Option<u64>,
    blocks_this_connection: u64,
    resumed_after_early_end: bool,
) -> CleanEndAction {
    let Some(stop_block) = stop_block else {
        return CleanEndAction::Reconnect;
    };
    let last_requested_block = stop_block.saturating_sub(1);
    if last_block_num.is_some_and(|block| block >= last_requested_block) {
        CleanEndAction::Complete
    } else if resumed_after_early_end && blocks_this_connection == 0 {
        CleanEndAction::Exhausted
    } else {
        CleanEndAction::Reconnect
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Compression, Partition};
    use std::path::PathBuf;
    use tokio::net::TcpListener;

    fn test_config(endpoint: &str) -> Config {
        Config {
            endpoint: endpoint.to_string(),
            api_key: None,
            jwt_token: None,
            start_block: None,
            stop_block: None,
            skip_missing_blocks: true,
            cursor_path: None,
            output: PathBuf::from("/tmp/output"),
            partition: Partition::None,
            flush_rows: None,
            flush_blocks: None,
            flush_bytes: 0,
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
            stream_idle_timeout_secs: None,
            reconnect_stall_timeout_secs: None,
        }
    }

    #[test]
    fn test_endpoint_enables_keepalive_settings() {
        let client = FirehoseClient::new(test_config("https://example.com")).unwrap();
        let endpoint = client.endpoint().expect("endpoint should build");
        let keepalive = FirehoseClient::endpoint_keepalive_settings();

        assert_eq!(
            endpoint.get_connect_timeout(),
            Some(Duration::from_secs(30))
        );
        assert_eq!(endpoint.get_tcp_keepalive(), Some(keepalive.tcp_keepalive));
        assert_eq!(
            keepalive,
            EndpointKeepaliveSettings {
                tcp_keepalive: Duration::from_secs(30),
                http2_keep_alive_interval: Duration::from_secs(30),
                keep_alive_timeout: Duration::from_secs(10),
                keep_alive_while_idle: true,
            }
        );
    }

    #[test]
    fn test_auth_metadata_trims_credentials_from_secret_files() {
        let mut config = test_config("https://example.com");
        config.api_key = Some("key-from-k8s-secret\n".to_string());
        config.jwt_token = Some("  token\r\n".to_string());

        let auth = AuthMetadata::from_config(&config).unwrap();
        let mut request = tonic::Request::new(());
        auth.apply(&mut request);

        assert_eq!(
            request.metadata().get("x-api-key").unwrap(),
            "key-from-k8s-secret"
        );
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer token"
        );
    }

    #[test]
    fn test_auth_metadata_ignores_blank_credentials() {
        let mut config = test_config("https://example.com");
        config.api_key = Some(" \n".to_string());

        let auth = AuthMetadata::from_config(&config).unwrap();
        let mut request = tonic::Request::new(());
        auth.apply(&mut request);

        assert!(request.metadata().get("x-api-key").is_none());
        assert!(request.metadata().get("authorization").is_none());
    }

    #[test]
    fn test_invalid_auth_header_value_is_an_error_not_a_panic() {
        let mut config = test_config("https://example.com");
        config.api_key = Some("key\nwith-embedded-newline".to_string());
        let error = FirehoseClient::new(config).err().expect("invalid API key");
        assert!(error.to_string().contains("API key"), "{error}");

        let mut config = test_config("https://example.com");
        config.jwt_token = Some("tok\u{7}en".to_string());
        let error = FirehoseClient::new(config)
            .err()
            .expect("invalid JWT token");
        assert!(error.to_string().contains("JWT token"), "{error}");
    }

    #[test]
    fn test_bounded_request_never_sends_unbounded_stop_block_num() {
        assert_eq!(firehose_stop_block_num(None), 0);
        // --stop-block 1 only wants block 0, which Firehose cannot express.
        assert_eq!(firehose_stop_block_num(Some(1)), 1);
        assert_eq!(firehose_stop_block_num(Some(2)), 1);
        assert_eq!(firehose_stop_block_num(Some(200)), 199);
    }

    #[test]
    fn test_clean_end_of_live_stream_reconnects() {
        assert_eq!(
            clean_end_action(None, Some(100), 5, false),
            CleanEndAction::Reconnect
        );
        assert_eq!(
            clean_end_action(None, None, 0, true),
            CleanEndAction::Reconnect
        );
    }

    #[test]
    fn test_clean_end_of_bounded_stream_completes_at_last_requested_block() {
        // --stop-block is exclusive: 200 means the last requested block is 199.
        assert_eq!(
            clean_end_action(Some(200), Some(199), 10, false),
            CleanEndAction::Complete
        );
        assert_eq!(
            clean_end_action(Some(200), Some(199), 0, true),
            CleanEndAction::Complete
        );
    }

    #[test]
    fn test_early_clean_end_of_bounded_stream_resumes_then_finishes_when_exhausted() {
        // The stream ends at 150: resume from the cursor instead of finishing.
        assert_eq!(
            clean_end_action(Some(200), Some(150), 10, false),
            CleanEndAction::Reconnect
        );
        // The resumed stream made progress but ended early again: resume again.
        assert_eq!(
            clean_end_action(Some(200), Some(180), 30, true),
            CleanEndAction::Reconnect
        );
        // The resumed stream delivered nothing: no more blocks in the range.
        assert_eq!(
            clean_end_action(Some(200), Some(180), 0, true),
            CleanEndAction::Exhausted
        );
        // A range with no blocks at all gets one resume before finishing.
        assert_eq!(
            clean_end_action(Some(200), None, 0, false),
            CleanEndAction::Reconnect
        );
        assert_eq!(
            clean_end_action(Some(200), None, 0, true),
            CleanEndAction::Exhausted
        );
    }

    #[tokio::test]
    async fn test_healthcheck_reports_unavailable_endpoint() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let endpoint = format!("http://127.0.0.1:{port}");
        let client = FirehoseClient::new(test_config(&endpoint)).unwrap();
        let err = client
            .healthcheck()
            .await
            .expect_err("closed port should fail the endpoint healthcheck");

        let error = err.to_string();
        assert!(error.contains("unavailable or unhealthy"));
        assert!(error.contains(&endpoint));
    }
}
