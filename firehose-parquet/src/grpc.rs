use crate::config::Config;
use crate::metrics::PipelineMetrics;
use crate::traits::BlockIdentity;
use anyhow::{Context, Result};
use backoff::backoff::Backoff;
use backoff::ExponentialBackoffBuilder;
use firehose_protos::firehose;
use std::time::{Duration, Instant};
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
    metrics: Option<PipelineMetrics>,
}

impl FirehoseClient {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            metrics: None,
        }
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
        if let Some(ref key) = self.config.api_key {
            request.metadata_mut().insert(
                "x-api-key",
                key.parse()
                    .expect("API key must be valid ASCII metadata value"),
            );
        }
        if let Some(ref token) = self.config.jwt_token {
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {token}")
                    .parse()
                    .expect("JWT token must be valid ASCII metadata value"),
            );
        }

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
            if let Some(ref key) = self.config.api_key {
                request
                    .metadata_mut()
                    .insert("x-api-key", key.parse().unwrap());
            }
            if let Some(ref token) = self.config.jwt_token {
                request
                    .metadata_mut()
                    .insert("authorization", format!("Bearer {token}").parse().unwrap());
            }

            let response = client.block(request).await?.into_inner();
            Ok(response.metadata.as_ref().map(|m| BlockIdentity {
                block_num: m.num,
                block_id: m.id.clone(),
                parent_num: m.parent_num,
                parent_id: m.parent_id.clone(),
                lib_num: m.lib_num,
                timestamp: m.time.as_ref().map_or(0, |t| t.seconds),
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
    /// and resume from the last cursor.
    ///
    /// Returns when the stream is cleanly exhausted (stop block reached) or an
    /// unrecoverable error occurs.
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
        let stream_idle_timeout = self
            .config
            .stream_idle_timeout_secs
            .map(Duration::from_secs);
        let reconnect_stall_timeout = self
            .config
            .reconnect_stall_timeout_secs
            .map(Duration::from_secs);

        let mut attempt = 0u64;
        let mut reconnect_stall_started_at: Option<Instant> = None;

        loop {
            attempt += 1;
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
                // CLI stop_block is exclusive; Firehose protocol is inclusive.
                // Subtract 1 to convert (0 means stream forever in both).
                stop_block_num: self.config.stop_block.map_or(0, |b| b.saturating_sub(1)),
                final_blocks_only: self.config.final_blocks_only,
                transforms: vec![],
            };

            let mut request = tonic::Request::new(req);
            if let Some(ref key) = self.config.api_key {
                request
                    .metadata_mut()
                    .insert("x-api-key", key.parse().unwrap());
            }
            if let Some(ref token) = self.config.jwt_token {
                request
                    .metadata_mut()
                    .insert("authorization", format!("Bearer {token}").parse().unwrap());
            }

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
                                fork_step: fork_step.clone(),
                            })
                            .unwrap_or_default();

                        if let Some(any) = resp.block {
                            handler(
                                any.value,
                                any.type_url,
                                new_cursor.clone(),
                                identity,
                                resp.step,
                            )?;
                        }

                        cursor = Some(new_cursor.clone());
                    }
                    Ok(None) => {
                        info!("stream ended (stop block reached or server closed)");
                        return Ok(());
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Compression, Partition};
    use std::path::PathBuf;

    fn test_config(endpoint: &str) -> Config {
        Config {
            endpoint: endpoint.to_string(),
            api_key: None,
            jwt_token: None,
            start_block: None,
            stop_block: None,
            skip_missing_blocks: false,
            cursor_path: None,
            output: PathBuf::from("/tmp/output"),
            partition: Partition::None,
            flush_rows: None,
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
        let client = FirehoseClient::new(test_config("https://example.com"));
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
}
