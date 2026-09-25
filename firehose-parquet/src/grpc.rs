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

pub use tokio_util::sync::CancellationToken;

/// Error returned when work stops because shutdown was requested
/// (SIGINT/SIGTERM). Callers detect it with [`is_shutdown_error`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("shutdown requested")]
pub struct ShutdownRequested;

/// Whether `error` (or any error in its chain) is [`ShutdownRequested`].
pub fn is_shutdown_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| cause.is::<ShutdownRequested>())
}

/// Await `future`, or return [`ShutdownRequested`] as soon as `shutdown` is
/// cancelled, whichever comes first.
pub async fn unless_shutdown<T>(
    shutdown: &CancellationToken,
    future: impl std::future::Future<Output = T>,
) -> Result<T> {
    tokio::select! {
        biased;
        _ = shutdown.cancelled() => Err(ShutdownRequested.into()),
        value = future => Ok(value),
    }
}

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

/// Returned by [`FirehoseClient::fetch_block_identity`] when a fetch does not finish within
/// its wait timeout.
///
/// Probe callers must retry it as a transient failure, never treat it as a missing block: on
/// a slow endpoint that would make sparse probing skip real blocks.
#[derive(Debug, thiserror::Error)]
#[error("fetching block {block_num} timed out after {timeout:?}")]
pub struct FetchTimeoutError {
    pub block_num: u64,
    pub timeout: Duration,
}

/// How a caller should treat a failed single-block fetch (see [`classify_fetch_error`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchErrorKind {
    /// The fetch did not finish in time; retry it.
    Timeout,
    /// The endpoint has no block at this number, e.g. a skipped Solana slot.
    NotFound,
    /// Retrying cannot help: authentication, permissions, or an invalid request.
    Fatal,
    /// An unavailable endpoint or dropped connection; live callers may retry later.
    Transient,
    /// An unrecognized failure; bounded retries must surface it rather than infer absence.
    Unexpected,
}

/// Classify a [`FirehoseClient::fetch_block_identity`] error, looking through any context.
pub fn classify_fetch_error(error: &anyhow::Error) -> FetchErrorKind {
    use tonic::Code;

    for cause in error.chain() {
        if cause.is::<FetchTimeoutError>() {
            return FetchErrorKind::Timeout;
        }
        if let Some(status) = cause.downcast_ref::<tonic::Status>() {
            match status.code() {
                Code::NotFound => return FetchErrorKind::NotFound,
                Code::DeadlineExceeded => return FetchErrorKind::Timeout,
                Code::Unauthenticated
                | Code::PermissionDenied
                | Code::InvalidArgument
                | Code::FailedPrecondition
                | Code::OutOfRange
                | Code::Unimplemented => return FetchErrorKind::Fatal,
                // Some proxies preserve this precise upstream missing-block
                // status in an Unknown response. Do not infer absence from
                // arbitrary error text: storage/auth failures can mention a
                // missing block file without establishing a skipped slot.
                Code::Unknown
                    if status.message()
                        == "rpc error: code = NotFound desc = block not found in files" =>
                {
                    return FetchErrorKind::NotFound;
                }
                Code::Unavailable | Code::Cancelled | Code::ResourceExhausted => {
                    return FetchErrorKind::Transient;
                }
                _ => return FetchErrorKind::Unexpected,
            }
        }
        if cause.is::<tonic::transport::Error>() {
            return FetchErrorKind::Transient;
        }
        if let Some(error) = cause.downcast_ref::<std::io::Error>() {
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::NotConnected
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::Interrupted
            ) {
                return FetchErrorKind::Transient;
            }
        }
    }

    FetchErrorKind::Unexpected
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
    /// Channel shared by single-block fetches. tonic multiplexes requests over it and
    /// reconnects when the connection drops, so sparse probes skip a TCP/TLS handshake each.
    fetch_channel: tokio::sync::OnceCell<Channel>,
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
            fetch_channel: tokio::sync::OnceCell::new(),
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

    /// Fetch endpoint metadata with three attempts, a ten-second timeout per
    /// attempt, and bounded exponential backoff. Transient failures are errors,
    /// never a successful response without a chain name.
    pub async fn info(&self) -> Result<EndpointInfo> {
        let response = retry_endpoint_info(
            || async {
                let channel = self.connect().await?;
                let mut client = firehose::endpoint_info_client::EndpointInfoClient::new(channel)
                    .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                    .max_decoding_message_size(128 * 1024 * 1024);
                let mut request = tonic::Request::new(firehose::InfoRequest {});
                self.auth.apply(&mut request);
                Ok(client.info(request).await?.into_inner())
            },
            Duration::from_secs(10),
            Duration::from_millis(250),
        )
        .await?;
        let info = EndpointInfo {
            chain_name: response.chain_name,
            chain_name_aliases: response.chain_name_aliases,
            first_streamable_block_num: response.first_streamable_block_num,
            first_streamable_block_id: response.first_streamable_block_id,
            block_id_encoding: response.block_id_encoding,
            block_features: response.block_features,
        };
        info!(
            chain_name = %info.chain_name,
            block_id_encoding = info.block_id_encoding,
            block_features = ?info.block_features,
            "received endpoint info"
        );
        Ok(info)
    }

    /// Channel for single-block fetches, connected on first use and reused afterwards.
    async fn fetch_channel(&self) -> Result<Channel> {
        self.fetch_channel
            .get_or_try_init(|| self.connect_with_log(false))
            .await
            .cloned()
    }

    /// Fetch one block's identity with the `Fetch/Block` RPC.
    ///
    /// Returns [`FetchTimeoutError`] when `wait_timeout` elapses. Some endpoints answer a
    /// request past the chain head with their head block, so callers should compare the
    /// returned `block_num` with the requested one.
    pub async fn fetch_block_identity(
        &self,
        block_num: u64,
        wait_timeout: Option<Duration>,
    ) -> Result<Option<BlockIdentity>> {
        let fetch = async {
            let channel = self.fetch_channel().await?;
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
            response
                .metadata
                .as_ref()
                .map(|metadata| checked_block_identity(metadata, None))
                .transpose()
        };

        match wait_timeout {
            Some(timeout) => match tokio::time::timeout(timeout, fetch).await {
                Ok(result) => result,
                Err(_) => Err(FetchTimeoutError { block_num, timeout }.into()),
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
    ///
    /// Every wait (connecting, the `Blocks` call, the next message, back-off)
    /// also watches `shutdown`, and returns [`ShutdownRequested`] as soon as
    /// it is cancelled. A block already passed to `handler` is never cut short.
    pub async fn stream_blocks<F>(
        &self,
        initial_cursor: Option<String>,
        shutdown: &CancellationToken,
        mut handler: F,
    ) -> Result<()>
    where
        F: FnMut(Vec<u8>, String, String, BlockIdentity, i32) -> Result<()>,
    {
        let mut cursor = initial_cursor;
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

        let mut reconnect = ReconnectState::new(reconnect_stall_timeout);
        // Highest block number received so far, across reconnects.
        let mut last_block_num: Option<u64> = None;
        // Set once a bounded stream ended before its last requested block.
        let mut resumed_after_early_end = false;

        loop {
            if shutdown.is_cancelled() {
                return Err(ShutdownRequested.into());
            }
            let mut blocks_this_connection = 0u64;
            let mut received_message = false;
            let channel = match unless_shutdown(shutdown, self.connect()).await? {
                Ok(ch) => ch,
                Err(e) => {
                    let wait = reconnect.record_failure(Instant::now(), &e)?;
                    warn!(
                        attempt = reconnect.failed_attempts,
                        error = %e,
                        retry_in = ?wait,
                        "connection failed, retrying"
                    );
                    self.record_reconnect_metric();
                    unless_shutdown(shutdown, tokio::time::sleep(wait)).await?;
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

            // Accepting the RPC is not progress: the back-off and the stall
            // timer are only reset once a message arrives.
            let stream = match unless_shutdown(shutdown, client.blocks(request)).await? {
                Ok(resp) => resp.into_inner(),
                Err(status) => {
                    self.fail_on_fatal_status(&status)?;
                    let wait = reconnect.record_failure(Instant::now(), &status)?;
                    warn!(
                        attempt = reconnect.failed_attempts,
                        error = %status,
                        retry_in = ?wait,
                        "Blocks RPC failed, will retry"
                    );
                    self.record_reconnect_metric();
                    unless_shutdown(shutdown, tokio::time::sleep(wait)).await?;
                    continue;
                }
            };

            let mut stream = stream;

            let session_end = loop {
                let next_message = if let Some(timeout) = stream_idle_timeout {
                    match unless_shutdown(shutdown, tokio::time::timeout(timeout, stream.message()))
                        .await?
                    {
                        Ok(msg) => msg,
                        Err(_) => {
                            warn!(idle_for = ?timeout, "stream idle timeout reached, will reconnect");
                            break SessionEnd::Idle;
                        }
                    }
                } else {
                    unless_shutdown(shutdown, stream.message()).await?
                };

                match next_message {
                    Ok(Some(resp)) => {
                        if !received_message {
                            received_message = true;
                            reconnect.record_progress();
                        }
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

                        let metadata = resp.metadata.as_ref().ok_or_else(|| anyhow::anyhow!(
                            "Firehose response is missing block metadata; cannot safely identify or checkpoint this block"))?;
                        let identity = checked_block_identity(metadata, fork_step)?;

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
                                break SessionEnd::Closed;
                            }
                        }
                    }
                    Err(status) => {
                        self.fail_on_fatal_status(&status)?;
                        warn!(
                            attempt = reconnect.failed_attempts + 1,
                            error = %status,
                            "stream error, will reconnect"
                        );
                        break SessionEnd::Failed(status);
                    }
                }
            };

            self.record_reconnect_metric();
            let wait = match session_end {
                // A quiet stream is not a failure (e.g. slow block times).
                SessionEnd::Idle => reconnect.next_delay(),
                SessionEnd::Closed if received_message => reconnect.next_delay(),
                SessionEnd::Closed => reconnect.record_failure(
                    Instant::now(),
                    &"stream closed by the server without sending a message",
                )?,
                SessionEnd::Failed(status) => reconnect.record_failure(Instant::now(), &status)?,
            };
            unless_shutdown(shutdown, tokio::time::sleep(wait)).await?;
        }
    }

    fn record_reconnect_metric(&self) {
        if let Some(ref m) = self.metrics {
            m.grpc_reconnects_total.inc();
            m.errors_total
                .get_or_create(&crate::metrics::ErrorLabels {
                    kind: "grpc_reconnect".to_string(),
                })
                .inc();
        }
    }

    /// Return an error for a gRPC status that reconnecting cannot fix.
    fn fail_on_fatal_status(&self, status: &tonic::Status) -> Result<()> {
        let Some(error) = fatal_status_error(status) else {
            return Ok(());
        };
        if let Some(ref m) = self.metrics {
            m.errors_total
                .get_or_create(&crate::metrics::ErrorLabels {
                    kind: "grpc_fatal".to_string(),
                })
                .inc();
        }
        Err(error)
    }
}

/// How a streaming session ended without finishing the run.
enum SessionEnd {
    /// No message arrived within the idle timeout.
    Idle,
    /// The server closed the stream cleanly and the loop reconnects.
    Closed,
    /// The stream failed with a retryable status.
    Failed(tonic::Status),
}

/// Consecutive failed attempts without receiving a message after which the
/// stream gives up, even when `--reconnect-stall-timeout-secs` is disabled.
/// With the 60 s maximum back-off this takes over 20 minutes.
const MAX_FAILED_ATTEMPTS_WITHOUT_PROGRESS: u64 = 30;

/// Reconnect bookkeeping for [`FirehoseClient::stream_blocks`].
///
/// Only a received message counts as progress. Connection failures, failed
/// RPCs, stream errors and clean closes without a message are failures: they
/// grow the back-off, run the stall timer and count toward
/// [`MAX_FAILED_ATTEMPTS_WITHOUT_PROGRESS`].
struct ReconnectState {
    backoff: backoff::ExponentialBackoff,
    stall_timeout: Option<Duration>,
    stall_started_at: Option<Instant>,
    failed_attempts: u64,
}

impl ReconnectState {
    fn new(stall_timeout: Option<Duration>) -> Self {
        Self {
            backoff: ExponentialBackoffBuilder::default()
                .with_initial_interval(Duration::from_secs(1))
                .with_max_interval(Duration::from_secs(60))
                .with_max_elapsed_time(None) // limits are enforced here
                .build(),
            stall_timeout,
            stall_started_at: None,
            failed_attempts: 0,
        }
    }

    /// A message arrived: reset the back-off, the stall timer and the count.
    fn record_progress(&mut self) {
        self.backoff.reset();
        self.stall_started_at = None;
        self.failed_attempts = 0;
    }

    /// Record a failed attempt. Returns the delay before the next attempt, or
    /// an error once the stall timeout or the attempt cap is reached.
    fn record_failure(&mut self, now: Instant, error: &dyn std::fmt::Display) -> Result<Duration> {
        self.failed_attempts += 1;
        let stall_elapsed =
            now.saturating_duration_since(*self.stall_started_at.get_or_insert(now));
        if let Some(stall_timeout) = self.stall_timeout {
            if stall_elapsed >= stall_timeout {
                return Err(anyhow::anyhow!(
                    "reconnect stalled: no stream message for {:?} (limit {:?}) after {} failed attempts; last error: {}",
                    stall_elapsed,
                    stall_timeout,
                    self.failed_attempts,
                    error
                ));
            }
        }
        if self.failed_attempts >= MAX_FAILED_ATTEMPTS_WITHOUT_PROGRESS {
            return Err(anyhow::anyhow!(
                "giving up after {} consecutive failed attempts without a stream message ({:?} since the first failure); last error: {}",
                self.failed_attempts,
                stall_elapsed,
                error
            ));
        }
        Ok(self.next_delay())
    }

    /// Delay before reconnecting, without recording a failure.
    fn next_delay(&mut self) -> Duration {
        self.backoff
            .next_backoff()
            .unwrap_or(Duration::from_secs(60))
    }
}

/// Endpoint metadata could not be obtained. Startup must stop before resolving
/// output and cursor paths; a network alias or block family cannot substitute
/// for the endpoint identity and metadata.
#[derive(Debug, thiserror::Error)]
#[error("EndpointInfo is unavailable; refusing to infer a different output or cursor path")]
pub struct EndpointInfoUnavailable;

async fn retry_endpoint_info<F, Fut>(
    mut request: F,
    timeout: Duration,
    initial_backoff: Duration,
) -> Result<firehose::InfoResponse>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<firehose::InfoResponse>>,
{
    for attempt in 1..=3 {
        let result = tokio::time::timeout(timeout, request()).await;
        let error = match result {
            Ok(Ok(response)) => {
                if response.chain_name.trim().is_empty() {
                    anyhow::bail!("EndpointInfo returned an empty chain_name; refusing to change the output or cursor path");
                }
                return Ok(response);
            }
            Ok(Err(error)) => error,
            Err(_) => anyhow::anyhow!("EndpointInfo attempt timed out after {timeout:?}"),
        };
        if let Some(status) = error.downcast_ref::<tonic::Status>() {
            if effective_status_code(status) == tonic::Code::Unimplemented {
                return Err(error.context(EndpointInfoUnavailable));
            }
            if fatal_status_error(status).is_some() {
                return Err(error.context("EndpointInfo rejected the request; not retrying"));
            }
        }
        if attempt == 3 {
            return Err(error
                .context("EndpointInfo failed after 3 attempts")
                .context(EndpointInfoUnavailable));
        }
        let backoff = initial_backoff * (1 << (attempt - 1));
        warn!(attempt, retry_in_ms = backoff.as_millis(), error = %error,
            "EndpointInfo failed; retrying before resolving output");
        tokio::time::sleep(backoff).await;
    }
    unreachable!("the final attempt always returns")
}

/// gRPC status codes that reconnecting with the same request and credentials
/// cannot fix.
fn is_fatal_code(code: tonic::Code) -> bool {
    matches!(
        code,
        tonic::Code::Unauthenticated
            | tonic::Code::PermissionDenied
            | tonic::Code::InvalidArgument
            | tonic::Code::FailedPrecondition
            | tonic::Code::OutOfRange
            | tonic::Code::Unimplemented
    )
}

/// The code a status really carries. Firehose can relay an upstream error as
/// `Unknown` with the original status in the message, e.g.
/// `rpc error: code = InvalidArgument desc = start block 5 is after stop block 4`.
fn effective_status_code(status: &tonic::Status) -> tonic::Code {
    if status.code() != tonic::Code::Unknown {
        return status.code();
    }
    status
        .message()
        .split_once("code = ")
        .and_then(|(_, rest)| rest.split_whitespace().next())
        .and_then(status_code_from_name)
        .unwrap_or(tonic::Code::Unknown)
}

/// Parse a gRPC status code name as Go's `status` package prints it.
fn status_code_from_name(name: &str) -> Option<tonic::Code> {
    use tonic::Code;
    Some(match name {
        "OK" => Code::Ok,
        "Canceled" => Code::Cancelled,
        "Unknown" => Code::Unknown,
        "InvalidArgument" => Code::InvalidArgument,
        "DeadlineExceeded" => Code::DeadlineExceeded,
        "NotFound" => Code::NotFound,
        "AlreadyExists" => Code::AlreadyExists,
        "PermissionDenied" => Code::PermissionDenied,
        "ResourceExhausted" => Code::ResourceExhausted,
        "FailedPrecondition" => Code::FailedPrecondition,
        "Aborted" => Code::Aborted,
        "OutOfRange" => Code::OutOfRange,
        "Unimplemented" => Code::Unimplemented,
        "Internal" => Code::Internal,
        "Unavailable" => Code::Unavailable,
        "DataLoss" => Code::DataLoss,
        "Unauthenticated" => Code::Unauthenticated,
        _ => return None,
    })
}

/// Build the error for a fatal status, with a hint on what to fix, or `None`
/// when the status is worth retrying.
fn fatal_status_error(status: &tonic::Status) -> Option<anyhow::Error> {
    let code = effective_status_code(status);
    // `ResourceExhausted` is usually a rate limit worth backing off for, but
    // an exhausted quota (e.g. "billable egress bytes quota exceeded") does
    // not recover within the run.
    let quota_exhausted = code == tonic::Code::ResourceExhausted
        && status.message().to_ascii_lowercase().contains("quota");
    if !is_fatal_code(code) && !quota_exhausted {
        return None;
    }
    let hint = match code {
        tonic::Code::ResourceExhausted => {
            "the credential's quota is used up; use another API key or token, or wait for the quota to reset"
        }
        tonic::Code::Unauthenticated | tonic::Code::PermissionDenied => {
            "check that the API key or token (--api-key-envvar / --api-token-envvar) is valid for this endpoint"
        }
        tonic::Code::InvalidArgument | tonic::Code::FailedPrecondition => {
            "check --start-block, --stop-block and the stored cursor (--cursor-override restarts from the CLI bounds)"
        }
        tonic::Code::OutOfRange => {
            "the request or a response is out of range, e.g. a block past the chain head or larger than the 128 MiB message limit"
        }
        _ => "the endpoint does not serve the Firehose v2 Stream API",
    };
    Some(anyhow::anyhow!(
        "Firehose rejected the stream with {code:?}: {}; not retrying: {hint}",
        status.message()
    ))
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

/// Build an identity only after all present time metadata has been validated.
fn checked_block_identity(
    metadata: &firehose::BlockMetadata,
    fork_step: Option<String>,
) -> Result<BlockIdentity> {
    if let Some(time) = &metadata.time {
        crate::traits::timestamp_millis(time.seconds, time.nanos)
            .with_context(|| format!("invalid timestamp metadata for block {}", metadata.num))?;
    }
    Ok(BlockIdentity {
        block_num: metadata.num,
        block_id: metadata.id.clone(),
        parent_num: metadata.parent_num,
        parent_id: metadata.parent_id.clone(),
        lib_num: metadata.lib_num,
        timestamp: metadata.time.as_ref().map_or(0, |time| time.seconds),
        timestamp_nanos: metadata.time.as_ref().map_or(0, |time| time.nanos),
        fork_step,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Compression, Partition};
    use std::path::PathBuf;
    use tokio::net::TcpListener;

    /// A real local gRPC stream, so handler failure is checked through the
    /// production reconnect loop rather than a model of its control flow.
    #[derive(Clone)]
    struct TwoBlockService(Vec<firehose::Response>);

    impl tonic::server::ServerStreamingService<firehose::Request> for TwoBlockService {
        type Response = firehose::Response;
        type ResponseStream =
            futures::stream::BoxStream<'static, Result<firehose::Response, tonic::Status>>;
        type Future =
            tonic::codegen::BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;

        fn call(&mut self, _: tonic::Request<firehose::Request>) -> Self::Future {
            let responses = self.0.clone();
            Box::pin(async move {
                let stream = futures::stream::iter(responses.into_iter().map(Ok));
                Ok(tonic::Response::new(
                    Box::pin(stream) as Self::ResponseStream
                ))
            })
        }
    }

    fn test_response(num: u64) -> firehose::Response {
        firehose::Response {
            block: Some(prost_types::Any {
                type_url: "test.Block".into(),
                value: vec![],
            }),
            step: 3,
            cursor: format!("cursor-{num}"),
            metadata: Some(firehose::BlockMetadata {
                num,
                ..Default::default()
            }),
        }
    }

    impl tonic::server::NamedService for TwoBlockService {
        const NAME: &'static str = "sf.firehose.v2.Stream";
    }

    impl tonic::codegen::Service<tonic::codegen::http::Request<tonic::body::Body>> for TwoBlockService {
        type Response = tonic::codegen::http::Response<tonic::body::Body>;
        type Error = std::convert::Infallible;
        type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;

        fn poll_ready(
            &mut self,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(
            &mut self,
            request: tonic::codegen::http::Request<tonic::body::Body>,
        ) -> Self::Future {
            let service = self.clone();
            Box::pin(async move {
                let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
                Ok(grpc.server_streaming(service, request).await)
            })
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cursor_save_exhaustion_stops_stream_before_the_next_block() {
        use crate::cursor::{CursorLocation, CursorState};
        use std::sync::atomic::AtomicBool;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let incoming = futures::stream::unfold(listener, |listener| async {
            Some((listener.accept().await.map(|(socket, _)| socket), listener))
        });
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async {
            tonic::transport::Server::builder()
                .add_service(TwoBlockService(vec![
                    test_response(100),
                    test_response(101),
                ]))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let blocked_parent = dir.path().join("not-a-directory");
        std::fs::write(&blocked_parent, b"file").unwrap();
        let location = CursorLocation::Local(blocked_parent.join("cursor.parquet"));
        let (_, metrics) = crate::metrics::init();
        let mut config = test_config(&endpoint);
        config.start_block = Some(100);
        config.stop_block = Some(102);
        let mut client = FirehoseClient::new(config).unwrap();
        client.set_metrics(metrics.clone());
        let mut processed = vec![];
        let result = client
            .stream_blocks(
                None,
                &CancellationToken::new(),
                |_, _, cursor, identity, _| {
                    processed.push(identity.block_num);
                    location.save_with_retry_blocking(
                        &CursorState {
                            cursor,
                            last_block_num: identity.block_num,
                            ..Default::default()
                        },
                        &metrics,
                        &AtomicBool::new(false),
                    )
                },
            )
            .await;
        stop.send(()).unwrap();
        server.await.unwrap();
        let error = result.unwrap_err();
        assert!(error
            .to_string()
            .contains("cursor persistence failed after 3 attempts"));
        assert_eq!(
            processed,
            vec![100],
            "a failed checkpoint must stop ingestion"
        );
        assert_eq!(metrics.cursor_save_failures_total.get(), 3);
        assert_eq!(metrics.cursor_saves_total.get(), 0);
        assert_eq!(metrics.cursor_last_success_timestamp_seconds.get(), 0);
        assert_eq!(metrics.grpc_reconnects_total.get(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn malformed_stream_identity_never_reaches_handler_or_checkpoint() {
        for invalid in [
            None,
            Some((i64::MIN, 0)),
            Some((i64::MAX, 0)),
            Some((1_700_000_000_000, 0)),
            Some((0, -1)),
            Some((0, 1_000_000_000)),
        ] {
            let mut bad = test_response(100);
            bad.metadata = invalid.map(|(seconds, nanos)| firehose::BlockMetadata {
                num: 100,
                time: Some(prost_types::Timestamp { seconds, nanos }),
                ..Default::default()
            });
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let incoming = futures::stream::unfold(listener, |listener| async {
                Some((listener.accept().await.map(|(socket, _)| socket), listener))
            });
            let (stop, stopped) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(TwoBlockService(vec![bad, test_response(101)]))
                    .serve_with_incoming_shutdown(incoming, async {
                        let _ = stopped.await;
                    })
                    .await
                    .unwrap();
            });
            let mut config = test_config(&endpoint);
            config.start_block = Some(100);
            config.stop_block = Some(102);
            let client = FirehoseClient::new(config).unwrap();
            let dir = tempfile::tempdir().unwrap();
            let cursor = dir.path().join("cursor.parquet");
            std::fs::write(&cursor, b"existing checkpoint").unwrap();
            let mut handled = 0;
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                client.stream_blocks(None, &CancellationToken::new(), |_, _, _, _, _| {
                    handled += 1;
                    std::fs::write(&cursor, b"advanced checkpoint")?;
                    std::fs::write(dir.path().join("published.parquet"), b"published")?;
                    Ok(())
                }),
            )
            .await
            .expect("invalid metadata must fail promptly");
            stop.send(()).unwrap();
            server.await.unwrap();
            let error = format!("{:#}", result.unwrap_err());
            assert!(error.contains("metadata"), "{error}");
            assert_eq!(handled, 0);
            assert_eq!(std::fs::read(&cursor).unwrap(), b"existing checkpoint");
            assert!(!dir.path().join("published.parquet").exists());
        }
    }

    #[test]
    fn metadata_identity_preserves_negative_and_absent_times() {
        let mut metadata = firehose::BlockMetadata {
            num: 42,
            ..Default::default()
        };
        assert_eq!(
            checked_block_identity(&metadata, None).unwrap().timestamp,
            0
        );
        metadata.time = Some(prost_types::Timestamp {
            seconds: -1,
            nanos: 500_000_000,
        });
        let identity = checked_block_identity(&metadata, Some("UNDO".into())).unwrap();
        assert_eq!(identity.timestamp_millis().unwrap(), -500);
        assert_eq!(identity.fork_step.as_deref(), Some("UNDO"));
    }

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
    fn test_provider_scoping_applies_to_rpc_auth_metadata() {
        for (endpoint, expected_key, expected_bearer) in [
            (
                "https://eth.firehose.pinax.network",
                Some("pinax-key"),
                None,
            ),
            (
                "https://mainnet.tron.streamingfast.io",
                None,
                Some("Bearer sf-token"),
            ),
            ("https://custom.example", None, None),
        ] {
            let credentials = crate::auth::resolve_with(endpoint, None, None, |name| match name {
                "PINAX_API_KEY" => Some("pinax-key".into()),
                "SUBSTREAMS_API_KEY" => Some("legacy-pinax-key".into()),
                "STREAMINGFAST_API_TOKEN" => Some("sf-token".into()),
                _ => None,
            })
            .unwrap();
            let mut config = test_config(endpoint);
            config.api_key = credentials.api_key;
            config.jwt_token = credentials.jwt_token;
            let client = FirehoseClient::new(config).unwrap();
            // Info, stream, and sparse probes all use this same metadata helper.
            let mut request = tonic::Request::new(firehose::InfoRequest {});
            client.auth.apply(&mut request);
            assert_eq!(
                request
                    .metadata()
                    .get("x-api-key")
                    .map(|v| v.to_str().unwrap()),
                expected_key
            );
            assert_eq!(
                request
                    .metadata()
                    .get("authorization")
                    .map(|v| v.to_str().unwrap()),
                expected_bearer
            );
        }
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
    fn test_fatal_status_codes_fail_fast() {
        for status in [
            tonic::Status::unauthenticated("invalid JWT token"),
            tonic::Status::permission_denied("quota plan does not allow this network"),
            tonic::Status::invalid_argument("invalid cursor"),
            tonic::Status::failed_precondition("cursor is on a forked block"),
            tonic::Status::out_of_range(
                "Error, decoded message length too large: found 200000000 bytes, the limit is: 134217728 bytes",
            ),
            tonic::Status::unimplemented("unknown service sf.firehose.v2.Stream"),
        ] {
            assert!(
                fatal_status_error(&status).is_some(),
                "{:?} should be fatal",
                status.code()
            );
        }
    }

    #[test]
    fn test_exhausted_quota_fails_fast_but_rate_limits_are_retried() {
        // Seen live on Pinax for a JWT over its egress quota.
        let quota = tonic::Status::resource_exhausted(
            "resource exhausted: billable egress bytes quota exceeded (quota '5368709120', current '15762311347')",
        );
        let error = fatal_status_error(&quota).expect("fatal").to_string();
        assert!(error.contains("ResourceExhausted"), "{error}");
        assert!(error.contains("quota is used up"), "{error}");

        let relayed = tonic::Status::unknown(
            "rpc error: code = ResourceExhausted desc = monthly Quota exceeded",
        );
        assert!(fatal_status_error(&relayed).is_some());

        let rate_limit = tonic::Status::resource_exhausted("rate limit: too many requests");
        assert!(fatal_status_error(&rate_limit).is_none());
    }

    #[test]
    fn test_transient_status_codes_are_retried() {
        for status in [
            tonic::Status::unavailable("connection reset"),
            tonic::Status::internal("h2 protocol error"),
            tonic::Status::resource_exhausted("rate limited"),
            tonic::Status::deadline_exceeded("timeout"),
            tonic::Status::cancelled("stream cancelled"),
            tonic::Status::aborted("aborted"),
            tonic::Status::unknown("transport error"),
        ] {
            assert!(
                fatal_status_error(&status).is_none(),
                "{:?} should be retried",
                status.code()
            );
        }
    }

    #[test]
    fn test_status_relayed_as_unknown_uses_the_wrapped_code() {
        // Seen live: start > stop comes back as `Unknown` with the upstream
        // status in the message, and used to be retried forever.
        let status = tonic::Status::unknown(
            "rpc error: code = InvalidArgument desc = start block 24000005 is after stop block 24000004",
        );
        assert_eq!(effective_status_code(&status), tonic::Code::InvalidArgument);
        let error = fatal_status_error(&status).expect("fatal").to_string();
        assert!(error.contains("InvalidArgument"), "{error}");
        assert!(error.contains("start block 24000005"), "{error}");
        assert!(error.contains("--cursor-override"), "{error}");

        let status = tonic::Status::unknown("rpc error: code = Unavailable desc = backend down");
        assert_eq!(effective_status_code(&status), tonic::Code::Unavailable);
        assert!(fatal_status_error(&status).is_none());
    }

    #[test]
    fn test_unauthenticated_error_points_at_the_credentials() {
        let error = fatal_status_error(&tonic::Status::unauthenticated("invalid JWT token"))
            .expect("fatal")
            .to_string();
        assert!(error.contains("invalid JWT token"), "{error}");
        assert!(error.contains("--api-token-envvar"), "{error}");
    }

    #[test]
    fn test_stall_timer_runs_from_first_failure_and_resets_only_on_progress() {
        let t0 = Instant::now();
        let mut state = ReconnectState::new(Some(Duration::from_secs(900)));

        // Failed sessions whose RPC was accepted but that never delivered a
        // message do not reset the timer.
        assert!(state.record_failure(t0, &"accepted, then Internal").is_ok());
        assert!(state
            .record_failure(t0 + Duration::from_secs(899), &"accepted, then Internal")
            .is_ok());
        let error = state
            .record_failure(t0 + Duration::from_secs(900), &"accepted, then Internal")
            .unwrap_err()
            .to_string();
        assert!(error.contains("reconnect stalled"), "{error}");
        assert!(error.contains("3 failed attempts"), "{error}");

        // A received message restarts the timer at the next failure.
        let mut state = ReconnectState::new(Some(Duration::from_secs(900)));
        assert!(state.record_failure(t0, &"down").is_ok());
        state.record_progress();
        assert!(state
            .record_failure(t0 + Duration::from_secs(1000), &"down")
            .is_ok());
        assert!(state
            .record_failure(t0 + Duration::from_secs(1899), &"down")
            .is_ok());
        assert!(state
            .record_failure(t0 + Duration::from_secs(1900), &"down")
            .is_err());
    }

    #[test]
    fn test_failed_attempts_are_counted_and_capped_without_stall_timeout() {
        let t0 = Instant::now();
        let mut state = ReconnectState::new(None);
        for attempt in 1..MAX_FAILED_ATTEMPTS_WITHOUT_PROGRESS {
            assert!(state.record_failure(t0, &"down").is_ok());
            assert_eq!(state.failed_attempts, attempt);
        }
        let error = state.record_failure(t0, &"down").unwrap_err().to_string();
        assert!(error.contains("30 consecutive failed attempts"), "{error}");

        state.record_progress();
        assert_eq!(state.failed_attempts, 0);
        assert!(state.record_failure(t0, &"down").is_ok());
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
    async fn test_endpoint_info_retries_transient_failures_with_backoff() {
        let attempts = std::sync::Mutex::new(Vec::new());
        let response = retry_endpoint_info(
            || {
                let mut attempts = attempts.lock().unwrap();
                attempts.push(Instant::now());
                let result = if attempts.len() < 3 {
                    Err(tonic::Status::unavailable("temporary outage").into())
                } else {
                    Ok(firehose::InfoResponse {
                        chain_name: "mainnet".into(),
                        ..Default::default()
                    })
                };
                std::future::ready(result)
            },
            Duration::from_secs(1),
            Duration::from_millis(2),
        )
        .await
        .unwrap();
        assert_eq!(response.chain_name, "mainnet");
        let attempts = attempts.lock().unwrap();
        assert_eq!(attempts.len(), 3);
        assert!(attempts[1].duration_since(attempts[0]) >= Duration::from_millis(2));
        assert!(attempts[2].duration_since(attempts[1]) >= Duration::from_millis(4));
    }

    #[tokio::test]
    async fn test_endpoint_info_exhaustion_and_hanging_attempts_are_errors() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let attempts = AtomicUsize::new(0);
        let error = retry_endpoint_info(
            || {
                attempts.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Err(tonic::Status::unavailable("outage").into()))
            },
            Duration::from_millis(5),
            Duration::from_millis(1),
        )
        .await
        .unwrap_err();
        assert!(error.is::<EndpointInfoUnavailable>());
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert!(format!("{error:#}").contains("3 attempts"));

        attempts.store(0, Ordering::SeqCst);
        let error = retry_endpoint_info(
            || {
                attempts.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<Result<firehose::InfoResponse>>()
            },
            Duration::from_millis(5),
            Duration::from_millis(1),
        )
        .await
        .unwrap_err();
        assert!(error.is::<EndpointInfoUnavailable>());
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert!(format!("{error:#}").contains("timed out"));
    }

    #[tokio::test]
    async fn test_endpoint_info_rejects_fatal_statuses_and_empty_names_without_retrying() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        for status in [
            tonic::Status::unauthenticated("invalid credential"),
            tonic::Status::permission_denied("denied"),
            tonic::Status::resource_exhausted("quota exceeded"),
            tonic::Status::unimplemented("no Info RPC"),
        ] {
            let attempts = AtomicUsize::new(0);
            let error = retry_endpoint_info(
                || {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    std::future::ready(Err(status.clone().into()))
                },
                Duration::from_secs(1),
                Duration::from_millis(1),
            )
            .await;
            assert!(error.is_err());
            assert_eq!(attempts.load(Ordering::SeqCst), 1);
        }
        for chain_name in ["", " \t"] {
            let attempts = AtomicUsize::new(0);
            let error = retry_endpoint_info(
                || {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    std::future::ready(Ok(firehose::InfoResponse {
                        chain_name: chain_name.into(),
                        ..Default::default()
                    }))
                },
                Duration::from_secs(1),
                Duration::from_millis(1),
            )
            .await
            .unwrap_err();
            assert_eq!(attempts.load(Ordering::SeqCst), 1);
            assert!(error.to_string().contains("empty chain_name"));
        }
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

    #[test]
    fn test_classify_fetch_error() {
        let timeout = || -> anyhow::Error {
            FetchTimeoutError {
                block_num: 7,
                timeout: Duration::from_secs(5),
            }
            .into()
        };
        let cases: Vec<(anyhow::Error, FetchErrorKind)> = vec![
            (timeout(), FetchErrorKind::Timeout),
            (
                timeout().context("probing partition boundary"),
                FetchErrorKind::Timeout,
            ),
            (
                tonic::Status::deadline_exceeded("deadline").into(),
                FetchErrorKind::Timeout,
            ),
            (
                tonic::Status::not_found("block 7 not found").into(),
                FetchErrorKind::NotFound,
            ),
            // Proxy-wrapped upstream NotFound, as returned for skipped Solana slots.
            (
                tonic::Status::unknown(
                    "rpc error: code = NotFound desc = block not found in files",
                )
                .into(),
                FetchErrorKind::NotFound,
            ),
            (
                tonic::Status::unauthenticated("bad token").into(),
                FetchErrorKind::Fatal,
            ),
            (
                tonic::Status::permission_denied("no access").into(),
                FetchErrorKind::Fatal,
            ),
            (
                tonic::Status::unavailable("service is currently unavailable").into(),
                FetchErrorKind::Transient,
            ),
            (
                std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "connection reset by peer",
                )
                .into(),
                FetchErrorKind::Transient,
            ),
            (
                tonic::Status::internal("block index not found; storage failure").into(),
                FetchErrorKind::Unexpected,
            ),
            (
                tonic::Status::unavailable(
                    "rpc error: code = NotFound desc = block not found in files",
                )
                .into(),
                FetchErrorKind::Transient,
            ),
            (
                tonic::Status::unknown("upstream block database not found").into(),
                FetchErrorKind::Unexpected,
            ),
            (
                anyhow::anyhow!("rpc error: code = NotFound desc = block not found in files"),
                FetchErrorKind::Unexpected,
            ),
            (
                tonic::Status::unauthenticated("timeout looking up block; token not found").into(),
                FetchErrorKind::Fatal,
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(classify_fetch_error(&error), expected, "{error:#}");
        }
    }

    #[tokio::test]
    async fn test_fetch_block_identity_times_out_with_typed_error_and_reuses_channel() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        // A server that accepts TCP connections but never answers, like a stalled endpoint.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = Arc::new(AtomicUsize::new(0));
        let accept_task = tokio::spawn({
            let accepted = Arc::clone(&accepted);
            async move {
                let mut held = Vec::new();
                while let Ok((socket, _)) = listener.accept().await {
                    accepted.fetch_add(1, Ordering::SeqCst);
                    held.push(socket);
                }
            }
        });

        let client = FirehoseClient::new(test_config(&format!("http://127.0.0.1:{port}"))).unwrap();
        for block_num in [7, 8] {
            let err = client
                .fetch_block_identity(block_num, Some(Duration::from_millis(300)))
                .await
                .expect_err("a stalled endpoint must not look like a missing block");
            assert_eq!(
                classify_fetch_error(&err),
                FetchErrorKind::Timeout,
                "{err:#}"
            );
            let timeout = err
                .downcast_ref::<FetchTimeoutError>()
                .expect("typed timeout error");
            assert_eq!(timeout.block_num, block_num);
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            1,
            "both probes should share one connection"
        );
        accept_task.abort();
    }

    #[derive(Clone)]
    struct SuccessfulFetchService;

    impl tonic::server::UnaryService<firehose::SingleBlockRequest> for SuccessfulFetchService {
        type Response = firehose::SingleBlockResponse;
        type Future = tonic::codegen::BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

        fn call(&mut self, request: tonic::Request<firehose::SingleBlockRequest>) -> Self::Future {
            assert_eq!(request.metadata().get("x-api-key").unwrap(), "test-key");
            assert_eq!(
                request.metadata().get("authorization").unwrap(),
                "Bearer test-token"
            );
            Box::pin(async move {
                let Some(firehose::single_block_request::Reference::BlockNumber(number)) =
                    request.into_inner().reference
                else {
                    panic!("expected number reference")
                };
                if number.num == 9 {
                    return std::future::pending().await;
                }
                Ok(tonic::Response::new(firehose::SingleBlockResponse {
                    block: None,
                    metadata: Some(firehose::BlockMetadata {
                        num: number.num,
                        id: format!("block-{}", number.num),
                        time: Some(prost_types::Timestamp {
                            seconds: 1_700_000_000,
                            nanos: 0,
                        }),
                        ..Default::default()
                    }),
                }))
            })
        }
    }

    impl tonic::server::NamedService for SuccessfulFetchService {
        const NAME: &'static str = "sf.firehose.v2.Fetch";
    }

    impl tonic::codegen::Service<tonic::codegen::http::Request<tonic::body::Body>>
        for SuccessfulFetchService
    {
        type Response = tonic::codegen::http::Response<tonic::body::Body>;
        type Error = std::convert::Infallible;
        type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;

        fn poll_ready(
            &mut self,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(
            &mut self,
            request: tonic::codegen::http::Request<tonic::body::Body>,
        ) -> Self::Future {
            Box::pin(async {
                let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
                Ok(grpc.unary(SuccessfulFetchService, request).await)
            })
        }
    }

    #[tokio::test]
    async fn test_successful_fetches_and_timeout_recovery_share_one_connection() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let accepted = Arc::new(AtomicUsize::new(0));
        let incoming = futures::stream::unfold(
            (listener, Arc::clone(&accepted)),
            |(listener, accepted)| async {
                let socket = listener.accept().await.map(|(socket, _)| {
                    accepted.fetch_add(1, Ordering::SeqCst);
                    socket
                });
                Some((socket, (listener, accepted)))
            },
        );
        let server = tokio::spawn(async {
            tonic::transport::Server::builder()
                .add_service(SuccessfulFetchService)
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        let mut config = test_config(&endpoint);
        config.api_key = Some("test-key".into());
        config.jwt_token = Some("test-token".into());
        let client = FirehoseClient::new(config).unwrap();
        for block_num in [7, 8, 9, 10] {
            let response = client
                .fetch_block_identity(block_num, Some(Duration::from_millis(300)))
                .await;
            if block_num == 9 {
                assert_eq!(
                    classify_fetch_error(&response.unwrap_err()),
                    FetchErrorKind::Timeout
                );
            } else {
                let block = response.unwrap().unwrap();
                assert_eq!(block.block_num, block_num);
                assert_eq!(block.block_id, format!("block-{block_num}"));
            }
        }
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            1,
            "successful requests and a cancelled RPC must retain the cached channel"
        );
        server.abort();
    }

    #[derive(Clone, Copy, Debug)]
    enum CancellationScenario {
        PendingRpc,
        Idle,
        RpcFailure,
        StreamFailure,
    }

    #[derive(Clone)]
    struct CancellationService {
        scenario: CancellationScenario,
        called: std::sync::Arc<tokio::sync::Notify>,
    }

    impl tonic::server::ServerStreamingService<firehose::Request> for CancellationService {
        type Response = firehose::Response;
        type ResponseStream =
            futures::stream::BoxStream<'static, Result<firehose::Response, tonic::Status>>;
        type Future =
            tonic::codegen::BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;

        fn call(&mut self, _: tonic::Request<firehose::Request>) -> Self::Future {
            use futures::StreamExt;
            let scenario = self.scenario;
            self.called.notify_one();
            Box::pin(async move {
                match scenario {
                    CancellationScenario::PendingRpc => std::future::pending().await,
                    CancellationScenario::RpcFailure => {
                        Err(tonic::Status::unavailable("retry fixture"))
                    }
                    CancellationScenario::Idle | CancellationScenario::StreamFailure => {
                        let first = futures::stream::once(async {
                            Ok(firehose::Response {
                                block: Some(prost_types::Any {
                                    type_url: "test.Block".into(),
                                    value: vec![],
                                }),
                                step: 3,
                                cursor: "cursor-100".into(),
                                metadata: Some(firehose::BlockMetadata {
                                    num: 100,
                                    ..Default::default()
                                }),
                            })
                        });
                        let rest: Self::ResponseStream = match scenario {
                            CancellationScenario::Idle => Box::pin(futures::stream::pending()),
                            CancellationScenario::StreamFailure => {
                                Box::pin(futures::stream::once(async {
                                    Err(tonic::Status::unavailable("stream retry fixture"))
                                }))
                            }
                            _ => unreachable!(),
                        };
                        Ok(tonic::Response::new(
                            Box::pin(first.chain(rest)) as Self::ResponseStream
                        ))
                    }
                }
            })
        }
    }

    impl tonic::server::NamedService for CancellationService {
        const NAME: &'static str = "sf.firehose.v2.Stream";
    }

    impl tonic::codegen::Service<tonic::codegen::http::Request<tonic::body::Body>>
        for CancellationService
    {
        type Response = tonic::codegen::http::Response<tonic::body::Body>;
        type Error = std::convert::Infallible;
        type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;

        fn poll_ready(
            &mut self,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(
            &mut self,
            request: tonic::codegen::http::Request<tonic::body::Body>,
        ) -> Self::Future {
            let service = self.clone();
            Box::pin(async move {
                let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
                Ok(grpc.server_streaming(service, request).await)
            })
        }
    }

    #[tokio::test]
    async fn test_shutdown_interrupts_established_stream_waits_and_backoffs() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        for (scenario, idle_timeout) in [
            (CancellationScenario::PendingRpc, Some(120)),
            (CancellationScenario::Idle, Some(120)),
            (CancellationScenario::Idle, None),
            (CancellationScenario::RpcFailure, Some(120)),
            (CancellationScenario::StreamFailure, Some(120)),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let incoming = futures::stream::unfold(listener, |listener| async {
                Some((listener.accept().await.map(|(socket, _)| socket), listener))
            });
            let called = Arc::new(tokio::sync::Notify::new());
            let service = CancellationService {
                scenario,
                called: Arc::clone(&called),
            };
            let server = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(service)
                    .serve_with_incoming(incoming)
                    .await
                    .unwrap();
            });
            let mut config = test_config(&endpoint);
            config.stream_idle_timeout_secs = idle_timeout;
            let (_, metrics) = crate::metrics::init();
            let mut client = FirehoseClient::new(config).unwrap();
            client.set_metrics(metrics.clone());
            let processed = Arc::new(AtomicUsize::new(0));
            let handler_processed = Arc::clone(&processed);
            let shutdown = CancellationToken::new();
            let stream_shutdown = shutdown.clone();
            let stream_task = tokio::spawn(async move {
                client
                    .stream_blocks(None, &stream_shutdown, |_, _, _, _, _| {
                        handler_processed.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                    .await
            });
            // Observe the actual production wait state, rather than assuming
            // that a fixed delay was long enough to establish the stream.
            tokio::time::timeout(Duration::from_secs(5), async {
                called.notified().await;
                match scenario {
                    CancellationScenario::PendingRpc => {}
                    CancellationScenario::Idle => {
                        while processed.load(Ordering::SeqCst) == 0 {
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                    }
                    CancellationScenario::RpcFailure | CancellationScenario::StreamFailure => {
                        while metrics.grpc_reconnects_total.get() == 0 {
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                    }
                }
            })
            .await
            .expect("fixture must reach the selected wait");
            let started = Instant::now();
            shutdown.cancel();
            let error = tokio::time::timeout(Duration::from_secs(2), stream_task)
                .await
                .expect("cancellation must interrupt the real RPC wait or backoff")
                .unwrap()
                .unwrap_err();
            assert!(is_shutdown_error(&error), "{scenario:?}: {error:#}");
            assert!(started.elapsed() < Duration::from_secs(2));
            let expected_processed = usize::from(matches!(
                scenario,
                CancellationScenario::Idle | CancellationScenario::StreamFailure
            ));
            assert_eq!(processed.load(Ordering::SeqCst), expected_processed);
            assert_eq!(
                metrics.grpc_reconnects_total.get(),
                u64::from(matches!(
                    scenario,
                    CancellationScenario::RpcFailure | CancellationScenario::StreamFailure
                ))
            );
            server.abort();
            let _ = server.await;
        }
    }

    /// Run `stream_blocks` against `endpoint`, cancel `shutdown` after
    /// `cancel_after`, and return the result plus how long the call took.
    async fn stream_until_cancelled(
        endpoint: &str,
        cancel_after: Duration,
    ) -> (Result<()>, Duration) {
        let client = FirehoseClient::new(test_config(endpoint)).unwrap();
        let shutdown = CancellationToken::new();
        let canceller = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(cancel_after).await;
            canceller.cancel();
        });
        let started = Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            client.stream_blocks(None, &shutdown, |_, _, _, _, _| {
                panic!("no block should be delivered")
            }),
        )
        .await
        .expect("stream_blocks must return promptly after shutdown");
        (result, started.elapsed())
    }

    #[tokio::test]
    async fn test_unless_shutdown_interrupts_a_long_wait() {
        // Back-off sleeps (up to 60 s) and idle waits (120 s by default) are
        // wrapped like this one-hour sleep.
        let shutdown = CancellationToken::new();
        let canceller = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            canceller.cancel();
        });
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            unless_shutdown(&shutdown, tokio::time::sleep(Duration::from_secs(3600))),
        )
        .await
        .expect("the wait must end as soon as shutdown is requested");
        assert!(is_shutdown_error(&result.unwrap_err()));
    }

    #[tokio::test]
    async fn test_shutdown_while_reconnecting_to_a_closed_port_returns_shutdown() {
        // A closed port fails to connect at once, so the loop alternates
        // between failed connects and back-off sleeps.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let (result, elapsed) = stream_until_cancelled(
            &format!("http://127.0.0.1:{port}"),
            Duration::from_millis(200),
        )
        .await;
        let error = result.expect_err("shutdown must end the stream");
        assert!(is_shutdown_error(&error), "{error:#}");
        assert!(elapsed < Duration::from_secs(2), "took {elapsed:?}");
    }

    #[tokio::test]
    async fn test_shutdown_while_waiting_on_a_silent_server_returns_promptly() {
        // The server accepts TCP but never answers, so the client waits in
        // the connect / Blocks call (30 s and 300 s timeouts) when shutdown is
        // requested.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });

        let (result, elapsed) = stream_until_cancelled(
            &format!("http://127.0.0.1:{port}"),
            Duration::from_millis(200),
        )
        .await;
        let error = result.expect_err("shutdown must end the stream");
        assert!(is_shutdown_error(&error), "{error:#}");
        assert!(elapsed < Duration::from_secs(2), "took {elapsed:?}");
    }

    #[tokio::test]
    async fn test_already_cancelled_shutdown_returns_before_connecting() {
        let client = FirehoseClient::new(test_config("http://127.0.0.1:1")).unwrap();
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let error = client
            .stream_blocks(None, &shutdown, |_, _, _, _, _| Ok(()))
            .await
            .expect_err("shutdown must end the stream");
        assert!(is_shutdown_error(&error));
    }

    #[test]
    fn test_shutdown_error_is_detected_through_context() {
        let error = anyhow::Error::from(ShutdownRequested).context("while mapping block 42");
        assert!(is_shutdown_error(&error));
        assert!(!is_shutdown_error(&anyhow::anyhow!("shutdown requested")));
    }
}
