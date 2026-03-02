use crate::config::Config;
use crate::cursor::load_cursor_parquet;
use crate::traits::BlockIdentity;
use anyhow::{Context, Result};
use backoff::ExponentialBackoffBuilder;
use firehose_protos::firehose;
use std::time::Duration;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tracing::{debug, info, warn};

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
}

impl FirehoseClient {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Build a tonic channel to the configured endpoint.
    async fn connect(&self) -> Result<Channel> {
        let uri = self.config.endpoint.clone();
        let mut endpoint = Endpoint::from_shared(uri.clone())
            .with_context(|| format!("invalid endpoint URI: {uri}"))?
            .timeout(Duration::from_secs(300))
            .connect_timeout(Duration::from_secs(30));

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
        info!(endpoint = %uri, tls = use_tls, "connected to Firehose");
        Ok(channel)
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
                key.parse().expect("API key must be valid ASCII metadata value"),
            );
        }
        if let Some(ref token) = self.config.jwt_token {
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {token}").parse().expect("JWT token must be valid ASCII metadata value"),
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

    /// Start streaming blocks. Calls `handler` for every block received as
    /// raw bytes (the `Any.value` field). The handler receives the raw bytes
    /// and the cursor string.
    ///
    /// On stream errors the client will retry with exponential back-off
    /// and resume from the last cursor.
    ///
    /// Returns when the stream is cleanly exhausted (stop block reached) or an
    /// unrecoverable error occurs.
    pub async fn stream_blocks<F>(&self, mut handler: F) -> Result<()>
    where
        F: FnMut(Vec<u8>, String, String, BlockIdentity, i32) -> Result<()>,
    {
        let mut cursor: Option<String> = self
            .config
            .cursor_path
            .as_deref()
            .and_then(load_cursor_parquet)
            .map(|state| state.cursor);
        let backoff_config = ExponentialBackoffBuilder::default()
            .with_initial_interval(Duration::from_secs(1))
            .with_max_interval(Duration::from_secs(60))
            .with_max_elapsed_time(None) // retry forever
            .build();

        let mut attempt = 0u64;

        loop {
            attempt += 1;
            let channel = match self.connect().await {
                Ok(ch) => {
                    attempt = 0;
                    ch
                }
                Err(e) => {
                    let wait = backoff::backoff::Backoff::next_backoff(
                        &mut backoff_config.clone(),
                    )
                    .unwrap_or(Duration::from_secs(60));
                    warn!(
                        attempt,
                        error = %e,
                        retry_in = ?wait,
                        "connection failed, retrying"
                    );
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
                stop_block_num: self.config.stop_block.unwrap_or(0),
                final_blocks_only: self.config.final_blocks_only,
                transforms: vec![],
            };

            let mut request = tonic::Request::new(req);
            if let Some(ref key) = self.config.api_key {
                request.metadata_mut().insert(
                    "x-api-key",
                    key.parse().unwrap(),
                );
            }
            if let Some(ref token) = self.config.jwt_token {
                request.metadata_mut().insert(
                    "authorization",
                    format!("Bearer {token}").parse().unwrap(),
                );
            }

            let stream = match client.blocks(request).await {
                Ok(resp) => resp.into_inner(),
                Err(e) => {
                    warn!(error = %e, "Blocks RPC failed, will retry");
                    tokio::time::sleep(Duration::from_secs(2u64.pow(attempt.min(5) as u32))).await;
                    continue;
                }
            };

            let mut stream = stream;

            loop {
                match stream.message().await {
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

                        let identity = resp.metadata.as_ref().map(|m| {
                            BlockIdentity {
                                block_num: m.num,
                                block_id: m.id.clone(),
                                parent_num: m.parent_num,
                                parent_id: m.parent_id.clone(),
                                lib_num: m.lib_num,
                                timestamp: m.time.as_ref().map_or(0, |t| t.seconds),
                                fork_step: fork_step.clone(),
                            }
                        }).unwrap_or_default();

                        if let Some(any) = resp.block {
                            handler(any.value, any.type_url, new_cursor.clone(), identity, resp.step)?;
                        }

                        cursor = Some(new_cursor.clone());
                    }
                    Ok(None) => {
                        info!("stream ended (stop block reached or server closed)");
                        return Ok(());
                    }
                    Err(e) => {
                        warn!(error = %e, "stream error, will reconnect");
                        break;
                    }
                }
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

