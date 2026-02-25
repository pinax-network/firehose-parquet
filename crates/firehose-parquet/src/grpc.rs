use crate::config::Config;
use crate::firehose;
use crate::solana;
use anyhow::{Context, Result};
use backoff::ExponentialBackoffBuilder;
use prost::Message;
use std::time::Duration;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tracing::{debug, info, warn};

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

        if self.config.endpoint.starts_with("https") {
            endpoint = endpoint.tls_config(ClientTlsConfig::new())?;
        }

        let channel = endpoint
            .connect()
            .await
            .with_context(|| format!("connecting to {uri}"))?;
        info!(endpoint = %uri, "connected to Firehose");
        Ok(channel)
    }

    /// Start streaming blocks.  Calls `handler` for every decoded Solana
    /// block.  On stream errors the client will retry with exponential back-off
    /// and resume from the last cursor.
    ///
    /// Returns when the stream is cleanly exhausted (stop block reached) or an
    /// unrecoverable error occurs.
    pub async fn stream_blocks<F>(&self, mut handler: F) -> Result<()>
    where
        F: FnMut(solana::Block, String) -> Result<()>,
    {
        let mut cursor: Option<String> = self.config.cursor.clone();
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

            let mut client = firehose::stream_client::StreamClient::new(channel);

            let start_block_num = match &cursor {
                // When we have a cursor the server ignores start_block_num, but
                // we still provide it as a hint.
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
            if let Some(ref token) = self.config.api_token {
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

            // Use a pinned stream for iteration
            let mut stream = stream;

            loop {
                match stream.message().await {
                    Ok(Some(resp)) => {
                        let new_cursor = resp.cursor.clone();
                        debug!(cursor = %new_cursor, step = ?resp.step, "received response");

                        // Only process NEW and FINAL steps
                        if resp.step == firehose::ForkStep::StepUndo as i32 {
                            cursor = Some(new_cursor);
                            continue;
                        }

                        if let Some(any) = resp.block {
                            let block = solana::Block::decode(any.value.as_ref())
                                .context("decoding Solana block from Any")?;
                            handler(block, new_cursor.clone())?;
                        }

                        cursor = Some(new_cursor);
                    }
                    Ok(None) => {
                        info!("stream ended (stop block reached or server closed)");
                        return Ok(());
                    }
                    Err(e) => {
                        warn!(error = %e, "stream error, will reconnect");
                        break; // break inner loop to reconnect
                    }
                }
            }

            // Small delay before reconnecting
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
