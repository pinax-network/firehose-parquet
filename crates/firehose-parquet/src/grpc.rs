use crate::config::Config;
use crate::cursor::{load_cursor, save_cursor};
use crate::traits::BlockIdentity;
use anyhow::{Context, Result};
use backoff::ExponentialBackoffBuilder;
use firehose_protos::firehose;
use std::sync::Arc;
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

        // Determine if TLS should be used:
        // --plaintext forces no TLS regardless of scheme.
        // Otherwise, TLS is enabled for https:// endpoints.
        let use_tls = !self.config.plaintext && self.config.endpoint.starts_with("https");

        if use_tls {
            if self.config.insecure {
                // Use custom connector that skips certificate validation.
                let channel = connect_insecure(endpoint).await
                    .with_context(|| format!("connecting (insecure) to {uri}"))?;
                info!(endpoint = %uri, insecure = true, "connected to Firehose");
                return Ok(channel);
            }
            endpoint = endpoint.tls_config(ClientTlsConfig::new())?;
        }

        let channel = endpoint
            .connect()
            .await
            .with_context(|| format!("connecting to {uri}"))?;
        info!(endpoint = %uri, tls = use_tls, "connected to Firehose");
        Ok(channel)
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
            .and_then(load_cursor);
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
                                timestamp: m.time.as_ref().map(|t| t.seconds),
                                fork_step: fork_step.clone(),
                            }
                        }).unwrap_or_default();

                        if let Some(any) = resp.block {
                            handler(any.value, any.type_url, new_cursor.clone(), identity, resp.step)?;
                        }

                        cursor = Some(new_cursor.clone());

                        if let Some(ref path) = self.config.cursor_path {
                            if let Err(e) = save_cursor(path, &new_cursor) {
                                warn!(error = %e, path = %path.display(), "failed to save cursor to file");
                            }
                        }
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

// ---------------------------------------------------------------------------
// Insecure TLS connection (--insecure flag)
// ---------------------------------------------------------------------------

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::DigitallySignedStruct;

/// A certificate verifier that accepts any server certificate without validation.
#[derive(Debug)]
struct NoCertificateVerification(Arc<rustls::crypto::CryptoProvider>);

impl ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// Connect to an endpoint with TLS but without certificate verification.
async fn connect_insecure(endpoint: Endpoint) -> Result<Channel> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let tls_config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoCertificateVerification(provider)))
        .with_no_client_auth();

    let tls_connector = tokio_rustls::TlsConnector::from(Arc::new(tls_config));

    let connector = tower::service_fn(move |uri: http::Uri| {
        let tls = tls_connector.clone();
        async move {
            let host = uri.host().unwrap_or("localhost").to_string();
            let port = uri.port_u16().unwrap_or(443);
            let addr = format!("{host}:{port}");

            let tcp = tokio::net::TcpStream::connect(addr).await?;
            let domain = ServerName::try_from(host)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
            let tls_stream = tls.connect(domain, tcp).await?;
            Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(tls_stream))
        }
    });

    endpoint
        .connect_with_connector(connector)
        .await
        .map_err(Into::into)
}
