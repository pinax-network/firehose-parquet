//! Capture one finalized raw EVM block; never generates golden expectations.
use anyhow::{ensure, Context, Result};
use clap::Parser;
use firehose_parquet::{
    auth::resolve_credentials,
    config::Config,
    grpc::{CancellationToken, FirehoseClient},
};
use firehose_protos::eth;
use prost::Message;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{fs, path::PathBuf, time::Duration};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    block: u64,
    /// New staging directory. Existing paths are refused, including fixtures.
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value = "https://eth.firehose.pinax.network:443")]
    endpoint: String,
    #[arg(long)]
    api_key_envvar: Option<String>,
    #[arg(long)]
    api_token_envvar: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    capture(Args::parse()).await
}

async fn capture(args: Args) -> Result<()> {
    ensure!(
        !args.output.try_exists()?,
        "output already exists; choose a new staging directory"
    );
    ensure!(
        args.block <= i64::MAX as u64,
        "block exceeds the signed Firehose range"
    );
    let stop = args
        .block
        .checked_add(1)
        .context("exclusive stop overflow")?;
    // Only user-provided selectors authorize custom destinations. Otherwise the
    // shared resolver scopes ambient credentials to known HTTPS provider hosts.
    // This helper never loads a dotenv file.
    let credentials = resolve_credentials(
        &args.endpoint,
        args.api_key_envvar.as_deref(),
        args.api_token_envvar.as_deref(),
    )?;
    let client = FirehoseClient::new(Config {
        endpoint: args.endpoint,
        api_key: credentials.api_key,
        jwt_token: credentials.jwt_token,
        start_block: Some(args.block),
        stop_block: Some(stop),
        final_blocks_only: true,
        stream_idle_timeout_secs: Some(10),
        reconnect_stall_timeout_secs: Some(10),
        ..Default::default()
    })?;
    let mut captured = None;
    tokio::time::timeout(Duration::from_secs(45), async {
        let info = client.info().await?;
        ensure!(
            info.chain_name == "mainnet"
                || info
                    .chain_name_aliases
                    .iter()
                    .any(|alias| alias == "mainnet"),
            "endpoint is not Ethereum mainnet"
        );
        client
            .stream_blocks(
                None,
                &CancellationToken::new(),
                |bytes, type_url, _cursor, identity, step| {
                    ensure!(captured.is_none(), "received a duplicate block");
                    ensure!(
                        identity.block_num == args.block && step == 3,
                        "expected the exact finalized block"
                    );
                    ensure!(
                        type_url.ends_with("/sf.ethereum.type.v2.Block")
                            || type_url == "sf.ethereum.type.v2.Block",
                        "unexpected protobuf type"
                    );
                    ensure!(
                        bytes.len() <= 16 * 1024 * 1024,
                        "fixture exceeds 16 MiB; choose a smaller block"
                    );
                    let block = eth::Block::decode(bytes.as_slice())?;
                    let header = block.header.as_ref().context("block has no header")?;
                    let hash = block
                        .hash
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect::<String>();
                    let parent = header
                        .parent_hash
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect::<String>();
                    ensure!(
                        block.number == args.block
                            && identity.block_id.trim_start_matches("0x") == hash,
                        "payload identity differs from Firehose metadata"
                    );
                    ensure!(
                        identity.parent_id.trim_start_matches("0x") == parent,
                        "payload parent differs from Firehose metadata"
                    );
                    let timestamp = header
                        .timestamp
                        .as_ref()
                        .context("block has no timestamp")?;
                    ensure!(
                        timestamp.seconds == identity.timestamp
                            && timestamp.nanos == identity.timestamp_nanos,
                        "payload timestamp differs from Firehose metadata"
                    );
                    let metadata = json!({
                        "format_version": 1,
                        "chain": "mainnet",
                        "protobuf_type": type_url,
                        "sha256": format!("{:x}", Sha256::digest(&bytes)),
                        "byte_length": bytes.len(),
                        "identity": {"block_num": identity.block_num, "block_id": identity.block_id,
                            "parent_num": identity.parent_num, "parent_id": identity.parent_id,
                            "lib_num": identity.lib_num, "timestamp": identity.timestamp,
                            "timestamp_nanos": identity.timestamp_nanos},
                        "fork_step": "FINAL"
                    });
                    captured = Some((bytes, metadata));
                    Ok(())
                },
            )
            .await
    })
    .await
    .context("capture exceeded the 45-second total deadline")??;
    let (bytes, metadata) = captured.context("stream ended without the requested block")?;
    fs::create_dir(&args.output).context("creating new staging directory")?;
    fs::write(args.output.join("block.pb"), &bytes)?;
    fs::write(
        args.output.join("metadata.json"),
        format!("{}\n", serde_json::to_string_pretty(&metadata)?),
    )?;
    println!(
        "Captured block {}: {} raw protobuf bytes; review expectations separately.",
        args.block,
        bytes.len()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use firehose_protos::firehose;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    #[derive(Clone)]
    struct InfoService(Arc<AtomicUsize>);
    impl tonic::server::UnaryService<firehose::InfoRequest> for InfoService {
        type Response = firehose::InfoResponse;
        type Future = tonic::codegen::BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
        fn call(&mut self, request: tonic::Request<firehose::InfoRequest>) -> Self::Future {
            let forbidden = request.metadata().contains_key("x-api-key")
                || request.metadata().contains_key("authorization");
            self.0
                .store(if forbidden { 2 } else { 1 }, Ordering::SeqCst);
            Box::pin(async {
                // Refuse before any block stream; this test checks auth routing.
                Ok(tonic::Response::new(firehose::InfoResponse {
                    chain_name: "foreign-chain".into(),
                    ..Default::default()
                }))
            })
        }
    }
    impl tonic::server::NamedService for InfoService {
        const NAME: &'static str = "sf.firehose.v2.EndpointInfo";
    }
    impl tonic::codegen::Service<tonic::codegen::http::Request<tonic::body::Body>> for InfoService {
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
                Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                    .unary(service, request)
                    .await)
            })
        }
    }

    #[tokio::test]
    async fn changing_endpoint_alone_never_sends_ambient_provider_credentials() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let observed = Arc::new(AtomicUsize::new(0));
        let incoming = futures::stream::unfold(listener, |listener| async {
            Some((listener.accept().await.map(|(socket, _)| socket), listener))
        });
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(InfoService(observed.clone()))
                .serve_with_incoming(incoming),
        );
        // A child process isolates fixture environment variables from parallel
        // tests, and exercises the real argument parser + shared auth resolver.
        let mut child = tokio::process::Command::new(std::env::current_exe().unwrap());
        child
            .args(["--exact", "tests::capture_auth_child", "--ignored"])
            .env_clear()
            .env("FIREPARQ_CAPTURE_TEST_ENDPOINT", endpoint)
            .env("PINAX_API_KEY", "fixture-key-never-forward")
            .env("PINAX_API_TOKEN", "fixture-token-never-forward")
            .env("SUBSTREAMS_API_KEY", "fixture-legacy-key-never-forward")
            .env("SUBSTREAMS_API_TOKEN", "fixture-legacy-token-never-forward")
            .kill_on_drop(true);
        let output = tokio::time::timeout(Duration::from_secs(5), child.output())
            .await
            .unwrap()
            .unwrap();
        server.abort();
        assert!(
            output.status.success(),
            "child failed: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert_eq!(
            observed.load(Ordering::SeqCst),
            1,
            "endpoint received no request or received a credential header"
        );
    }

    #[test]
    #[ignore = "subprocess fixture invoked by the auth-routing test"]
    fn capture_auth_child() {
        let endpoint = std::env::var("FIREPARQ_CAPTURE_TEST_ENDPOINT").unwrap();
        let temp = tempfile::tempdir().unwrap();
        let output = temp.path().join("capture");
        let args = Args::try_parse_from([
            "refresh_evm_golden",
            "--block",
            "26049575",
            "--endpoint",
            &endpoint,
            "--output",
            output.to_str().unwrap(),
        ])
        .unwrap();
        assert!(args.api_key_envvar.is_none() && args.api_token_envvar.is_none());
        let error = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(capture(args))
            .unwrap_err();
        assert!(error.to_string().contains("not Ethereum mainnet"));
        assert!(!output.exists());
    }
}
