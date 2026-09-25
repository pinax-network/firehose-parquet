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
    #[arg(long, default_value = "PINAX_API_KEY")]
    api_key_envvar: String,
    #[arg(long, default_value = "PINAX_API_TOKEN")]
    api_token_envvar: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
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
    // Explicit selectors and no dotenv loading prevent unrelated legacy tokens
    // from being picked up when capturing from the default provider.
    let credentials = resolve_credentials(
        &args.endpoint,
        Some(&args.api_key_envvar),
        Some(&args.api_token_envvar),
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
