//! Replay fixtures: one captured `EndpointInfo` and up to 200 consecutive
//! finalized blocks, without provider cursors, in one zstd file.
use anyhow::{bail, ensure, Context, Result};
use firehose_parquet::{
    auth::resolve_credentials,
    config::Config,
    grpc::{CancellationToken, FirehoseClient},
};
use firehose_protos::firehose;
use prost::Message;
use serde_json::json;
use std::{
    fs,
    io::{Read, Write},
    path::Path,
    time::Duration,
};

const MAGIC: &[u8] = b"fireparq-658-fixture-v1\n";
/// The benchmark only needs a small sample; looping supplies the volume.
pub const MAX_CAPTURE_BLOCKS: u64 = 200;
/// Never set; selecting it keeps the ambient bearer token out of the request.
const NO_TOKEN_ENVVAR: &str = "FIREPARQ_BENCH_658_NO_TOKEN";

pub struct Fixture {
    pub info: firehose::InfoResponse,
    pub blocks: Vec<firehose::Response>,
}

impl Fixture {
    pub fn read(path: &Path) -> Result<Self> {
        let mut bytes = Vec::new();
        zstd::stream::read::Decoder::new(fs::File::open(path)?)?.read_to_end(&mut bytes)?;
        let mut buffer = bytes
            .strip_prefix(MAGIC)
            .context("not a bench_live_flush fixture")?;
        let info = firehose::InfoResponse::decode_length_delimited(&mut buffer)?;
        let mut blocks = Vec::new();
        while !buffer.is_empty() {
            blocks.push(firehose::Response::decode_length_delimited(&mut buffer)?);
        }
        ensure!(blocks.len() >= 2, "a fixture needs at least two blocks");
        for pair in blocks.windows(2) {
            let (a, b) = (metadata(&pair[0]), metadata(&pair[1]));
            ensure!(b.num == a.num + 1, "fixture blocks must be consecutive");
        }
        Ok(Self { info, blocks })
    }

    /// Mean spacing of the captured block timestamps, in nanoseconds.
    pub fn block_interval_ns(&self) -> u64 {
        let first = timestamp_ns(metadata(&self.blocks[0]));
        let last = timestamp_ns(metadata(self.blocks.last().unwrap()));
        ((last - first).max(1) as u64) / (self.blocks.len() as u64 - 1)
    }

    pub fn payload_bytes(&self) -> usize {
        self.blocks
            .iter()
            .map(|block| block.block.as_ref().map_or(0, |any| any.value.len()))
            .sum()
    }
}

pub fn metadata(response: &firehose::Response) -> &firehose::BlockMetadata {
    response.metadata.as_ref().expect("fixture block metadata")
}

pub fn timestamp_ns(metadata: &firehose::BlockMetadata) -> i128 {
    let time = metadata.time.as_ref().expect("fixture block time");
    time.seconds as i128 * 1_000_000_000 + time.nanos as i128
}

/// Only built-in Pinax HTTPS hosts on port 443 may be captured from.
fn require_pinax(endpoint: &str) -> Result<()> {
    let rest = endpoint
        .strip_prefix("https://")
        .context("capture requires an https:// Pinax endpoint")?;
    let (host, port) = rest
        .trim_end_matches('/')
        .rsplit_once(':')
        .context("capture requires an explicit :443 port")?;
    ensure!(
        port == "443" && host.ends_with(".firehose.pinax.network") && !host.contains(['@', '/']),
        "capture only talks to *.firehose.pinax.network:443"
    );
    Ok(())
}

pub async fn capture(
    endpoint: &str,
    start: u64,
    count: u64,
    output: &Path,
    api_key_envvar: &str,
) -> Result<()> {
    require_pinax(endpoint)?;
    ensure!(!output.try_exists()?, "output already exists");
    ensure!(
        (2..=MAX_CAPTURE_BLOCKS).contains(&count),
        "capture 2..={MAX_CAPTURE_BLOCKS} blocks"
    );
    let stop = start.checked_add(count).context("stop overflow")?;
    let credentials = resolve_credentials(endpoint, Some(api_key_envvar), Some(NO_TOKEN_ENVVAR))?;
    ensure!(
        credentials.api_key.is_some(),
        "{api_key_envvar} is not set in this shell"
    );
    let client = FirehoseClient::new(Config {
        endpoint: endpoint.into(),
        api_key: credentials.api_key,
        jwt_token: None,
        start_block: Some(start),
        stop_block: Some(stop),
        final_blocks_only: true,
        stream_idle_timeout_secs: Some(30),
        reconnect_stall_timeout_secs: Some(60),
        ..Default::default()
    })?;
    let info = client.info().await?;
    let info = firehose::InfoResponse {
        chain_name: info.chain_name,
        chain_name_aliases: info.chain_name_aliases,
        first_streamable_block_num: info.first_streamable_block_num,
        first_streamable_block_id: info.first_streamable_block_id,
        block_id_encoding: info.block_id_encoding,
        block_features: info.block_features,
    };
    let mut blocks = Vec::new();
    let mut next = start;
    tokio::time::timeout(Duration::from_secs(600), async {
        client
            .stream_blocks(
                None,
                &CancellationToken::new(),
                |bytes, type_url, _cursor, identity, step| {
                    ensure!(
                        identity.block_num == next && step == 3,
                        "expected consecutive finalized blocks"
                    );
                    next += 1;
                    // The provider cursor is dropped: it is opaque and private.
                    blocks.push(firehose::Response {
                        block: Some(prost_types::Any {
                            type_url,
                            value: bytes,
                        }),
                        step,
                        cursor: String::new(),
                        metadata: Some(firehose::BlockMetadata {
                            num: identity.block_num,
                            id: identity.block_id.clone(),
                            parent_num: identity.parent_num,
                            parent_id: identity.parent_id.clone(),
                            lib_num: identity.lib_num,
                            time: Some(prost_types::Timestamp {
                                seconds: identity.timestamp,
                                nanos: identity.timestamp_nanos,
                            }),
                            ..Default::default()
                        }),
                    });
                    Ok(())
                },
            )
            .await
    })
    .await
    .context("capture exceeded its deadline")??;
    if blocks.len() as u64 != count {
        bail!("captured {} of {count} blocks", blocks.len());
    }
    let mut encoded = MAGIC.to_vec();
    info.encode_length_delimited(&mut encoded)?;
    for block in &blocks {
        block.encode_length_delimited(&mut encoded)?;
    }
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)?;
    file.write_all(&zstd::stream::encode_all(encoded.as_slice(), 19)?)?;
    file.sync_all()?;
    let fixture = Fixture::read(output)?;
    println!(
        "{}",
        json!({
            "endpoint": endpoint,
            "chain_name": fixture.info.chain_name,
            "block_features": fixture.info.block_features,
            "range": [start, stop],
            "blocks": fixture.blocks.len(),
            "raw_payload_bytes": fixture.payload_bytes(),
            "fixture_bytes": fs::metadata(output)?.len(),
            "block_interval_ms": fixture.block_interval_ns() as f64 / 1e6,
        })
    );
    Ok(())
}
