# Getting started

Install `fireparq`, run a first `build` and read what it wrote. The Firehose credentials it sends are described in [Authentication](authentication.md).

> `v0.5.0+` renames the installed CLI binary from `firehose-parquet` to `fireparq`. The repository/crate names and Parquet metadata namespace remain `firehose-parquet.*`.

## Install

Each GitHub release attaches `fireparq` binaries for Linux and macOS
(`x86_64` and `aarch64`), with build provenance attestations:

```bash
curl -LO https://github.com/pinax-network/firehose-parquet/releases/download/v1.2.0/fireparq-linux-x86_64.tar.gz
tar xzf fireparq-linux-x86_64.tar.gz
./fireparq-linux-x86_64/fireparq --version
```

The other archives are `fireparq-linux-aarch64`, `fireparq-macos-x86_64` and
`fireparq-macos-aarch64`. To build from source instead, run
`cargo install --path blocks` or use the commands below. A
[Docker image](#docker) is also published.

## Run

```bash
# Build
cargo build --release --workspace

# Stream Solana blocks to Delta tables (auto-detect chain). --output is the dataset
# root; {chain} names a directory after the endpoint's chain_name, so these
# examples write ./output/solana-mainnet-beta/, ./output/mainnet/, ...
./target/release/fireparq build \
  --network solana-mainnet-beta \
  --start-block 200000000 \
  --stop-block 200001000 \
  --output './output/{chain}' \
  --compression zstd

# Or use an explicit endpoint directly
./target/release/fireparq build \
  --endpoint https://solana.firehose.pinax.network:443 \
  --start-block 200000000 \
  --stop-block 200001000 \
  --output './output/{chain}' \
  --compression zstd

# Disable Solana vote transactions explicitly
./target/release/fireparq build \
  --network solana-mainnet-beta \
  --start-block 200000000 \
  --stop-block 200001000 \
  --without-votes \
  --output './output/{chain}'

# Disable extended EVM tables explicitly (explicit block type)
./target/release/fireparq build \
  --block-type evm \
  --endpoint https://eth.firehose.pinax.network:443 \
  --start-block 19000000 \
  --stop-block 19001000 \
  --without-extended \
  --output './output/{chain}'

# Write to S3: the output must be an explicit s3:// URI, and S3 output needs
# AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (or the matching flags). Several
# networks share this bucket: the dataset is s3://my-bucket/v1/mainnet
./target/release/fireparq build \
  --network mainnet \
  --start-block 20000000 \
  --stop-block 20001000 \
  --output 's3://my-bucket/v1/{chain}'

# One bucket per network: the dataset is the bucket root
# (s3://ethereum-mainnet/blocks/..., see single-network buckets)
./target/release/fireparq build \
  --network mainnet \
  --start-block 20000000 \
  --stop-block 20001000 \
  --output s3://ethereum-mainnet

# Stream Antelope blocks
./target/release/fireparq build \
  --block-type antelope \
  --endpoint https://eos.firehose.pinax.network:443 \
  --start-block 1000000 \
  --stop-block 1001000 \
  --output './output/{chain}'

# Stream HyperCore (PINAX_API_KEY holds the key). Its data is known from
# 2026-01-01: without --start-block a new root starts at block 846903317, and
# an earlier start is refused (docs/chains/hypercore.md)
./target/release/fireparq build \
  --network hypercore \
  --stop-block 846904317 \
  --output './output/{chain}'

# Backfill from a block and keep following finalized blocks
./target/release/fireparq build \
  --network solana-mainnet-beta \
  --start-block 250000000 \
  --output './output/{chain}'

# Start live mode from the endpoint's first streamable block
./target/release/fireparq build \
  --network mainnet \
  --output './output/{chain}'

# Add verbose operational logs for debugging without changing normal output by default
./target/release/fireparq build \
  --network mainnet \
  --start-block 20000000 \
  --stop-block 20001000 \
  --output './output/{chain}' \
  --verbose
```

Read what a run wrote with DuckDB 1.5 or later, through each table's Delta log
([Reading the tables](reading-tables.md) has Polars and S3 too):

```bash
duckdb -c "INSTALL delta; LOAD delta;
  SELECT count(*), min(block_num), max(block_num), min(date), max(date)
  FROM delta_scan('output/solana-mainnet-beta/blocks')"
```

## Docker

The image is published to GitHub Container Registry for each release tag; this
release is tagged `1.2.0`, `1.2`, `1` and `latest`. The image path stays
`ghcr.io/pinax-network/firehose-parquet`, and the container entrypoint runs
`fireparq`.

```bash
docker pull ghcr.io/pinax-network/firehose-parquet:1.2.0

docker run --rm \
  -e PINAX_API_KEY=your-key \
  -v $(pwd)/output:/output \
  ghcr.io/pinax-network/firehose-parquet:1.2.0 \
  build \
  --endpoint https://eth.firehose.pinax.network:443 \
  --start-block 19000000 \
  --stop-block 19001000 \
  --output /output
```

The same image runs the Delta maintenance job, `fireparq maintenance`
([Delta maintenance](delta-maintenance.md)).
