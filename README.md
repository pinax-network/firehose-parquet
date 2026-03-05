# firehose-parquet

A production-grade Rust toolkit that consumes [StreamingFast Firehose](https://firehose.streamingfast.io/) v2 gRPC streams and writes **Apache Parquet** files. A single unified binary (`firehose-parquet`) supports multiple blockchain types with automatic chain detection.

## Supported Chains

| `--block-type` | Endpoint Example | Tables |
|---|---|---|
| `evm` | `eth.firehose.pinax.network:443` | blocks, transactions, logs |
| `evm --extended` | | + calls, balance_changes, code_changes, storage_changes, nonce_changes, gas_changes, account_creations |
| `solana` | `solana.firehose.pinax.network:443` | blocks, transactions, messages, instructions, rewards |
| `bitcoin` | `btc.firehose.pinax.network:443` | blocks, transactions, inputs, outputs |
| `beacon` | `beacon.firehose.pinax.network:443` | blocks, attestations, deposits, proposer_slashings, attester_slashings, voluntary_exits, execution_payload, blob_sidecars |
| `tron` | `tron.firehose.pinax.network:443` | blocks, transactions, logs, internal_transactions |
| `cosmos` | `cosmoshub.firehose.pinax.network:443` | blocks, transactions, events, messages |
| `antelope` | `eos.firehose.pinax.network:443` | blocks, transactions, actions, db_ops |
| `near` | `near.firehose.pinax.network:443` | blocks, chunks, transactions, receipts, state_changes |

> **Tip:** Use `--block-type auto` (the default) to auto-detect the chain from the Firehose stream's protobuf `type_url`.

## Features

- **Single binary** — one `firehose-parquet` binary handles all chains via `--block-type` with auto-detection
- **Multi-chain** — pluggable `BlockMapper` trait with per-chain mapper modules
- **Canonical identity columns** — `block_num`, `block_id`, `parent_num`, `parent_id`, `lib_num`, `timestamp` on every table (from Firehose `BlockMetadata`)
- **gRPC streaming** — connects to any Firehose v2 endpoint via tonic, with TLS and API key / JWT auth
- **Automatic retry / resume** — exponential back-off on connection errors; resumes from the last cursor
- **Recovery guardrails** — optional stream idle timeout and reconnect stall timeout to force self-recovery or fail-fast restarts
- **Cursor persistence** — pipeline state saved as `cursor.parquet` with full parameter validation on resume
- **S3-aware cursor** — cursor automatically stored alongside output (local or S3)
- **Partitioning** — `none`, `block_range`, `date`, `hour`, `minute`, or `second` layouts
- **File rollover** — flush by row count, byte size, or time interval
- **Fork handling** — `--final-blocks-only` (default) or include `fork_step` column (`NEW`/`UNDO`/`FINAL`)
- **Failed transaction filtering** — `--include-failed-transactions` to opt in to failed/reverted txs (excluded by default)
- **Byte encoding** — configurable encoding for binary fields: `binary` (raw), `hex`, `hex_no_prefix`, `base58`, `tron_base58`, `auto`
- **Compression** — zstd (default), snappy, gzip, or none
- **Parquet file metadata** — every file embeds pipeline provenance (`firehose-parquet.*` key-value pairs) in the Parquet footer
- **Prometheus metrics** — opt-in `/metrics` endpoint for monitoring throughput, buffer state, and errors
- **Graceful shutdown** — SIGINT/SIGTERM flush all buffers and save cursor before exit
- **Docker support** — multi-stage Dockerfile, published to GHCR
- **Arrow-native pipeline** — column builders produce `RecordBatch`es that flush to Parquet

## Quick Start

```bash
# Build
cargo build --release --workspace

# Stream Solana blocks to Parquet (auto-detect chain)
./target/release/firehose-parquet \
  --endpoint https://solana.firehose.pinax.network:443 \
  --start-block 200000000 \
  --stop-block 200001000 \
  --output ./output \
  --partition date \
  --compression zstd

# Stream EVM blocks with extended traces (explicit block type)
./target/release/firehose-parquet \
  --block-type evm \
  --endpoint https://eth.firehose.pinax.network:443 \
  --start-block 19000000 \
  --stop-block 19001000 \
  --extended \
  --bytes-encoding hex
```

### Docker

The image is published to GitHub Container Registry on each release:

```bash
docker pull ghcr.io/pinax-network/firehose-parquet:latest

docker run --rm \
  -e SUBSTREAMS_API_KEY=your-key \
  -v $(pwd)/output:/output \
  ghcr.io/pinax-network/firehose-parquet \
  --endpoint https://eth.firehose.pinax.network:443 \
  --start-block 19000000 \
  --stop-block 19001000 \
  --output /output \
  --partition date
```

## Cursor & Resume

`firehose-parquet` persists pipeline state in a `cursor.parquet` file so streams can be interrupted and resumed without re-processing blocks. The cursor system provides deterministic, crash-safe resume with full parameter validation.

### How It Works

1. **Synchronized flush** — when any table triggers a file rollover (partition change or size threshold), *all* tables are flushed together. This ensures every table is consistent at the cursor point.
2. **Cursor saved after writes** — `cursor.parquet` is only updated *after* all table files have been successfully written to disk (or S3). If the process crashes mid-write, the cursor still points to the last complete flush.
3. **Resume from cursor** — on startup, if `cursor.parquet` exists, the pipeline sends the stored Firehose cursor token to resume the gRPC stream exactly where it left off.

### Cursor File Format

The cursor is stored as a single-row Parquet file with two layers of data:

**Row data** (essential resume state):

| Column | Type | Description |
|---|---|---|
| `cursor` | Utf8 | Firehose opaque cursor token |
| `last_block_num` | UInt64 | Last processed block number |
| `last_block_id` | Binary | Last processed block ID (raw bytes) |
| `updated_at` | Utf8 | ISO 8601 timestamp of last save |
| `start_block` | UInt64 (nullable) | Pipeline start block |
| `stop_block` | UInt64 (nullable) | Pipeline stop block (exclusive) |
| `extended` | Boolean | Whether extended mode was enabled |
| `final_blocks_only` | Boolean | Whether only finalized blocks were processed |
| `include_failed_transactions` | Boolean | Whether failed txs were included |

**File-level metadata** (Parquet key-value pairs in `firehose-parquet.*` namespace):

Pipeline configuration and firehose endpoint metadata are embedded in the file footer — same convention as table files. This includes `endpoint`, `chain_name`, `partition`, `compression`, `bytes_encoding`, and more.

### S3-Aware Cursor

When output is written to S3, the cursor file is automatically placed alongside the data in the same S3 bucket/prefix — no special configuration needed:

| Output | `--cursor` value | Cursor location |
|---|---|---|
| `./output` | *(default)* | `./cursor.parquet` |
| `s3://bucket/prefix` | *(default)* | `s3://bucket/prefix/cursor.parquet` |
| `s3://bucket/prefix` | `my-cursor.parquet` | `s3://bucket/prefix/my-cursor.parquet` |
| `s3://bucket/prefix` | `s3://other/path.parquet` | `s3://other/path.parquet` |

### Parameter Validation on Resume

When resuming from an existing `cursor.parquet`, the pipeline validates that the current CLI parameters match those stored in the cursor. Checked parameters include:

- `start_block`, `stop_block`, `extended`, `final_blocks_only`, `include_failed_transactions`
- `endpoint`, `partition`, `block_range_size`, `compression`, `bytes_encoding` (from file metadata)

If any parameter differs, the pipeline exits with a clear error showing the mismatches. Use `--cursor-override` to force resume with the current parameters (e.g. when intentionally changing `stop_block`).

```bash
# Force resume despite parameter changes
firehose-parquet \
  --endpoint https://eth.firehose.pinax.network:443 \
  --cursor cursor.parquet \
  --cursor-override \
  --stop-block 20000000
```

### Graceful Shutdown

On SIGINT (Ctrl-C) or SIGTERM, the pipeline:

1. Stops consuming new blocks from the gRPC stream
2. Flushes all in-memory buffers to Parquet files
3. Saves the cursor for the last successfully written block
4. Exits cleanly

This prevents corrupted or partial files and ensures the next run resumes from a consistent point.

## CLI Reference

```
$ firehose-parquet --help

Convert Firehose gRPC stream to Apache Parquet

Usage: firehose-parquet [OPTIONS] [COMMAND]

Commands:
  completions  Generate shell completions for the given shell
  scan         Read and inspect Parquet files (schema, row counts, sample rows)
  inspect      Display full metadata for a single Parquet file
  validate     Check partition integrity (gaps, ordering, duplicates)
  rollup       Roll up fine-grained partitions into coarser ones (e.g. minute → date)
  merge        Consolidate small part files within each partition into larger files
  truncate     Delete parquet files with optional partition filtering
  help         Print this message or the help of the given subcommand(s)

Options:
      --log-level <LOG_LEVEL>  Log level: trace, debug, info, warn, error [env: LOG_LEVEL] [default: info]
      --dry-run                Decode and map but don't write files [env: DRY_RUN]
  -h, --help                   Print help
  -V, --version                Print version

Connection:
  -e, --endpoint <ENDPOINT>
          Firehose gRPC endpoint URL [env: ENDPOINT]
      --api-key-envvar <API_KEY_ENVVAR>
          Name of environment variable containing the API key for authentication [env: API_KEY_ENVVAR] [default: SUBSTREAMS_API_KEY]
      --api-token-envvar <API_TOKEN_ENVVAR>
          Name of environment variable containing the JWT bearer token for authentication [env: API_TOKEN_ENVVAR] [default: SUBSTREAMS_API_TOKEN]
      --metrics-port <METRICS_PORT>
          Prometheus /metrics HTTP port [env: METRICS_PORT]
      --stream-idle-timeout-secs <STREAM_IDLE_TIMEOUT_SECS>
          Force a reconnect if no stream message is received for N seconds [env: STREAM_IDLE_TIMEOUT_SECS] [default: 120]
      --reconnect-stall-timeout-secs <RECONNECT_STALL_TIMEOUT_SECS>
          Exit with an error if reconnecting continuously for N seconds [env: RECONNECT_STALL_TIMEOUT_SECS] [default: 900]

Block Range:
  -s, --start-block <START_BLOCK>  Start block number (inclusive) [env: START_BLOCK]
  -t, --stop-block <STOP_BLOCK>    Stop block number (exclusive, 0 = stream forever) [env: STOP_BLOCK]
  -c, --cursor <CURSOR>            Path to cursor file for resuming a previous session [env: CURSOR]
      --cursor-override            Override cursor parameter validation on resume [env: CURSOR_OVERRIDE]
      --final-blocks-only          Only process finalized blocks (when false, adds fork_step column) [env: FINAL_BLOCKS_ONLY]

Output:
      --output <OUTPUT>
          Output directory [env: OUTPUT] [default: .]
      --partition <PARTITION>
          Partitioning mode: none, block_range, date, hour, minute, second [env: PARTITION] [default: none]
      --block-range-size <BLOCK_RANGE_SIZE>
          Block range size when partition=block_range [env: BLOCK_RANGE_SIZE] [default: 10000]
      --compression <COMPRESSION>
          Compression codec: zstd, snappy, gzip, none [env: COMPRESSION] [default: zstd]

Flush:
      --flush-rows <FLUSH_ROWS>
          Max rows per file before flush (disabled by default) [env: FLUSH_ROWS]
      --flush-bytes <FLUSH_BYTES>
          Max bytes per file before flush [env: FLUSH_BYTES] [default: 134217728]
      --flush-interval-secs <FLUSH_INTERVAL_SECS>
          Time-based flush interval in seconds (disabled by default) [env: FLUSH_INTERVAL_SECS]

AWS / S3:
      --aws-access-key-id <AWS_ACCESS_KEY_ID>
          AWS access key ID (for S3 output) [env: AWS_ACCESS_KEY_ID]
      --aws-secret-access-key <AWS_SECRET_ACCESS_KEY>
          AWS secret access key (for S3 output) [env: AWS_SECRET_ACCESS_KEY]
      --aws-session-token <AWS_SESSION_TOKEN>
          AWS session token (for S3 output) [env: AWS_SESSION_TOKEN]
      --aws-region <AWS_REGION>
          AWS region (for S3 output) [env: AWS_REGION]
      --aws-endpoint-url <AWS_ENDPOINT_URL_S3>
          AWS endpoint URL (for S3-compatible services) [env: AWS_ENDPOINT_URL_S3]
      --s3-bucket <S3_BUCKET>
          S3 bucket name (when set, output is written to s3://<bucket>/<output>) [env: S3_BUCKET]
      --cache-control <CACHE_CONTROL>
          Cache-Control header for S3 uploads (empty string = no header) [env: CACHE_CONTROL] [default: "public, max-age=31536000, immutable"]

Chain:
      --block-type <BLOCK_TYPE>
          Block type to process. Use "auto" to detect from the Firehose stream.
          Options: auto, evm, bitcoin, solana, near, antelope, cosmos, tron, beacon [env: BLOCK_TYPE] [default: auto]
      --extended
          Enable extended detail level (EVM only: calls, balance_changes, etc.) [env: EXTENDED]
      --bytes-encoding <BYTES_ENCODING>
          Byte encoding strategy for binary fields (hashes, addresses, etc.)
          Options: binary (raw bytes), hex (0x-prefixed), base58, tron_base58, auto (chain-appropriate) [env: BYTES_ENCODING] [default: auto]
      --include-failed-transactions
          Include failed/reverted transactions in output (default: false) [env: INCLUDE_FAILED_TRANSACTIONS]
```

## Subcommands

### `scan` — Inspect Parquet Files

Read and inspect Parquet files: shows schema, row counts, and sample rows. Supports local paths and S3 URIs.

```bash
firehose-parquet scan ./output/blocks/
firehose-parquet scan s3://my-bucket/evm/blocks/
```

### `inspect` — Display File Metadata

Displays comprehensive metadata for a single Parquet file: file-level key-value pairs (including custom `firehose-parquet.*` entries), the full Parquet schema with physical/logical types, row group statistics, and per-column chunk details (encoding, compression, sizes). Supports local paths and S3 URIs.

```bash
# Inspect a local file
firehose-parquet inspect ./output/blocks/year=2026/month=01/date=15/part-000001.parquet

# Inspect an S3 file
firehose-parquet inspect s3://my-bucket/evm/blocks/year=2026/month=01/date=15/part-000001.parquet
```

**Output includes:**

| Section | Details |
|---|---|
| **File info** | Total rows, row groups, columns, file size, created_by, Parquet version |
| **File metadata** | All key-value pairs stored in the Parquet footer |
| **Schema** | Physical types, logical types (e.g. String, Timestamp), repetition levels, nested groups |
| **Row groups** | Per-group row count, compressed/uncompressed size, compression ratio |
| **Column details** | Per-column encoding, compression codec, compressed/uncompressed size, ratio |

### `validate` — Check Partition Integrity

Validates partitioned Parquet data for gaps, ordering errors, duplicates, parent hash mismatches, and timestamp reversals. Only partitions with issues are printed; valid ones are silently counted.

```bash
firehose-parquet validate ./output/blocks/
firehose-parquet validate s3://my-bucket/evm/blocks/

# Solana: allow skipped slots (normal chain behavior, not data corruption)
firehose-parquet validate s3://my-bucket/solana/blocks/ --allow-gaps
```

| Flag | Default | Description |
|---|---|---|
| `--cross-partition` | `false` | Check continuity between adjacent partitions |
| `--allow-gaps` | `false` | Suppress gap reporting (useful for Solana skipped slots) |

### `rollup` — Roll Up Partitions

Rolls up fine-grained partitions (e.g. `minute` or `hour`) into coarser ones (e.g. `date`). Reads source files, concatenates them by target partition, and writes new files respecting `--flush-bytes`.

```bash
# Roll up minute-partitioned data into daily partitions
firehose-parquet rollup ./output/blocks/ -p date

# Roll up to a different output directory
firehose-parquet rollup ./output/blocks/ -o ./rolled-up/blocks/ -p date

# Delete source files after successful rollup
firehose-parquet rollup ./output/blocks/ -p date --delete-source
```

| Flag | Default | Description |
|---|---|---|
| `-o, --output` | same as source | Output path (local or S3 URI) |
| `-p, --target-partition` | `date` | Target partition interval: `hour` or `date` |
| `--compression` | `zstd` | Compression codec: zstd, snappy, gzip, none |
| `--flush-bytes` | 128 MB | Max compressed bytes per output file |
| `--delete-source` | `false` | Delete source files after successful rollup |

### `merge` — Consolidate Part Files

Consolidates multiple small part files within each partition directory into fewer, larger files. Unlike `rollup` (which changes partition granularity), `merge` keeps the same partition layout but reduces file count.

All parts in a partition are read into memory, sorted by `block_num`, and written back as new files respecting `--flush-bytes`. Original parts are deleted after successful merge.

```bash
# Merge small parts within each partition (default 256 MB per file)
firehose-parquet merge ./output/blocks/

# Dry run — show what would be merged without writing
firehose-parquet merge ./output/blocks/ --dry-run

# Merge with custom file size limit
firehose-parquet merge ./output/blocks/ --flush-bytes 536870912

# Merge S3-hosted data
firehose-parquet merge s3://my-bucket/evm/blocks/
```

| Flag | Default | Description |
|---|---|---|
| `--compression` | `zstd` | Compression codec: zstd, snappy, gzip, none |
| `--flush-bytes` | 256 MB | Max compressed bytes per output file |
| `--dry-run` | `false` | Show what would be merged without writing |

> **Memory note:** Merge reads all parts in a partition at once. Ensure sufficient memory for the largest partition.

> **Metadata preservation:** Both `merge` and `rollup` preserve Parquet file-level metadata (`firehose-parquet.*` keys) from the source files into the output files.

### `truncate` — Delete Parquet Files

Deletes `.parquet` files from local filesystem or S3 with optional partition filtering. Supports glob patterns for flexible selection.

```bash
# Delete all parquet files in a directory
firehose-parquet truncate ./output/blocks/

# Delete a specific partition
firehose-parquet truncate ./output/blocks/ -p "year=2026/month=01/date=01"

# Delete all partitions under a key
firehose-parquet truncate ./output/blocks/ -p date

# Glob pattern matching
firehose-parquet truncate s3://bucket/prefix -p "year=2026/month=01/date=*"

# Multiple partitions
firehose-parquet truncate ./output/ -p "year=2026/month=01/date=01" -p "year=2026/month=01/date=02"

# Dry run — show what would be deleted
firehose-parquet truncate ./output/blocks/ --dry-run
```

| Flag | Default | Description |
|---|---|---|
| `-p, --partition` | *(none)* | Partition filter (repeatable, supports globs) |
| `--dry-run` | `false` | Show what would be deleted without removing files |

## Failed Transaction Filtering

By default, failed/reverted transactions are excluded from output. Use `--include-failed-transactions` to include them.

Per-chain filtering logic:

| Chain | Filter condition |
|---|---|
| **Solana** | `meta.err` has non-empty bytes |
| **EVM** | `status != 1` |
| **NEAR** | `status == "Failure"` |
| **Cosmos** | `code != 0` in `TxResult` |
| **Tron** | `result != "SUCCESS"` |
| **Antelope** | Filtered by action trace status |
| **Bitcoin** | *(not applicable — Bitcoin has no failed txs)* |

When failed transactions are included, chain-specific fields like Solana's `err` bytes and `success` flag reflect the actual transaction status.

## Prometheus Metrics

Enable the metrics server with `--metrics-port <PORT>` (env: `METRICS_PORT`). A lightweight HTTP server binds to `0.0.0.0:<PORT>` serving three endpoints:

| Endpoint | Description |
|---|---|
| `/metrics` | Prometheus text exposition format |
| `/health` | Returns `200 OK` (liveness check) |
| `/ready` | Returns `200 OK` (readiness check) |

### Available Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `firehose_parquet_blocks_processed_total` | Counter | — | Total blocks processed since start |
| `firehose_parquet_bytes_read_total` | Counter | — | Total protobuf bytes consumed from stream |
| `firehose_parquet_rows_written_total` | Counter | `table` | Rows written per table |
| `firehose_parquet_current_block_number` | Gauge | — | Most recently processed block number |
| `firehose_parquet_min_block_number` | Gauge | — | Minimum block number seen |
| `firehose_parquet_max_block_number` | Gauge | — | Maximum block number seen |
| `firehose_parquet_blocks_per_second` | Gauge | — | Rolling throughput (blocks/s) |
| `firehose_parquet_bytes_per_second` | Gauge | — | Rolling throughput (bytes/s) |
| `firehose_parquet_elapsed_seconds` | Gauge | — | Seconds since pipeline start |
| `firehose_parquet_files_written_total` | Counter | `table`, `partition` | Parquet files written |
| `firehose_parquet_file_bytes_total` | Counter | `table`, `partition` | Total compressed bytes written |
| `firehose_parquet_flushes_total` | Counter | `trigger` | Flush count by trigger type |
| `firehose_parquet_buffer_estimated_bytes` | Gauge | `table` | Current in-memory buffer size |
| `firehose_parquet_buffer_rows` | Gauge | `table` | Current buffered row count |
| `firehose_parquet_cursor_saves_total` | Counter | — | Cursor persistence count |
| `firehose_parquet_cursor_last_block_num` | Gauge | — | Block number from last saved cursor |
| `firehose_parquet_errors_total` | Counter | `kind` | Errors by category |
| `firehose_parquet_grpc_reconnects_total` | Counter | — | gRPC stream reconnections |
| `firehose_parquet` | Info | *(pipeline config)* | Pipeline metadata (chain, endpoint, version) |

```bash
# Enable metrics on port 9090
firehose-parquet \
  --endpoint https://eth.firehose.pinax.network:443 \
  --metrics-port 9090 \
  --start-block 19000000

# Scrape metrics
curl http://localhost:9090/metrics
```

## Parquet File Metadata

Every Parquet file written by the pipeline embeds key-value metadata in the file footer under the `firehose-parquet.*` namespace. This allows consumers to identify the source pipeline, encoding, and chain without external sidecar files.

| Key | Example Value |
|---|---|
| `firehose-parquet.version` | `0.3.2` |
| `firehose-parquet.block_type` | `evm` |
| `firehose-parquet.bytes_encoding` | `hex` |
| `firehose-parquet.endpoint` | `https://eth.firehose.pinax.network:443` |
| `firehose-parquet.chain_name` | `eth-mainnet` |
| `firehose-parquet.chain_name_aliases` | `ethereum,eth` |
| `firehose-parquet.first_streamable_block_num` | `0` |
| `firehose-parquet.first_streamable_block_id` | `0x0000...` |
| `firehose-parquet.block_id_encoding` | `hex_0x` |
| `firehose-parquet.block_features` | `extended,base` |
| `firehose-parquet.compression` | `zstd` |
| `firehose-parquet.partition` | `date` |
| `firehose-parquet.block_range_size` | `10000` |

### Reading Metadata

```python
import pyarrow.parquet as pq

meta = pq.read_metadata("output/blocks/year=2026/month=01/date=15/part-000001.parquet")
for i in range(meta.metadata.count()):
    key = meta.metadata.keys()[i]
    if key.startswith("firehose-parquet."):
        print(f"{key} = {meta.metadata.values()[i]}")
```

```sql
-- DuckDB
SELECT key, value
FROM parquet_kv_metadata('output/blocks/year=2026/month=01/date=15/part-000001.parquet')
WHERE key LIKE 'firehose-parquet.%';
```

## Output Directory Layout

```
<chain_name>/
├── cursor.parquet
├── blocks/
│   ├── year=2026/month=02/date=25/
│   │   ├── part-000001.parquet
│   │   └── part-000002.parquet
│   └── year=2026/month=02/date=26/
│       └── part-000001.parquet
├── transactions/
│   └── ...
└── logs/
    └── ...
```

## Canonical Identity Columns

Every table across all chains includes these 6 columns (from Firehose `BlockMetadata`):

| Column | Type | Description |
|---|---|---|
| `block_num` | UInt64 | Block number |
| `block_id` | Utf8 | Block ID (hex for EVM, base58 for Solana) |
| `parent_num` | UInt64 | Parent block number |
| `parent_id` | Utf8 | Parent block ID |
| `lib_num` | UInt64 | Last irreversible block number |
| `timestamp` | Int64 | Block time (unix seconds) |

## Byte Encoding

| Mode | Description | Best For |
|---|---|---|
| `binary` | Raw bytes (Arrow `Binary`) | Parquet-native workflows (DuckDB, Spark) |
| `hex` | `0x`-prefixed hex strings | EVM ecosystem tools |
| `base58` | Base58 strings | Solana ecosystem tools |
| `tron_base58` | Tron Base58Check addresses (hex fallback) | Tron-specific tools |
| `auto` | Chain-appropriate default | General use |

### Auto Encoding Per Chain

| Chain | `auto` resolves to |
|---|---|
| EVM | `hex` |
| Solana | `base58` |
| Bitcoin | `hex` |
| Tron | `tron_base58` |
| Beacon | `hex` |
| Cosmos | `hex` |
| Antelope | `hex` |
| NEAR | `hex` |

## Environment Variables

CLI flags can also be set via environment variables. Copy `.env.example` to `.env`:

```bash
# Authentication — set the env vars that the CLI reads by default
SUBSTREAMS_API_KEY=your-api-key-here
SUBSTREAMS_API_TOKEN=your-jwt-token-here

# Prometheus metrics (optional)
# METRICS_PORT=9090

# Failed transaction filtering (optional)
# INCLUDE_FAILED_TRANSACTIONS=true

# AWS S3 output (optional)
# AWS_ACCESS_KEY_ID=...
# AWS_SECRET_ACCESS_KEY=...
# AWS_REGION=us-east-1
```

See `.env.example` for the full list of supported environment variables.

## CLI Architecture

The binary is built with [`clap`](https://docs.rs/clap) v4 using derive macros, chosen for its idiomatic Rust approach, excellent documentation, built-in shell completion support, and widespread community adoption.

### Key crates

| Crate | Purpose |
|---|---|
| `clap` (derive) | `#[derive(Parser)]` / `#[derive(Args)]` argument parsing with type-safe enums and defaults |
| `clap_complete` | Generates shell completions for Bash, Zsh, Fish, Elvish, and PowerShell |
| `anyhow` | Ergonomic error handling with context in binary crates |
| `thiserror` | Structured error types in the core library |
| `tracing` / `tracing-subscriber` | Structured, filterable logging |
| `tokio` | Async runtime for gRPC streaming |
| `prometheus-client` | Prometheus metrics exposition |
| `object_store` | S3-compatible object storage (data + cursor) |

### Shared CLI module

Common arguments, parsing helpers, and completions are defined once in `firehose-parquet::cli`:

```rust
use firehose_parquet::cli::{CommonArgs, Commands, build_config, init_tracing};

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    #[command(flatten)]
    common: CommonArgs,

    // binary-specific flags (block_type, extended, bytes_encoding)
}
```

### Shell completions

The binary supports the `completions` subcommand:

```bash
# Bash
firehose-parquet completions bash > ~/.local/share/bash-completion/completions/firehose-parquet

# Zsh
firehose-parquet completions zsh > ~/.zfunc/_firehose-parquet

# Fish
firehose-parquet completions fish > ~/.config/fish/completions/firehose-parquet.fish
```

## Repository Structure

```
firehose-parquet/
├── Cargo.toml                              # workspace root
├── Dockerfile                              # multi-stage Docker build
├── .env.example                            # environment variables template
├── .github/workflows/
│   ├── ci.yml                              # CI pipeline (build + test)
│   ├── docker-publish.yml                  # GHCR Docker image publish
│   └── release.yml                         # release assets for Linux/macOS targets
├── proto/                                  # Protobuf definitions (flat layout)
│   ├── firehose.proto                      # Firehose streaming protocol
│   ├── ethereum.proto
│   ├── solana.proto
│   ├── bitcoin.proto
│   ├── beacon.proto
│   ├── tron.proto
│   ├── cosmos.proto
│   ├── antelope.proto
│   └── near.proto
├── firehose-protos/                        # Centralized proto compilation
│   ├── build.rs                            # Compiles ./proto/*.proto at build time
│   └── src/lib.rs                          # include_proto! modules and aliases
├── firehose-parquet/                       # Core library
│   └── src/
│       ├── cli.rs                          # Shared CLI args, subcommands, helpers
│       ├── config.rs                       # Config, partitioning, compression enums
│       ├── cursor.rs                       # Cursor persistence (parquet format)
│       ├── encode.rs                       # Binary encoding strategies
│       ├── grpc.rs                         # Firehose gRPC client + reconnect logic
│       ├── metrics.rs                      # Prometheus metrics & HTTP server
│       ├── merge.rs                        # merge subcommand implementation
│       ├── rollup.rs                       # rollup subcommand implementation
│       ├── truncate.rs                     # truncate subcommand implementation
│       ├── s3.rs                           # object_store/S3 abstraction
│       ├── traits.rs                       # BlockMapper trait, canonical fields
│       └── writer.rs                       # Arrow->Parquet writer and flushing
├── blocks/                                 # Chain mappers + unified binary
│   └── src/
│       ├── bin/main.rs                     # Single unified binary (firehose-parquet)
│       ├── evm/                            # mapper.rs, schema.rs, proto.rs
│       ├── solana/
│       ├── bitcoin/
│       ├── beacon/
│       ├── tron/
│       ├── cosmos/
│       ├── antelope/
│       └── near/
└── target/                                 # build output
```

## Development

```bash
# Build
cargo build --workspace

# Test
cargo test --workspace

# Build release
cargo build --release --workspace

# Install
cargo install --path blocks
```

## License

[MIT](LICENSE)
