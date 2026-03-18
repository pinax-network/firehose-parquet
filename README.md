# firehose-parquet

A production-grade Rust toolkit that consumes [StreamingFast Firehose](https://firehose.streamingfast.io/) v2 gRPC streams and writes **Apache Parquet** files. A single unified binary (`fireparq`) supports multiple blockchain types with automatic chain detection.

## Supported Chains

| `--block-type` | Endpoint Example | Tables |
|---|---|---|
| `evm` | `eth.firehose.pinax.network:443` | blocks, transactions, logs, calls, balance_changes, code_changes, storage_changes, nonce_changes, gas_changes, account_creations (`--extended false` disables extra tables) |
| `solana` | `solana.firehose.pinax.network:443` | blocks, transactions, messages, instructions, rewards, token_balances, account_lookups, vote_transactions (`--with-votes false` disables `vote_transactions`) |
| `bitcoin` | `btc.firehose.pinax.network:443` | blocks, transactions, inputs, outputs |
| `beacon` | `beacon.firehose.pinax.network:443` | blocks, attestations, deposits, proposer_slashings, attester_slashings, voluntary_exits, execution_payload, blob_sidecars |
| `tron` | `tron.firehose.pinax.network:443` | blocks, transactions, logs, internal_transactions |
| `cosmos` | `cosmoshub.firehose.pinax.network:443` | blocks, transactions, events, messages |
| `antelope` | `eos.firehose.pinax.network:443` | blocks, transactions, actions, db_ops |
| `near` | `near.firehose.pinax.network:443` | blocks, chunks, transactions, receipts, state_changes |

> **Tip:** Use `--block-type auto` (the default) to auto-detect the chain from the Firehose stream's protobuf `type_url`.

## Features

- **Single binary** — one `fireparq` binary handles all chains via `--block-type` with auto-detection
- **Multi-chain** — pluggable `BlockMapper` trait with per-chain mapper modules
- **Canonical identity columns** — `block_num`, `block_id`, `parent_num`, `parent_id`, `lib_num`, `timestamp`, `date` on every table; `date` is an Arrow `Date32` derived from the UTC block timestamp. For Solana, canonical `timestamp` / `date` stay nullable when `block_time` is missing, and synthetic timing is used only for time-based partition routing
- **gRPC streaming** — connects to any Firehose v2 endpoint via tonic, with TLS and API key / JWT auth
- **Network aliases** — `--network` resolves built-in Firehose names and supports `FIREHOSE_ENDPOINT_*` per-network overrides
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

> `v0.5.0+` renames the installed CLI binary from `firehose-parquet` to `fireparq`. The repository/crate names and Parquet metadata namespace remain `firehose-parquet.*`.

```bash
# Build
cargo build --release --workspace

# Stream Solana blocks to Parquet (auto-detect chain)
./target/release/fireparq build \
  --network solana-mainnet-beta \
  --start-block 200000000 \
  --stop-block 200001000 \
  --output ./output \
  --partition date \
  --compression zstd

# Or use an explicit endpoint directly
./target/release/fireparq build \
  --endpoint https://solana.firehose.pinax.network:443 \
  --start-block 200000000 \
  --stop-block 200001000 \
  --output ./output \
  --partition date \
  --compression zstd

# Disable Solana vote transactions explicitly
./target/release/fireparq build \
  --network solana-mainnet-beta \
  --start-block 200000000 \
  --stop-block 200001000 \
  --with-votes false \
  --output ./output

# Disable extended EVM tables explicitly (explicit block type)
./target/release/fireparq build \
  --block-type evm \
  --endpoint https://eth.firehose.pinax.network:443 \
  --start-block 19000000 \
  --stop-block 19001000 \
  --extended false

# Stream Antelope blocks
./target/release/fireparq build \
  --block-type antelope \
  --endpoint https://eos.firehose.pinax.network:443 \
  --start-block 1000000 \
  --stop-block 1001000 \
  --output ./output

# Backfill from a block and keep following finalized blocks
./target/release/fireparq build \
  --network solana-mainnet-beta \
  --start-block 250000000 \
  --live \
  --output ./output \
  --partition date

# Start live mode from the endpoint's first streamable block
./target/release/fireparq build \
  --network mainnet \
  --live \
  --output ./output
```

### Docker

The image is published to GitHub Container Registry on each release:

The image path stays `ghcr.io/pinax-network/firehose-parquet`, but the container entrypoint now runs `fireparq`.

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

`fireparq` persists pipeline state in a `cursor.parquet` file so streams can be interrupted and resumed without re-processing blocks. The cursor system provides deterministic, crash-safe resume with full parameter validation.

## Network Aliases

`fireparq` can resolve a small built-in set of Firehose network aliases instead of requiring `--endpoint` every time.

Supported names in `v0.5.3`:

- `mainnet`, `eth` → `https://eth.firehose.pinax.network:443`
- `solana-mainnet-beta`, `solana` → `https://solana.firehose.pinax.network:443`
- `tron` → `https://tron.firehose.pinax.network:443`
- `tronevm` → `https://tronevm.firehose.pinax.network:443`

Resolution precedence:

1. `--endpoint` or `ENDPOINT`
2. `--network` with `FIREHOSE_ENDPOINT_*` override lookup
3. `--network` built-in default endpoint

Per-network env overrides normalize network names by uppercasing and converting non-alphanumeric separators to underscores.

```bash
# Built-in alias
fireparq --network eth --start-block 20000000 --stop-block 20001000

# Alias-specific override
export FIREHOSE_ENDPOINT_ETH=https://eth.internal.example.com:443
fireparq --network eth --start-block 20000000 --stop-block 20001000

# Canonical-name override also works for aliases
export FIREHOSE_ENDPOINT_SOLANA_MAINNET_BETA=https://solana.internal.example.com:443
fireparq --network solana-mainnet-beta --start-block 250000000 --stop-block 250100000
```

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

When output is written locally or to S3, the default cursor file is automatically placed alongside the data under the resolved chain output root — no special configuration needed:

| Output | `--cursor` value | Cursor location |
|---|---|---|
| `./output` | *(default)* | `./output/<chain>/cursor.parquet` |
| `s3://bucket/prefix` | *(default)* | `s3://bucket/prefix/cursor.parquet` |
| `s3://bucket/prefix` | `my-cursor.parquet` | `s3://bucket/prefix/my-cursor.parquet` |
| `s3://bucket/prefix` | `s3://other/path.parquet` | `s3://other/path.parquet` |

### Parameter Validation on Resume

When resuming from an existing `cursor.parquet`, the pipeline validates that the current CLI parameters match those stored in the cursor. Checked parameters include:

- `start_block`, `extended`, `final_blocks_only`, `include_failed_transactions`
- `with_votes` (Solana only) for vote table output
- `endpoint`, `partition`, `block_range_size`, `compression`, `bytes_encoding` (from file metadata)

On resume, the cursor's stored `start_block` is reused when present. The cursor's `stop_block` may be omitted from the CLI for bounded resume, replaced with a new explicit `--stop-block`, or omitted with `--live` to continue streaming indefinitely. Other parameter mismatches still fail fast unless `--cursor-override` is set.

When `--cursor-override` is set, the CLI request takes precedence over the stored cursor range. The pipeline still loads the cursor file for validation/logging, but it restarts from the CLI-provided or endpoint-default start block and does not pass the stored stream cursor token to Firehose.

```bash
# Restart from the requested range despite parameter changes in cursor.parquet
fireparq \
  --endpoint https://eth.firehose.pinax.network:443 \
  --cursor cursor.parquet \
  --cursor-override \
  --start-block 1 \
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

The top-level `fireparq` invocation is the ingestion/build pipeline. Utility workflows stay under subcommands such as `partitions`, `scan`, `inspect`, `validate`, `verify`, `rollup`, `merge`, and `truncate`.

```
$ fireparq --help

Build Apache Parquet datasets from Firehose gRPC streams

Usage: fireparq [OPTIONS] [COMMAND]

Commands:
  completions  Generate shell completions for the given shell
  partitions   Partition index utilities (`partitions.parquet` workflows)
  scan         Read and inspect Parquet files (schema, row counts, sample rows)
  inspect      Display full metadata for a single Parquet file
  validate     Check partition integrity (gaps, ordering, duplicates)
  verify       Verify table roots and protocol checks (chain-aware hash strategy)
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
      --network <NETWORK>
          Built-in Firehose network alias. `--endpoint` or `ENDPOINT` takes precedence. Supports per-network overrides such as `FIREHOSE_ENDPOINT_ETH` [env: NETWORK] [possible values: mainnet, eth, solana-mainnet-beta, solana, tron, tronevm]
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
                                  In `--live` mode, omitting this resumes from an existing cursor when available, otherwise starts from the endpoint's first streamable block
      --live                      Keep the stream open and continue following finalized blocks [env: LIVE] [default: false]
  -t, --stop-block <STOP_BLOCK>    Stop block number (exclusive)
                                  Required unless `--live` is set or an existing cursor provides one [env: STOP_BLOCK]
      --skip-missing-blocks
          Skip missing block numbers after retries are exhausted; useful for sparse chains like Solana, but may continue past gaps instead of failing fast [env: SKIP_MISSING_BLOCKS]
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
          Flush mapper state after this many rows; does not guarantee parquet files are materialized (disabled by default) [env: FLUSH_ROWS]
      --flush-bytes <FLUSH_BYTES>
          Flush mapper state at this many in-memory bytes and target roughly this many compressed bytes per parquet file [env: FLUSH_BYTES] [default: 134217728]
      --flush-interval-secs <FLUSH_INTERVAL_SECS>
          Flush mapper state every N seconds; does not guarantee parquet files are materialized (disabled by default) [env: FLUSH_INTERVAL_SECS]

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
      --extended [<EXTENDED>]
          Enable extended detail level for chains that support extra tables (for example EVM calls/balance_changes/etc.)
          Enabled by default; disable with `--extended false`. [env: EXTENDED] [default: true]
      --with-votes [<WITH_VOTES>]
          Include Solana vote_transactions output. Enabled by default; disable with `--with-votes false`
          [env: WITH_VOTES] [default: true]
      --include-failed-transactions
          Include failed/reverted transactions in output (default: false) [env: INCLUDE_FAILED_TRANSACTIONS]
```

`--flush-rows` and `--flush-interval-secs` flush mapper state into the writer,
not directly to disk/S3. `--flush-bytes` also sets the writer's target part
size, but no `--flush-*` flag guarantees immediate file materialization on its
own. Watch for the runtime logs that distinguish `mapper flush emitted record
batches`, `writer buffered mapper flush`, and `writer materialized parquet
output`. On graceful shutdown, the process now logs any buffered rows/bytes that
were intentionally left unmaterialized to preserve deterministic partition
boundaries.

When a partition boundary is detected during ingestion, the mapper flush for the
old partition is forced through writer materialization immediately, and the same
writer outcome logs are emitted for that boundary-triggered flush.

When `--bootstrap-missing-genesis-timestamp` is enabled in strict mode, leading
bootstrap blocks with missing timestamps are still written to output. Their
timestamps are synthesized from the first later block that includes timestamp
metadata.

For Solana time-based partitions, missing `block_time` values keep canonical
`timestamp` / `date` null. Partition routing uses the last known timestamp only,
seeded from the Solana first-streamable anchor (`2020-03-16 14:29:00 UTC`) for
the initial span and updated whenever a real block timestamp is observed.

## Subcommands

### `partitions build` — Generate `/<chain>/partitions.parquet`

Builds a canonical partition index directly from Firehose block timestamps, without requiring a pre-existing `blocks/` table.

```bash
# Build a date index locally
fireparq partitions build \
  --endpoint https://eth.firehose.pinax.network:443 \
  --stop-block 10010000 \
  --partition date \
  --output ./output

# Override the default zstd compression with snappy
fireparq partitions build \
  --endpoint https://eth.firehose.pinax.network:443 \
  --stop-block 10010000 \
  --partition date \
  --compression snappy \
  --output ./output

# Build an hour index to S3
fireparq partitions build \
  --endpoint https://eth.firehose.pinax.network:443 \
  --stop-block 10010000 \
  --partition hour \
  --s3-bucket my-bucket \
  --json

# Resume from an existing canonical index and append only missing coverage
fireparq partitions build \
  --endpoint https://eth.firehose.pinax.network:443 \
  --stop-block 10020000 \
  --partition date \
  --output ./output \
  --resume

# Continue extending the canonical index in live mode
fireparq partitions build \
  --network mainnet \
  --partition date \
  --output ./output \
  --live

# Poll every 15s while keeping the canonical index current
fireparq partitions build \
  --network mainnet \
  --partition date \
  --output ./output \
  --live \
  --poll-interval-secs 15
```

Behavior:

- writes one row per discovered partition to `/<chain>/partitions.parquet`
- writes `partitions.parquet` with `zstd` compression by default (overridable with `--compression`)
- writes a required non-null `chain` column on every partition row
- writes exact partition envelopes when the enclosing boundaries are discoverable
- supports one partition granularity per run (`date`, `hour`, `minute`, `second`)
- derives canonical UTC partition keys using rounded interval starts
- uses sparse single-block probes plus exponential/binary search to skip across ranges instead of streaming every block
- uses the Firehose single-block fetch path for sparse probes instead of a normal block stream
- writes contract metadata including schema version, chain scope, and covered block range
- writes an initial checkpoint as soon as the first row exists
- checkpoints long bounded and live runs continuously by elapsed time and partition rollovers
- `--resume` reuses the trailing rows from the existing canonical index and continues from the stored frontier
- bounded builds may expand the requested start/stop to the enclosing partition boundaries so each completed row remains exact
- `--live` treats existing `partitions.parquet` rows as the restart anchor, polls for new finalized blocks, and keeps extending the canonical index
- `--skip-missing-blocks` lets sparse probes scan forward a small window when a chain legitimately skips block numbers
- sparse probes treat missing/non-positive timestamps as missing metadata and borrow a nearby subsequent finalized block timestamp before partitioning

| Flag | Default | Description |
|---|---|---|
| `--partition` | none | Partition to build: `date`, `hour`, `minute`, or `second` |
| `--start-block` | inferred | Explicit probe seed, otherwise sibling cursor then endpoint first streamable block; bounded builds may expand downward to the enclosing partition start |
| `--stop-block` | none in live mode | Required for bounded builds; incompatible with `--live`; bounded builds expand upward to the enclosing partition end |
| `--live` | `false` | Keep extending `partitions.parquet` and resume from its latest covered frontier |
| `--poll-interval-secs` | `30` | Live-mode poll interval while waiting for the next finalized block frontier |
| `--skip-missing-blocks` | `false` | Allow sparse probes to skip over a small window of missing block numbers after retries are exhausted |
| `--output` | inferred from `--s3-bucket` | Output root directory or `s3://` URI prefix |
| `--s3-bucket` | none | S3 bucket used when `--output` is omitted or should be prefixed |
| `--resume` | `false` | Reuse the existing canonical index at the resolved output path and continue from its frontier |
| `--json` | `false` | Emit machine-readable output |

### `partitions ls` — Query Partition Index Rows

Lists rows from `partitions.parquet` with optional filters and deterministic ascending order by `partition_start_ts`.

```bash
# List hour partitions from a local index
fireparq partitions ls \
  --partitions-index ./output/eth-mainnet/partitions.parquet \
  --partition-type hour

# Filter chain + time window and return JSON
fireparq partitions ls \
  --partitions-index s3://my-bucket/eth-mainnet/partitions.parquet \
  --partition-type date \
  --partition-chain eth-mainnet \
  --from '2015-07-29 00:00:00' \
  --to '2015-07-31 00:00:00' \
  --limit 200 \
  --json
```

| Flag | Default | Description |
|---|---|---|
| `--partition-type` | none | Optional partition type filter |
| `--partition-chain` | none | Optional chain filter |
| `--from` | none | Inclusive lower bound for `partition_start_ts` |
| `--to` | none | Inclusive upper bound for `partition_start_ts` |
| `--limit` | `100` | Maximum rows returned |
| `--json` | `false` | Emit machine-readable output |

### `partitions shard` — Deterministic Partition Assignment

Assigns filtered partition rows to one shard for multi-container runs.

```bash
# Ordinal assignment: shard 1 of 4
fireparq partitions shard \
  --partitions-index ./output/eth-mainnet/partitions.parquet \
  --partition-type hour \
  --shard-count 4 \
  --shard-index 1

# Hash assignment over a window with JSON output
fireparq partitions shard \
  --partitions-index s3://my-bucket/eth-mainnet/partitions.parquet \
  --partition-type date \
  --partition-chain eth-mainnet \
  --from '2015-07-29 00:00:00' \
  --to '2015-07-31 00:00:00' \
  --shard-count 8 \
  --shard-index 0 \
  --strategy hash \
  --json
```

Strategies:

- `ordinal` — assigns by sorted row ordinal modulo `shard_count`
- `hash` — assigns by stable hash of `(chain, partition_type, partition_value)` modulo `shard_count`

| Flag | Default | Description |
|---|---|---|
| `--shard-count` | none | Total shard count |
| `--shard-index` | none | Zero-based shard index |
| `--strategy` | `ordinal` | Assignment strategy: `ordinal` or `hash` |
| `--json` | `false` | Emit machine-readable output |

### `partitions validate` — Check Partition Index Integrity

Validates continuity and basic invariants in `partitions.parquet`.

```bash
# Validate all rows in a local index
fireparq partitions validate \
  --partitions-index ./output/eth-mainnet/partitions.parquet

# Validate one chain/type and emit JSON
fireparq partitions validate \
  --partitions-index s3://my-bucket/partitions.parquet \
  --partition-type date \
  --partition-chain eth-mainnet \
  --json
```

Checks:

- `start_block < stop_block` for every row
- adjacent rows in the same `(chain, partition_type)` do not overlap
- adjacent rows are contiguous unless `--allow-gaps` is set

Violations exit non-zero for CI gating.

### `partitions resolve` — Resolve Partition Block Bounds

Resolves one row from `partitions.parquet` and prints the exact ingestion range (`start_block` inclusive, `stop_block` exclusive).

```bash
# Local index
fireparq partitions resolve \
  --partitions-index ./output/eth-mainnet/partitions.parquet \
  --partition-type hour \
  --partition-value '2015-07-30 15:00:00' \
  --partition-chain eth-mainnet

# S3 index with machine-readable output
fireparq partitions resolve \
  --partitions-index s3://my-bucket/eth-mainnet/partitions.parquet \
  --partition-type date \
  --partition-value '2015-07-30 00:00:00' \
  --partition-chain eth-mainnet \
  --json

# Require a unique chain match when using a global index
fireparq partitions resolve \
  --partitions-index s3://my-bucket/partitions.parquet \
  --partition-type date \
  --partition-value '2015-07-30 00:00:00' \
  --strict-single-chain
```

Helpful guard:

- `--strict-single-chain` fails fast when a global index contains multiple chain rows for the same partition descriptor and `--partition-chain` was omitted

See `docs/partitions-parquet-contract.md` for the versioned `partitions.parquet` schema and metadata compatibility contract.

`partitions resolve` reads the canonical `partitions.parquet` index directly.

### Resolving ranges from `partitions.parquet`

The main `fireparq build` workflow uses explicit block bounds or `--live`.
If you want to ingest the range covered by a partition in `partitions.parquet`,
resolve it first under the `partitions` namespace and then run `build` with the
returned block range.

```bash
# 1) Resolve an exact block range from the canonical index
fireparq partitions resolve \
  --partitions-index ./output/eth-mainnet/partitions.parquet \
  --partition-type hour \
  --partition-value '2015-07-30 15:00:00' \
  --partition-chain eth-mainnet

# 2) Run ingestion with explicit block bounds
fireparq build --network mainnet \
  --start-block 200 \
  --stop-block 300
```

### Custom Cursor Paths (`--cursor-template`)

Use `--cursor-template` to choose a deterministic cursor path for a build run.

```bash
# Keep a dedicated cursor for this live pipeline
fireparq build --network mainnet \
  --live \
  --cursor-template 'cursor/live-mainnet.parquet'

# Store a cursor under the S3 output prefix
fireparq build --network mainnet \
  --start-block 20000000 \
  --stop-block 20001000 \
  --output s3://my-bucket/backfill/eth-mainnet \
  --cursor-template 'cursor/backfill.parquet'
```

Rules:

- template path must end in `.parquet`
- `{{` and `}}` escape literal braces
- with S3 output, relative cursor template paths are stored under the output prefix

### `scan` — Inspect Parquet Files

Read and inspect Parquet files: shows schema, row counts, and sample rows. By default, sampled rows render in a boxed table in ascending row order; use `--order desc` to inspect the latest rows first, `--vertical` for row-by-row output, or `--json` for machine-readable output. When scanning multiple files, `--limit` and `--offset` apply across the full scan result set, so scanning stops once enough rows have been collected. Supports local paths, shorthand S3 keys/prefixes via `S3_BUCKET`, and explicit S3 URIs.

```bash
fireparq scan ./output/blocks/
S3_BUCKET=my-bucket fireparq scan evm/blocks/
fireparq scan s3://my-bucket/evm/blocks/
fireparq scan s3://my-bucket/evm/partitions.parquet
fireparq scan ./output/blocks/part-000001.parquet --vertical
fireparq scan ./output/blocks/part-000001.parquet --json
fireparq scan ./output/blocks/ --limit 10
fireparq scan ./output/blocks/part-000001.parquet --order desc --limit 20
fireparq scan ./output/blocks/part-000001.parquet --order desc --offset 20 --limit 20
```

Lookup order:

1. Explicit `s3://bucket/...` URIs are used as-is.
2. Non-URI paths use the local filesystem when the path exists.
3. Otherwise, if `S3_BUCKET` is set, relative paths fall back to `s3://<bucket>/<path>`.

### `inspect` — Display File Metadata

Displays comprehensive metadata for a single Parquet file: file-level key-value pairs (including custom `firehose-parquet.*` entries), the full Parquet schema with physical/logical types, row group statistics, and per-column chunk details (encoding, compression, sizes). Supports local paths, shorthand S3 keys via `S3_BUCKET`, and explicit S3 URIs.

```bash
# Inspect a local file
fireparq inspect ./output/blocks/year=2026/month=01/date=15/part-000001.parquet

# Resolve a shorthand key against S3_BUCKET when no local path matches
S3_BUCKET=my-bucket fireparq inspect evm/partitions.parquet

# Inspect an S3 file
fireparq inspect s3://my-bucket/evm/blocks/year=2026/month=01/date=15/part-000001.parquet

# Show only schema fields, including explicit nullability
fireparq inspect s3://my-bucket/evm/partitions.parquet --schema-only

# Emit machine-readable schema JSON for a single parquet artifact
fireparq inspect s3://my-bucket/evm/partitions.parquet --schema-only --json
```

Lookup order matches `scan`: explicit `s3://...` URIs win, existing local paths win over shorthand S3 resolution, and only missing relative paths fall back to `s3://<S3_BUCKET>/<path>`.

**Output includes:**

| Section | Details |
|---|---|
| **File info** | Total rows, row groups, columns, file size, created_by, Parquet version |
| **File metadata** | All key-value pairs stored in the Parquet footer |
| **Schema** | Physical types, logical types (e.g. String, Timestamp), repetition levels, explicit `nullable=` output, nested groups |
| **Row groups** | Per-group row count, compressed/uncompressed size, compression ratio |
| **Column details** | Per-column encoding, compression codec, compressed/uncompressed size, ratio |

### `validate` — Check Partition Integrity

Validates partitioned Parquet data for gaps, ordering errors, duplicates, parent hash mismatches, and timestamp reversals. Only partitions with issues are printed; valid ones are silently counted. Supports local paths, shorthand S3 keys/prefixes via `S3_BUCKET`, and explicit S3 URIs.

```bash
fireparq validate ./output/blocks/
S3_BUCKET=my-bucket fireparq validate evm/blocks/
fireparq validate s3://my-bucket/evm/blocks/

# Solana: allow skipped slots (normal chain behavior, not data corruption)
fireparq validate s3://my-bucket/solana/blocks/ --allow-gaps
```

Lookup order matches `scan` / `inspect`: explicit `s3://...` URIs win, existing local paths win over shorthand S3 resolution, and only missing relative paths fall back to `s3://<S3_BUCKET>/<path>`.

| Flag | Default | Description |
|---|---|---|
| `--cross-partition` | `false` | Check continuity between adjacent partitions |
| `--allow-gaps` | `false` | Suppress gap reporting (useful for Solana skipped slots) |

### `verify` — Deterministic Roots + Check Profiles

Verifies deterministic partition Merkle roots and optional protocol checks under one command surface. Supports local paths, shorthand S3 keys/prefixes via `S3_BUCKET`, and explicit S3 URIs for the data path.

```bash
# Standard profile (default): roots + protocol
fireparq verify ./output/evm/mainnet/blocks --chain evm --table blocks

# Quick profile (low-cost)
fireparq verify ./output/evm/mainnet/blocks --profile quick

# Explicit checks override profile defaults
fireparq verify ./output/evm/mainnet/blocks --checks roots,protocol

# Publish report to the suggested artifact path
fireparq verify ./output/evm/mainnet/blocks --publish-report

# Resolve a shorthand S3 data path when no local match exists
S3_BUCKET=my-bucket fireparq verify evm/mainnet/blocks --chain evm --table blocks
```

Lookup order for the data path matches `scan` / `inspect`: explicit `s3://...` URIs win, existing local paths win over shorthand S3 resolution, and only missing relative paths fall back to `s3://<S3_BUCKET>/<path>`.

| Flag | Default | Description |
|---|---|---|
| `--checks` | *(from profile)* | Comma-separated check families: `roots`, `protocol`, `continuity`, `completeness` |
| `--profile` | `standard` | Preset families: `quick` (roots), `standard` (roots+protocol), `deep` (adds continuity+completeness) |
| `--scope` | `table` | Metadata scope tag in reports: `chain`, `table`, `partition`, `run` |
| `--hash-strategy` | `auto` | Hash strategy for leaves+Merkle nodes: `auto`, `keccak256`, `sha256` |
| `--publish-report` | `false` | Publish `report.json` to the suggested verify artifact path |
| `--publish-report-path` | *(suggested path)* | Override where the published report is written (local or `s3://`) |

Migration note: existing verify flags (`--no-fail-fast`, `--report-json`, `--registry-path`, `--update-registry`) remain unchanged.

See [Cross-chain verifiability hash strategy](docs/verifiability-hash-strategy.md) for defaults and normalization rules.

See [Verify report contract](docs/verify-report-contract.md) for schema versioning, run metadata fields, and artifact path guidance.

See [Verifiability artifact runbook](docs/verifiability-artifact-runbook.md) for registry/report lifecycle, S3 publication guidance, and operational workflows.

### `rollup` — Roll Up Partitions

Rolls up fine-grained partitions (e.g. `minute` or `hour`) into coarser ones (e.g. `date`). Reads source files, concatenates them by target partition, and writes new files respecting `--flush-bytes`. The source path supports local paths, shorthand S3 keys/prefixes via `S3_BUCKET`, and explicit S3 URIs.

```bash
# Roll up minute-partitioned data into daily partitions
fireparq rollup ./output/blocks/ -p date

# Roll up to a different output directory
fireparq rollup ./output/blocks/ -o ./rolled-up/blocks/ -p date

# Delete source files after successful rollup
fireparq rollup ./output/blocks/ -p date --delete-source

# Resolve a shorthand S3 source path when no local match exists
S3_BUCKET=my-bucket fireparq rollup evm/blocks/ -p date
```

Lookup order for the source path matches `scan` / `inspect`: explicit `s3://...` URIs win, existing local paths win over shorthand S3 resolution, and only missing relative paths fall back to `s3://<S3_BUCKET>/<path>`.

| Flag | Default | Description |
|---|---|---|
| `-o, --output` | same as source | Output path (local or S3 URI) |
| `-p, --partition` | `date` | Target partition interval: `hour` or `date` |
| `--compression` | `zstd` | Compression codec: zstd, snappy, gzip, none |
| `--flush-bytes` | 128 MB | Max compressed bytes per output file |
| `--delete-source` | `false` | Delete source files after successful rollup |

### `merge` — Consolidate Part Files

Consolidates multiple small part files within each partition directory into fewer, larger files. Unlike `rollup` (which changes partition granularity), `merge` keeps the same partition layout but reduces file count. Supports local paths, shorthand S3 keys/prefixes via `S3_BUCKET`, and explicit S3 URIs.

`merge` processes one table at a time and, within each table, one partition at a time. All parts in each partition are read into memory, sorted by `block_num`, and written back as new files respecting `--flush-bytes`. Original parts are deleted after successful merge.

```bash
# Merge small parts within each partition (default 256 MB per file)
fireparq merge ./output/blocks/

# Dry run — show what would be merged without writing
fireparq merge ./output/blocks/ --dry-run

# Merge with custom file size limit
fireparq merge ./output/blocks/ --flush-bytes 536870912

# Merge S3-hosted data
fireparq merge s3://my-bucket/evm/blocks/

# Resolve a shorthand S3 path when no local match exists
S3_BUCKET=my-bucket fireparq merge evm/blocks/
```

Lookup order matches `scan` / `inspect`: explicit `s3://...` URIs win, existing local paths win over shorthand S3 resolution, and only missing relative paths fall back to `s3://<S3_BUCKET>/<path>`.

| Flag | Default | Description |
|---|---|---|
| `--compression` | `zstd` | Compression codec: zstd, snappy, gzip, none |
| `--flush-bytes` | 256 MB | Max compressed bytes per output file |
| `--dry-run` | `false` | Show what would be merged without writing |

> **Memory note:** Merge reads all parts in a partition at once. Ensure sufficient memory for the largest partition.

> **Metadata preservation:** Both `merge` and `rollup` preserve Parquet file-level metadata (`firehose-parquet.*` keys) from the source files into the output files.

### `truncate` — Delete Parquet Files

Deletes `.parquet` files from local filesystem or S3 with optional partition filtering. Supports glob patterns for flexible selection, local paths, shorthand S3 keys/prefixes via `S3_BUCKET`, and explicit S3 URIs.

When truncating a network root, root-level `.parquet` artifacts such as `partitions.parquet` and `cursor.parquet` are included in the matched files. `--dry-run` prints every matched file explicitly so those artifacts are visible before deletion. You can also target a single `.parquet` file directly, such as `fireparq truncate unichain/partitions.parquet`.

```bash
# Delete all parquet files in a directory
fireparq truncate ./output/blocks/

# Delete all parquet files under a network root, including root-level artifacts
fireparq truncate ./output/mainnet/ --dry-run

# Delete a single parquet file directly
fireparq truncate ./output/mainnet/partitions.parquet

# Delete a specific partition
fireparq truncate ./output/blocks/ -p "year=2026/month=01/date=01"

# Delete all partitions under a key
fireparq truncate ./output/blocks/ -p date

# Glob pattern matching
fireparq truncate s3://bucket/prefix -p "year=2026/month=01/date=*"

# Resolve a shorthand S3 path when no local match exists
S3_BUCKET=my-bucket fireparq truncate evm/blocks/ -p "month=01"

# Multiple partitions
fireparq truncate ./output/ -p "year=2026/month=01/date=01" -p "year=2026/month=01/date=02"

# Dry run — show what would be deleted
fireparq truncate ./output/blocks/ --dry-run
```

Lookup order matches `scan` / `inspect`: explicit `s3://...` URIs win, existing local paths win over shorthand S3 resolution, and only missing relative paths fall back to `s3://<S3_BUCKET>/<path>`.

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
fireparq \
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
| `firehose-parquet.version` | `0.5.3` |
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
| `firehose-parquet.synthetic_timestamps` | `true` |
| `firehose-parquet.synthetic_timestamp_policy` | `last_known_partition_routing` |

`firehose-parquet.bytes_encoding` and `firehose-parquet.block_id_encoding` describe the emitted output contract, not just the upstream Firehose endpoint.

For Solana time-based partitions, the `firehose-parquet.synthetic_*` metadata
keys mark routing as using a synthetic last-known timestamp anchor while
canonical `timestamp` / `date` remain chain-sourced and nullable when
`block_time` is missing.

Byte encoding is determined internally from the resolved block type/profile. Operators no longer configure it manually.

| Block type / profile | Binary field encoding (`firehose-parquet.bytes_encoding`) | Block encoding (`firehose-parquet.block_id_encoding`) |
|---|---|---|
| `evm` | `hex` | `hex_0x` |
| `bitcoin` | `hex` | `hex_0x` |
| `solana` | `base58` | `base58` |
| `near` | `base58` | `base58` |
| `antelope` | `hex_no_prefix` | `hex_no_prefix` |
| `cosmos` | `hex` | `hex_0x` |
| `tron` | `tron_base58` | `hex_no_prefix` |
| `beacon` | `hex` | `hex_0x` |
| `tron-evm` (`evm` Tron-style profile) | `tron_base58` | `hex_no_prefix` |

Endpoint `block_id_encoding` remains a fallback only when the chain does not resolve to a known block-type/profile contract.

For Tron-style profiles (tron and tron-evm):

- metadata reports `bytes_encoding=tron_base58`
- metadata reports `block_id_encoding=hex_no_prefix`
- canonical hash/topic-like fields stay hex without `0x`
- address-like byte fields use Tron Base58

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
| `tron_base58` | Tron Base58Check addresses; non-address Tron values fall back to hex without `0x` | Tron-specific tools |
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

When `tron` resolves `auto` to `tron_base58`, account/address-like byte fields are emitted as Tron Base58Check. Reserved protocol identifiers remain lowercase hex without `0x`: block hash, parent block hash, transaction hash, and log topics. Additional Tron hash-like fields such as `tx_trie_root` and internal transaction `hash` also remain hex without `0x`.

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

    // binary-specific flags (block_type, extended)
}
```

### Shell completions

The binary supports the `completions` subcommand:

```bash
# Bash
fireparq completions bash > ~/.local/share/bash-completion/completions/fireparq

# Zsh
fireparq completions zsh > ~/.zfunc/_fireparq

# Fish
fireparq completions fish > ~/.config/fish/completions/fireparq.fish
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
│       ├── bin/main.rs                     # Single unified binary (fireparq)
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
