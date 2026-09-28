# firehose-parquet

A production-grade Rust toolkit that consumes [StreamingFast Firehose](https://firehose.streamingfast.io/) v2 gRPC streams and writes **Apache Parquet** files. A single unified binary (`fireparq`) supports multiple blockchain types with automatic chain detection.

## Supported Chains

| `--block-type` | Example `--network` → endpoint | Tables, columns and types |
|---|---|---|
| `evm` | `mainnet` → `eth.firehose.pinax.network:443` | [EVM schema](docs/schemas/evm.md). `--without-extended` drops the call, state-change and `system_*` tables |
| `solana` | `solana-mainnet-beta` → `solana.firehose.pinax.network:443` | [Solana schema](docs/schemas/solana.md). `--without-votes` drops `vote_transactions` |
| `bitcoin` | `btc` → `bitcoin.firehose.pinax.network:443` | [Bitcoin schema](docs/schemas/bitcoin.md) |
| `beacon` | `mainnet-cl` → `eth-cl.firehose.pinax.network:443` | [Beacon schema](docs/schemas/beacon.md) ([fork coverage](#beacon-chain-tables)) |
| `tron` | `tron` → `mainnet.tron.streamingfast.io:443` | [Tron schema](docs/schemas/tron.md) |
| `cosmos` | no built-in name; `--endpoint https://mainnet.injective.streamingfast.io:443` | [Cosmos schema](docs/schemas/cosmos.md) |
| `antelope` | `eos` → `eos.firehose.pinax.network:443` | [Antelope schema](docs/schemas/antelope.md) |
| `near` | `near-mainnet` → `mainnet.near.streamingfast.io:443` | [NEAR schema](docs/schemas/near.md) |

> **Tip:** Use `--block-type auto` (the default) to auto-detect the chain from the Firehose stream's protobuf `type_url`.

The generated [schema reference](docs/schemas/README.md) lists every table,
column, Arrow type and nullability. The sections below explain semantics, joins
and queries. Select columns by name, not position: the non-final `fork_step` and
`stream_ordinal` are not always the last columns, because later additions follow
them on several Solana, Antelope, NEAR and Tron tables.

## What's new in v1.0.0

v1.0.0 is a breaking release that follows the v0.7 series. The
[v1.0.0 release notes](docs/releases/v1.0.0.md) list every change and include
the upgrade guide.

- **Data-integrity hardening.** `build` commits every table of a flush in one
  transaction. The authoritative checkpoint is stored under
  `<output>/.fireparq-ingest/`, and `_fireparq/cursor.parquet` is now an
  optional mirror. `build` and `recovery` take dataset ownership, which
  `fireparq recovery` can inspect.
- **One consistent schema across chains.** Canonical `timestamp` is a UTC
  timestamp with millisecond values on every table, and every table is
  partitioned by UTC day as `<table>/date=YYYY-MM-DD/`, which is its `date`
  column. Every part is a Delta data file with Delta column types (#643).
  Most chains gain columns and tables, for example EVM withdrawals,
  access lists and EIP-7702 authorizations, NEAR receipt actions and logs,
  Beacon Electra requests and Tron contracts. See the
  [schema reference](docs/schemas/README.md).
- **Delta Lake tables only.** Every table is a Delta table, read through its
  log with DuckDB `delta_scan` or Polars `scan_delta`; there is no plain-Parquet
  output and no reader that walks table directories (#643). `validate` reads
  the active files of a pinned Delta snapshot. See
  [Reading the tables](#reading-the-tables).
- **Removed: `merge`, `truncate`, `verify` and `scan`.** Compaction is the job
  of an off-the-shelf `deltalake` maintenance CronJob (#643,
  [Delta maintenance](#delta-maintenance)), `truncate` has no
  safe Delta equivalent (rebuild into a new root instead), `verify` returns over
  Delta snapshots in #666, and DuckDB, Polars and the Delta log replace `scan`.
- **Engine-friendly layout.** A dataset root holds only its table directories,
  `_fireparq/` (the cursor mirror) and dot-prefixed control state, so engines that skip `_` and `.`
  paths never read fireparq's own files as table data. `--output` is that
  root, used exactly as given; `{chain}` opts into a directory named after the
  network (`--output 's3://datasets/{chain}'`). v0.7.x appended the chain
  name, so add `/{chain}` to keep that layout. See
  [Output Directory Layout](#output-directory-layout).
- **Explicit credentials and destinations.** Firehose credentials are scoped to
  the provider (`PINAX_*`, `STREAMINGFAST_*`). S3 writes need an explicit
  `s3://bucket/prefix` output, and `.env` is read only from the working
  directory or from `--env-file`.
- **Failed transactions.** EVM includes failed transactions by default, with
  only their persistent state changes. Solana, Tron, Antelope and NEAR child
  rows carry the outcome of their parent transaction or receipt.
- **Performance.** Identifier columns are encoded once per block without
  per-value allocations, and EVM decimals are written directly into Arrow.
  Each `build` flush encodes and publishes its tables concurrently within
  explicit bounds. File sizes
  follow an adaptive compressed-size target. Firehose receive windows are 16 MiB
  and accept zstd replies.
- **Existing datasets must be rebuilt into a new output root.** v1.0.0 does not
  adopt output written by earlier releases, and the schemas changed. Rebuild
  into a new empty output root and keep old datasets only for read-only tools.
  See the [upgrade guide](docs/releases/v1.0.0.md#upgrade-guide-read-first).

## Features

- **Single binary** — one `fireparq` binary handles all chains via `--block-type` with auto-detection
- **Multi-chain** — pluggable `BlockMapper` trait with per-chain mapper modules
- **Canonical identity columns** — `block_num`, `block_id`, `parent_num`, `parent_id`, `lib_num`, `timestamp`, `date` on every table; `timestamp` is a Delta `timestamp` (Parquet `TIMESTAMP(MICROS, isAdjustedToUTC=true)`, so DuckDB, Polars and ClickHouse read it as a timestamp) and keeps sub-second block times, to the millisecond, where Firehose provides them; `date` is the partition column, the UTC day of the block time, stored as the `date=YYYY-MM-DD` directory rather than in the files. For Solana, canonical `timestamp` stays null when `block_time` is missing, and such a row's `date` is its routing day, the last known block time. Chain-specific columns never reuse these names: Tron `transactions` stores the transaction's own creation and expiration times as `tx_timestamp_ms` / `expiration_ms` (Int64 unix milliseconds; `tx_timestamp_ms` is set by the sender, so it can be 0 or use another unit)
- **gRPC streaming** — connects to any Firehose v2 endpoint via tonic, with TLS and API key / JWT auth
- **Network aliases** — `--network` resolves built-in Firehose names and supports `FIREHOSE_ENDPOINT_*` per-network overrides
- **Automatic retry / resume** — exponential back-off on connection errors; restarts from the authoritative output checkpoint
- **Recovery guardrails** — optional stream idle timeout and reconnect stall timeout to force self-recovery or fail-fast restarts
- **Crash recovery** — all-table transactions and an authoritative output checkpoint; `_fireparq/cursor.parquet` remains an optional compatible mirror
- **S3-aware cursor** — cursor automatically stored alongside output (local or S3)
- **Delta Lake tables** — every table is a Delta table: its log in `<table>/_delta_log/`, its data files in `<table>/date=YYYY-MM-DD/`, and `date` its partition column; DuckDB (`delta_scan`) and Polars (`scan_delta`) read it through the log ([reading the tables](#reading-the-tables), [engine compatibility](#engine-compatibility))
- **Delta maintenance** — an off-the-shelf `deltalake` job compacts closed days, vacuums and checkpoints beside the writer, with a reference script and Kubernetes CronJob ([Delta maintenance](#delta-maintenance))
- **Delta Lake types** — every part is a Delta data file: checked signed integers, `decimal(20,0)` for currency amounts and other unchecked 64-bit values, `string` enums and microsecond timestamps, mapped once per flush before anything is written ([type mapping](docs/schemas/README.md))
- **File rollover** — flush by row count, byte size, or time interval; the interval applies at the chain head, and a catch-up flushes by size ([details](#flush-interval-and-catch-up))
- **Fork handling** — finalized output by default; `--final-blocks-only=false` preserves append-only `fork_step` events numbered by a durable `stream_ordinal` ([canonical live view](#canonical-live-view))
- **Failed transactions** — EVM includes failed/reverted txs by default with only their persistent state changes (`--exclude-failed-transactions` drops them); Solana, Tron, Antelope, Cosmos and NEAR exclude them unless `--include-failed-transactions` is set, and label child rows with their parent outcome ([details](#failed-transaction-filtering))
- **Block-type-based encoding** — identifiers follow the resolved chain/profile defaults, recorded in Parquet metadata; opaque Solana payloads use Binary and account indices use UInt8 lists
- **Compression** — zstd (default level 3), explicit `zstd:<level>`, snappy, gzip, or none
- **Parquet file metadata** — every file embeds pipeline provenance (`firehose-parquet.*` key-value pairs) in the Parquet footer
- **Prometheus metrics** — opt-in `/metrics` endpoint for monitoring throughput, buffer state, and errors
- **Graceful shutdown** — SIGINT/SIGTERM and write/stream errors never save the cursor past unwritten data; the next run resumes from the last committed flush
- **Docker support** — multi-stage Dockerfile, published to GHCR
- **Arrow-native pipeline** — column builders produce `RecordBatch`es that flush to Parquet

## Quick Start

> `v0.5.0+` renames the installed CLI binary from `firehose-parquet` to `fireparq`. The repository/crate names and Parquet metadata namespace remain `firehose-parquet.*`.

### Install

Each GitHub release attaches `fireparq` binaries for Linux and macOS
(`x86_64` and `aarch64`), with build provenance attestations:

```bash
curl -LO https://github.com/pinax-network/firehose-parquet/releases/download/v1.0.0/fireparq-linux-x86_64.tar.gz
tar xzf fireparq-linux-x86_64.tar.gz
./fireparq-linux-x86_64/fireparq --version
```

The other archives are `fireparq-linux-aarch64`, `fireparq-macos-x86_64` and
`fireparq-macos-aarch64`. To build from source instead, run
`cargo install --path blocks` or use the commands below. A
[Docker image](#docker) is also published.

### Run

```bash
# Build
cargo build --release --workspace

# Stream Solana blocks to Parquet (auto-detect chain). --output is the dataset
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

### Authentication

Credentials are selected from the **resolved endpoint host**, including any
`--endpoint`, `ENDPOINT`, or `FIREHOSE_ENDPOINT_*` override:

| Destination | API key environment variables, in priority order | Bearer token environment variables, in priority order |
|---|---|---|
| Built-in Pinax host over HTTPS on port 443 | `PINAX_API_KEY`, then `SUBSTREAMS_API_KEY` | `PINAX_API_TOKEN`, then `SUBSTREAMS_API_TOKEN` |
| Built-in StreamingFast host over HTTPS on port 443 | `STREAMINGFAST_API_KEY` | `STREAMINGFAST_API_TOKEN` |
| Other host, port, or plaintext connection | No automatic credentials | No automatic credentials |

```bash
export PINAX_API_KEY=your-pinax-api-key
# For near-mainnet, near-testnet, tron, or tron-evm:
export STREAMINGFAST_API_TOKEN=your-streamingfast-compatible-token
```

`SUBSTREAMS_API_KEY` and `SUBSTREAMS_API_TOKEN` are legacy **Pinax-only**
fallbacks. If you previously used `SUBSTREAMS_API_TOKEN` with StreamingFast,
move that token to `STREAMINGFAST_API_TOKEN` or explicitly select it with
`--api-token-envvar SUBSTREAMS_API_TOKEN` for that endpoint.

For a custom endpoint, explicitly select the credential names with
`--api-key-envvar` / `--api-token-envvar` (or `API_KEY_ENVVAR` /
`API_TOKEN_ENVVAR`). An explicit selector authorizes that credential for the
chosen destination and overrides automatic selection for that header. If the
selected variable is unset or blank, that header is omitted; it does not fall
back to another variable. The other header still follows its own selection
rules. Only explicitly select a credential for a destination you intend it to reach.

An explicit selector is **not provider-scoped**. If `API_KEY_ENVVAR` or
`API_TOKEN_ENVVAR` is set globally, for example to the legacy
`SUBSTREAMS_API_KEY` in a shared `.env` or container environment, that
credential is sent to every endpoint the process connects to, including
StreamingFast and custom hosts. Startup logs a `WARN` line naming the variable
(never its value) and the host whenever an explicitly selected credential is
sent to a non-Pinax host, with a stronger message for Pinax and legacy
`SUBSTREAMS_*` names. A `STREAMINGFAST_*` variable sent to a built-in
StreamingFast host is its normal destination and is not warned about. When
migrating, unset global selectors and rely on the provider-scoped variables
above, or pass `--api-key-envvar` / `--api-token-envvar` only on the command
for the intended endpoint.

Startup logs identify the destination host, provider, and names of credential
variables selected for transmission (`none` when absent), never their values.
Surrounding whitespace is trimmed, so a key mounted from a secret file with a
trailing newline works. A selected credential that still contains characters a
gRPC header cannot carry (control characters or line breaks inside the value)
fails at startup with an error.

### Docker

The image is published to GitHub Container Registry for each release tag; this
release is tagged `1.0.0`, `1.0`, `1` and `latest`. The image path stays
`ghcr.io/pinax-network/firehose-parquet`, and the container entrypoint runs
`fireparq`.

```bash
docker pull ghcr.io/pinax-network/firehose-parquet:1.0.0

docker run --rm \
  -e PINAX_API_KEY=your-key \
  -v $(pwd)/output:/output \
  ghcr.io/pinax-network/firehose-parquet:1.0.0 \
  build \
  --endpoint https://eth.firehose.pinax.network:443 \
  --start-block 19000000 \
  --stop-block 19001000 \
  --output /output
```

## Cursor & Resume

`fireparq build` stores an authoritative checkpoint and an all-table transaction
journal under `<output>/.fireparq-ingest/`, in the dataset root that `--output`
names ([dataset layout](#output-directory-layout)). Every flush either recovers
its complete committed output or removes its owned uncommitted parts before
replay. The optional `_fireparq/cursor.parquet` file mirrors this authority;
deleting the mirror cannot rewind ingestion. These controls contain opaque source cursors
and should receive the same access restrictions as the cursor file.

Existing datasets without this authority are not adopted automatically. Rebuild
into a new empty output root, with an absent cursor mirror. Keep legacy datasets available
for read-only tools. See the
[transaction and migration contract](docs/audit/468-ingestion-runtime.md).

### Startup cost

A restart costs the same however large the dataset has grown (#655). When
`--output` already holds a dataset, `build` reads its control records only:
the S3 owner record, `.fireparq-ingest/` (the authority and a pending
transaction) and the `_fireparq/cursor.parquet` mirror. It lists no
data objects and walks no data directory. The only other requests are one LIST
of a control prefix (`<ancestor>/.fireparq-ingest/`) per directory above the
dataset root, which is none for a dataset at the bucket root, and after a crash
the GETs of the pending transaction's own parts.

The whole root is listed only when the dataset is created, when the root must
be empty anyway: `build` then checks the root, every directory above it and
every directory below it for another dataset. A dataset created later inside an
existing one is refused by its own check of the directories above it, so a
resume never needs to look below its root. Each listing request has a
60-second timeout; a listing has no overall deadline and logs its progress
every 10 seconds.

`firehose_parquet_startup_list_requests` and
`firehose_parquet_startup_listing_seconds` report what the last start listed,
and the `protected dataset startup checks finished` log line gives the same
counts. See [the implementation record](docs/audit/655-resume-cost.md).

## Network Aliases

`fireparq` can resolve a checked-in set of built-in Firehose network names instead of requiring `--endpoint` every time.

Examples:

- `mainnet` → `https://eth.firehose.pinax.network:443`
- `solana-mainnet-beta` → `https://solana.firehose.pinax.network:443`
- `tron` → `https://mainnet.tron.streamingfast.io:443`
- `tron-evm` → `https://mainnet-evm.tron.streamingfast.io:443`

Provider hostnames do not always mirror the network name exactly. For example, `matic` resolves to the provider hostname `polygon.firehose.pinax.network`. Run `fireparq build --help` to list every built-in name.

Aliases use the Pinax endpoint that The Graph networks registry lists. `near-mainnet`, `near-testnet`, `tron`, and `tron-evm` use StreamingFast endpoints because Pinax no longer serves them; those need a credential StreamingFast accepts, such as a The Graph Market API token in `STREAMINGFAST_API_TOKEN`. See `docs/network-registry-integration.md` for the provider policy and the weekly endpoint check.

Resolution precedence:

1. `--endpoint` or `ENDPOINT`
2. `--network` with `FIREHOSE_ENDPOINT_*` override lookup
3. `--network` built-in default endpoint

Per-network env overrides normalize network names by uppercasing and converting non-alphanumeric separators to underscores.

Removed networks are rejected during argument parsing, and startup fails early if the resolved endpoint is unavailable or unhealthy.

`build` requires EndpointInfo with a nonempty chain name before resolving output or cursor paths. Transient Info failures get three attempts with bounded backoff; exhausted retries, authentication errors, or unsupported Info stop startup. `--network`, `--block-type`, and `--cursor-override` do not bypass this requirement. This prevents a temporary metadata failure from changing the output root or hiding the existing cursor. Older servers must expose the Info RPC. Protected ingestion resolves its mapper before recovery; unknown custom chain metadata requires an explicit `--block-type`. See [the implementation record](docs/audit/467-endpoint-info.md) for retry limits and validation.

```bash
# Built-in alias
fireparq build --network mainnet --start-block 20000000 --stop-block 20001000

# Per-network override
export FIREHOSE_ENDPOINT_MAINNET=https://eth.internal.example.com:443
fireparq build --network mainnet --start-block 20000000 --stop-block 20001000

# Hyphens in the network name become underscores in the override variable
export FIREHOSE_ENDPOINT_SOLANA_MAINNET_BETA=https://solana.internal.example.com:443
fireparq build --network solana-mainnet-beta --start-block 250000000 --stop-block 250100000
```

### How It Works

1. **Receive and map** — assign source-event order before filtering or timestamp
   buffering. Every table belongs to one contiguous accepted event window;
   filtered events can advance a zero-row checkpoint.
2. **Prepare and publish** — validate the whole table inventory, save the pending
   transaction, and publish complete parts with deterministic owned names.
   Record each file's size, checksum and schema before publication.
3. **Commit and mirror** — after verifying every part, record the transaction's
   commit, advance output authority, repair the cursor mirror, and clear pending.
4. **Recover before streaming** — roll back a Writing transaction or finish a
   Committed transaction before opening Firehose Blocks: commit it to each
   Delta table whose `txn` lacks it, `blocks` last, then advance authority.
   Never infer progress from the greatest block number, a filename, or an
   external cursor alone.

### Local part publication

Protected local parts use a hidden transaction-owned `.tmp` name in the same
directory as their final `part-v1-*.parquet` name. The writer completes and syncs
the Parquet file, creates the final name with a no-clobber hard link, and syncs
directory links. Recovery verifies exact journal ownership before removing a
partial transaction or accepting a committed file. Canonical and lexical output
ancestry are both synced, preserving explicit output-root symlink aliases.

The staging names are `.fireparq-txn-<transaction>-<index>.tmp` next to each
final part; plain `*.parquet` globs ignore them. After an ordinary error (a
failed write, publish, journal update or mirror save) the build removes its own
staging names before exiting, on a best-effort basis, and leaves the journal for
recovery. If that cleanup itself fails, for example because the directory is no
longer writable, or the process is killed, a staging name can remain until the
next `build` or `fireparq recovery recover <root>` removes it from the journal plan.

This requires atomic same-directory hard links, file and directory sync, readable
directory ancestry, and macOS/Linux inode locking. Unsupported operations fail
closed. Nested symlink entries inside guarded trees are refused. External writers
that bypass ownership are unsupported. The unprotected low-level writer keeps its
own naming rules.

Final `.parquet` files are individually complete. Concurrent readers using plain
globs can still see only some tables during publication; this protocol does not
provide atomic multi-table query snapshots. Do not remove control records or
another active writer's temporary files. See the
[local publication tests](docs/audit/578-atomic-local-parquet.md) and
[transaction recovery contract](docs/audit/468-ingestion-runtime.md).

### Cursor File Format

The cursor is stored as a single-row Parquet file with two layers of data:

**Row data** (essential resume state):

| Column | Type | Description |
|---|---|---|
| `cursor` | Utf8 | Firehose opaque cursor token |
| `last_block_num` | UInt64 | Last processed block number |
| `last_block_id` | Binary | Last processed block ID (raw bytes) |
| `last_timestamp` | Int64 (nullable) | Committed routing anchor, or actual source timestamp when no anchor is needed |
| `updated_at` | Utf8 | ISO 8601 timestamp of last save |
| `start_block` | UInt64 (nullable) | Pipeline start block |
| `stop_block` | UInt64 (nullable) | Last durably proven completed exclusive request bound |

**File-level metadata** (Parquet key-value pairs in `firehose-parquet.*` namespace):

The protected mirror footer contains a versioned checkpoint envelope, its digest, and duplicated semantic configuration. Every row/configuration duplicate must agree with authority. Legacy cursor files remain readable by inspection tools; they cannot establish protected output authority.

### S3-Aware Cursor

When output is written locally or to S3, the default cursor file
(`--cursor _fireparq/cursor.parquet`) is automatically placed in the
`_fireparq/` artifact directory of the resolved dataset root — no special
configuration needed. A relative `--cursor` path resolves against the dataset
root (`--output`, with any `{chain}` expanded), in the same bucket for S3
output; an absolute local path is used as given, and an `s3://bucket/key` URI
selects its own bucket:

| Output | `--cursor` value | Cursor location |
|---|---|---|
| `./output` | *(default)* | `./output/_fireparq/cursor.parquet` |
| `s3://bucket` or `s3://bucket/` | *(default)* | `s3://bucket/_fireparq/cursor.parquet` |
| `s3://bucket/prefix` | *(default)* | `s3://bucket/prefix/_fireparq/cursor.parquet` |
| `s3://bucket/prefix/{chain}` | *(default)* | `s3://bucket/prefix/<chain_name>/_fireparq/cursor.parquet` |
| `s3://bucket/prefix` | `my-cursor.parquet` | `s3://bucket/prefix/my-cursor.parquet` |
| `s3://bucket/prefix` | `s3://other/path.parquet` | `s3://other/path.parquet` |
| `./output` | `s3://other/path.parquet` | `s3://other/path.parquet` |

An explicit S3 cursor URI uses its own bucket and key. It can be separate from
the data bucket; both use the configured AWS credentials, region and endpoint.
For separate buckets, use a service endpoint or omit the endpoint for standard
AWS S3. Bucket-specific AWS endpoints (including global, regional, dualstack and
accelerate forms) and `bucket.fly.storage.tigris.dev` use virtual-hosted requests
and reject a different cursor bucket. Other custom endpoints must support
path-style requests at a service endpoint; arbitrary bucket-specific custom
domains are not inferred. These addressing rules also apply to S3 inspection
and recovery commands.
Relative cursor paths inherit the resolved output bucket and prefix, and
absolute local cursor paths remain absolute for local output.

The mirror location is bound when a dataset is created. Before v1.0.0 the
default was `<dataset root>/cursor.parquet`; a dataset created with that default
refuses the new default before any Blocks request and names the fix: keep
passing `--cursor cursor.parquet` (`CURSOR=cursor.parquet`) for it. The mirror
is never moved automatically.

**S3 writes require an explicit `s3://bucket/prefix` output** (#617). `build`
(`--output` / `OUTPUT`) never expands a relative output into `--s3-bucket` /
`S3_BUCKET`. When a bucket option is set, a relative output (including the
default `.`) is rejected before contacting Firehose or storage, with the
explicit URI suggested.
Without a bucket option a relative output is a local path. Explicit local paths
(`./output`, `../output`, or an absolute path) are always local. When `--output`
is an S3 URI, `--s3-bucket` / `S3_BUCKET`, if set, must name the same bucket; this
check does not restrict an explicit cursor URI to the data bucket. The bucket
name is always literal (`{chain}` may only appear in the key prefix), so these
checks run before the endpoint is contacted. Before the first write, `build`
logs `resolved write destinations` with the absolute output and cursor-mirror
locations (and `resolved --output template` when `{chain}` was expanded).

An S3 cursor requires complete explicit AWS credentials even when data output
is local; `--cursor` never silently falls back to instance metadata credentials.

Authenticated S3 `build` spools each Parquet part to private temporary disk in
`$TMPDIR` (else `/tmp`), then streams one conditional PUT and verifies the entire
object through a second private spool before committing. Budget roughly two
encoded parts of free space for every part in flight, in addition to mapper
memory: with the default flush concurrency that is up to `--flush-inflight-bytes`
(256 MiB) of upload spools plus one readback spool per concurrent publication,
about 400 MiB at the 32 MiB file target; `--flush-inflight-bytes 1` brings it
back to two parts. Kubernetes pods with `readOnlyRootFilesystem` need a writable
volume there, for example an `emptyDir` mounted at `/tmp`. Native ingestion requires an HTTPS endpoint and
limits a part to 5,000,000,000 encoded bytes and its serialized footer to 32 MiB;
resume verification applies the same limits. Connections have a 10-second timeout;
upload and complete readback each have a 15-minute deadline. A write whose
outcome is uncertain (timed out, cancelled, lost or unverifiable acknowledgement)
retains ownership for provider-quiescent recovery; an HTTP 401/403 refusal is
definite and does not. See [qualification and limits](docs/audit/520-bounded-s3-ingestion.md).

### Parameter Validation on Resume

Protected output binds the original start, chain and mapper family, exact table
schemas and mapper epoch (which fixes the `date=YYYY-MM-DD` layout), identifier
encoding,
effective feature flags, output storage identity and mirror location. A mismatch
stops before Blocks. Compression and flush thresholds may change without changing
logical rows. Endpoint aliases do not relax storage-service binding.

Rerun with the same original start, or omit `--start-block` to use the stored
origin. An already completed stop is a no-op after recovery and mirror repair;
an increased stop resumes from the exact authoritative source cursor. Omit the
stop for live continuation. Solana routing anchors and non-nullable-chain
bootstrap lookahead are persisted with their source identity, so restart does
not choose a new timestamp for already accepted rows.

### Missing or Unreadable Cursor

A missing or genuinely older mirror is repaired from authority before streaming.
An ahead, foreign, malformed or unreadable mirror fails closed; it never selects
a new resume point. An existing legacy cursor also blocks initialization of a
new dataset. Changing or disabling the mirror of an existing protected dataset is refused.

`--cursor none` (any case, also `CURSOR=none`) creates a dataset without a
mirror. Authority under `.fireparq-ingest/` remains mandatory and alone selects
the resume cursor, completed bounds and routing anchors, so resume, extension,
same-bound no-ops and recovery behave exactly as with a mirror. The choice is
bound when the dataset is created: every later `build` must pass `--cursor none`
again, and a dataset created with a mirror cannot drop it. Without a mirror
there is no `<root>/_fireparq/cursor.parquet` hint for other tools.

Local mirror saves use private same-directory temporary files, atomic replacement,
file and directory sync, and up to three attempts with 1 and 2 second backoff.
S3 mirror updates use one conditional Create/Update with transport retries disabled,
then exact readback. Failed or cancelled saves preserve pending recovery state;
a shutdown during local retry backoff still reports the durability failure.

`build` and `recovery` hold common ownership over the output and an external
cursor location. Local ownership uses macOS/Linux directory locks; nested
symlinks inside mutation trees are refused. S3 ownership covers the whole bucket:
there is one owner per bucket, and a second writing command on any prefix of it
fails with `bucket ownership is held`. It requires conditional-write
support plus access to reserved control keys. Unresolved remote errors retain
ownership without an expiry or automatic takeover, and the next run fails until
it is released. A failed `build` releases S3 ownership on exit when every request
it sent had a definite outcome, including when its failed transaction is still
pending: the next `build` recovers that transaction before streaming. It keeps
ownership after an uncertain request (timeout, lost acknowledgement, connection
reset, unverifiable readback, 5xx, 409/412), a second shutdown signal or a
panic, and its error then says why and prints the exact `recovery status` and
`recovery release` commands. A Delta log commit is the exception: the next
start reads the table's `txn` to learn whether it landed, so an uncertain one
does not keep the owner. `recovery` keeps S3 ownership after any error and
logs the same guidance.
`fireparq recovery status <path>` reads a summary. Explicit remote release requires
the exact owner/generation and evidence that both the writer and all prior remote
requests are quiescent; stopping the process alone is insufficient. See the
[ownership and recovery runbook](docs/audit/468-stage1-ownership.md).

#### Delta tables and the maintenance job

The owner guards one fireparq writer (`build`, or `recovery recover`) and the
state only it changes: `.fireparq-ingest/` (authority and the pending
journal), `_fireparq/cursor.parquet` and its own uncommitted parts. It does not
make the Delta tables exclusive. The `deltalake` maintenance job (OPTIMIZE,
VACUUM, checkpoints, log cleanup) commits to them through their logs beside a
running `build` and never takes the owner (#636):

- fireparq's commits are blind appends with a `txn` per stream; it never
  removes, rewrites or deletes a committed file, and maintenance and its
  appends retry at the next version instead of conflicting;
- recovery reads each table's `txn`: it commits an interrupted transaction
  only to the tables whose logs lack it, and never reads a part a log already
  holds, so a compacted and vacuumed part cannot stop a restart;
- a lite VACUUM (the job's default) never deletes a part no log references
  yet. A full VACUUM with a retention shorter than an outage can; the next
  start then stops with a message naming the part and keeps the journal, and
  the dataset is rebuilt into a new root. Run full VACUUM at most weekly with
  the enforced 7-day retention.

Give the job its own S3 user: List on the bucket; Get, Put and Delete on each
table prefix (`<table>/*`, data files and `_delta_log/`); nothing on
`.fireparq-ingest/`, `_fireparq/`, `.fireparq-owner-v1.json` or
`.fireparq-owner-probes-v1/`. Give the writer's user no DeleteObject on
`*/_delta_log/*`: fireparq writes no checkpoint and cleans no log. The
[#636 record](docs/audit/636-delta-ownership.md) has an RGW bucket policy for
both users, and the [recovery record](docs/audit/643-l4-delta-recovery.md)
the crash cases.

`recovery recover` selected at a table, a partition or a parent directory
discovers every affected protected dataset and its external mirror before it
recovers anything.

### Cursor Override and Migration

`--cursor-override` cannot reset, rewind or change protected output semantics,
and a real `build` rejects it before contacting the endpoint, even at a new root.
Use a new empty output and absent mirror when changing the original range, schema
or feature flags. Legacy random-name output has no proof relating all parts to
its cursor, so this release provides no implicit adoption or override escape.
Read-only dry-run behavior can still inspect legacy cursor defaults.

### Graceful Shutdown

On SIGINT (Ctrl-C) or SIGTERM, the pipeline:

1. Stops consuming new blocks from the gRPC stream. A block being processed is
   finished first; waits for the endpoint (connecting, reconnect back-off, an
   idle stream, startup checks) are interrupted without waiting for their
   network timeout, even on a quiet chain
2. Discards partial in-memory buffers instead of writing extra part files
3. Leaves the cursor at the last committed flush
4. Exits cleanly (exit code 0)

An in-flight block or storage write finishes before exit. If a cursor save has
failed, the same signal interrupts its retry backoff and the durability error
still produces a non-zero exit.

A second SIGINT or SIGTERM exits immediately with code 130, without waiting for
the current block. In-flight writes may be interrupted: a hidden temporary part
may remain incomplete, or a cursor update may not finish its durability checks.
A published local table part already has a complete footer. The next owned
recovery reconciles its pending transaction before any source replay. A first
signal releases S3 bucket ownership on exit; a second signal keeps it, because
an interrupted request may still complete.

If a write (local disk or S3), a block mapping, or the stream fails, the
pipeline also discards partial buffers and does not save the cursor, then exits
non-zero. Recovery removes verified parts from an uncommitted transaction before
replaying its window, or finishes a committed transaction without remapping it.
S3 ownership is released on exit unless a request had an uncertain outcome.
S3 recovery additionally requires explicit release after provider-confirmed
request quiescence whenever the prior owner remains retained.

Only a stream that ends cleanly (for example, by reaching `--stop-block`)
flushes the remaining buffers and saves the final cursor.

### Start and Stop Blocks

- **Start above the last irreversible block.** With `--final-blocks-only`
  (the default), Firehose serves a request whose start block is above the
  current last irreversible block (LIB) from LIB+1. Without a resume cursor,
  blocks below `--start-block` are skipped before mapping: the first one is
  logged, and all of them are counted in
  `firehose_parquet_blocks_skipped_below_start_total` and in the
  `blocks_skipped_below_start` field of the final summary.
- **Bounded runs** (`--stop-block` is exclusive) record completion only after
  clean EOF, all received events are acknowledged, and the last accepted event
  reaches `stop_block - 1`. A sparse or empty tail alone cannot prove coverage,
  including on Solana, NEAR and Beacon: the accepted prefix is durable, but the
  command exits nonzero with a diagnostic. `--dry-run` applies the same rule
  on every chain, so it fails exactly where the real build would. Repeating an
  already proven bound opens no Blocks request; extending it uses the
  authoritative cursor.
- **Live runs** (no `--stop-block`) never end on their own: if the server or a
  proxy closes the stream cleanly, the run reconnects from the last cursor with
  the usual back-off.

## CLI Reference

The primary ingestion workflow is `fireparq build`. Utility workflows stay
under the subcommands `inspect`, `validate`, `recovery` and `completions`;
engines read the tables ([Reading the tables](#reading-the-tables)). The global flags
`--log-level` (`LOG_LEVEL`, default `info`), `--verbose` (`VERBOSE`) and
`--env-file` (`FIREPARQ_ENV_FILE`) apply to every command.

For full CLI help, run `fireparq --help` for the top-level command surface or
`fireparq build --help` for ingestion-specific flags. The summary below keeps
the main operator path up front and leaves the less-common deployment and
recovery knobs to dedicated advanced sections.

### Common ingestion flags

| Area | Common flags |
|---|---|
| Connection | `--network <NETWORK>` or `--endpoint <ENDPOINT>` |
| Range | `--start-block <START_BLOCK>`, `--stop-block <STOP_BLOCK>` (omit the stop block for live mode) |
| Resume | Rerun the same original range; output authority selects progress and repairs the bound optional cursor mirror (`--cursor`, default `_fireparq/cursor.parquet` in the dataset root, or `none`) |
| Output | `--output <OUTPUT>` (`OUTPUT`, default `.`; an explicit `s3://bucket/prefix` for S3): the dataset root, used exactly as given, with an opt-in `{chain}` placeholder for the endpoint's chain name, for example `--output 's3://datasets/{chain}'` ([dataset layout](#output-directory-layout)); every table is a Delta table at `<table>/`, with its data files in `<table>/date=YYYY-MM-DD/`; `--compression <COMPRESSION>` (default `zstd`) |
| Chain | `--block-type <BLOCK_TYPE>` (default `auto`), plus chain-specific toggles like `--without-extended` or `--without-votes` only when needed |
| Runtime | `--final-blocks-only[=true\|false]` (default `true`), `--flush-bytes <FLUSH_BYTES>` (compressed file target, `0` disables), `--flush-memory-bytes <FLUSH_MEMORY_BYTES>` (summed mapper estimate), optional `--flush-rows` / `--flush-blocks` / `--flush-interval-secs` (`0` disables rows and interval; the interval applies at the chain head only, [details](#flush-interval-and-catch-up)) |
| Flush concurrency | `--flush-encode-concurrency` (`FLUSH_ENCODE_CONCURRENCY`, default `2`), `--flush-publish-concurrency` (`FLUSH_PUBLISH_CONCURRENCY`, default `4`, also the local I/O threads), `--flush-inflight-bytes` (`FLUSH_INFLIGHT_BYTES`, default 256 MiB): bounded table work inside each flush ([details](#advanced-s3--deployment-knobs)) |

### Non-final streams and reorgs

Finalized-only output is the default. Use `--final-blocks-only=false` to receive
reversible blocks, or set `FINAL_BLOCKS_ONLY=false`. An explicit CLI value takes
precedence over the environment. The bare `--final-blocks-only` flag still means
`true`; optional values use `=` so the flag cannot consume a following command.
Whether a run is live (no `--stop-block`) is independent of whether blocks must
be final.

Non-final output is an **append-only event history**. Every mapped envelope adds
its block's rows (to `blocks` and to every other table the block has rows in),
with two extra columns that final-only output does not have, `stream_ordinal`
directly after `fork_step`:

- `fork_step` (`Utf8`): `NEW` adds a block, `UNDO` records its removal from the
  chain (the undone block's rows are written again, marked `UNDO`), and `FINAL`
  is an explicit final event if the endpoint sends it. The usual non-final
  protocol sends `NEW` and occasional `UNDO`, not a later `FINAL` for every
  block. UNDO does not delete earlier rows.
- `stream_ordinal` (`long`): the accepted-event ordinal of the envelope that
  produced the row, the same for every row of that envelope in every table. It
  is strictly increasing in delivery order and durable: the protected session
  assigns it when the envelope is received and continues it from the output
  authority (`.fireparq-ingest/`) across reconnects and restarts, and each
  part's name records the window of ordinals its rows belong to
  (`part-v1-<stream>-<first>-<last>-...`). After a crash, recovery either keeps
  a transaction's rows with their ordinals or removes its rows before those
  ordinals are assigned again, so no two committed events share an ordinal.
  Ordinals can skip values (envelopes below `--start-block` write no rows).

A block identity can return as `NEW` after an `UNDO`, and a replay or reconnect
can deliver the same block again; every delivery is a new event with a new
ordinal. Block height, block time, `lib_num`, file names and row order are not
delivery-order keys, and neither the steps alone nor counting NEW minus UNDO
gives the current state: `NEW(A), UNDO(A), NEW(A)` ends with A present, while
`NEW(A), NEW(A), UNDO(A)` ends with A absent despite the same unordered rows.
`stream_ordinal` is the order that decides.

#### Canonical live view

For each `block_num`, the event with the highest `stream_ordinal` decides the
head:

- latest is `NEW` (or `FINAL`) of block X: X is canonical at that height;
- latest is `UNDO`: the height currently has no block (a reorg removed it and
  nothing has replaced it yet, typically at the tip);
- any other step is treated as unresolved: no block.

A row of any table, `blocks` included, belongs to the head only when its
`(block_num, block_id, stream_ordinal)` matches that latest event. Matching the
ordinal and not only the block identity keeps exactly one copy of a block that
was delivered more than once: after `NEW(A), UNDO(A), NEW(B)` only B's rows
remain, and after `NEW(A), UNDO(A), NEW(A)` only the rows of the second
`NEW(A)`.

```sql
-- DuckDB views over one live (non-final) chain root. Replace live/mainnet
-- with that root, for example s3://live-bucket/v1/mainnet.
CREATE OR REPLACE VIEW live_head AS
SELECT block_num, block_id, stream_ordinal
FROM (
  SELECT block_num, block_id, fork_step, stream_ordinal,
         row_number() OVER (PARTITION BY block_num ORDER BY stream_ordinal DESC) AS latest
  FROM read_parquet('live/mainnet/blocks/**/*.parquet', hive_partitioning = false)
) events
WHERE latest = 1 AND fork_step IN ('NEW', 'FINAL');

-- One view per table, blocks included: the rows of each height's latest event.
CREATE OR REPLACE VIEW live_blocks AS
SELECT t.*
FROM read_parquet('live/mainnet/blocks/**/*.parquet', hive_partitioning = false) t
WHERE EXISTS (
  SELECT 1 FROM live_head h
  WHERE h.block_num = t.block_num
    AND h.block_id = t.block_id
    AND h.stream_ordinal = t.stream_ordinal
);

CREATE OR REPLACE VIEW live_transactions AS
SELECT t.*
FROM read_parquet('live/mainnet/transactions/**/*.parquet', hive_partitioning = false) t
WHERE EXISTS (
  SELECT 1 FROM live_head h
  WHERE h.block_num = t.block_num
    AND h.block_id = t.block_id
    AND h.stream_ordinal = t.stream_ordinal
);
```

`live_head` reads only the `blocks` table, which has exactly one row per event.
`hive_partitioning = false` keeps the partition directories out of the columns,
so every view has the table's own schema.

**Spark and Trino.** The window subquery and the `EXISTS` semi-join are standard
SQL and run unchanged there (DuckDB's `QUALIFY` is avoided because Spark and
Trino lack it); replace `read_parquet(...)` with a table or path over the same
files, for example `` parquet.`s3a://live-bucket/v1/mainnet/blocks/` `` with
`recursiveFileLookup` in Spark, or an external Hive table in Trino. Engines
without unsigned integers may read `block_num` and `stream_ordinal` as
`DECIMAL(20,0)` (Spark) or as a signed 64-bit integer; ordinals stay far below
2^63, so ordering and equality are unaffected. The union below uses DuckDB's
`SELECT * EXCLUDE` and `UNION ALL BY NAME`: in Spark use
`DataFrame.drop("fork_step", "stream_ordinal")` and `unionByName`, in Trino list
the columns.

#### Two-bucket union

A live view shows the reversible head, but the separately built final-only
dataset is the source of truth (see
[Live + final two-bucket deployment](#live--final-two-bucket-deployment)). Read
every table from the final bucket up to its **final frontier**, the highest
`block_num` in its `blocks` table, and from the live view above it:

```sql
-- DuckDB, after the live views above. Replace final/mainnet with the final
-- dataset's chain root, for example s3://final-bucket/v1/mainnet.
CREATE OR REPLACE VIEW final_frontier AS
SELECT max(block_num) AS block_num
FROM read_parquet('final/mainnet/blocks/**/*.parquet', hive_partitioning = false);

CREATE OR REPLACE VIEW canonical_blocks AS
SELECT *
FROM read_parquet('final/mainnet/blocks/**/*.parquet', hive_partitioning = false)
WHERE block_num <= (SELECT block_num FROM final_frontier)
UNION ALL BY NAME
SELECT * EXCLUDE (fork_step, stream_ordinal)
FROM live_blocks
WHERE block_num > (SELECT block_num FROM final_frontier)
   OR (SELECT block_num FROM final_frontier) IS NULL;

CREATE OR REPLACE VIEW canonical_transactions AS
SELECT *
FROM read_parquet('final/mainnet/transactions/**/*.parquet', hive_partitioning = false)
WHERE block_num <= (SELECT block_num FROM final_frontier)
UNION ALL BY NAME
SELECT * EXCLUDE (fork_step, stream_ordinal)
FROM live_transactions
WHERE block_num > (SELECT block_num FROM final_frontier)
   OR (SELECT block_num FROM final_frontier) IS NULL;
```

- The result has the final-only schema. Both datasets must be the same chain
  with the same byte encoding and table options (for example both with or both
  without `--without-extended`).
- Every table is cut at the frontier of `blocks`, so a table never mixes both
  buckets at one height. A final flush publishes its tables one after another:
  a query that runs while the final writer publishes can briefly see a new
  `blocks` part before its child parts. Query between final runs, or accept
  that short gap.
- The live bucket must still hold every height above the final frontier. Size
  its expiration so that the frontier's worst lag (a day, plus the time the
  daily job takes) stays well inside it.

Use different output roots/cursors for final-only and non-final captures;
resuming a cursor with a different mode is incompatible. A bounded non-final
run warns on successful completion because reaching its stop does not prove
that its tail is final, and later UNDO events will not be received after it
stops. A saved cursor or successful exit is not a finality certificate. See the
[`stream_ordinal` and live view record](docs/audit/648-stream-ordinal.md) and the
[original non-final implementation](docs/audit/474-non-final-streams.md).

### Live + final two-bucket deployment

A dataset that shows the chain head while keeping a canonical history uses two
writers, each with its own bucket (S3 ownership is bucket-wide, so a live
`build` that never stops would block every other mutating command in its
bucket). Query them with the [two-bucket union](#two-bucket-union).

| Setting | Final writer (source of truth) | Live writer (chain head) |
|---|---|---|
| `FINAL_BLOCKS_ONLY` | `true` (the default) | `false` |
| Partitions | `<table>/date=YYYY-MM-DD/` | `<table>/date=YYYY-MM-DD/` |
| Range | Bounded daily runs: the same `START_BLOCK` on every run, `STOP_BLOCK` at the first block of the next UTC day | Live: no `STOP_BLOCK` |
| Flush | Defaults (`FLUSH_BYTES` 32 MiB target) | `FLUSH_INTERVAL_SECS` (at the head; [size-based while catching up](#flush-interval-and-catch-up)) and/or `FLUSH_BLOCKS` |
| Compaction | The `deltalake` maintenance CronJob (#643) | None: live parts expire |
| Retention | Kept | S3 lifecycle expiration, for example after 2 days (48 hours), on table prefixes only |

**Final writer.** One protected stream per root: each run repeats the original
`START_BLOCK` and extends `STOP_BLOCK` (exclusive) to the first block of the
next UTC day, for example the live bucket's first block of that day (see
[block range of a day](#block-range-of-a-day)). A repeated bound opens no Blocks request;
a larger one resumes from the output authority. `build` exits at the bound, and
a run that stops early resumes from the same authority the next day.

**Live writer.** One unbounded `build` with `FINAL_BLOCKS_ONLY=false`. For the
first run, set `START_BLOCK` at or below the final
dataset's frontier so the union has no gap; later runs resume from authority.
Rows reach the bucket at the next flush: `FLUSH_INTERVAL_SECS=N` flushes when a
block arrives at least N seconds after the previous flush, `FLUSH_BLOCKS=K`
after K blocks, whichever comes first (day boundaries and the size triggers,
`FLUSH_BYTES` and `FLUSH_MEMORY_BYTES`, also flush). Lower values mean fresher
data and more objects. The interval applies once the writer has caught up
with the head: after a restart or an outage it catches up with size-based
flushes first ([flush interval and catch-up](#flush-interval-and-catch-up)).
The live parts are expired rather than compacted.

**Expected objects per day.** Each flush writes one part per table that has rows
in it.

- Live: about (flushes per day) × (tables with rows). At the head, flushes per
  day are the larger of 86,400 / `FLUSH_INTERVAL_SECS` (at most one per block) and blocks
  per day / `FLUSH_BLOCKS`, plus the day boundary and any size-triggered
  flushes. Ethereum (7,200 blocks a day) with
  `FLUSH_INTERVAL_SECS=60` makes about 1,460 flushes a day: with 15 tables with
  rows, about 22,000 objects a day. `FLUSH_BLOCKS=1` instead makes about 108,000.
  Because S3 rounds each expiry up to the next midnight UTC, a 48-hour rule
  keeps two to three days of objects.
- Final: about one part per table per flush, where flushes follow the 32 MiB
  target of the largest table plus one per day boundary.

A `build` start lists the dataset only when it creates it
([startup cost](#startup-cost)), so the number of retained objects does not
slow a restart.

**Lifecycle expiration.** S3 lifecycle filters select objects by prefix, tag or
size and cannot exclude a path, so create one expiration rule per table prefix:
`<prefix>/<table>/` below the dataset root (`<table>/` at a bucket root), for
every table the chain writes ([schema reference](docs/schemas/README.md)). A rule
must never match control state:

- `.fireparq-ingest/` in the dataset root: the output authority, checkpoint and
  transaction journal;
- the `.fireparq-owner*` records at the bucket root: the owner record and its
  probes;
- `_fireparq/`, which holds the cursor mirror.

If bucket versioning is enabled, an expiration only adds a delete marker: add a
noncurrent-version expiration (and expired delete marker cleanup) to reclaim the
space.

Expiring committed parts is safe for the live writer. `build` never reads a
committed part outside its own pending transaction: a running build and its
next flushes, a restart (recovery and resume from authority, which read only
control records, [startup cost](#startup-cost)), the cursor mirror, `recovery status` and
`recovery recover` are unaffected when every part of earlier hours disappears.
The one exception is a writer that crashes with a committed but unfinished
transaction and then stays down longer than the expiration: its next start
verifies that transaction's parts and refuses if they expired
(`committed transaction is missing a required final part`). Live data is
disposable: start a new live dataset (a new prefix or an emptied bucket) at or
below the final frontier.

### Advanced authentication

Most deployments should use the provider-scoped variables in [Authentication](#authentication).
For custom endpoints or different secret names, explicitly authorize a credential
for the destination with:

- `--api-key-envvar <API_KEY_ENVVAR>`
- `--api-token-envvar <API_TOKEN_ENVVAR>`

```bash
fireparq build \
  --network mainnet \
  --api-key-envvar INTERNAL_FIREHOSE_API_KEY \
  --start-block 20000000 \
  --stop-block 20001000 \
  --output ./output
```

### Advanced recovery / override behavior

These flags are intended for recovery-heavy or operator-managed deployments
rather than the default workflow:

| Flag | Use when |
|---|---|
| `--cursor-override` | Read-only `--dry-run` only: ignore legacy cursor defaults or an unreadable cursor. A real `build` rejects it, even at a new root; protected output never rewinds, so use a new empty root for changed semantics |
| `--stream-idle-timeout-secs <N>` | Supervising long-lived pipelines that should self-reconnect after a silent stream stall (default 120; `0` disables and relies on HTTP/2 keepalive). On slow chains such as Bitcoin (~600 s blocks), set it above the block time to avoid a reconnect every 120 s. An idle reconnect is not counted as a failure. |
| `--reconnect-stall-timeout-secs <N>` | Fail fast when reconnect loops should hand control back to an external supervisor (default 900; `0` disables). The timer starts at the first failed attempt and is reset only when a stream message arrives, not when a connection or RPC succeeds. |

#### Receive transport

`build` uses 16 MiB HTTP/2 stream and connection
receive windows and accepts plain, gzip, or zstd replies. The server selects the
response encoding; requests remain uncompressed. Parquet `--compression` is
independent of transport compression.

| Flag / environment | Behavior |
|---|---|
| `--grpc-window-bytes` / `GRPC_WINDOW_BYTES` | Initial stream and connection receive window, default `16777216`. `0` restores the underlying library defaults. Larger windows allow more data in flight and can increase buffering. |
| `--grpc-adaptive-window[=true\|false]` / `GRPC_ADAPTIVE_WINDOW` | Opt into automatic window tuning; default false. When true, it overrides `--grpc-window-bytes`. |
| `--grpc-max-message-bytes` / `GRPC_MAX_MESSAGE_BYTES` | Maximum encoded or decompressed protobuf response bytes, default `134217728` (128 MiB). Values must be positive and fit UInt32. Applies to Info and the ingestion stream. |

The message limit is a per-response bound, not a cap on total process memory.
An oversized response fails with an error; increasing the limit permits larger
allocations. To restore the previous windows explicitly, use
`--grpc-window-bytes 0 --grpc-adaptive-window=false`.
The selected 16 MiB default improved a bounded local transport benchmark at both
zero added latency and 50 ms simulated round-trip latency; this is not an
end-to-end ingestion or provider performance guarantee. See the
[measurements and tradeoffs](docs/audit/517-grpc-transport.md).

#### Connection errors

- **Fatal errors fail fast.** A stream rejected with `Unauthenticated`,
  `PermissionDenied`, `InvalidArgument`, `FailedPrecondition`, `OutOfRange` or
  `Unimplemented` ends the run with an error and a hint (credentials, range or
  cursor), instead of reconnecting. This includes statuses Firehose relays as
  `Unknown` with the original code in the message
  (`rpc error: code = InvalidArgument desc = ...`), and credentials that are not
  valid for the endpoint (for example a Pinax token used against a
  StreamingFast endpoint). A `ResourceExhausted` error that reports an
  exhausted quota (for example `billable egress bytes quota exceeded`) is
  fatal too, as is a local decompressed-response size limit violation. They are counted in
  `firehose_parquet_errors_total{kind="grpc_fatal"}`.
- **Other errors are retried** with exponential back-off from 1 s to 60 s,
  including other `ResourceExhausted` errors such as rate limits. The back-off
  resets only once a stream message arrives.
- **Limits.** A run gives up when `--reconnect-stall-timeout-secs` passes
  without a stream message, or after 30 consecutive failed attempts without a
  message (over 20 minutes at the maximum back-off), even when the stall
  timeout is disabled.

### Advanced S3 / deployment knobs

Most operators point `--output` at a local path or an explicit
`s3://bucket/prefix`. S3 output or an S3 cursor for `build` requires both an
access key ID and a secret access key, from the flags or `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY`. `build` never
fall back to profile or instance-metadata credentials. The remaining flags are
only needed for custom deployment environments:

| Flag (environment) | Purpose |
|---|---|
| `--s3-bucket` (`S3_BUCKET`) | Optional check that an explicit `s3://` output uses this bucket; a relative output is then rejected, never expanded |
| `--aws-access-key-id`, `--aws-secret-access-key`, `--aws-session-token`, `--aws-region` (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_REGION`) | S3 credentials and region |
| `--aws-endpoint-url` (`AWS_ENDPOINT_URL_S3`) | Target S3-compatible object stores |
| `--cache-control` (`CACHE_CONTROL`) | Cache-Control header for S3 uploads, default `public, max-age=31536000, immutable`; an empty string sends no header |
| `--metrics-port` (`METRICS_PORT`) | Expose Prometheus and health endpoints for monitored deployments |

Each `build` mapper flush commits its nonempty tables together before advancing
output authority and the cursor mirror. `--flush-bytes` is a **target compressed
size for the largest table's file**, defaulting to 32 MiB in Config and
`build`. Build starts with a conservative
calibration flush, then learns the compressed-to-mapper-size ratio from actual
committed file sizes. This prediction resets on restart; dry runs keep the
conservative estimate because they produce no file receipts. Files can overshoot by
one mapped block, and smaller tables naturally produce smaller files.

Build separately flushes when the **sum of all mapper table estimates** reaches
`--flush-memory-bytes` (default 256 MiB, positive). This remains active with
`--flush-bytes 0`, which disables only the compressed-size target. The estimate
counts populated mapper values, **not process RSS**: decoder and bootstrap
payloads, reserved allocator capacity, materialized Arrow batches, and the active
Parquet encoder/output need additional memory. A single block can exceed the
threshold before the next check. Compared with the former 32 MiB largest-table
trigger, adaptive windows may use substantially more memory; lower this separate
threshold to constrain estimated accumulation.

`--flush-rows`, `--flush-blocks`, `--flush-interval-secs` (at the chain head
only, see below), UTC day changes, the
memory threshold and clean end of input can all force files below the size
target. `--flush-rows 0` and `--flush-interval-secs 0` disable those triggers,
like `--flush-bytes 0`; `--flush-blocks` and `--flush-memory-bytes` must be positive. Highly compressible data may never reach 32 MiB before the memory
threshold; increasing the file target does not bypass that threshold.

#### Flush interval and catch-up

`--flush-interval-secs` bounds how long rows wait for a commit while `build`
follows the chain head. While it replays history instead (a first start, a
restart after an outage, or a `--stop-block` backfill far behind the head),
nobody is waiting for those rows, so the interval is suspended. The size
triggers (`--flush-bytes`, `--flush-memory-bytes`), `--flush-rows`,
`--flush-blocks`, day boundaries and the end of the stream still flush, and
files reach the 32 MiB target instead of holding an interval's worth of
historical blocks. No flag controls this: `build` measures its pace from the
stream, for final-only and non-final streams and any block time.

- **Catching up**: three samples in a row, covering at least 30 seconds, in
  which block time advanced more than 2× faster than the wall clock. A sample
  lasts at least 5 seconds and ends at a block with a timestamp.
- **Caught up**: samples at no more than 1.25× the wall clock covering 20
  seconds (the writer's own commits do not count toward them), or 60 seconds
  without a block timestamp. Samples between 1.25× and 2× change nothing.
- Block age is not used: a final-only stream stays about the finality lag
  behind the tip even at the head (measured on 2026-09-27: 15.8 minutes on
  Ethereum, 6.7 on Base, 1.0 on Arbitrum, 0.4 on Robinhood). Finality bursts,
  such as an Ethereum epoch of 32 blocks every 6.4 minutes, do not look like a
  catch-up: a wait longer than one sample between two blocks is measured on
  its own, so the gap after each burst reads as real time.
- Every run starts caught up, which is also the answer when in doubt. Missing,
  repeated or backward timestamps (such as Solana slots without a block time)
  never count as catching up, and a catch-up slower than 2× the chain's rate
  keeps the interval.
- After a catch-up reaches the head, the first block after 20 seconds of
  real-time pace flushes the open window on the interval. On a bursty
  final-only chain that is the first block after the finality gap, the first
  block that could flush anyway. A stall of more than 20 seconds during a
  catch-up (a reconnect, for example) also reads as caught up, so the next block
  can flush a smaller file; the replay switches back 30 seconds after it
  resumes.

Each switch is logged once at `info`, as `catching up: ...` or `caught up: ...`
with `block_time_ratio`, `blocks_per_sec` and `evidence_secs`. The
`firehose_parquet_catching_up` gauge is 1 while catching up, and
`firehose_parquet_flushes_total` counts flushes by `trigger` and `pace`. See
[the implementation record](docs/audit/659-adaptive-flush.md).

Within one flush, tables are encoded and published concurrently but under
explicit bounds. `--flush-encode-concurrency` (default 2, 1-64) caps Parquet
encoders running at once; `--flush-publish-concurrency` (default 4, 1-64) caps
part publications, and for local output also the threads that stage, publish and
verify files. `--flush-inflight-bytes` (default 256 MiB) caps encoded parts that
are encoding, staged or publishing: memory for local and generic S3 output,
private disk spool for native S3 (whose readback verification can hold one more
copy per publication). An encoder is admitted only when its initial reservation
fits and grows it while writing; if the budget cannot grow it, the part is encoded
again alone later. One part larger than the whole budget runs alone and logs an
overshoot warning. Each active encoder additionally holds its table's encoder
working memory, and mapper batches stay allocated until the flush commits.

The transaction contract is unchanged: one Writing journal at a time, receipts
journaled one at a time with each part's exact receipt durable before that part
publishes, and authority and the mirror advance only after every part has
published and every final verified. The first error stops new work, waits for
work already started, and leaves the journal for recovery. Output bytes and part
names do not depend on these settings. `1`/`1` still lets the next table encode
while the previous one publishes; `--flush-inflight-bytes 1` makes table work
strictly one part at a time. The committed-flush log reports `commit_ms` and the
observed `peak_encoders`, `peak_publications` and `peak_inflight_bytes`. See
[the qualification and benchmark](docs/audit/516-bounded-flush-concurrency.md).

Runtime logs distinguish mapper batches from successfully materialized Parquet
output. On graceful shutdown or failure, remaining mapper data is not written
and authority is not advanced. An interrupted transaction is reconciled before
replay; a storage error stops ingestion and retains recovery evidence.

When a partition boundary is detected during ingestion, the mapper flush for the
old partition is written immediately, and the same
writer outcome logs are emitted for that boundary-triggered flush.

When the first streamable block is missing timestamp metadata, fireparq now
automatically preserves those leading bootstrap blocks in output and
synthesizes their timestamps from the first later block that includes timestamp
metadata.

For Solana date partitions, missing `block_time` values keep canonical
`timestamp` / `date` null. Partition routing uses the last known timestamp only,
seeded from the Solana first-streamable anchor (`2020-03-16 14:29:00 UTC`) for
the initial span and updated whenever a real block timestamp is observed.

## Subcommands

`scan` was removed in v1.0.0: DuckDB and Polars read the tables, and a
table's files, rows, bytes and days come from its Delta log (see
[Reading the tables](#reading-the-tables)).

### `inspect` — Display File Metadata

Displays comprehensive metadata for a single Parquet file: file-level key-value pairs (including custom `firehose-parquet.*` entries), the full Parquet schema with physical/logical types, row group statistics, and per-column chunk details (encoding, compression, sizes). It reads one file, such as a table's data file, a Delta checkpoint or the cursor mirror, and refuses a directory. Supports local paths, shorthand S3 keys via `S3_BUCKET`, and explicit S3 URIs.

```bash
# Inspect a data file of a local table
fireparq inspect ./output/mainnet/blocks/date=2026-01-15/part-v1-<...>.parquet

# Resolve a shorthand key against S3_BUCKET when no local path matches
S3_BUCKET=ethereum-mainnet fireparq inspect _fireparq/cursor.parquet

# Inspect a data file of an S3 table
fireparq inspect s3://ethereum-mainnet/blocks/date=2026-01-15/part-v1-<...>.parquet

# Show only schema fields, including explicit nullability
fireparq inspect s3://ethereum-mainnet/_fireparq/cursor.parquet --schema-only

# Emit machine-readable schema JSON for a single parquet artifact
fireparq inspect s3://ethereum-mainnet/_fireparq/cursor.parquet --schema-only --json
```

Lookup order: explicit `s3://...` URIs win, existing local paths win over shorthand S3 resolution, and only missing relative paths fall back to `s3://<S3_BUCKET>/<path>`.

**Output includes:**

| Section | Details |
|---|---|
| **File info** | Total rows, row groups, columns, file size, created_by, Parquet version |
| **File metadata** | All key-value pairs stored in the Parquet footer |
| **Schema** | Physical types, logical types (e.g. String, Timestamp), repetition levels, explicit `nullable=` output, nested groups |
| **Row groups** | Per-group row count, compressed/uncompressed size, compression ratio |
| **Column details** | Per-column encoding, compression codec, compressed/uncompressed size, ratio |

### `validate` — Check Block Continuity

Validates a Delta `blocks` table for gaps, duplicates, parent hash mismatches and timestamp reversals, per `date` partition and across the whole table (`--cross-partition` also checks each pair of adjacent partitions). It pins the table's latest version and reads exactly the active data files that snapshot lists, from the log. It never lists a directory, so a file that the maintenance job's OPTIMIZE replaced but VACUUM has not deleted yet is not counted twice, and the log's checkpoint Parquet files are never read as data. If an active file of the pinned version has disappeared (a VACUUM removed it during the run), the run fails; run it again. Only partitions with issues or warnings are printed; clean ones are silently counted. Pass the table, normally `<dataset root>/blocks`: a local path, a shorthand S3 key via `S3_BUCKET`, or an explicit S3 URI.

Timestamp reversals (a block whose `timestamp` is earlier than the previous block that has one) are reported as warnings and do not change the exit code, because some chains (for example Bitcoin) allow non-monotonic block times. Null timestamps are skipped.

```bash
fireparq validate ./output/mainnet/blocks
S3_BUCKET=ethereum-mainnet fireparq validate blocks
fireparq validate s3://ethereum-mainnet/blocks

# Solana: allow skipped slots (normal chain behavior, not data corruption)
fireparq validate s3://solana-mainnet-beta/blocks --allow-gaps
```

The output starts with the pinned version (`Validating blocks in <table> at version <N>`), and the exit code is 1 when a check fails.

Lookup order matches `inspect`: explicit `s3://...` URIs win, existing local paths win over shorthand S3 resolution, and only missing relative paths fall back to `s3://<S3_BUCKET>/<path>`.

| Flag | Default | Description |
|---|---|---|
| `--cross-partition` | `false` | Check continuity between adjacent partitions |
| `--allow-gaps` | `false` | Suppress gap reporting (useful for Solana skipped slots) |

### `recovery` — Ownership and Recovery State

`build` and `recovery recover` hold dataset ownership. After an interrupted
run, use `recovery` to inspect and finish that state. Each
subcommand takes an existing local dataset root or an explicit
`s3://bucket/prefix` URI: the root that `build --output` resolved to, with
`{chain}` already expanded (`./output/mainnet` for `--output './output/{chain}'`).

```bash
# Read ownership and control-record summaries (changes nothing)
fireparq recovery status ./output/mainnet

# Recover protected ingestion and the cursor mirror
fireparq recovery recover ./output/mainnet

# Release one exact S3 owner after provider-confirmed request quiescence
fireparq recovery release s3://my-bucket/v1/mainnet \
  --expected-owner <owner-uuid-from-status> \
  --expected-generation <generation-from-status> \
  --stopped-writer-evidence <reference> \
  --provider-quiescence-evidence <reference>
```

| Subcommand | Behavior |
|---|---|
| `status` | Read-only ownership and control-record summary |
| `recover` | Recover protected ingestion and the mirror under one owner, including the Delta tables of an interrupted transaction, and print `{"recovered_protected_roots": N}`. A retained S3 owner must be released first |
| `release` | S3 only. Requires `--expected-owner` and `--expected-generation` exactly as reported by `status`, plus `--stopped-writer-evidence` and `--provider-quiescence-evidence` (non-secret operator references). It changes only ownership and repairs no data |

Local ownership is an OS directory lock that is released when the owning process
exits; it cannot be forcibly released. Stopping a process or waiting does not
make a delayed remote PUT or DELETE safe, so release an S3 owner only with
provider evidence. Like the other commands, `recovery` reads its S3-compatible
endpoint from `--aws-endpoint-url` / `AWS_ENDPOINT_URL_S3`, and also accepts
`AWS_ENDPOINT_URL` as a fallback. See the
[ownership and recovery runbook](docs/audit/468-stage1-ownership.md).

## Schema Reference

Per-chain schema references are generated from the mapper schemas and list
every table, column, Delta type and nullability, and each chain's mapping from
the mapper's Arrow types onto the Delta types of the files (#643). The chain
sections of this README name the mapper's Arrow types:

- [Schema reference index](docs/schemas/README.md)
- [EVM](docs/schemas/evm.md), [Solana](docs/schemas/solana.md),
  [Bitcoin](docs/schemas/bitcoin.md), [Beacon](docs/schemas/beacon.md),
  [Tron](docs/schemas/tron.md), [Cosmos](docs/schemas/cosmos.md),
  [Antelope](docs/schemas/antelope.md), [NEAR](docs/schemas/near.md)

The README keeps the semantics that a column list cannot show: failed-transaction
rules, join keys, ordering and example queries. `fireparq inspect <file> --schema-only`
prints the schema of an existing file. Select columns by name: `fork_step` and
`stream_ordinal` (non-final streams only) are followed by later columns on
several Solana, Antelope, NEAR and Tron tables.

## Failed Transaction Filtering

EVM includes failed/reverted transactions by default. Solana, Tron, Antelope, Cosmos and NEAR exclude them unless you pass `--include-failed-transactions`. Bitcoin and Beacon have no failed transactions. `--exclude-failed-transactions` drops them on every chain and takes precedence. The flags select whole transactions: every row that belongs to a dropped transaction is dropped with it. NEAR receipts are not transactions and are always written ([below](#near-failed-receipts)).

| Flag | EVM | Solana, Tron, Antelope, Cosmos, NEAR |
|---|---|---|
| *(none)* | included, with their persistent state changes | excluded |
| `--exclude-failed-transactions` | excluded | excluded |
| `--include-failed-transactions` | deprecated, no effect (warns) | included |

Per-chain failure condition, and the columns that label the rows of an included failed transaction:

| Chain | Failed when | Outcome columns |
|---|---|---|
| **EVM** | `status != SUCCEEDED` | `transactions.status`; `state_reverted` and `persisted` on state changes ([below](#evm-persistent-state-changes-of-failed-transactions)) |
| **Solana** | `meta.err` has non-empty bytes | `transactions.success`; `transaction_success` on child tables ([details](#solana-transaction-outcome-context)) |
| **Tron** | `TransactionInfo.result` is `FAILED`, or the receipt result is neither `DEFAULT` nor `SUCCESS` (for example `REVERT` or `OUT_OF_ENERGY`). The Firehose wrapper `result`/`code` are always true/`SUCCESS` and do not report execution | `transaction_success` on `transactions`, `logs`, `internal_transactions`, `contracts`, `internal_call_values` ([below](#tron-failed-smart-contract-calls)) |
| **Antelope** | the trace carries an exception, or its receipt status is not `EXECUTED`, `SOFTFAIL` or `DELAYED` (so `HARDFAIL`, `EXPIRED` and statuses without an execution fail) | `transactions.transaction_success`; `transaction_status` and `transaction_success` on `actions` and `db_ops` ([below](#antelope-deferred-transactions-and-onerror)) |
| **NEAR** | the transaction's own outcome is `Failure` (an inclusion failure) | `receipt_status` on `receipt_actions` and `execution_logs` ([below](#near-failed-receipts)) |
| **Cosmos** | `code != 0` in `TxResult` | `transactions.code` |
| **Bitcoin** | *(not applicable — Bitcoin has no failed txs)* | — |
| **Beacon** | *(not applicable — consensus blocks have no transaction outcomes)* | — |

A failed transaction still pays fees on every chain. The outcome columns describe the parent transaction or receipt; they do not assert that an individual instruction, contract or action ran.

### Tron: failed smart-contract calls

java-tron sets the transaction wrapper `result`/`code` to true/`SUCCESS` for every transaction it includes in a block, so `transactions.result` and `code` never report a failed TVM call. The outcome comes from `TransactionInfo`: `result = FAILED` (set with a runtime error such as `REVERT opcode executed`) or a receipt result other than `DEFAULT` (non-VM contracts) or `SUCCESS`. Unknown enum values count as failures. Before #550 the filter used the wrapper, so reverted calls were written by default.

A failed call still pays its fee, energy and bandwidth: `fee` and the `receipt_*` columns keep them. The VM discards the logs of a reverted call and marks its internal transactions `rejected = true`. `contracts` rows are the submitted contracts, not executed transfers. Every row of `transactions`, `logs`, `internal_transactions`, `contracts` and `internal_call_values` carries the parent's non-null Boolean `transaction_success`. `transactions.contract_address` is the smart contract created or called; it is NULL when `TransactionInfo` has none (plain transfers and other system contracts).

### Antelope: deferred transactions and onerror

Receipt statuses other than `EXECUTED` come from deferred (scheduled) transactions:

| Status | Trace | Selected by default |
|---|---|---|
| `EXECUTED` | executed normally | yes |
| `DELAYED` | scheduled for later execution; no actions ran yet | yes |
| `SOFTFAIL` | for a failed deferred transaction the producer writes two traces: the **failed deferred trace**, which carries the exception and whose database operations the producer already reverted, then the **`onerror` handler trace**, which ran without an exception and whose actions and database operations persisted | the `onerror` trace only |
| `HARDFAIL` | the deferred transaction failed and its `onerror` handler failed or none ran; nothing persisted | no |
| `EXPIRED` | the deferred transaction expired unexecuted | no |

`transactions.transaction_success` and the `transaction_success` of `actions` and `db_ops` say whether the trace's effects persisted. `actions` and `db_ops` also carry the parent receipt status as `transaction_status` (`Dictionary(Int32, Utf8)`, the labels of `transactions.status`). Before #550 only `EXECUTED` traces were selected by default, which dropped successful `onerror` handlers and scheduled transactions.

### NEAR: failed receipts

NEAR fails per receipt, not per transaction. A failed receipt's actions do not take effect, but its `gas_burnt` and `tokens_burnt` persist, and the logs it emitted before failing stay in its outcome. `receipts`, `receipt_actions` and `execution_logs` are written for every executed receipt whatever the failed-transaction flags say. `receipt_actions` and `execution_logs` carry the receipt's own outcome as `receipt_status` (`Dictionary(Int32, Utf8)`: `SuccessValue`, `SuccessReceiptId`, `Failure` or `Unknown`, the values of `receipts.status`); keep `receipt_status <> 'Failure'` for actions that took effect. The transaction filter only drops a transaction whose own outcome is `Failure`. A transaction's outcome is almost always `SuccessReceiptId` and says nothing about the receipts it later spawned; see [final transaction outcome](#near-final-transaction-outcome).

When failed transactions are included, chain-specific fields like Solana's `err` bytes and `success` flag reflect the actual transaction status.

### Cosmos: unknown results, ordered events and transaction metadata

Cosmos transactions with missing `TxResult` have null `code`, gas and result text
fields. They remain included as unknown; `WHERE code = 0` selects only confirmed
source success. `decode_success` reports decoding of the supported SDK envelope
and metadata fields, not valid signatures or successful execution. Exact Binary
`raw_tx` remains available, and `blocks.tx_decode_failures` counts malformed
transactions across the full source block, including filtered failed rows.

Events retain `event_index` and nullable UInt32 `attribute_index`. An event without
attributes has one row with null attribute index/key/value. Empty source strings
are present values. Block events have null `tx_index` and `tx_hash`; transaction
events and messages use the same UInt32 source index as `transactions.index`.

Memo, timeout height, fee gas limit/payer/granter, ordered fee coins, signer infos
and signatures live on `transactions`. Coin amounts are exact strings. Missing
body/auth/fee is null; a present empty value or list remains empty. Public keys
are optional, and signer/signature arrays keep their independent source order
and lengths. Signer `mode_info` retains the opaque embedded protobuf payload
(concatenated in source order if repeated); it is not semantically validated.
All raw payloads are Binary regardless of identifier encoding.

```sql
SELECT m.block_num, m.tx_index, m.message_index, m.type_url,
       t.memo, t.fee_amount, t.signer_infos
FROM read_parquet('output/cosmos/messages/**/*.parquet') m
JOIN read_parquet('output/cosmos/transactions/**/*.parquet') t
  ON m.block_num = t.block_num AND m.block_id = t.block_id
 AND m.tx_index = t."index"
WHERE t.code = 0;
```

These Cosmos schema changes require rebuilding into a new root or explicit
conversion into a separate dataset. Old fabricated zeros and omitted rows need
source replay to recover their meaning. See the [migration and live RPC comparison](docs/audit/510-cosmos-values.md).

### EVM: persistent state changes of failed transactions

A failed EVM transaction still changes chain state: the sender pays for gas, the fee recipients are paid, and the sender's nonce goes up. Everything else it did is rolled back. So `balance_changes` and `nonce_changes` only reconcile with on-chain balances and nonces when failed transactions are included, and only if their rolled-back changes are left out.

For a failed or reverted transaction, `fireparq` writes:

| Table | Rows written |
|---|---|
| `transactions` | the transaction, with `status` `FAILED` or `REVERTED` |
| `logs` | none (failed transactions have no receipt logs) |
| `calls` | every call (`status_failed` / `state_reverted` tell you what happened) |
| `gas_changes` | every gas change (the gas was consumed and paid for) |
| `balance_changes` | root-call changes with reason `GAS_BUY`, `GAS_REFUND`, `REWARD_TRANSACTION_FEE`, or `INCREASE_MINT` (OP Stack deposits keep their mint when they fail) |
| `nonce_changes` | the sender's nonce increment (the root call's earliest nonce change), plus one per accepted EIP-7702 authorization |
| `code_changes` | at most one per accepted EIP-7702 authorization (the delegation) |
| `storage_changes`, `account_creations` | none |

This follows the rule documented on `TransactionTrace.status` in `proto/ethereum.proto`. Rolled-back transfers and storage writes of failed transactions are not written. Successful transactions keep every state change, including those of calls that were reverted inside them. The `persisted` column tells them apart (see below).

Resuming protected EVM authority that records failed transactions as excluded keeps excluding them, so one output does not mix both modes. Legacy cursor-only datasets need a new empty output root. `fireparq` logs a warning. Pass `--exclude-failed-transactions` to keep that and silence the warning. To switch semantics, rebuild into a fresh output root with an absent mirror; `--cursor-override` cannot change protected output.

### EVM: which call recorded a change, and whether it persisted

The transaction-scoped change tables (`balance_changes`, `nonce_changes`, `code_changes`, `storage_changes`, `account_creations`, `gas_changes`) carry the transaction and call that recorded each change:

| Column | Type | Meaning |
|---|---|---|
| `tx_index` | `UInt32` | The transaction's index in the block. Joins `transactions.index`. |
| `call_index` | `UInt32` | The recording call's Firehose index (starts at 1). Joins `calls.call_index` with `tx_hash`. |
| `state_reverted` | `Boolean` | The recording call's `state_reverted` flag, the same value as in `calls`. |
| `persisted` | `Boolean` | Whether the change is part of chain state after the transaction. Not on `gas_changes`: gas is consumed even in reverted calls. |

`persisted` is `NOT state_reverted` for successful transactions. For failed or reverted transactions it is always `true`: only their persistent changes are written, and those come from the root call, whose `state_reverted` is `true`. To rebuild state from the change tables, filter on `persisted`:

```sql
-- Balance of each address at the end of the range
SELECT address, new_value AS balance
FROM read_parquet('output/mainnet/balance_changes/**/*.parquet')
WHERE persisted
QUALIFY row_number() OVER (PARTITION BY address ORDER BY block_number DESC, ordinal DESC) = 1;
```

Order persisted changes by `(block_number, ordinal)`; ordinals are unique within a block. Ordinals of changes in reverted calls may be `0`.

The `system_*` change tables have a nullable `call_index`: the index of the system call that recorded the change, or `NULL` for block-level changes such as beacon-chain withdrawals. System call indexes are not unique within a block: the system calls that run after the transactions (EIP-7002 and EIP-7251 requests) restart at 1. Join a change to its system call on the ordinal range as well:

```sql
SELECT c.*, s.address AS system_contract
FROM read_parquet('output/mainnet/system_storage_changes/**/*.parquet') c
JOIN read_parquet('output/mainnet/system_calls/**/*.parquet') s
  ON s.block_number = c.block_number AND s.call_index = c.call_index
 AND c.ordinal BETWEEN s.begin_ordinal AND s.end_ordinal;
```

The `logs` table holds receipt logs only, so logs emitted by reverted calls are never in it.

### EVM: receipt log indices and RPC joins

The `logs` table maps `TransactionReceipt.logs`. It contains receipt logs, so
logs from reverted calls are absent. The two index columns have different scopes:

| Column | Source | Meaning |
|---|---|---|
| `log_index` | Firehose `Log.index` | Transaction-relative Firehose log index. Different transactions can have the same value. The protobuf only guarantees this field at `EXTENDED` detail. |
| `block_index` | Firehose `Log.blockIndex` | Block-relative receipt log index, corresponding to JSON-RPC `logIndex` after converting the RPC hexadecimal quantity to an integer. |
| `tx_index` | Firehose transaction index | Position of the transaction in its block; corresponds to RPC `transactionIndex`. |

For RPC joins, use the same chain, `block_id`/RPC `blockHash`, and
`block_index`/RPC `logIndex`, with matching hash encoding. Including `tx_hash`
provides an additional transaction check. `log_index` alone is not an RPC join
key. Block hashes distinguish forks at the same block number; reversible output
also requires applying NEW/UNDO events to select the canonical logs.

```sql
-- Export RPC-compatible index names from finalized EVM output.
SELECT block_id, tx_hash,
       block_index AS rpc_log_index,
       tx_index AS rpc_transaction_index,
       log_index AS firehose_transaction_log_index
FROM read_parquet('output/mainnet/logs/**/*.parquet');
```

`fireparq` preserves the indices supplied by Firehose and does not renumber logs
after filtering. Do not infer a transaction-local position from a default zero
when the upstream source omits `Log.index` at a lower detail level.

### EVM: tables that can be absent

`gas_changes` and `account_creations` are extended tables populated only from
upstream call arrays. A sampled range can contain no rows even when transactions
and calls are present. The September 2026 audit's Ethereum mainnet block-version-5
samples contained no rows for either table; this is a bounded observation, not
a guarantee about every network, provider or block version.

- `account_creations` is deprecated upstream: the checked-in Ethereum protobuf
  says account-creation records are unsupported from block version 4. Do not use
  an absent table to conclude that no contracts or accounts were created.
- `gas_changes` contains explicit upstream gas-change records. Missing rows do
  not mean zero gas usage; transaction/receipt gas fields provide separate data.
- `--without-extended` disables both tables regardless of source contents.

The ingestion writer skips zero-row batches, so an empty table usually has no
Parquet file or directory. A DuckDB glob for such a table reports no matching
files; it does not automatically produce an empty relation. Enumerate available
files before querying optional tables. No placeholder files are synthesized.

### EVM: withdrawals, access lists and EIP-7702 authorizations

Three tables hold block and transaction data that is not a column of `blocks` or `transactions`. They are written at both detail levels, including with `--without-extended`. Access-list and authorization rows follow their transaction: they are written for failed transactions too, and dropped with `--exclude-failed-transactions`. Columns and types are in the [EVM schema reference](docs/schemas/evm.md).

| Table | One row per | Notes |
|---|---|---|
| `withdrawals` | beacon-chain withdrawal in the block (Shanghai and later) | `index` is the global withdrawal index; `amount_gwei` is in gwei, not wei |
| `access_lists` | entry of a transaction's access list (EIP-2930) | `access_index` is the position in the list; `storage_keys` may be empty |
| `set_code_authorizations` | authorization of a `SET_CODE` transaction (EIP-7702) | `authorization_index` is the position in the list; `chain_id` is a decimal string (`0` allows any chain); `address` is the delegation target and `authority` the recovered signer |

- A withdrawal also appears in `system_balance_changes` with reason `WITHDRAWAL`, in wei and without the validator. `sum(amount_gwei) * 1e9` per block and address equals the summed balance delta there.
- `authority` is `NULL` when it can't be recovered from the signature, and those authorizations are `discarded`. `address` is `NULL` on the few testnet blocks where Firehose did not record it.
- `discarded = true` means the chain skipped the authorization as invalid. Accepted authorizations take effect even when the transaction fails (see failed transactions above).

### EVM: header, signature, blob and ordinal columns

`blocks`, `transactions`, `calls`, `system_calls` and `logs` carry Firehose header, signature, blob and ordinal fields as they are, with bytes in the output encoding and big integers as decimal strings like the other value columns. The [EVM schema reference](docs/schemas/evm.md) lists each column, type and nullability.

- Fields introduced by a fork are `NULL` in blocks and transactions from before it: `withdrawals_root` (Shanghai), `blob_gas_used` / `excess_blob_gas` and `parent_beacon_root` (Cancun), and `requests_hash` (Prague).
- The blob columns of `transactions` are `NULL` for non-blob transactions, and `blob_hashes` is an empty list. `transactions.logs_bloom` comes from the receipt and is `NULL` without one.
- `failure_reason` on `calls` and `system_calls` is `NULL` when the call did not fail; `address_delegates_to` is the EIP-7702 delegation target of the called account.
- `begin_ordinal` / `end_ordinal` on transactions and calls, and `ordinal` on logs, give the execution order in the block. Ordinals are unique within a block, so `(block_number, ordinal)` orders every log, call and state change of a block. They are not reliable for anything inside a reverted call.

## Tron Contracts, Receipts and Internal Values

`contracts` retains every source contract with `transaction_index`, `tx_hash`,
`contract_index`, enum label/number, permission ID and raw Binary Any payload.
TransferContract, TransferAssetContract and TriggerSmartContract expose typed
owner/recipient/amount or target/data/call-value fields. Unsupported types keep
their raw payload with null decoded fields. `transactions.contract_type` remains
the first-contract projection and is null when the contract list is empty.

`transactions` includes nullable `receipt_*` energy/net fees, usage and result,
receipt `contract_address` (NULL when absent), and Binary `res_message`. Missing receipts are null;
present zero/empty values remain values. `internal_call_values` retains each
ordered source `(call_value, token_id)` pair, including repeated or empty token
IDs, joined by block identity, transaction index/hash and `internal_index`.

`transaction_index` and `logs.block_log_index` count original source positions,
including transactions omitted by failed filtering. Existing `logs.log_index`
remains per transaction. Failed calls and their `transaction_success` column are
described under [failed transaction filtering](#tron-failed-smart-contract-calls).

```sql
SELECT t.block_num, t.txid, c.contract_index, c.contract_type,
       c.owner_address, c.to_address, c.amount, c.contract_address,
       hex(c.data) AS call_data_hex, c.call_value, t.receipt_energy_fee
FROM read_parquet('output/**/transactions/*.parquet') t
JOIN read_parquet('output/**/contracts/*.parquet') c
  ON t.block_num = c.block_num AND t.block_id = c.block_id
 AND t.transaction_index = c.transaction_index AND t.txid = c.tx_hash;
```

These fields require a new output root/rebuild or explicit schema migration;
old files cannot recover dropped source fields. See the [mapping contract,
validation and RPC-backed qualification limits](docs/audit/509-tron-contract-fields.md).

## Solana Vote Filtering

The optional `vote_transactions` table contains conservatively recognized simple
votes. `--without-votes` omits those transactions. A candidate must be a legacy
transaction with one or two signatures, a consistent message header, and exactly
one instruction whose program index resolves to the Vote program. Its complete,
canonical payload must decode as `Vote`, `VoteSwitch`, `UpdateVoteState`,
`UpdateVoteStateSwitch`, `CompactUpdateVoteState`,
`CompactUpdateVoteStateSwitch`, `TowerSync`, or `TowerSyncSwitch`, with a nonempty
vote history. Payloads larger than the current 1,232-byte packet ceiling stay in
ordinary output.

Administrative calls such as Withdraw, Authorize and InitializeAccount,
transactions with multiple instructions, versioned transactions, unused Vote
account keys, and unknown or malformed payloads keep their ordinary transaction,
message, instruction, balance, lookup and reward rows, subject to the usual
failed-transaction filter. This classification checks structure and recognized
payloads; it does not verify signatures cryptographically or prove execution
validity. Recognized votes retain the existing compact output policy: their
transaction row goes to `vote_transactions` and their detail rows are omitted.

Earlier versions classified any transaction mentioning the Vote account as a
vote. Their output can therefore omit administrative activity. This fix changes
row counts without changing schemas. Rebuild affected ranges into a separate
output root to recover missing rows; appending a corrected replay to old results
does not remove existing rows or guarantee deduplication.

## Solana Transaction Outcome Context

Solana `messages`, `instructions`, `token_balances`, and `account_lookups` append
a non-null Boolean `transaction_success`. It describes the parent transaction:
`false` means its source metadata contains nonempty error bytes; absent or empty
error bytes mean `true`. Transactions without metadata remain omitted. The
existing failed-transaction and vote filters still select exactly the same rows.

`rewards.transaction_success` is nullable: transaction rewards carry their
parent's outcome; block rewards have `NULL` because they have no parent
transaction. This does not change reward indices or amounts.

This is outcome context, not an instruction result or a `reverted` flag.
Submitted top-level instructions can include instructions that never executed;
recorded inner calls do not establish each call's success. Token balances remain
literal pre/post snapshots, and transaction fees and lamport balances remain
unchanged. A failed transaction can still pay fees or advance a durable nonce;
do not discard its balance observations merely because `transaction_success`
is false. This addition does not provide a canonical view of reversible events.

Start a fresh output root and replay when adopting these schemas. Old files lack
the context; a missing column is not `false`. Protected output bindings refuse
mixed old/new schemas. An explicit conversion must write
a separate dataset and preserve unknown historical context. See the
[source evidence, migration and offline comparison](docs/audit/550-solana-execution-context.md).

## Solana Payloads and Account Indices

Opaque `instructions.data`, `transactions.err` / `return_data` and
`vote_transactions.err` / `return_data` are native Binary, independently of the
identifier encoding. Signatures, hashes, keys and `return_data_program_id` retain
the selected identifier format (base58 by default). Missing return data is null;
a present empty payload stays empty. Absent or empty errors remain null.

`instructions.accounts`, `account_lookups.writable_indexes` and
`account_lookups.readonly_indexes` are non-null lists of non-null UInt8. They retain
source order, duplicates and empty lists. These are indices, not resolved keys.

```sql
-- Inspect payload bytes without requiring base58 conversion.
SELECT block_num, block_id, transaction_index, instruction_index,
       is_inner, inner_instruction_index, hex(data) AS data_hex, accounts
FROM read_parquet('output/**/instructions/*.parquet');

-- Expand instruction account indices while preserving their source positions.
SELECT block_num, block_id, transaction_index, instruction_index,
       is_inner, inner_instruction_index,
       generate_subscripts(accounts, 1) - 1 AS account_position,
       unnest(accounts) AS account_index
FROM read_parquet('output/**/instructions/*.parquet');
```

This changes older output schemas, including Binary-mode index columns. Start a
new output root and rebuild, or explicitly convert into a separate dataset; do
not append these types into an old dataset. Verification roots change with the
schema. See the [migration and measured validation](docs/audit/503-solana-binary-payloads.md).

## Solana Instruction Order

The `instructions` table preserves the upstream order inside each top-level
instruction's inner set with two nullable `UInt32` columns:

| Column | Top-level instruction | Inner instruction |
|---|---|---|
| `parent_instruction_index` | `NULL` | Zero-based index of the top-level instruction that owns the inner set |
| `inner_instruction_index` | `NULL` | Zero-based position within that parent's inner set |

The existing `instruction_index` still numbers all top-level instructions first,
then all inner instructions in upstream group order. It is a row index, not call
order. The existing `inner_index` still contains the top-level parent's index;
it has the same value as `parent_instruction_index`.

For one transaction in one block event, order its instructions as follows:

```sql
SELECT *
FROM read_parquet('instructions/*.parquet')
WHERE block_id = '<block id>' AND transaction_index = 0
ORDER BY coalesce(parent_instruction_index, instruction_index),
         is_inner,
         inner_instruction_index;
```

This puts each top-level instruction before its recorded inner calls and keeps
the upstream order of those calls, including nested calls. `stack_height`, when
present, provides depth; `parent_instruction_index` identifies the top-level
owner, not the immediate caller of a nested instruction. For failed transactions
included with `--include-failed-transactions`, listed top-level instructions may
include instructions that did not execute. Apply fork semantics first when
querying reversible output; the ordering fields do not identify event delivery
order or remove replay duplicates.

Older files lack both new columns. Readers that union schemas by name can read
them as null, but nulls alone cannot distinguish old inner rows from top-level
rows. Check `is_inner` and rebuild old ranges into a separate output root before
depending on these fields.

## Solana Reward Indices

`rewards.reward_index` is a zero-based index within one block envelope. Rewards
from included transactions are numbered in transaction order and in each
transaction's upstream reward order, followed by block rewards in their upstream
order. The counter restarts for every block; flush size and restarts do not change
it. `source` identifies `transaction` or `block`, and `transaction_index` is null
for block rewards.

For finalized output, use `(block_id, reward_index)` as the reward key. Include
the chain/network when combining datasets. With reversible output, NEW and UNDO
rows are separate events that can share this key; apply fork semantics before
using it as a unique key. Changing transaction filters can change indices.

Older output may contain colliding indices within a block and indices offset by
earlier buffered blocks. Rebuild affected ranges into a separate output root
before relying on the corrected key; appending new output does not repair old rows.

## Beacon Chain Tables

Each Beacon table gets rows from the fork that introduced its data. Blocks from earlier forks add no rows to it, so a range from before that fork writes no file for the table. The [Beacon schema reference](docs/schemas/beacon.md) lists every column; the table below gives each table's source and first fork.

| Table | Rows from | Source |
|---|---|---|
| `blocks` | Phase0 | Block header, plus the body's `graffiti` |
| `attestations` | Phase0 | `body.attestations`; `committee_bits` from Electra |
| `deposits` | Phase0 | Deposits from the Eth1 bridge (`body.deposits`) |
| `proposer_slashings`, `attester_slashings`, `voluntary_exits` | Phase0 | `body.proposer_slashings`, `body.attester_slashings`, `body.voluntary_exits` |
| `execution_payload` | Bellatrix | `body.execution_payload` |
| `withdrawals` | Capella | `execution_payload.withdrawals` (up to 16 per block) |
| `bls_to_execution_changes` | Deneb | `body.bls_to_execution_changes`. The Firehose Capella body has no such field, so changes included in Capella blocks are not available. |
| `blob_sidecars` | Deneb | `body.embedded_blobs` |
| `deposit_requests` | Electra | `execution_requests.deposits` (EIP-6110) |
| `withdrawal_requests` | Electra | `execution_requests.withdrawals` (EIP-7002) |
| `consolidation_requests` | Electra | `execution_requests.consolidations` (EIP-7251) |

`execution_payload.base_fee_per_gas` is an exact unsigned decimal string in wei
per gas. It is independent of the selected byte encoding; use a checked numeric cast for
arithmetic (the full uint256 range needs up to 78 decimal digits).
`blob_sidecars.blob` is always Binary; hashes, roots, commitments, and proofs
retain the selected byte encoding. `blocks.spec` uses generated enum names in
an Arrow string dictionary, with `UNKNOWN` for unrecognized numeric values.
Missing nested messages produce null descendants, while present zero values
and empty byte/list values remain present. Absent bodies or execution payloads
produce no child rows.

These schema changes require a fresh output root or a verified conversion of
existing files. Old fee bytes have different byte orders by payload type:
Bellatrix/Capella are fixed little-endian, Deneb and later are big-endian.
Historical fake zeros for missing messages cannot be repaired without source
replay. See [the producer evidence and migration procedure](docs/audit/505-beacon-values.md).

Amounts (`amount`) are in Gwei. `block_slot` joins `blocks.slot`. `withdrawals.withdrawal_index` is the chain-wide withdrawal index; `change_index` and `request_index` are positions within the block.

- **Attestations after Electra.** EIP-7549 moved the committee out of the signed data: `committee_index` is always `0`, and `committee_bits` (8 bytes, a 64-bit bitvector) says which committees an aggregate covers. Bit `i` is bit `i % 8` of byte `i / 8`. `aggregation_bits` then spans those committees in index order. `committee_bits` is null before Electra.
- **Deposits after Electra.** New deposits reach the chain as `deposit_requests`, whose `deposit_index` is the deposit contract index (the `index` of EIP-6110). `deposits` only holds deposits from the Eth1 bridge, which stop once its backlog is processed. `deposits.deposit_index` is the position within the block.
- **Withdrawal requests.** `amount` `0` requests a full exit; any other value is a partial withdrawal.
- **Consolidation requests.** A request whose `source_pubkey` equals its `target_pubkey` switches the validator to compounding withdrawal credentials.
- **Attester slashings.** `attestation_1_attesting_indices` and `attestation_2_attesting_indices` (`List<UInt64>`) are the two conflicting attestations' validators. The slashed validators are in both lists:

  ```sql
  SELECT block_slot, list_intersect(attestation_1_attesting_indices, attestation_2_attesting_indices) AS slashed
  FROM read_parquet('output/mainnet-cl/attester_slashings/**/*.parquet');
  ```

- **Graffiti.** `blocks.graffiti` is the proposer's raw 32 bytes, usually zero-padded text. It is null only for a block without a body.

## Prometheus Metrics

Enable the metrics server with `--metrics-port <PORT>` (env: `METRICS_PORT`). A lightweight HTTP server binds to `0.0.0.0:<PORT>` serving three endpoints:

| Endpoint | Description |
|---|---|
| `/metrics` | Prometheus text exposition format |
| `/health` | `200 OK` while the pipeline is running, reconnecting or committing final output; `503` after it stops |
| `/ready` | `200 OK` after a valid stream message while connected and within the freshness threshold; otherwise `503` |

`--metrics-stale-after-secs` / `METRICS_STALE_AFTER_SECS` sets the readiness
threshold (default 120 seconds; must be positive). It uses monotonic time since
the last valid message, so historical backfills can be ready even when block
timestamps are old. Disconnects and stream completion make readiness false.
Liveness stays true through final file publication and cursor persistence.
Readiness establishes recent stream activity; it does not prove forward block
progress, chain-head agreement or crash/replay safety.

### Available Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `firehose_parquet_blocks_processed_total` | Counter | — | Total blocks processed since start |
| `firehose_parquet_bytes_read_total` | Counter | — | Total protobuf bytes consumed from stream |
| `firehose_parquet_rows_written_total` | Counter | `table` | Rows written per table |
| `firehose_parquet_current_block_number` | Gauge | — | Most recently processed block number |
| `firehose_parquet_min_block_number` | Gauge | — | Minimum block number seen |
| `firehose_parquet_max_block_number` | Gauge | — | Maximum block number seen |
| `firehose_parquet_elapsed_seconds` | Gauge | — | Monotonic seconds since metrics initialization; refreshed on scrape |
| `firehose_parquet_last_block_timestamp_seconds` | Gauge | — | Last valid stream message's block timestamp; `NaN` when absent |
| `firehose_parquet_block_time_lag_seconds` | Gauge | — | Wall-clock age of that timestamp, clamped at zero; `NaN` when absent; refreshed on scrape |
| `firehose_parquet_last_message_age_seconds` | Gauge | — | Monotonic seconds since the last valid message; `NaN` before one; refreshed on scrape |
| `firehose_parquet_files_written_total` | Counter | `table` | Parquet files written |
| `firehose_parquet_file_bytes_total` | Counter | `table` | Total compressed bytes written |
| `firehose_parquet_flushes_total` | Counter | `trigger`, `pace` | Committed flushes by trigger (`bytes`, `memory`, `rows`, `blocks`, `interval`, `partition_boundary`, `stream_end`) and by the stream pace at the flush (`catching_up` or `caught_up`) |
| `firehose_parquet_catching_up` | Gauge | — | 1 while the stream replays history faster than real time, which suspends `--flush-interval-secs`; 0 at the head or when the pace is unknown ([details](#flush-interval-and-catch-up)) |
| `firehose_parquet_buffer_estimated_bytes` | Gauge | — | Writer-owned buffers, estimated compressed bytes |
| `firehose_parquet_buffer_rows` | Gauge | `table` | Writer-owned rows, including failed/unattempted tables |
| `firehose_parquet_mapper_buffer_rows` | Gauge | — | Mapper-owned rows summed across tables |
| `firehose_parquet_mapper_largest_table_estimated_bytes` | Gauge | — | Largest mapper table estimate, used to predict compressed file size |
| `firehose_parquet_mapper_buffer_estimated_bytes` | Gauge | — | Summed logical mapper estimates used by the memory trigger; not RSS |
| `firehose_parquet_bootstrap_buffered_blocks` | Gauge | — | Raw blocks awaiting the initial timestamp anchor |
| `firehose_parquet_bootstrap_buffered_bytes` | Gauge | — | Raw protobuf bytes awaiting that anchor |
| `firehose_parquet_cursor_saves_total` | Counter | — | Cursor persistence count |
| `firehose_parquet_cursor_save_failures_total` | Counter | — | Failed cursor mirror saves, once per failed attempt (including local retries, S3 reads/validation/owner checks before the PUT, and ambiguous or refused S3 publication) |
| `firehose_parquet_cursor_last_success_timestamp_seconds` | Gauge | — | Unix time of the last successful cursor save in this process; 0 before the first save |
| `firehose_parquet_cursor_last_block_num` | Gauge | — | Block number from the loaded cursor, then the last successful save; 0 when neither exists |
| `firehose_parquet_startup_list_requests` | Gauge | — | LIST requests (S3 pages of up to 1,000 keys, or local directory reads) made while opening the dataset; a resume lists no data objects ([startup cost](#startup-cost)) |
| `firehose_parquet_startup_listing_seconds` | Gauge | — | Seconds those startup listings took |
| `firehose_parquet_delta_log_tail_commits` | Gauge | `table` | Commits after the last checkpoint in the table's Delta log, which readers and the next start replay; it grows until the maintenance job checkpoints the table, so alert when it keeps growing (#643) |
| `firehose_parquet_delta_commit_seconds` | Histogram | `table` | Duration of each Delta commit of the table, from its request to a durable version |
| `firehose_parquet_delta_commit_retries_total` | Counter | `table` | Lost conditional puts the table's Delta commits retried at a later version (another writer, usually the maintenance job, committed first) |
| `firehose_parquet_errors_total` | Counter | `kind` | Errors by category |
| `firehose_parquet_grpc_reconnects_total` | Counter | — | gRPC retries scheduled, once per reconnect path |
| `firehose_parquet_blocks_skipped_below_start_total` | Counter | — | Blocks received below the effective start block and skipped |
| `firehose_parquet_info` | Info | *(pipeline config)* | Pipeline metadata (chain, endpoint, version) |

Use `rate(firehose_parquet_blocks_processed_total[5m])` and
`rate(firehose_parquet_bytes_read_total[5m])` for throughput. The old rate gauges
were cumulative averages and have been removed, along with the always-zero
Solana `backfill_*` gauges. Block-time lag measures age against the local clock;
it is not a measured remote chain-head lag. Buffer estimates are not process RSS.

Dashboard migration: counters previously registered with `_total` emitted
`_total_total`, because the Prometheus library adds the suffix. These now emit
exactly one `_total`; update queries that used the doubled names.
`cursor_save_failures_total` already had the correct name and is unchanged.
`files_written_total` now has only the `table` label; remove per-partition filters
and groupings. Existing series remain in your monitoring system until its normal
retention expires.

```bash
# Enable metrics on port 9090
fireparq build \
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
| `firehose-parquet.version` | `1.0.0` |
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
| `firehose-parquet.synthetic_timestamps` | `true` |
| `firehose-parquet.synthetic_timestamp_policy` | `last_known_partition_routing` |
| `firehose-parquet.with_votes` | `true` |

Table files carry the pipeline, chain, encoding and compression keys.
`partition` (always `date`) is written to `_fireparq/cursor.parquet`, which also
records `extended`, `final_blocks_only` and `include_failed_transactions`. `with_votes` appears on Solana output, and the
`synthetic_*` keys only when synthetic routing is used.

`firehose-parquet.bytes_encoding` and `firehose-parquet.block_id_encoding` describe the emitted output contract, not just the upstream Firehose endpoint. See [Output Encoding by Block Type](#output-encoding-by-block-type) for the operator-facing defaults by supported chain/profile.

For Solana date partitions, the `firehose-parquet.synthetic_*` metadata
keys mark routing as using a synthetic last-known timestamp anchor while
canonical `timestamp` / `date` remain chain-sourced and nullable when
`block_time` is missing.

Endpoint `block_id_encoding` remains a fallback only when the chain does not resolve to a known block-type/profile contract.

### Reading Metadata

```python
import pyarrow.parquet as pq

meta = pq.read_metadata("output/blocks/date=2026-01-15/part-000001.parquet")
for i in range(meta.metadata.count()):
    key = meta.metadata.keys()[i]
    if key.startswith("firehose-parquet."):
        print(f"{key} = {meta.metadata.values()[i]}")
```

```sql
-- DuckDB
SELECT key, value
FROM parquet_kv_metadata('output/blocks/date=2026-01-15/part-000001.parquet')
WHERE key LIKE 'firehose-parquet.%';
```

## Parquet Lookup Metadata

Ingestion writes bounded Bloom filters for selected scalar
hash, signature and account/address columns. Readers that support these filters
can skip row groups for equality lookups; positive matches still require row
filtering. Filters do not answer `IS NULL` predicates. Row groups contain at most
65,536 rows, with at most eight filters per group. This changes physical layout,
not table schemas or row order. Dictionary encoding retains its existing policy.
The retained-data benchmark measured 0.74–1.92% larger files and faster missing-key
lookups; readers without Bloom pruning may only see the storage overhead.

Complete ingestion parts declare ascending `block_num` only when every observed
height proves that order. Ingestion neither sorts nor reconstructs
reversible-chain history.

Use `--compression zstd:6` to select an explicit Zstandard level; `zstd` and
`zstd:3` retain level 3. Zero is rejected as ambiguous. Explicit non-default
levels participate in protected transaction identity; recover any pending work
with a supporting version before downgrading. See the
[lookup properties and measurements](docs/audit/519-parquet-lookup-properties.md).

## Output Directory Layout

`--output` is the dataset root, used exactly as given: `build` writes one
network's dataset directly into it. To name a directory after the network, put
the `{chain}` placeholder anywhere in the path or S3 key prefix:

| `--output` (mainnet endpoint) | Dataset root |
|---|---|
| `./output` | `./output` |
| `./output/{chain}` | `./output/mainnet` |
| `s3://ethereum-mainnet` or `s3://ethereum-mainnet/` | `s3://ethereum-mainnet` (the bucket root) |
| `s3://datasets/{chain}` | `s3://datasets/mainnet` |
| `s3://datasets/v1/{chain}/raw` | `s3://datasets/v1/mainnet/raw` |
| `./output/{{chain}}` | `./output/{chain}` (escaped braces) |

- `{chain}` expands to the endpoint's canonical `chain_name` from EndpointInfo.
  EndpointInfo with a nonempty chain name is required either way: the name is
  also recorded in the `firehose-parquet.chain_name` file metadata and in the
  protected dataset identity. It must be one path segment of ASCII letters,
  digits, `-`, `_` and `.`; every built-in network qualifies.
- `{chain}` is the only variable. `{{` and `}}` are literal braces. An unknown
  variable, an unterminated `{` or an unmatched
  `}` is an error before the endpoint is contacted.
- The S3 bucket name is literal, so `s3://{chain}/...` is refused. Credentials,
  the `S3_BUCKET` check and the bucket-wide owner are settled per bucket before
  the endpoint is contacted. Put the placeholder in the key prefix.
- An S3 root drops trailing `/` separators, so `s3://bucket/` and `s3://bucket`
  are the same bucket root. A local root is used as given.
- The resolved root is bound when the dataset is created. A later run whose
  `--output` resolves inside or above the dataset is refused before any Blocks
  request (`selected ingestion output overlaps another protected root`). For
  example, a dataset created at `s3://b` cannot be resumed as `s3://b/{chain}`,
  and one created at `s3://b/{chain}` cannot be resumed as `s3://b`. Any other
  root is a different dataset and must start empty.
- v0.7.x appended `<chain_name>` to `--output`. To keep that layout, add
  `/{chain}`: `--output ./output` becomes `--output './output/{chain}'`.

```
<root>/
├── .fireparq-ingest/          # authoritative checkpoint and transaction journal (do not edit)
├── _fireparq/                 # fireparq's artifacts, never table data
│   └── cursor.parquet         # optional mirror of the checkpoint (absent with --cursor none)
├── blocks/                    # one Delta table
│   ├── _delta_log/            # its log: the table's only file index
│   │   ├── 00000000000000000000.json
│   │   └── ...
│   ├── date=2026-02-25/
│   │   ├── part-v1-<stream>-<first>-<last>-<txn>-<index>.parquet
│   │   └── part-v1-<stream>-<first>-<last>-<txn>-<index>.parquet
│   └── date=2026-02-26/
│       └── part-v1-<stream>-<first>-<last>-<txn>-<index>.parquet
├── transactions/
│   └── ...
└── logs/
    └── ...
```

`build` names each part deterministically from its stream, the first and last
accepted event of its transaction, the transaction ID and the part index.

The dataset root holds only the table directories, `_fireparq/` and
dot-prefixed control state (`.fireparq-ingest/`, and at a bucket root the
`.fireparq-owner-v1.json` record and `.fireparq-owner-probes-v1/`). Spark,
Trino, Hive and Delta skip paths that start with `_` or `.`, so a table
location or a dataset-wide read never picks up fireparq's files. Read each
table through its Delta log, as below.
Releases before v1.0.0 wrote `cursor.parquet` at the dataset root; a dataset
whose mirror was bound at that old default keeps `--cursor cursor.parquet`.

Every table is partitioned by UTC day: `build` writes its data files to
`<table>/date=YYYY-MM-DD/part-*.parquet`, and there is no other layout. `date`
is the table's Delta partition column: the log records it for every file, and
the data files do not store it (#643). It comes from the same whole-second
block time as each row's `timestamp`, and `build` refuses to write a row whose
time is in another day. (Earlier releases wrote `year=YYYY/month=MM/` and a
`date=DD` or `day=DD` day of the month. v1.0.0 writes only
`date=YYYY-MM-DD`.)

### Reading the tables

Read every table through its Delta log, never by globbing its files or
listing its directories. The log is the table's only file index: a glob or a
listing also finds the files that the [maintenance job](#delta-maintenance)'s
OPTIMIZE replaced but VACUUM has not deleted yet (their rows twice), files of a
transaction that is not committed yet, and the log's own checkpoint Parquet
files. DuckDB (its
`delta` extension) and Polars read the `date` partition column from the log
and prune by it.

```sql
-- DuckDB 1.5.5, from a local dataset root
INSTALL delta; LOAD delta;
-- Rows, blocks and days of one table
SELECT count(*) AS rows, min(block_num) AS first_block, max(block_num) AS last_block,
       min(date) AS first_day, max(date) AS last_day
FROM delta_scan('output/mainnet/blocks');
-- Its schema, and the latest rows
DESCRIBE SELECT * FROM delta_scan('output/mainnet/blocks');
SELECT * FROM delta_scan('output/mainnet/blocks') ORDER BY block_num DESC LIMIT 20;
-- One day: pruned by the partition, then by the block_num statistics
SELECT count(*) FROM delta_scan('output/mainnet/blocks') WHERE date = DATE '2026-02-25';

-- From a bucket with anonymous public read: an empty key and secret send
-- unsigned requests. Set the endpoint and region of the S3-compatible service.
CREATE SECRET lake (TYPE s3, KEY_ID '', SECRET '', REGION 'us-east-1',
                    ENDPOINT 'storage.example.com', URL_STYLE 'path');
SELECT count(*) FROM delta_scan('s3://ethereum-mainnet/blocks')
WHERE date = DATE '2026-02-25';
```

```python
# Polars 1.44 with deltalake 1.6 (pip install polars deltalake), from a local dataset root
import datetime
import polars as pl

blocks = pl.scan_delta("output/mainnet/blocks")
day = blocks.filter(pl.col("date") == datetime.date(2026, 2, 25)).collect()

# From a bucket with anonymous public read (no credentials, unsigned
# requests); set the endpoint and region of the S3-compatible service
remote = pl.scan_delta(
    "s3://ethereum-mainnet/blocks",
    storage_options={
        "aws_endpoint_url": "https://storage.example.com",
        "aws_region": "us-east-1",
        "aws_skip_signature": "true",
    },
)
```

Each transaction commits to one table after another, `blocks` last. A block
visible in `blocks` has all of its rows in every other table, so bound other
tables by the newest `blocks` row for a consistent cut:

```sql
WITH f AS (SELECT max(block_num) AS b FROM delta_scan('s3://ethereum-mainnet/blocks'))
SELECT count(*) FROM delta_scan('s3://ethereum-mainnet/logs'), f
WHERE date >= DATE '2026-09-25' AND block_num <= f.b;
```

Use DuckDB 1.5 or later for S3: DuckDB 1.1's `delta` extension fails
anonymous reads once a table has a checkpoint, which the maintenance job
writes every hour.

A table's files, rows (each file's `numRecords`), bytes and days come from
its log alone, without reading any data file. This is what `fireparq scan`
reported before v1.0.0:

```python
# Table summary from the Delta log
import polars as pl
from deltalake import DeltaTable

table = DeltaTable("output/mainnet/blocks")
files = pl.DataFrame(table.get_add_actions(flatten=True))
summary = files.select(
    files=pl.len(),
    rows=pl.col("num_records").sum(),
    bytes=pl.col("size_bytes").sum(),
    first_day=pl.col("partition.date").min(),
    last_day=pl.col("partition.date").max(),
)
print(f"version {table.version()}", summary)
```

`fireparq validate <root>/blocks` checks block continuity over the same
snapshot of the log, and `fireparq inspect` reads the footer of one file.

#### Block range of a day

fireparq keeps no partition index (`partitions.parquet` and the `partitions`
subcommands were removed in #653): each table's log records every file's
`date` partition and `block_num` statistics, which answer the lookup. The
block range of one UTC day is:

```sql
-- DuckDB delta extension
SELECT min(block_num), max(block_num)
FROM delta_scan('<root>/blocks')
WHERE date = DATE '2026-09-25';
```

### Engine compatibility

DuckDB and Polars are the supported engines. CI builds real EVM (final and
non-final) and Solana output with a mock Firehose, writes a checkpoint of
every table, and reads every table with both engines through its Delta log
(`blocks/tests/engine_compat.rs`), at pinned versions: DuckDB 1.5.5 with its
`delta` extension `45c4087` (both checksum-verified), and Polars 1.44.2 with
`deltalake` 1.6.6 (hash-pinned). `blocks/tests/delta_maintenance.rs` reads the
tables again after the maintenance job compacted and vacuumed them beside a
running `build`.

| Delta type written | DuckDB | Polars | Notes |
|---|---|---|---|
| `long` | `BIGINT` | `Int64` | The mapper's `UInt64` (checked: a value above `i64::MAX` refuses the flush), `UInt32` and `UInt16` |
| `short` | `SMALLINT` | `Int16` | The mapper's `UInt8` |
| `decimal(20,0)` | `DECIMAL(20,0)` | `Decimal(precision=20, scale=0)` | Currency amounts and unchecked 64-bit values, exact up to `u64::MAX` |
| `timestamp` | `TIMESTAMP WITH TIME ZONE` | `Datetime(time_unit='us', time_zone='UTC')` | Parquet `TIMESTAMP(MICROS, isAdjustedToUTC=true)` holding whole milliseconds |
| `date` (partition column) | `DATE` | `Date` | Filters on `date` read only that day's files |
| `string` enum labels | `VARCHAR` | `String` | Pages still dictionary-encoded |
| `array<T>` | `T[]`, for example `SMALLINT[]` | `List(T)`, for example `List(Int16)` | |
| `binary` | `BLOB` | `Binary` | |

- Both engines read `date` from the Delta log (`delta_scan`, `scan_delta`); the
  data files do not contain it.
- Non-final output adds `fork_step` (`VARCHAR` / `String`) and `stream_ordinal`
  (`BIGINT` / `Int64`).
- [`docs/schemas/`](docs/schemas/README.md) lists every column's Delta type and
  each chain's mapping from the mapper's Arrow types.
- JVM engines (Spark, Trino) are not a target: the tables use reader
  version 1 with no table features, but CI does not test these engines.

To check anonymous reads of a deployment's public-read bucket (for example
Ceph RGW) with both engines, run the opt-in test against it. It sends only
unsigned requests, reads the newest closed day of `blocks` and one other
table, and compares the engines' rows, block ranges and pruning:

```bash
FIREPARQ_RGW_ENDPOINT=https://storage.example.com FIREPARQ_RGW_BUCKET=ethereum-mainnet \
FIREPARQ_DUCKDB=/path/to/duckdb FIREPARQ_POLARS_PYTHON=/path/to/venv/bin/python \
cargo test -p blocks --test engine_compat anonymous -- --nocapture
```

`FIREPARQ_RGW_PREFIX` names a dataset below the bucket root,
`FIREPARQ_RGW_REGION` the region (default `us-east-1`) and
`FIREPARQ_RGW_TABLE` the other table (default `transactions`). The Python needs
`blocks/tests/engines/requirements.txt`. Without `FIREPARQ_RGW_ENDPOINT` the
test is skipped, as in CI.

### Single-network buckets

With one bucket per network, the bucket root is the dataset root:

```bash
OUTPUT=s3://<bucket> fireparq build --network mainnet
# s3://<bucket>/.fireparq-ingest/, s3://<bucket>/_fireparq/cursor.parquet,
# s3://<bucket>/<table>/date=YYYY-MM-DD/part-*.parquet
```

- Other commands take the root or its tables directly:
  `recovery status s3://<bucket>`, `validate s3://<bucket>/blocks`, and
  `inspect` on one file such as `s3://<bucket>/_fireparq/cursor.parquet`.
- The bucket root then lists only the table prefixes, `_fireparq/` and the
  dot-prefixed control state, so a Spark, Trino, Hive or Delta table location
  at `s3://<bucket>/<table>/` or a hidden-path-aware scan of the whole bucket
  reads table data only; with DuckDB, read `delta_scan('s3://<bucket>/<table>')`.
- The first `build` needs an empty bucket; only the bucket owner record may
  already exist there. S3 ownership is
  bucket-wide in any case, so a bucket per network also gives each concurrently
  running `build` its own owner.
- To keep several networks in one bucket instead, use
  `--output 's3://<bucket>/{chain}'`. They then share the bucket-wide owner, so
  their mutating commands run one at a time.

## Delta Maintenance

fireparq only appends: each flush adds one file per table and day, and one
Delta commit per table. Compacting those files, deleting replaced ones and
checkpointing the logs is platform-side policy, not a fireparq command (#643):
[`scripts/delta_maintenance.py`](scripts/delta_maintenance.py) runs the
off-the-shelf `deltalake` Python package (pinned to 1.6.6 in
[`scripts/delta_maintenance.requirements.txt`](scripts/delta_maintenance.requirements.txt))
on a schedule, beside a running `build`. It needs no fireparq ownership:
fireparq's commits are blind appends that rebase over the job's commits, and
the job never touches `.fireparq-ingest/` or `_fireparq/`.

For each table it runs, in order:

1. **OPTIMIZE** each closed `date` with more than one file, to the table's
   `delta.targetFileSize` (256 MiB). A day is closed once `blocks` holds a
   later day: `blocks` commits last, so every earlier day is complete in every
   table.
2. **VACUUM**, lite by default: it deletes only files that a `remove`
   tombstone older than the retention names, and never a part that `build`
   published but has not committed yet. A full VACUUM (`FULL_VACUUM=1`, run
   weekly) also deletes untracked files older than the retention (the files of
   a failed OPTIMIZE), so the job refuses it below 168 hours.
3. **A checkpoint**, after VACUUM: a checkpoint drops expired tombstones, and
   a VACUUM after it would leave their files behind. It is skipped when
   VACUUM failed.
4. **Log cleanup** of commits older than `delta.logRetentionDuration`
   (7 days) behind a checkpoint.

Every step is idempotent: a failed or conflicting run changes nothing that
the next run cannot finish, and the writer never notices. One JSON object per
line goes to stdout (`start`, one `table` line per table, `done`); the exit
status is 0, 1 when a table failed, or 2 for a configuration error.
Credentials are read from the environment and never printed.

| Variable | Default | Meaning |
|---|---|---|
| `LAKE_ROOT` or `LAKE_BUCKET` | (required) | The dataset root, `s3://bucket[/prefix]` or a local path; `LAKE_BUCKET=b` is `s3://b`, a dataset at the bucket root |
| `LAKE_TABLES` | (required) | Comma-separated tables, for example every table of the network's [schema](docs/schemas/README.md); `blocks` must exist |
| `S3_ENDPOINT` | AWS | S3 endpoint URL, for example the in-cluster RGW |
| `AWS_REGION` | `us-east-1` | |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | (required on S3) | The maintenance user; `AWS_SESSION_TOKEN` is optional |
| `AWS_ALLOW_HTTP`, `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` | `false` | Plain HTTP; virtual-hosted instead of path-style requests |
| `FULL_VACUUM` | `0` | `1` for the weekly full VACUUM |
| `VACUUM_RETENTION_HOURS` | the table's 7 days | Lower only shortens how long readers of older snapshots find replaced files; a full VACUUM refuses less than 168 |
| `OPTIMIZE_DATES` | `closed` | `all` also compacts the newest day: safe beside the writer, but repeated every run; use it once the writer has stopped for good |
| `OPTIMIZE_TARGET_SIZE` | `delta.targetFileSize` | Bytes |
| `OPTIMIZE_ZSTD_LEVEL` | `3` | Compression of the compacted files |
| `OPTIMIZE_MAX_CONCURRENT_TASKS` | CPU count | Bounds OPTIMIZE's memory |
| `DRY_RUN` | `0` | `1` reports what would be compacted and deleted, and changes nothing |

```bash
pip install --only-binary=:all: --require-hashes -r scripts/delta_maintenance.requirements.txt
DRY_RUN=1 LAKE_ROOT=output/mainnet LAKE_TABLES=blocks,transactions,logs \
  python scripts/delta_maintenance.py
```

On Kubernetes,
[`deploy/examples/delta-maintenance-cronjob.yaml`](deploy/examples/delta-maintenance-cronjob.yaml)
runs it hourly (`17 * * * *`, `concurrencyPolicy: Forbid`) and a full VACUUM
weekly, as an unprivileged user with a read-only root filesystem and a `/tmp`
`emptyDir`. Give the job its own S3 user, limited to the table prefixes
(their `_delta_log/` included), with no access to `.fireparq-ingest/`,
`_fireparq/` or the owner record, and give the writer no delete permission on
`*/_delta_log/*`.

- A transaction that `build` has committed but not yet added to every table's
  log (after a crash in between, until the next start recovers it) must not
  stay pending longer than `delta.deletedFileRetentionDuration` (7 days), or a
  full VACUUM may delete its parts. Alert on a writer that has been down for
  more than a day.
- Alert on `firehose_parquet_delta_log_tail_commits` (see
  [Prometheus Metrics](#prometheus-metrics)) growing past a few hundred: the
  hourly checkpoints have stopped, and every reader and restart replays the
  whole tail.
- `blocks/tests/delta_maintenance.rs` runs the job over and over beside a real
  `build`, on local disk and on a loopback S3 endpoint, and checks the exact
  rows, the `txn` versions and the file counts afterwards.

## Canonical Identity Columns

Every table across all chains includes these 7 columns (from Firehose `BlockMetadata`):

| Column | Type | Description |
|---|---|---|
| `block_num` | long | Block number |
| `block_id` | string | Block ID (format depends on block type; see [Output Encoding by Block Type](#output-encoding-by-block-type)) |
| `parent_num` | long | Parent block number |
| `parent_id` | string | Parent block ID |
| `lib_num` | long | Last irreversible block number |
| `timestamp` | timestamp | Block time, UTC. Parquet logical type `TIMESTAMP(MICROS, isAdjustedToUTC=true)`; keeps sub-second precision to the millisecond where the chain has it (e.g. Antelope's 500 ms blocks) |
| `date` | date (partition) | UTC day of the block time: the `date=YYYY-MM-DD` directory, not stored in the files |

The `date=YYYY-MM-DD` directory is derived from the whole-second block time, so a block at `23:59:59.500` lands in the same day as one at `23:59:59.000`. Solana tables keep `timestamp` null when `block_time` is missing; such rows are routed to the day of the last known block time, which is their `date`.

## Output Encoding by Block Type

`fireparq` determines output encoding from the resolved block type/profile. Operators do not need to set a separate encoding flag. The effective values written for each file are exposed in Parquet metadata as `firehose-parquet.bytes_encoding` and `firehose-parquet.block_id_encoding`.

| Block type / profile | Block encoding | Transaction / hash encoding | Address / other binary field encoding | Notes |
|---|---|---|---|---|
| `evm` | `hex_0x` | `hex` | `hex` | `block_id` is `0x`-prefixed hex. Transaction hashes, log topics, and addresses are `0x`-prefixed hex. |
| `bitcoin` | `hex_0x` | Upstream text | Upstream text | Canonical IDs use `0x`-prefixed hex. Native hashes/txids/scripts/witnesses remain the original protobuf strings, normally Bitcoin Core hex without `0x`; addresses keep their native text format. |
| `solana` | `base58` | `base58` | Identifiers: `base58`; payloads: Binary; indices: List(UInt8) | Block IDs and binary identifiers stay base58. Instruction/error/return payloads and account-index lists have fixed types; see [Solana Payloads](#solana-payloads-and-account-indices). |
| `near` | `base58` | `base58` | `base58` | Block IDs, transaction hashes, receipt IDs, and key-like binary fields stay base58. `receipt_actions.args` is raw `Binary` under every encoding. |
| `antelope` | `hex_no_prefix` | `hex_no_prefix` | `hex_no_prefix` | Uses lowercase hex without `0x` for both block IDs and other binary fields. |
| `cosmos` | `hex_0x` | `hex` | `hex` | Block IDs are `0x`-prefixed hex. Other binary identifiers are `0x`-prefixed hex. |
| `tron` | `hex_no_prefix` | `hex_no_prefix` | `tron_base58` for addresses; `hex_no_prefix` for other binary fields | Address-like fields use Tron Base58Check. Canonical hashes, topics, and other non-address bytes remain lowercase hex without `0x`. |
| `beacon` | `hex_0x` | `hex` | `hex` | Block roots and other binary identifiers are `0x`-prefixed hex. |
| `tron-evm` (`evm` Tron-style profile) | `hex_no_prefix` | `hex_no_prefix` | `tron_base58` for addresses; `hex_no_prefix` for other binary fields | Same operator-facing contract as `tron`: address-like fields use Tron Base58Check, while canonical hashes/topics stay lowercase hex without `0x`. |

### Bitcoin amounts, input joins and missing fields

Use `outputs.value_sats` (`UInt64`) for exact sums. The original `value` column
remains a `Float64` coin amount for compatibility. When the protobuf includes
`Transaction.hex`, `value_sats` comes directly from its serialized integer
outputs; output counts, indices and the decoded coin amounts must agree. Older
payloads without raw transaction bytes use a strict conversion only when one
integer base-unit value recreates the supplied double. Ambiguous/fractional,
negative, non-finite or inconsistent amounts fail before any row of that block
is appended. The mapper is shared with Litecoin, so it does not impose Bitcoin's
21-million monetary bound; its unit scale is 100,000,000 per coin.

```sql
SELECT SUM(value_sats) AS total_sats
FROM read_parquet('output/btc/outputs/**/*.parquet');
```

`inputs.tx_index` joins to `transactions.tx_index` within the same canonical
`block_id`; add `input_index` for an input's position. Coinbase inputs have null
`prev_txid`, `prev_vout` and script-signature columns. Ordinary inputs have null
`coinbase`; a missing previous txid also keeps `prev_vout` null rather than
inventing output zero. A real previous output zero remains zero. A missing
script-signature message is null, while a present empty script remains `''`.
Witnesses retain the protobuf list, including an empty list when none is supplied.

`script_pubkey_address` prefers a nonempty modern `address`, then the first
legacy `addresses` entry, and is null if neither is available. The legacy first
entry is not a claim of exclusive ownership of a multi-address script. Missing
script-public-key messages yield null script columns; present empty fields stay
empty. Native protobuf strings are copied verbatim without adding prefixes or
reversing display-order hashes. Encoding settings apply to canonical ID columns.

These additions and nullable-field changes affect the Bitcoin table schemas.
Use a new output dataset or rebuild the affected tables when upgrading; merging
old and new files requires explicit schema reconciliation. Readers such as
DuckDB may use `union_by_name=true` when intentionally comparing versions, with
new columns null for older files.

### Antelope database-operation joins

`db_ops` includes `tx_hash` (the enclosing trace ID), `tx_index` (`UInt64`, the
original trace index), and `db_op_index` (`UInt32`, zero-based within that trace).
Filtering can leave transaction-index gaps. Operation positions restart for each
transaction and remain stable across flushes. Scope joins to the canonical block:

```sql
SELECT d.block_id, d.tx_hash, d.db_op_index, d.operation, t.status
FROM read_parquet('output/eos/db_ops/**/*.parquet') d
JOIN read_parquet('output/eos/transactions/**/*.parquet') t
  ON d.block_id = t.block_id
 AND d.tx_hash = t.tx_hash
 AND d.tx_index = t."index";
```

`actions` and `db_ops` end with the parent `transaction_status` and
`transaction_success` (see [deferred transactions and onerror](#antelope-deferred-transactions-and-onerror)).

The action fields `transaction_id`, `trace_block_num`, `producer_block_id`, and
`block_time` are deprecated for ordinary joins and routing; prefer `tx_hash` and
canonical block identity/time. They remain verbatim action metadata, which can
be missing or differ from canonical values. No removal is scheduled. Existing
action JSON, nulls and enum labels remain unchanged.

Use a new dataset or rebuild older ranges to populate the added columns. Schema
union makes them null in old files. See [the implementation and live comparison](docs/audit/508-antelope-db-joins.md).

## NEAR: Transactions, Receipts, Actions and Logs

A NEAR transaction's own outcome records its inclusion and conversion into a receipt. Contract calls run when action receipts execute, often in later blocks and on other shards. `transactions` and `receipts` carry the keys to follow that chain (every column is in the [NEAR schema reference](docs/schemas/near.md)):

| Table | Column | Type | Meaning |
|---|---|---|---|
| `transactions` | `transaction_index` | `UInt32` | Position in the block: chunks in shard order, then each chunk's transactions. Failed transactions left out by the filter keep their index. |
| `transactions` | `receipt_ids` | list of bytes | The outcome's `receipt_ids`. |
| `transactions` | `converted_into_receipt_id` | bytes, nullable | The receipt the transaction was converted into. Joins `receipts.receipt_id`. Null when the outcome has no receipt. |
| `transactions` | `status` | `Utf8` | The transaction's **own** outcome: `SuccessReceiptId` once it was converted into a receipt, `Failure` if it failed inclusion. It is not the final result of the contract calls ([final outcome](#near-final-transaction-outcome)). |
| `receipts` | `success_receipt_id` | bytes, nullable | For a `SuccessReceiptId` outcome, the receipt whose outcome becomes this receipt's result (the next link of NEAR's result chain). Null for other outcomes. |
| `transactions`, `receipts` | `tokens_burnt` | `Utf8` | yoctoNEAR burnt for gas, as a decimal string. |
| `receipts` | `receipt_index` | `UInt32` | Position of the execution outcome in the block: shards in order, then each shard's receipts. |
| `receipts` | `tx_hash` | bytes, nullable | The originating transaction, when it is in the same block (see below). |
| `receipts` | `signer_id` | `Utf8`, nullable | Signer of the transaction that started the receipt chain (`ReceiptAction.signer_id`). |
| `receipts` | `receipt_ids` | list of bytes | Receipts created by this execution. |

Two tables hold what each executed receipt did:

- **`receipt_actions`**: one row per action, keyed by `(receipt_id, action_index)`, with `receipt_index`, `tx_hash`, `shard_id`, `predecessor_id`, `receiver_id`, `signer_id` and:

  | Column | Type | Set for |
  |---|---|---|
  | `action_kind` | `Dictionary(Int32, Utf8)` | every row: `CreateAccount`, `DeployContract`, `FunctionCall`, `Transfer`, `Stake`, `AddKey`, `DeleteKey`, `DeleteAccount`, `Delegate` (the labels of `transactions.actions`) |
  | `method_name` | `Utf8` | `FunctionCall` |
  | `args` | `Binary` | `FunctionCall`. Raw bytes, usually JSON: `decode(args)` in DuckDB |
  | `gas` | `UInt64` | `FunctionCall`: the gas attached |
  | `deposit` | `Utf8` | `FunctionCall`, `Transfer`: yoctoNEAR, as a decimal string |

  The payload columns are null for the other kinds. A `Delegate` row (NEP-366 meta-transaction) only records the kind: the delegated actions run in a receipt of their own and appear as that receipt's rows.
- **`execution_logs`**: one row per line of the outcome's `logs`, keyed by `(receipt_id, log_index)`, with `receipt_index`, `tx_hash`, `shard_id`, `executor_id` (the account whose code logged), `predecessor_id` and `log`. NEP-297 events such as NEP-141 (fungible tokens) and NEP-171 (NFTs) are the lines that start with `EVENT_JSON:`. Transaction outcomes have no logs: converting a transaction runs no contract code.

Both tables cover every receipt in `receipts`, including failed ones, and end with the receipt's own `receipt_status` ([failed receipts](#near-failed-receipts)):

```sql
-- NEP-141 events of receipts that did not fail
WITH events AS (
  SELECT block_num, executor_id AS token,
         TRY_CAST(substr(log, 12) AS JSON) AS event  -- the text after 'EVENT_JSON:'
  FROM read_parquet('output/near-mainnet/execution_logs/**/*.parquet')
  WHERE log LIKE 'EVENT_JSON:%'
    AND receipt_status <> 'Failure'
)
SELECT block_num, token, event->>'event' AS event, event->'data' AS data
FROM events
WHERE event->>'standard' = 'nep141';
```

`receipts.tx_hash` is only filled from the same block, which in practice means the receipt NEAR runs right away when a transaction's `signer_id` is also its `receiver_id`. Most receipts run in a later block, so most have a null `tx_hash`. Filling it from earlier blocks would make the output depend on where a run started. To find the originating transaction of every receipt, follow `converted_into_receipt_id` and `receipt_ids` over the range:

```sql
WITH RECURSIVE origin(receipt_id, tx_hash) AS (
  SELECT converted_into_receipt_id, hash
  FROM read_parquet('output/near-mainnet/transactions/**/*.parquet')
  WHERE converted_into_receipt_id IS NOT NULL
  UNION
  SELECT child.receipt_id, origin.tx_hash
  FROM origin
  JOIN (
    SELECT receipt_id AS parent_id, unnest(receipt_ids) AS receipt_id
    FROM read_parquet('output/near-mainnet/receipts/**/*.parquet')
  ) AS child ON child.parent_id = origin.receipt_id
)
SELECT r.receipt_id, origin.tx_hash
FROM read_parquet('output/near-mainnet/receipts/**/*.parquet') AS r
LEFT JOIN origin USING (receipt_id);
```

Receipts whose transaction or any intermediate lineage link is outside the available range stay unresolved. These queries assume a finalized dataset; append-only non-final events need the finalized-reference handling described above.

### NEAR final transaction outcome

A transaction's result is decided by later receipts, usually in later blocks, so one block cannot hold it and `transactions.status` does not report it. NEAR's final outcome (the RPC's `FinalExecutionStatus`) starts at the transaction and follows `SuccessReceiptId` links until an outcome that is not `SuccessReceiptId`: `SuccessValue` or `Failure`. Receipts off that chain, such as a failed cross-contract call whose callback handled the error, do not change it. Over a range of blocks:

```sql
-- Final outcome of each NEAR transaction: follow SuccessReceiptId links
WITH RECURSIVE chain(tx_hash, receipt_id, depth) AS (
  SELECT hash, converted_into_receipt_id, 0
  FROM read_parquet('output/near-mainnet/transactions/**/*.parquet')
  WHERE status = 'SuccessReceiptId'
  UNION ALL
  SELECT chain.tx_hash, r.success_receipt_id, chain.depth + 1
  FROM chain
  JOIN read_parquet('output/near-mainnet/receipts/**/*.parquet') AS r
    ON r.receipt_id = chain.receipt_id
  WHERE r.status = 'SuccessReceiptId'
),
last AS (
  SELECT tx_hash, arg_max(receipt_id, depth) AS receipt_id
  FROM chain
  GROUP BY tx_hash
)
SELECT t.hash,
       CASE WHEN t.status <> 'SuccessReceiptId' THEN t.status
            WHEN r.status IS NULL THEN 'Pending'  -- the chain continues past the range
            ELSE r.status END AS final_status
FROM read_parquet('output/near-mainnet/transactions/**/*.parquet') AS t
LEFT JOIN last ON last.tx_hash = t.hash
LEFT JOIN read_parquet('output/near-mainnet/receipts/**/*.parquet') AS r
  ON r.receipt_id = last.receipt_id;
```

To find transactions with a failed receipt anywhere in their receipt tree, including side calls, reuse the lineage walk above:

```sql
-- Transactions with a failed receipt anywhere in their tree
WITH RECURSIVE origin(receipt_id, tx_hash) AS (
  SELECT converted_into_receipt_id, hash
  FROM read_parquet('output/near-mainnet/transactions/**/*.parquet')
  WHERE converted_into_receipt_id IS NOT NULL
  UNION
  SELECT child.receipt_id, origin.tx_hash
  FROM origin
  JOIN (
    SELECT receipt_id AS parent_id, unnest(receipt_ids) AS receipt_id
    FROM read_parquet('output/near-mainnet/receipts/**/*.parquet')
  ) AS child ON child.parent_id = origin.receipt_id
)
SELECT origin.tx_hash, count(*) AS failed_receipts
FROM origin
JOIN read_parquet('output/near-mainnet/receipts/**/*.parquet') AS r USING (receipt_id)
WHERE r.status = 'Failure'
GROUP BY origin.tx_hash;
```

Both need the whole chain in the range; a chain that continues past its end is `Pending` or incomplete.

### NEAR state changes

`state_changes` has one row per entry of the Firehose block's `state_changes` list:

| Column | Type | Meaning |
|---|---|---|
| `state_change_index` | `UInt32` | Position in the block's list (entries without a value or cause are skipped but keep their position) |
| `type`, `cause` | `Dictionary(Int32, Utf8)` | Change kind (`AccountUpdate`, `DataUpdate`, `AccessKeyUpdate`, ...) and cause (`TransactionProcessing`, `ReceiptProcessing`, `ActionReceiptGasReward`, ...) |
| `cause_tx_hash` | bytes, nullable | The transaction of a `TransactionProcessing` cause |
| `cause_receipt_hash` | bytes, nullable | The receipt of an `ActionReceiptProcessingStarted`, `ActionReceiptGasReward`, `ReceiptProcessing` or `PostponedReceipt` cause; joins `receipts.receipt_id` |
| `account_id` | `Utf8` | The changed account |
| `data_key`, `data_value` | bytes, nullable | `DataUpdate` key and value; `DataDeletion` key |
| `amount`, `locked` | `Utf8`, nullable | `AccountUpdate` balances in yoctoNEAR, as decimal strings |
| `storage_usage`, `code_hash` | `UInt64` / bytes, nullable | `AccountUpdate` storage in bytes and contract code hash |

Bytes columns follow the identifier encoding. Columns that do not apply to a row's change kind are NULL. Access-key permissions and contract code are not materialized.

**The table is empty with the StreamingFast NEAR producer.** Every published version of `near-firehose-indexer` (checked from 2021-08 to 2026-07) writes an empty `Block.state_changes`, and the Firehose protobuf has no per-shard state-change field. The columns above are mapped and tested, including on a projection of real NEAR state changes, so they fill in if a producer supplies the list ([#625](https://github.com/pinax-network/firehose-parquet/issues/625)).

These added columns and tables require a fresh dataset or an explicit rebuild; protected ingestion refuses to resume an incompatible schema inventory. See the [bounded public-source comparison and its coverage limits](docs/audit/506-near-public-qualification.md) and the [#507 status and state-change record](docs/audit/507-near-status-state-changes.md).

## Environment Variables

CLI flags can also be set via environment variables. Copy `.env.example` to `.env`
in the directory you run `fireparq` from. Since #617:

- `fireparq` loads `.env` from the **current working directory only**. Parent
  directories are never searched, so a run started in a subdirectory or a git
  worktree below a checkout does not inherit that checkout's production settings.
- `--env-file <PATH>` (or `FIREPARQ_ENV_FILE` in the process environment) loads
  exactly that file instead; it must exist, and `./.env` is then ignored.
- Process environment variables and CLI flags win over the file.
- Startup names the loaded file and the variables it supplied, never their
  values: an INFO `loaded env file` log line for `build`, and one stderr line
  for other commands. A malformed file is an error that never echoes the offending line.

```bash
# Authentication — use credentials scoped to the destination provider
PINAX_API_KEY=your-pinax-api-key-here
# PINAX_API_TOKEN=your-pinax-jwt-token-here
# STREAMINGFAST_API_TOKEN=your-streamingfast-compatible-token-here

# Prometheus metrics (optional)
# METRICS_PORT=9090

# Failed transaction filtering (optional)
# EXCLUDE_FAILED_TRANSACTIONS=true   # drop failed txs (EVM includes them by default)
# INCLUDE_FAILED_TRANSACTIONS=true   # include failed txs on non-EVM chains

# AWS S3 output (optional): an explicit URI plus both keys
# OUTPUT=s3://my-bucket/v1
# AWS_ACCESS_KEY_ID=...
# AWS_SECRET_ACCESS_KEY=...
# AWS_REGION=us-east-1
```

See `.env.example` for the full list of supported environment variables.

### Migrating Kubernetes deployments (#617)

Deployments that set `S3_BUCKET` plus a relative `OUTPUT` (or no `OUTPUT`) must
switch to an explicit S3 URI; otherwise `build` now exits
with an error before writing anything. Kubernetes expands `$(VAR)` references to
variables defined earlier in the same container's `env` list:

```yaml
env:
  - name: BUCKET_NAME
    valueFrom:
      configMapKeyRef: { name: fireparq, key: bucket }
  # Before: S3_BUCKET=$(BUCKET_NAME) and OUTPUT=v1, implicitly
  # s3://<bucket>/v1/<chain_name>. /{chain} keeps that directory.
  - name: OUTPUT
    value: s3://$(BUCKET_NAME)/v1/{chain}
```

`--output` is now used exactly as given, so a v0.7 `OUTPUT` keeps its layout only
with `/{chain}` added; a bucket per network can use `s3://$(BUCKET_NAME)` for a
bucket-root dataset instead ([single-network buckets](#single-network-buckets)).
Keep `S3_BUCKET` only if you want its bucket consistency check (it must then match
the `OUTPUT` bucket) or read-only shorthand keys; it no longer selects the output.
Relative cursor paths still land under the resolved dataset root. Check the
`resolved write destinations` log line after rollout.

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

Subcommands, shared arguments (`AwsArgs`, `CommonArgs`, `BuildArgs`), parsing
helpers and completions are defined once in `firehose_parquet::cli`. The binary
in `blocks/src/bin/main.rs` only adds the global logging flags and dispatches:

```rust
use firehose_parquet::cli::{build_config, init_tracing, Commands};

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>, // Build(BuildArgs), Scan, Validate, Recovery, ...

    #[command(flatten)]
    global: GlobalArgs, // --log-level, --verbose, --env-file
}
```

`BuildArgs` flattens `CommonArgs` (connection, range, output, flush and AWS
flags) and adds the chain flags (`--network`, `--block-type`,
`--without-extended`, `--without-votes`, the failed-transaction flags and
`--cursor-override`). `Commands::Build` runs `ingestion::run_ingestion` in
`blocks/src/bin/ingestion/`.

### Shell completions

The binary supports the `completions` subcommand for `bash`, `zsh`, `fish`,
`elvish` and `powershell`:

```bash
# Bash
fireparq completions bash > ~/.local/share/bash-completion/completions/fireparq

# Zsh
fireparq completions zsh > ~/.zfunc/_fireparq

# Fish
fireparq completions fish > ~/.config/fish/completions/fireparq.fish
```

## Repository Structure

A summary of the workspace; [docs/repo-navigation.md](docs/repo-navigation.md)
maps every module and where to edit for common tasks.

```
firehose-parquet/
├── Cargo.toml                              # workspace root (firehose-protos, firehose-parquet, blocks)
├── Dockerfile                              # multi-stage Docker build
├── .env.example                            # environment variables template (drift-tested against the CLI)
├── .github/workflows/                      # ci, docker-publish, release, network-endpoints
├── proto/                                  # chain and Firehose .proto files, plus proto/core/ dependencies
├── firehose-protos/                        # compiles proto/*.proto (build.rs) and exposes the modules
├── scripts/                                # generate_networks.rs, check_network_endpoints.sh,
│                                           #   delta_maintenance.py (the Delta maintenance job)
├── deploy/examples/                        # example Kubernetes manifests (the maintenance CronJob)
├── docs/                                   # contracts, runbooks, schema reference, release notes, audit records
├── firehose-parquet/                       # core library
│   └── src/
│       ├── cli.rs, cli/                    # shared Clap args and subcommands; configuration, paths,
│       │                                   #   inspect and validate helpers
│       ├── ingest/                         # all-table transactions, output authority, cursor mirror, recovery
│       ├── date_partition.rs               # the date=YYYY-MM-DD key, formatted and parsed in one place
│       ├── writer.rs, writer/              # Arrow -> Parquet part encoding, partition routing, protected parts
│       ├── delta/                          # Delta tables: type mapping, log stores, stats, commits
│       ├── dataset_lock/, dataset_lock_s3.rs  # local directory and bucket-wide S3 ownership
│       ├── durable_state.rs, durable_state_s3.rs  # versioned control records
│       ├── recovery.rs                     # `fireparq recovery`
│       ├── maintenance/                    # shared read-only listings (startup and recovery checks)
│       ├── grpc.rs, grpc/                  # Firehose stream client, auth, reconnects
│       ├── s3.rs, s3/                      # AWS config, bounded uploads
│       ├── auth.rs                         # provider-scoped credential selection
│       ├── networks.rs, networks_generated.rs  # built-in --network names (generated)
│       ├── config.rs, flush.rs, cursor.rs  # config model, flush sizing, cursor Parquet format
│       ├── encode.rs, encode/              # identifier encodings
│       └── artifacts.rs, metrics.rs, traits.rs  # reserved names, Prometheus, BlockMapper trait
├── blocks/                                 # chain mappers + unified binary
│   ├── src/
│   │   ├── bin/main.rs                     # `fireparq` entrypoint and command dispatch
│   │   ├── bin/ingestion/                  # `build`: mod.rs, setup.rs (endpoint/resume), runtime.rs
│   │   ├── chain.rs                        # ChainKind / ChainProfile per chain family
│   │   └── evm/, solana/, bitcoin/, beacon/, tron/, cosmos/, antelope/, near/
│   │                                       # per chain: proto.rs, schema.rs, mapper.rs
│   └── tests/                              # integration tests, including `fireparq` runs against a mock Firehose
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
