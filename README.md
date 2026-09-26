# firehose-parquet

A production-grade Rust toolkit that consumes [StreamingFast Firehose](https://firehose.streamingfast.io/) v2 gRPC streams and writes **Apache Parquet** files. A single unified binary (`fireparq`) supports multiple blockchain types with automatic chain detection.

## Supported Chains

| `--block-type` | Endpoint Example | Tables |
|---|---|---|
| `evm` | `eth.firehose.pinax.network:443` | blocks, transactions, logs, withdrawals, access_lists, set_code_authorizations, calls, balance_changes, code_changes, storage_changes, nonce_changes, gas_changes, account_creations, system_* (`--without-extended` disables the call and state-change tables) |
| `solana` | `solana.firehose.pinax.network:443` | blocks, transactions, messages, instructions, rewards, token_balances, account_lookups, vote_transactions (`--without-votes` disables `vote_transactions`) |
| `bitcoin` | `bitcoin.firehose.pinax.network:443` | blocks, transactions, inputs, outputs |
| `beacon` | `eth-cl.firehose.pinax.network:443` | blocks, attestations, deposits, proposer_slashings, attester_slashings, voluntary_exits, execution_payload, blob_sidecars, withdrawals, bls_to_execution_changes, deposit_requests, withdrawal_requests, consolidation_requests ([details](#beacon-chain-tables)) |
| `tron` | `mainnet.tron.streamingfast.io:443` | blocks, transactions, logs, internal_transactions |
| `cosmos` | `mainnet.injective.streamingfast.io:443` | blocks, transactions, events, messages |
| `antelope` | `eos.firehose.pinax.network:443` | blocks, transactions, actions, db_ops |
| `near` | `mainnet.near.streamingfast.io:443` | blocks, chunks, transactions, receipts, receipt_actions, execution_logs, state_changes |

> **Tip:** Use `--block-type auto` (the default) to auto-detect the chain from the Firehose stream's protobuf `type_url`.

## Features

- **Single binary** — one `fireparq` binary handles all chains via `--block-type` with auto-detection
- **Multi-chain** — pluggable `BlockMapper` trait with per-chain mapper modules
- **Canonical identity columns** — `block_num`, `block_id`, `parent_num`, `parent_id`, `lib_num`, `timestamp`, `date` on every table; `timestamp` is `Timestamp(Millisecond, UTC)` (Parquet `TIMESTAMP(MILLIS, isAdjustedToUTC=true)`, so DuckDB, Spark, Trino and ClickHouse read it as a timestamp) and keeps sub-second block times where Firehose provides them; `date` is an Arrow `Date32` derived from the UTC block timestamp. For Solana, canonical `timestamp` / `date` stay nullable when `block_time` is missing, and synthetic timing is used only for time-based partition routing. Chain-specific columns never reuse these names: Tron `transactions` stores the transaction's own creation and expiration times as `tx_timestamp_ms` / `expiration_ms` (Int64 unix milliseconds; `tx_timestamp_ms` is set by the sender, so it can be 0 or use another unit)
- **gRPC streaming** — connects to any Firehose v2 endpoint via tonic, with TLS and API key / JWT auth
- **Network aliases** — `--network` resolves built-in Firehose names and supports `FIREHOSE_ENDPOINT_*` per-network overrides
- **Automatic retry / resume** — exponential back-off on connection errors; restarts from the authoritative output checkpoint
- **Recovery guardrails** — optional stream idle timeout and reconnect stall timeout to force self-recovery or fail-fast restarts
- **Crash recovery** — all-table transactions and an authoritative output checkpoint; `cursor.parquet` remains an optional compatible mirror
- **S3-aware cursor** — cursor automatically stored alongside output (local or S3)
- **Partitioning** — `none`, `block_range`, `date`, `hour`, `minute`, or `second` layouts
- **File rollover** — flush by row count, byte size, or time interval
- **Fork handling** — finalized output by default; `--final-blocks-only=false` preserves append-only `fork_step` events ([query semantics](#non-final-streams-and-reorgs))
- **Failed transactions** — EVM includes failed/reverted txs by default with only their persistent state changes (`--exclude-failed-transactions` drops them); other chains exclude them unless `--include-failed-transactions` is set
- **Block-type-based encoding** — identifiers follow the resolved chain/profile defaults, recorded in Parquet metadata; opaque Solana payloads use Binary and account indices use UInt8 lists
- **Compression** — zstd (default level 3), explicit `zstd:<level>`, snappy, gzip, or none
- **Parquet file metadata** — every file embeds pipeline provenance (`firehose-parquet.*` key-value pairs) in the Parquet footer
- **Prometheus metrics** — opt-in `/metrics` endpoint for monitoring throughput, buffer state, and errors
- **Graceful shutdown** — SIGINT/SIGTERM and write/stream errors never save the cursor past unwritten data; the next run resumes from the last committed flush
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
  --without-votes \
  --output ./output

# Disable extended EVM tables explicitly (explicit block type)
./target/release/fireparq build \
  --block-type evm \
  --endpoint https://eth.firehose.pinax.network:443 \
  --start-block 19000000 \
  --stop-block 19001000 \
  --without-extended \
  --bytes-encoding hex

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
  --output ./output \
  --partition date

# Start live mode from the endpoint's first streamable block
./target/release/fireparq build \
  --network mainnet \
  --output ./output

# Add verbose operational logs for debugging without changing normal output by default
./target/release/fireparq build \
  --network mainnet \
  --start-block 20000000 \
  --stop-block 20001000 \
  --verbose
```

### Authentication

Credentials are selected from the **resolved endpoint host**, including any
`--endpoint`, `ENDPOINT`, or `FIREHOSE_ENDPOINT_*` override. The same rules apply
to `build` and `partitions build`:

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

Startup logs identify the destination host, provider, and names of credential
variables selected for transmission (`none` when absent), never their values.
Surrounding whitespace is trimmed, so a key mounted from a secret file with a
trailing newline works. A selected credential that still contains characters a
gRPC header cannot carry (control characters or line breaks inside the value)
fails at startup with an error.

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

`fireparq build` stores an authoritative checkpoint and an all-table transaction
journal under `<output>/<chain>/.fireparq-ingest/`. Every flush either recovers
its complete committed output or removes its owned uncommitted parts before
replay. The optional `cursor.parquet` file mirrors this authority; deleting the
mirror cannot rewind ingestion. These controls contain opaque source cursors
and should receive the same access restrictions as the cursor file.

Existing datasets without this authority are not adopted automatically. Rebuild
into a new empty output root, with an absent cursor mirror. A verified same-chain
v2 `partitions.parquet` may already exist there. Keep legacy datasets available
for read-only tools and guarded legacy maintenance. See the
[transaction and migration contract](docs/audit/468-ingestion-runtime.md).

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

Both `build` and `partitions build` require EndpointInfo with a nonempty chain name before resolving output or cursor paths. Transient Info failures get three attempts with bounded backoff; exhausted retries, authentication errors, or unsupported Info stop startup. `--network`, `--block-type`, and `--cursor-override` do not bypass this requirement. This prevents a temporary metadata failure from changing the output root or hiding the existing cursor. Older servers must expose the Info RPC. Protected ingestion resolves its mapper before recovery; unknown custom chain metadata requires an explicit `--block-type`. See [the implementation record](docs/audit/467-endpoint-info.md) for retry limits and validation.

```bash
# Built-in alias
fireparq --network mainnet --start-block 20000000 --stop-block 20001000

# Per-network override
export FIREHOSE_ENDPOINT_MAINNET=https://eth.internal.example.com:443
fireparq --network mainnet --start-block 20000000 --stop-block 20001000

# Canonical-name override
export FIREHOSE_ENDPOINT_SOLANA_MAINNET_BETA=https://solana.internal.example.com:443
fireparq --network solana-mainnet-beta --start-block 250000000 --stop-block 250100000
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
   Committed transaction before opening Firehose Blocks. Never infer progress
   from the greatest block number, a filename, or an external cursor alone.

### Local part publication

Protected local parts use a hidden transaction-owned `.tmp` name in the same
directory as their final `part-v1-*.parquet` name. The writer completes and syncs
the Parquet file, creates the final name with a no-clobber hard link, and syncs
directory links. Recovery verifies exact journal ownership before removing a
partial transaction or accepting a committed file. Canonical and lexical output
ancestry are both synced, preserving explicit output-root symlink aliases.

This requires atomic same-directory hard links, file and directory sync, readable
directory ancestry, and macOS/Linux inode locking. Unsupported operations fail
closed. Nested symlink entries inside guarded trees are refused. External writers
that bypass ownership are unsupported. Legacy low-level writers and maintenance
retain their independent naming and journal rules.

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

When output is written locally or to S3, the default cursor file is automatically placed alongside the data under the resolved chain output root — no special configuration needed:

| Output | `--cursor` value | Cursor location |
|---|---|---|
| `./output` | *(default)* | `./output/<chain>/cursor.parquet` |
| `s3://bucket/prefix` | *(default)* | `s3://bucket/prefix/<chain>/cursor.parquet` |
| `s3://bucket/prefix` | `my-cursor.parquet` | `s3://bucket/prefix/<chain>/my-cursor.parquet` |
| `s3://bucket/prefix` | `s3://other/path.parquet` | `s3://other/path.parquet` |
| `./output` | `s3://other/path.parquet` | `s3://other/path.parquet` |

An explicit S3 cursor URI uses its own bucket and key. It can be separate from
the data bucket; both use the configured AWS credentials, region and endpoint.
For separate buckets, use a service endpoint or omit the endpoint for standard
AWS S3. Bucket-specific AWS endpoints (including global, regional, dualstack and
accelerate forms) and `bucket.fly.storage.tigris.dev` use virtual-hosted requests
and reject a different cursor bucket. Other custom endpoints must support
path-style requests at a service endpoint; arbitrary bucket-specific custom
domains are not inferred. These addressing rules also apply to S3 maintenance
and inspection commands.
Relative cursor paths inherit the resolved output bucket and prefix. Explicit
local output paths (`./output`, `../output`, or an absolute path) stay local even
when `S3_BUCKET` is set, and absolute local cursor paths remain absolute for
local output.

When `--output` is an S3 URI, `--s3-bucket` or `S3_BUCKET`, if set, must name the
same output bucket. A mismatch now fails before contacting Firehose or storage;
unset the bucket option or make it match. This consistency check applies to
`build` and `partitions build` and does not restrict an explicit cursor URI to
the data bucket.

An S3 cursor requires complete explicit AWS credentials even when data output
is local. This is validated after `--cursor-template` expansion as well as for
`--cursor`; neither form silently falls back to instance metadata credentials.

Authenticated S3 `build` spools one Parquet part to private temporary disk, then
streams one conditional PUT and verifies the entire object through a second
private spool before committing. Budget temporary disk for two encoded parts,
in addition to mapper memory. Native ingestion requires an HTTPS endpoint and
limits a part to 5,000,000,000 encoded bytes and its serialized footer to 32 MiB;
resume verification applies the same limits. Connections have a 10-second timeout;
upload and complete readback each have a 15-minute deadline. Failed or cancelled
writes retain ownership for provider-quiescent recovery. Maintenance commands and
the generic `ParquetTableWriter::new_s3` API keep their existing buffering and
endpoint policy. See [qualification and limits](docs/audit/520-bounded-s3-ingestion.md).

### Parameter Validation on Resume

Protected output binds the original start and block-range anchor, chain and mapper
family, exact table schemas and mapper epoch, identifier encoding, partitioning,
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
new dataset. `--cursor none` disables the mirror only; authority remains mandatory.
Changing or disabling the mirror of an existing protected dataset is refused.

Local mirror saves use private same-directory temporary files, atomic replacement,
file and directory sync, and up to three attempts with 1 and 2 second backoff.
S3 mirror updates use one conditional Create/Update with transport retries disabled,
then exact readback. Failed or cancelled saves preserve pending recovery state;
a shutdown during local retry backoff still reports the durability failure.

Mutating commands hold common ownership over output, source and external cursor
or artifact locations. Local ownership uses macOS/Linux directory locks; nested
symlinks inside mutation trees are refused. S3 ownership covers the whole bucket
and requires conditional-write support plus access to reserved control keys.
Unresolved remote errors retain ownership without an expiry or automatic takeover.
`fireparq recovery status <path>` reads a summary. Explicit remote release requires
the exact owner/generation and evidence that both the writer and all prior remote
requests are quiescent; stopping the process alone is insufficient. See the
[ownership and recovery runbook](docs/audit/468-stage1-ownership.md).

Protected datasets allow guarded lossless merge and copy-only rollup into a
separate unprotected output. Truncate, in-place rollup and source-deleting rollup
are refused because they cannot reconcile the ingestion checkpoint. Maintenance
selected at a table/partition or parent root discovers every affected protected
dataset and its external mirror before recovery or data reads.

### Cursor Override and Migration

`--cursor-override` cannot reset, rewind or change protected output semantics.
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
recovery reconciles its pending transaction before any source replay.

If a write (local disk or S3), a block mapping, or the stream fails, the
pipeline also discards partial buffers and does not save the cursor, then exits
non-zero. Recovery removes verified parts from an uncommitted transaction before
replaying its window, or finishes a committed transaction without remapping it.
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
  command exits nonzero with a diagnostic. Repeating an already proven bound
  opens no Blocks request; extending it uses the authoritative cursor.
- **Live runs** (no `--stop-block`) never end on their own: if the server or a
  proxy closes the stream cleanly, the run reconnects from the last cursor with
  the usual back-off.

## CLI Reference

The primary ingestion workflow is `fireparq build`. Utility workflows stay
under subcommands such as `partitions`, `scan`, `inspect`, `validate`,
`verify`, `rollup`, `merge`, and `truncate`.

For full CLI help, run `fireparq --help` for the top-level command surface or
`fireparq build --help` for ingestion-specific flags. The summary below keeps
the main operator path up front and leaves the less-common deployment and
recovery knobs to dedicated advanced sections.

### Common ingestion flags

| Area | Common flags |
|---|---|
| Connection | `--network <NETWORK>` or `--endpoint <ENDPOINT>` |
| Range | `--start-block <START_BLOCK>`, `--stop-block <STOP_BLOCK>`, `--live` |
| Resume | Rerun the same original range; output authority selects progress and repairs the bound optional cursor mirror |
| Output | `--output <OUTPUT>`, `--partition <PARTITION>`, `--compression <COMPRESSION>` |
| Chain | `--block-type <BLOCK_TYPE>` (default `auto`), plus chain-specific toggles like `--extended false` or `--with-votes false` only when needed |
| Runtime | `--final-blocks-only[=true|false]` (default `true`), `--flush-bytes <FLUSH_BYTES>`, optional `--flush-rows` / `--flush-interval-secs` |

### Non-final streams and reorgs

Finalized-only output is the default. Use `--final-blocks-only=false` to receive
reversible blocks, or set `FINAL_BLOCKS_ONLY=false`. An explicit CLI value takes
precedence over the environment. The bare `--final-blocks-only` flag still means
`true`; optional values use `=` so the flag cannot consume a following command.
An unbounded `--live` run controls when streaming stops, independently of whether
blocks must be final.

Non-final output is an **append-only event history**. Every mapped envelope adds
rows carrying `fork_step`: `NEW` adds a block, `UNDO` records its removal from the
chain, and `FINAL` is an explicit final event if the endpoint sends it. The usual
non-final protocol sends `NEW` and occasional `UNDO`, not a later `FINAL` for
every block. UNDO does not delete earlier rows. A block identity can return as
`NEW` after an `UNDO`; replay/reconnect can also repeat deliveries. Treat unknown
steps as unresolved rather than inferring their effect.

The current table schema has **no global event sequence**. Block height, block
time, `lib_num`, filenames, file enumeration, and row order in a multi-file scan
are not delivery-order keys. The opaque saved cursor is a resume checkpoint,
not a sortable per-row sequence. Neither filtering out every identity with an
UNDO nor counting NEW minus UNDO reconstructs arbitrary canonical state:
`NEW(A), UNDO(A), NEW(A)` ends with A present, while a repeated `NEW(A), NEW(A),
UNDO(A)` ends with A absent despite the same unordered rows. Consequently the
current reversible dataset alone cannot supply a general canonical-tail query.

For a safe **finalized block-identity subset**, build a separate dataset with
`--final-blocks-only=true` covering the desired range on the same chain/network,
with matching identifier encoding. Then intersect its authoritative identities with observed
positive events:

```sql
-- DuckDB: finality comes from the separate finalized-only capture.
-- Returns one identity per finalized block also observed as NEW/FINAL.
WITH finalized AS (
  SELECT DISTINCT block_num, block_id
  FROM read_parquet('finalized/mainnet/blocks/**/*.parquet')
), observed AS (
  SELECT DISTINCT block_num, block_id
  FROM read_parquet('reversible/mainnet/blocks/**/*.parquet')
  WHERE fork_step IN ('NEW', 'FINAL')
)
SELECT f.block_num, f.block_id
FROM finalized f
JOIN observed o USING (block_num, block_id)
ORDER BY f.block_num, f.block_id;
```

This query is limited to the finalized reference's coverage; it says nothing
about the remaining reversible tail or the ordering of its events. It handles
repeated NEW/UNDO/NEW identities without inventing ordering. For transaction,
log, or other child-table aggregates, query the finalized-only dataset directly.
Joining child rows to these identities does **not** remove repeated deliveries,
and generic `DISTINCT *` can collapse legitimate duplicate rows. Reconstructing
a full reversible state requires a separately preserved, complete ordered event
log and event/row occurrence keys; the current public Parquet schema does not
provide those guarantees.

Use different output roots/cursors for final-only and non-final captures;
resuming a cursor with a different mode is incompatible. A bounded non-final
run warns on successful completion because reaching its stop does not prove
that its tail is final, and later UNDO events will not be received after it
stops. A saved cursor or successful exit is not a finality certificate. See the
[implementation and offline query checks](docs/audit/474-non-final-streams.md).

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
| `--cursor-override` | Legacy dry-run override; protected output refuses rewinds and requires a new empty root for changed semantics |
| `--stream-idle-timeout-secs <N>` | Supervising long-lived pipelines that should self-reconnect after a silent stream stall (default 120; `0` disables and relies on HTTP/2 keepalive). On slow chains such as Bitcoin (~600 s blocks), set it above the block time to avoid a reconnect every 120 s. An idle reconnect is not counted as a failure. |
| `--reconnect-stall-timeout-secs <N>` | Fail fast when reconnect loops should hand control back to an external supervisor (default 900; `0` disables). The timer starts at the first failed attempt and is reset only when a stream message arrives, not when a connection or RPC succeeds. |

#### Receive transport

Both `build` and `partitions build` use 16 MiB HTTP/2 stream and connection
receive windows and accept plain, gzip, or zstd replies. The server selects the
response encoding; requests remain uncompressed. Parquet `--compression` is
independent of transport compression.

| Flag / environment | Behavior |
|---|---|
| `--grpc-window-bytes` / `GRPC_WINDOW_BYTES` | Initial stream and connection receive window, default `16777216`. `0` restores the underlying library defaults. Larger windows allow more data in flight and can increase buffering. |
| `--grpc-adaptive-window[=true\|false]` / `GRPC_ADAPTIVE_WINDOW` | Opt into automatic window tuning; default false. When true, it overrides `--grpc-window-bytes`. |
| `--grpc-max-message-bytes` / `GRPC_MAX_MESSAGE_BYTES` | Maximum encoded or decompressed protobuf response bytes, default `134217728` (128 MiB). Values must be positive and fit UInt32. Applies to Info, Fetch, ingestion, and finalized index traversal/proof calls. |

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

Most operators can point `--output` directly at a local path or `s3://...`
prefix and rely on ambient AWS credentials. These flags are only needed for
custom deployment environments:

| Flag group | Purpose |
|---|---|
| `--s3-bucket <S3_BUCKET>` | Prefix relative output paths with `s3://<bucket>/...`; must match an explicit S3 output URI |
| `--aws-access-key-id`, `--aws-secret-access-key`, `--aws-session-token`, `--aws-region` | Override ambient AWS credential and region resolution |
| `--aws-endpoint-url <AWS_ENDPOINT_URL_S3>` | Target S3-compatible object stores |
| `--cache-control <CACHE_CONTROL>` | Set upload headers for CDN or static distribution workflows |
| `--metrics-port <METRICS_PORT>` | Expose Prometheus and health endpoints for monitored deployments |

Each `build` mapper flush commits its nonempty tables together before advancing
output authority and the cursor mirror. `--flush-bytes` is a **target compressed
size for the largest table's file**, defaulting to 32 MiB in Config and the
`build`, `merge`, and `rollup` commands. Build starts with a conservative
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

`--flush-rows`, `--flush-blocks`, `--flush-interval-secs`, partition changes, the
memory threshold and clean end of input can all force files below the size
target. Highly compressible data may never reach 32 MiB before the memory
threshold; increasing the file target does not bypass that threshold. `merge`
and `rollup` use their own streaming writer and memory policies.

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

- Writes a v2 finalized snapshot to `/<chain>/partitions.parquet`, with `zstd` compression by default. Chain and partition type are stored in file metadata.
- Time indexes traverse every finalized block in the requested interval and check canonical parent links. They preserve the existing raw-timestamp routing: timestamps `[A, B, A]` produce three contiguous runs, including two separate runs for A.
- `--start-block` is inclusive and `--stop-block` is exclusive. Bounds are never expanded. Clipped first/last spans and the current finalized-head time span carry `complete=false`.
- A complete span has both natural boundaries established within the declared snapshot. It does **not** establish globally complete calendar coverage: the same date can recur elsewhere or later.
- A bounded two-call Stream check proves an exact finalized block identity before accepting coverage. A future stop, non-final response, missing identity, contradictory ancestry or unresolved boundary fails without publishing a new snapshot.
- Solana missing timestamps use a verified prior anchor, matching ongoing ingestion routing. Missing required context fails; there is no future-timestamp borrowing. Non-Solana missing-time bootstrap is refused for time indexes. `block_range` remains available without timestamp routing.
- `--resume` and `--live` continue from the stored source-block frontier and verified context, including an open final span. They do not resume by the greatest calendar key. A bounded stop already covered is a no-op after endpoint/finality validation.
- Existing files require `--resume` or `--overwrite`. Legacy indexes lack trustworthy completeness and must be rebuilt with `--overwrite` or into a fresh output root.
- Each successful bounded run or live extension publishes one validated snapshot. A failed or cancelled scan leaves the previous snapshot intact. Long backfills should use successive bounded runs; time index construction now reads every covered block.

See [the build contract and limits](docs/partitions-build-defaults.md) for finality,
parent context, skipped-slot handling and endpoint requirements.

| Flag | Default | Description |
|---|---|---|
| `--partition` | none | `date`, `hour`, `minute`, `second`, or `block_range` |
| `--block-range-size` | required for `block_range` | Width of deterministic block-number partitions |
| `--start-block` | inferred | Fresh bounded start: explicit value, sibling cursor frontier, then endpoint first streamable block; fresh live uses explicit value or endpoint |
| `--stop-block` | none in live mode | Exclusive bounded stop, no later than the proven finalized block plus one; incompatible with `--live` |
| `--live` | `false` | Poll finalized coverage and extend the stored source frontier |
| `--poll-interval-secs` | `30` | Wait between live finalized-head checks |
| `--output` | inferred from `--s3-bucket` | Output root directory or `s3://` URI prefix |
| `--s3-bucket` | none | S3 bucket used when no explicit local output overrides the environment default |
| `--resume` | `false` | Extend an existing verified v2 index |
| `--overwrite` | `false` | Rebuild and replace the index after successful validation; conflicts with `--resume` |
| `--json` | `false` | Emit result and declared coverage as JSON; use `--log-level error` for stdout without progress logs |

### `partitions ls` — Query Partition Index Rows

Lists rows from `partitions.parquet` with declared coverage, per-span completeness, required prior routing context, optional filters and deterministic ascending order by partition value. Legacy completeness is reported as unknown. Ordering and `--from` / `--to` filters use the numeric partition value (start block for `block_range`, UTC epoch seconds otherwise), so block ranges such as `8000000` sort before `10000000`.

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
| `--from` | none | Inclusive lower bound on the partition value (`YYYY-MM-DD HH:MM:SS`, or a start block for `block_range`) |
| `--to` | none | Inclusive upper bound on the partition value (`YYYY-MM-DD HH:MM:SS`, or a start block for `block_range`) |
| `--limit` | `100` | Maximum rows returned |
| `--json` | `false` | Emit machine-readable output |

### `partitions shard` — Deterministic Partition Assignment

Assigns complete, independently routable v2 spans to one shard for multi-container runs. Any selected incomplete or legacy row is refused. Each returned row carries its own exact bounds; repeated calendar values are never collapsed into one range. Hash assignment keeps repeated values on the same shard.

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

V2 validation checks the entire snapshot's source-order continuity, declared
finalized bounds, span flags, identity links and routing evidence. Repeated
calendar values are valid. The report separately counts incomplete spans;
`valid=true` means structurally valid, not that every span is complete or that a
calendar date is globally covered. Legacy files retain geometry checks, report
unknown completeness and cannot be used for strict resolution. `--allow-gaps`
applies only to legacy geometry checks. Malformed v2 files fail regardless of it.

Violations exit non-zero for CI gating.

### `partitions resolve` — Resolve Partition Block Bounds

Resolves one complete, independently routable span from a v2 index and prints its exact bounds (`start_block` inclusive, `stop_block` exclusive) plus the declared finalized coverage. Incomplete, legacy and ambiguous repeated-value lookups fail. This is coverage within the observed snapshot, not a promise that every occurrence of the date has been found.

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

# Inspect every complete run for a repeated calendar value
fireparq --log-level error partitions resolve \
  --partitions-index ./output/eth-mainnet/partitions.parquet \
  --partition-type date \
  --partition-value '2015-07-30 00:00:00' \
  --all-spans --json
```

`--all-spans` requires `--json` and returns ordered separate spans without an
enclosing `start_block`/`stop_block`. It still rejects incomplete matches and
marks any span whose routing requires prior timestamp context. Such a span is
inspection evidence, not an independently usable ingestion range. `--strict-single-chain`
remains accepted for compatibility; v2 already requires one chain per index.

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

### Advanced Cursor Management (`--cursor-template`)

Most operators can rely on the default `cursor.parquet` placement. Use
`--cursor-template` only when you need deterministic per-run or per-deployment
cursor paths.

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
fireparq inspect ./output/blocks/year=2026/month=01/day=15/part-000001.parquet

# Resolve a shorthand key against S3_BUCKET when no local path matches
S3_BUCKET=my-bucket fireparq inspect evm/partitions.parquet

# Inspect an S3 file
fireparq inspect s3://my-bucket/evm/blocks/year=2026/month=01/day=15/part-000001.parquet

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

Validates partitioned Parquet data for gaps, ordering errors, duplicates, parent hash mismatches, and timestamp reversals. Only partitions with issues or warnings are printed; clean ones are silently counted. Supports local paths, shorthand S3 keys/prefixes via `S3_BUCKET`, and explicit S3 URIs.

Timestamp reversals (a block whose `timestamp` is earlier than the previous block that has one) are reported as warnings and do not change the exit code, because some chains (for example Bitcoin) allow non-monotonic block times. The `timestamp` column may use any Arrow timestamp unit or legacy `Int64` epoch seconds, and null timestamps are skipped.

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

Verifies deterministic partition Merkle roots and optional protocol checks under one command surface, one table of one network per run. Supports local paths, shorthand S3 keys/prefixes via `S3_BUCKET`, and explicit S3 URIs for the data path.

The chain and table are inferred from the data: the chain from the `firehose-parquet.block_type` file metadata, the table from the directory layout (`<output>/<chain_name>/<table>/...`). The registry defaults to `<output>/<chain_name>/merkle_roots.parquet`, one per network, and published reports go to `<output>/<chain_name>/verify_runs/<run_id>/report.json`.

```bash
# Standard profile (default): roots + protocol
fireparq verify ./output/mainnet/blocks

# Quick profile (low-cost)
fireparq verify ./output/mainnet/blocks --profile quick

# Explicit checks override profile defaults
fireparq verify ./output/mainnet/blocks --checks roots,protocol

# Publish report to the suggested artifact path
fireparq verify ./output/mainnet/blocks --publish-report

# Resolve a shorthand S3 data path when no local match exists
S3_BUCKET=my-bucket fireparq verify mainnet/blocks
```

Lookup order for the data path matches `scan` / `inspect`: explicit `s3://...` URIs win, existing local paths win over shorthand S3 resolution, and only missing relative paths fall back to `s3://<S3_BUCKET>/<path>`.

| Flag | Default | Description |
|---|---|---|
| `--chain` | *(from `firehose-parquet.block_type`)* | Chain family (`evm`, `bitcoin`, `solana`, ...). Needed only for files without that metadata; a different value is an error |
| `--table` | *(from the table directory)* | Table name. Needed only when files are not inside a table directory; a different value is an error |
| `--registry-path` | `<chain_root>/merkle_roots.parquet` | Explicit registry location (local or `s3://`); rows are keyed by network, so one registry can serve several networks |
| `--update-registry` | `false` | Accept the current data: replace differing roots (reported as `updated`; the run passes once the registry is written) |
| `--checks` | *(from profile)* | Comma-separated check families: `roots`, `protocol`, `continuity`, `completeness` |
| `--profile` | `standard` | Preset families: `quick` (roots), `standard` (roots+protocol), `deep` (adds continuity+completeness) |
| `--scope` | `table` | Metadata scope tag in reports: `chain`, `table`, `partition`, `run` |
| `--hash-strategy` | `auto` | Hash strategy for leaves+Merkle nodes: `auto`, `keccak256`, `sha256` |
| `--publish-report` | `false` | Publish `report.json` to the suggested verify artifact path |
| `--publish-report-path` | *(suggested path)* | Override where the published report is written (local or `s3://`) |

Migration note: `--chain` and `--table` no longer default to `evm` and `blocks`, and the default registry moved from `<chain>/mainnet/merkle_roots.parquet` to the network directory. `verify` warns when it finds a registry at the old location; see [Moving a registry from the old default location](docs/verifiability-artifact-runbook.md#moving-a-registry-from-the-old-default-location).

Roots use the versioned `merkle_v2` construction, recorded as `merkle_version` in `merkle_roots.parquet` and in the report. Registries written by v0.7.1 and earlier hold legacy `merkle_v1` roots: `verify` reports them as mismatches until they are rebuilt with `--update-registry`.

A failing run never changes the registry: roots are recorded only when no protocol check failed and no root differs (or `--update-registry` was given). The newest partition of a dataset whose `cursor.parquet` has not reached its stop block is reported as `open` and is not recorded. Registry writes are atomic locally and use conditional puts on S3, so concurrent runs do not lose updates. See [Root registry update semantics](docs/verifiability-artifact-runbook.md#root-registry-update-semantics). See the [runbook](docs/verifiability-artifact-runbook.md#migrating-a-legacy-merkle_v1-registry) for the procedure.

See [Cross-chain verifiability hash strategy](docs/verifiability-hash-strategy.md) for defaults and normalization rules.

See [Verify report contract](docs/verify-report-contract.md) for schema versioning, run metadata fields, and artifact path guidance.

See [Verifiability artifact runbook](docs/verifiability-artifact-runbook.md) for registry/report lifecycle, S3 publication guidance, and operational workflows.

### `rollup` — Roll Up Partitions

Rolls up fine-grained partitions (e.g. `minute` or `hour`) into coarser ones (e.g. `date`). Validates each target partition before writing, then streams source batches into new files using the `--flush-bytes` target. The source path is a local path or an explicit S3 URI.

```bash
# Roll up minute-partitioned data into daily partitions, replacing the minute files
fireparq rollup ./output/blocks/ -p date --delete-source

# Roll up into a different output directory, keeping the source files
fireparq rollup ./output/blocks/ -o ./rolled-up/blocks/ -p date

# Roll up S3 data in place
fireparq rollup s3://my-bucket/evm/blocks/ -p date --delete-source
```

The source path must exist locally or be an explicit `s3://...` URI. Unlike `scan` and `inspect`, `rollup` never falls back to `s3://<S3_BUCKET>/<path>` when a relative path is missing (`.env` is loaded automatically, so a typo could otherwise target a bucket).

Rollup holds one output part and one input batch instead of the whole target partition. The byte target limits the encoded output part; active row groups have a separate 32 MiB estimated-memory budget. A memory-bound row group is flushed within the same part; a new file starts when its encoded bytes reach the target. Checks occur between batches, so a wide row, Parquet page/dictionary, and codec overhead can exceed the target. `--flush-bytes 0` leaves the output part unlimited, while the row-group budget remains finite. A bounded validation pass reads all input data before the encoding pass so a damaged later file is rejected before that group writes anything. S3 reads use pinned byte ranges rather than downloading complete source objects; the two passes trade extra reads for bounded memory and early failure.

Which files rollup reads, writes, and deletes:

- Only `part-*.parquet` files below a partition finer than `--partition` are read (for `-p date`, files under `hour=`, `minute=`, or `second=` directories). Files already at the target granularity, including earlier rollup outputs, are never re-read or deleted. The same goes for files outside time partitions (`block_range=` or unpartitioned tables).
- Root artifacts (`cursor.parquet`, `partitions.parquet`, `merkle_roots.parquet`, and anything under `verify_runs/`) are skipped, so rolling up a network root is safe.
- Every run writes new, uniquely named files and never overwrites existing ones.
- With `--delete-source`, outputs are named like ingestion parts (`part-<run>-NNNNNN.parquet`), and each source file is deleted once its target partition is written. A re-run after new data arrives only rolls up the new files.
- Without `--delete-source`, outputs are named `part-rollup-<run>-NNNNNN.parquet`. They are copies of source files that are kept, so a re-run replaces the `part-rollup-*` files it wrote earlier in each target partition it rolls up, and leaves other files there alone. Because the re-run rebuilds those partitions from the source files that exist at that point, don't delete source files by hand between runs; use `--delete-source` instead.
- An in-place rollup (no `--output`) requires `--delete-source`. Keeping the sources next to their rolled-up copy would store every row twice under the same root.
- Files are only combined when they have the same columns: the same names, types, nullability, and order. A target partition with mixed schemas, such as files from two tool versions or with `--without-extended` toggled, is left untouched: nothing is written or deleted for it. The other partitions are still rolled up, and `rollup` exits non-zero with a list of the skipped partitions.

| Flag | Default | Description |
|---|---|---|
| `-o, --output` | same as source | Output path (local or S3 URI). In-place rollups require `--delete-source` |
| `-p, --partition` | `date` | Target partition interval: `hour` or `date` |
| `--compression` | `zstd` | Compression codec: zstd (level 3), zstd:<level>, snappy, gzip, none |
| `--flush-bytes` | 32 MiB | Target compressed bytes per output part, with batch/codec overhead; 0 disables size-based closure |
| `--delete-source` | `false` | Delete each source file once its target partition is written (required in place) |

### `merge` — Consolidate Part Files

Consolidates multiple small part files within each partition directory into fewer, larger files. Unlike `rollup` (which changes partition granularity), `merge` keeps the same partition layout but reduces file count. Supports local paths and explicit S3 URIs.

`merge` processes one table at a time and, within each table, one partition at a time. It reads the parts of a partition one after another in file-name order and streams their rows into new files, starting a new file at `--flush-bytes` or `--flush-rows`. Rows keep the order of the parts they came from; they are not re-sorted, so when a partition holds parts from several writers, `block_num` is not necessarily ascending across the merged file. The original parts are deleted once the merged files are written. Root artifacts (`cursor.parquet`, `partitions.parquet`, `merkle_roots.parquet`, and anything under `verify_runs/`) are skipped, so merging a network root is safe.

Parts are only merged when every part in the partition has the same columns: the same names, types, nullability, and order. Merge checks each part's footer before writing anything. A partition with mixed schemas, such as files from two tool versions or with `--without-extended` toggled, is left untouched and listed in the summary, and `merge` exits non-zero after processing the other partitions. `--dry-run` reports these partitions too.

```bash
# Merge small parts within each partition (default 32 MB target per file)
fireparq merge ./output/blocks/

# Dry run — show what would be merged without writing
fireparq merge ./output/blocks/ --dry-run

# Merge with custom file size limit
fireparq merge ./output/blocks/ --flush-bytes 536870912

# Merge with a row-based flush limit
fireparq merge ./output/blocks/ --flush-rows 100000

# Merge S3-hosted data
fireparq merge s3://my-bucket/evm/blocks/
```

The path must exist locally or be an explicit `s3://...` URI. Unlike `scan` and `inspect`, `merge` never falls back to `s3://<S3_BUCKET>/<path>` when a relative path is missing (`.env` is loaded automatically, so a typo could otherwise target a bucket).

| Flag | Default | Description |
|---|---|---|
| `--compression` | `zstd` | Compression codec: zstd (level 3), zstd:<level>, snappy, gzip, none |
| `--flush-bytes` | 32 MiB | Target compressed bytes per output file |
| `--flush-rows` | disabled | Flush merged output after this many rows |
| `--dry-run` | `false` | Show what would be merged without writing |

> **Memory note:** Merge holds the encoded output part plus an active row group with a separate 32 MiB estimated-memory budget. Input batches, Parquet pages/dictionaries and codec overhead add to this; `--flush-bytes` is an approximate output-size target, not an absolute memory limit. On S3, merge still downloads each whole source object before reading it, so the largest source part also contributes to peak memory. Rollup uses bounded source ranges instead.

Local interrupted merges recover under exclusive ownership:

- Each partition merge has a journal, `_fireparq_merge.json`, created before output. It is committed after all outputs complete and removed after source deletion. Local output is completed and synced before publication.
- Under the common local directory guard, the next run finishes a committed journal or removes an uncommitted run's outputs before retrying. The legacy local `.fireparq-merge.lock` remains for recognizing old journals.
- S3 uses the persistent bucket-wide owner. An interrupted remote run retains ownership until an operator establishes writer cessation and provider-confirmed request quiescence and explicitly releases that exact owner. The subsequent guarded merge can recover journals written under this ownership protocol. Legacy S3 journals using the old expiring lock require separately reviewed migration and are refused automatically.
- There is no timestamp takeover or best-effort conditional-write fallback. Conflicting local parent/child operations and all mutations in one S3 bucket fail immediately. See [the recovery limits and procedure](docs/audit/468-stage1-ownership.md).

> **Metadata preservation:** Both `merge` and `rollup` preserve Parquet file-level metadata (`firehose-parquet.*` keys) from the source files into the output files.

### `truncate` — Delete Parquet Files

Deletes `.parquet` files from local filesystem or S3 with optional partition filtering. Never deletes buckets or non-parquet files.

Nothing is deleted without `--yes`. Without it, `truncate` prints a summary of what matched (file count, total size, and the first 10 paths) and exits non-zero. `--dry-run` lists every matched file instead. When truncating a network root without filters, root-level artifacts such as `partitions.parquet` and `cursor.parquet` are included; the summary calls them out. You can also target a single `.parquet` file directly, such as `fireparq truncate ./unichain/partitions.parquet --yes`.

```bash
# Preview what would be deleted
fireparq truncate ./output/blocks/ --dry-run

# Delete all parquet files in a directory
fireparq truncate ./output/blocks/ --yes

# Delete one day (also matches legacy `date=15` directories)
fireparq truncate ./output/blocks/ -p "year=2026/month=01/day=15" --yes

# Delete one day in every table of a network root
fireparq truncate ./output/mainnet/ -p "year=2026/month=01/day=15" --yes

# Delete January 2026 on S3 (filters on different keys must all match)
fireparq truncate s3://bucket/evm/blocks/ -p year=2026 -p month=01 --yes

# Delete two days (filters on the same key match either one)
fireparq truncate ./output/blocks/ -p "year=2026/month=01/day=01" -p "year=2026/month=01/day=02" --yes

# Delete all minute-level partitions (key-only filter)
fireparq truncate ./output/blocks/ -p minute --yes

# Glob pattern matching
fireparq truncate s3://bucket/evm/blocks/ -p "year=2026/month=01/day=0*" --dry-run
```

Partition filters (`-p`, repeatable):

| Filter | Matches |
|---|---|
| `month=01` | Files under a `month=01` directory. On its own that is January of every year; combine it with `-p year=2026` for one month. |
| `minute` | Every value of the key (`minute=*`). |
| `year=2026/month=01/day=15` | A partition path: files whose partition directories (the `key=value` directories below the given path) start with exactly these segments, in every table under the path. |

- Filters on different keys must all match (`-p year=2026 -p month=01` is January 2026 only). Filters on the same key match either value (`-p day=01 -p day=02`). Path filters match if any of them does, and must also satisfy the single-key filters.
- Each segment may contain one `*` glob, such as `day=0*`. `day` also matches the legacy `date=DD` directories written by earlier releases.
- Filters only match partition directories, so they never select root artifacts.

The path must exist locally or be an explicit `s3://...` URI. Unlike `scan` and `inspect`, `truncate` never falls back to `s3://<S3_BUCKET>/<path>` when a relative path is missing (`.env` is loaded automatically, so a typo could otherwise target a bucket).

| Flag | Default | Description |
|---|---|---|
| `-p, --partition` | *(none)* | Partition filter (repeatable, supports globs and partition paths) |
| `--dry-run` | `false` | List every file that would be deleted without removing anything |
| `-y, --yes` | `false` | Delete the matched files. Without it, truncate prints a summary and exits non-zero |

## Failed Transaction Filtering

EVM includes failed/reverted transactions by default. The other chains exclude them unless you pass `--include-failed-transactions`. `--exclude-failed-transactions` drops them on every chain and takes precedence.

| Flag | EVM | Other chains |
|---|---|---|
| *(none)* | included, with their persistent state changes | excluded |
| `--exclude-failed-transactions` | excluded | excluded |
| `--include-failed-transactions` | deprecated, no effect (warns) | included |

Per-chain failure condition:

| Chain | Failed when |
|---|---|
| **Solana** | `meta.err` has non-empty bytes |
| **EVM** | `status != SUCCEEDED` |
| **NEAR** | `status == "Failure"` |
| **Cosmos** | `code != 0` in `TxResult` |
| **Tron** | `result != "SUCCESS"` |
| **Antelope** | Filtered by action trace status |
| **Bitcoin** | *(not applicable — Bitcoin has no failed txs)* |

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

Three tables hold block and transaction data that is not a column of `blocks` or `transactions`. They are written at both detail levels, including with `--without-extended`. Their rows follow their transaction: they are written for failed transactions too, and dropped with `--exclude-failed-transactions`.

| Table | One row per | Columns |
|---|---|---|
| `withdrawals` | beacon-chain withdrawal in the block (Shanghai and later) | `block_number`, `index` (the global withdrawal index), `validator_index`, `address`, `amount_gwei` (`UInt64`, in gwei, not wei) |
| `access_lists` | entry of a transaction's access list (EIP-2930) | `block_number`, `tx_hash`, `tx_index`, `access_index` (position in the list), `address`, `storage_keys` (list of bytes, may be empty) |
| `set_code_authorizations` | authorization of a `SET_CODE` transaction (EIP-7702) | `block_number`, `tx_hash`, `tx_index`, `authorization_index` (position in the list), `chain_id` (decimal string; `0` allows any chain), `address` (delegation target), `nonce`, `v`, `r`, `s`, `authority` (recovered signer), `discarded` |

- A withdrawal also appears in `system_balance_changes` with reason `WITHDRAWAL`, in wei and without the validator. `sum(amount_gwei) * 1e9` per block and address equals the summed balance delta there.
- `authority` is `NULL` when it can't be recovered from the signature, and those authorizations are `discarded`. `address` is `NULL` on the few testnet blocks where Firehose did not record it.
- `discarded = true` means the chain skipped the authorization as invalid. Accepted authorizations take effect even when the transaction fails (see failed transactions above).

### EVM: header, signature, blob and ordinal columns

These columns hold Firehose fields as they are, with bytes in the output encoding and big integers as decimal strings like the other value columns. Fields introduced by a fork are `NULL` in blocks and transactions from before it.

| Table | Column | Type | Notes |
|---|---|---|---|
| `blocks` | `uncle_hash`, `logs_bloom` | bytes | |
| `blocks` | `withdrawals_root` | bytes, nullable | Shanghai |
| `blocks` | `blob_gas_used`, `excess_blob_gas` | `UInt64`, nullable | Cancun (EIP-4844) |
| `blocks` | `parent_beacon_root` | bytes, nullable | Cancun (EIP-4788) |
| `blocks` | `requests_hash` | bytes, nullable | Prague (EIP-7685) |
| `transactions` | `v`, `r`, `s` | bytes | signature |
| `transactions` | `return_data` | bytes | |
| `transactions` | `logs_bloom` | bytes, nullable | from the receipt; `NULL` without a receipt |
| `transactions` | `blob_gas`, `blob_gas_fee_cap` | `UInt64` / decimal `Utf8`, nullable | blob transactions only |
| `transactions` | `blob_hashes` | list of bytes | empty for non-blob transactions |
| `transactions` | `blob_gas_used`, `blob_gas_price` | `UInt64` / decimal `Utf8`, nullable | from the receipt, blob transactions only |
| `transactions` | `begin_ordinal`, `end_ordinal` | `UInt64` | execution-order range of the transaction in the block |
| `calls`, `system_calls` | `failure_reason` | `Utf8`, nullable | `NULL` when the call did not fail |
| `calls`, `system_calls` | `address_delegates_to` | bytes, nullable | EIP-7702 delegation target of the called account |
| `calls`, `system_calls` | `begin_ordinal`, `end_ordinal` | `UInt64` | execution-order range of the call |
| `logs` | `ordinal` | `UInt64` | execution order in the block |

The new columns come after the existing ones in each table. Ordinals are unique within a block, so `(block_number, ordinal)` orders every log, call and state change of a block. They are not reliable for anything inside a reverted call.

## Tron Contracts, Receipts and Internal Values

`contracts` retains every source contract with `transaction_index`, `tx_hash`,
`contract_index`, enum label/number, permission ID and raw Binary Any payload.
TransferContract, TransferAssetContract and TriggerSmartContract expose typed
owner/recipient/amount or target/data/call-value fields. Unsupported types keep
their raw payload with null decoded fields. `transactions.contract_type` remains
the first-contract projection and is null when the contract list is empty.

`transactions` includes nullable `receipt_*` energy/net fees, usage and result,
receipt `contract_address`, and Binary `res_message`. Missing receipts are null;
present zero/empty values remain values. `internal_call_values` retains each
ordered source `(call_value, token_id)` pair, including repeated or empty token
IDs, joined by block identity, transaction index/hash and `internal_index`.

`transaction_index` and `logs.block_log_index` count original source positions,
including transactions omitted by failed filtering. Existing `logs.log_index`
remains per transaction. Included failed effects remain source records.

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
the context; a missing column is not `false`. Strict maintenance and protected
output bindings refuse mixed old/new schemas. An explicit conversion must write
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
depending on these fields. The existing strict-schema maintenance commands
refuse to merge or roll up mixed old/new instruction schemas.

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

Each Beacon table gets rows from the fork that introduced its data. Blocks from earlier forks add no rows to it, so a range from before that fork writes no file for the table.

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
| `firehose_parquet_flushes_total` | Counter | `trigger` | Flush count by trigger type |
| `firehose_parquet_buffer_estimated_bytes` | Gauge | — | Writer-owned buffers, estimated compressed bytes |
| `firehose_parquet_buffer_rows` | Gauge | `table` | Writer-owned rows, including failed/unattempted tables |
| `firehose_parquet_mapper_buffer_rows` | Gauge | — | Mapper-owned rows summed across tables |
| `firehose_parquet_mapper_largest_table_estimated_bytes` | Gauge | — | Largest mapper table estimate, used to predict compressed file size |
| `firehose_parquet_mapper_buffer_estimated_bytes` | Gauge | — | Summed logical mapper estimates used by the memory trigger; not RSS |
| `firehose_parquet_bootstrap_buffered_blocks` | Gauge | — | Raw blocks awaiting the initial timestamp anchor |
| `firehose_parquet_bootstrap_buffered_bytes` | Gauge | — | Raw protobuf bytes awaiting that anchor |
| `firehose_parquet_cursor_saves_total` | Counter | — | Cursor persistence count |
| `firehose_parquet_cursor_save_failures_total` | Counter | — | Failed cursor save attempts, including retries |
| `firehose_parquet_cursor_last_success_timestamp_seconds` | Gauge | — | Unix time of the last successful cursor save in this process; 0 before the first save |
| `firehose_parquet_cursor_last_block_num` | Gauge | — | Block number from the loaded cursor, then the last successful save; 0 when neither exists |
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

`firehose-parquet.bytes_encoding` and `firehose-parquet.block_id_encoding` describe the emitted output contract, not just the upstream Firehose endpoint. See [Output Encoding by Block Type](#output-encoding-by-block-type) for the operator-facing defaults by supported chain/profile.

For Solana time-based partitions, the `firehose-parquet.synthetic_*` metadata
keys mark routing as using a synthetic last-known timestamp anchor while
canonical `timestamp` / `date` remain chain-sourced and nullable when
`block_time` is missing.

Endpoint `block_id_encoding` remains a fallback only when the chain does not resolve to a known block-type/profile contract.

### Reading Metadata

```python
import pyarrow.parquet as pq

meta = pq.read_metadata("output/blocks/year=2026/month=01/day=15/part-000001.parquet")
for i in range(meta.metadata.count()):
    key = meta.metadata.keys()[i]
    if key.startswith("firehose-parquet."):
        print(f"{key} = {meta.metadata.values()[i]}")
```

```sql
-- DuckDB
SELECT key, value
FROM parquet_kv_metadata('output/blocks/year=2026/month=01/day=15/part-000001.parquet')
WHERE key LIKE 'firehose-parquet.%';
```

## Output Directory Layout

```
<chain_name>/
├── cursor.parquet
├── blocks/
│   ├── year=2026/month=02/day=25/
│   │   ├── part-000001.parquet
│   │   └── part-000002.parquet
│   └── year=2026/month=02/day=26/
│       └── part-000001.parquet
├── transactions/
│   └── ...
└── logs/
    └── ...
```

Time-based partitioning writes Hive-style directories: `--partition date` writes `year=YYYY/month=MM/day=DD/`, and `hour`, `minute` and `second` add `hour=HH/`, `minute=MM/` and `second=SS/` below it. `--partition block_range` writes `block_range=<start>-<stop>/`.

The day-of-month key is `day=`. Earlier releases wrote `date=DD`, which collides with the canonical `date` column under Hive partitioning: DuckDB's default `hive_partitioning` replaced the `date` DATE values with the day number, and Polars' `hive_partitioning=True` failed to parse `26` as a date. `rollup` and `truncate` still accept legacy `date=` directories.

Query a dataset with its partition columns, e.g. in DuckDB:

```sql
SELECT date, day, count(*)
FROM read_parquet('output/mainnet/blocks/**/*.parquet')  -- hive_partitioning is on by default
GROUP BY ALL;
-- date: DATE (the canonical data column); year/month/day/hour: partition columns
```

## Canonical Identity Columns

Every table across all chains includes these 7 columns (from Firehose `BlockMetadata`):

| Column | Type | Description |
|---|---|---|
| `block_num` | UInt64 | Block number |
| `block_id` | Utf8 | Block ID (format depends on block type; see [Output Encoding by Block Type](#output-encoding-by-block-type)) |
| `parent_num` | UInt64 | Parent block number |
| `parent_id` | Utf8 | Parent block ID |
| `lib_num` | UInt64 | Last irreversible block number |
| `timestamp` | Timestamp(Millisecond, UTC) | Block time. Parquet logical type `TIMESTAMP(MILLIS, isAdjustedToUTC=true)`; keeps sub-second precision where the chain has it (e.g. Antelope's 500 ms blocks) |
| `date` | Date32 | UTC day of the block time |

Time-based partition directories (`year=`/`month=`/`day=`/`hour=`/…) and `date` are derived from the whole-second block time, so a block at `12:00:00.500` lands in the same `second=00` partition as one at `12:00:00.000`. Solana tables keep `timestamp` and `date` null when `block_time` is missing.

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

The action fields `transaction_id`, `trace_block_num`, `producer_block_id`, and
`block_time` are deprecated for ordinary joins and routing; prefer `tx_hash` and
canonical block identity/time. They remain verbatim action metadata, which can
be missing or differ from canonical values. No removal is scheduled. Existing
action JSON, nulls and enum labels remain unchanged.

Use a new dataset or rebuild older ranges to populate the added columns. Schema
union makes them null in old files; strict merge/rollup requires explicit schema
reconciliation. See [the implementation and live comparison](docs/audit/508-antelope-db-joins.md).

## NEAR: Transactions, Receipts, Actions and Logs

A NEAR transaction's own outcome records its inclusion and conversion into a receipt. Contract calls run when action receipts execute, often in later blocks and on other shards. `transactions` and `receipts` carry the keys to follow that chain:

| Table | Column | Type | Meaning |
|---|---|---|---|
| `transactions` | `transaction_index` | `UInt32` | Position in the block: chunks in shard order, then each chunk's transactions. Failed transactions left out by the filter keep their index. |
| `transactions` | `receipt_ids` | list of bytes | The outcome's `receipt_ids`. |
| `transactions` | `converted_into_receipt_id` | bytes, nullable | The receipt the transaction was converted into. Joins `receipts.receipt_id`. Null when the outcome has no receipt. |
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

Both tables cover every receipt in `receipts`, including failed ones. Join `receipts` on `receipt_id` to check `status`:

```sql
-- NEP-141 events of receipts that did not fail
WITH events AS (
  SELECT receipt_id, block_num, executor_id AS token,
         TRY_CAST(substr(log, 12) AS JSON) AS event  -- the text after 'EVENT_JSON:'
  FROM read_parquet('output/near-mainnet/execution_logs/**/*.parquet')
  WHERE log LIKE 'EVENT_JSON:%'
)
SELECT e.block_num, e.token, e.event->>'event' AS event, e.event->'data' AS data
FROM events AS e
JOIN read_parquet('output/near-mainnet/receipts/**/*.parquet') AS r USING (receipt_id)
WHERE e.event->>'standard' = 'nep141'
  AND r.status <> 'Failure';
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

These added columns and tables require a fresh dataset or an explicit rebuild; protected ingestion refuses to resume an incompatible schema inventory. See the [bounded public-source comparison and its coverage limits](docs/audit/506-near-public-qualification.md).

## Environment Variables

CLI flags can also be set via environment variables. Copy `.env.example` to `.env`:

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
│       ├── artifacts.rs                    # Reserved dataset artifact names (cursor, partitions, verify)
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

## Parquet Lookup Metadata

Ingestion, merge and rollup write bounded Bloom filters for selected scalar
hash, signature and account/address columns. Readers that support these filters
can skip row groups for equality lookups; positive matches still require row
filtering. Filters do not answer `IS NULL` predicates. Row groups contain at most
65,536 rows, with at most eight filters per group. This changes physical layout,
not table schemas or row order. Dictionary encoding retains its existing policy.
The retained-data benchmark measured 0.74–1.92% larger files and faster missing-key
lookups; readers without Bloom pruning may only see the storage overhead.

Complete ingestion parts declare ascending `block_num` only when every observed
height proves that order. Streaming maintenance omits that assertion. Neither
path sorts or reconstructs reversible-chain history.

Use `--compression zstd:6` to select an explicit Zstandard level; `zstd` and
`zstd:3` retain level 3. Zero is rejected as ambiguous. Explicit non-default
levels participate in protected transaction identity; recover any pending work
with a supporting version before downgrading. See the
[lookup properties and measurements](docs/audit/519-parquet-lookup-properties.md).
