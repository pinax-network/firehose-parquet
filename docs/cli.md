# CLI reference

`fireparq build` streams Firehose blocks into the Delta tables. The other
[subcommands](#subcommands) are `maintenance`, `hypercore-reference`,
`validate`, `inspect`, `recovery` and `completions`; DuckDB and Polars read the tables
([Reading the tables](reading-tables.md)). The global flags
`--log-level` (`LOG_LEVEL`, default `info`), `--verbose` (`VERBOSE`) and
`--env-file` (`FIREPARQ_ENV_FILE`) apply to every command.
`fireparq --help` and `fireparq <command> --help` print the full help.

Non-final output (`--final-blocks-only=false`) is described in [Non-final streams and reorgs](non-final-streams.md), and Firehose credentials in [Authentication](authentication.md).

## Common ingestion flags

| Area | Common flags |
|---|---|
| Connection | `--network <NETWORK>` or `--endpoint <ENDPOINT>` |
| Range | `--start-block <START_BLOCK>` (default: the endpoint's first streamable block, or the network's data origin when later, such as HyperCore's 846903317; [start and stop blocks](cursor-and-resume.md#start-and-stop-blocks)), `--stop-block <STOP_BLOCK>` (exclusive; omit it for live mode) |
| Resume | Rerun the same original range; output authority selects progress and repairs the bound optional cursor mirror (`--cursor`, default `_fireparq/cursor.parquet` in the dataset root, or `none`) |
| Output | `--output <OUTPUT>` (`OUTPUT`, default `.`; an explicit `s3://bucket/prefix` for S3): the dataset root, used exactly as given, with an opt-in `{chain}` placeholder for the endpoint's chain name, for example `--output 's3://datasets/{chain}'` ([dataset layout](output-layout.md#output-directory-layout)); every table is a Delta table at `<table>/`, with its data files in `<table>/date=YYYY-MM-DD/`; `--compression <COMPRESSION>` (default `zstd`) |
| Chain | `--block-type <BLOCK_TYPE>` (default `auto`), plus chain-specific toggles like `--without-extended` or `--without-votes` only when needed |
| Runtime | `--final-blocks-only[=true\|false]` (default `true`), `--dry-run` (decode and map without writing), `--flush-bytes <FLUSH_BYTES>` (compressed file target, `0` disables), `--flush-memory-bytes <FLUSH_MEMORY_BYTES>` (summed mapper estimate), optional `--flush-rows` / `--flush-blocks` / `--flush-interval-secs` / `--flush-idle-secs` (`0` disables rows, interval and idle; the interval applies at the chain head only, [details](#flush-interval-and-catch-up)) |
| Flush concurrency | `--flush-encode-concurrency` (`FLUSH_ENCODE_CONCURRENCY`, default `2`), `--flush-publish-concurrency` (`FLUSH_PUBLISH_CONCURRENCY`, default `4`, also the local I/O threads), `--flush-inflight-bytes` (`FLUSH_INFLIGHT_BYTES`, default 256 MiB): bounded table work inside each flush ([details](#flush-concurrency)) |

## Network aliases

`fireparq` can resolve a checked-in set of built-in Firehose network names instead of requiring `--endpoint` every time.

Examples:

- `mainnet` → `https://eth.firehose.pinax.network:443`
- `solana-mainnet-beta` → `https://solana.firehose.pinax.network:443`
- `tron` → `https://mainnet.tron.streamingfast.io:443`
- `tron-evm` → `https://mainnet-evm.tron.streamingfast.io:443`
- `hypercore` → `https://hypercore.firehose.pinax.network:443`

Provider hostnames do not always mirror the network name exactly. For example, `matic` resolves to the provider hostname `polygon.firehose.pinax.network`. Run `fireparq build --help` to list every built-in name.

Aliases come from two sources: The Graph networks registry, and a short reviewed list of Pinax-served networks the registry does not list yet (`PINAX_NETWORKS`, currently `hypercore`; an entry is dropped once the registry gives the network an alias). Registry aliases use the Pinax endpoint the registry lists. `near-mainnet`, `near-testnet`, `tron`, and `tron-evm` use StreamingFast endpoints because Pinax no longer serves them; those need a credential StreamingFast accepts, such as a The Graph Market API token in `STREAMINGFAST_API_TOKEN`. See [Network registry integration](network-registry-integration.md) for the provider policy, the internal list and the weekly endpoint check.

Resolution precedence:

1. `--endpoint` or `ENDPOINT`
2. `--network` with `FIREHOSE_ENDPOINT_*` override lookup
3. `--network` built-in default endpoint

Per-network env overrides normalize network names by uppercasing and converting non-alphanumeric separators to underscores.

A name outside the built-in list is rejected during argument parsing, and startup fails early if the resolved endpoint is unavailable or unhealthy.

`build` requires EndpointInfo with a nonempty chain name before resolving output or cursor paths. Transient Info failures get three attempts with bounded backoff; exhausted retries, authentication errors, or a server without the Info RPC stop startup. `--network`, `--block-type`, and `--cursor-override` do not bypass this requirement. This prevents a temporary metadata failure from changing the output root or hiding the existing cursor. Protected ingestion resolves its mapper before recovery; unknown custom chain metadata requires an explicit `--block-type`. See [the implementation record](audit/467-endpoint-info.md) for retry limits and validation.

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

## Environment variables

Every `build` flag can also be set with the environment variable that
`fireparq build --help` names. [`.env.example`](../.env.example) lists them
all (a test keeps it in sync with the CLI); copy it to `.env` in the directory
you run `fireparq` from. `fireparq maintenance` takes its own
variables instead ([Delta maintenance](delta-maintenance.md)).
`fireparq hypercore-reference` reads `HLREF_*`, `HL_INFO_URL` and, like
`build`, `AWS_ENDPOINT_URL_S3` and the AWS credentials, through its flags; an
empty value is unset for it ([HyperLiquid reference tables](hypercore-reference.md)).
Since #617:

- `fireparq` loads `.env` from the **current working directory only**. Parent
  directories are never searched, so a run started in a subdirectory or a git
  worktree below a checkout does not inherit that checkout's production settings.
- `--env-file <PATH>` (or `FIREPARQ_ENV_FILE` in the process environment) loads
  exactly that file instead; it must exist, and `./.env` is then ignored.
- Process environment variables and CLI flags win over the file.
- Startup names the loaded file and the variables it supplied, never their
  values: an INFO `loaded env file` log line for `build`, and one stderr line
  for the other commands except `completions`. A malformed file is an error
  that never echoes the offending line.

## Advanced recovery / override behavior

These flags are intended for recovery-heavy or operator-managed deployments
rather than the default workflow:

| Flag | Use when |
|---|---|
| `--cursor-override` | Read-only `--dry-run` only: ignore legacy cursor defaults or an unreadable cursor. A real `build` rejects it, even at a new root; protected output never rewinds, so use a new empty root for changed semantics |
| `--stream-idle-timeout-secs <N>` | Supervising long-lived pipelines that should self-reconnect after a silent stream stall (default 120, or 93600 for `--block-type sec`; `0` disables and relies on HTTP/2 keepalive). On slow chains such as Bitcoin (~600 s blocks), set it above the block time to avoid a reconnect every 120 s. An idle reconnect is not counted as a failure. |
| `--reconnect-stall-timeout-secs <N>` | Fail fast when reconnect loops should hand control back to an external supervisor (default 900; `0` disables). The timer starts at the first failed attempt and is reset only when a stream message arrives, not when a connection or RPC succeeds. |

### Receive transport

`build` uses 16 MiB HTTP/2 stream and connection receive windows and accepts
plain, gzip, or zstd replies. The server selects the response encoding;
requests remain uncompressed. Parquet `--compression` is independent of
transport compression.

| Flag / environment | Behavior |
|---|---|
| `--grpc-window-bytes` / `GRPC_WINDOW_BYTES` | Initial stream and connection receive window, default `16777216`. `0` restores the underlying library defaults. Larger windows allow more data in flight and can increase buffering. |
| `--grpc-adaptive-window[=true\|false]` / `GRPC_ADAPTIVE_WINDOW` | Opt into automatic window tuning; default false. When true, it overrides `--grpc-window-bytes`. |
| `--grpc-max-message-bytes` / `GRPC_MAX_MESSAGE_BYTES` | Maximum encoded or decompressed protobuf response bytes, default `134217728` (128 MiB), or `536870912` (512 MiB) for the ingestion stream of `--block-type sec`, whose 13F and N-PX deadline-day windows exceed 128 MiB. Values must be positive and fit UInt32. Applies to Info and the ingestion stream. |

The message limit bounds one response, not process memory: an oversized
response fails with an error, and a higher limit permits larger allocations.
The 16 MiB window default improved a local transport benchmark at zero and
50 ms of added round-trip latency; it is not an end-to-end ingestion guarantee
([measurements and tradeoffs](audit/517-grpc-transport.md)).

### Connection errors

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

## Advanced S3 / deployment knobs

Most operators point `--output` at a local path or an explicit
`s3://bucket/prefix`. S3 output or an S3 cursor for `build` requires both an
access key ID and a secret access key, from the flags or `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY`; `build` never falls back to profile or
instance-metadata credentials. The remaining flags are only needed for custom
deployment environments:

| Flag (environment) | Purpose |
|---|---|
| `--s3-bucket` (`S3_BUCKET`) | Optional check that an explicit `s3://` output uses this bucket; a relative output is then rejected, never expanded |
| `--aws-access-key-id`, `--aws-secret-access-key`, `--aws-session-token`, `--aws-region` (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_REGION`) | S3 credentials and region |
| `--aws-endpoint-url` (`AWS_ENDPOINT_URL_S3`) | Target S3-compatible object stores; `build` output needs an HTTPS endpoint ([S3-aware cursor](cursor-and-resume.md#s3-aware-cursor)) |
| `--cache-control` (`CACHE_CONTROL`) | Cache-Control header for S3 uploads, default `public, max-age=31536000, immutable`; an empty string sends no header |
| `--metrics-port` (`METRICS_PORT`), `--metrics-stale-after-secs` (`METRICS_STALE_AFTER_SECS`) | Serve `/metrics`, `/health` and `/ready`; `/ready` returns 503 after N seconds without a stream message, default 120 ([metrics](metrics.md)) |

### S3 output in Kubernetes

Kubernetes expands `$(VAR)` references to variables defined earlier in the same
container's `env` list, so a bucket per network
([single-network buckets](output-layout.md#single-network-buckets)) can take
its output from a ConfigMap:

```yaml
env:
  - name: BUCKET_NAME
    valueFrom:
      configMapKeyRef: { name: fireparq, key: bucket }
  - name: OUTPUT
    value: s3://$(BUCKET_NAME)
```

Use `s3://$(BUCKET_NAME)/{chain}` to share one bucket between networks. A
relative `OUTPUT` with `S3_BUCKET` set exits with an error before writing
anything. Check the `resolved write destinations` log line after a rollout.
Deployments from v0.7 follow the
[v1.0.0 upgrade guide](releases/v1.0.0.md#upgrade-guide-read-first).

## Flushes

### Flush size and memory

Each `build` flush commits its nonempty tables together before output
authority and the cursor mirror advance. `--flush-bytes` is a **target
compressed size for the largest table's file**, default 32 MiB. `build` starts
with a conservative calibration flush, then learns the compressed-to-mapper-size
ratio from the committed file sizes. The ratio resets on restart; dry runs,
which commit no files, keep the conservative estimate. A file can overshoot by
one mapped block, and smaller tables produce smaller files.

`build` also flushes when the **sum of all mapper table estimates** reaches
`--flush-memory-bytes` (default 256 MiB, positive). This remains active with
`--flush-bytes 0`, which disables only the compressed-size target. The estimate
counts populated mapper values, **not process RSS**: decoder and bootstrap
payloads, reserved allocator capacity, materialized Arrow batches, and the active
Parquet encoder/output need additional memory. A single block can exceed the
threshold before the next check. Lower it to bound memory;
[Recommended deployment](deployment.md) raises it on fast chains.

`--flush-rows`, `--flush-blocks`, `--flush-interval-secs` (at the chain head
only, see below), UTC day changes, the memory threshold and clean end of input
can all force files below the size target. `--flush-rows 0` and
`--flush-interval-secs 0` disable those triggers, like `--flush-bytes 0`;
`--flush-blocks` and `--flush-memory-bytes` must be positive. Highly
compressible data may never reach 32 MiB before the memory threshold;
increasing the file target does not bypass that threshold.

On graceful shutdown or failure, unflushed mapper data is not written and
authority does not advance; the next run replays it
([graceful shutdown](cursor-and-resume.md#graceful-shutdown)).

### Flush concurrency

Within one flush, tables are encoded and published concurrently but under
explicit bounds. `--flush-encode-concurrency` (default 2, 1-64) caps Parquet
encoders running at once; `--flush-publish-concurrency` (default 4, 1-64) caps
part publications, and for local output also the threads that stage, publish and
verify files. `--flush-inflight-bytes` (default 256 MiB) caps encoded parts that
are encoding, staged or publishing: memory for local output, private disk spool
for S3 output (whose readback verification can hold one more copy per
publication). An encoder is admitted only when its initial reservation fits and
grows it while writing; if the budget cannot grow it, the part is encoded again
alone later. One part larger than the whole budget runs alone and logs an
overshoot warning. Each active encoder additionally holds its table's encoder
working memory, and mapper batches stay allocated until the flush commits.

Concurrency does not change the transaction contract: one Writing journal at a
time, receipts journaled one at a time with each part's exact receipt durable
before that part publishes, and authority and the mirror advance only after
every part has published and every final verified. The first error stops new
work, waits for work already started, and leaves the journal for recovery.
Output bytes and part names do not depend on these settings. `1`/`1` still lets
the next table encode while the previous one publishes; `--flush-inflight-bytes 1`
makes table work strictly one part at a time. The committed-flush log reports
`commit_ms` and the observed `peak_encoders`, `peak_publications` and
`peak_inflight_bytes`. See
[the qualification and benchmark](audit/516-bounded-flush-concurrency.md).

### Flush interval and catch-up

`--flush-interval-secs` bounds how long rows wait for a commit while `build`
follows the chain head. While it replays history instead (a first start, a
restart after an outage, or a `--stop-block` backfill far behind the head),
nobody is waiting for those rows, so the interval is suspended. The size
triggers (`--flush-bytes`, `--flush-memory-bytes`), `--flush-rows`,
`--flush-blocks`, day boundaries and the end of the stream still flush, and
files reach the `--flush-bytes` target instead of holding an interval's worth
of historical blocks. No flag controls this: `build` measures its pace from the
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
[the implementation record](audit/659-adaptive-flush.md).

### Flush when the stream goes quiet

The interval and size triggers are checked when a block arrives, so rows
mapped before a long silence wait for the next block. `--flush-idle-secs <N>`
/ `FLUSH_IDLE_SECS` commits them once the stream has delivered no message for
`N` seconds, at any pace (`trigger="idle"`): a stream with nothing to send is
at the head of what its server has. Use it for feeds that arrive in bursts.
It is off by default, and 60 for `--block-type sec`, whose feed is one burst
of 144 windows per EDGAR feed day followed by about a day of silence
([SEC notes](chains/sec.md)). One quiet period flushes at most once; the next
message starts a new one. It is not attempted while `build` reconnects: it
waits for the new stream and then flushes at once if the stream has been quiet
long enough.

### Day boundaries and missing timestamps

When a block starts a new UTC day (a new `date` partition), the mapper flush of
the previous day is committed first, with the same writer outcome logs, so a
part never spans two days.

When the first streamable blocks are missing timestamp metadata, fireparq keeps
those leading bootstrap blocks and synthesizes their timestamps from the first
later block that has one.

For Solana, a missing `block_time` keeps canonical `timestamp` null, and the
row's `date` is its routing day. Routing uses the last known timestamp only,
seeded from the Solana first-streamable anchor (`2020-03-16 14:29:00 UTC`) for
the initial span and updated whenever a real block timestamp is observed.

## Family defaults

`build --block-type sec` (or `auto` when the endpoint's EndpointInfo names the
chain `sec`) changes four defaults, each only when neither the flag nor its
environment variable is set, and logs `applied the block family's build
defaults to settings left unset` with the values:
`--grpc-max-message-bytes 536870912`, `--flush-idle-secs 60`,
`--stream-idle-timeout-secs 93600` (26 hours) and
`--metrics-stale-after-secs 129600` (36 hours). They follow the family known
before streaming: an `auto` build that recognizes SEC only from the first
block's type URL keeps the generic defaults, as does every other family.

## Subcommands

`scan` was removed in v1.0.0: DuckDB and Polars read the tables, and a
table's files, rows, bytes and days come from its Delta log (see
[Reading the tables](reading-tables.md)).

### `maintenance` — compact, VACUUM and checkpoint

The Delta maintenance job, run on a schedule beside `build`: for each table it
compacts closed dates in the writer's row order, runs VACUUM, checkpoints and
cleans up the log. It takes only the global flags; its settings are
environment variables, listed by `fireparq maintenance --help` (`LAKE_ROOT` or
`LAKE_BUCKET`, `LAKE_TABLES`, `VACUUM`, `FULL_VACUUM`, ...). It reads the S3
endpoint from `S3_ENDPOINT`, not `AWS_ENDPOINT_URL_S3`. It prints one JSON
object per line and exits 0 when every table was maintained or skipped, 1 when
a table failed, and 2 for a configuration error.

```bash
DRY_RUN=1 LAKE_ROOT=./output/mainnet LAKE_TABLES=blocks,transactions fireparq maintenance
```

[Delta maintenance](delta-maintenance.md) has every setting, the schedule and
the example CronJobs.

### `hypercore-reference` — HyperLiquid reference tables

Snapshots HyperLiquid's `/info` (token and pair symbols, perp dexes and
assets, HIP-4 outcomes and questions, funding history) into the append-only
`hl_*` Delta tables, in a root of their own beside a HyperCore lake, never a
fireparq dataset root. Three jobs run on a schedule, each the only writer of
its tables; `maintain` and `verify` are run by hand:

```
fireparq hypercore-reference [OPTIONS] <outcomes|meta|funding|maintain|verify> [JOB OPTIONS]
```

Options come before the job and job options after it. The options are
`--root` (`HLREF_ROOT`, required: `s3://<bucket>[/<prefix>]` or a local
directory), `--info-url` (`HL_INFO_URL`, default
`https://api.hyperliquid.xyz/info`; only its scheme and host are ever shown),
`--weight-per-minute` (`HLREF_WEIGHT_PER_MINUTE`, default 400, at least 20),
`--time-budget-seconds` (`HLREF_TIME_BUDGET_SECONDS`, default 240),
`--no-maintenance` and the `AWS / S3` flags; the S3 endpoint is
`AWS_ENDPOINT_URL_S3` (`https://` only), and `S3_ENDPOINT` or
`AWS_ENDPOINT_URL` with another value is refused. It prints one JSON object
per line and exits 0 when done (an exhausted time budget included), 1 when a
request, commit or invariant failed, and 2 for a configuration error.

```bash
HLREF_ROOT=s3://hypercore-ref fireparq hypercore-reference outcomes
fireparq hypercore-reference --root ./reference funding --coins BTC,xyz:XYZ100 \
  --funding-start 2026-03-01 --funding-end 2026-03-08
fireparq hypercore-reference --root ./reference verify
```

[HyperLiquid reference tables](hypercore-reference.md) has the tables, the
views and joins with the lake, every setting and event, and the example
CronJobs.

### `inspect` — display file metadata

Displays the metadata of a single Parquet file, such as a table's data file, a Delta checkpoint or the cursor mirror; it refuses a directory. It takes a local path, a shorthand S3 key via `S3_BUCKET`, or an explicit S3 URI.

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

The output has these sections (`--schema-only` keeps only the schema, `--json` emits JSON):

| Section | Details |
|---|---|
| **File info** | Total rows, row groups, columns, file size, created_by, Parquet version |
| **File metadata** | All key-value pairs stored in the Parquet footer |
| **Schema** | Physical types, logical types (e.g. String, Timestamp), repetition levels, explicit `nullable=` output, nested groups |
| **Row groups** | Per-group row count, compressed/uncompressed size, compression ratio |
| **Column details** | Per-column encoding, compression codec, compressed/uncompressed size, ratio |

### `validate` — check block continuity

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

### `recovery` — ownership and recovery state

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
fireparq recovery release s3://ethereum-mainnet \
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
provider evidence. Like `build`, `validate` and `inspect`, `recovery` reads its
S3-compatible endpoint from `--aws-endpoint-url` / `AWS_ENDPOINT_URL_S3`, and
falls back to `AWS_ENDPOINT_URL` when that is unset. See the
[ownership and recovery runbook](audit/468-stage1-ownership.md).

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
