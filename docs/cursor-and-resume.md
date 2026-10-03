# Cursor and resume

`fireparq build` stores an authoritative checkpoint and an all-table transaction
journal under `<output>/.fireparq-ingest/`, in the dataset root that `--output`
names ([dataset layout](output-layout.md#output-directory-layout)). Every flush either recovers
its complete committed output or removes its owned uncommitted parts before
replay. The optional `_fireparq/cursor.parquet` file mirrors this authority;
deleting the mirror cannot rewind ingestion. These controls contain opaque source cursors
and should receive the same access restrictions as the cursor file.

Existing datasets without this authority are not adopted automatically. Rebuild
into a new empty output root, with an absent cursor mirror. Keep legacy datasets available
for read-only tools. See the
[transaction and migration contract](audit/468-ingestion-runtime.md).

## Startup cost

A restart costs the same however large the dataset has grown (#655). When
`--output` already holds a dataset, `build` reads:

- its control records: the S3 owner record, `.fireparq-ingest/` (the authority
  and a pending transaction) and the `_fireparq/cursor.parquet` mirror;
- each table's Delta log from its last checkpoint: `_delta_log/_last_checkpoint`,
  the checkpoint, and the commits after it (one LIST of `_delta_log/` and one
  GET per commit). The hourly [maintenance](delta-maintenance.md) checkpoint
  keeps that tail short; `firehose_parquet_delta_log_tail_commits` reports it.

It lists no data objects and walks no data directory. The only other requests
are one LIST of a control prefix (`<ancestor>/.fireparq-ingest/`) per directory
above the dataset root, which is none for a dataset at the bucket root, and
after a crash the GETs of the pending transaction's own parts.

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
counts. See [the implementation record](audit/655-resume-cost.md).

## How it works

1. **Receive and map** — assign source-event order before filtering or timestamp
   buffering. Every table belongs to one contiguous accepted event window;
   filtered events can advance a zero-row checkpoint.
2. **Prepare and publish** — validate the whole table inventory, save the pending
   transaction, and publish complete parts with deterministic owned names.
   Record each file's size, checksum and schema before publication.
3. **Commit and mirror** — after verifying every part, record the transaction's
   commit, commit each table's part to its Delta log with a `txn` marker (the
   other tables first, `blocks` last), advance output authority, repair the
   cursor mirror, and clear pending.
4. **Recover before streaming** — roll back a Writing transaction or finish a
   Committed transaction before opening Firehose Blocks: commit it to each
   Delta table whose `txn` lacks it, `blocks` last, then advance authority.
   Never infer progress from the greatest block number, a filename, or an
   external cursor alone.

## Crash recovery

A crash at any point leaves a journal under `.fireparq-ingest/` that the next
`build`, or `fireparq recovery recover <root>` offline, finishes before any
Blocks request:

- **Before the journal is Committed** the transaction is rolled back: its own
  parts are removed, no log references them, and its window is replayed under
  the same deterministic part names.
- **After it is Committed** the transaction is rolled forward exactly once.
  Every start first reads each table's `txn` and refuses a log ahead of the
  authority (an authority restored from an older copy, or another writer using
  the stream's `appId`) before it changes anything. The transaction is then
  committed only to the tables whose `txn` lacks it, `blocks` last, after only
  those tables' parts are verified against the journal, and authority
  advances. A roll-forward that is itself interrupted finishes the same way.
- Tables that already hold the transaction are not read again, so a restart
  works after the [maintenance job](delta-maintenance.md) compacted and vacuumed
  their files. Only a full VACUUM with a retention shorter than the outage can
  delete a part that no log references yet; recovery then stops with a message
  naming the part and the tables that hold it, keeps the journal, and the
  dataset is rebuilt into a new root.
- A Delta commit whose outcome is unknown (a timeout, a lost response) does not
  keep the S3 owner: the next start reads the table's `txn` and commits only
  what did not land.

The [crash matrix](design/delta-lake.md#4-crash-matrix) lists every
interruption point and its test, and the
[recovery record](audit/643-l4-delta-recovery.md) the implementation.

## Local part publication

Protected local parts use a hidden transaction-owned `.tmp` name in the same
directory as their final `part-v1-*.parquet` name. The writer completes and syncs
the Parquet file, creates the final name with a no-clobber hard link, and syncs
directory links. Recovery verifies exact journal ownership before removing a
partial transaction or accepting a committed file. Canonical and lexical output
ancestry are both synced, preserving explicit output-root symlink aliases.

The staging names are `.fireparq-txn-<transaction>-<index>.tmp` next to each
final part; no Delta log references them. After an ordinary error (a
failed write, publish, journal update or mirror save) the build removes its own
staging names before exiting, on a best-effort basis, and leaves the journal for
recovery. If that cleanup itself fails, for example because the directory is no
longer writable, or the process is killed, a staging name can remain until the
next `build` or `fireparq recovery recover <root>` removes it from the journal plan.

This requires atomic same-directory hard links, file and directory sync, readable
directory ancestry, and macOS/Linux inode locking. Unsupported operations fail
closed. Nested symlink entries inside guarded trees are refused. External writers
that bypass ownership are unsupported.

A published part is complete, but readers see it only once its table's log
commits it. The tables commit one after another, `blocks` last, so there is no
atomic multi-table snapshot: use the
[frontier rule](reading-tables.md#consistent-reads-across-tables) for a consistent read. Do not
remove control records or another active writer's temporary files. See the
[local publication tests](audit/578-atomic-local-parquet.md) and
[transaction recovery contract](audit/468-ingestion-runtime.md).

## Cursor file format

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

## S3-aware cursor

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
definite and does not. See [qualification and limits](audit/520-bounded-s3-ingestion.md).

## Parameter validation on resume

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

## Missing or unreadable cursor

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

## Ownership

`build` and `recovery` hold common ownership over the output and an external
cursor location. Local ownership uses macOS/Linux directory locks; nested
symlinks inside mutation trees are refused. S3 ownership covers the whole bucket:
there is one owner per bucket, and a second writing command on any prefix of it
fails with `bucket ownership is held`. It requires conditional-write
support plus access to reserved control keys. Before it takes ownership, a
canary on a private probe key checks that the provider applies
`If-None-Match: *` and a correct `If-Match`, and refuses wrong and stale
versions. Ceph RGW 19.2.x compares `If-Match` literally with the ETag
without its quotes, so it refuses the quoted form that S3 returns and
that AWS S3 and MinIO expect. The canary detects this, reruns with
unquoted ETags, and uses that form for the rest of the run only when every
check passes. It then logs `s3 conditional writes: If-Match ETags sent
unquoted (provider compares them literally)`. A provider that passes
neither form fails with `conditional-write capability could not be
proven` (v1.0.1, [#678](audit/rgw-if-match-etag.md), which also
describes an opt-in check of a disposable bucket). Unresolved remote errors retain
ownership without an expiry or automatic takeover, and the next run fails until
it is released. A failed `build` releases S3 ownership on exit when every request
it sent had a definite outcome, including when its failed transaction is still
pending: the next `build` recovers that transaction before streaming. When a
PUT's outcome is unknown (timeout, lost acknowledgement, connection reset, 5xx),
it reads the key back for about 15 s: exactly the part, control record, mirror
or owner record the request writes proves the request, and the build continues
([#646](audit/646-uncertain-mutation-readback.md)). Absence is never
proof. It keeps ownership after a request that no readback proves, an ambiguous
DELETE, a 409/412, a second shutdown signal or a panic, and its error then says
why and prints the exact `recovery status` and `recovery release` commands. The
release itself is retried for about two minutes while the provider does not
answer. A Delta log commit is the exception: the next
start reads the table's `txn` to learn whether it landed, so an uncertain one
does not keep the owner. A commit is never resent, but reads of the Delta logs
(GET, HEAD and listings of log objects and checkpoints) are: up to 3 attempts
on a transport error, 408, 429 or 5xx, each retry logged as a warning
(`retrying an idempotent Delta log read after a transient error`; v1.0.2,
[#680](audit/680-delta-read-retries.md)). `recovery` keeps S3 ownership
after any error and logs the same guidance.
`fireparq recovery status <path>` reads a summary. Explicit remote release requires
the exact owner/generation and evidence that both the writer and all prior remote
requests are quiescent; stopping the process alone is insufficient. See the
[ownership and recovery runbook](audit/468-stage1-ownership.md).

### What the owner guards: the Delta tables and the maintenance job

The owner guards one fireparq writer (`build`, or `recovery recover`) and the
state only it changes: `.fireparq-ingest/` (authority and the pending
journal), `_fireparq/cursor.parquet` and its own uncommitted parts. It does not
make the Delta tables exclusive. The maintenance job (`fireparq maintenance`: OPTIMIZE,
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
[#636 record](audit/636-delta-ownership.md) has an RGW bucket policy for
both users, and the [recovery record](audit/643-l4-delta-recovery.md)
the crash cases.

`recovery recover` selected at a table, a partition or a parent directory
discovers every affected protected dataset and its external mirror before it
recovers anything.

## Cursor override and migration

`--cursor-override` cannot reset, rewind or change protected output semantics,
and a real `build` rejects it before contacting the endpoint, even at a new root.
Use a new empty output and absent mirror when changing the original range, schema
or feature flags. Legacy random-name output has no proof relating all parts to
its cursor, so this release provides no implicit adoption or override escape.
Read-only dry-run behavior can still inspect legacy cursor defaults.

## Graceful shutdown

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
S3 ownership is released on exit unless a request had an uncertain outcome
that an exact readback could not prove.
S3 recovery additionally requires explicit release after provider-confirmed
request quiescence whenever the prior owner remains retained.

Only a stream that ends cleanly (for example, by reaching `--stop-block`)
flushes the remaining buffers and saves the final cursor.

## Start and stop blocks

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
