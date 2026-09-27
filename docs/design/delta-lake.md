# Delta Lake output (#643): design, spike and plan

Status: proposed design, 2026-09-27. Nothing here changes what `fireparq` builds
today. The spike lives in [`spikes/delta-lake/`](../../spikes/delta-lake/) and
runs in CI as the `delta-spike` job. Refs #643, #636, #653, #655, #658, #659;
part of #463.

## Decisions this design follows (2026-09-27)

- Delta Lake is the default and only output format, and ships in **the v1.0.0
  launch**. There is no plain-Parquet release before it, no `--table-format`
  flag and no legacy code path. Release-note entries go in
  [`docs/releases/v1.0.0.md`](../releases/v1.0.0.md).
- Target readers are DuckDB (`delta_scan`) and Polars (`scan_delta`). JVM
  engines are not a target.
- The lake is final-only. There is one continuous final-only writer per
  network and one bucket per network, with the dataset at the bucket root
  (`--output` as given, with an opt-in `{chain}`). Tables are partitioned by
  `date` only. Non-final mode stays a CLI capability but is not deployed.
- The writer commits every 60–120 s at the chain head, and in size-based
  batches while backfilling (#659).
- Maintenance is an off-the-shelf scheduled job, not fireparq logic. A k8s
  CronJob runs the `deltalake` Python package beside the writer. fireparq's
  `merge` is removed, and its ownership stops blocking other writers to the
  Delta tables (#636).
- `partitions.parquet` and the `partitions` subcommands are removed by #653
  (PR #662, merged).

## Summary

| Question | Answer |
|---|---|
| Crate | `deltalake-core` **1.0.0** (2026-09-21), `default-features = false, features = ["rustls"]`. No DataFusion and no `deltalake-aws` (the AWS SDK): S3 goes through `object_store` with delta-rs's conditional-put `DefaultLogStore`. |
| Arrow | delta-rs 1.0.0 is on Arrow/Parquet **59** and object_store **0.13**; fireparq is on 60 and 0.12. Downgrading to 59 would undo the #568 Thrift fix. The two versions **coexist**: no Arrow value crosses between them (measured). Cost: roughly 2× release binary, +77 crates, +~160 s of release build on this host, Rust ≥ 1.94.1. |
| Pre-written parts | **Committed as-is.** fireparq keeps writing its deterministic `part-v1-*` files with Parquet 60 and commits them with `add` actions. The bytes are unchanged (measured). fireparq computes `add.stats` itself, because delta-rs keeps its footer-to-stats helper private. |
| Exactly-once | One Delta commit per table with `txn {appId: fireparq:<descriptor hash>, version: last accepted ordinal}`. Recovery rolls each table forward only when its `txn` version is below the transaction's. A stale or duplicate commit with the same `appId` fails with `ConcurrentTransaction` instead of adding a second copy (measured). |
| Concurrency | `deltalake` OPTIMIZE, lite VACUUM, checkpoints and log cleanup ran beside the writer on the very partition being appended, on local disk and loopback S3: 0 writer failures, 0 maintenance conflicts, exact rows (measured). |
| Readers | DuckDB 1.1.1 (delta v0.2.1), DuckDB 1.5.5 (delta `45c4087`) and Polars 1.44.2 (deltalake 1.6.6) read every mapped type exactly, including after OPTIMIZE, VACUUM and checkpoints. Anonymous S3 works with DuckDB 1.5.5 and Polars. **Proposal: move the CI DuckDB pin to 1.5.5.** |
| Protocol | `minReaderVersion 1`, `minWriterVersion 2`, no table features. `delta.appendOnly = true` makes the log refuse deletes; OPTIMIZE is still allowed (measured). |
| Crash safety | The #468 transaction is unchanged up to Committed. Delta commits sit between Committed and the authority advance, with `blocks` committed last. Delta's `txn` is the per-table progress marker, so no new journal phase is needed. |
| Ownership | The owner record keeps guarding one fireparq writer and its state (`.fireparq-ingest/`, `_fireparq/`, its own uncommitted parts). The Delta tables are shared through the log, and fireparq never deletes, rewrites or relies on a committed part. |
| Resume | Startup reads control records, then each table's `_last_checkpoint`, its checkpoint and its log tail. No data listing. 100,000 active files open in 147 ms with 78 MB RSS (measured, local). A 1,500-commit tail took 4.7 s on loopback S3 and 33 ms after a checkpoint. |

## 1. Dependency spike

### 1.1 Versions (pinned-version note)

| Component | Version | Notes |
|---|---|---|
| `deltalake-core` | 1.0.0 | Newest release. MSRV **1.94.1** (the workspace pins 1.93). |
| `buoyant_kernel` | 0.28.1 | delta-rs 1.0.0 depends on this delta-kernel-rs fork. The official `delta_kernel` 0.28.0 has the same Arrow ceiling (`arrow-58`/`arrow-59`, object_store 0.13.2). |
| Arrow / Parquet inside delta-rs | 59.3.0 | The workspace stays on 60.0.0. Parquet 59.3.0 still lacks the #10979 Thrift list bound ([#11186](https://github.com/apache/arrow-rs/issues/11186) open), so a workspace downgrade is ruled out. |
| `object_store` inside delta-rs | 0.13.2 | Pulls `quick-xml` 0.39.4, still under RUSTSEC-2026-0194/0195. The fix needs object_store 0.14.2 (`quick-xml` ^0.41), which no delta-rs release uses yet (#632). |
| Spike toolchain | Rust 1.98.1 | `spikes/delta-lake/rust-toolchain.toml` (channel `1.98`). |
| Python `deltalake` | 1.6.6 | Maintenance job and Polars' Delta support. |
| Polars | 1.44.2 | Same pin as `blocks/tests/engines/requirements.txt`. `scan_delta` also needs `deltalake`. |
| DuckDB CLI | 1.1.1 (delta v0.2.1), 1.5.5 (delta `45c4087`) | 1.1.1 is the current CI pin; 1.5.5 is the proposed one. Linux amd64 SHA-256: `7f3f1a26…118ae` (1.1.1), `08c0ca11…643d05` (1.5.5). |
| moto (loopback S3) | 5.2.3 (`moto[s3]`) | Honors `If-None-Match: *` (412), checked at startup by `py/loopback_s3.py`. |

The spike's `Cargo.lock` and hash-pinned `requirements.txt` pin every
transitive version.

Upstream at the time of writing: delta-rs `main` still declares Arrow 59 and
object_store 0.13.2. Arrow 60 was released 2026-09-15. The 1.0.0 release came
out six days later on 59. Expect the duplicate Arrow to go away when delta-rs
adopts Arrow ≥ 60 (an estimate, not a commitment). The upgrade lane (L1 below)
should check for a newer delta-rs first.

### 1.2 Can fireparq share delta-rs's Arrow?

No, not today. There are three options:

1. **Downgrade the workspace to Arrow/Parquet 59 and object_store 0.13.**
   Rejected: it undoes the #568 security fix (the metadata-list allocation
   bound lands only in Parquet 60) and still leaves quick-xml unfixed.
2. **Coexist.** Keep Arrow/Parquet 60 and object_store 0.12 (or 0.14 after
   #632) for everything fireparq encodes, and let delta-rs bring its own 59 and
   0.13 for the log. **Chosen.** No Arrow value crosses the boundary. The Delta
   schema is built by name from fireparq's Arrow 60 types
   (`spikes/delta-lake/src/delta.rs::delta_type`), and commits carry only
   paths, sizes, partition values and statistics JSON.
3. **Write the Delta log in fireparq.** This avoids the dependency, but fireparq
   would own protocol, replay and conflict logic that the maintenance job
   already runs off the shelf. It stays the fallback if option 2's cost is
   unacceptable. See §12.

The trial below was measured on macOS arm64 with Rust 1.98.1, at `8d18681`
(before #653 merged). It used a scratch copy of the workspace with `deltalake-core` linked into `fireparq`
through a probe that loads a snapshot, reads `txn` and builds a commit. That
change was not committed.

| | Workspace today | With `deltalake-core` 1.0.0 |
|---|---|---|
| Release `fireparq` binary | 36.7 MB | 71.5 MB |
| Release build | 92 s cold | +160 s incremental |
| Unique normal dependencies (`cargo tree -p blocks`) | 289 | 366 |
| `cargo deny --locked check advisories` (0.20.2) | ok | ok (same two quick-xml ignores; their reason must name object_store 0.13 too) |

The main duplicates are the whole Arrow family (59 and 60), Parquet 59/60,
object_store 0.12/0.13, quick-xml 0.38/0.39, reqwest 0.12/0.13, zstd
0.13/0.14, brotli 8/9 and syn 2/3. Blast radius on the source: no existing
code has to change. Only new modules call delta-rs. The toolchain moves from
1.93 to ≥ 1.94.1 (`rust-toolchain.toml` and `Dockerfile`), and the #632
object_store 0.14 upgrade stays independent.

### 1.3 Footprint

- `default-features = false` alone **does not compile**: `deltalake-core`
  requires `rustls` or `native-tls` (`compile_error!`). Use `features = ["rustls"]`.
- No `datafusion`. Without it delta-rs has no `write`, `optimize`, `delete` or
  `merge`. fireparq needs none of them. `create`, `vacuum`, commits and
  snapshots are available.
- No `deltalake-aws`, which brings `aws-config`, `aws-sdk-sts` and the Smithy
  runtime. For `s3://` URLs, fireparq registers a two-line `LogStoreFactory`
  that returns delta-rs's `default_logstore`. That log store commits with
  `PutMode::Create`, which object_store sends as `If-None-Match: *`
  (`S3ConditionalPut::ETagMatch`, the object_store 0.13 default). See
  `spikes/delta-lake/src/storage.rs`.
- The Delta log store gets its own object_store 0.13 `AmazonS3`, built from the
  same `AwsConfig`, with **`RetryConfig { max_retries: 0 }`**. object_store
  resends even non-idempotent PUTs on 5xx (`client/retry.rs`), so a commit
  whose response is lost could come back as a 412, be misread as a lost race,
  and be retried at the next version. With a single attempt the outcome
  surfaces as an error, and recovery resolves it through `txn` (§3.5).
- `BlindDeltaTable` (the lazy, append-only handle) is not usable. It keeps only
  `numRecords` of `add.stats` and exposes no `txn`. The `CommitBuilder` path
  needs an `EagerSnapshot`, which materializes the active files. See §8 for
  what that costs.

### 1.4 Committing pre-written parts

Yes. `commit_parts` (`spikes/delta-lake/src/delta.rs`) sends one
`CommitBuilder` commit with `Action::Add` per part, a
`Transaction::new(app_id, ordinal)` and `isBlindAppend: true`. Checkpoints and
log cleanup are turned off in its post-commit hook (`with_create_checkpoint(false)`,
`with_cleanup_expired_logs(Some(false))`). The test
`pre_written_parts_are_committed_byte_for_byte_with_a_txn` covers local disk,
the in-memory store and loopback S3. After the commit, the log's `add.path` is
the published name, the object's bytes and size equal what was encoded, and
fireparq's Parquet footer metadata is still there.

What fireparq must supply for each `add`:

- `path`, relative to the table root: `date=YYYY-MM-DD/part-v1-<stream>-<first>-<last>-<txn>-<index>.parquet`.
- `partitionValues`: `{"date": "YYYY-MM-DD"}`.
- `size`, from the receipt.
- `modificationTime`, recorded in the journal so a roll-forward is deterministic.
- `dataChange: true`.
- `stats`, as JSON: `numRecords` plus `minValues`, `maxValues` and `nullCount`
  for the statistics columns. delta-rs's `create_add` and
  `stats_from_parquet_metadata` are `pub(crate)`, so fireparq computes stats
  from the batch it just encoded (`part.rs::stats_json`). Timestamp statistics
  are ISO-8601 strings with millisecond precision. fireparq's times are whole
  milliseconds, so the bounds are exact.

DuckDB 1.5.5 prunes files with these statistics. `WHERE block_num < 20` reported
`Scanning Files: 1/3` (measured).

### 1.5 `txn` read-back

`DeltaTableState::transaction_version(log_store, app_id)` returns the last
committed version for an `appId`, from the checkpoint plus the tail. It works
from a freshly opened handle on every store (tested).
`recovery_rolls_each_table_forward_exactly_once` plays out a crash after the
first table's commit. Recovery skips that table, commits the other one once,
and a second recovery commits nothing. It refuses a pending transaction whose
ordinal is behind a table's `txn`, because that would mean the log is ahead of
authority. The row counts are exact.

### 1.6 Storage

| Store | Result |
|---|---|
| Local disk | All 7 tests pass. Parts are published with `PutMode::Create`, a no-clobber hard link. |
| `object_store::memory::InMemory`, shared by the parts and the log store | All pass, except the VACUUM test, which is skipped because it needs listing timestamps. |
| Loopback S3 (moto 5.2.3 on `127.0.0.1`), conditional puts, single-attempt client | All 7 pass. Four concurrent writers make 32 commits: every commit lands exactly once at a distinct version, with about 30–70 lost conditional puts retried per run on each store. |

The spike never contacts a real endpoint. `Lake::s3` refuses non-loopback
hosts, and `run.sh` runs every command in a cleared environment in a temp
directory.

### 1.7 Maintenance beside the writer

`py/concurrent_maintenance.py` drives the writer (`delta-lake-spike write`,
which commits both tables every 20 ms with a `txn`) while a loop runs the
CronJob's calls on the **same `date` partition the writer appends to**. That
is the worst case: the deployed job compacts only closed dates. Each round
runs `optimize.compact(partition_filters=[("date", "=", D)])`, then
`vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False)`
(lite mode), then `create_checkpoint()`, then `cleanup_metadata()`.

| Run (from `run.sh`) | Writer | Maintenance | Result |
|---|---|---|---|
| Local disk, 120 transactions | exit 0; 97 lost conditional puts rebased | 144 OPTIMIZE commits (72 and 70 between writer commits), 152 checkpoints, 0 conflicts, 0 errors | `txn` = 120 on both tables; 7,200 and 2,400 rows, all keys distinct; 1 active file per table |
| Loopback S3, 80 transactions | exit 0; 7 rebased | 17 OPTIMIZE commits (7 and 8 between writer commits), 19 checkpoints, 0 conflicts | `txn` = 80; 4,800 and 1,600 rows, exact |

Why it works: fireparq's commits are blind appends (`isBlindAppend: true`, no
read predicate). delta-rs's conflict checker lets them rebase over OPTIMIZE,
which only removes files with `dataChange: false`. OPTIMIZE runs under snapshot
isolation and ignores added files. A lost conditional put is only a retry at
the next version.

The two VACUUM modes behave differently (test
`lite_vacuum_keeps_uncommitted_parts_and_full_vacuum_deletes_them`):

- **Lite** (the Python default, `full=False`) deletes only files that a
  `remove` names. With retention 0 it left a published part that no log entry
  referenced.
- **Full** also deletes untracked files older than the retention, and it
  deleted that part.

§4.1 builds the VACUUM rule on this.

### 1.8 Readers

`py/read_check.py` checks exact values of the fixture (`mapping.rs`) in each
engine:

- row counts;
- `sum(block_num)`;
- Decimal(20,0) values up to `u64::MAX` (`18446744073709551615`), inside
  `List<Decimal(20,0)>` too;
- an Int16 list holding 255, which came from a `UInt8`;
- microsecond UTC timestamps;
- strings that were dictionaries, `List<Utf8>`, `Binary`;
- the `date` partition column;
- a filtered count.

| Engine | Local (plain, compacted, vacuumed, checkpointed) | Loopback S3 | Types seen |
|---|---|---|---|
| DuckDB 1.1.1, delta v0.2.1 | pass | pass with placeholder credentials; **anonymous fails once a checkpoint exists** (unsigned `HEAD` of the checkpoint refused) | `BIGINT`, `DECIMAL(20,0)`, `TIMESTAMP WITH TIME ZONE`, `DATE`, `SMALLINT[]`, `DECIMAL(20,0)[]`, `VARCHAR[]`, `BLOB` |
| DuckDB 1.5.5, delta `45c4087` | pass, with statistics-based file pruning | pass, **anonymous** (`CREATE SECRET (TYPE s3, KEY_ID '', SECRET '', …)`) | same |
| Polars 1.44.2 + deltalake 1.6.6 | pass | pass, anonymous (`aws_skip_signature`) | `Int64`, `Decimal(20,0)`, `Datetime(us, UTC)`, `Date`, `List(Int16)`, `List(Decimal(20,0))`, `List(String)`, `Binary`, `String` |

Two layout variants were probed with `--variant`:

- **Files that also keep the `date` column** (fireparq's current layout): all
  three engines read the files correctly. Delta convention and delta-rs's
  OPTIMIZE output leave the partition column out of the file, so the design
  does too (§6).
- **Millisecond timestamps in the file under a Delta `timestamp` column**:
  DuckDB reads them, but **Polars refuses**: `SchemaError: data type mismatch
  for column timestamp: incoming Datetime('ms', 'UTC') != target
  Datetime('μs', 'UTC')`. The error's hint to pass `cast_options` does not work
  with `scan_delta`. Parts must store microseconds.

The anonymous-S3 results come from moto, not RGW. moto refused some unsigned
`HEAD` requests that a public-read RGW bucket would allow. L8 repeats the
anonymous checks against the deployment's RGW.

### 1.9 Resume-cost measurements

These are `delta-lake-spike bench-load`, release build, one table. Each figure
is the time to open the table and read `txn`.

| Log | Open + `txn` | Peak RSS |
|---|---|---|
| 10,000 active files, checkpoint (414 KB) | 19 ms | 27 MB |
| 100,000 active files, checkpoint (4.0 MB) | 147 ms | 78 MB |
| 100,000 active files, 20 JSON commits, no checkpoint | 303 ms | 112 MB* |
| 1,500 one-file commits (a day at 60 s), no checkpoint, local | 299 ms | — |
| same, loopback S3 | 4,711 ms | — |
| same after `create_checkpoint()`, loopback S3 | 33 ms | — |

\* Includes the writing phase of that run.

### 1.10 Rough edges found

1. `deltalake-core` does not compile with `default-features = false` unless a
   TLS feature is enabled.
2. MSRV 1.94.1, above the workspace's 1.93.
3. The Arrow 59, object_store 0.13 and quick-xml 0.39 duplicates (§1.2), and
   the doubled binary.
4. `create_add` and `stats_from_parquet_metadata` are crate-private, so
   fireparq computes `add.stats` itself.
5. `BlindDeltaTable` drops `add.stats` beyond `numRecords` and has no `txn`.
   The full `CommitBuilder` needs an `EagerSnapshot`, which materializes files.
6. `logstore_with` needs a registered factory for `s3://`. Without
   `deltalake-aws` there is none, so the spike registers one.
7. object_store resends conditional PUTs on 5xx by default, so the log store
   client must run with `max_retries: 0` (§1.3).
8. object_store 0.13 moved `get`/`head` to `ObjectStoreExt`, an API change the
   #632 lane will meet too.
9. `CommitMetrics::num_retries` counts only lost conditional puts, not
   rebases after reading newer versions. A commit that rebased over
   concurrent versions can report 0 retries.
10. `validator_derive` → `proc-macro-error2` 2.0.1 produces a
    future-incompatibility warning.
11. Polars rejects millisecond files under a Delta `timestamp` column (§1.8).
12. DuckDB 1.1.1's delta extension fails anonymous S3 reads once a checkpoint
    exists (§1.8).
13. Each Python `vacuum` writes `VACUUM START`/`VACUUM END` commits. Lite
    vacuum with retention 0 re-deletes already deleted tombstones each run
    (harmless, and it counts them again).
14. OPTIMIZE output drops fireparq's footer key-value metadata (only
    `ARROW:schema` is left), uses `part-00000-<uuid>-c000.zstd.parquet` names,
    and does not keep block order. The rows of two parts came out in a
    different order (measured).
15. moto quirks: anonymous access needs a bucket policy, and some unsigned
    `HEAD` requests are refused anyway (§1.8).
16. macOS `SystemTime` has microsecond resolution. Two parallel tests got the
    same clock-based S3 prefix, and their data mixed until the prefix gained a
    process-wide counter. (Spike-only, but a reminder to derive names from
    identities, never from clocks.)

### 1.11 Running the spike

The CI job `delta-spike` (`.github/workflows/ci.yml`) runs all of it. It
installs the spike's toolchain, both DuckDB CLIs (checksum-verified) and the
hash-pinned Python packages. Locally:

```sh
uv venv --python 3.12 /tmp/delta-spike
uv pip install --python /tmp/delta-spike/bin/python --require-hashes -r spikes/delta-lake/requirements.txt
DELTA_SPIKE_PYTHON=/tmp/delta-spike/bin/python \
DELTA_SPIKE_DUCKDB=/path/to/duckdb-1.5.5 \
DELTA_SPIKE_DUCKDB_SIGNED=/path/to/duckdb-1.1.1 \
spikes/delta-lake/run.sh
```

Only the Rust part, with no Python or DuckDB:
`cd spikes/delta-lake && cargo test --locked`. That covers local disk and the
in-memory store. Set `DELTA_SPIKE_S3_ENDPOINT` to a loopback server to add S3.
`cd` into the directory so its `rust-toolchain.toml` applies. The crate has an
empty `[workspace]` table, so the workspace's `cargo fmt --all` and
`cargo test --workspace` ignore it.

## 2. Protocol and table properties

**Protocol: `minReaderVersion: 1`, `minWriterVersion: 2`, no reader or writer
features.** This is delta-rs's default, and both reader families read it
(§1.8). A Python OPTIMIZE keeps it. The design needs no column mapping (every
fireparq column name is a plain identifier), no deletion vectors (appends and
whole-file compaction only), no `timestampNtz` (every timestamp is UTC), and
no v2 checkpoints or domain metadata. Raising any of these needs DuckDB and
Polars checks first.

Table properties, set when fireparq creates each table:

| Property | Value | Why, for about 700–1,500 commits a day per table |
|---|---|---|
| `delta.appendOnly` | `true` | The log refuses `remove` actions with `dataChange: true`. A `DELETE` fails with "Delta table is append-only", but OPTIMIZE (`dataChange: false`) still works (measured). This puts the "final data is immutable" rule on the platform side. |
| `delta.checkpointInterval` | `100` | fireparq writes no checkpoints. The CronJob checkpoints each table every run (hourly, so a tail of at most about 60–120 commits). The interval only decides when the CronJob's own OPTIMIZE commits checkpoint as a side effect. |
| `delta.logRetentionDuration` | `interval 7 days` | Instead of 30 days: about 10,500 retained commit files per table at 1,500 a day (about 45,000 at the default), with no time-travel use beyond a week. Cleanup removes only commits that are older than this and covered by a checkpoint. |
| `delta.deletedFileRetentionDuration` | `interval 7 days` (the default) | How long files removed by OPTIMIZE stay for readers of older snapshots, and the full-VACUUM age threshold for untracked files. It bounds how long a fireparq transaction may stay pending (§4.1). Storage cost: about a week of pre-compaction parts. Lowering it is a platform trade-off. |
| `delta.dataSkippingStatsColumns` | `block_num,timestamp` | The columns readers prune on. fireparq and OPTIMIZE both write statistics only for these, which keeps `add.stats` and checkpoints small. Partition columns never have statistics. |
| `delta.targetFileSize` | `268435456` (256 MiB) | OPTIMIZE's default target. This is an estimate to revisit with #658 figures. |
| `delta.setTransactionRetentionDuration` | **unset** | `txn` entries must never expire, or exactly-once recovery breaks. |
| `fireparq.descriptor`, `fireparq.chain`, `fireparq.blockType`, `fireparq.network` | stream identity | Replaces the Parquet footer keys that OPTIMIZE drops (§1.10 item 14). `verify` reads them (§7.2). Custom keys need `with_raise_if_key_not_exists(false)`. |

## 3. Commit mapping onto the #468 protected transaction

### 3.1 Order

The #468 phases and stage hooks stay as in
[468-transaction-controller.md](../audit/468-transaction-controller.md). One
step is inserted between `CommittedPersisted` and `AuthorityAdvanced`:

1. Preflight the batch map, then persist the **Writing** journal
   (`WritingPersisted`). This step also maps Arrow types onto Delta types
   (§6), so a value that does not fit fails before anything is written.
2. Encode, journal each receipt, then publish each part with a conditional
   create (unchanged). The receipt also records the part's `add.stats` JSON
   and a `modificationTime`, so recovery can build the exact `add` without
   reading the file.
3. Verify every final part, then persist **Committed** (`CommittedPersisted`).
4. **New: one Delta commit per table that has parts in this transaction.**
   Each commit holds that table's `add` actions and
   `txn {appId, version: pending.last_ordinal}`. The non-`blocks` tables
   commit first, concurrently within `FlushConcurrency`. **`blocks` commits
   last.** Stage hooks: `DeltaCommitted(i)` after each table, then
   `DeltaCommittedAll`.
5. Advance authority from H to H′ (`AuthorityAdvanced`), reconcile the mirror,
   remove temporaries and clear pending (unchanged).

A transaction whose tables all have zero rows makes no Delta commit and
advances authority as today. A table with no rows in a transaction gets no
commit in it, so each table's `txn` version is the last transaction that gave
it rows. Versions only increase.

### 3.2 `txn` identity

- `appId = "fireparq:" + <stream descriptor SHA-256>`. The descriptor already
  binds chain, family, encoding, mapper epoch, schema digests, filters, output
  identity and mode. A different stream is a different application, and
  eligibility refuses it anyway.
- `version = pending.last_ordinal`, the `u64` accepted-event ordinal
  (`ingest/frontier.rs`) converted to `i64` with a checked cast. Ordinals are
  far below 2^63 (#648).
- The table metadata records the same descriptor hash
  (`fireparq.descriptor`). At startup fireparq refuses a table whose metadata
  names another descriptor, or whose schema, partition columns or protocol
  differ from the ones it would create.

### 3.3 Table creation

Tables are created after authority is initialized, never before: eligibility
refuses any non-empty root, including a `_delta_log/`
([`eligibility.rs`](../../firehose-parquet/src/ingest/eligibility.rs)). Every
startup ensures each table in the descriptor exists. It creates missing ones
with commit 0 (`protocol` + `metaData`) and validates existing ones. Creation
is idempotent under conditional puts: a lost race at version 0 reloads the
table and validates it. Every table exists from the first run, even one that
never gets rows, so readers never hit a missing table.

### 3.4 What readers can observe

Tables commit separately, so there is no atomic multi-table snapshot. During
step 4, a reader can see transaction T in some tables and not in others. After
a crash in step 4 that lasts until recovery runs. Two guarantees hold:

- **Per table**, every Delta snapshot holds a prefix of fireparq's
  transactions: each commit carries whole transactions, in order. Its `txn`
  version tells which one.
- **Across tables**, `blocks` commits last. A block visible in `blocks` has all
  of its rows visible in every other table. A consistent cut is:

  ```sql
  WITH f AS (SELECT max(block_num) AS b FROM delta_scan('s3://ethereum-mainnet/blocks'))
  SELECT … FROM delta_scan('s3://ethereum-mainnet/transactions'), f WHERE block_num <= f.b;
  ```

  Rows above the frontier may be visible in child tables for a moment. The
  README should present the frontier filter as the rule.

### 3.5 Conflicts and ambiguous outcomes

- **Lost conditional put** (412, another writer took the version): delta-rs
  re-lists the log and checks the winning commits. A blind append conflicts
  with neither OPTIMIZE nor another application's append, so it retries at the
  next version (measured).
- **Same `appId` in a winning commit**: `CommitConflictError::ConcurrentTransaction`
  (measured). This is the safety net that makes every ambiguous case below
  resolve to exactly one copy.
- **Ambiguous PUT** (timeout, 5xx, connection reset): with `max_retries: 0` the
  commit fails with a transport error. The controller is poisoned and the
  process stops, as for any unresolved mutation today. On restart, recovery
  reads the table's `txn` version:
  - If the commit landed, `txn == L`: skip.
  - If it did not, commit it. A delayed copy of the lost PUT that arrives
    later finds its version taken (412) and is discarded.
  - If the delayed copy lands first, recovery's own commit meets it as a
    winning commit with the same `appId`, and recovery fails with
    `ConcurrentTransaction`. The next start sees `txn == L`.
  - No outcome adds the parts twice.

  Proposed: an ambiguous **log** commit does not set the S3 owner's
  uncertainty latch, because it resolves itself without provider quiescence.
  Ambiguous **part** PUTs keep today's latch and quiescence rules unchanged.
  The recovery lane (L4) must confirm this before relying on it.
- **Duplicate `add` of the same path**: Delta reconciles `add`s by path, so
  re-adding a live file does not duplicate rows. Re-adding a file that
  OPTIMIZE already removed would, which is why roll-forward is gated by `txn`
  and never by checking whether the path is live.

## 4. Crash matrix

`L` is the pending transaction's last ordinal. Readers see a table's rows only
through its log.

| Crash point | Durable state | What readers can see | Recovery before any Blocks request |
|---|---|---|---|
| Before `WritingPersisted` | nothing new | nothing | resume H |
| Writing, parts partly published | Writing + some receipts + some parts | nothing (parts are not in any log) | Roll back as today: verify existing parts against receipts, delete exactly those, clear. The S3 quiescence rule for delayed part PUTs is unchanged. Delta is untouched. |
| All parts published, before `CommittedPersisted` | Writing | nothing | same rollback |
| `CommittedPersisted`, no Delta commit yet | Committed; every table's `txn` < L | nothing of T | For each table with parts, in commit order (`blocks` last): verify its parts against receipts, then commit it with `txn L`. Then advance authority, reconcile the mirror, clear. |
| Between table commits (`DeltaCommitted(i)`) | Committed; tables 0..i have `txn = L` | T in tables 0..i, not in `blocks` (the frontier filter hides it) | Skip the tables with `txn = L` **without touching their parts** (OPTIMIZE may have rewritten them), and commit the rest. |
| A table's commit PUT is ambiguous | Committed; that table's `txn` is L or less | either | Recovery reads `txn` (§3.5). Exactly one copy lands. |
| All Delta commits done, before `AuthorityAdvanced` | Committed; all `txn = L` | all of T | No commit. Advance authority, reconcile the mirror, clear. |
| `AuthorityAdvanced`, before mirror or clear | Committed, authority H′ | all of T | **Change:** do not verify parts when authority already equals the target (today `verify_all_finals` runs anyway, `controller.rs:115`, and would fail once OPTIMIZE and VACUUM removed a committed part). Reconcile the mirror and clear. |
| During table creation at initialization | authority, some tables | empty tables | Ensure the remaining tables exist and validate the rest. |
| `txn` > L on some table | log ahead of authority | — | Stop with evidence retained. Only a foreign writer using fireparq's `appId`, or a restored authority, can cause this. |
| An uncommitted table's part is missing or corrupt | Committed | — | Stop with evidence retained, as today (`committed_missing_or_corrupt_part_cannot_roll_forward_or_replay`). §4.1 describes the only maintenance path that can cause it. |

The existing real-binary crash hooks (`FIREPARQ_DEBUG_FAULT`) gain the new
stages. `blocks/tests/ingestion_transactions.rs` gets one crash test per new
row, which reopens the tables and compares exact rows and `txn` versions.

### 4.1 VACUUM and uncommitted parts: the rule

A part is **untracked** from its upload until its table's Delta commit.
Normally that is a fraction of a second. After a crash in the Committed phase
it lasts until the writer restarts.

- **Lite VACUUM** (`vacuum(full=False)`, the Python default, used every run)
  deletes only files named by `remove` tombstones older than the retention. It
  **never deletes an untracked part**, whatever the retention (measured with
  retention 0).
- **Full VACUUM** (`full=True`) also deletes untracked files older than the
  retention, by last-modified time. It is needed only to clean up files that a
  failed OPTIMIZE wrote and never committed. The job runs it **at most weekly,
  with the enforced default retention (168 h = `deletedFileRetentionDuration`)**.
  It never runs with `enforce_retention_duration=False`.
- **fireparq's side of the rule:** a transaction may stay pending (Committed,
  not yet in every log) for at most `deletedFileRetentionDuration` (7 days).
  In practice a restarted pod resolves it in seconds. If a full VACUUM removed
  a part of a still-uncommitted table, recovery fails closed with evidence, as
  in the last matrix row. Rows of that transaction are then visible in the
  tables that did commit and missing in the others, and the operator rebuilds
  into a new root. To make that visible early, `build` logs the pending
  journal's age at startup and exports it as a metric, and the platform alerts
  on a writer that has been down for more than a day.
- **Order inside each job run:** run VACUUM before `create_checkpoint()`. A
  checkpoint drops tombstones older than `deletedFileRetentionDuration`. If
  lite VACUUM ran after the checkpoint with the same retention, the newest
  expired tombstones would leave the snapshot before any VACUUM saw them, and
  their files would become orphans that only a full VACUUM finds. This comes
  from reading the delta-rs and kernel sources, not from a measurement. L9
  should check it.

## 5. Ownership (#636)

What the fireparq owner still guards:

- the S3 record `.fireparq-owner-v1.json` at the bucket root, or the local
  inode lock;
- **one fireparq ingestion writer, and its offline recovery, per dataset**;
- the state that writer alone mutates: `.fireparq-ingest/` (authority and
  pending), `_fireparq/cursor.parquet`, and its own uncommitted parts, which
  only Writing rollback deletes, by exact journal path.

The record keeps its protocol: persistent, conditional, with no expiry or
takeover, and with the uncertainty latch for data PUTs. With `partitions build`
gone (#653) and `merge` and `truncate` removed (L5), only `build` and
`recovery recover` take it. `verify` stays ownerless and writes its registry
conditionally (#621).

What it no longer implies: exclusive control of the Delta tables. Any
Delta-aware writer, in practice the maintenance CronJob, commits through each
table's log with conditional puts and never takes the fireparq owner. Why
that is safe:

1. **fireparq only appends.** Its commits are blind appends with a `txn`. It
   never writes a `remove`, never rewrites a file and never deletes a
   committed file. `merge`, `truncate` and `rollup` are removed, and
   `delta.appendOnly` makes the log itself refuse data removal.
2. **Maintenance never conflicts with those appends.** OPTIMIZE rebases on
   them, and they rebase on OPTIMIZE (§1.7, measured). VACUUM deletes only
   tombstoned files, or untracked files older than 7 days (§4.1).
3. **fireparq never relies on a committed part:**
   - Recovery skips tables whose `txn` shows the transaction, and it stops
     verifying parts once authority reached the target (the §4 change).
   - `verify` reads a pinned Delta snapshot, never a directory listing (§7.2).
   - Eligibility runs only while a root has no authority, so it never
     inspects committed data. `_delta_log/` makes a root ineligible, and
     fireparq creates tables only after authority exists.
   - Startup lists no data objects (§8). The only data-object requests
     fireparq makes are HEADs of the next planned part names and GETs of a
     pending transaction's own uncommitted parts.
4. **Stream exclusivity is still fireparq's job.** Two fireparq writers on one
   dataset would both append validly to Delta, but their authorities would
   diverge. The owner record prevents that, and the same-`appId` conflict is a
   second line of defense.

Platform-side policy, following the user's preference: give the CronJob an RGW
user limited to Get/Put/Delete/List on `*/_delta_log/*` and the table data
prefixes, with no access to `.fireparq-ingest/`, `_fireparq/` or the owner
key. Give the writer no DeleteObject on `*/_delta_log/*`. Writing rollback
deletes only data parts, and log cleanup belongs to the CronJob.

## 6. Types

Delta has no unsigned or dictionary types. The conversion happens once, at the
flush boundary, before the Writing journal (`to_delta_batch` in
`spikes/delta-lake/src/mapping.rs`). Every cast uses `safe: false`, so a value
that does not fit **fails the transaction before anything is written**. It
never wraps and never becomes null. The mapper builders keep their current
Arrow types. Making them build Delta types directly would save a copy per
flush and can come later.

Rules:

| fireparq Arrow type | Delta type | Rule |
|---|---|---|
| `UInt64` bounded by the protocol | `long` (Int64), checked | block, slot, epoch and height numbers, indexes, ordinals, counts, gas and compute units, sizes, sequences, protocol-validated nonces |
| `UInt64` holding a currency or token quantity, or a value the chain accepts from a sender or signer without a range check | `decimal(20,0)` | Every u64 fits exactly. This covers native balances, amounts and fees as the user decided: no per-chain economic reasoning. The one exception is Bitcoin satoshis, which consensus caps at 2.1·10^15. |
| `UInt32`, `UInt16` | `long` | lossless |
| `UInt8` | `short` (Int16) | lossless |
| `List<UIntN>`, a UInt inside a struct | the same rule, element-wise | |
| `Dictionary(Int32, Utf8)` | `string` | Parquet still dictionary-encodes the pages. |
| `Timestamp(Millisecond, "UTC")` | `timestamp` (microseconds, UTC) | Values unchanged. Written as `TIMESTAMP_MICROS`, which Polars requires (§1.8). |
| `date` (`Date32`) | `date` **partition column** | Kept out of the data files. Solana rows without a block time now get the routing day as `date` instead of null; `timestamp` stays null. |
| `Utf8`, `Binary`, `Boolean`, `Int64`, `Float64`, `List<Utf8>`, structs of those | same | |

Per-chain mapping of every unsigned and dictionary column (from `docs/schemas/`
at `8d18681`). On every chain, `block_num`, `parent_num`, `lib_num` and (non-final
only) `stream_ordinal` become `long`, `timestamp` becomes Delta `timestamp`, and
`date` becomes the partition column.

| Chain | `decimal(20,0)` | `long` (from `UInt64`) | `long` (from `UInt32`) | `short` | `string` (was a dictionary) |
|---|---|---|---|---|---|
| EVM | `blocks.nonce` (PoW nonce, any 64-bit value), `set_code_authorizations.nonce` (signer-chosen, not range-checked at inclusion), `withdrawals.amount_gwei` | `number`, `gas_used`, `gas_limit` (blocks, transactions, calls, system_calls), `size`, `blob_gas_used`, `excess_blob_gas`, `block_number`, `cumulative_gas_used`, `blob_gas`, `begin_ordinal`, `end_ordinal`, `ordinal`, `withdrawals.index`, `withdrawals.validator_index`, `transactions.nonce`, `gas_consumed`, `old_value`/`new_value` of `nonce_changes`, `gas_changes` and their `system_*` versions | `num_transactions`, `index`, `tx_index`, `log_index`, `block_index`, `access_index`, `authorization_index`, `v`, `call_index`, `parent_index`, `depth` | — | `detail_level`, `transactions.type`, `transactions.status`, `call_type`, `reason` |
| Solana | `fee`, `pre_balances`/`post_balances` (`List<decimal(20,0)>`) in transactions and vote_transactions, `rewards.post_balance` (all lamports) | `slot`, `parent_slot`, `block_height`, `compute_units_consumed`, `cost_units` | every `UInt32` (`transaction_index`, `num_*`, `*_index`, `stack_height`, `decimals`, ...) | `instructions.accounts`, `account_lookups.writable_indexes`, `readonly_indexes` (`List<short>`) | `rewards.reward_type` |
| Beacon | `amount` (deposits, withdrawals, deposit_requests, withdrawal_requests); the slashing evidence fields the protocol takes as signed: `proposer_slashings.header_{1,2}_slot`, `attester_slashings.attestation_{1,2}_{slot,committee_index,source_epoch,target_epoch}` | `slot`, `parent_slot`, `proposer_index`, `block_slot`, `committee_index`, `source_epoch`, `target_epoch` (attestations), `validator_index`, `header_{1,2}_proposer_index`, `epoch` (voluntary_exits), `block_number`, `gas_limit`, `gas_used`, `blob_gas_used`, `excess_blob_gas`, `blob_index`, `withdrawal_index`, `deposit_requests.deposit_index`, `attestation_{1,2}_attesting_indices` (`List<long>`) | `attestation_index`, `deposit_index`, `slashing_index`, `exit_index`, `change_index`, `request_index` | — | `blocks.spec` |
| NEAR | — | `height`, `prev_height`, `chunks_included`, `shard_id`, `gas_used`, `gas_limit`, `height_created`, `height_included`, `encoded_length`, `nonce` (the protocol bounds it by the block height × 10^6), `gas_burnt`, `gas`, `storage_usage` | `latest_protocol_version`, `transaction_index`, `receipt_index`, `action_index`, `log_index`, `state_change_index` | — | `action_kind`, `receipt_status`, `state_changes.type`, `state_changes.cause` |
| Antelope | `actions.error_code` (`eosio_assert_code` takes any uint64) | `transactions.index`, `net_usage`, `trace_block_num`, `receipt_{global,recv,code,abi}_sequence`, `db_ops.tx_index` | `number`, `confirmed`, `schedule_version`, `cpu_usage_us`, `action_ordinal`, `creator_action_ordinal`, `closest_unnotified_ancestor_action_ordinal`, `execution_index`, `action_index`, `db_op_index` | — | `transaction_status` |
| Tron | — | `number`, `parent_number`, `block_number`, `block_log_index` | `version`, `num_transactions`, `transaction_index`, `log_index`, `internal_index`, `contract_index`, `call_value_index` | — | `code`, `contract_type`, `receipt_result` |
| Cosmos | `timeout_height` (sender-chosen), `fee_gas_limit` (sender-chosen; unbounded when a chain's max block gas is -1) | `signer_infos[].sequence` (struct field) | `num_txs`, `tx_decode_failures`, `index`, `code`, `tx_index`, `event_index`, `attribute_index`, `message_index` | — | — |
| Bitcoin | — | `value_sats` (consensus `MAX_MONEY`) | `nonce`, `n_tx`, `version`, `locktime`, `tx_index`, `input_index`, `prev_vout`, `sequence`, `output_index` | — | — |

Columns that are already strings stay strings: EVM `value`, `base_fee_per_gas`
and balance values; Solana `token_balances.amount`; NEAR `amount` in
yoctoNEAR; Cosmos `fee_amount[].amount`. Parsing Solana `token_balances.amount`
into `decimal(20,0)` is a possible later improvement, not part of this design.

Consequences:

- The change happens once per schema, so the mapper epoch advances. The
  generated `docs/schemas/*.md` show the Delta types, and the per-chain
  classifications above become a table in `blocks/src/chain.rs` (the
  `ChainProfile` gains the Decimal(20,0) column list). The schema contract test
  asserts, for every table of every chain, that the mapped schema has no
  unsigned, dictionary or millisecond type, and that a value above
  `i64::MAX` in a `long` column is refused (the spike's
  `checked_casts_refuse_values_that_do_not_fit`).
- `verify`'s `batch_blocks` accepts only `UInt64` `block_num` today
  (`verify.rs`). It must accept Int64, or every partition looks open.

## 7. What is removed and what changes

### 7.1 Removed, with no compatibility path

| Removed | Replaced by |
|---|---|
| Plain-Parquet output: the unprotected `OutputWriter` in `writer.rs`, Hive-directory discovery as the way to read a table, the `date` column inside data files | Delta tables, read through the log |
| `merge`, `merge/engine.rs`, `merge/read.rs`, `merge_journal.rs` (`_fireparq_merge.json`), the merge-journal checks in `ingest/maintenance.rs` and at startup | the CronJob's OPTIMIZE |
| `truncate` and `date_partition::is_date_value_pattern` | Nothing. There is no safe Delta equivalent, because deleting committed rows contradicts fireparq's authority and `appendOnly` refuses it. A bad dataset is rebuilt into a new root. An operator can still clear `appendOnly` and run a `deltalake` DELETE, outside fireparq's guarantees. |
| `maintenance/compaction.rs` (the merge encoder and receipt stripping) | — |
| Finer partition code | Already gone in #652, leaving only `date=`. |
| `partitions.parquet` and `partitions` | #653 (PR #662, merged) |

### 7.2 `verify` over Delta snapshots

What changes, following the subagent audit of `verify.rs`:

- **File set:** pin a table version and take its active `add`s grouped by
  `partitionValues.date`, instead of `list_verify_files` or `list_verify_objects`.
  `_delta_log/` must never be read as data. Today a `*.checkpoint.parquet`
  sorts first, becomes `first_file` and breaks chain and table detection.
- **Unchanged-snapshot check:** a pinned version is immutable, so the second
  listing and ETag comparison go away. If a pinned file disappears, the run
  fails with a clear error. That only happens when a VACUUM removed a file
  tombstoned more than 7 days before, so a run never takes that long.
- **Root:** today's `merkle_v2` hashes rows in path-sorted file order. OPTIMIZE
  changes file names and row order (§1.10 item 14), and after OPTIMIZE but
  before VACUUM a directory listing sees each row twice. **`merkle_v3`** keeps
  the row encoding, but orders leaves canonically by `(block_num, leaf hash)`
  within each `(table, date)`. That makes the root independent of file layout,
  compaction and row order, while still counting duplicate rows. Memory is
  about 40 bytes per row per partition (roughly 200 MB for a 5-million-row
  day, an estimate), or an external sort. Changing the hash strategy bumps
  `merkle_version`, following `docs/verifiability-hash-strategy.md`.
- **Open partitions:** in the final-only lake, a date is closed once `blocks`
  holds a later date. That follows from `blocks` committing last and from
  monotonic dates. It replaces the frontier heuristics. Non-final datasets stay
  all-open, as today.
- **Identity:** chain, block type and network come from the tables'
  `fireparq.*` properties (or authority), not from Parquet footers, which
  OPTIMIZE drops.
- The registry `_fireparq/merkle_roots.parquet` and the reports stay as they
  are.

### 7.3 `validate`, `scan`, `inspect`

- `inspect <file>` stays a single-file Parquet inspector.
- `validate` (block continuity) reads the active files of a pinned snapshot
  instead of walking directories. A walk would see tombstoned files as
  duplicates and fail on checkpoint Parquet files.
- `scan` becomes a summary from the log (files, rows from `numRecords`, bytes,
  the date range per table), or is dropped in favor of documented DuckDB
  queries. It is a small lane either way.
- Walkers that remain (`discovery.rs`) skip `_delta_log/`.

## 8. Resume cost independent of data size (#655)

What `build` reads at startup with Delta:

- **Control state (unchanged):** the owner record, `.fireparq-ingest/state.json`
  and `pending.json`, and the `_fireparq/cursor.parquet` mirror.
- **For each table:**
  - `_delta_log/_last_checkpoint` (one GET);
  - the checkpoint Parquet (one or a few range GETs, O(active files): 4.0 MB
    for 100,000 files, measured);
  - one LIST of `_delta_log/` starting after the checkpoint version, O(tail);
  - the tail commits, one GET each, O(commits since the checkpoint).
- **Only with a pending transaction:** GETs of its own uncommitted parts.

There is no data listing. `EagerSnapshot` keeps the active files of each table
in memory for the run: about 0.8 KB each after the checkpoint (78 MB for
100,000 files, measured). With daily OPTIMIZE, active files grow by the
number of files per compacted day, not by commit count. For example, 20
tables × 10 years × a few files a day is tens of thousands of files per table
(an estimate). If that ever matters, a lazy snapshot path (the kernel
transaction API, which `BlindDeltaTable` already uses) is the upgrade. It is
not needed for the launch.

The tail is bounded by the CronJob's checkpoint cadence. At 1,500 commits a
day, the hourly run keeps it at about 60–120 commits. A one-day tail cost 4.7 s
on loopback S3, and 33 ms after a checkpoint (measured). Real RGW latency is
higher per GET, which is another reason for hourly checkpoints. `build`
exports the tail length (`fireparq_delta_log_tail_commits` per table) so the
platform can alert when the job stops.

Removing today's three full-root listings (nested-marker discovery twice and
merge-journal discovery) is #655's own work. With `merge` gone, the
merge-journal listing disappears altogether.

Update (#655): done. A resume lists no data objects: nested and enclosing
datasets are checked in full at creation and through the ancestors on resume,
and merge journals are looked for only while `.fireparq-ingest/merge-intent.json`
exists. L5 deletes that record (`ControlKey::MergeIntent`) with `merge`. See
[the #655 record](../audit/655-resume-cost.md).

## 9. Maintenance CronJob

The CronJob is not fireparq code: it is the `deltalake` Python package
(pinned, `deltalake==1.6.6`) on a schedule. L9 adds the reference script to
the repository (`scripts/delta_maintenance.py`) and tests it in CI beside a
running `build`, reusing the spike's `concurrent_maintenance.py`.

```python
"""Hourly Delta maintenance for one network bucket (all tables at the root)."""
import os
from deltalake import DeltaTable, WriterProperties

BUCKET = os.environ["LAKE_BUCKET"]                     # e.g. ethereum-mainnet
STORAGE = {
    "AWS_ENDPOINT_URL": os.environ["S3_ENDPOINT"],     # in-cluster RGW
    "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
    "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
    "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
    "aws_conditional_put": "etag",                     # conditional-put commits, no DynamoDB
    "AWS_ALLOW_HTTP": os.environ.get("AWS_ALLOW_HTTP", "false"),
}
TABLES = os.environ["LAKE_TABLES"].split(",")          # blocks,transactions,logs,...
FULL_VACUUM = os.environ.get("FULL_VACUUM") == "1"     # set by the weekly schedule
PROPS = WriterProperties(compression="ZSTD", compression_level=3)

def table(name):
    return DeltaTable(f"s3://{BUCKET}/{name}", storage_options=STORAGE)

# blocks commits last in every fireparq transaction, so every date before its
# newest one is complete in every table.
open_date = max(p["date"] for p in table("blocks").partitions())

for name in TABLES:
    dt = table(name)
    for d in sorted({p["date"] for p in dt.partitions()}):
        day = [("date", "=", d)]
        if d < open_date and len(dt.file_uris(file_pruning_predicate=day)) > 1:
            dt.optimize.compact(partition_filters=day, writer_properties=PROPS)
    dt.vacuum(dry_run=False, full=FULL_VACUUM)         # lite; full only weekly, 168 h enforced
    dt.create_checkpoint()                             # after VACUUM (§4.1)
    dt.cleanup_metadata()                              # commits older than 7 days behind a checkpoint
```

The loop body was run with `deltalake` 1.6.6 against a two-day spike lake. It
compacted the closed day to one file and left the open day's three files alone.

Notes:

- **Closed dates:** a date is complete once `blocks` holds a later one (§3.4).
  Only those are compacted, so OPTIMIZE never touches the head. It would be
  safe to (§1.7), but it would repeat work. Dates with one file are skipped.
  `compact` targets `delta.targetFileSize`.
- **Schedule:**
  - hourly, for example `17 * * * *`, with `concurrencyPolicy: Forbid` and
    `activeDeadlineSeconds: 3000`;
  - a second weekly CronJob (`FULL_VACUUM=1`) at a quiet hour;
  - the image is `python:3.12-slim` plus the pinned wheel. It runs as an RGW
    user limited as in §5.
- **Failure behavior:** a failed or conflicting run is simply retried next
  hour. OPTIMIZE files that failed to commit are untracked and are removed by
  the weekly full VACUUM after 7 days. The writer is never affected.
- **Bloom filters and sort metadata:** fireparq writes both. OPTIMIZE output
  keeps neither unless `WriterProperties(column_properties=…)` enables Bloom
  filters. Choose the columns with the #658 numbers. That tuning is
  platform-side too.

## 10. Reader examples (anonymous, public-read RGW)

`rgw.example.org` and `ethereum-mainnet` are placeholders.

DuckDB 1.5.5:

```sql
INSTALL delta; LOAD delta;
CREATE SECRET lake (TYPE s3, KEY_ID '', SECRET '', REGION 'us-east-1',
                    ENDPOINT 'rgw.example.org', URL_STYLE 'path');
-- One day of blocks: pruned by the partition column, then by block_num statistics.
SELECT count(*), min(block_num), max(block_num)
FROM delta_scan('s3://ethereum-mainnet/blocks') WHERE date = DATE '2026-09-25';
-- A consistent cut across tables (blocks commits last).
WITH f AS (SELECT max(block_num) AS b FROM delta_scan('s3://ethereum-mainnet/blocks'))
SELECT count(*) FROM delta_scan('s3://ethereum-mainnet/logs'), f
WHERE date >= DATE '2026-09-25' AND block_num <= f.b;
```

The first query is also #653's replacement for "block range of a day".

Polars 1.44.2 (with `deltalake` installed):

```python
import polars as pl
opts = {"aws_endpoint_url": "https://rgw.example.org", "aws_region": "us-east-1",
        "aws_skip_signature": "true"}
blocks = pl.scan_delta("s3://ethereum-mainnet/blocks", storage_options=opts)
day = blocks.filter(pl.col("date") == pl.date(2026, 9, 25)).select(pl.len()).collect()
```

## 11. Implementation plan

Sizes are estimates: S < 300 changed lines, M 300–1,000, L 1,000–3,000 (not
counting deletions or generated schema docs). Each lane has its own tests and
its own entry in `docs/releases/v1.0.0.md`.

| Lane | Scope | Tests | Size | Depends on | Parallel with |
|---|---|---|---|---|---|
| **L0** (this PR) | design, spike, `delta-spike` CI job | spike suite | — | — | all |
| **L1** deps | toolchain 1.93 → 1.98 (`rust-toolchain.toml`, `Dockerfile`); `deltalake-core =1.0.0` (no default features, `rustls`); `deny.toml` ignore reasons also naming object_store 0.13; recheck for a newer delta-rs on Arrow ≥ 60 first | full suite, `cargo deny` | S | — | #655, #658, #659 |
| **L2** types | `firehose-parquet/src/delta/types.rs` (checked flush-boundary mapping), the `ChainProfile` Decimal(20,0) lists, parts without the `date` column and with µs timestamps, mapper epoch bump, regenerated `docs/schemas/`, schema contract assertions, `verify` accepting Int64 `block_num` | schema contract (every table, encoding and `fork_step` setting), overflow refusal, golden fixtures re-pinned | L | L1 | #655, #659 |
| **L3** commit layer | `delta/{mod,store,stats,commit}.rs`: object_store 0.13 log store from `AwsConfig` with a single attempt; the `s3://` factory; table creation and validation after authority init (`fireparq.*` properties, §2); per-table commits with `txn` after Committed, `blocks` last; receipts that carry stats and `modificationTime`; new stage hooks | the spike's tests ported: byte-for-byte parts, `txn` read-back, concurrent writers, same-`appId` conflict, on local, in-memory and loopback S3 | L | L1, L2 | #655, #659 |
| **L4** recovery and ownership | Committed roll-forward gated by `txn`; no part verification when authority equals the target; "log ahead" refusal; ensure-tables at startup; the log-commit uncertainty decision (§3.5); owner semantics and RGW policy docs (#636) | real-binary crash test for each §4 row (`ingestion_transactions.rs`), an external OPTIMIZE and VACUUM between crash and restart, `txn` and exact rows | L | L3 | #659; rebase with #655 (`session.rs`, `ingest/maintenance.rs`) |
| **L5** removals | plain-Parquet `OutputWriter` and readers, `merge` and its journal, `truncate`, `maintenance/compaction.rs`, merge-journal startup checks, their CLI flags, README sections, `maintenance_crash_hooks.rs` cases | the remaining suite stays green; CLI help tests | M (mostly deletions) | L3 | #655 (shared `ingest/maintenance.rs`); #653 already merged |
| **L6** verify | pinned-snapshot file sets, `merkle_v3` canonical order, open dates from `blocks`, identity from table properties; `docs/verifiability-hash-strategy.md`, the report contract | golden roots unchanged by OPTIMIZE (same root before and after compaction), refusal on a vacuumed pinned file, registry tests | L | L3 | L4, L5, L7 |
| **L7** validate, scan, inspect | snapshot-based `validate`, log-based `scan` (or its removal), `_delta_log/` skipped by walkers | CLI tests over Delta tables | M | L3 | L4–L6 |
| **L8** engine CI | `blocks/tests/engine_compat.rs` on `delta_scan` and `scan_delta` for every chain's tables (final and non-final); DuckDB pin → 1.5.5; add `deltalake` to `blocks/tests/engines/requirements.txt`; anonymous-read checks against the deployment's RGW (opt-in) | engine test required in CI | M | L3 | L4–L7 |
| **L9** maintenance job | `scripts/delta_maintenance.py`, a k8s CronJob example, the CI test beside a real `fireparq build` (local and loopback S3), the VACUUM-then-checkpoint ordering check (§4.1) | the concurrency test from the spike, run against the binary | M | L3 | L4–L8 |
| **L10** docs and release | README (outputs, readers, maintenance, ownership, frontier rule), `docs/releases/v1.0.0.md` (breaking changes, and "JVM engines are not a target" replacing "the planned Delta mode covers them"), `docs/repo-navigation.md`, k8s-parquet examples moved to `delta_scan`/`scan_delta` | doc drift tests | M | L2–L9 | — |

Ordering: L1 → L2 → L3 → {L4, L5, L6, L7, L8, L9} → L10. L2 can start on
Arrow 60 before L1 lands, because it needs no delta-rs. #658's benchmark
should be re-run after L3 to measure per-flush commit overhead (about one PUT
and one LIST per table with rows, an estimate). #659's interval switching gives
the 60–120 s head cadence and needs no Delta changes. The spike (L0) can be
deleted once L3 and L9 cover its tests.

## 12. Risks and open questions

- **Binary size and build time roughly double** while delta-rs and fireparq
  are on different Arrow majors (§1.2). If that is unacceptable before
  delta-rs moves to Arrow ≥ 60, the fallback is a fireparq-native log writer:
  commit JSON with `PutMode::Create`, `txn` and `metaData` replay from the
  classic checkpoint plus the tail, and blind-append conflict handling. That
  is estimated at L size. It would keep the same protocol, properties and
  journal integration, so every other lane is unaffected.
- **Quick-xml advisories:** object_store 0.13 inside delta-rs keeps
  RUSTSEC-2026-0194/0195 ignored until delta-rs moves to object_store ≥ 0.14.2.
  #632 can still move fireparq's own client to 0.14.
- **Anonymous DuckDB reads** were validated against moto, not RGW (§1.8).
- **Log-commit uncertainty** (§3.5): the proposal not to latch the owner on
  ambiguous log commits needs the L4 review against #468's quiescence
  reasoning.
- **`merkle_v3` memory** on the largest partitions (§7.2) needs a #658-style
  measurement.
- **Target file size and Bloom filters** for compacted files are estimates.
  The #658 figures should set them.
