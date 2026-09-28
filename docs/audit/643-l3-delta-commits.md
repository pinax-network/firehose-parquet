# Delta commit layer (#643, lane L3)

Refs #643; part of #463. PR: [#671](https://github.com/pinax-network/firehose-parquet/pull/671). Design:
[`docs/design/delta-lake.md`](../design/delta-lake.md) §1.3–1.6, §2, §3, §4, §8
and the L3 row of §11. Index: the #643 rows of the [audit index](README.md).

## Diagnosis

After L1 (the `deltalake-core` 1.0.0 dependency) and L2 (every part a valid
Delta data file), `build` still wrote plain Parquet: nothing created a Delta
table, and no part reached a Delta log. The v1.0.0 launch is Delta-only, so
every `build` has to commit its parts to one Delta table per mapper table,
without rewriting them and without weakening the #468 transaction.

## Decision

- Every `build` writes Delta tables. There is no flag and no plain-Parquet
  mode; the parts stay where they are, as the tables' data files.
- delta-rs is used off the shelf (with its own Arrow 59 and object_store
  0.13), through `CommitBuilder`. No Arrow value crosses between the two
  Arrow majors: commits carry paths, sizes, partition values and statistics.
- The identity properties are `fireparq.descriptor`, `fireparq.chain` and
  `fireparq.blockType`. `fireparq.network`, which design §2 listed, would
  only repeat `fireparq.chain`, so it was dropped (coordinator decision).
- The Committed-phase roll-forward gated by `txn`, dropping part
  re-verification once authority reached the target (`controller.rs`, the
  §4 change) and the ownership docs are L4. The crash rows this leaves open
  are listed below.

## Implementation

### Modules (`firehose-parquet/src/delta/`)

- `mod.rs`: the protocol (reader 1, writer 2, no features), the table
  properties, `DeltaIdentity` (the three `fireparq.*` properties and the
  `txn` application id `fireparq:<descriptor SHA-256>`), `delta_columns` (the
  Delta columns of an Arrow 60 data file schema, by name, plus the `date`
  partition column) and `create_table`, which now also sets the identity and
  commits version 0 with `max_retries = 0`, so a creator that loses the race
  fails and validates the winner's table instead of committing a second
  `protocol` + `metaData`.
- `store.rs`: `DeltaStore`, a local dataset root or an S3 bucket plus prefix;
  the log store of each table (`logstore_with`, delta-rs's `DefaultLogStore`
  with conditional-put commits); the `s3://` log store factory, registered
  once, since there is no `deltalake-aws`; and `s3_log_client`, an
  object_store 0.13 `AmazonS3` built from the same `AwsConfig` fields as the
  other ingestion clients (credentials or the provider chain, region,
  endpoint and its addressing style), with `S3ConditionalPut::ETagMatch`, a
  60 s request timeout and **`max_retries: 0`**. On local disk, fireparq syncs
  each new commit file and its `_delta_log/` directory (and, at creation, the
  table directory and the root) before the commit counts, because
  object_store's local store does not. Update (#680, v1.0.2): the client's
  HTTP connector now resends idempotent reads (`GET`, `HEAD`) up to 3 times
  on a transient failure; every write still has one attempt
  ([record](680-delta-read-retries.md)).
- `stats.rs`: each part's `add.stats`, computed from the batch it encodes.
- `commit.rs`: `DeltaTables` (open, create, validate, commit) and `PartAdd`.

### Tables: creation and validation

`IngestionSession::open` opens the tables right after the controller opened
(authority exists, the descriptor matched and pending recovery ran), at most
`--flush-publish-concurrency` at a time:

- one Delta table per descriptor table, at `<root>/<table>/`, even a table
  that never gets rows;
- a missing table is created only while the authority has accepted nothing
  (ordinal 0), which covers a crash during creation. Afterwards a table
  without a log is refused ("its rows are unreachable"): that is a dataset
  written before Delta commits or a removed `_delta_log/`;
- an existing table must have exactly the configuration fireparq creates
  (every property below, nothing else), a `fireparq.descriptor` equal to this
  stream's (an error names both hashes), reader 1 / writer 2 without
  features, `date` as the only partition column, and exactly the mapper's
  Delta schema. The session derives that schema from the Delta data file
  schemas (`declare_data_schemas`, the same empty flush as the inventory) and
  checks each one against its descriptor digest first.

Table properties:

| Property | Value |
|---|---|
| `delta.appendOnly` | `true` |
| `delta.checkpointInterval` | `100` |
| `delta.logRetentionDuration` | `interval 7 days` |
| `delta.deletedFileRetentionDuration` | `interval 7 days` |
| `delta.dataSkippingStatsColumns` | `block_num,timestamp` |
| `delta.targetFileSize` | `268435456` |
| `fireparq.descriptor` | the stream descriptor's SHA-256 |
| `fireparq.chain` | the descriptor's `chain` (EndpointInfo chain name) |
| `fireparq.blockType` | the block family (`evm`, `solana`, ...) |

`delta.setTransactionRetentionDuration` stays unset, so `txn` entries never
expire.

### Receipts

The journal's `PartReceipt` gains `stats` (the `add.stats` JSON) and
`modification_time` (milliseconds since the epoch). The pipeline sets both
once, when the receipt is made; `PendingTransaction::validate` requires a
positive time and statistics whose `numRecords` is the part's row count. A
commit, and later a roll-forward, builds the `add` from the journal alone.
Pending journals written by earlier development builds of mapper epoch v3
lack the fields and no longer parse; v1.0.0 is not released.

Statistics (`delta::stats::stats_json`):

- `numRecords`;
- `minValues` / `maxValues` for `block_num` (a JSON integer) and `timestamp`
  (ISO-8601 UTC with milliseconds; the minimum rounded down and the maximum
  up, which is exact for fireparq's whole-millisecond times);
- `nullCount` for both. A column whose values are all null gets its
  `nullCount` and no bounds. Partition columns have no statistics.

### Commit order

`TransactionController::publish_and_commit`, after `CommittedPersisted`:

1. For each table with a part (one part per table per transaction), one
   commit: the part's `add` (`path` relative to the table,
   `partitionValues.date`, `size`, `modificationTime`, `dataChange: true`,
   `stats`) and `txn {appId: fireparq:<descriptor>, version: last ordinal}`
   (checked `u64` to `i64`), as a blind append (`Write`/`Append`, no
   predicate, `isBlindAppend` and the transaction id and ordinals in
   `commitInfo`), with checkpoints and log cleanup off and up to 25 attempts
   for lost conditional puts. The non-`blocks` tables commit first,
   concurrently within `--flush-publish-concurrency`; **`blocks` commits
   last**. Stage hooks: `DeltaCommitted(entry index)` after each durable
   commit, then `DeltaCommittedAll`.
2. Authority advances (`AuthorityAdvanced`), the mirror reconciles, pending
   clears, as before.

A transaction without rows makes no Delta commit. A table without rows in a
transaction gets no commit in it. On the first failure no further commit
starts, the started ones finish, and the controller is poisoned with the
journal Committed. A failure that may have sent a log write without a
definite outcome (anything but a conflict or an exhausted retry budget)
marks the S3 owner's uncertainty latch, like an unresolved part PUT, so the
owner is not released (the conservative side of design §3.5; L4 decides
whether log commits can skip the latch).

Real-binary fault hooks (debug builds, `FIREPARQ_DEBUG_FAULT`):
`delta-commit:<table>` fails before the table's commit request, and
`crash-after-delta-commit:<table>` aborts once it is durable (with `blocks`,
after every Delta commit and before authority advances).

### Metrics and logs

- `firehose_parquet_delta_log_tail_commits{table}`: commits after the last
  checkpoint (design §8 called it `fireparq_delta_log_tail_commits`; every
  metric of the exporter has the `firehose_parquet_` prefix). Set at startup
  and after each commit, from `_delta_log/_last_checkpoint`, which is read
  again at most once a minute per table.
- `firehose_parquet_delta_commit_seconds{table}`: a histogram of each
  table's commit, from request to durable version.
- `firehose_parquet_delta_commit_retries_total{table}`: lost conditional
  puts retried at a later version.
- The "committed flush size observation" log line gains `delta_commits`,
  `delta_ms` and `delta_retries`.

## Tests

- `delta::commit::tests` (the spike's tests, ported), each on local disk, an
  in-memory store and a loopback S3 endpoint
  (`delta/commit/tests/loopback_s3.rs`: conditional PUTs, listings, byte
  ranges, one attempt per request):
  - `pre_written_parts_are_committed_byte_for_byte_with_a_txn_blocks_last`:
    the log adds exactly the published object, whose bytes and footer are
    unchanged; the commit is `commitInfo`, `add` and `txn` only, with the
    journaled path, partition, size, time and statistics; `txn` reads back
    from a fresh handle (and `None` for another application); `blocks`
    commits after the other table; two parts for one table are refused.
  - `stale_writers_rebase_on_blind_appends_but_not_on_their_own_app_id`: a
    foreign append at the same version makes ours commit at the next one; a
    stale handle of the same stream fails with `ConcurrentTransaction`,
    classified as definite, and no part is added twice.
  - `concurrent_writers_serialize_through_conditional_puts`: fireparq and three
    other applications append 32 commits at once; every one lands once at a
    distinct version (for example 8 retried on local disk and 21 lost
    conditional puts on the S3 endpoint in one run).
  - `tables_are_created_once_then_validated_against_the_stream`: no creation
    once transactions exist, a concurrent creation race ends with one valid
    version 0, and another descriptor, schema or a table without the identity
    properties is refused.
  - `the_log_tail_counts_commits_after_the_last_checkpoint`: the tail grows
    per commit, a checkpoint resets it, and commits continue after it.
- `delta::stats::tests` and `delta::tests`: statistics bounds, rounding and
  null handling; the table properties, identity and protocol of a created
  table; every Delta data file type mapped by `delta_columns`.
- `ingest::controller::tests::delta`: one commit per table, `blocks` last,
  versions and `txn` per transaction, the adds against the parts on disk, and
  no commit for a transaction without rows; a failure after `logs` commits
  leaves `blocks` without the transaction and authority behind
  (`recovery_does_not_roll_delta_commits_forward_yet` pins today's recovery,
  which L4 replaces); a stop after `DeltaCommittedAll` recovers consistently.
- `ingest::session::tests::delta`: through the real session, local and S3:
  tables at the first open, `txn` equal to the authority's ordinal, the
  exported metrics, a resume (also by a new S3 owner) that continues, the
  refusal of a table without a log once the stream has committed, and a
  `blocks` commit whose response is lost: the flush fails, the commit
  landed, `logs` committed, authority stays at 0 and the S3 owner is
  uncertain.
- `blocks/tests/delta_tables.rs`, the real binary on a cursor-aware mock
  Firehose (four final EVM blocks over two days, one transaction per block,
  `[100, 102)` then a clean restart to `[100, 104)`), on local disk and on the
  loopback HTTPS S3 endpoint of `examples/bench_live_flush/s3.rs`:
  - from the logs: every mapper table exists with the protocol, partition,
    properties and identity; per table, one commit per transaction with rows,
    whose `add` is the part on disk (size and `numRecords` against the file)
    and whose `txn` increases to the authority's ordinal; `blocks` commits
    after the other tables of each transaction; each S3 log key was PUT once;
  - DuckDB `delta_scan` and Polars `scan_delta` read exact rows and blocks of
    `blocks`, `transactions`, `logs` and `access_lists`, the `date` partition
    (a `date` filter returns one day), `BIGINT` / `Int64`, `DECIMAL(20,0)`
    (a `u64::MAX` nonce, exactly), microsecond UTC timestamps, and an empty
    table that never had rows;
  - `a_crash_after_every_delta_commit_restarts_without_a_duplicate`: with
    `crash-after-delta-commit:blocks` the process aborts after the first
    transaction's commits, before authority; the restart recovers and
    continues, and each table holds each transaction once.
- Updated: `assert_dataset_root_layout` (`ingestion_transactions.rs`) accepts
  a table directory that holds only its `_delta_log/`; a session test that
  asserted no `blocks/` directory now asserts no part.

## Validation

- `cargo fmt --all` and `cargo test --workspace --locked` pass with
  `FIREPARQ_REQUIRE_DUCKDB=1` (DuckDB 1.1.1) and `FIREPARQ_REQUIRE_POLARS=1`
  (Polars 1.44.2, `deltalake` 1.6.6): 976 passed, 14 ignored, on origin/main
  `00734e4` (after L5a) plus this change. 19 of them are new: 16 in the
  `firehose-parquet` library and 3 in `blocks/tests/delta_tables.rs`.
- Readers: DuckDB 1.1.1 (delta v0.2.1, the CI pin), DuckDB 1.5.5 (delta
  `45c4087`) and Polars 1.44.2 with `deltalake` 1.6.6 all pass
  `blocks/tests/delta_tables.rs` on local and S3 output.
- `blocks/tests/engines/requirements.txt` adds the hash-pinned `deltalake`
  1.6.6 (with `arro3-core`, `deprecated` and `wrapt`), which `scan_delta`
  needs, and CI checks it imports. The DuckDB CI pin stays 1.1.1; L8 owns
  the engine matrix and the move to 1.5.5.
- Release binary (`cargo build --release --locked --bin fireparq`, cold, macOS
  arm64 on a shared host): **69.7 MB** (69,729,808 bytes), against 37.6 MB at
  L1, now that `build` calls delta-rs; the design's trial estimate was
  71.5 MB. The build took 163 s wall and 1,395 s CPU.

## Crash rows left for L4

From the design's §4 matrix. L4's real-binary crash tests can use the two new
fault hooks.

Update (L4): every row is closed as the last column says; the log commit
skips the latch. See [643-l4-delta-recovery.md](643-l4-delta-recovery.md).

| Row | L3 behavior | L4 |
|---|---|---|
| `CommittedPersisted`, no Delta commit yet | The restart verifies the parts, advances authority and clears: the transaction is in no Delta table (its parts are untracked). A logged warning names the ordinals. | Commit each table gated by its `txn`, `blocks` last, then advance. |
| Between table commits (`DeltaCommitted(i)`) | Same: tables that had committed hold the transaction, the others (always `blocks`) miss it. `recovery_does_not_roll_delta_commits_forward_yet` pins this. | Skip tables whose `txn` is L without touching their parts; commit the rest. |
| A table's commit PUT is ambiguous | The flush fails, the controller stops, the S3 owner is uncertain (not released). The restart behaves as above. | Resolve through `txn` (§3.5), and decide whether a log commit may skip the latch. |
| All Delta commits done, before `AuthorityAdvanced` | Recovered correctly (tested with the real binary), but through `verify_all_finals`. | No commit; advance without re-verifying parts. |
| `AuthorityAdvanced`, before mirror or clear | Unchanged: `verify_all_finals` runs, and would fail once OPTIMIZE and VACUUM removed a committed part. | Skip part verification when authority equals the target. |
| During table creation at initialization | The next start creates the missing tables (ordinal 0) and validates the others; covered by the unit race test, not by a real-binary crash test. | Real-binary crash test. |
| `txn` > L on some table | Not checked. | Refuse with evidence ("log ahead"). |
| An uncommitted table's part is missing or corrupt | Unchanged: recovery stops with evidence. | The same with Delta, after an external OPTIMIZE and VACUUM. |

## Limits

- Until L4, a crash or a failed Delta commit between Committed and the
  authority advance loses that transaction from the Delta tables that had not
  committed it. A normal run and a clean restart lose nothing. (Closed by L4.)
- `recovery recover` and the pre-ingestion recovery of other roots do not
  touch Delta (L4). (Closed by L4: `recovery recover` rolls forward too.)
- Each open table keeps delta-rs's eager snapshot of its active files in
  memory (design §8). Opening reads each table's `_last_checkpoint`,
  checkpoint and log tail (one LIST of its `_delta_log/`), which
  `firehose_parquet_startup_list_requests` does not count: it counts dataset
  listings, and a resume still lists no data object.
- `validate`, `scan` and `inspect` still read the files, not the logs (L7).
