# Verify follow-ups from the maintenance validation pass

A validation pass at main `9372f99` reproduced five gaps in `fireparq verify`
after the #487–#490 root work, the #521 streaming rewrite and the #591
ownership stage. This record covers the diagnosis, the selected behavior, the
tests and the end-to-end checks. Refs #487, #488, #489, #490, #510 and #591;
part of #463.

`merkle_v2` and the typed row encoding are unreleased (the next release is
v1.0.0), so the encoding addition is folded into `merkle_v2`.

## 1. Struct columns had no `merkle_v2` encoding

Cosmos `transactions.fee_amount` and `signer_infos` are `List<Struct>` since
#510. `verify` failed on them with `Arrow type Struct(...) has no merkle_v2
encoding`, so no Cosmos `transactions` root could be computed.

A struct now encodes as `u32le(field_count)` followed by each field's value, in
declared field order, each with its own null tag. A null struct is only the
null tag; its fields are not read, so child values Arrow keeps under a null
parent cannot affect the root. Field names are not encoded (the column name is).
The rule nests, so `List<Struct<...>>` and structs holding lists work. The spec,
with golden values, is in `docs/verifiability-hash-strategy.md`.

Adding a rule for a type that had none cannot change a root an earlier build
could compute, because such a column made `verify` fail. `merkle_version`
therefore stays `merkle_v2`; the spec now says so explicitly, and that changing
an existing rule still requires a new version.

No other Arrow type is missing: the schema contract fixtures produce exactly
one previously unsupported type (`Struct`, inside `List`) across every table of
every chain.

Tests:

- Golden vectors for a struct, null children, a null struct over non-null
  children, a sliced struct, an empty struct, `List<Struct>` (including an
  offset into shared struct values and a null element, and `LargeList` parity)
  and `List<Struct<UInt64, List<Utf8>>>`. They were computed with an
  independent Python reference encoder,
  [`validation-verify-struct-vectors.py`](validation-verify-struct-vectors.py),
  written from the spec without Arrow or fireparq code.
- An unsupported struct field is named in the error.
- `verify_hashes_every_table_of_every_chain` (`blocks/src/schema_contract_tests.rs`)
  hashes every flushed table of every chain under every bytes encoding and both
  `fork_step` settings with both hash strategies, and asserts the Cosmos
  nested columns are among them.
- `verify_records_and_rematches_every_table_of_every_chain` writes every table
  of every chain (binary and hex encodings) with the production writer, runs
  `verify_parquet` on each table directory, and checks that a second run
  matches the recorded root.

## 2. `verify` could not run while `build` owned the network

Since #591, `verify` with roots or report output acquired exclusive dataset
ownership: of the chain root locally, of the whole bucket on S3. It failed at
once with `cannot acquire Exclusive dataset ownership` while `build` ran, so
#489's handling of partitions a live build is writing could never run.

### Decision

`verify` now reads table data **without any dataset ownership**, and writes
only its own artifacts under their own atomic or conditional protocol. A
shared lock was not an option: a shared directory lock conflicts with `build`'s
exclusive one, and S3 ownership has no shared mode. Soundness instead comes from
four rules:

1. **Frontier before listing.** For a roots run, `verify` reads where the
   writer stands before it lists the files it scans. For a protected dataset
   that is the authoritative ingestion state (`.fireparq-ingest/state.json`),
   read as it is: no ownership, no recovery. Writers replace it atomically.
   Transactions publish every part before they commit, and authority advances
   only after the commit, so every row at or below the frontier is in the
   listing. Legacy datasets keep using `cursor.parquet`.
2. **Open partitions.** Rows after the frontier are open (uncommitted, or past
   the last legacy cursor save). While the stream is unfinished, the partition
   holding the last block at or before the frontier is open too. #489 opened
   only the newest partition; that missed a real race, in which a running
   transaction had published a later partition's part but not yet the part of
   the partition holding the frontier. An unfinished reversible stream opens
   every partition (a reorg can append rows for earlier blocks), and so does a
   partition without `block_num` values. #489 used to record everything in
   the latter case. A completed request counts as finished only while the
   frontier is still exactly at its stop. `completed_stop` persists when a
   longer request extends the stream, and the mirrored cursor's `stop_block`
   carried that stale bound, so #489's `last + 1 >= stop` check would have
   declared an extending build finished.
3. **Unchanged snapshot.** `verify` records the identity of every file it reads
   (local fstat of the handle actually read: device, inode, size, mtime; S3:
   ETag, version, size, last-modified, with each GET pinned by `If-Match` to
   the listed ETag). After the scan it lists the table again. A partition that
   would be compared or recorded and that gained, lost or replaced a file fails
   the run before anything is compared or written. This covers a concurrent
   `merge`, `rollup` or `truncate`. Without ownership, none of them can be
   excluded, but none can go unnoticed during the read either. The snapshot is
   the one validated by that second listing; a change after it is an ordinary
   later change for the next run.
4. **Artifact writes keep their own guarantees.** Locally, the registry lock
   file (`merkle_roots.parquet.lock`) plus a temporary file, fsync and rename.
   On S3, one conditional put on the ETag read before comparing, with the
   zero-retry mutation client. A put whose response was lost can land later
   only if the registry is still unchanged, and then it holds exactly the rows
   the run computed. The bucket-wide owner is not taken, so `verify` does not
   block or wait for `build`. Artifact destinations still get the protected
   dataset guards from maintenance (no control path, cursor mirror, recovery
   metadata or ordinary protected data part); the markers and mirror bindings
   they need are read without ownership.

Protocol-only runs stay read-only as before.

### Limits

- Time partitions follow block timestamps. Where timestamps can decrease
  (Bitcoin), a block can land in a partition treated as closed. During a run,
  rule 3 fails the run; afterwards, the next run reports a mismatch for that
  partition. This limit already applied to #489's open detection.
- A legacy dataset whose cursor lives outside the chain root (`build
  --cursor`) is not detected, as before. Legacy datasets cannot be appended to
  by the current `build`.
- The cooperating-writer assumptions of #468 still apply: a process that
  rewrites a file in place without changing its size, inode or mtime is not
  detected.

### Tests

- A real `fireparq build` (protected ingestion, local mock Firehose kept open)
  commits blocks 100–105 into two-block partitions and keeps streaming. While
  it holds the dataset, a real `fireparq verify` records `block_range=100-102`
  and `102-104`, reports `104-106` as `open`, and a second run matches both
  (`verify_runs_beside_a_live_build_and_leaves_its_partitions_open`).
- With the real ingestion controller: the partition holding the last committed
  block and a partition with an uncommitted part stay open while `build` owns
  the dataset, a completed request closes the former, and an extension past a
  completed stop reopens the stream. The same protected state read from an
  in-memory bucket gives the same result.
- A part replaced, added or removed in a closed partition between the scan and
  the check fails the run with nothing written, locally and on S3. A part added
  to an open partition does not.
- Tables without `block_num` stay open while the stream is live.
- Registry and reports are written while another process owns every scope. S3
  registry and report writes make exactly one attempt and never create an
  owner record. A lost acknowledgement leaves the accepted object in place.
- Writing the registry into a protected partition, over `cursor.parquet` or
  under `.fireparq-ingest` is refused.

## 3. A custom `--registry-path` inside the verified path was hashed

A registry such as `<table>/roots.parquet` has a non-reserved name, so the
next run scanned it as table data and failed (`cannot infer the chain`).

Scans now skip the configured `--registry-path`, `--report-json` and
`--publish-report-path` files whatever their names. Local paths match after
canonicalizing their parent directory, so an aliased spelling is skipped too;
S3 paths match by bucket and key. Reports are JSON, so earlier reports are never
read as data. A registry inside the table directory under a non-reserved name
also gets a warning, because `merge`, `rollup` and `validate` still read it.

The regression runs `verify` three times with a registry, a report named
`.parquet` inside a partition and a published report inside the table
directory, and once more through a directory alias. Without the fix it fails.

## 4. `verify` recovered interrupted merges

Acquiring ownership ran maintenance recovery, which finished or rolled back
interrupted merges (deleting files) and recovered pending ingestion
transactions before hashing, with no trace in the report.

**Decision: refuse, do not recover.** A read-only command must not delete data,
and without ownership it cannot recover safely anyway. When the scanned tree has
a merge journal (`_fireparq_merge.json`), or the partition of a verified file
does, `verify` fails before reading any row, whatever the checks:
`cannot verify <path>: it has an unfinished merge (<journal>). ... run
`fireparq recovery recover <path>` ... then re-run verify`. The refusal is also
logged as a warning. The check runs again on the second listing, so a merge
that starts during the scan is refused too. A pending ingestion transaction is
not an error: its parts are after the frontier and stay open.

The regression crashes a real `merge` after it wrote its output (sources and
output coexist), then shows that roots and protocol-only runs both refuse with
the file tree byte-for-byte unchanged, a single-file verify inside the claimed
partition refuses too, and after `recovery recover` the run records roots.

## 5. Stale documentation

- `docs/releases/unreleased.md` (#489 entry) and the runbook's Open Partitions
  section promised live-build support that could not run, and described the
  cursor-only rule. Both now describe the frontier sources and rules above.
- Both claimed S3 registry writes retry up to five times with an unconditional
  fallback. The code makes one conditional put with no retry and no fallback,
  and the docs now say so.
- The #591 ownership entry, the report contract, the CLI help and the #468
  verify-ownership, protected-maintenance and runtime records now state that
  `verify` takes no ownership and never recovers.

## Validation

On the branch rebased onto main `8462692` (#614), `cargo fmt --all -- --check`
and `cargo test --workspace --locked` passed: **1,113 tests, 0 failures, 14
intentional ignores**, with no compiler warnings. CI's extra steps also passed
locally: the `refresh_evm_golden` example test, the `fireparq` build and the
bash, zsh and fish completions. The focused verify suite has 56 tests (15
row-encoding tests, 11 concurrency tests and 30 others); the two new
schema-contract tests and the live-build integration test run in the `blocks`
crate.

## End-to-end checks

All runs used local copies only, absolute paths, and a working directory
outside the repository with every `S3_BUCKET`/`AWS_*` variable cleared. No
Firehose endpoint or bucket was contacted. The branch release binary was
compared with the main binary at `9372f99`.

- **Cosmos.** The retained #510 fixture (`cosmos-33121486`) was prepared with
  `510-qualify.py` (replay SHA-256 `0cc9f598…` as recorded) and replayed with
  `replay_cosmos`. The main binary fails on `transactions` with `column
  fee_amount: Arrow type Struct(...) has no merkle_v2 encoding`. The branch
  records `blocks`, `events` (1,138 rows), `messages` and `transactions`, and a
  second run matches each.
- **Every EVM table.** On the retained 30-block mainnet dataset (e2e-478),
  `verify` with roots and protocol checks recorded all 13 tables (78 partition
  roots, 36 protocol passes) and a second run matched all of them. The 78 roots
  are identical to the main binary's, so no existing encoding changed.
- **Ownership held.** While another process held an exclusive lock on the
  chain root, the main binary failed with `cannot acquire Exclusive dataset
  ownership`; the branch recorded six roots and published its report.
- **Custom registry.** With `--registry-path <table>/roots.parquet`, the main
  binary's second run failed; the branch passed three runs with the warning.
- **Interrupted merge.** After a `merge` aborted with
  `FIREPARQ_TEST_MERGE_CRASH_AT=after-outputs`, the main binary's verify exited
  0 after silently deleting files and the journal. The branch refused with the
  file tree unchanged; after `fireparq recovery recover` it recorded six roots.
- **Concurrent rewrite.** A `gas_changes` part in a closed partition was
  replaced while `verify` read the table. The run failed with `the data changed
  while verify was reading it` and no registry was written.
