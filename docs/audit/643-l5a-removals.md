# Remove `merge`, `truncate` and `verify` (#643, lane L5a)

Refs #643; part of #463. Design: [`docs/design/delta-lake.md`](../design/delta-lake.md)
§7.1, §7.2, §8 and the L5 row of §11. Follow-up: #666. Index: the #643 rows of
the [audit index](README.md).

## Decision

The user decided on 2026-09-27:

- The v1.0.0 launch writes Delta Lake as its only format (#643). fireparq is a
  proof of concept: no legacy code, no compatibility shims and minimal
  configuration.
- Compaction and cleanup of the Delta tables are an off-the-shelf `deltalake`
  CronJob beside the writer (design §9), not fireparq logic.
- `verify` is deferred until after the launch. The launch removes it, its
  registry and its reports, and #666 brings it back as `merkle_v3` over pinned
  Delta snapshots.

So `merge`, `truncate` and `verify`, and everything only they used, are
removed with no compatibility path. Why each one goes:

- **`merge`**: the CronJob's OPTIMIZE compacts closed dates through the Delta
  log, beside a running `build` (§1.7, measured in the spike). A fireparq
  rewrite of committed parts would bypass the log that readers trust.
- **`truncate`**: deleting committed rows contradicts fireparq's authority,
  and `delta.appendOnly` refuses a data removal. There is no safe Delta
  equivalent; a bad dataset is rebuilt into a new root. An operator can still
  clear `appendOnly` and run a `deltalake` DELETE, outside fireparq's
  guarantees (§7.1).
- **`verify`**: `merkle_v2` hashes rows in path-sorted file order. OPTIMIZE
  changes file names and row order, and a directory walk after OPTIMIZE but
  before VACUUM sees tombstoned files as duplicates, so v2 roots mean nothing
  on Delta tables (§7.2).

L5 is split. L5a (this record) has no dependency. L5b removes the plain-Parquet
`OutputWriter` and readers after the Delta commit layer (L3) lands.

## Removed

**Commands and flags.** `fireparq merge` (`--compression`, `--flush-rows`,
`--flush-bytes`, `--dry-run`, `--cache-control`), `fireparq truncate`
(`-p/--partition`, `--dry-run`, `-y/--yes`) and `fireparq verify` (`--chain`,
`--table`, `--hash-strategy`, `--checks`, `--profile`, `--scope`,
`--no-fail-fast`, `--report-json`, `--publish-report`,
`--publish-report-path`, `--registry-path`, `--update-registry`), with their
help and examples. `Commands::{Merge, Truncate, Verify}` and their dispatch in
`blocks/src/bin/main.rs`. `build` keeps `--flush-bytes`, `--flush-rows`,
`--compression` and `--cache-control` / `CACHE_CONTROL`, which it uses too.

**`merge`.**

- `firehose-parquet/src/merge.rs`, `merge/engine.rs` (the crash-safe partition
  sequence shared by local and S3) and `merge/read.rs` (bounded pinned S3 read
  windows), with their tests and benchmarks.
- `firehose-parquet/src/merge_journal.rs`: the `_fireparq_merge.json` journal,
  its recovery (`recover_guarded_for_ingestion`), the legacy
  `.fireparq-merge.lock`, the crash hook `FIREPARQ_TEST_MERGE_CRASH_AT`, and
  the merge intent record `.fireparq-ingest/merge-intent.json`
  (`ControlKey::MergeIntent`, added by #655 in PR #668).
- `firehose-parquet/src/maintenance/compaction.rs`: the schema and value
  metadata checks, receipt stripping, streaming part writer and merge encoder.
- `firehose-parquet/src/s3/delete.rs`: the single-attempt, bounded-concurrency
  maintenance deletes (#523). Only `merge` and `truncate` deleted through it;
  ingestion's Writing rollback deletes its own parts in `ingest/parts.rs`.
- In `ingest/maintenance.rs`: the merge-journal scan before recovery
  (`validate_ingestion_recovery_order` and `MergeJournals`, including the
  conditional `MergeJournals::IfIntended` scan of #668), `prepare_ingestion`
  (which finished journals at a `build` start while an intent existed),
  merge-journal recovery in `recover_roots` and for legacy trees
  (`recover_selected_legacy_merges`), and `PreparedMaintenance::recovered_merges`.
  `build` (`ingest/session.rs`) no longer calls either check.
- `writer::properties::for_schema`, the streaming writer properties only the
  merge encoder used (and its variant in `examples/bench_lookup_properties.rs`).
- `firehose-parquet/tests/maintenance_output_properties.rs` and
  `blocks/tests/maintenance_crash_hooks.rs`, whose only case was the merge
  crash hook.

**`truncate`.** `firehose-parquet/src/truncate.rs` and
`date_partition::is_date_value_pattern`, which only checked `truncate -p date=`
globs.

**`verify`.**

- `firehose-parquet/src/verify.rs`, `verify/row_encoding.rs` (`merkle_v2`) and
  `verify/tests/`; `ingest/observe.rs`, the read-only authority observation
  only `verify` used; the `tiny-keccak` dependency of `firehose-parquet`.
- The registry `_fireparq/merkle_roots.parquet` and the reports
  `_fireparq/verify_runs/`: `artifacts::DatasetArtifact` (`MerkleRoots`,
  `VerifyRuns` and `CursorMirror`, whose paths nothing else resolved through
  it) with its join helpers, `legacy_artifact_refusal`,
  `MERKLE_ROOTS_FILENAME`, `VERIFY_RUNS_DIR` and `RESERVED_ARTIFACT_FILENAMES`.
- The verify tests of `blocks/src/schema_contract_tests.rs` (every table
  hashes, and records and rematches a root), the real-binary
  `verify_runs_beside_a_live_build_and_leaves_its_partitions_open`, and the
  `verify` runs of `output_is_the_dataset_root_and_every_command_follows_it`
  (`blocks/tests/ingestion_transactions.rs`).
- Docs: `docs/verify-report-contract.md`,
  `docs/verifiability-artifact-runbook.md` and
  `docs/verifiability-hash-strategy.md` are **deleted**, not replaced by
  pointers: a pointer file is legacy the launch does not want, and #666 needs
  the full text, which git keeps. They are at the L5a base commit `4bab87a`:
  [hash strategy](https://github.com/pinax-network/firehose-parquet/blob/4bab87a/docs/verifiability-hash-strategy.md),
  [report contract](https://github.com/pinax-network/firehose-parquet/blob/4bab87a/docs/verify-report-contract.md),
  [artifact runbook](https://github.com/pinax-network/firehose-parquet/blob/4bab87a/docs/verifiability-artifact-runbook.md),
  and the code at
  [`firehose-parquet/src/verify.rs`](https://github.com/pinax-network/firehose-parquet/blob/4bab87a/firehose-parquet/src/verify.rs)
  and [`verify/row_encoding.rs`](https://github.com/pinax-network/firehose-parquet/blob/4bab87a/firehose-parquet/src/verify/row_encoding.rs).
  No row-hashing helper is kept: nothing else used one, and dead code is not
  kept for a later lane.

**Maintenance ownership paths only these commands used.**

- `MaintenancePolicy` (`Merge`, `Truncate` and `Recover`; only `recovery`
  acquires now, so the policy argument goes), `acquire_blocking`,
  `validate_artifact_destinations` and the `artifact_output` flag of
  `MaintenanceTarget`.
- `DatasetOwnership::validate_local_trees` (the tree walk `prepare_ingestion`
  ran before its journal scan) and `DatasetOwnership::release_blocking` (only
  `merge` and `truncate` released synchronously). `validate_local_mutation_trees`
  no longer counts the directories it reads, which only the startup listing
  metrics of the journal scan used.
- `cli::resolve_destructive_input_path` (no `S3_BUCKET` fallback for commands
  that delete) and `cli::reject_implicit_s3_write` (verify's refusal to write
  artifacts through the shorthand).
- The local walker policies only these commands used: `MUTATION_PARQUET`,
  `VERIFY_PARQUET`, `named` and `named_any`, and `discovery::first_object`.
  `collect_local` is now the one read-only walker of `scan`, `inspect` and
  `validate`, and takes no policy.

**Tests.** The merge, truncate and verify tests in `cli/tests.rs`,
`ingest/maintenance/tests.rs` (journal coexistence, bound and unbound journal
recovery, legacy journal recovery, protected merge, merge intent, truncate
refusal, remote journal recovery, malformed journal) with
`ingest/maintenance/tests/remote_deletion.rs`,
`ingest/session/maintenance_tests.rs` (journal coexistence),
`ingest/session/tests/resume_cost.rs` (the merge-intent listing),
`maintenance/discovery/tests.rs` (the merge, truncate and verify walkers) and
`writer/properties/tests.rs` (the streaming properties).

## Kept

- `build`, `recovery`, `inspect`, `validate` and `scan`. L7 changes `validate`
  and `scan` for Delta later.
- The plain-Parquet `OutputWriter`; L5b removes it after L3.
- The `_fireparq/cursor.parquet` mirror, `ARTIFACTS_DIR`,
  `DEFAULT_CURSOR_MIRROR` and `is_reserved_artifact_path`. The whole
  `_fireparq/` subtree stays reserved, so a leftover
  `_fireparq/merkle_roots.parquet` or `_fireparq/verify_runs/` is skipped, not
  read as table data. A root `cursor.parquet` stays reserved because
  `--cursor cursor.parquet` still puts the mirror there.
- `ingest/maintenance.rs`, reduced to what `recovery recover` and `build` use:
  protected-root discovery and recovery (`acquire`, `MaintenanceTarget`,
  `ProtectedRoot`, `PreparedMaintenance`) and the overlapping-root check
  `validate_ingestion_target` (#655). The module keeps its name to keep the
  parallel L3 rebase small.
- `maintenance/discovery.rs`: `visit_objects`, `ListingStats`,
  `list_objects`, `read_object_bytes`, `relative_key` and `collect_local`.
- `writer::properties::for_batch`, the properties of every ingestion part.
- `DatasetOwnership::acquire_blocking`, which only tests used before this
  change too.
- `firehose_parquet_startup_list_requests` and
  `firehose_parquet_startup_listing_seconds`: they still report the creation
  and ancestor listings of a `build` start.

## Behavior changes

- `fireparq merge`, `truncate` and `verify` fail with `unrecognized subcommand`,
  and the root help lists none of them.
- A resumed `build` never lists data: with the merge intent gone, there is no
  case left in which it scans the dataset for journals. A leftover
  `.fireparq-ingest/merge-intent.json` or `_fireparq_merge.json` of an old
  dataset is not looked at; such a dataset is refused anyway (mapper epoch
  `v3`, L2), and old data is rebuilt into a new root.
- `recovery recover` recovers protected ingestion and the cursor mirror only,
  and prints `{"recovered_protected_roots": N}` (the
  `recovered_merge_journals` field is gone).
- `merkle_roots.parquet` and `verify_runs/` are no longer reserved names
  outside `_fireparq/`: a walker that starts at a dataset root reads a root
  `merkle_roots.parquet` of a pre-v1.0.0 dataset as table data. Delete it.

## Superseded records

This removal supersedes the `merge`, `truncate` and `verify` parts of:
[#479](https://github.com/pinax-network/firehose-parquet/issues/479) (schema
and value metadata checks),
[#480](https://github.com/pinax-network/firehose-parquet/issues/480) (merge
journal), [#481](https://github.com/pinax-network/firehose-parquet/issues/481)
(`truncate` filters and `--yes`), [#487](https://github.com/pinax-network/firehose-parquet/issues/487)–[#490](https://github.com/pinax-network/firehose-parquet/issues/490)
and [#521](https://github.com/pinax-network/firehose-parquet/issues/521)
(`merkle_v2`, the registry and streaming roots, the starting point of #666),
[#523](523-s3-maintenance-concurrency.md) (maintenance deletes and merge
reads), [#529](529-maintenance-engine.md) (the shared maintenance engines),
[the #468 verify ownership record](468-verify-ownership.md),
[the verify follow-ups](validation-verify-followups.md) (PR #621),
[the maintenance safety follow-ups](maintenance-safety-followups.md), and the
registry and report parts of [#647](647-fireparq-artifact-dir.md) and the
merge-intent part of [#655](655-resume-cost.md). The records stay as history.
The #473 record (responsive shutdown) has no `merge`, `truncate` or `verify`
part and is unchanged.

## Validation

Commands, from the worktree:

```
cargo fmt --all
cargo build --workspace --all-targets
cargo test --workspace --locked --no-fail-fast
cargo run -p blocks --example dump_schemas
```

- Workspace tests: 957 passed, 0 failed, 14 ignored, and
  `cargo test -p blocks --example refresh_evm_golden --locked` passes.
  `cargo build --workspace --all-targets` has no compiler warnings.
- `dump_schemas` rewrote `docs/schemas/README.md`, whose conventions no longer
  list the Merkle registry and verify reports under `_fireparq/`; the drift
  test passes.
- New CLI tests: `merge`, `truncate` and `verify` are
  `InvalidSubcommand` errors, and the root help names none of them
  (`blocks/src/bin/main.rs`, `firehose-parquet/src/cli/tests.rs`).
- `blocks/tests/dataset_ownership.rs` now checks that `recovery recover`
  conflicts with a descendant owner, where it checked `merge` and `truncate`.
- No real endpoint or bucket was used: tests run against loopback mock
  Firehose servers and in-memory or loopback S3, in temporary directories with
  a cleared environment.

## Limits

- Until L3 and L9 land, fireparq output has no compaction at all: `merge` is
  gone and the CronJob has nothing to commit against yet.
- The k8s-parquet deployment docs live in another repository; their `merge`
  and `verify` jobs are not changed here.
