# Remove `partitions.parquet` and the `partitions` subcommands (#653)

Closes #653 (PR #TBD); part of #463.

## Decision

`fireparq partitions build` wrote a block-to-partition index,
`_fireparq/partitions.parquet`, and `partitions ls`, `validate`, `resolve` and
`shard` read it. The v2 index (#486) proved finalized coverage with its own
Firehose probes. The user decided on 2026-09-27:

- fireparq is a proof of concept. Compatibility may break, no legacy code or
  compatibility shim is kept, and configuration stays minimal.
- The launch release, v1.0.0, writes Delta Lake as its only format (#643). The
  Delta log records the files of each `date` partition with per-file
  `block_num` and `timestamp` statistics, so readers get partition pruning and
  block-range lookups without a custom index that no engine understands.
- `--stop-date` (#645) was closed, so no command needs a day-boundary lookup.

So the index and every command that writes or reads it are removed, with no
compatibility path. This lands in the same v1.0.0 launch release as #643.

## Removed

**Commands and flags.** `fireparq partitions` and its subcommands `build`,
`ls`, `validate`, `resolve` and `shard`, with all their flags (`--partition`,
`--block-range-size`, `--live`, `--poll-interval-secs`, `--resume`,
`--overwrite`, `--partitions-index`, `--partition-type`, `--partition-value`,
`--partition-chain`, `--from`, `--to`, `--limit`, `--shard-count`,
`--shard-index`, `--strategy`, `--strict-single-chain`, `--all-spans`,
`--allow-gaps`, `--json`), help and examples. The root help no longer lists
`partitions`.

**`--cursor-template` / `CURSOR_TEMPLATE`.** `build` resolved the template with
an empty context: it supplied no variable, not even `{chain}`, and the partition
variables (`{partition_type}`, `{partition_value}`, `{partition_from}`,
`{partition_to}`) had no producer at all. Without them a template was a literal
path equal to `--cursor`, so the flag had no remaining purpose and is removed
too. `--cursor` takes the same path (relative to the dataset root, absolute, or
`s3://`). `{chain}` stays, in `--output` only; `parse_template` remains the
`--output` parser.

**Library code.**

- `firehose-parquet/src/cli/partitions/` (`mod.rs`, `io.rs`, `queries.rs`): the
  `Partition*` request, result and row types, strict v2 index IO and
  publication (`read_verified_partitions_index*`,
  `write_verified_partitions_index`), `partitions_index_path_in`,
  `parse_partition_build_types`, and resolution, listing, sharding and
  validation.
- `firehose-parquet/src/partition_index.rs` and `partition_index/{builder,scan}.rs`:
  the coverage proof model, the exact span builder and the bounded time scan.
- `firehose-parquet/src/grpc/finality.rs` (`FinalizedAnchor`,
  `FirehoseClient::finalized_anchor`, `FinalityTimeoutError`) and
  `grpc/finalized_range.rs` (`FinalizedMetadataStream`,
  `finalized_metadata_stream`): the finalized-coverage probing that existed
  only to build the index.
- The single-block Fetch path: `FirehoseClient::fetch_block_identity`, its
  shared fetch channel, `FetchTimeoutError`, `classify_fetch_error` and
  `FetchErrorKind`. Only the index's boundary probes used it.
- `DatasetArtifact::PartitionsIndex` and `PARTITIONS_INDEX_FILENAME` in
  `artifacts.rs`; `partitions.parquet` leaves `RESERVED_ARTIFACT_FILENAMES`.
- `ingest::prepare_partitions_index_write` and the legacy root-index refusal
  (`refuse_legacy_partitions_index`) from #650, and the crate-private
  `MaintenancePolicy::Artifacts`, which only the index write used. `verify`
  still checks its registry and report destinations with
  `validate_artifact_destinations` directly; the maintenance tests that used
  the policy as a generic read-only acquisition use `Recover`.
- In `ingest/eligibility.rs`, the exception that let a new protected stream
  initialize a root holding a same-chain verified `_fireparq/partitions.parquet`,
  and the dedicated refusal of a legacy root `partitions.parquet`.
- In `blocks/src/bin/main.rs`, `run_partitions_build` and its helpers: the
  `--start-block` inference from the index frontier or the sibling
  `_fireparq/cursor.parquet`, block-range alignment, probe retries
  (`ProbeRetryPolicy`, `retry_probe_fetch_with_policy`), the live retry delay,
  index metadata, and the dispatch and printing of every subcommand. The
  helpers only the index used go with it: `chain_uses_tron_style_evm_profile`
  (index file metadata) and `has_nullable_timestamps` (index routing policy).
  The `blocks` crate drops its direct `object_store` dependency, which only the
  index reader's not-found check used.
- `cli::resolve_cursor_template`, `cli::CursorTemplateContext` and
  `CommonArgs::cursor_template`.

**Tests.** `blocks/tests/partition_coverage.rs`,
`blocks/tests/partition_probe_failures.rs`, and the partitions unit tests in
`firehose-parquet/src/cli/tests.rs`, `blocks/src/bin/main.rs`,
`ingest/mod.rs`, `ingest/eligibility.rs`, the grpc auth and transport tests
(Fetch and finality paths) and the chain-profile oracle test. The mock Firehose
of `blocks/tests/dataset_ownership.rs` loses its `Finality` service, and the one
of `blocks/tests/ingestion_transactions.rs` loses its finality-probe answers and
its `Fetch` service. `partitions build` runs are dropped from the real-binary
tests that exercised every command (`endpoint_info_startup.rs`,
`dataset_ownership.rs`, `ingestion_transactions.rs`), and the
`--cursor-template` tests use `--cursor` or are removed.

**Docs.** `docs/partitions-build-defaults.md`, `docs/partitions-cli-migration.md`
and `docs/partitions-parquet-contract.md` are deleted.
`docs/partition-vocabulary.md` keeps only the `date=YYYY-MM-DD` key and the
flags that refer to it. The README `partitions` sections, flag tables and the
`--cursor-template` section are removed; `.env.example`, `docs/repo-navigation.md`,
the verify runbook and the v1.0.0 release notes follow.

## Kept

- The `date=YYYY-MM-DD` layout and `firehose-parquet/src/date_partition.rs`
  (#652), and `build`, `verify`, `recovery`, `merge`, `truncate`, `scan`,
  `validate` and `inspect`.
- The Stream client: `build` uses EndpointInfo, the healthcheck and the Stream
  RPC only, never Fetch or the finality proof. `checked_block_identity` stays
  because the Stream path validates every streamed identity with it.
- The `_fireparq/` reservation. Every walker still skips the whole subtree, so a
  leftover `_fireparq/partitions.parquet` is ignored rather than read as table
  data. The legacy root names that stay reserved are `cursor.parquet`,
  `merkle_roots.parquet` and `verify_runs/`.
- `{chain}` in `--output`, with the same parser and escapes.
- The Solana genesis routing timestamp. `build` imported it from
  `partition_index` (`SOLANA_GENESIS_TIMESTAMP`); it now uses the identical
  `ingest::SOLANA_GENESIS_ROUTING_SECONDS` that protected authority already
  validates, re-exported from `firehose_parquet::ingest`.
- The UTC second formatter that `validate` uses for timestamp reversals. It
  lived in `cli/partitions` (`format_partition_timestamp`) and moves into
  `cli/validate.rs`.
- The progress-log timestamp formatter of `build`, renamed from
  `format_optional_probe_timestamp` to `format_optional_block_timestamp`.
- The oracle copies named `infer_partitions_block_type` in
  `blocks/src/chain/tests.rs` and `blocks/src/bin/chain_profile_tests/`: they are
  verbatim historical helpers that check the chain-name inference `build` still
  uses.

## Behavior changes

- `fireparq partitions ...` and `--cursor-template` are unknown arguments;
  `CURSOR_TEMPLATE` is no longer read.
- A new `build` root must be empty (bucket ownership keys at a bucket root
  excepted). A root that holds only `_fireparq/partitions.parquet`, or a legacy
  root `partitions.parquet`, is refused like any other unrelated file, before
  any Blocks request. Delete the file or use a new root.
- A root `partitions.parquet` is no longer a reserved name, so walkers that
  start at a dataset root (`truncate`, `merge`, `validate`, `scan`, `verify`)
  may treat it as table data. Delete it.

## Replacement for consumers

The block range of one UTC day, from the plain Parquet output:

```sql
SELECT min(block_num), max(block_num)
FROM read_parquet('<root>/blocks/date=2026-09-25/*.parquet');
```

On the Delta output (#643), answered from the log's statistics:

```sql
SELECT min(block_num), max(block_num)
FROM delta_scan('<root>/blocks')
WHERE date = DATE '2026-09-25';
```

Operators replace `partitions resolve` plus `build --start-block/--stop-block`
with this query, and drop `partitions build` jobs.

## Superseded records

This removal supersedes the partitions parts of:
[#482](https://github.com/pinax-network/firehose-parquet/issues/482) (numeric `block_range` order),
[#483](https://github.com/pinax-network/firehose-parquet/issues/483) (index resume/overwrite rules),
[#485](485-partition-probe-reliability.md) (probe reliability),
[#486](486-partition-index-design.md) (verified v2 coverage),
[validation follow-ups 10-15](validation-misc-followups-partitions.md) (live
retries, index time type and v2 `partitions validate`) and the
`partitions.parquet` parts of [#647](647-fireparq-artifact-dir.md) and
[#654](654-output-template.md). The records stay as history.

## Validation

Commands, from the worktree:

```
cargo fmt --all
FIREPARQ_REQUIRE_DUCKDB=1 FIREPARQ_DUCKDB=/opt/homebrew/bin/duckdb \
  cargo test --workspace --locked --no-fail-fast
cargo test -p blocks --example refresh_evm_golden --locked
cargo run -p blocks --example dump_schemas
```

- Workspace tests: 1,096 passed, 0 failed, 15 ignored, with the DuckDB checks
  required (`engine_compat.rs`, `non_final_stream.rs`); Polars ran in CI only.
  `cargo build --workspace --all-targets` has no warnings.
- `dump_schemas` rewrote `docs/schemas/README.md`, whose conventions no longer
  list a partition index; the drift test passes.
- `fireparq partitions ...` fails with `unrecognized subcommand 'partitions'`,
  and the root help lists no `partitions` command.
- CI: TBD.
- No real endpoint or bucket was used: tests run against loopback mock Firehose
  servers and in-memory or loopback S3, in temporary directories with a cleared
  environment.

## Limits

- The k8s-parquet deployment docs live in another repository; their
  `partitions.parquet` references are not changed here.
- Until Delta output (#643) lands, the day-range lookup reads the `block_num`
  values of the plain Parquet files (DuckDB can answer `min` / `max` from
  row-group statistics).
