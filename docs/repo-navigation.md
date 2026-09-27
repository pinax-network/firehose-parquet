# Repository Navigation Guide

This document is a tool-agnostic map of the `firehose-parquet` workspace: where code lives, how data flows, and where to edit for common tasks.

Related docs:

- `docs/releases/`: release notes; `docs/releases/unreleased.md` collects changes merged since the last release.
- `docs/schemas/`: per-chain table and column reference, generated from the code (see "Change a table schema" below).
- `docs/audit/README.md`: issue-by-issue implementation and validation records of the September 2026 audit (#463).
- `docs/design/delta-lake.md`: the Delta Lake output design for the v1.0.0 launch (#643): dependency spike and pinned versions, protocol and table properties, the commit mapping onto the #468 transaction, crash matrix and VACUUM rule, ownership (#636), type mapping, removals, resume cost (#655), the maintenance CronJob, reader examples and the PR-sized implementation lanes.
- `docs/verifiability-hash-strategy.md`: `merkle_v2` row encoding, normalization rules and golden values.
- `docs/verifiability-artifact-runbook.md`: publishing, retaining and migrating verify artifacts (`_fireparq/merkle_roots.parquet`, `_fireparq/verify_runs/<run_id>/report.json`), including moving pre-v1.0.0 root artifacts into `_fireparq/`.
- `docs/verify-report-contract.md`: the verify report JSON contract.
- `docs/partition-vocabulary.md`: the single `date=YYYY-MM-DD` output key and the CLI terms that refer to it.
- `docs/network-registry-integration.md`: how built-in `--network` aliases are generated, the provider policy, and the endpoint check.

## Workspace Layout

- `Cargo.toml` (root): Rust workspace with three members, `firehose-protos`, `firehose-parquet` and `blocks`, sharing one `[workspace.package]` version.
- `firehose-protos/`: protobuf compilation crate.
  - `build.rs`: compiles `proto/*.proto` into Rust modules with `tonic-prost-build`; byte fields are generated as `Bytes`.
  - `src/lib.rs`: exposes the compiled modules and aliases (`firehose`, `eth`, `solana`, ...).
- `firehose-parquet/`: core library crate used by the binary.
  - CLI (`src/cli.rs` and `src/cli/`):
    - `cli.rs`: Clap definitions (`Cli`, `Commands`, `GlobalArgs`, `AwsArgs`, `CommonArgs`, `BuildArgs`) and the stable public re-exports.
    - `cli/configuration.rs`: CLI-to-`Config` conversion, value parsers, working-directory `.env` / `--env-file` loading, logging and completions.
    - `cli/paths.rs`: local/S3 input and output policy (explicit `s3://` for writes, `S3_BUCKET` shorthand for reads), credential preflight, and the `--output` dataset-root resolver (`resolve_output_root`, `{chain}` expansion, `{{`/`}}` escapes).
    - `cli/inspect.rs`, `cli/validate.rs`: read-only `scan` / `inspect` and canonical block `validate`.
    - `cli/{tests,validate_tests}.rs`: CLI and validation regressions.
  - Configuration and endpoints:
    - `src/config.rs`: pipeline `Config`, `Compression` (including `zstd:<level>`) and receive-transport defaults.
    - `src/date_partition.rs`: the `date=YYYY-MM-DD` partition key, the only output layout (#652): `DatePartition` formats it from a block time, parses it strictly, and gives its `Date32` value; `is_date_value_pattern` checks `truncate -p date=` globs.
    - `src/networks.rs`, `src/networks_generated.rs`: built-in `--network` aliases (generated; do not edit) and `FIREHOSE_ENDPOINT_*` overrides.
    - `src/auth.rs`: Firehose credential selection by resolved provider host, and explicit env-var selectors.
    - `src/grpc.rs`: the Firehose stream client (EndpointInfo, healthcheck and the Stream RPC), shared authenticated transport, reconnect/back-off/timeouts and fatal-status classification.
  - Protected ingestion (`src/ingest/`), used by every non-dry-run `build`:
    - `session.rs`: `IngestionSession`, assembling storage identity, eligibility, recovery and the accepted frontier; `ingestion_mutation_scopes` derives build ownership.
    - `controller.rs`: all-table publication and restart decisions: journal each flush, publish parts, advance authority, reconcile the mirror.
    - `controller/pipeline.rs`, `controller/lane.rs`: bounded concurrent encode/stage/receipt/publish of one transaction's parts (#516 stage A) and the scoped blocking lane for local part I/O.
    - `state.rs`: strict transaction identity and durable records (`PendingTransaction`, deterministic `part-v1-*` / `.fireparq-txn-*.tmp` names, `BlockFamily`, `MAPPER_EPOCH`).
    - `store.rs`, `parts.rs`: typed record transitions, and physical part ownership checks and cleanup.
    - `frontier.rs`: the accepted-event prefix; `binding.rs`: output and mirror identities from the actual storage configuration (`--cursor none` binds no mirror).
    - `eligibility.rs`: new streams may only initialize empty destinations; legacy data, cursors and any other file are refused.
    - `mirror.rs`: the non-authoritative cursor mirror (default `_fireparq/cursor.parquet`), reconciled from authority.
    - `maintenance.rs`: protected-root discovery and recovery before maintenance commands; `observe.rs`: read-only authority observation for `verify`.
  - Writing Parquet:
    - `src/writer.rs`: Parquet encoding, the `<table>/date=YYYY-MM-DD` directory of a flush (`ParquetTableWriter::partition_suffix`), the partition contract (every row's time and `date` column must match that directory) and the unprotected low-level `OutputWriter` (not used by protected `build`).
    - `src/writer/protected.rs`, `src/writer/protected/verification.rs`: prepared complete parts, publication and exact receipt/schema verification; `src/writer/protected/budget.rs`: the `--flush-inflight-bytes` encoded-byte budget.
    - `src/writer/local.rs`: atomic, synced publication of one local part; `src/writer/properties.rs`: bounded Bloom filters, row-group limits and sort metadata shared by ingestion and maintenance.
    - `src/flush.rs`: adaptive compressed `--flush-bytes` targets and the summed `--flush-memory-bytes` trigger.
    - `src/flush/pace.rs`: catch-up detection (#659): a pure `PaceDetector` that compares block time with the wall clock, so `--flush-interval-secs` applies only at the chain head.
    - `src/cursor.rs`: cursor Parquet row format and legacy inspection.
    - `src/encode.rs`, `src/encode/fixed_base58.rs`: byte encodings (`hex`, `hex_no_prefix`, `base58`, `tron_base58`, `binary`) and the fixed-width Base58 fast path.
    - `src/traits.rs`: `BlockMapper`, canonical identity columns, timestamp helpers, and the shared enum helpers and non-final event helpers (`StreamEvent`: the `fork_step` and `stream_ordinal` columns).
  - Ownership, durable state and S3:
    - `src/dataset_lock/{mod,local,operation,session}.rs`: local directory-inode ownership shared by all mutating commands; `DatasetOwnership::finish` ends `build` ownership (release after success or after a failure whose requests all had a definite outcome). `src/dataset_lock_s3.rs`: the persistent bucket-wide S3 owner (`.fireparq-owner-v1.json`) and its uncertainty latch.
    - `src/durable_state.rs`, `src/durable_state_s3.rs`: strict versioned local/remote control records and CAS tombstones.
    - `src/recovery.rs`: `fireparq recovery` (`status`, `recover`, `release`): ownership inspection, guarded transaction recovery and provider-quiescent S3 owner release.
    - `src/s3.rs`: shared AWS configuration and S3 client builders with explicit credential/retry policies (`build_ingestion_mutation_client`, `AwsConfig::build_read_client`).
    - `src/s3/upload.rs`: native ingestion uploads (disk spool, one conditional PUT, spooled readback verification, explicit timeouts).
    - `src/s3/delete.rs`: single-attempt, bounded-concurrency maintenance deletes.
  - Maintenance and verification:
    - `src/merge.rs`, `src/merge/engine.rs`, `src/merge_journal.rs`, `src/merge/read.rs`: `merge`, its crash-safe partition sequence shared by local and S3, the `_fireparq_merge.json` journal and recovery, and bounded pinned S3 read windows.
    - `src/truncate.rs`: `truncate` planning (`date=` filters, `--yes`) and deletion.
    - `src/maintenance/compaction.rs`: shared schema/value-metadata checks, receipt stripping, streaming part writer and the merge encoder.
    - `src/maintenance/discovery.rs`: shared local walker policies, S3 listing and whole-object reads for maintenance, `verify`, `scan` and `validate`.
    - `src/artifacts.rs`: the one place dataset artifact paths resolve: `DatasetArtifact` (`_fireparq/cursor.parquet`, `_fireparq/merkle_roots.parquet`, `_fireparq/verify_runs/`, plus their legacy root names) with local/S3 join helpers, `DEFAULT_CURSOR_MIRROR` (the clap default), the legacy-artifact refusal, and `is_reserved_artifact_path`, which every dataset walker uses to skip the whole `_fireparq/` subtree, the legacy root names and control state.
    - `src/verify.rs`, `src/verify/row_encoding.rs`, `src/verify/tests/`: `fireparq verify` (read-only scan, open partitions from the writer frontier, unchanged-snapshot check, atomic/conditional registry writes) and the `merkle_v2` row encoding.
    - `src/metrics.rs`: Prometheus registry, `/metrics`, `/health` and `/ready`.
  - `tests/`: Parquet compatibility and maintenance output-property integration tests.
- `blocks/`: chain-specific mapping crate and the unified `fireparq` binary.
  - `src/bin/main.rs`: `fireparq` entrypoint, command dispatch (`Commands::*`), shared ingestion helpers, and the `.env.example` drift test.
  - `src/bin/ingestion/mod.rs`: `run_ingestion`, the `fireparq build` orchestration; dataset ownership outlives the session and runtime.
  - `src/bin/ingestion/setup.rs`: endpoint preflight (EndpointInfo, block type, encodings), resumed configuration and metrics labels.
  - `src/bin/ingestion/runtime.rs`: ordered receive/filter/routing state and flush windows; only the borrowed session commits authority and mirrors.
  - `src/bin/chain_profile_tests/`: #526 oracle tests for chain detection and profiles.
  - `src/chain.rs`: `ChainKind` / `ChainProfile`, the per-family facts (label, `type_url` marker, protected family, encoding contract, nullable timestamps, block gaps, extended/votes handling, failed-transaction default, chain-name rules) and mapper construction.
  - `src/<chain>/{mod,proto,schema,mapper}.rs` for `evm`, `solana`, `beacon`, `near`, `antelope`, `tron`, `cosmos`, `bitcoin`: protobuf aliases, Arrow table schemas and row mapping, plus chain-specific helpers (`evm/decimal.rs`, `solana/vote.rs`, `tron/contracts.rs`, `bitcoin/amounts.rs`, `cosmos/tx_metadata.rs`, `antelope/text.rs`) and value/outcome tests.
  - `src/schema_docs.rs`: renders `docs/schemas/<chain>.md` from the schema constructors; `examples/dump_schemas.rs` writes the files and a test fails when they drift.
  - `src/schema_contract_tests.rs`: every table of every chain, under every encoding and both `fork_step` settings, has unique names and round-trips through Parquet.
  - `src/mapping_bench.rs`: ignored whole-block mapping benchmarks.
  - `examples/`: replay, benchmark and golden-refresh tools (`replay_*`, `bench_*`, `measure_flush_sizing`, `refresh_evm_golden`, `dump_schemas`). `bench_ingestion_concurrency` is the #516 flush-concurrency benchmark; `bench_live_flush` runs the real binary against a looping mock Firehose and a loopback HTTPS S3 with injected latency to measure #658 catch-up throughput and commit phases.
  - `tests/`: real-binary integration tests against a mock Firehose (`ingestion_transactions.rs`, `dataset_ownership.rs`, `endpoint_info_startup.rs`, `non_final_stream.rs`, `metrics_readiness.rs`, `shutdown_signals.rs`, and `adaptive_flush.rs`, which paces its mock faster than or at real time), the DuckDB and Polars engine test (`engine_compat.rs`, with `tests/engines/`), the maintenance crash-hook gating test (`maintenance_crash_hooks.rs`) and the offline EVM golden regression (`evm_golden.rs`, `tests/fixtures/`).
- `spikes/delta-lake/`: the #643 Delta Lake spike, a standalone crate outside the workspace (its own `[workspace]`, `Cargo.lock` and `rust-toolchain.toml`, because `deltalake-core` 1.0.0 needs Rust 1.94.1 and Arrow 59). It commits pre-written Parquet 60 parts to Delta tables (`src/{mapping,part,delta,storage}.rs`, `tests/spike.rs`), and `run.sh` adds loopback S3 (`py/loopback_s3.py`), `deltalake` maintenance beside the writer (`py/concurrent_maintenance.py`) and DuckDB/Polars reads (`py/read_check.py`). Nothing in `fireparq` depends on it; see `docs/design/delta-lake.md`.
- `proto/`: source `.proto` files and Buf config, including `proto/core/*` dependencies.
- `scripts/`: `generate_networks.rs` (the `generate-networks` bin that writes `firehose-parquet/src/networks_generated.rs`) and `check_network_endpoints.sh` (live check of every built-in endpoint).
- `.env.example`: every environment variable the CLI reads, kept in sync by a test.
- `deny.toml`: cargo-deny advisory policy.
- `.github/workflows/`: `ci.yml`, `advisories.yml`, `docker-publish.yml`, `release.yml`, `network-endpoints.yml`.

## Data-Flow Mental Model

1. `blocks/src/bin/main.rs` parses the CLI (`firehose-parquet/src/cli.rs`), loads `.env` from the working directory or `--env-file`, and dispatches. `fireparq build` (`Commands::Build(BuildArgs)`) calls `ingestion::run_ingestion` in `blocks/src/bin/ingestion/mod.rs`.
2. `ingestion/setup.rs` resolves the endpoint and provider-scoped credentials (`auth.rs`), requires EndpointInfo (`grpc.rs`), selects the `ChainKind` (`blocks/src/chain.rs`: from `--block-type`, the endpoint chain names, or the first payload's `type_url` in a dry run) and resolves the dataset root (`resolve_output`, which calls `firehose_parquet::cli::resolve_output_root`: `--output` as given, with an opt-in `{chain}` placeholder expanded to the EndpointInfo chain name) and cursor mirror.
3. Mutating commands acquire dataset ownership first (`dataset_lock/`, `dataset_lock_s3.rs`). `ingest/session.rs` then recovers any pending transaction and opens the accepted frontier from the authority under `<dataset root>/.fireparq-ingest/` before any Blocks request.
4. Stream messages come from `grpc.rs`. `ingestion/runtime.rs` filters, orders and routes them; the chain mapper (`blocks/src/<chain>/mapper.rs`) decodes protobuf blocks and appends Arrow columns using `schema.rs`.
5. When a flush trigger fires (`flush.rs`, partition boundaries, completion), `ingest/controller.rs` journals the all-table transaction, `writer/protected.rs` publishes the deterministic parts (local via `writer/local.rs`, S3 via `s3/upload.rs`) and verifies them, and the controller advances authority, then the optional `_fireparq/cursor.parquet` mirror.
6. `metrics.rs` exposes counters, readiness and health.
7. Maintenance (`merge`, `truncate`) runs the shared engines under the same ownership, after `ingest/maintenance.rs` recovers protected roots. `verify` reads without ownership, using `ingest/observe.rs` to find open partitions.

## Where To Edit For X

- Add/change a CLI flag or subcommand:
  - `firehose-parquet/src/cli.rs` (flags and subcommands); `cli/configuration.rs` (config conversion and value validation); `cli/paths.rs` (input/output policy).
  - `blocks/src/bin/main.rs` (dispatch) and `blocks/src/bin/ingestion/setup.rs` (ingestion configuration).
  - Update `.env.example` when a flag reads an environment variable (a test in `blocks/src/bin/main.rs` fails otherwise), the README flag tables, and `docs/releases/unreleased.md`.
- Change a table schema (add, rename or retype a column, add a table):
  - `blocks/src/<chain>/schema.rs` and `mapper.rs`.
  - Regenerate the reference with `cargo run -p blocks --example dump_schemas` and commit `docs/schemas/<chain>.md`; the drift test fails otherwise.
  - A schema change changes the table digests bound by protected output, so existing roots refuse to resume: document it in `docs/releases/unreleased.md` as a rebuild-required change. Row or routing changes without a schema change must advance `MAPPER_EPOCH` in `firehose-parquet/src/ingest/state.rs`.
  - `blocks/src/schema_contract_tests.rs` covers every table; add fixture rows for a new table.
- Add a new chain family:
  - `blocks/src/<chain>/{mod,proto,schema,mapper}.rs` (exported from `blocks/src/lib.rs`); use the shared `firehose_parquet::traits` helpers (`push_fork_step_field`, `fork_step_builder`, `append_fork_step`, `finish_fork_step`, `est_fork_step`, `enum_data_type`, `estimated_dictionary_index_bytes`, `strip_enum_prefix`) instead of per-chain copies.
  - `blocks/src/chain.rs`: add a `ChainKind` variant (append it to `ChainKind::ALL`, which is also the `type_url` detection order), its `ChainProfile` in `ChainKind::profile` and its constructor in `ChainKind::create_mapper`. Add ordered `CHAIN_NAME_RULES` entries for endpoint-name inference. Fill `strict_chain_names` when the family has votes, nullable timestamps or unsupported extended output, because those flags are resolved before the first block.
  - `firehose-parquet/src/ingest/state.rs` (`BlockFamily`) and `ingest/mirror.rs`: add the protected family; its serde name must equal the profile label.
  - `blocks/src/bin/main.rs` (`BLOCK_TYPES`, the unsupported-type error list) and the `--block-type` help in `firehose-parquet/src/cli.rs` (`BuildArgs`). A test checks that `BLOCK_TYPES` matches `ChainKind::ALL`.
  - `blocks/src/schema_docs.rs`: add the chain so `docs/schemas/<chain>.md` is generated.
  - The #526 oracle tests (`blocks/src/chain/tests.rs`, `blocks/src/bin/chain_profile_tests/`) cover the eight current families; extend their expected lists and re-pin the schema digest.
- Adjust per-family behavior (encodings, nullable timestamps, block gaps, extended/votes, failed-transaction defaults):
  - `blocks/src/chain.rs` (`ChainProfile`); ingestion reads these properties instead of comparing `block_type` strings.
- Change partitioning or output file layout:
  - `firehose-parquet/src/date_partition.rs` (the `date=YYYY-MM-DD` key) and `firehose-parquet/src/writer.rs` (`ParquetTableWriter::partition_suffix` and the partition contract).
  - A layout change must advance `MAPPER_EPOCH` in `firehose-parquet/src/ingest/state.rs`, so protected roots of the old layout are refused instead of resumed into a mixed layout; `PendingTransaction::validate` checks recorded partitions against the layout.
  - `blocks/tests/engine_compat.rs` reads real output with DuckDB and Polars.
  - Artifact locations (`_fireparq/`) and what walkers reserve: `firehose-parquet/src/artifacts.rs` only; the dataset root must hold only table directories, `_fireparq/` and dot-prefixed control state (`assert_dataset_root_layout` in `blocks/tests/ingestion_transactions.rs`).
  - `firehose-parquet/src/ingest/state.rs` (deterministic `part-v1-*` and `.fireparq-txn-*.tmp` names) and `firehose-parquet/src/writer/protected.rs` (staging, publication, receipt verification).
  - `blocks/src/bin/ingestion/runtime.rs` (flush windows and partition-boundary flushes), `firehose-parquet/src/flush.rs` (size and memory triggers) and `firehose-parquet/src/flush/pace.rs` (when the interval applies); the trigger order is `next_mapper_flush_trigger` in `blocks/src/bin/main.rs`.
  - `firehose-parquet/src/ingest/controller/pipeline.rs` and `firehose-parquet/src/writer/protected/budget.rs` for flush concurrency and the in-flight byte budget.
  - `firehose-parquet/src/writer/properties.rs` for row groups, Bloom filters and sort metadata.
- Change resume, cursor or recovery behavior:
  - `firehose-parquet/src/ingest/{session,controller,state,store,parts,frontier,binding,eligibility,mirror}.rs`.
  - `firehose-parquet/src/cursor.rs` (row format and legacy inspection) and `firehose-parquet/src/recovery.rs` (`fireparq recovery`).
  - `blocks/src/bin/ingestion/{setup,runtime}.rs` (request defaults and ordered receipt/mapping queues).
  - Prefer real-binary regressions in `blocks/tests/ingestion_transactions.rs`.
- Change S3 behavior:
  - `firehose-parquet/src/s3.rs` (client policies), `s3/upload.rs` (ingestion uploads), `s3/delete.rs` (maintenance deletes), `dataset_lock_s3.rs` (bucket owner), `cli/paths.rs` (destination rules).
- Change encoding of hashes/addresses/bytes:
  - `firehose-parquet/src/encode.rs` and the mapper call sites in `blocks/src/*/mapper.rs`; per-family defaults live in `blocks/src/chain.rs`.
- Change gRPC retry/auth/stream lifecycle:
  - `firehose-parquet/src/grpc.rs` and `firehose-parquet/src/auth.rs`.
- Change built-in `--network` aliases or endpoint override behavior:
  - `firehose-parquet/src/networks.rs` (resolution and `FIREHOSE_ENDPOINT_*` overrides).
  - `scripts/generate_networks.rs` (provider policy); regenerate `firehose-parquet/src/networks_generated.rs` instead of editing it, following `docs/network-registry-integration.md`.
- Change metrics names/labels/endpoint behavior:
  - `firehose-parquet/src/metrics.rs`, plus the README metrics table.
- Change verify roots, row encoding or registry behavior:
  - `firehose-parquet/src/verify.rs`, `firehose-parquet/src/verify/row_encoding.rs`.
  - `docs/verifiability-hash-strategy.md` (spec and golden values; changing an existing encoding rule bumps `merkle_version`), `docs/verify-report-contract.md` and `docs/verifiability-artifact-runbook.md`.
- Change merge/truncate behavior:
  - `firehose-parquet/src/merge.rs`, `merge/engine.rs`, `merge_journal.rs`, `merge/read.rs`.
  - `firehose-parquet/src/truncate.rs`.
  - `firehose-parquet/src/maintenance/{compaction,discovery}.rs` for encoding, schema checks and discovery shared by several commands. Engine changes apply to local and S3 alike; storage-specific steps stay in each command's local and S3 hooks.
- Change protobuf definitions:
  - `proto/*.proto` (and `proto/core/*.proto`); bindings are rebuilt by Cargo via `firehose-protos/build.rs`.
- Cut a release:
  - Set `[workspace.package] version` in `Cargo.toml` and run `cargo update -w`.
  - Fold `docs/releases/unreleased.md` into `docs/releases/vX.Y.Z.md` and reset it.

## Build, Run, and CI Anchors

- Build workspace: `cargo build --workspace`
- Run tests: `cargo test --workspace --locked`
- Format: `cargo fmt --all`
- Real-path ingestion regressions: `blocks/tests/ingestion_transactions.rs` drives the built `fireparq` binary against a cursor-aware mock Firehose; prefer it over unit tests of helpers when a fix concerns what `build` commits
- Regenerate the schema reference: `cargo run -p blocks --example dump_schemas`
- README live-view SQL: `blocks/tests/non_final_stream.rs` runs the README "Non-final streams and reorgs" SQL in DuckDB when a CLI is found (`FIREPARQ_DUCKDB`, else `duckdb` on `PATH`); CI installs a pinned, checksum-verified CLI and sets `FIREPARQ_REQUIRE_DUCKDB`, so the check cannot be skipped there
- Engine compatibility: `blocks/tests/engine_compat.rs` builds EVM (final and non-final) and Solana output and reads it with the DuckDB CLI (as above) and with Polars through the interpreter in `FIREPARQ_POLARS_PYTHON`; CI installs Polars from the hash-pinned `blocks/tests/engines/requirements.txt` (`pip install --require-hashes`) and sets `FIREPARQ_REQUIRE_POLARS`. Locally, for example: `uv venv /tmp/polars && uv pip install --python /tmp/polars/bin/python -r blocks/tests/engines/requirements.txt`, then `FIREPARQ_POLARS_PYTHON=/tmp/polars/bin/python cargo test -p blocks --test engine_compat`
- Build release: `cargo build --release --workspace`
- Run binary from source: `cargo run --bin fireparq -- --help`
- Run ingestion (preferred form): `cargo run --bin fireparq -- build --network mainnet --start-block 100`
- Install binary locally: `cargo install --path blocks`
- Generate shell completions: `cargo run --bin fireparq -- completions zsh`
- CI entrypoint: `.github/workflows/ci.yml` (`build-and-test`, the `delta-spike` job and the `advisories` job, which calls `advisories.yml`)
- Delta Lake spike (#643): the `delta-spike` CI job runs `spikes/delta-lake/run.sh` with DuckDB 1.1.1 and 1.5.5 and the hash-pinned `spikes/delta-lake/requirements.txt`. Locally, `cd spikes/delta-lake && cargo test --locked` (local disk and in-memory store; the directory's toolchain file applies), or `run.sh` with `DELTA_SPIKE_PYTHON`, `DELTA_SPIKE_DUCKDB` and `DELTA_SPIKE_DUCKDB_SIGNED` for everything, as in `docs/design/delta-lake.md` §1.11
- Dependency advisory gate: `cargo deny --locked check advisories` in `.github/workflows/advisories.yml` (on every push and pull request through `ci.yml`, weekly on its own, and on manual dispatch), configured by `deny.toml` (RustSec advisories only; ignored advisories need a recorded reason)
- Crash-test hooks: `FIREPARQ_TEST_MERGE_CRASH_AT` and `FIREPARQ_DEBUG_FAULT` abort or fail the real binary at a named step for recovery tests, and `FIREPARQ_DEBUG_PACE_SAMPLE_MS` shortens the catch-up detection windows (`blocks/tests/adaptive_flush.rs`). Only debug builds (as built by `cargo test`) read them; release binaries ignore them (`blocks/tests/maintenance_crash_hooks.rs`).
- Docker publish workflow: `.github/workflows/docker-publish.yml` (supports a build-only manual run)
- Release assets workflow: `.github/workflows/release.yml` (supports a dry-run dispatch)
- Built-in network endpoint check (weekly, needs network access): `.github/workflows/network-endpoints.yml`, locally `scripts/check_network_endpoints.sh`

## Parquet Enum Convention

- Materialized protobuf enum-backed fields should be written to Parquet as stable protobuf label strings, not raw integer values.
- Prefer Arrow dictionary-encoded `Utf8` (`Dictionary(Int32, Utf8)`) for enum columns that are newly materialized or migrated for readability.
- Existing readable `Utf8` enum columns remain acceptable until they are migrated to the shared representation.
- Bitcoin currently has no protobuf enum-backed fields materialized into Parquet.

## Source vs Generated/Artifact Directories

- Source-of-truth directories:
  - `blocks/`, `firehose-parquet/`, `firehose-protos/`, `proto/`, `scripts/`, `.github/workflows/`
  - `spikes/delta-lake/` (design spike, not part of the workspace)
- Generated but committed (regenerate instead of editing):
  - `firehose-parquet/src/networks_generated.rs` (`generate-networks`)
  - `docs/schemas/*.md` (`cargo run -p blocks --example dump_schemas`)
- Generated build artifacts (do not edit or commit):
  - `target/` (Cargo build output)
  - protobuf Rust modules under Cargo `OUT_DIR` (created during `firehose-protos` build)
- Runtime output examples (produced data, not source):
  - `output/` (local Parquet output samples)
