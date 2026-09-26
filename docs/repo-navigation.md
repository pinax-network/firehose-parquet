# Repository Navigation Guide

This document is a tool-agnostic map of the `firehose-parquet` workspace: where code lives, how data flows, and where to edit for common tasks.

Related design docs:

- `docs/audit/README.md`: issue-by-issue implementation process and validation records.

- `docs/verifiability-hash-strategy.md`: cross-chain verify hash defaults, normalization rules, and onboarding path.
- `docs/partition-vocabulary.md`: naming convention for partition-related CLI flags.
- `docs/partitions-build-defaults.md`: inference and bounded-range rules for `fireparq partitions build`.
- `docs/network-registry-integration.md`: how built-in `--network` aliases are generated, the provider policy, and the endpoint check.

## Workspace Layout

- `Cargo.toml` (root): Rust workspace with three members: `firehose-protos`, `firehose-parquet`, `blocks`.
- `docs/verifiability-artifact-runbook.md`: operational guidance for publishing and retaining verify artifacts (`merkle_roots.parquet`, `verify_runs/<run_id>/report.json`).
- `firehose-protos/`: protobuf compilation crate.
  - `firehose-protos/build.rs`: compiles `proto/*.proto` into Rust modules with `tonic-prost-build`.
  - `firehose-protos/src/lib.rs`: exposes compiled protobuf modules and aliases (`firehose`, `eth`, `solana`, etc.).
- `firehose-parquet/`: core library crate used by the binary.
  - `src/cli.rs`: shared Clap args (`AwsArgs`, `CommonArgs`, `BuildArgs`), subcommands and stable public re-exports.
  - `src/cli/configuration.rs`: CLI-to-config conversion, argument value parsing, logging and completions.
  - `src/cli/paths.rs`: local/S3 input and output policy, credential preflight and cursor templates.
  - `src/cli/inspect.rs`, `src/cli/validate.rs`: read-only scan/inspection and canonical block validation.
  - `src/cli/partitions/{mod,builder,io,queries}.rs`: partition models/vocabulary, incremental span construction, strict index IO and resolution/listing/sharding.
  - `src/cli/{tests,validate_tests}.rs`: existing CLI and validation regressions.
  - `src/networks.rs`: built-in Firehose network alias registry and env override resolution.
  - `src/config.rs`: pipeline config model, partition key behavior, compression enum.
  - `src/auth.rs`: credential selection by resolved provider host and explicit env-var selectors.
  - `src/grpc.rs`: Firehose stream client, auth headers, reconnect/backoff/timeouts.
  - `src/grpc/{finality,finalized_range}.rs`: bounded explicit finalized-anchor proof and exact metadata traversal.
  - `src/partition_index.rs`, `src/partition_index/{builder,scan}.rs`: v2 finalized coverage, contiguous raw-time spans, ancestry/routing context and resume.
  - `src/writer.rs`: Parquet encoding, partition routing (`ParquetTableWriter::partition_suffix`) and the unprotected single-file `OutputWriter`; protected `build` does not use `OutputWriter`.
  - `src/flush.rs`: adaptive compressed flush sizing and the summed mapper memory trigger.
  - `src/ingest/`: versioned all-table transactions, authority, accepted frontier, cursor mirrors, recovery and maintenance policy.
  - `src/cursor.rs`: compatible cursor Parquet encoding and legacy inspection.
  - `src/writer/protected.rs`: prepared complete parts and exact receipt/schema verification.
  - `src/dataset_lock/`, `src/dataset_lock_s3.rs`: common local directory and persistent S3 bucket ownership for mutating commands.
  - `src/durable_state.rs`, `src/durable_state_s3.rs`: strict versioned local/remote control records and CAS tombstones.
  - `src/recovery.rs`: read-only ownership/control summaries and explicit provider-quiescent S3 owner release.
  - `src/encode.rs`: byte encoding modes (`hex`, `base58`, `tron_base58`, etc.).
  - `src/metrics.rs`: Prometheus metrics registry and `/metrics` server helpers.
  - `src/rollup.rs`, `src/merge.rs`, `src/truncate.rs`: maintenance subcommand implementations.
  - `src/artifacts.rs`: reserved dataset artifact names (`cursor.parquet`, `partitions.parquet`, `merkle_roots.parquet`, `verify_runs/`) and `is_reserved_artifact_path`, which commands that walk a dataset tree use to skip them.
  - `src/s3.rs`: shared AWS configuration and S3 builder with explicit credential/retry policies used by writer/cursor/tools.
- `blocks/`: chain-specific mapping crate and the unified binary.
  - `src/bin/main.rs`: `fireparq` executable entrypoint and shared command helpers.
  - `src/bin/ingestion/{mod,setup,runtime}.rs`: `fireparq build` orchestration, endpoint/resume setup and ordered runtime.
  - `src/chain.rs`: `ChainKind`/`ChainProfile`, the per-family facts (label, `type_url` marker, protected family, encoding contract, nullable timestamps, block gaps, extended/votes handling, failed-transaction default, chain-name rules) and mapper construction.
  - `src/<chain>/{mapper,proto,schema}.rs`: per-chain decode, table schema, row mapping.
  - `src/lib.rs`: exports chain modules.
- `proto/`: source `.proto` files and Buf config.
  - Includes top-level chain proto files plus `proto/core/*` dependencies.
- `scripts/`: `generate_networks.rs` (the `generate-networks` bin that writes `firehose-parquet/src/networks_generated.rs`) and `check_network_endpoints.sh` (live check of every built-in endpoint).
- `.github/workflows/`: CI/CD entrypoints (`ci.yml`, `docker-publish.yml`, `release.yml`, `network-endpoints.yml`).

## Data-Flow Mental Model

1. CLI options/env load in `blocks/src/bin/main.rs` using shared structures from `firehose-parquet/src/cli.rs`.
   The primary ingestion path dispatches to `fireparq build` (`Commands::Build(BuildArgs)`) which calls `ingestion::run_ingestion` in `blocks/src/bin/ingestion/mod.rs`.
   Its `setup.rs` resolves the endpoint and owned resume configuration; `runtime.rs` owns receive/filter/routing state and flush windows while borrowing the durable session.
2. Endpoint metadata and stream messages come from `firehose-parquet/src/grpc.rs`.
3. The chain mapper selected by `ChainKind` (`blocks/src/chain.rs`: from `--block-type`, the endpoint chain names, or the first payload's `type_url` in a dry run) decodes protobuf blocks in `blocks/src/<chain>/mapper.rs` and builds Arrow columns using schemas from `schema.rs`.
4. `firehose-parquet/src/writer/protected.rs` validates the full table inventory and publishes transaction-owned partitioned Parquet parts (local or S3).
5. `firehose-parquet/src/ingest/session.rs` journals every all-table flush, advances output authority, and reconciles the optional cursor mirror. Recovery completes before Blocks requests.
6. Optional observability is emitted by `firehose-parquet/src/metrics.rs`.

## Where To Edit For X

- Add/change CLI flag or subcommand:
  - `firehose-parquet/src/cli.rs` (shared flags/subcommands including `BuildArgs` for `fireparq build`); `firehose-parquet/src/cli/configuration.rs` for config conversion and value validation
  - `blocks/src/bin/main.rs` (binary command dispatch) and `blocks/src/bin/ingestion/setup.rs` (ingestion configuration)
- Add a new chain family:
  - `blocks/src/<chain>/{mod,proto,schema,mapper}.rs` (exported from `blocks/src/lib.rs`) for protobuf aliases, Arrow schemas and row mapping; use the shared `firehose_parquet::traits` helpers (`push_fork_step_field`, `fork_step_builder`, `append_fork_step`, `finish_fork_step`, `enum_data_type`, `estimated_dictionary_index_bytes`, `strip_enum_prefix`) instead of per-chain copies.
  - `blocks/src/chain.rs`: add a `ChainKind` variant (append it to `ChainKind::ALL`, which is also the `type_url` detection order), its `ChainProfile` in `ChainKind::profile` and its constructor in `ChainKind::create_mapper`. Add ordered `CHAIN_NAME_RULES` entries for endpoint-name inference. Fill `strict_chain_names` when the family has votes, nullable timestamps or unsupported extended output, because those flags are resolved before the first block.
  - `firehose-parquet/src/ingest/state.rs` (`BlockFamily`) and `ingest/mirror.rs`: add the protected family; its serde name must equal the profile label.
  - `blocks/src/bin/main.rs` (`BLOCK_TYPES`, the unsupported-type error list) and the `--block-type` help in `firehose-parquet/src/cli.rs` (`BuildArgs`): add the label. A test checks that `BLOCK_TYPES` matches `ChainKind::ALL`.
  - The #526 oracle tests (`blocks/src/chain/tests.rs`, `blocks/src/bin/chain_profile_tests/`) cover the eight pre-#526 families. Extend their expected lists or retire them, and re-pin the schema digest.
- Adjust chain-specific table mapping or per-family behavior:
  - `blocks/src/<chain>/schema.rs` and `mapper.rs` for tables and rows.
  - `blocks/src/chain.rs` (`ChainProfile`) for encodings, timestamps, gaps, extended/votes and failed-transaction defaults; the ingestion code reads these properties instead of comparing `block_type` strings.
  - `firehose-parquet/src/ingest/state.rs` (`MAPPER_EPOCH`): advance the epoch when row/routing semantics change without a schema change; protected output must not silently mix those meanings.
- Change partitioning or output file layout:
  - `firehose-parquet/src/config.rs` (`Partition::partition_key`)
  - `firehose-parquet/src/writer.rs` (`ParquetTableWriter::partition_suffix` partition directories, shared by protected commits)
  - `firehose-parquet/src/ingest/state.rs` (`PendingTransaction` deterministic `part-v1-*` and `.fireparq-txn-*.tmp` names) and `firehose-parquet/src/writer/protected.rs` (staging, publication and receipt verification)
  - `blocks/src/bin/ingestion/runtime.rs` (flush windows and partition-boundary flushes) and `firehose-parquet/src/flush.rs` (size/memory triggers)
- Change partition index building/consumption:
  - `firehose-parquet/src/partition_index{.rs,/}` (proof model, scanner and span builder)
  - `firehose-parquet/src/cli/partitions/{io,queries,builder}.rs` (v2 IO, strict range queries and incremental construction)
  - `blocks/src/bin/main.rs` (`run_partitions_build` lifecycle and publication)
  - `docs/partitions-parquet-contract.md`, `docs/partitions-build-defaults.md` (coverage and migration contract)
- Change resume/cursor behavior:
  - `firehose-parquet/src/ingest/{state,frontier,controller,session,mirror,binding}.rs` (`binding.rs` resolves the output and mirror identities; `session::ingestion_mutation_scopes` derives build ownership from that binding; `--cursor none` binds no mirror)
  - `firehose-parquet/src/cursor.rs` (Parquet row format and legacy inspection)
  - `blocks/src/bin/ingestion/{setup,runtime}.rs` (request defaults and ordered receipt/mapping queues)
- Change encoding of hashes/addresses/bytes:
  - `firehose-parquet/src/encode.rs`
  - chain mapper usage in `blocks/src/*/mapper.rs`
- Change gRPC retry/auth/stream lifecycle:
  - `firehose-parquet/src/grpc.rs`
- Change built-in `--network` aliases or endpoint override behavior:
  - `firehose-parquet/src/networks.rs` (resolution and `FIREHOSE_ENDPOINT_*` overrides)
  - `scripts/generate_networks.rs` (provider policy); regenerate `firehose-parquet/src/networks_generated.rs` instead of editing it, following `docs/network-registry-integration.md`
- Change metrics names/labels/endpoint behavior:
  - `firehose-parquet/src/metrics.rs`
- Change rollup/merge/truncate behavior:
  - `firehose-parquet/src/rollup.rs`
  - `firehose-parquet/src/merge.rs`
  - `firehose-parquet/src/truncate.rs`
- Change protobuf definitions:
  - `proto/*.proto` (and `proto/core/*.proto` dependencies)
  - generated bindings are rebuilt by Cargo via `firehose-protos/build.rs`

## Build, Run, and CI Anchors

- Build workspace: `cargo build --workspace`
- Run tests: `cargo test --workspace`
- Real-path ingestion regressions: `blocks/tests/ingestion_transactions.rs` drives the built `fireparq` binary against a cursor-aware mock Firehose; prefer it over unit tests of helpers when a fix concerns what `build` commits
- Build release: `cargo build --release --workspace`
- Run binary from source: `cargo run --bin fireparq -- --help`
- Run ingestion (preferred form): `cargo run --bin fireparq -- build --network mainnet --start-block 100`
- Install binary locally: `cargo install --path blocks`
- Generate shell completions: `cargo run --bin fireparq -- completions zsh`
- CI entrypoint: `.github/workflows/ci.yml`
- Docker publish workflow: `.github/workflows/docker-publish.yml`
- Release assets workflow: `.github/workflows/release.yml`
- Built-in network endpoint check (weekly, needs network access): `.github/workflows/network-endpoints.yml`, locally `scripts/check_network_endpoints.sh`

## Parquet Enum Convention

- Materialized protobuf enum-backed fields should be written to Parquet as stable protobuf label strings, not raw integer values.
- Prefer Arrow dictionary-encoded `Utf8` (`Dictionary(Int32, Utf8)`) for enum columns that are newly materialized or migrated for readability.
- Existing readable `Utf8` enum columns remain acceptable until they are migrated to the shared representation.
- Bitcoin currently has no protobuf enum-backed fields materialized into Parquet.

## Source vs Generated/Artifact Directories

- Source-of-truth directories:
  - `blocks/`, `firehose-parquet/`, `firehose-protos/`, `proto/`, `.github/workflows/`
- Generated/build artifacts (do not edit manually):
  - `target/` (Cargo build output)
  - protobuf Rust modules under Cargo `OUT_DIR` (created during `firehose-protos` build)
- Runtime output examples (produced data, not source):
  - `output/` (local Parquet output samples)
