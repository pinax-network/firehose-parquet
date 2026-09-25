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
  - `src/cli.rs`: shared CLI args (`CommonArgs`, `BuildArgs`), subcommands, parsing, validation, and utility routines.
  - `src/networks.rs`: built-in Firehose network alias registry and env override resolution.
  - `src/config.rs`: pipeline config model, partition key behavior, compression enum.
  - `src/auth.rs`: credential selection by resolved provider host and explicit env-var selectors.
  - `src/grpc.rs`: Firehose stream client, auth headers, reconnect/backoff/timeouts.
  - `src/grpc/{finality,finalized_range}.rs`: bounded explicit finalized-anchor proof and exact metadata traversal.
  - `src/partition_index.rs`, `src/partition_index/{builder,scan}.rs`: v2 finalized coverage, contiguous raw-time spans, ancestry/routing context and resume.
  - `src/writer.rs`: Arrow builders to Parquet file writing, flush/rollover logic.
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
  - `src/s3.rs`: object_store/S3 helpers used by writer/cursor/tools.
- `blocks/`: chain-specific mapping crate and the unified binary.
  - `src/bin/main.rs`: `fireparq` executable entrypoint.
  - `src/<chain>/{mapper,proto,schema}.rs`: per-chain decode, table schema, row mapping.
  - `src/lib.rs`: exports chain modules.
- `proto/`: source `.proto` files and Buf config.
  - Includes top-level chain proto files plus `proto/core/*` dependencies.
- `scripts/`: `generate_networks.rs` (the `generate-networks` bin that writes `firehose-parquet/src/networks_generated.rs`) and `check_network_endpoints.sh` (live check of every built-in endpoint).
- `.github/workflows/`: CI/CD entrypoints (`ci.yml`, `docker-publish.yml`, `release.yml`, `network-endpoints.yml`).

## Data-Flow Mental Model

1. CLI options/env load in `blocks/src/bin/main.rs` using shared structures from `firehose-parquet/src/cli.rs`.
   The primary ingestion path dispatches to `fireparq build` (`Commands::Build(BuildArgs)`) which calls `run_ingestion`.
2. Endpoint metadata and stream messages come from `firehose-parquet/src/grpc.rs`.
3. Selected chain mapper (`blocks/src/<chain>/mapper.rs`) decodes protobuf blocks and builds Arrow columns using schemas from `schema.rs`.
4. `firehose-parquet/src/writer/protected.rs` validates the full table inventory and publishes transaction-owned partitioned Parquet parts (local or S3).
5. `firehose-parquet/src/ingest/session.rs` journals every all-table flush, advances output authority, and reconciles the optional cursor mirror. Recovery completes before Blocks requests.
6. Optional observability is emitted by `firehose-parquet/src/metrics.rs`.

## Where To Edit For X

- Add/change CLI flag or subcommand:
  - `firehose-parquet/src/cli.rs` (shared flags/subcommands including `BuildArgs` for `fireparq build`, and argument validation)
  - `blocks/src/bin/main.rs` (binary-specific wiring: `run_ingestion` and subcommand dispatch)
- Add a new chain or adjust chain-specific table mapping:
  - `blocks/src/<chain>/proto.rs` for protobuf type aliases
  - `blocks/src/<chain>/schema.rs` for Arrow schema
  - `blocks/src/<chain>/mapper.rs` for row extraction/transform logic
  - `blocks/src/bin/main.rs` for mapper wiring and `--block-type` handling
  - `firehose-parquet/src/ingest/state.rs` (`MAPPER_EPOCH`): advance the epoch when row/routing semantics change without a schema change; protected output must not silently mix those meanings.
- Change partitioning or output file layout:
  - `firehose-parquet/src/config.rs` (`Partition::partition_key`)
  - `firehose-parquet/src/writer.rs` (directory/file naming and flush behavior)
- Change partition index building/consumption:
  - `firehose-parquet/src/partition_index{.rs,/}` (proof model, scanner and span builder)
  - `firehose-parquet/src/cli.rs` (v2 IO and strict range/inspection helpers)
  - `blocks/src/bin/main.rs` (`run_partitions_build` lifecycle and publication)
  - `docs/partitions-parquet-contract.md`, `docs/partitions-build-defaults.md` (coverage and migration contract)
- Change resume/cursor behavior:
  - `firehose-parquet/src/ingest/{state,frontier,controller,session,mirror}.rs`
  - `firehose-parquet/src/cursor.rs` (Parquet row format and legacy inspection)
  - `blocks/src/bin/main.rs` (receipt/mapping queues and request defaults)
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
