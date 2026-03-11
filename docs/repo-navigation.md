# Repository Navigation Guide

This document is a tool-agnostic map of the `firehose-parquet` workspace: where code lives, how data flows, and where to edit for common tasks.

Related design docs:

- `docs/verifiability-hash-strategy.md`: cross-chain verify hash defaults, normalization rules, and onboarding path.
- `docs/partition-vocabulary.md`: naming convention for partition-related CLI flags.
- `docs/partitions-build-defaults.md`: inference and bounded-range rules for `fireparq partitions build`.

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
  - `src/grpc.rs`: Firehose stream client, auth headers, reconnect/backoff/timeouts.
  - `src/writer.rs`: Arrow builders to Parquet file writing, flush/rollover logic.
  - `src/cursor.rs`: resume state persistence (`cursor.parquet`) and parameter checks.
  - `src/encode.rs`: byte encoding modes (`hex`, `base58`, `tron_base58`, etc.).
  - `src/metrics.rs`: Prometheus metrics registry and `/metrics` server helpers.
  - `src/rollup.rs`, `src/merge.rs`, `src/truncate.rs`: maintenance subcommand implementations.
  - `src/s3.rs`: object_store/S3 helpers used by writer/cursor/tools.
- `blocks/`: chain-specific mapping crate and the unified binary.
  - `src/bin/main.rs`: `fireparq` executable entrypoint.
  - `src/<chain>/{mapper,proto,schema}.rs`: per-chain decode, table schema, row mapping.
  - `src/lib.rs`: exports chain modules.
- `proto/`: source `.proto` files and Buf config.
  - Includes top-level chain proto files plus `proto/core/*` dependencies.
- `.github/workflows/`: CI/CD entrypoints (`ci.yml`, `docker-publish.yml`, `release.yml`).

## Data-Flow Mental Model

1. CLI options/env load in `blocks/src/bin/main.rs` using shared structures from `firehose-parquet/src/cli.rs`.
   The primary ingestion path dispatches to `fireparq build` (`Commands::Build(BuildArgs)`) which calls `run_ingestion`.
   The legacy root-level invocation still works but emits a deprecation warning.
2. Endpoint metadata and stream messages come from `firehose-parquet/src/grpc.rs`.
3. Selected chain mapper (`blocks/src/<chain>/mapper.rs`) decodes protobuf blocks and builds Arrow columns using schemas from `schema.rs`.
4. `firehose-parquet/src/writer.rs` flushes `RecordBatch`es to partitioned Parquet files (local or S3).
5. `firehose-parquet/src/cursor.rs` snapshots resume state to `cursor.parquet` at synchronized flush boundaries.
6. Optional observability is emitted by `firehose-parquet/src/metrics.rs`.

## Where To Edit For X

- Add/change CLI flag or subcommand:
  - `firehose-parquet/src/cli.rs` (shared flags/subcommands including `BuildArgs` for `fireparq build`, and argument validation)
  - `blocks/src/bin/main.rs` (binary-specific wiring: `run_ingestion`, backward-compat deprecated root path)
- Add a new chain or adjust chain-specific table mapping:
  - `blocks/src/<chain>/proto.rs` for protobuf type aliases
  - `blocks/src/<chain>/schema.rs` for Arrow schema
  - `blocks/src/<chain>/mapper.rs` for row extraction/transform logic
  - `blocks/src/bin/main.rs` for mapper wiring and `--block-type` handling
- Change partitioning or output file layout:
  - `firehose-parquet/src/config.rs` (`Partition::partition_key`)
  - `firehose-parquet/src/writer.rs` (directory/file naming and flush behavior)
- Change resume/cursor behavior:
  - `firehose-parquet/src/cursor.rs`
  - `blocks/src/bin/main.rs` (resume flow and validation overrides)
- Change encoding of hashes/addresses/bytes:
  - `firehose-parquet/src/encode.rs`
  - chain mapper usage in `blocks/src/*/mapper.rs`
- Change gRPC retry/auth/stream lifecycle:
  - `firehose-parquet/src/grpc.rs`
- Change built-in `--network` aliases or endpoint override behavior:
  - `firehose-parquet/src/networks.rs`
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
- Run ingestion (preferred form): `cargo run --bin fireparq -- build --network mainnet --start-block 100 --live`
- Install binary locally: `cargo install --path blocks`
- Generate shell completions: `cargo run --bin fireparq -- completions zsh`
- CI entrypoint: `.github/workflows/ci.yml`
- Docker publish workflow: `.github/workflows/docker-publish.yml`
- Release assets workflow: `.github/workflows/release.yml`

## Source vs Generated/Artifact Directories

- Source-of-truth directories:
  - `blocks/`, `firehose-parquet/`, `firehose-protos/`, `proto/`, `.github/workflows/`
- Generated/build artifacts (do not edit manually):
  - `target/` (Cargo build output)
  - protobuf Rust modules under Cargo `OUT_DIR` (created during `firehose-protos` build)
- Runtime output examples (produced data, not source):
  - `output/` (local Parquet output samples)
