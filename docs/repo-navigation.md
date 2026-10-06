# Repository Navigation Guide

This document is a tool-agnostic map of the `firehose-parquet` workspace: where code lives, how data flows, and where to edit for common tasks.

Related docs:

- `README.md`: the front page (what fireparq does, supported chains, install, quick start, the recommended deployment in brief) and a table of every page below.
- `docs/getting-started.md`: install (release archives, Docker image, from source), every `build` example of the quick start, and reading a first run with DuckDB.
- `docs/authentication.md`: provider-scoped Firehose credentials and explicit credential selectors.
- `docs/cli.md`: the CLI reference: common ingestion flags, network aliases, environment variables and `.env` loading, the advanced recovery, transport and S3 knobs, flush sizing and the catch-up rule, and the `inspect`, `validate`, `recovery` and `completions` subcommands.
- `docs/features.md`: the full feature list, each linked to the page that documents it.
- `docs/non-final-streams.md`: `--final-blocks-only=false`, the append-only `fork_step` / `stream_ordinal` history and the canonical live view (its SQL is run by `blocks/tests/non_final_stream.rs`).
- `docs/output-layout.md`: the dataset root and `{chain}`, the directory tree, single-network buckets, canonical identity columns, identifier encoding by block type, and Parquet file and lookup metadata.
- `docs/reading-tables.md`: reading the Delta tables with DuckDB and Polars, the frontier rule for consistent reads, a day's block range, and engine compatibility.
- `docs/chains/`: per-chain notes (`README.md` indexes them): failed transaction filtering and the EVM, Solana, Bitcoin, Beacon, Tron, Cosmos, Antelope, NEAR and HyperCore semantics (joins, ordering, example queries; HyperCore's views, monitors and cookbook, which `blocks/tests/engine_compat.rs` runs).
- `docs/deployment.md`: the recommended final-only deployment, one writer per network.
- `docs/cursor-and-resume.md`: output authority and the cursor mirror, startup cost, crash recovery, local part publication, S3 cursors, ownership, graceful shutdown, and start and stop blocks.
- `docs/delta-maintenance.md`: the `fireparq maintenance` job, its settings, row order and CronJobs.
- `docs/metrics.md`: the Prometheus metrics and the `/health` and `/ready` endpoints.
- `docs/development.md`: build and test commands, the repository structure and the CLI architecture.
- `docs/releases/`: release notes; `docs/releases/unreleased.md` collects changes merged since the last release.
- `docs/schemas/`: per-chain table and column reference, generated from the code (see "Change a table schema" below).
- `docs/audit/README.md`: issue-by-issue implementation and validation records of the September 2026 audit (#463).
- `docs/design/delta-lake.md`: the Delta Lake output design for the v1.0.0 launch (#643), implemented in lanes L1–L5 and L7–L10 (L6, `verify`, is #666): dependency spike (removed in L8/L9; its checks and what now covers them are in §1.1–1.11) and pinned versions, protocol and table properties, the commit mapping onto the #468 transaction, crash matrix and VACUUM rule, ownership (#636), type mapping, removals, resume cost (#655), the maintenance CronJob, reader examples and the PR-sized implementation lanes.
- `docs/partition-vocabulary.md`: the single `date=YYYY-MM-DD` output key and the CLI terms that refer to it.
- `docs/network-registry-integration.md`: how built-in `--network` aliases are generated, the provider policy, and the endpoint check.

## Workspace Layout

- `Cargo.toml` (root): Rust workspace with four members, `firehose-protos`, `firehose-parquet`, `blocks` and `maintenance`, sharing one `[workspace.package]` version. All code is Rust; nothing in the repository needs Python.
- `firehose-protos/`: protobuf compilation crate.
  - `build.rs`: compiles `proto/*.proto` into Rust modules with `tonic-prost-build`; byte fields are generated as `Bytes`.
  - `src/lib.rs`: exposes the compiled modules and aliases (`firehose`, `eth`, `solana`, ...).
- `firehose-parquet/`: core library crate used by the binary.
  - CLI (`src/cli.rs` and `src/cli/`):
    - `cli.rs`: Clap definitions (`Cli`, `Commands`, `GlobalArgs`, `AwsArgs`, `CommonArgs`, `BuildArgs`) and the stable public re-exports.
    - `cli/configuration.rs`: CLI-to-`Config` conversion, value parsers, working-directory `.env` / `--env-file` loading, logging and completions.
    - `cli/paths.rs`: local/S3 input and output policy (explicit `s3://` for writes, `S3_BUCKET` shorthand for reads), credential preflight, and the `--output` dataset-root resolver (`resolve_output_root`, `{chain}` expansion, `{{`/`}}` escapes).
    - `cli/inspect.rs`, `cli/validate.rs`: read-only single-file `inspect`, and `validate`, which checks block continuity over the active files of a pinned Delta snapshot of a `blocks` table (never a directory walk, #643). `scan` is removed: engines read the tables through their logs.
    - `cli/{tests,validate_tests}.rs`: CLI and validation regressions.
  - Configuration and endpoints:
    - `src/config.rs`: pipeline `Config`, `Compression` (including `zstd:<level>`) and receive-transport defaults.
    - `src/date_partition.rs`: the `date=YYYY-MM-DD` partition key, the only output layout (#652): `DatePartition` formats it from a block time, parses it strictly, and gives its `Date32` value.
    - `src/networks.rs`, `src/networks_generated.rs`: built-in `--network` aliases (generated; do not edit) and `FIREHOSE_ENDPOINT_*` overrides.
    - `src/auth.rs`: Firehose credential selection by resolved provider host, and explicit env-var selectors.
    - `src/grpc.rs`: the Firehose stream client (EndpointInfo, healthcheck and the Stream RPC), shared authenticated transport, reconnect/back-off/timeouts and fatal-status classification.
  - Protected ingestion (`src/ingest/`), used by every non-dry-run `build`:
    - `session.rs`: `IngestionSession`, assembling storage identity, eligibility, recovery and the accepted frontier; `ingestion_mutation_scopes` derives build ownership.
    - `controller.rs`: all-table publication and restart decisions: journal each flush, publish parts, commit them to the Delta logs, advance authority, reconcile the mirror; at open, check every table's `txn` and roll a Committed journal forward into the tables whose logs lack it (#643 L4).
    - `controller/pipeline.rs`, `controller/lane.rs`: bounded concurrent encode/stage/receipt/publish of one transaction's parts (#516 stage A) and the scoped blocking lane for local part I/O.
    - `state.rs`: strict transaction identity and durable records (`PendingTransaction`, deterministic `part-v1-*` / `.fireparq-txn-*.tmp` names, `BlockFamily`, `MAPPER_EPOCH`).
    - `store.rs`, `parts.rs`: typed record transitions, and physical part ownership checks and cleanup.
    - `frontier.rs`: the accepted-event prefix; `binding.rs`: output and mirror identities from the actual storage configuration (`--cursor none` binds no mirror).
    - `eligibility.rs`: new streams may only initialize empty destinations; legacy data, cursors and any other file are refused.
    - `mirror.rs`: the non-authoritative cursor mirror (default `_fireparq/cursor.parquet`), reconciled from authority.
    - `maintenance.rs`: protected-root discovery and recovery for `recovery recover` (with each root's existing Delta tables when its journal needs them), and `build`'s startup check for overlapping roots (the whole tree when a dataset is created, only ancestors on resume, #655).
  - Writing data files (the Parquet parts):
    - `src/delta/types.rs`: the checked flush-boundary mapping onto Delta data file types (#643): `DeltaTypes` (a chain's `decimal(20,0)` columns), `data_schema` (the declared table digests), `data_batches` (every flush, in `IngestionSession::flush` before the transaction journals anything: checked `long`, `decimal(20,0)`, `short`, `string` enums, microsecond timestamps, `date` left out after checking it against the partition), `Conversion` (the rules `docs/schemas/` documents) and `is_delta_type`.
    - `src/writer.rs`: part encoding (`encode_parquet`, which tests and offline tools use too, and the crate-internal `encode_into` / `writer_properties`), the `<table>/date=YYYY-MM-DD` directory of a flush (`partition_suffix`) and the partition contract (`validate_partition`: every row's time must fall in that partition, and a mapper `date` column must equal it). It publishes nothing: every table file is a part of the protected transaction.
    - `src/writer/protected.rs`, `src/writer/protected/verification.rs`: prepared complete parts, publication and exact receipt/schema verification; `src/writer/protected/budget.rs`: the `--flush-inflight-bytes` encoded-byte budget.
    - `src/writer/local.rs`: atomic, synced publication of one local part; `src/writer/properties.rs`: bounded Bloom filters, row-group limits and sort metadata of ingestion parts.
    - `src/flush.rs`: adaptive compressed `--flush-bytes` targets and the summed `--flush-memory-bytes` trigger.
    - `src/flush/pace.rs`: catch-up detection (#659): a pure `PaceDetector` that compares block time with the wall clock, so `--flush-interval-secs` applies only at the chain head.
    - `src/cursor.rs`: cursor Parquet row format and legacy inspection.
    - `src/encode.rs`, `src/encode/fixed_base58.rs`: byte encodings (`hex`, `hex_no_prefix`, `base58`, `tron_base58`, `binary`) and the fixed-width Base58 fast path.
    - `src/traits.rs`: `BlockMapper`, canonical identity columns, timestamp helpers, and the shared enum helpers and non-final event helpers (`StreamEvent`: the `fork_step` and `stream_ordinal` columns).
  - Delta Lake (#643, `docs/design/delta-lake.md`):
    - `src/delta/mod.rs`: `deltalake-core` 1.0.0 (its own Arrow/Parquet 59 and object_store 0.13, beside the workspace's 60 and 0.12), the table protocol and properties (design §2), `DeltaIdentity` (the `fireparq.descriptor`, `fireparq.chain` and `fireparq.blockType` properties and the `txn` application id `fireparq:<descriptor>`), `delta_columns` (a data file schema's Delta columns plus `date`) and table creation and opening.
    - `src/delta/store.rs`: where the tables live (`DeltaStore`: a local root or an S3 bucket and prefix), each table's conditional-put log store, the `s3://` log store factory, and `s3_log_client`, the object_store 0.13 client built from `AwsConfig` (attached to the output bucket's owner, `S3Ownership::delta_log`) that sends every write once; local commits are synced before they count. `store/read_retry.rs` is its HTTP connector (#680): idempotent reads (`GET`, `HEAD`) are sent up to 3 times on a transport error, 408, 429 or 5xx, with jittered backoff and a warning per retry, and every other method once.
    - `src/delta/stats.rs`: each part's `add.stats` (`numRecords`, bounds and null counts of `block_num` and `timestamp`), journaled with its receipt.
    - `src/delta/commit.rs`: `DeltaTables`, opened by `IngestionSession::open` before recovery (created only while authority is at ordinal 0, then validated: properties, identity, protocol, partition and schema; `open_existing` for `recovery recover`, without the schema), `commit`, one Delta commit per table with a part of a Committed transaction (`add` + `txn`, `blocks` last), called by the controller before authority advances (#643 L3), and recovery's `check_progress` (every table's `txn` against authority: the log-ahead refusal and the tables a Committed journal is missing from) and `roll_forward` (those commits, a same-`appId` conflict resolved from the log) (#643 L4). `commit/tests/loopback_s3.rs` is the loopback S3 endpoint of its tests.
  - Ownership, durable state and S3:
    - `src/dataset_lock/{mod,local,operation,session}.rs`: local directory-inode ownership shared by `build` and `recovery`; `DatasetOwnership::finish` ends `build` ownership (release after success or after a failure whose requests all had a definite outcome). `src/dataset_lock_s3.rs`: the persistent bucket-wide S3 owner (`.fireparq-owner-v1.json`) and its uncertainty latch.
    - `src/durable_state.rs`, `src/durable_state_s3.rs`: strict versioned local/remote control records and CAS tombstones.
    - `src/recovery.rs`: `fireparq recovery` (`status`, `recover`, `release`): ownership inspection, guarded transaction recovery and provider-quiescent S3 owner release.
    - `src/s3.rs`: shared AWS configuration and S3 client builders with explicit credential/retry policies (`build_ingestion_mutation_client`, `AwsConfig::build_read_client`).
    - `src/s3/upload.rs`: native ingestion uploads (disk spool, one conditional PUT, spooled readback verification, explicit timeouts).
  - Discovery, artifacts and metrics:
    - `src/maintenance/discovery.rs`: `visit_objects`, the paged listing with a timeout per request and no total deadline, and `ListingStats` (#655), which the startup checks of `build` and `recovery` use, and `inspect`'s whole-object read. No table is read through a listing. `discovery/paged_bucket.rs` is a test bucket of a million generated keys that counts LIST pages.
    - `src/artifacts.rs`: dataset artifact names: `ARTIFACTS_DIR` (`_fireparq/`), `DEFAULT_CURSOR_MIRROR` (`_fireparq/cursor.parquet`, the clap default), the owner record names, and `DELTA_LOG_DIR` / `is_in_delta_log`, which the protected-root walker of `recovery` and `build` (`ingest/maintenance.rs`) skips.
    - `src/metrics.rs`: Prometheus registry, `/metrics`, `/health` and `/ready`.
  - `tests/`: Parquet compatibility integration tests.
- `blocks/`: chain-specific mapping crate and the unified `fireparq` binary.
  - `src/bin/main.rs`: `fireparq` entrypoint, command dispatch (`Commands::*`), shared ingestion helpers, and the `.env.example` drift test.
  - `src/bin/ingestion/mod.rs`: `run_ingestion`, the `fireparq build` orchestration; dataset ownership outlives the session and runtime.
  - `src/bin/ingestion/setup.rs`: endpoint preflight (EndpointInfo, block type, encodings), resumed configuration and metrics labels.
  - `src/bin/ingestion/runtime.rs`: ordered receive/filter/routing state and flush windows; only the borrowed session commits authority and mirrors.
  - `src/bin/chain_profile_tests/`: #526 oracle tests for chain detection and profiles.
  - `src/chain.rs`: `ChainKind` / `ChainProfile`, the per-family facts (label, `type_url` marker, protected family, encoding contract, nullable timestamps, block gaps, extended/votes handling, failed-transaction default, chain-name rules, the `decimal_columns` its Delta tables store as `decimal(20,0)`) and mapper construction.
  - `src/<chain>/{mod,proto,schema,mapper}.rs` for `evm`, `solana`, `beacon`, `near`, `antelope`, `tron`, `cosmos`, `bitcoin`, `hypercore`: protobuf aliases, Arrow table schemas and row mapping, plus chain-specific helpers (`evm/decimal.rs`, `solana/vote.rs`, `tron/contracts.rs`, `bitcoin/amounts.rs`, `cosmos/tx_metadata.rs`, `antelope/text.rs`, `hypercore/decimal.rs`, the strict `decimal(38,10)` parser) and value/outcome tests.
  - `src/hypercore/`: the HyperCore (`pinax.hypercore.v1`) mapper stages and validates a whole block before appending (refusal rules R1–R11 in `docs/chains/hypercore.md`), destructures every proto message exhaustively, and writes its decimal block ids with `CanonicalBuilder::prepare_with_text_ids` (`ChainProfile::block_id_text`). `value_tests.rs` maps the real fixtures of `tests/fixtures/hypercore/` (golden counts and values, a byte-exact rebuild of every payload, the events column matrix, a pinned output hash) and checks every refusal; `fixtures.rs` also derives the contract-test blocks. Its schemas are pinned by `HYPERCORE_SCHEMA_DIGEST` (`src/chain/tests.rs`): a change needs a new output root (a schema epoch).
  - `src/schema_docs.rs`: renders `docs/schemas/<chain>.md` from the schema constructors; `examples/dump_schemas.rs` writes the files and a test fails when they drift.
  - `src/schema_contract_tests.rs`: every table of every chain, under every encoding and both `fork_step` settings, has unique names and round-trips through Parquet, and its Delta data file mapping holds only Delta types, exactly the profile's `decimal(20,0)` columns and every value; a value above `i64::MAX` in a `long` column is refused.
  - `src/mapping_bench.rs`: ignored whole-block mapping benchmarks.
  - `examples/`: replay, benchmark and golden-refresh tools (`replay_*`, `bench_*`, `measure_flush_sizing`, `refresh_evm_golden`, `dump_schemas`). `bench_ingestion_concurrency` is the #516 flush-concurrency benchmark; `bench_live_flush` runs the real binary against a looping mock Firehose and a loopback HTTPS S3 with injected latency to measure #658 catch-up throughput and commit phases; its S3 endpoint (`bench_live_flush/s3.rs`) also injects per-request faults (a status, a dropped connection, or a write applied whose answer is lost) and records each request's `If-Match` and client for the tests.
  - `tests/`: real-binary integration tests against a mock Firehose (`ingestion_transactions.rs`, `dataset_ownership.rs`, `endpoint_info_startup.rs`, `metrics_readiness.rs`, `shutdown_signals.rs`, and `adaptive_flush.rs`, which paces its mock faster than or at real time), the engine test (`engine_compat.rs`: every Delta table of EVM final and non-final, Solana and HyperCore output through DuckDB `delta_scan` and delta-rs, the HyperCore view pack, monitors and cookbook of `docs/chains/hypercore.md` over a build of its 36 real fixture blocks, plus an opt-in anonymous read of a deployment bucket), the Delta table test (`delta_tables.rs`: the logs of a local and a loopback-S3 `build`, a clean restart, a crash after every Delta commit, a first start without an ERROR line, and, through the loopback server's injected faults, retried log reads and log commits sent once, #680), the non-final stream test (`non_final_stream.rs`: `stream_ordinal` and the live view of `docs/non-final-streams.md` through `delta_scan`), the maintenance test (`delta_maintenance.rs`: `fireparq maintenance` run over and over beside a real `build`, local and loopback S3; the VACUUM rules, skipped missing tables, idempotent reruns and no `If-Match` from delta-rs), the read-only commands over Delta tables (`delta_readers.rs`: `validate` over a pinned snapshot, locally and on loopback S3, after a simulated OPTIMIZE and a real one (the maintenance job) with a checkpoint; `inspect`; the table summary from the log of `docs/reading-tables.md`), the Delta recovery test (`delta_recovery.rs`: a crash test for every row of the design's crash matrix and `recovery recover`, local and loopback S3, with the maintenance job (OPTIMIZE, VACUUM) or a delta-rs full VACUUM between the crash and the restart, and the job beside a running `build` for #636), their shared helpers (`tests/common/mod.rs`: the DuckDB CLI, Delta reads through `deltalake-core` snapshots and the active files' Parquet, delta-rs VACUUM and checkpoints, and `fireparq maintenance`), and the offline EVM golden regression (`evm_golden.rs`, `tests/fixtures/`; the HyperCore fixtures in `tests/fixtures/hypercore/` are mapped by `src/hypercore/value_tests.rs`).
- `maintenance/`: the Delta maintenance job (#643, design §9; `docs/delta-maintenance.md`), a library crate run as `fireparq maintenance` (`blocks/src/bin/main.rs`); it links no DataFusion.
  - `src/lib.rs` (`fireparq_maintenance`): settings from the environment (`Settings::from_env`), the lake's object store and delta-rs log store (`Lake`: local or S3 with conditional-create commits), the per-table order (OPTIMIZE of closed dates, VACUUM, checkpoint, log cleanup) with delta-rs's own operations, a full VACUUM that keeps each table's `metadata/` (`ICEBERG_METADATA_DIR`, Iceberg metadata of an XTable sync), and the JSON-line output (`run`).
  - `tests/compaction.rs`: the compaction's row order on local tables: a repair across several windows, a tie left as it is, repairs spread over runs, a table with no known order. `blocks/tests/maintenance_cli.rs` runs `fireparq maintenance` itself: configuration errors, store errors with credentials redacted, missing tables skipped, and a full VACUUM that keeps `metadata/`.
- `proto/`: source `.proto` files and Buf config, including `proto/core/*` dependencies and HyperCore's `proto/pinax/hypercore/v1/*.proto` (vendored with their package path and imports).
- `scripts/`: `generate_networks.rs` (the `generate-networks` bin that writes `firehose-parquet/src/networks_generated.rs`) and `check_network_endpoints.sh` (live check of every built-in endpoint).
- `deploy/examples/`: example Kubernetes manifests for adoption in a deployment repository (`delta-maintenance-cronjob.yaml`: the hourly maintenance and weekly full-VACUUM CronJobs, the writer's image with `args: ["maintenance"]`).
- `.env.example`: every environment variable the CLI reads, kept in sync by a test.
- `deny.toml`: cargo-deny advisory policy.
- `.github/workflows/`: `ci.yml`, `advisories.yml`, `docker-publish.yml`, `release.yml`, `network-endpoints.yml`.

## Data-Flow Mental Model

1. `blocks/src/bin/main.rs` parses the CLI (`firehose-parquet/src/cli.rs`), loads `.env` from the working directory or `--env-file`, and dispatches. `fireparq build` (`Commands::Build(BuildArgs)`) calls `ingestion::run_ingestion` in `blocks/src/bin/ingestion/mod.rs`.
2. `ingestion/setup.rs` resolves the endpoint and provider-scoped credentials (`auth.rs`), requires EndpointInfo (`grpc.rs`), selects the `ChainKind` (`blocks/src/chain.rs`: from `--block-type`, the endpoint chain names, or the first payload's `type_url` in a dry run) and resolves the dataset root (`resolve_output`, which calls `firehose_parquet::cli::resolve_output_root`: `--output` as given, with an opt-in `{chain}` placeholder expanded to the EndpointInfo chain name) and cursor mirror.
3. `build` and `recovery recover` acquire dataset ownership first (`dataset_lock/`, `dataset_lock_s3.rs`). `ingest/session.rs` then recovers any pending transaction and opens the accepted frontier from the authority under `<dataset root>/.fireparq-ingest/` before any Blocks request.
4. Stream messages come from `grpc.rs`. `ingestion/runtime.rs` filters, orders and routes them; the chain mapper (`blocks/src/<chain>/mapper.rs`) decodes protobuf blocks and appends Arrow columns using `schema.rs`.
5. When a flush trigger fires (`flush.rs`, partition boundaries, completion), `ingest/controller.rs` journals the all-table transaction, `writer/protected.rs` publishes the deterministic parts (local via `writer/local.rs`, S3 via `s3/upload.rs`) and verifies them, the controller commits each table's part to its Delta log (`delta/commit.rs`, `blocks` last), then advances authority, then the optional `_fireparq/cursor.parquet` mirror.
6. `metrics.rs` exposes counters, readiness and health.
7. `recovery recover` takes the same ownership and runs the same transaction recovery offline (`ingest/maintenance.rs`). Compaction and cleanup of the Delta tables are a separate CronJob, `fireparq maintenance` (`maintenance/`), not writer code (#643, `docs/design/delta-lake.md` §9).

## Where To Edit For X

- Document a feature or behavior:
  - The `docs/` page that covers it (listed above and in the README's Documentation table). Keep `README.md` a short front page; add a new page to that table and to the list above.
- Add/change a CLI flag or subcommand:
  - `firehose-parquet/src/cli.rs` (flags and subcommands); `cli/configuration.rs` (config conversion and value validation); `cli/paths.rs` (input/output policy).
  - `blocks/src/bin/main.rs` (dispatch) and `blocks/src/bin/ingestion/setup.rs` (ingestion configuration).
  - Update `.env.example` when a flag reads an environment variable (a test in `blocks/src/bin/main.rs` fails otherwise), the flag tables in `docs/cli.md`, and `docs/releases/unreleased.md`.
- Change a table schema (add, rename or retype a column, add a table):
  - `blocks/src/<chain>/schema.rs` and `mapper.rs`.
  - Regenerate the reference with `cargo run -p blocks --example dump_schemas` and commit `docs/schemas/<chain>.md`; the drift test fails otherwise.
  - A schema change changes the table digests bound by protected output, so existing roots refuse to resume: document it in `docs/releases/unreleased.md` as a rebuild-required change. Row or routing changes without a schema change must advance `MAPPER_EPOCH` in `firehose-parquet/src/ingest/state.rs`.
  - A new `UInt64` column becomes a checked Delta `long`; list it in the chain's `ChainProfile::decimal_columns` (`blocks/src/chain.rs`) when it holds a currency amount or a value a sender or signer chooses without a range check. The Delta data schema digest in `blocks/src/chain/tests.rs` pins every chain's Delta types.
  - `blocks/src/schema_contract_tests.rs` covers every table; add fixture rows for a new table.
- Add a new chain family:
  - `blocks/src/<chain>/{mod,proto,schema,mapper}.rs` (exported from `blocks/src/lib.rs`); use the shared `firehose_parquet::traits` helpers (`push_fork_step_field`, `fork_step_builder`, `append_fork_step`, `finish_fork_step`, `est_fork_step`, `enum_data_type`, `estimated_dictionary_index_bytes`, `strip_enum_prefix`) instead of per-chain copies.
  - `blocks/src/chain.rs`: add a `ChainKind` variant (append it to `ChainKind::ALL`, which is also the `type_url` detection order), its `ChainProfile` in `ChainKind::profile` and its constructor in `ChainKind::create_mapper`. Add ordered `CHAIN_NAME_RULES` entries for endpoint-name inference. Fill `strict_chain_names` when the family has votes, nullable timestamps or unsupported extended output, because those flags are resolved before the first block.
  - `firehose-parquet/src/ingest/state.rs` (`BlockFamily`) and `ingest/mirror.rs`: add the protected family; its serde name must equal the profile label.
  - `blocks/src/bin/main.rs` (`BLOCK_TYPES`, the unsupported-type error list) and the `--block-type` help in `firehose-parquet/src/cli.rs` (`BuildArgs`). A test checks that `BLOCK_TYPES` matches `ChainKind::ALL`.
  - `blocks/src/schema_docs.rs`: add the chain so `docs/schemas/<chain>.md` is generated.
  - The #526 oracle tests (`blocks/src/chain/tests.rs`, `blocks/src/bin/chain_profile_tests/`) cover the nine current families; extend their expected lists and re-pin the schema digests (`CURRENT_SCHEMA_DIGEST`, `DELTA_DATA_SCHEMA_DIGEST`; the historical `FINAL_ONLY_SCHEMA_DIGEST` and `PRE_550_SCHEMA_DIGEST_WITHOUT_RESTRUCTURED` leave out families added after them, through `schema_digest_where`'s family filter).
  - A family whose Firehose block id is not a hash sets `ChainProfile::block_id_text` and prepares its ids with `CanonicalBuilder::prepare_with_text_ids` (HyperCore); the file metadata then records `block_id_encoding = decimal`.
- Adjust per-family behavior (encodings, nullable timestamps, block gaps, extended/votes, failed-transaction defaults):
  - `blocks/src/chain.rs` (`ChainProfile`); ingestion reads these properties instead of comparing `block_type` strings.
- Change partitioning or output file layout:
  - `firehose-parquet/src/date_partition.rs` (the `date=YYYY-MM-DD` key) and `firehose-parquet/src/writer.rs` (`partition_suffix` and the partition contract, `validate_partition`).
  - A layout change must advance `MAPPER_EPOCH` in `firehose-parquet/src/ingest/state.rs`, so protected roots of the old layout are refused instead of resumed into a mixed layout; `PendingTransaction::validate` checks recorded partitions against the layout.
  - `blocks/tests/engine_compat.rs` reads real output with DuckDB and delta-rs through the Delta logs, and `blocks/tests/delta_maintenance.rs` after the maintenance job compacted it.
  - Artifact locations (`_fireparq/`) and what the protected-root walker skips (`_delta_log/`): `firehose-parquet/src/artifacts.rs` only; the dataset root must hold only table directories, `_fireparq/` and dot-prefixed control state (`assert_dataset_root_layout` in `blocks/tests/ingestion_transactions.rs`).
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
  - `firehose-parquet/src/s3.rs` (client policies), `s3/upload.rs` (ingestion uploads), `dataset_lock_s3.rs` (bucket owner), `cli/paths.rs` (destination rules).
- Change the Delta maintenance job (settings, order, output):
  - `maintenance/src/lib.rs`; keep `docs/delta-maintenance.md`, `deploy/examples/delta-maintenance-cronjob.yaml` and design §9 in step. Its tests: `maintenance/tests/cli.rs` (the binary) and `blocks/tests/delta_maintenance.rs` (beside a real `build`). Never enable `datafusion` outside `maintenance/`.
- Change encoding of hashes/addresses/bytes:
  - `firehose-parquet/src/encode.rs` and the mapper call sites in `blocks/src/*/mapper.rs`; per-family defaults live in `blocks/src/chain.rs`.
- Change gRPC retry/auth/stream lifecycle:
  - `firehose-parquet/src/grpc.rs` and `firehose-parquet/src/auth.rs`.
- Change built-in `--network` aliases or endpoint override behavior:
  - `firehose-parquet/src/networks.rs` (resolution and `FIREHOSE_ENDPOINT_*` overrides).
  - `scripts/generate_networks.rs` (provider policy); regenerate `firehose-parquet/src/networks_generated.rs` instead of editing it, following `docs/network-registry-integration.md`.
- Change metrics names/labels/endpoint behavior:
  - `firehose-parquet/src/metrics.rs`, plus the metrics table in `docs/metrics.md`.
- Change protobuf definitions:
  - `proto/*.proto` (and `proto/core/*.proto`); bindings are rebuilt by Cargo via `firehose-protos/build.rs`.
- Cut a release:
  - Set `[workspace.package] version` in `Cargo.toml` and run `cargo update -w`.
  - Fold `docs/releases/unreleased.md` into `docs/releases/vX.Y.Z.md` and reset it.

## Build, Run, and CI Anchors

- Toolchain: `rust-toolchain.toml` pins Rust 1.98, which CI and the release workflow install with `rustup toolchain install`; the `Dockerfile` builder image matches it. `deltalake-core` 1.0.0 needs at least 1.94.1.
- Build workspace: `cargo build --workspace`
- Run tests: `cargo test --workspace --locked`
- Format: `cargo fmt --all`
- Real-path ingestion regressions: `blocks/tests/ingestion_transactions.rs` drives the built `fireparq` binary against a cursor-aware mock Firehose; prefer it over unit tests of helpers when a fix concerns what `build` commits
- Regenerate the schema reference: `cargo run -p blocks --example dump_schemas`
- Documented SQL and programs: `blocks/tests/non_final_stream.rs` runs the live view of `docs/non-final-streams.md` ("Canonical live view") over its non-final Delta tables, and `blocks/tests/engine_compat.rs` the HyperCore SQL of `docs/chains/hypercore.md`, with DuckDB's `delta` extension (the engine helpers of `tests/common/mod.rs`: `FIREPARQ_DUCKDB`, else `duckdb` on `PATH`, 1.5 or later); CI installs a pinned, checksum-verified CLI and sets `FIREPARQ_REQUIRE_DUCKDB`, so the check cannot be skipped there. The Python (Polars) examples in `docs/` are for users and are not run: no repository code needs Python, and CI installs none
- Engine compatibility and maintenance: `blocks/tests/engine_compat.rs` builds EVM (final and non-final), Solana and HyperCore output and reads every Delta table with DuckDB `delta_scan` and delta-rs (Polars' `scan_delta` reads through delta-rs, which these reads cover); `blocks/tests/delta_tables.rs` checks the logs; `blocks/tests/delta_maintenance.rs` runs the maintenance job beside a real `build`; `blocks/tests/delta_readers.rs` runs `validate`, `inspect` and the table summary from the log over Delta tables, after the job's OPTIMIZE and checkpoint; `blocks/tests/delta_recovery.rs` runs the job (and a delta-rs full VACUUM) between crashes and restarts. Helpers (`blocks/tests/common/mod.rs`): the DuckDB CLI (as above; its `delta` extension from `FIREPARQ_DUCKDB_EXTENSION_DIR`, installed there when missing, and checked against `FIREPARQ_DUCKDB_DELTA_VERSION` when set), Delta reads through `deltalake-core` snapshots (with `firehose-parquet`'s features: no DataFusion), and `fireparq maintenance` of the binary under test. CI pins DuckDB 1.5.5 and its `delta` extension by checksum and sets `FIREPARQ_REQUIRE_DUCKDB`. Locally, for example: `FIREPARQ_DUCKDB=/path/to/duckdb-1.5.5 cargo test -p blocks --test engine_compat --test delta_tables --test delta_maintenance --test delta_readers --test delta_recovery`, and `cargo test -p fireparq-maintenance` for the compaction's own tests. The opt-in anonymous read of a deployment bucket (`FIREPARQ_RGW_ENDPOINT`, `FIREPARQ_RGW_BUCKET`) is in `docs/reading-tables.md` ("Engine compatibility")
- Nothing links DataFusion: `maintenance/` compacts with its own code over delta-rs's Parquet writer, so `cargo tree --workspace` lists no `datafusion` crate, dev-dependencies included (a CI step checks it).
- Build release: `cargo build --release -p blocks` (`fireparq`, the maintenance job included)
- Run binary from source: `cargo run --bin fireparq -- --help`
- Run ingestion (preferred form): `cargo run --bin fireparq -- build --network mainnet --start-block 100`
- Install binaries locally: `cargo install --path blocks` (`fireparq`)
- Generate shell completions: `cargo run --bin fireparq -- completions zsh`
- CI entrypoint: `.github/workflows/ci.yml` (`build-and-test` and the `advisories` job, which calls `advisories.yml`)
- Dependency advisory gate: `cargo deny --locked check advisories` in `.github/workflows/advisories.yml` (on every push and pull request through `ci.yml`, weekly on its own, and on manual dispatch), configured by `deny.toml` (RustSec advisories only; ignored advisories need a recorded reason)
- Crash-test hooks: `FIREPARQ_DEBUG_FAULT` fails the real binary at a named step for recovery tests (`blocks/tests/ingestion_transactions.rs`; the Delta kinds, including `crash-at:<Stage>`, are listed in `ingest/controller/pipeline.rs` and used by `blocks/tests/delta_recovery.rs`), and `FIREPARQ_DEBUG_PACE_SAMPLE_MS` shortens the catch-up detection windows (`blocks/tests/adaptive_flush.rs`). Only debug builds (as built by `cargo test`) read them; release binaries ignore them.
- Docker publish workflow: `.github/workflows/docker-publish.yml` builds the `firehose-parquet` image (`Dockerfile`, `fireparq` as its entrypoint, the maintenance job included); tags push it, a manual run only builds it
- Release assets workflow: `.github/workflows/release.yml` (supports a dry-run dispatch)
- Built-in network endpoint check (weekly, needs network access): `.github/workflows/network-endpoints.yml`, locally `scripts/check_network_endpoints.sh`

## Parquet Enum Convention

- Materialized protobuf enum-backed fields should be written to Parquet as stable protobuf label strings, not raw integer values.
- Prefer Arrow dictionary-encoded `Utf8` (`Dictionary(Int32, Utf8)`) for enum columns that are newly materialized or migrated for readability. The Delta data files store them as plain `string` (#643); Parquet still dictionary-encodes their pages.
- Existing readable `Utf8` enum columns remain acceptable until they are migrated to the shared representation.
- Bitcoin currently has no protobuf enum-backed fields materialized into Parquet.

## Source vs Generated/Artifact Directories

- Source-of-truth directories:
  - `blocks/`, `firehose-parquet/`, `firehose-protos/`, `proto/`, `scripts/`, `deploy/examples/`, `.github/workflows/`
- Generated but committed (regenerate instead of editing):
  - `firehose-parquet/src/networks_generated.rs` (`generate-networks`)
  - `docs/schemas/*.md` (`cargo run -p blocks --example dump_schemas`)
- Generated build artifacts (do not edit or commit):
  - `target/` (Cargo build output)
  - protobuf Rust modules under Cargo `OUT_DIR` (created during `firehose-protos` build)
- Runtime output examples (produced data, not source):
  - `output/` (local output samples: Delta tables and control state)
