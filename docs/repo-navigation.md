# Repository Navigation Guide

This document is a tool-agnostic map of the `firehose-parquet` workspace: where code lives, how data flows, and where to edit for common tasks.

Related docs:

- `README.md`: the front page (what fireparq does, supported chains, install, quick start, the recommended deployment in brief) and a table of every page below.
- `docs/getting-started.md`: install (release archives, Docker image, from source), every `build` example of the quick start, and reading a first run with DuckDB.
- `docs/authentication.md`: provider-scoped Firehose credentials and explicit credential selectors.
- `docs/cli.md`: the CLI reference: common ingestion flags, network aliases, environment variables and `.env` loading, the advanced recovery, transport and S3 knobs, flushes (size, concurrency, the catch-up rule, idle streams), per-family defaults, and the `maintenance`, `hypercore-reference`, `inspect`, `validate`, `recovery` and `completions` subcommands.
- `docs/features.md`: the full feature list, each linked to the page that documents it.
- `docs/non-final-streams.md`: `--final-blocks-only=false`, the append-only `fork_step` / `stream_ordinal` history and the canonical live view (its SQL is run by `blocks/tests/non_final_stream.rs`).
- `docs/output-layout.md`: the dataset root and `{chain}`, the directory tree, single-network buckets, canonical identity columns, identifier encoding by block type, and Parquet file and lookup metadata.
- `docs/reading-tables.md`: reading the Delta tables with DuckDB and Polars, the frontier rule for consistent reads, a day's block range, and engine compatibility.
- `docs/chains/`: per-chain notes (`README.md` indexes them): failed transaction filtering and the EVM, Solana, Bitcoin, Beacon, Tron, Cosmos, Antelope, NEAR, SEC EDGAR and HyperCore semantics (joins, ordering, example queries; HyperCore's views, monitors and cookbook, which `blocks/tests/engine_compat.rs` runs). `sec.md` also holds the shipped SEC views (`sec-views-v1`), whose SQL `blocks/tests/sec_docs_sql.rs` runs.
- `docs/deployment.md`: the recommended final-only deployment, one writer per network.
- `docs/cursor-and-resume.md`: output authority and the cursor mirror, startup cost, crash recovery, local part publication, S3 cursors, ownership, graceful shutdown, and start and stop blocks.
- `docs/delta-maintenance.md`: the `fireparq maintenance` job, its settings, row order and CronJobs.
- `docs/hypercore-reference.md`: `fireparq hypercore-reference`, the HyperLiquid `/info` reference tables (`hl_*`) beside a HyperCore lake: tables, their views and the joins with the lake (run by `blocks/tests/hypercore_reference_cli.rs` and `engine_compat.rs`), settings, events, concurrency, compaction and VACUUM, the S3 user and CronJobs.
- `docs/metrics.md`: the Prometheus metrics and the `/health` and `/ready` endpoints.
- `docs/development.md`: build and test commands, the repository structure and the CLI architecture.
- `docs/releases/`: release notes; `docs/releases/unreleased.md` collects changes merged since the last release.
- `docs/schemas/`: per-chain table and column reference, generated from the code (see "Change a table schema" below).
- `docs/audit/README.md`: issue-by-issue implementation and validation records of the September 2026 audit (#463).
- `docs/design/delta-lake.md`: the Delta Lake output design for the v1.0.0 launch (#643), implemented in lanes L1–L5 and L7–L10 (L6, `verify`, is #666): pinned versions, protocol and table properties, the commit mapping onto the #468 transaction, crash matrix and VACUUM rule, ownership (#636), type mapping, removals, resume cost (#655), the maintenance job (§9) and reader examples.
- `docs/partition-vocabulary.md`: the single `date=YYYY-MM-DD` output key and the CLI terms that refer to it.
- `docs/network-registry-integration.md`: how built-in `--network` aliases are generated from the registry and the internal list of Pinax-served networks it lacks (`PINAX_NETWORKS`), the provider policy, and the endpoint check.

## Workspace Layout

- `Cargo.toml` (root): Rust workspace with five members, `firehose-protos`, `firehose-parquet`, `blocks`, `maintenance` and `reference`, sharing one `[workspace.package]` version. All code is Rust; nothing in the repository needs Python.
- `firehose-protos/`: protobuf compilation crate.
  - `build.rs`: compiles `proto/*.proto` and `proto/pinax/{sec,hypercore}/v1/*.proto` into Rust modules with `tonic-prost-build` (`protoc` must be on `PATH`); byte fields are generated as `Bytes`.
  - `src/lib.rs`: exposes the compiled modules and aliases (`firehose`, `eth`, `solana`, `sec`, `hypercore`, ...).
- `firehose-parquet/`: core library crate used by the binary.
  - CLI (`src/cli.rs` and `src/cli/`):
    - `cli.rs`: Clap definitions (`Commands`, `GrpcArgs`, `AwsArgs`, `CommonArgs`, `BuildArgs`) and the stable public re-exports. The root `Cli` and its `GlobalArgs` (logging, `--env-file`) are in `blocks/src/bin/main.rs`.
    - `cli/configuration.rs`: CLI-to-`Config` conversion, value parsers, working-directory `.env` / `--env-file` loading, logging and completions.
    - `cli/paths.rs`: local/S3 input and output policy (explicit `s3://` for writes, `S3_BUCKET` shorthand for reads), credential preflight, and the `--output` dataset-root resolver (`resolve_output_root`, `{chain}` expansion, `{{`/`}}` escapes).
    - `cli/hypercore_reference.rs`: the arguments of `fireparq hypercore-reference` (options, jobs, the `--outcome-ids`, `--coins` and `--funding-start` parsers); the job itself is `reference/`.
    - `cli/inspect.rs`, `cli/validate.rs`: read-only single-file `inspect`, and `validate`, which checks block continuity over the active files of a pinned Delta snapshot of a `blocks` table (never a directory walk, #643).
    - `cli/{tests,validate_tests}.rs`: CLI and validation regressions.
  - Configuration and endpoints:
    - `src/config.rs`: pipeline `Config`, `Compression` (including `zstd:<level>`) and receive-transport defaults.
    - `src/date_partition.rs`: the `date=YYYY-MM-DD` partition key, the only output layout (#652): `DatePartition` formats it from a block time, parses it strictly, and gives its `Date32` value.
    - `src/networks.rs`, `src/networks_generated.rs`: built-in `--network` aliases (generated; do not edit), `FIREHOSE_ENDPOINT_*` overrides, and the hand-written `NETWORK_DATA_ORIGINS` (where a network's known data starts; the start-block resolution in `blocks/src/bin/main.rs` applies it).
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
    - `src/delta/store.rs`: where the tables live (`DeltaStore`: a local root or an S3 bucket and prefix), each table's conditional-put log store, the `s3://` log store factory, `s3_data_client` (the same client with three retries of every request, for data files whose names are unique; `fireparq hypercore-reference` writes its data files through it), and `s3_log_client`, the object_store 0.13 client built from `AwsConfig` (attached to the output bucket's owner, `S3Ownership::delta_log`) that sends every write once; local commits are synced before they count. `store/read_retry.rs` is its HTTP connector (#680): idempotent reads (`GET`, `HEAD`) are sent up to 3 times on a transport error, 408, 429 or 5xx, with jittered backoff and a warning per retry, and every other method once.
    - `src/delta/stats.rs`: each part's `add.stats` (`numRecords`, bounds and null counts of `block_num` and `timestamp`), journaled with its receipt.
    - `src/delta/commit.rs`: `DeltaTables`, opened by `IngestionSession::open` before recovery (created only while authority is at ordinal 0, then validated: properties, identity, protocol, partition and schema; `open_existing` for `recovery recover`, without the schema), `commit`, one Delta commit per table with a part of a Committed transaction (`add` + `txn`, `blocks` last), called by the controller before authority advances (#643 L3), and recovery's `check_progress` (every table's `txn` against authority: the log-ahead refusal and the tables a Committed journal is missing from) and `roll_forward` (those commits, a same-`appId` conflict resolved from the log) (#643 L4). `commit/tests/loopback_s3.rs` is the loopback S3 endpoint of its tests.
  - Ownership, durable state and S3:
    - `src/dataset_lock/{mod,local,operation,session}.rs`: local directory-inode ownership shared by `build` and `recovery`; `DatasetOwnership::finish` ends `build` ownership (release after success or after a failure whose requests all had a definite outcome). `src/dataset_lock_s3.rs`: the persistent bucket-wide S3 owner (`.fireparq-owner-v1.json`) and its uncertainty latch; its tests model Ceph RGW 19's `If-Match` comparison (`dataset_lock_s3/rgw19_store.rs`, #678).
    - `src/durable_state.rs`, `src/durable_state_s3.rs`: strict versioned local/remote control records and CAS tombstones.
    - `src/recovery.rs`: `fireparq recovery` (`status`, `recover`, `release`): ownership inspection, guarded transaction recovery and provider-quiescent S3 owner release.
    - `src/s3.rs`: shared AWS configuration and S3 client builders with explicit credential/retry policies (`build_ingestion_mutation_client`, `AwsConfig::build_read_client`).
    - `src/s3/upload.rs`: native ingestion uploads (disk spool, one conditional PUT, spooled readback verification, explicit timeouts).
  - Discovery, artifacts and metrics:
    - `src/maintenance/discovery.rs`: `visit_objects`, the paged listing with a timeout per request and no total deadline, and `ListingStats` (#655), which the startup checks of `build` and `recovery` use, and `inspect`'s whole-object read. No table is read through a listing. `discovery/paged_bucket.rs` is a test bucket of a million generated keys that counts LIST pages.
    - `src/artifacts.rs`: dataset artifact names: `ARTIFACTS_DIR` (`_fireparq/`), `DEFAULT_CURSOR_MIRROR` (`_fireparq/cursor.parquet`, the clap default), the owner record names, and `DELTA_LOG_DIR` / `is_in_delta_log`, which the protected-root walker of `recovery` and `build` (`ingest/maintenance.rs`) skips.
    - `src/metrics.rs`: Prometheus registry, `/metrics`, `/health` and `/ready`.
  - `tests/parquet_compatibility.rs`: reads the Parquet 58 files of `tests/fixtures/parquet58/` (never regenerate them) with the current Parquet crate.
- `blocks/`: chain-specific mapping crate and the unified `fireparq` binary.
  - `src/bin/main.rs`: `fireparq` entrypoint, command dispatch (`Commands::*`), shared ingestion helpers, and the `.env.example` drift test.
  - `src/bin/ingestion/mod.rs`: `run_ingestion`, the `fireparq build` orchestration; dataset ownership outlives the session and runtime.
  - `src/bin/ingestion/setup.rs`: endpoint preflight (EndpointInfo, block type, encodings), resumed configuration and metrics labels.
  - `src/bin/ingestion/runtime.rs`: ordered receive/filter/routing state and flush windows; only the borrowed session commits authority and mirrors.
  - `src/bin/chain_profile_tests/`: #526 oracle tests for chain detection and profiles.
  - `src/chain.rs`: `ChainKind` / `ChainProfile`, the per-family facts (label, `type_url` marker, protected family, encoding contract, nullable timestamps, block gaps, extended/votes handling, failed-transaction default, chain-name rules, the `decimal_columns` its Delta tables store as `decimal(20,0)`, text block ids, `build` defaults in `BuildDefaults`) and mapper construction.
  - `src/<chain>/{mod,proto,schema,mapper}.rs` for `evm`, `solana`, `beacon`, `near`, `antelope`, `tron`, `cosmos`, `bitcoin`, `hypercore`, and `src/sec/` (43 tables: `schema/`, `parse.rs`, `prepare/`, `build/`, `mapper.rs`) for `sec`: protobuf aliases, Arrow table schemas and row mapping, plus chain-specific helpers (`evm/decimal.rs`, `solana/vote.rs`, `tron/contracts.rs`, `bitcoin/amounts.rs`, `cosmos/tx_metadata.rs`, `antelope/text.rs`, `hypercore/decimal.rs`, the strict `decimal(38,10)` parser) and value/outcome tests.
  - `src/hypercore/`: the HyperCore (`pinax.hypercore.v1`) mapper stages and validates a whole block before appending (refusal rules R1–R11 in `docs/chains/hypercore.md`), derives `fills.market_type`, `fills.dex`, `fills.counterparty`, `outcome_fills`, `liquidations`, `funding_rates` and the event routing in an infallible `derive()` step (rules R-D1–R-D6), and stops a block whose routed event values lack a column of their table before any append (`check_routed_columns`, a mapper-bug guard). Its block ids are decimal text (`CanonicalBuilder::prepare_with_text_ids`). `schema.rs` builds the five event tables from one column catalogue (`event_fields`) and stamps every schema with the derivation version (`fireparq.hypercore.derivation`). `value_tests.rs` maps the real fixtures of `tests/fixtures/hypercore/` (golden counts and values, byte-exact payload rebuilds, every case of the vendored `EventBody` and `LedgerUpdateDelta` oneofs, an independent re-derivation of every derived value, a pinned output hash) and checks every refusal; `fixtures.rs` derives the contract-test blocks. `HYPERCORE_SCHEMA_DIGEST` (`src/chain/tests.rs`) pins its schemas with their metadata, so a change, even of the derivation version alone, needs a new output root.
  - `src/schema_docs.rs`: renders `docs/schemas/<chain>.md` from the schema constructors; `examples/dump_schemas.rs` writes the files and a test fails when they drift.
  - `src/schema_contract_tests.rs`: every table of every chain, under every encoding and both `fork_step` settings, has unique names and round-trips through Parquet, and its Delta data file mapping holds only Delta types, exactly the profile's `decimal(20,0)` columns and every value; a value above `i64::MAX` in a `long` column is refused.
  - `src/mapping_bench.rs`: ignored whole-block mapping benchmarks.
  - `examples/`: replay, benchmark and golden-refresh tools (`replay_*`, `bench_*`, `measure_flush_sizing`, `refresh_evm_golden`, `refresh_sec_golden`, `dump_schemas`). `replay_sec` serves a firesec `.fire` file to the real binary and reports peak RSS and each table's Delta files and rows (run by hand on the sample days, not in CI); `bench_ingestion_concurrency` is the #516 flush-concurrency benchmark; `bench_live_flush` runs the real binary against a looping mock Firehose and a loopback HTTPS S3 with injected latency (#658 catch-up throughput and commit phases). That S3 endpoint, `bench_live_flush/s3.rs`, is also the loopback S3 of the Delta tests: it injects per-request faults (a status, a dropped connection, a lost answer to an applied write) for a key, a key prefix or a listing, and records each request's `If-Match`, `If-None-Match`, raw query, ACL and tagging headers and a bulk delete's keys.
  - `tests/`: real-binary integration tests against a mock Firehose (the Delta tests also on the loopback S3 of `examples/bench_live_flush/s3.rs`):
    - Ingestion: `ingestion_transactions.rs` (protected transactions, crash and restart), `dataset_ownership.rs`, `endpoint_info_startup.rs` (Info before any destination; HyperCore's data origin as the default start and the refusal of earlier starts), `metrics_readiness.rs`, `shutdown_signals.rs`, and `adaptive_flush.rs` (a mock paced faster than or at real time, with stalls for `--flush-idle-secs`).
    - Delta: `delta_tables.rs` (the logs of a `build` and a clean restart, a crash after every Delta commit, RGW 19's unquoted `If-Match` ETags (#678), retried log reads and log commits sent once (#680)); `delta_recovery.rs` (every row of the design's crash matrix and `recovery recover`, with the maintenance job or a delta-rs full VACUUM between crash and restart, and the job beside a running `build`, #636); `delta_readers.rs` (`validate` over a pinned snapshot after OPTIMIZE and a checkpoint, `inspect`, the table summary of `docs/reading-tables.md`).
    - Maintenance: `delta_maintenance.rs` (`fireparq maintenance` run over and over beside a real `build`: row order, carried footer metadata, the VACUUM rules, `VACUUM=0`, skipped missing tables, no `If-Match` from delta-rs) and `maintenance_cli.rs` (the subcommand itself: settings, exit statuses and JSON lines, store errors with credentials redacted, a full VACUUM that keeps `metadata/`).
    - Engines and documented SQL: `engine_compat.rs` (every Delta table of EVM final and non-final, Solana, HyperCore and SEC output through DuckDB `delta_scan` and delta-rs; the HyperCore SQL of `docs/chains/hypercore.md` over its 36 fixture blocks, and the reference joins and cross-checks of `docs/hypercore-reference.md` over them and `hl_*` tables the reference job wrote; a compacted HyperCore root that validates and resumes; the opt-in anonymous read of a deployment bucket; and a check that the "Engine compatibility" section of `docs/reading-tables.md` names every family and DuckDB type it reads), `non_final_stream.rs` (`stream_ordinal` and the live view of `docs/non-final-streams.md`) and `sec_docs_sql.rs` (the SQL of `docs/chains/sec.md` on a `build` of the SEC fixture).
    - Reference tables: `hypercore_reference_cli.rs` (`fireparq hypercore-reference` over a loopback `/info`: exit statuses, configuration errors before any request, the URL and credentials never printed, and the views of `docs/hypercore-reference.md` in DuckDB over the tables it wrote) and `hypercore_reference_s3.rs` (on the loopback S3: a lost conditional commit, a lost commit answer, a failed confirming read, a retried data write, and the requests' envelope against the S3 user's grant).
    - Goldens: `evm_golden.rs` (`tests/fixtures/`) and `sec_golden.rs` (every column of every table of `tests/fixtures/sec-v013/` against the reference prototype's rows; helpers in `tests/sec_fixture/mod.rs`). The HyperCore fixtures in `tests/fixtures/hypercore/` are mapped by `src/hypercore/value_tests.rs`.
    - `tests/common/mod.rs`: the shared engine helpers (the DuckDB CLI, Delta reads through `deltalake-core` snapshots, delta-rs VACUUM and checkpoints, `fireparq maintenance` of the binary under test, the Markdown sections' SQL) and `tests/common/info.rs` (a loopback `/info` answering from the reference crate's fixtures).
- `maintenance/`: the Delta maintenance job (#643, design §9; `docs/delta-maintenance.md`), a library crate run as `fireparq maintenance` (`blocks/src/bin/main.rs`); it links no DataFusion.
  - `src/lib.rs` (`fireparq_maintenance`): settings from the environment (`Settings::from_env`), the lake's object store and delta-rs log store (`Lake`: local or S3 with conditional-create commits), the per-table order (OPTIMIZE of closed dates, VACUUM, checkpoint, log cleanup), the `firehose-parquet.*` footer keys a compaction carries (`carried_metadata`), a full VACUUM that keeps each table's `metadata/` (`ICEBERG_METADATA_DIR`, Iceberg metadata of an XTable sync), and the JSON-line output (`run`). VACUUM, checkpoints and log cleanup are delta-rs's; OPTIMIZE is the crate's own.
  - `src/compact.rs`: the OPTIMIZE that keeps the writer's row order: it concatenates parts in block order and sorts a file an older compaction left out of order back into it (#690). `src/row_order.rs`: each EVM table's in-block key for that repair.
  - `tests/compaction.rs`: the compaction's row order on local tables: a repair across several windows, a tie left as it is, repairs spread over runs, a table with no known order. `fireparq maintenance` itself is tested by `blocks/tests/maintenance_cli.rs` and `blocks/tests/delta_maintenance.rs`.
- `reference/`: `fireparq hypercore-reference` (`docs/hypercore-reference.md`), the HyperLiquid `/info` reference tables, a library crate (`fireparq_reference`) run by `blocks/src/bin/main.rs`, a Rust port of k8s-parquet PR #82's Python job; it links no DataFusion.
  - `src/lib.rs`: `Settings` (from the clap arguments of `firehose-parquet/src/cli/hypercore_reference.rs`; an empty value is unset), the configuration checks, `start`, the job, `done` and the exit status (`run`, `run_with`).
  - `src/json.rs` (the lossless JSON tree over serde_json's `RawValue` and the canonical text payload hashes cover), `src/values.rs` (decimals, times, `k:v` descriptions, ids, first-position maps), `src/model.rs` (the eleven tables and the row builders), `src/arrow.rs` (Delta schemas and properties, rows to and from delta-rs's Arrow 59).
  - `src/info.rs` (the rate-limited, retrying `/info` client that never shows its URL), `src/clock.rs`, `src/log.rs` (sorted, redacted JSON lines).
  - `src/store.rs` (roots and their S3 guards, open/create/evolve, reads of a pinned snapshot's files, appends committed once, checkpoints), `src/compact.rs` (an Arrow-level OPTIMIZE), `src/vacuum.rs` (a full VACUUM planned by delta-rs, committed by the job).
  - `src/jobs/`: outcomes, meta, funding, maintain and verify; `src/testing.rs` (the fake clock, `/info` and log, also for `blocks` tests through the `test-support` feature).
  - Tests: unit tests per module, `src/{tests,jobs/tests,store/tests}.rs`, and `tests/golden.rs` (byte parity with the Python job: `tests/fixtures/golden-vectors.json` and the recorded `/info` fixtures, `tests/fixtures/README.md`).
- `proto/`: source `.proto` files and Buf config, including `proto/core/*` dependencies and the SEC and HyperCore `proto/pinax/{sec,hypercore}/v1/*.proto` (vendored with their package path and imports).
- `scripts/`: `generate_networks.rs` (the `generate-networks` bin that writes `firehose-parquet/src/networks_generated.rs`) and `check_network_endpoints.sh` (live check of every built-in endpoint).
- `deploy/examples/`: example Kubernetes manifests for adoption in a deployment repository (`delta-maintenance-cronjob.yaml`: the hourly maintenance and weekly full-VACUUM CronJobs, the writer's image with `args: ["maintenance"]`; `hypercore-reference-cronjob.yaml`: the reference tables' three CronJobs, `args: ["hypercore-reference", "<job>"]`).
- `.env.example`: every environment variable the CLI reads, kept in sync by a test.
- `deny.toml`: cargo-deny advisory policy.
- `.github/workflows/`: `ci.yml`, `advisories.yml`, `docker-publish.yml`, `release.yml`, `network-endpoints.yml`.

## Data-Flow Mental Model

1. `blocks/src/bin/main.rs` parses the CLI (`firehose-parquet/src/cli.rs`), loads `.env` from the working directory or `--env-file`, and dispatches. `fireparq build` (`Commands::Build(BuildArgs)`) calls `ingestion::run_ingestion` in `blocks/src/bin/ingestion/mod.rs`.
2. `ingestion/setup.rs` resolves the endpoint and provider-scoped credentials (`auth.rs`), requires EndpointInfo (`grpc.rs`), selects the `ChainKind` (`blocks/src/chain.rs`: from `--block-type`, the endpoint chain names, or the first payload's `type_url` in a dry run) and resolves the dataset root (`resolve_output` in `main.rs`, which calls `firehose_parquet::cli::resolve_output_root`: `--output` as given, with an opt-in `{chain}` placeholder expanded to the EndpointInfo chain name) and cursor mirror.
3. `build` and `recovery recover` acquire dataset ownership first (`dataset_lock/`, `dataset_lock_s3.rs`). `ingest/session.rs` then recovers any pending transaction and opens the accepted frontier from the authority under `<dataset root>/.fireparq-ingest/` before any Blocks request.
4. Stream messages come from `grpc.rs`. `ingestion/runtime.rs` filters, orders and routes them; the chain mapper (`blocks/src/<chain>/mapper.rs`) decodes protobuf blocks and appends Arrow columns using `schema.rs`.
5. When a flush trigger fires (`flush.rs`, partition boundaries, completion), `ingest/controller.rs` journals the all-table transaction, `writer/protected.rs` publishes the deterministic parts (local via `writer/local.rs`, S3 via `s3/upload.rs`) and verifies them, the controller commits each table's part to its Delta log (`delta/commit.rs`, `blocks` last), then advances authority, then the optional `_fireparq/cursor.parquet` mirror.
6. `metrics.rs` exposes counters, readiness and health.
7. `recovery recover` takes the same ownership and runs the same transaction recovery offline (`ingest/maintenance.rs`). Compaction and cleanup of the Delta tables are a separate CronJob, `fireparq maintenance` (`maintenance/`), not writer code (#643, `docs/design/delta-lake.md` §9). The HyperLiquid reference tables are a separate CronJob too, `fireparq hypercore-reference` (`reference/`), writing their own root, never a fireparq one.

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
  - The #526 oracle tests (`blocks/src/chain/tests.rs`, `blocks/src/bin/chain_profile_tests/`) cover the ten current families; extend their expected lists and re-pin `CURRENT_SCHEMA_DIGEST` and `DELTA_DATA_SCHEMA_DIGEST` (the historical `FINAL_ONLY_SCHEMA_DIGEST` and `PRE_550_SCHEMA_DIGEST_WITHOUT_RESTRUCTURED` leave later families out through `schema_digest_where`). A re-pin cannot hide a change to an existing family: `pre_sec_families_reproduce_their_pre_sec_digests` holds the eight older families to their pre-SEC digests, and `each_later_family_reproduces_its_own_pinned_digests` holds SEC and HyperCore to their own mapper and Delta digests over every option and encoding. A new family adds its own pair there.
  - A family whose Firehose block id is not a hash sets `ChainProfile::block_id_text` and prepares its ids as text (`CanonicalBuilder::prepare_with_text_ids` in HyperCore, `PreparedIdentity::with_text_ids` in SEC); the file metadata then records `block_id_encoding = decimal`, and `docs/schemas/` types the ids `string` (decimal).
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
  - `maintenance/src/lib.rs` (settings, order, output), `compact.rs` and `row_order.rs` (compaction); the `Maintenance` help in `firehose-parquet/src/cli.rs` lists the settings. Keep `docs/delta-maintenance.md`, `deploy/examples/delta-maintenance-cronjob.yaml` and design §9 in step.
  - Tests: `maintenance/tests/compaction.rs` (row order), `blocks/tests/maintenance_cli.rs` (the subcommand) and `blocks/tests/delta_maintenance.rs` (beside a real `build`).
  - Never enable `datafusion`, in `maintenance/` or anywhere else (a CI step checks it).
- Change the HyperLiquid reference job (`fireparq hypercore-reference`):
  - `reference/src/` (jobs, tables, `/info` client, store); its arguments and help in `firehose-parquet/src/cli/hypercore_reference.rs` and the `HypercoreReference` help in `firehose-parquet/src/cli.rs`. Keep `docs/hypercore-reference.md` (whose SQL the tests run), `deploy/examples/hypercore-reference-cronjob.yaml` and `.env.example` in step.
  - Tests: `cargo test -p fireparq-reference`, `blocks/tests/hypercore_reference_cli.rs`, `blocks/tests/hypercore_reference_s3.rs` and the joins in `blocks/tests/engine_compat.rs`. A changed table or row builder must still match `golden-vectors.json`, or say why not.
  - A new `hl_*` table needs the job's S3 user granted its prefix before the release runs.
- Change encoding of hashes/addresses/bytes:
  - `firehose-parquet/src/encode.rs` and the mapper call sites in `blocks/src/*/mapper.rs`; per-family defaults live in `blocks/src/chain.rs`.
- Change gRPC retry/auth/stream lifecycle:
  - `firehose-parquet/src/grpc.rs` and `firehose-parquet/src/auth.rs`.
- Change built-in `--network` aliases or endpoint override behavior:
  - `firehose-parquet/src/networks.rs` (resolution, `FIREHOSE_ENDPOINT_*` overrides and `NETWORK_DATA_ORIGINS`).
  - `scripts/generate_networks.rs` (provider policy and the internal `PINAX_NETWORKS` list); regenerate `firehose-parquet/src/networks_generated.rs` instead of editing it, following `docs/network-registry-integration.md`.
- Change metrics names/labels/endpoint behavior:
  - `firehose-parquet/src/metrics.rs`, plus the metrics table in `docs/metrics.md`.
- Change protobuf definitions:
  - `proto/*.proto` (and `proto/core/*.proto`, `proto/pinax/*/v1/*.proto`, each listed in `firehose-protos/build.rs`); bindings are rebuilt by Cargo via `firehose-protos/build.rs`.
- Cut a release:
  - Set `[workspace.package] version` in `Cargo.toml` and run `cargo update -w`.
  - Fold `docs/releases/unreleased.md` into `docs/releases/vX.Y.Z.md`, reset it, and add the release to the README's Documentation table.
  - Bump the documented image tags and download links (`README.md`, `docs/getting-started.md`, `docs/delta-maintenance.md`) and the image of every `deploy/examples/*.yaml` (`delta-maintenance-cronjob.yaml`, `hypercore-reference-cronjob.yaml`).

## Build, Run, and CI Anchors

- Toolchain: `rust-toolchain.toml` pins Rust 1.98, which CI and the release workflow install with `rustup toolchain install`; the `Dockerfile` builder image matches it. `deltalake-core` 1.0.0 needs at least 1.94.1.
- Build workspace: `cargo build --workspace`
- Run tests: `cargo test --workspace --locked`; CI also runs the golden-refresh examples' tests (`cargo test -p blocks --example refresh_evm_golden --example refresh_sec_golden --locked`)
- Format: `cargo fmt --all`
- Real-path ingestion regressions: `blocks/tests/ingestion_transactions.rs` drives the built `fireparq` binary against a cursor-aware mock Firehose; prefer it over unit tests of helpers when a fix concerns what `build` commits
- Regenerate the schema reference: `cargo run -p blocks --example dump_schemas`
- Documented SQL: `blocks/tests/non_final_stream.rs` runs the live view of `docs/non-final-streams.md`, `blocks/tests/sec_docs_sql.rs` the shipped views and example queries of `docs/chains/sec.md`, `blocks/tests/engine_compat.rs` the HyperCore SQL of `docs/chains/hypercore.md` and the reference joins of `docs/hypercore-reference.md`, and `blocks/tests/hypercore_reference_cli.rs` the reference views, all with DuckDB's `delta` extension. The Polars examples in `docs/` are for users and are not run: no repository code needs Python, and CI installs none
- DuckDB in tests (`blocks/tests/common/mod.rs`): the CLI of `FIREPARQ_DUCKDB`, else `duckdb` on `PATH` (1.5 or later; the checks are skipped locally without it), with its `delta` extension from `FIREPARQ_DUCKDB_EXTENSION_DIR` (installed there when missing, checked against `FIREPARQ_DUCKDB_DELTA_VERSION` when set). CI pins DuckDB 1.5.5 and the `delta` extension `45c4087` by checksum and sets `FIREPARQ_REQUIRE_DUCKDB`, so the checks cannot be skipped there. Locally, for example: `FIREPARQ_DUCKDB=/path/to/duckdb-1.5.5 cargo test -p blocks --test engine_compat --test delta_tables --test delta_maintenance --test delta_readers --test delta_recovery --test hypercore_reference_cli --test hypercore_reference_s3`, `cargo test -p fireparq-maintenance` for the compaction's own tests, and `cargo test -p fireparq-reference` for the reference job's. The opt-in anonymous read of a deployment bucket is in `docs/reading-tables.md` ("Engine compatibility")
- Rust only, no DataFusion: CI fails on any tracked `.py` or `requirements*.txt` file (`docs/audit/rust-only.md`) and on any `datafusion` crate in `cargo tree --workspace`, dev-dependencies included; `maintenance/` compacts with its own code over delta-rs's Parquet writer.
- Build release: `cargo build --release -p blocks` (`fireparq`, the maintenance and reference jobs included)
- Run binary from source: `cargo run --bin fireparq -- --help`
- Run ingestion (preferred form): `cargo run --bin fireparq -- build --network mainnet --start-block 100`
- Install binaries locally: `cargo install --path blocks` (`fireparq`)
- Generate shell completions: `cargo run --bin fireparq -- completions zsh`
- CI entrypoint: `.github/workflows/ci.yml` (`build-and-test` and the `advisories` job, which calls `advisories.yml`)
- Dependency advisory gate: `cargo deny --locked check advisories` in `.github/workflows/advisories.yml` (on every push and pull request through `ci.yml`, weekly on its own, and on manual dispatch), configured by `deny.toml` (RustSec advisories only; ignored advisories need a recorded reason)
- Crash-test hooks: `FIREPARQ_DEBUG_FAULT` fails the real binary at a named step for recovery tests (`blocks/tests/ingestion_transactions.rs`; the Delta kinds, including `crash-at:<Stage>`, are listed in `ingest/controller/pipeline.rs` and used by `blocks/tests/delta_recovery.rs`), and `FIREPARQ_DEBUG_PACE_SAMPLE_MS` shortens the catch-up detection windows (`blocks/tests/adaptive_flush.rs`). Only debug builds (as built by `cargo test`) read them; release binaries ignore them.
- Docker publish workflow: `.github/workflows/docker-publish.yml` builds the `firehose-parquet` image (`Dockerfile`, `fireparq` as its entrypoint, the maintenance and reference jobs included); tags push it, a manual run only builds it
- Release assets workflow: `.github/workflows/release.yml` (supports a dry-run dispatch)
- Built-in network endpoint check (weekly and on pull requests that change the generated list or the script; needs network access): `.github/workflows/network-endpoints.yml`, locally `scripts/check_network_endpoints.sh`

## Parquet Enum Convention

- Materialized protobuf enum-backed fields should be written to Parquet as stable protobuf label strings, not raw integer values.
- Prefer Arrow dictionary-encoded `Utf8` (`Dictionary(Int32, Utf8)`) for enum columns that are newly materialized or migrated for readability. The Delta data files store them as plain `string` (#643); Parquet still dictionary-encodes their pages.
- Existing readable `Utf8` enum columns remain acceptable until they are migrated to the shared representation.
- Bitcoin currently has no protobuf enum-backed fields materialized into Parquet.

## Source vs Generated/Artifact Directories

- Source-of-truth directories:
  - `blocks/`, `firehose-parquet/`, `firehose-protos/`, `maintenance/`, `reference/`, `proto/`, `scripts/`, `deploy/examples/`, `.github/workflows/`, `docs/` (except `docs/schemas/`)
- Generated but committed (regenerate instead of editing):
  - `firehose-parquet/src/networks_generated.rs` (`generate-networks`)
  - `docs/schemas/*.md` (`cargo run -p blocks --example dump_schemas`)
- Generated build artifacts (do not edit or commit):
  - `target/` (Cargo build output)
  - protobuf Rust modules under Cargo `OUT_DIR` (created during `firehose-protos` build)
- Runtime output examples (produced data, not source):
  - `output/` (local output samples: Delta tables and control state)
