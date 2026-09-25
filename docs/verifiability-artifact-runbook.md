# Verifiability Artifact Runbook

This runbook describes how to produce, publish, update, and consume verifiability artifacts for `fireparq verify`.

It complements:

- `docs/verify-report-contract.md` for the JSON report schema contract
- `docs/verifiability-hash-strategy.md` for chain-aware hashing behavior

## Dataset Layout and Resolution

`fireparq build` writes one directory per network, the **chain root**:

```
<output>/<chain_name>/                 chain root (for example output/mainnet, output/sepolia)
  <table>/<partition dirs>/*.parquet   table data (blocks/, transactions/, ...)
  cursor.parquet                       resume state (build)
  partitions.parquet                   partition index (partitions build)
  merkle_roots.parquet                 canonical root registry (verify)
  verify_runs/<run_id>/report.json     per-run reports (verify --publish-report)
```

`verify` checks one table of one network per run. Point it at a table directory (`output/mainnet/blocks`), a partition directory or file inside it, or a chain root that holds a single table. It resolves:

| Value | Source | Explicit flag |
|-------|--------|---------------|
| chain (family: `evm`, `bitcoin`, `solana`, ...) | `firehose-parquet.block_type` file metadata | `--chain`, required only when files have no `block_type` |
| table | the table directory: the nearest ancestor of each file that is not a Hive partition (`k=v`) | `--table`, required only when a file is not inside a table directory |
| chain root | the parent of the table directory | none (`--registry-path` and `--publish-report-path` override the artifact paths) |
| network (reported only) | `firehose-parquet.chain_name` file metadata, else the chain root directory name | none |

An explicit `--chain` or `--table` that differs from what the files say is an error, so a mislabeled run cannot write rows under the wrong key. `verify` also fails when the scanned files span more than one table directory or more than one `firehose-parquet.chain_name`; verify each table directory separately.

Paths are matched by component, so `output/blocks-archive/mainnet/transactions` resolves to table `transactions`, not `blocks`. Reserved artifacts (`cursor.parquet`, `partitions.parquet`, `merkle_roots.parquet` and anything under `verify_runs/`) are never scanned as table data, wherever they sit under the verify path.

## Artifact Families

Two artifact families serve different purposes and should be operated separately.

### 1. Canonical Registry

Path:

- `<chain_root>/merkle_roots.parquet`, one registry per network, shared by that network's tables (for example `output/mainnet/merkle_roots.parquet`).
- `--registry-path` overrides it. A custom registry can be shared by several networks: rows are keyed by `network`, `chain`, `table` and `partition`, so networks of the same chain family (every EVM network is `chain = evm`) never collide.

Purpose:

- Stores the canonical root registry used to compare computed partition roots.
- Represents the long-lived verification baseline for a chain/table dataset.

Operational properties:

- Mutable over time.
- Updated only by explicit verify runs.
- Should be retained longer than per-run reports.

Schema (one row per `network`/`chain`/`table`/`partition`, all columns non-null `Utf8`):

| Column | Meaning |
|--------|---------|
| `network` | Registry key: the report's `network` (`firehose-parquet.chain_name`, else the chain root directory name); empty when unknown |
| `chain` | Registry key: chain family (`firehose-parquet.block_type`, or `--chain`) |
| `table` | Registry key: table directory name (or `--table`) |
| `partition` | Registry key: Hive partition path such as `year=2024/month=01/day=01`, or `unpartitioned` |
| `algorithm` | Hash strategy used for the root (`keccak256` or `sha256`) |
| `merkle_version` | Merkle construction used for the root (currently `merkle_v2`); see `docs/verifiability-hash-strategy.md` |
| `merkle_root` | Lowercase hex partition root |
| `updated_at` | RFC 3339 timestamp of the last write of this row |

Registries written before `merkle_version` existed lack that column. `verify` reads them as `merkle_v1` and adds the column on the next registry write.

Registries written before the `network` column existed are read with an empty `network`. A row with an empty `network` applies to whichever network looks it up, so older registries keep matching. When `verify` replaces such a row (`--update-registry`), the new row carries the network, and the old row is removed.

Each write also leaves a `merkle_roots.parquet.lock` file next to a local registry (see [Concurrency and Atomic Writes](#concurrency-and-atomic-writes)). It is not table data, and no command scans it.

### 2. Per-Run Reports

Path:

- `<chain_root>/verify_runs/<run_id>/report.json` (for example `output/mainnet/verify_runs/<run_id>/report.json`)

Purpose:

- Captures run-specific metadata and results for one verify execution.
- Provides an immutable audit trail for operational review, automation, and incident response.

Operational properties:

- Immutable after publication.
- Safe to publish with shorter retention than the canonical registry.
- Can be fan-out consumed by monitoring, reporting, or analytics systems.

## Recommended Lifecycle

### Local Run

1. Run `verify` against a local or S3-backed dataset.
2. Emit the human-readable terminal summary to stdout.
3. Optionally write `report.json` using `--report-json`.
4. Optionally publish the report to the suggested artifact path with `--publish-report`.
5. Optionally override the publish destination with `--publish-report-path`.
6. If roots are missing or intentionally updated, write `merkle_roots.parquet`.

### S3 Publication

For S3-backed operations:

1. Treat `merkle_roots.parquet` as the canonical mutable registry object.
2. Publish `report.json` under a unique `run_id` prefix, typically using `--publish-report`.
3. Do not overwrite an existing report path for the same `run_id`.
4. Keep registry lifecycle and report lifecycle policies independent.

## Root Registry Update Semantics

A failing run never changes the registry. `verify` writes `merkle_roots.parquet` only when all of these hold:

- no protocol check failed, and the scan was not cut short by fail-fast;
- no partition root differs from the registry, or `--update-registry` was given;
- there is something to write (a missing root, or a root that `--update-registry` replaces).

When a write is held back, the report's `warnings` say why (`the registry was not updated: ...`).

Per partition:

| Situation | Finding `status` | Registry | Run result |
|-----------|------------------|----------|------------|
| Root equals the registry | `match` | unchanged | pass |
| No registry row | `missing_expected` | row added, when the run writes | pass |
| Root differs (including an `algorithm` or `merkle_version` change) | `mismatch` | unchanged | **fail** (exit 1) |
| Root differs, with `--update-registry`, and the write succeeds | `updated`: `expected_root` holds the replaced root, `error` the reason for an algorithm or version change | row replaced | pass |
| Partition may still receive rows from `build` (see [Open Partitions](#open-partitions)) | `open` | never written | pass (not verified) |
| Partition only partly read when fail-fast stopped at a protocol failure | none (named in `warnings`) | never written | fail (protocol) |

`--update-registry` is the explicit decision to accept the current data as canonical. The run exits 0 once the registry is written, and the report's `updated` findings are the record of what changed. A rebuild no longer needs `--no-fail-fast`: with `--update-registry`, a differing root does not stop the run. `--no-fail-fast` still controls whether a protocol failure stops the scan.

### Open Partitions

`fireparq build` may still be writing, or may resume into, the newest partition of a table. Recording its root would make the next verify fail as soon as more rows land. `verify` reads `<chain_root>/cursor.parquet`. If the cursor's stream has not reached its stop block (a live build, or a bounded build that stopped early), these partitions are `open`:

- the newest partition, meaning the one with the highest `block_num`;
- every partition holding rows beyond the cursor block (written after the last cursor save).

Open partitions are neither compared nor recorded, and `warnings` lists them. When the cursor reached its stop block (`last_block_num + 1 >= stop_block`), nothing is open. The same applies when there is no `cursor.parquet` in the chain root, so a cursor kept elsewhere with `build --cursor` is not detected. An unpartitioned table is a single partition that keeps growing, so it is `open` for as long as a live cursor exists.

### Concurrency and Atomic Writes

Registry writes cannot lose another run's update:

- **Local.** `verify` takes an exclusive lock on `merkle_roots.parquet.lock`, re-reads the registry, applies its changes, and replaces the file atomically. It writes a unique temporary file in the same directory, fsyncs it, renames it over the registry, and fsyncs the directory. A crash leaves the old or the new registry, never a partial one.
- **S3.** `verify` writes with a conditional put: `If-Match` on the ETag it read before comparing, or `If-None-Match: *` when it created the registry. If another run wrote in between, it re-reads the registry, re-applies its changes, and retries (up to 5 retries, with backoff). If the store does not support conditional writes, `verify` writes unconditionally and adds a warning.

A change is re-applied only if the row it was compared against is unchanged, or already holds the same root. If another run changed that row to a different root in the meantime, `verify` fails with `the registry row for ... changed while verify was running ...`. Re-run it to compare against the new registry.

Recommended operator posture:

- Use missing-root fills as the normal onboarding path.
- Use root overwrites only with explicit operator intent and a preserved run report.
- Preserve the prior registry object version when object-store versioning is available.

### Migrating a Legacy (`merkle_v1`) Registry

`merkle_v1` roots were computed by fireparq v0.7.1 and earlier. They cannot be compared with `merkle_v2` roots, and `verify` does not recompute `merkle_v1` roots because that construction cannot detect a duplicated trailing row. Against a legacy registry, `verify` exits 1 and reports scanned partitions as `mismatch` with the error `merkle version mismatch: registry=merkle_v1 runtime=merkle_v2; ...` (only the first one unless `--no-fail-fast` is set).

To rebuild the registry:

1. Preserve the current registry object (bucket versioning or a copy). It is the only record of the `merkle_v1` baseline.
2. Confirm the dataset is trusted. The rebuild records the current data as canonical, and nothing compares it with the old baseline.
3. Run `fireparq verify <data-path> --update-registry --publish-report`. Every scanned partition is reported as `updated`, with the legacy root in `expected_root`, and the run exits 0 once the registry is written. Keep its report as the migration record.
4. Run `fireparq verify <data-path>` again. It should report only matches and exit zero.

Rows for partitions outside `<data-path>` keep their `merkle_version = merkle_v1` label until a run that scans those partitions rebuilds them. Deleting the registry and letting the next run recreate it from missing-root fills is an equivalent reset.

### Moving a Registry From the Old Default Location

Before v0.8.0, the default registry path was derived from `--chain` (default `evm`) and a hard-coded `mainnet`:

| Data path | Old default registry | New default registry |
|-----------|----------------------|----------------------|
| `output/mainnet/blocks` | `output/mainnet/evm/mainnet/merkle_roots.parquet` | `output/mainnet/merkle_roots.parquet` |
| `output/sepolia/blocks` | `output/sepolia/evm/mainnet/merkle_roots.parquet` | `output/sepolia/merkle_roots.parquet` |
| `s3://bucket/mainnet/blocks` | `s3://bucket/evm/mainnet/merkle_roots.parquet` | `s3://bucket/mainnet/merkle_roots.parquet` |
| `s3://bucket/sepolia/blocks` | `s3://bucket/evm/mainnet/merkle_roots.parquet` (same object) | `s3://bucket/sepolia/merkle_roots.parquet` |

On S3, every network shared one registry object with colliding keys, so each network's run overwrote the others' roots. Locally, the registry was written under a fake `evm/mainnet/` directory inside the network directory.

`verify` no longer reads the old location. When a file exists there (and no `--registry-path` is given), every run adds a warning to the terminal summary and to the report's `warnings`, naming both paths. To migrate:

1. Keep a copy of the old registry. On S3 it may hold rows from several networks mixed together, so treat it as a record, not as a baseline.
2. Run `fireparq verify <chain_root>/<table> --update-registry` for each table of each network, against trusted data. This creates `<chain_root>/merkle_roots.parquet`. Old registries from v0.7.1 and earlier hold `merkle_v1` roots, which have to be rebuilt anyway (see above).
3. Delete the old file. Locally, remove the whole `<chain_root>/evm/` directory. Other commands such as `rollup` would otherwise see `evm/` as a table directory.
4. Run `verify` again. It should report only matches and no warnings.

To keep using a registry at a custom location, pass `--registry-path` explicitly. No warning is shown then.

## Immutability and Versioning Guidance

### Reports

- Reports should be immutable.
- Prefer object-store paths keyed by `run_id`.
- Do not republish different content to the same `run_id` path.

### Registry

- Registry content is mutable, but updates should be controlled.
- Prefer bucket versioning where available.
- Treat each write as a canonical state transition, not as ephemeral output.

## Publication Formats

### Required Today

- Human text summary to stdout
- JSON report file (`report.json`)
- Canonical registry parquet (`merkle_roots.parquet`)

### Optional / Future

- Summary parquet for fleet analytics
- Materialized text summaries stored alongside `report.json`

Current recommendation:

- Keep JSON as the primary machine-readable run artifact.
- Add summary parquet only when multi-run fleet analytics require it.

## Resource Use

- **Memory does not grow with row count.** Each partition's Merkle root is built as rows stream in, keeping one 32-byte node per tree height (O(log n)), not one leaf per row. Verifying 9 million rows peaks at about 30 MiB of resident memory.
- **Protocol-only runs do not hash.** With `--checks protocol` (no `roots`), `verify` reads only the columns the protocol checks use (for example `block_num` and `block_number` for EVM `transactions`, `logs` and `calls`). For tables without protocol checks it reads only file footers, and it never encodes or hashes rows.
- **S3 reads are prefetched.** Objects are fetched up to 4 at a time, ahead of the scan and in listing order, with at most 256 MiB of object data held ahead of it. An object larger than that budget is fetched on its own. A protocol-only run on S3 still downloads each object whole: the column projection saves decoding and hashing, not transfer.

## S3 Operations Guidance

### Bucket Layout

Recommended layout (the defaults for data written by `fireparq build --output s3://<bucket>`):

- `s3://<bucket>/<chain_name>/merkle_roots.parquet`
- `s3://<bucket>/<chain_name>/verify_runs/<run_id>/report.json`

### Retention

- `verify_runs/`: shorter retention is acceptable.
- `merkle_roots.parquet`: longer retention is recommended.

### Versioning

- Enable bucket versioning when possible.
- This is especially valuable for `merkle_roots.parquet`, where overwrites reflect canonical-state changes.

### Metadata / Headers

Recommended S3 publication posture:

- Reports may use immutable cache semantics because paths are run-id scoped.
- Registry objects should avoid misleading long-lived immutable caching because the canonical object can change.

## Consumer Guidance

Consumers should:

- Read `report_schema_version` before parsing `report.json`.
- Compare roots across reports or registries only when both `algorithm` and `merkle_version` are equal.
- Treat `run_id` as the unique execution key.
- Use `registry_path` to identify the canonical registry consulted by the run, and `network` to tell networks of the same chain family apart.
- Surface `warnings`; they flag operator action such as an ignored registry at the old default location.
- Use `suggested_run_report_path` as the publication target, not as proof that upload already happened.

Consumers should not:

- Treat `merkle_roots.parquet` as a run-history log.
- Assume every report was published to object storage.
- Infer canonical state from a single report alone.

## Testable Operational Checklist

### Local

- Run `verify` locally and confirm stdout summary renders expected metadata.
- Run with `--report-json` and confirm JSON includes run metadata and schema version.
- Run with `--publish-report` and confirm the report is written to the suggested run path.

### S3

- Run `verify` against an S3 path with explicit AWS configuration.
- Confirm registry reads/writes resolve correctly.
- Publish `report.json` to the suggested run path using `--publish-report`.
- Confirm lifecycle policies for `verify_runs/` and registry are configured independently.

## Suggested Team Policy

- Keep one canonical registry per network (the default `<chain_root>/merkle_roots.parquet`).
- Publish one immutable report per run.
- Require a preserved run report for any canonical registry overwrite.
- Prefer docs-first contract changes before introducing new artifact formats.
