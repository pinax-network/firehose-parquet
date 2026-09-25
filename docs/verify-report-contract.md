# Verify Report Contract

This document defines the versioned JSON report contract emitted by `fireparq verify`.

## Scope

- Applies to every verify run (local and S3-backed datasets).
- Separates two artifact families:
  - **Canonical roots registry**: `merkle_roots.parquet`
  - **Per-run report artifacts**: `verify_runs/<run_id>/report.json`

## Schema Versioning

- Contract field: `report_schema_version`
- Current version: `2.0.0`
- Policy:
  - Backward-compatible additions increment **minor**.
  - Breaking shape/semantic changes increment **major**.
  - Consumers should gate parsing on `report_schema_version`.

### Version History

| Version | Change |
|---------|--------|
| `2.0.0` | Roots use the `merkle_v2` construction, so `computed_root` changes for identical data. Added `merkle_version`, `network`, `warnings`, the `updated` and `open` finding statuses, and the `summary.updated` / `summary.open_partitions` counters. `chain` and `table` are inferred from the dataset, and the registry and report paths moved under the network's chain root. |
| `1.0.0` | Initial contract. Roots used the legacy `merkle_v1` construction. |

## Required Run Metadata

Each report includes stable run-level metadata:

- `run_id`
- `started_at`
- `finished_at`
- `duration_ms`
- `tool_version`
- `chain`: chain family, from `firehose-parquet.block_type` file metadata or `--chain`
- `table`: table directory name, or `--table`
- `network`: `firehose-parquet.chain_name` file metadata, else the chain root directory name; `null` when neither exists
- `profile`
- `requested_checks`
- `effective_checks`
- `algorithm`
- `merkle_version`
- `registry_path`
- `warnings`: operator-facing messages (for example an ignored registry at the old default location); empty when there are none

## Root Comparability

A root is defined by the data plus two fields:

- `algorithm`: hash strategy (`keccak256` or `sha256`)
- `merkle_version`: Merkle construction (currently `merkle_v2`), specified in `docs/verifiability-hash-strategy.md`

Roots are comparable only when both fields are equal. Future changes to row encoding or tree construction bump `merkle_version`, so consumers that compare roots must check it, not only `report_schema_version`.

When a registry row was written with a different `algorithm` or `merkle_version`, the finding has `status: "mismatch"` (or `"updated"` when `--update-registry` replaced it), `expected_root` set to the registry value, and an `error` that starts with one or both of these reasons, joined by `; `:

- `algorithm mismatch: registry=<registry algorithm> runtime=<algorithm>`
- `merkle version mismatch: registry=<registry merkle_version> runtime=<merkle_version>; ...`

Registries without a `merkle_version` column predate it and are read as `merkle_v1`.

## Findings and Exit Code

`findings[].status` is one of:

| Status | Meaning | Counter |
|--------|---------|---------|
| `match` | Computed root equals the registry root | `summary.matches` |
| `missing_expected` | No registry row; added when the run writes the registry (`summary.wrote_registry`) | `summary.missing_expected` |
| `mismatch` | Registry root differs and was not replaced | `summary.mismatches` |
| `updated` | Registry root differed and `--update-registry` replaced it; `expected_root` is the replaced root | `summary.updated` |
| `open` | Partition may still receive rows from `fireparq build`; not compared or recorded, reason in `error` | `summary.open_partitions` |

The run passes (`fireparq verify` exits 0) when `summary.mismatches` and `summary.protocol_failed` are both 0. `updated` and `open` findings do not fail a run. The registry is written only by a passing run; `warnings` explain a held-back write. See "Root Registry Update Semantics" in `docs/verifiability-artifact-runbook.md`.

## Artifact Location Contract

Suggested report artifact path is deterministic and included as:

- `suggested_run_report_path`

Pattern, where `<chain_root>` is the network directory that holds the table directories (`<output>/<chain_name>` for `fireparq build` output):

- `<chain_root>/verify_runs/<run_id>/report.json`

For S3:

- `s3://<bucket>/<chain_name>/verify_runs/<run_id>/report.json` (with any prefix before `<chain_name>` kept)

The default registry is `<chain_root>/merkle_roots.parquet` (`registry_path`). See the dataset resolution rules in `docs/verifiability-artifact-runbook.md`.

`merkle_roots.parquet` remains the canonical registry for computed roots and must not be treated as run-report storage.

## Publication + Retention Guidance (S3)

- Publish `report.json` under immutable `run_id` prefixes.
- Configure lifecycle retention separately from registry retention:
  - Reports: shorter retention (for operational observability).
  - `merkle_roots.parquet`: longer retention (canonical verification baseline).
- Keep report publication idempotent by run ID.
