# Verify Report Contract

This document defines the versioned JSON report contract emitted by `firehose-parquet verify`.

## Scope

- Applies to every verify run (local and S3-backed datasets).
- Separates two artifact families:
  - **Canonical roots registry**: `merkle_roots.parquet`
  - **Per-run report artifacts**: `verify_runs/<run_id>/report.json`

## Schema Versioning

- Contract field: `report_schema_version`
- Current version: `1.0.0`
- Policy:
  - Backward-compatible additions increment **minor**.
  - Breaking shape/semantic changes increment **major**.
  - Consumers should gate parsing on `report_schema_version`.

## Required Run Metadata

Each report includes stable run-level metadata:

- `run_id`
- `started_at`
- `finished_at`
- `duration_ms`
- `tool_version`
- `chain`
- `table`
- `profile`
- `requested_checks`
- `effective_checks`
- `algorithm`
- `registry_path`

## Artifact Location Contract

Suggested report artifact path is deterministic and included as:

- `suggested_run_report_path`

Pattern:

- `/<chain>/mainnet/verify_runs/<run_id>/report.json`

For S3:

- `s3://<bucket>/<chain>/mainnet/verify_runs/<run_id>/report.json`

`merkle_roots.parquet` remains the canonical registry for computed roots and must not be treated as run-report storage.

## Publication + Retention Guidance (S3)

- Publish `report.json` under immutable `run_id` prefixes.
- Configure lifecycle retention separately from registry retention:
  - Reports: shorter retention (for operational observability).
  - `merkle_roots.parquet`: longer retention (canonical verification baseline).
- Keep report publication idempotent by run ID.
