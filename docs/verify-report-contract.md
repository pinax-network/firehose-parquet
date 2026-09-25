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
| `2.0.0` | Roots use the `merkle_v2` construction, so `computed_root` changes for identical data. Added `merkle_version`. |
| `1.0.0` | Initial contract. Roots used the legacy `merkle_v1` construction. |

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
- `merkle_version`
- `registry_path`

## Root Comparability

A root is defined by the data plus two fields:

- `algorithm`: hash strategy (`keccak256` or `sha256`)
- `merkle_version`: Merkle construction (currently `merkle_v2`), specified in `docs/verifiability-hash-strategy.md`

Roots are comparable only when both fields are equal. Future changes to row encoding or tree construction bump `merkle_version`, so consumers that compare roots must check it, not only `report_schema_version`.

When a registry row was written with a different `algorithm` or `merkle_version`, the finding has `status: "mismatch"`, `expected_root` set to the registry value, and an `error` that starts with one or both of these reasons, joined by `; `:

- `algorithm mismatch: registry=<registry algorithm> runtime=<algorithm>`
- `merkle version mismatch: registry=<registry merkle_version> runtime=<merkle_version>; ...`

Registries without a `merkle_version` column predate it and are read as `merkle_v1`.

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
