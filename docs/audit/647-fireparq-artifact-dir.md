# Issue #647: fireparq's artifacts under `_fireparq/`

Closes #647 (PR [#650](https://github.com/pinax-network/firehose-parquet/pull/650)); part of #463.

## Problem

`build`, `partitions build` and `verify` wrote `cursor.parquet`,
`partitions.parquet`, `merkle_roots.parquet` and `verify_runs/` beside the table
directories of a dataset root. A reader that globs the dataset
(`s3://ethereum-mainnet/**/*.parquet`, or a Spark or Trino location at the root)
picked up files with other schemas. With one bucket per network at its root
(#642), that root is what customers see. Hadoop/Hive-convention engines (Spark,
Trino, Hive, Athena, Delta) already skip paths starting with `_` or `.`; the root
files did not follow it.

## Layout

| Artifact | Before | Now | Written by |
|---|---|---|---|
| Cursor mirror (default `--cursor`) | `<root>/cursor.parquet` | `<root>/_fireparq/cursor.parquet` | `build` |
| Partition index | `<root>/partitions.parquet` | `<root>/_fireparq/partitions.parquet` | `partitions build` |
| Merkle roots registry (default `--registry-path`) | `<root>/merkle_roots.parquet` | `<root>/_fireparq/merkle_roots.parquet` | `verify` |
| Verify reports | `<root>/verify_runs/<run_id>/report.json` | `<root>/_fireparq/verify_runs/<run_id>/report.json` | `verify --publish-report` |
| Output authority | `<root>/.fireparq-ingest/` | unchanged | `build` |
| S3 owner record and probes | `.fireparq-owner-v1.json`, `.fireparq-owner-probes-v1/` at the bucket root | unchanged | every mutating S3 command |
| Merge and rollup journals | `_fireparq_merge.json`, `_fireparq_rollup.json` in partitions | unchanged | `merge`, `rollup` |

`<root>` is `<output>/<chain_name>`, or `<output>` itself with
`--without-chain-dir`, locally and on S3 (including a bucket root). The root now
holds only table directories, `_fireparq/` and dot-prefixed control state.

## One source of truth

`firehose-parquet/src/artifacts.rs` defines `ARTIFACTS_DIR` (`_fireparq`),
`DatasetArtifact` (`CursorMirror`, `PartitionsIndex`, `MerkleRoots`,
`VerifyRuns`) with `relative_path`, `path_in` / `legacy_path_in` for local paths
and `s3://` URIs, `key_in` / `legacy_key_in` for S3 prefixes (empty at a bucket
root), and `DEFAULT_CURSOR_MIRROR`, the literal clap uses as `--cursor`'s
`default_value` (a test pins it to `DatasetArtifact::CursorMirror`). The
partitions helpers (`partitions_index_path_in`, `build_partitions_cursor_path`),
ingestion eligibility, the `partitions build` start-block inference and the
verify registry, report and legacy-cursor paths all resolve through it. No path
literal remains outside tests and help text.

## Reservation

`is_reserved_artifact_path` (relative to the walked directory or prefix) now
reserves any path with a `_fireparq` component, in addition to the legacy file
names `cursor.parquet`, `partitions.parquet` and `merkle_roots.parquet`,
anything under `verify_runs/`, and control paths. Callers:

- `merge` and `rollup` (local and S3), `rollup` and `merge` journal validation,
  and `verify` (whole dataset and per table, local and S3) skip reserved paths.
- `truncate` still lists unfiltered artifacts as "not table data" (an unfiltered
  truncate of a root deletes everything, as before), but a partition filter now
  never matches a reserved path, including a partition-shaped path under
  `_fireparq/` or `verify_runs/`.
- `scan` and `validate` directory walks now skip reserved paths too
  (`read_walk_skips`), unless the walked directory is itself inside
  `_fireparq/`; single files are always read, so `inspect` / `scan` of
  `_fireparq/partitions.parquet` work.
- Journal discovery (`LocalPolicy::named` / `named_any`, the S3 merge, rollup and
  verify journal scans, and protected merge-journal recovery) prunes
  `_fireparq/`: no partition, so no journal, lives there.
- `recovery status` / `recover` / `release` refuse a path inside `_fireparq/`.
- Protected-root marker discovery still walks everything, so a nested root is
  never missed.

## No silent shadowing

Nothing is migrated. Each check runs before any row or index is read and before
anything is written, and names the legacy path, the new path and the move:

- `verify` with a `roots` check and the default registry refuses while
  `<root>/merkle_roots.parquet` exists (through the verified source's store on
  S3). Protocol-only runs and an explicit `--registry-path` are unaffected.
- `partitions build` refuses every mode (bounded, `--resume`, `--live`,
  `--overwrite`) while `<root>/partitions.parquet` exists. The check runs inside
  `prepare_partitions_index_write` with the held ownership's store; on refusal
  the ownership is finished, and since only reads were sent the S3 owner is
  released.
- A new `build` refuses to initialize beside a legacy root index (eligibility
  accepts only `_fireparq/partitions.parquet`), with the same message.
- Old `verify_runs/` reports are immutable per-run files, so a legacy directory
  is not refused; new reports go to `_fireparq/verify_runs/`.

## Mirror binding and resume

The descriptor binds the mirror as an absolute local path or an S3 bucket and
key, resolved exactly as before: a relative `--cursor` joins the dataset root, so
the default binds `<root>/_fireparq/cursor.parquet`. A fresh dataset created with
the new default resumes with it without drift (session and real-binary tests).
A dataset created before this change bound `<root>/cursor.parquet`; rerunning it
with the new default fails before Blocks with `PRE_V1_DEFAULT_MIRROR`, which
names `--cursor cursor.parquet` and echoes no path. That flag resumes it, and
the mirror is never moved.

Nothing assumed the mirror was a direct child of the root: ownership scopes
already use the mirror's parent directory (reduced to the output root), S3 key
checks require the key inside a declared scope, `ProtectedMirror::local_path`
canonicalizes the nearest existing ancestor, and artifact-destination guards
normalize the parent. The artifact-destination guard now takes its cursor name
from `CURSOR_PARQUET_FILENAME`.

## Directory creation

Local writes create `_fireparq/` on demand; S3 needs nothing:

- The protected mirror creates missing mirror directories with mode `0700`,
  except `_fireparq/` itself, which gets default permissions because it is
  shared with the index, registry and reports; the mirror file stays `0600`.
  Publication is unchanged: a unique same-directory temporary, `fsync`, rename,
  directory sync.
- The index writer, the local registry commit and local report publication create
  their parent with `create_dir_all_durable` (the new directory's entry is
  synced) and keep their same-directory temporary-and-rename writes. The local
  registry's `merkle_roots.parquet.lock` now sits in `_fireparq/`.

## Validation

- Unit: `artifacts` path table, reservation and legacy-message tests; binding
  and session scopes for the default mirror (local, S3 prefix, bucket root);
  `default_mirror_is_bound_in_the_artifact_directory_and_resumes_without_drift`
  (also asserts the root holds only `.fireparq-ingest`, `_fireparq` and the
  tables) and `pre_v1_default_mirror_is_named_when_resumed_with_the_new_default`;
  the mirror's `_fireparq/` creation and permissions; eligibility for
  `_fireparq/partitions.parquet` and the legacy refusal locally and on an
  in-memory bucket root; `a_legacy_root_partition_index_is_refused` (local, S3
  prefix and bucket root, in-memory store); verify's local
  `a_legacy_root_registry_is_refused_instead_of_shadowed` (chain directory and
  output root) and `remote_legacy_root_registry_is_refused_instead_of_shadowed`
  (in-memory S3, prefix and bucket root); `_fireparq/` entries added to the
  reserved-artifact tests of merge, rollup, truncate, verify, scan, validate,
  journal discovery and recovery.
- Real binary (`blocks/tests`): every default-mirror assertion moved to
  `_fireparq/cursor.parquet`;
  `a_mirror_bound_at_the_pre_v1_default_resumes_only_with_that_cursor`;
  `cli_legacy_root_index_is_refused_until_moved_into_fireparq` (every mode,
  default layout and `--without-chain-dir`); the `--without-chain-dir` and
  default-layout tests assert the root layout, that a per-table glob reads only
  table parts, and that a `_`/`.`-skipping dataset walk equals the union of the
  table globs, and run DuckDB (`read_parquet('<root>/blocks/**/*.parquet')`
  returns every block row and no artifact, while a dataset-wide `glob()` would
  match `_fireparq/`).
- `cargo fmt --all` and `cargo test --workspace --locked` pass (see the PR for
  counts); CI runs the DuckDB check with `FIREPARQ_REQUIRE_DUCKDB`.

## Limits

- DuckDB and other engines that do not follow the hidden-path convention still
  see `_fireparq/` under a dataset-wide glob; read per table.
- A pre-v1.0.0 dataset keeps its mirror at `<root>/cursor.parquet` for its
  lifetime (the binding is part of its identity), so its root still holds that
  one legacy file.
