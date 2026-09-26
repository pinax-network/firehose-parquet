# #529 maintenance engine consolidation

Baseline: `origin/main` `9372f99`, after the merged schema checks (#479/#542),
merge journal (#480/#561), dataset ownership (#591), streaming rollup (#522/#604),
bounded S3 maintenance concurrency (#523/#609), shared AWS configuration
(#527/#610) and CLI modules (#528/#612).

Prior art: Codex's unpushed commit `2ec6413` ("share schema and reader encoding
engine") was cherry-picked unchanged with its authorship. Its uncommitted
`maintenance/discovery.rs` and `rollup/engine.rs` drafts in the
`fireparq-maintenance-engine-529` worktree were adopted, completed (the rollup
engine was not yet wired or compiling) and tested. That worktree was only read.

## Re-scoped against current code

The audit's figures predate the later fixes. Current state before this change:

| Audit claim | Current `9372f99` |
|---|---|
| `writer_properties` 4x | Shared by `writer::properties::for_schema` since #519; merge and rollup still had identical 13-line metadata wrappers (2x). |
| File collectors 5x | Six native walkers: merge parquet, merge journals, rollup, truncate, verify, CLI scan/validate. |
| S3 listing 6x | Six inline `list(prefix).try_collect()` blocks, plus five inlined prefix-relative key copies. |
| "Rollup is merge with a different grouping key" | They already shared `StreamingPartWriter` (#604) and `SchemaCheck`, but each command still had its own local and S3 copies of its orchestration: two ~200-line merge partition sequences with two grouping loops, header printers and recovery loops, two rollup group loops, and four per-batch encode loops. |
| Use `Arc<dyn ObjectStore>` with `LocalFileSystem` locally | Rejected, see below. |

Merge and rollup keep separate orchestration. Their contracts differ on purpose:
merge is per directory, journaled, deterministic `part-NNNNNN` numbering, lazy
writer with first-available footer, bounded 4-object/64 MiB whole-object windows
(#523); rollup groups across directories, validates every source (schema and row
count) in a first pass, uses random run-id names with copy/in-place rules and
pinned range reads (#522). Folding them into one sequence would change naming,
crash-safety and memory semantics. They share the encoder beneath that level.

### Why local storage stays on `std::fs`

`LocalFileSystem` would change observable behavior: symlink handling, non-UTF-8
names, `read_dir` error kinds, and it cannot express `create_new` +
fsync + rename + directory fsync outputs, hard-link journal claims, directory
inode revalidation (#591) or recognition of the legacy local run lock. Instead,
each engine takes a small storage trait: S3 implements it with the existing
`Arc<dyn ObjectStore>` code, local with the existing `std::fs` code.

## What was consolidated

| Shared piece | Replaces | Kept per storage or command |
|---|---|---|
| `maintenance::compaction`: `StreamingPartWriter`, `SchemaCheck`, `writer_properties`, receipt stripping, `Encoder` | 2 metadata wrappers; 4 per-batch encode loops (merge local/S3, rollup local/S3) | Merge's lazy first-nonempty writer and first-`Some` footer; rollup's first-file schema/footer, checked row count |
| `maintenance::discovery`: `collect_local` + `LocalPolicy`, `list_objects`, `relative_key`, `read_object_bytes` | 6 native walkers; 6 S3 listings; 5 relative-key copies; 3 whole-object reads (scan, inspect, validate) | Missing-root handling, control pruning, verify's case-insensitive extension, exact journal name; filtering, sorting, HEAD fallback, clients and retries stay with callers |
| `rollup::engine` (`Backend`: `Local`, `Remote`) | 2 two-pass group loops | Local revalidation, `create_new` publication and per-file error context; S3 put with retained-sources error, same-bucket source filter, batched single-attempt deletes |
| `merge::engine` (`PartitionMerge`: `LocalMerge`, `S3PartitionMerge`) | 2 partition sequences, 2 grouping loops, 2 table-header printers, 2 recovery loops | Source sizing, footer reads (local files vs S3 windows), claim-time ownership check in its original order, local changed-source recheck, publication, deletion and crash points |
| `truncate::plan` | 2 select/report/confirm flows | Inventory, displayed location, deletion and local empty-directory cleanup |
| CLI `scan_files`, `build_scan_file_result`, `no_files_result` | 2 scan loops, 2 file-result builders, 2 empty validate results | Listing, display paths and error messages |

The shared merge sequence is exactly the pre-existing one:

```
estimate -> schema preflight -> journal claim -> [local: changed-source recheck]
-> streamed outputs -> sync -> after-outputs -> owner check -> commit
-> after-commit -> source deletes (after-first-delete) -> sync
-> journal removal -> sync
```

`S3Partition::sync` was and remains a no-op, so the shared syncs keep the S3
request sequence unchanged. Output names, journal contents, printed messages,
tracing fields and targets are unchanged. Code moved into `merge::engine` and
`rollup::engine` sets the old `firehose_parquet::merge` and
`firehose_parquet::rollup` log targets explicitly, so `RUST_LOG` filters still
match.

## Deliberately left separate

- `merge_journal::recover` and `recover_remote_journal`/`recover_guarded_for_ingestion`:
  the guarded ingestion recovery has request deadlines, uncertainty marking and a
  separate journal barrier (#468). Merging them would change crash semantics.
- Merge's bounded S3 read windows and rollup's pinned range reads (#523, #522).
- Verify's ordered prefetcher, and its atomic local vs conditional S3 registry
  and report writers.
- Rollup's and truncate's `cleanup_empty_dirs`: they differ in root removal,
  control-tree skipping, error handling and counting.
- `retry_merge_s3_operation` and other single-command policy code with no
  local/S3 twin.

## Equivalence evidence

Unit tests (all pre-existing tests pass unmodified; the merge and rollup test
modules only gained a local helper for the removed walker name):

- Discovery: every policy against frozen `origin/main` copies of all six walkers
  (append order, error kind and message, symlinks, broken links, controls,
  missing and non-directory roots, permission errors; non-UTF-8 names where the
  filesystem allows them), the raw listing against the former inline form, and
  `relative_key` against the inlined copies.
- Encoder: frozen copies of the four `origin/main` encode loops compared byte for
  byte with `Encoder` over 4 input sets x 8 settings (32 comparisons: receipt
  metadata, leading empty files, late footers, all-empty, single file,
  compression/flush-bytes/flush-rows/initial part). Codex's footer-selection test
  covers six metadata sequences under both command policies.
- Engines: recording fixtures pin the merge step order above, each crash point,
  owner check before commit, in-use claims, changed sources, empty inputs,
  dry-run/mismatch/no-op preflight and recovery admission; and the rollup hook
  order per group, copy mode, empty/corrupt preflight, mixed schemas, changed row
  counts and failures in every mutation hook.
- Cross-backend: the same sources merge and roll up into identical part names and
  bytes locally and on an in-memory object store; truncate selects identical files
  and sizes locally and in memory under eight filter sets.

End-to-end, local data only: the `origin/main` release binary (`9372f99`) and
the branch release binary ran the same 24 scenarios (33 CLI steps) on fresh
copies of the retained mainnet blocks 24000000-24000029 dataset (13 tables,
1,049,978 rows, minute partitions):

- merge: default, `--flush-rows 700 --compression snappy`, `--dry-run`, one table
  with `--flush-bytes 65536`, and kills (`FIREPARQ_TEST_MERGE_CRASH_AT`) at
  `after-outputs`, `after-commit` (then a dry run) and `after-first-delete`,
  each followed by the recovering run;
- rollup: copy mode to hour twice (replacing the earlier copy), copy mode to date
  with `--flush-bytes 65536`, in place to date, in place to hour with 64 KiB
  snappy parts, and the refused in-place run without `--delete-source`;
- truncate: `--dry-run`, no `--yes` (refused), `--yes` with two filters, a table,
  and the network root;
- verify with an explicit registry (update then deep/roots+protocol comparisons,
  including before and after a merge), validate, scan (`--json`, schema only) and
  inspect.

Every step had the same exit code and the same stdout/stderr at debug log level
(timestamps, durations and random run ids normalized). All 4,601 resulting files
(4,543 Parquet) had identical SHA-256 digests; rollup names were compared with
their run ids normalized, and the verify registry by content without
`updated_at`. DuckDB compared 325 table snapshots (25,565,136 rows): row counts
were equal and `EXCEPT ALL` returned 0 rows in both directions for every table.
The self-comparison of the baseline binary with itself was also all-equal, which
validated the normalization.

## Line counts

Measured against `origin/main` with inline `#[cfg(test)] mod tests` blocks and
test files counted separately.

| | Before | After | Change |
|---|---:|---:|---:|
| Production lines (touched files and new modules) | 8,201 | 8,451 | +250 |
| Production code lines (without blanks and comments) | 7,087 | 7,229 | +142 |
| Test lines | 4,370 | 6,076 | +1,706 |

`merge.rs` lost 510 production lines and `rollup.rs` 291, while the new shared
modules added 1,133 (including module and trait documentation). The audit's
"about 1,500 fewer lines" did not materialize: #519, #522/#604 and #523/#609
had already removed the writer-property and streaming duplication, and the
remaining duplication was control flow. Collapsing it into one sequence per
command with typed storage hooks is roughly line-neutral. The result is one
implementation of each maintenance sequence instead of two to six, so a fix to
the merge journal order, rollup barriers, truncate selection, discovery rules or
encoding now reaches local and S3 alike.

## Found while preserving behavior

`max_part_number_in_s3_objects` ignores objects without a `/` in their key, and
the S3 written-output set prefixes names with `{partition_key}/`. Merging part
files that sit directly at an S3 bucket root (empty prefix) therefore numbers the
output `part-000001.parquet` over an existing source and then deletes it with the
other sources. An in-memory reproduction on `origin/main` reported one merged
partition and left no rows. This change preserves the logic exactly; the fix is
tracked separately.

## Validation

- `cargo fmt --all --check`
- `cargo test --workspace --locked`: see the pull request for the final count
  after rebasing on current main.
- The E2E harness and its self-comparison, described above.
