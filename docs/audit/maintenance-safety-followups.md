# Rollup crash safety, copy ownership and value metadata (validation follow-ups)

> Superseded by [#643 L5a](643-l5a-removals.md): `rollup` was removed by #652 and `merge` by #643 L5a. This record is kept as history.

Refs #478, #479, #480, #522; part of #463. Validation of `9372f99` reproduced three
maintenance defects end to end (evidence retained outside the repository). A fourth
turned up while preserving behavior for #529. All four are fixed on top of the #529
engines, where each fix applies to local and S3 storage at once.

## Defects

1. **Rollup was not crash-safe.** In validation, an in-place `rollup --delete-source` was
   killed with `kill -9` right after its first output part, then re-run. It ended with
   29,684,810 rows instead of 29,306,640. The 378,170 extra rows were permanent because
   the sources were then deleted. Causes:
   - there was no journal;
   - local outputs used `create_new` + `write_all`, with no temporary file or fsync, before
     sources were deleted;
   - source deletions were not synced;
   - S3 range reads had a single attempt on the zero-retry mutation client (merge retries
     reads).

   The #604 second pass reads sources while it publishes; that alone is safe because
   outputs are at the target granularity and are never re-read.
2. **Copy rollup + merge + copy rollup doubled rows.** Copy outputs were recognized only
   by their `part-rollup-*` names. Merge renames them to `part-NNNNNN.parquet`, so the
   next copy rollup did not replace them: `balance_changes` had 47,392 rows instead of
   23,696.
3. **The schema check ignored value metadata.** Files with identical columns but
   `firehose-parquet.block_id_encoding` `hex` and `base58` were merged into one file
   labelled `hex`.
4. **Merging parts at an S3 bucket root lost them.** `max_part_number_in_s3_objects`
   skipped keys without `/`, so outputs were numbered from 1 and overwrote
   `part-000001.parquet`. The written-output set spelled keys `/name`, so the source
   deletes then removed that output too. An in-memory reproduction reported one merged
   partition and kept no rows.

## Fixes

**Rollup journal** (`rollup/journal.rs`, `rollup/engine.rs`). Each target partition is
journaled in `_fireparq_rollup.json` in its output directory:
- The journal is created exclusively, in state `writing`, with the run id, the source root
  (canonical local path or `s3://bucket/prefix`) and every source path relative to it,
  before any output.
- Outputs are published: locally, an fsynced temporary file hard-linked into place (never
  replacing a file); on S3, a single PUT.
- The output directory is synced. Then the journal is committed with the output names.
- Earlier copies are removed, and with `--delete-source` the sources are deleted (the
  deletions are synced locally). Then the journal is removed and the directory synced.

Before discovering sources, rollup recovers journals in its output:
- `writing` is rolled back: that run's outputs and temporary files, identified by its run
  id, are deleted. Sources were never touched.
- `committed` is rolled forward: earlier copies are removed and the remaining recorded
  sources are deleted, which requires the same source root. A missing committed output
  keeps the sources and stops with an explanation, like merge.

The ownership check (local directory guard; S3: every mutated bucket's persistent owner)
runs before the claim and before the commit. `FIREPARQ_TEST_ROLLUP_CRASH_AT` aborts at
`after-first-part`, `after-outputs`, `after-commit` or `after-first-delete`.

**Shared journal storage.** `merge_journal::PartitionFiles` now stores bounded control
records by name: exclusive create, atomic replace and bounded reads. Merge journals use the
same code with unchanged contents and messages.

**Merge and pending rollups.** Merge leaves partitions at or below a directory that holds
a rollup journal alone, lists them and exits non-zero: renaming their parts would hide the
rollup's outputs or sources from its recovery. S3 batch deletes refuse rollup journal keys,
as they refuse merge journals.

**Retried S3 reads.** Rollup's `RangeReader` now uses merge's `pinned_range`: bounded
retries of the same pinned version on request, body and timeout failures, and an immediate
failure when the object changed after listing. The transport client stays zero-retry, as
for merge, so retries remain explicit and version-pinned.

**Copy ownership.** Copy outputs carry the footer key `firehose-parquet.rollup_copy=true`.
It is a value metadata key (below), so merge never combines copies with other files, and
its first-footer policy keeps the marker. Copy cleanup removes files named `part-rollup-*`
(from earlier releases) or carrying the marker. For each other Parquet file in the target
directory, this costs one footer read (a range read on S3).

**Value metadata.** `SchemaCheck` also compares these footer keys: `chain_name`,
`block_type`, `bytes_encoding`, `block_id_encoding`, `with_votes`, `synthetic_timestamps`,
`synthetic_timestamp_policy`, `extended`, `final_blocks_only`,
`include_failed_transactions` and `rollup_copy`. A key present in one file and absent in
another counts as a difference. Descriptive keys (`version`, `endpoint`, `compression`,
first-streamable block, features) may differ. Mismatched partitions (merge) or groups
(rollup) are left untouched and reported with the key and both values, like column
mismatches. Merge reads the metadata from the footers it already fetches; rollup uses its
first-pass reader.

**S3 bucket root.** Output numbering uses every listed file name, and written keys are
built the way `object_store` lists them.

## Evidence

Unit and integration tests:
- Every rollup crash point, in place and in copy mode, locally and on an in-memory store,
  recovers to exactly the original block numbers, with all sources deleted in place and
  kept in copy mode.
- Engine fixtures pin the journaled step order, each crash barrier, failures in every
  mutation hook, and recovery outcomes: rollback, rollforward, missing outputs, foreign
  source roots, and copies from any root.
- Journal encoding rejects inconsistent phases, foreign or unsafe names, duplicates,
  unknown fields and future versions.
- Merge waits for an interrupted rollup (local and S3) and still merges unrelated
  partitions.
- Copy rollup, merge and copy rollup keep the original rows, locally and on S3.
- Value metadata: exact messages, descriptive keys ignored, every value key enforced;
  merge (local, S3) and rollup (local, S3) leave hex/base58 partitions byte-identical.
- An S3 bucket-root merge keeps all 30 rows in `part-000004.parquet`.
- A transient read failure retries the same pinned snapshot.

`cargo test --workspace --locked`: 1,159 passed, 0 failed, 14 ignored on `050f9ec` with
#529; 1,173 passed, 0 failed, 14 ignored after rebasing on `97dd244` (16 more than #529
alone).

End to end ([script](https://github.com/pinax-network/firehose-parquet/blob/v1.0.1/docs/audit/maintenance-safety-e2e.py), [report](maintenance-safety-e2e.json)),
local data only: the retained mainnet 24000000–24000029 dataset (13 tables, 1,049,978
rows), the `origin/main` `050f9ec` release binary and the fixed release binary:

| Scenario | main `050f9ec` | fixed |
|---|---|---|
| In-place `rollup --delete-source --flush-bytes 65536`, SIGKILL at the first written part, re-run | 1,051,002 rows (+1,024 permanent duplicates) | 1,049,978 rows, no journal left |
| Deterministic crash at each of the 4 steps (in place) and 3 steps (copy), re-run | not applicable | all 7: rows equal the pristine counts, `EXCEPT ALL` 0 in both directions, no journal left |
| Copy rollup with small parts, `merge` of the output, copy rollup again | `balance_changes` 47,392, total 2,098,996 | `balance_changes` 23,696, total 1,049,978 |
| Two parts, same columns, `block_id_encoding` hex vs base58, `merge` | exit 0, one file labelled `hex` | exit 1, both files untouched, mismatch reported |

Every run used an empty S3/AWS environment and a working directory outside the repository.

Release binaries now ignore `FIREPARQ_TEST_ROLLUP_CRASH_AT`: re-running the
deterministic-crash cases needs a debug build, or one built with
`CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true` (see the script's docstring).

## Compatibility and limits

- Rollup outputs have not changed except for copies, which gain the `rollup_copy` footer
  key. Directories may briefly hold `_fireparq_rollup.json`.
- Merge now refuses partitions that mix copies with other files, or that disagree on value
  metadata. Before, it combined them. Combining a copy with durable rows would let the next
  copy rollup delete those rows.
- A committed interrupted in-place rollup can only be finished by rolling up the same
  source again. The output directory keeps the journal until then, and merge leaves it
  alone.
- Local rollup sources must be UTF-8 paths below the source root, so they can be recorded
  in the journal. Other sources now fail the run before any output.
- A journal holds every source of one target partition. Its 4 MiB control-record limit is
  roughly 50,000 source paths. Larger groups are refused before any output, with advice to
  roll up to a finer target first.
- `truncate` and `scan` do not consult rollup journals, just as they do not consult
  merge journals. `verify` refuses both since the
  [verify follow-ups](validation-verify-followups.md), and says to run the
  interrupted rollup again.
- An interrupted S3 rollup keeps the bucket owner, like merge. After a remote error,
  release still requires provider-confirmed quiescence.
