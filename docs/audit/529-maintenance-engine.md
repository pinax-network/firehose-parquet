# #529 maintenance engine consolidation

Baseline main `81f5b79`, including the merged #528 CLI split. The original audit's four
writer-property implementations already share writer/properties.rs after #519;
merge and rollup still duplicate the identical transaction-metadata stripping
wrapper. #522 already shares StreamingPartWriter, and #523 intentionally gives
merge bounded whole-object windows while rollup retains pinned range reads.
Those are accepted policies to preserve, not accidental duplication to erase.

## Boundaries

1. Introduce a private maintenance/compaction module for the shared schema check,
   exact metadata policy and encoder. Move StreamingPartWriter without behavior
   changes. Add one generic reader-to-encoder path used by local/S3 merge and
   local/S3 rollup. Merge retains lazy first-nonempty writer initialization and
   its current first-Some file-metadata behavior; rollup retains its all-input
   row-validating preflight and first-file schema/metadata. The engine takes an
   explicit initial schema/metadata mode, reader batch policy and publication
   callback, returns checked row/file/byte stats, and never owns locks/journals.
   Keep the different metadata initialization semantics explicit, not implicit
   Option fallback that changes empty-file handling.

2. Introduce a shared ObjectStore-backed *read location* and discovery layer,
   using Arc<dyn ObjectStore> for S3 and LocalFileSystem for local file access
   only where it preserves the existing semantics. Native std::fs traversal
   remains an explicit local discovery policy if ObjectStore listing skips
   symlinks, changes non-UTF8 paths, hidden paths or errors. Do not silently
   convert every local walk into LocalFileSystem::list. Deduplicate traversal
   with explicit policy knobs only for existing differences: missing-root
   handling, case-sensitive extension vs verify's ASCII-insensitive local
   extension, control-directory pruning, reserved-artifact filtering, exact
   object vs prefix handling, ordering, retained path/display labels. Existing
   S3 credentials, bucket routing and read/mutation retry policy stay at callers.
   Read mode explicitly selects local file, S3 pinned page ranges, or S3 merge's
   max4/64MiB complete-window reservation; verify's existing prefetch stays
   bounded and ordered. A shared get/list helper must not weaken those contracts.

3. Share the compaction orchestration after the encoder boundary is proven:
   backend adapters supply source inventory/preflight readers/publication and
   mutation hooks, while one operation engine drives the chosen policy. Merge
   and rollup grouping remains separate and observable behavior stays exact.
   Merge's durable journal/publication/sync/deletion barriers must remain in
   the same order. Local publication keeps create_new/temp+fsync+rename and
   directory inode revalidation, old local run-lock compatibility and source
   existence recheck after journal claim. S3 keeps owner checks, exact conditional
   writes, original read versions, first-error stop/drain max10 deletes, no
   mutation retry or delete after failed output/journal commit. Do not replace
   these with generic LocalFileSystem::put or a broad bulk delete interface.
   Rollup remains its existing two-pass group writer with caller's copy cleanup,
   random names, in-place guard, same-storage restriction and per-group source
   deletion. Truncate and verify share appropriate read/discovery pieces, not a
   generic mutation wrapper that changes confirmation, recovery or registry CAS.

## Qualification

First freeze fixtures/outcomes from the current production methods, including:
- local/S3 merge and rollup rows, complete Arrow types/nullability/metadata, part
  counts and row boundaries, metadata absent/empty/present, leading empty files,
  zero-row sets, missing paths, mixed schemas, corrupt later inputs;
- scan/validate/verify/truncate selection under explicit file/root/prefix,
  extension case, reserved/control trees, sibling textual prefixes, symlinks and
  representative display paths (including spaces/percent characters);
- both merge journals at after-outputs, after-commit and after-first-delete, all
  current owner/path/capability and read/delete concurrency faults;
- schema mismatch status/messages, no-op estimate, dry-run stats and no writes,
  all rollup grouping/copy/in-place modes, existing verify hashes and reports;
- no large-object memory regression; retain #522/#523 reservation tests and run
  representative guarded baseline/candidate equality and memory measurements.

Do not claim #529 complete merely for moving helpers. The final review must show
one shared compaction path, shared storage/discovery rules with explicitly
preserved differences, actual duplication removed, full current-main validation
and documented exceptions. The old approximate1,500-line estimate is not a goal
for code golf; report actual net production reduction and prior deduplication.

## Stage 1: shared reader/encoder

The private `maintenance::compaction` module now owns the previously shared
StreamingPartWriter plus schema comparison and transaction-metadata stripping.
One Encoder drives decoded input batches for local/S3 merge and rollup. The moved
helper bodies are unchanged; independent review compared them to `81f5b79`.
Storage ownership, discovery, journals, callbacks and deletion order remain at
their previous call sites in this checkpoint.

Merge accepts the first Some footer only until the first nonempty batch starts
its writer. A later Some cannot supply metadata retroactively. Rollup freezes
the first input's schema/metadata even if that input is empty. SchemaCheck still
compares ordered field names, types and nullability while intentionally ignoring
top-level schema/field metadata. Full output metadata is separately qualified.

The new six-sequence metadata regression covers absent, empty and populated
footers, leading empty inputs, nonempty inputs without metadata and all-empty
groups under both command policies. Three existing writer boundary/memory tests
moved unchanged. All targeted compaction, merge and rollup tests passed. The first
compile exposed a missing test-only DataType import, fixed before the clean run.
Stage 1 alone does not complete #529; shared discovery and orchestration follow.

The baseline executable was preserved before edits (SHA-256
`6fb841b4f0331348fcaa5863d836ac9d7e65e63c4dfbd3ecda861b1eee6a498c`).
Builds and later measurements share the repository qualification process lock.
