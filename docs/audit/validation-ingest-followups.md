# Validation follow-ups for protected ingestion

A validation pass over the #468 protected ingestion path (merged in #600 and
extended through #613) found gaps between the documented and actual behavior,
tests that only exercised dead code, and stale guidance. This record lists each
finding, the fix and the tests that now cover it. Refs #464 #465 #466 #468 #469
#470 #472 #572 #578; part of #463.

Since #600, a real `build` commits only through `IngestionSession` →
`TransactionController` → `ProtectedMirror`. Every test added here that concerns
what `build` commits drives the built `fireparq` binary against the cursor-aware
mock Firehose in `blocks/tests/ingestion_transactions.rs`.

## 1. `--cursor none` was documented but rejected

**Finding.** README and the #468 release note said `--cursor none` disables the
mirror, but `build_config` required a `.parquet` path and always set
`cursor_path`, so `MirrorBinding::Disabled` was unreachable from the CLI.

**Fix.** `none` (any case, surrounding whitespace ignored, also `CURSOR=none`)
now yields `cursor_path = None`, which resolves to `MirrorBinding::Disabled`.
Combining it with `--cursor-template` is rejected; `none.parquet` stays an
ordinary path. Without a mirror, authority alone selects the resume cursor,
completed stop and routing anchors, exactly as it already did with a mirror, so
resume, extension, same-bound no-ops and recovery are unchanged. The mirror
binding is part of the immutable stream identity: a later run without
`--cursor none`, or `--cursor none` against a dataset created with a mirror, is
refused before Blocks with a specific message. Without a mirror, `verify` cannot
classify open partitions and `partitions build` cannot infer `--start-block`
from `<chain>/cursor.parquet`; the README documents both.

**Tests.** `cli::tests::test_cursor_none_disables_the_mirror_case_insensitively`,
`cli::tests::test_cursor_none_rejects_contradictory_template_and_near_misses`;
CLI `cursor_none_keeps_mandatory_authority_without_a_mirror_and_binds_that_choice`
(creation without a mirror file, no-op repeat, refused mirror addition before
Blocks, authority-only extension) and `cursor_none_cannot_drop_an_existing_bound_mirror`.

## 2. Stale `--cursor-override` advice and a silently ignored flag

**Finding.** Protected output refuses to rewind, but the gRPC
InvalidArgument/FailedPrecondition hint, the EVM failed-transaction warning,
the dry-run cursor load and mismatch errors, and three release-note migration
steps still recommended `--cursor-override`. At a new root the flag was accepted
and silently ignored while its help said protected runs refuse it.

**Fix.** The hints now point to a new empty output root (the gRPC hint also to
`fireparq recovery status`), describe the override as dry-run only, and the
release notes (#535 endpoint move, #494 failed-transaction migration, #465
unreadable cursor) describe the protected behavior. A non-dry-run `build` now
rejects `--cursor-override` before any endpoint or storage access, even at a new
root, and `load_authoritative_resume` refuses it whether or not authority
exists. The help text says so. Read-only dry runs keep the flag.

**Tests.** `grpc::tests::test_status_relayed_as_unknown_uses_the_wrapped_code`
(new wording, no override advice); `ingest::session::tests::cursor_override_is_refused_even_before_authority_exists`;
CLI `cursor_override_is_refused_at_a_new_root_but_still_serves_dry_runs` (no
Blocks request, no output root; dry run still works), updated
`legacy_data_and_cursors_cannot_initialize_authority_even_with_override` and
`protected_origin_mode_and_override_changes_are_refused_before_blocks`;
binary `test_load_existing_cursor_fails_on_corrupt_cursor_unless_overridden`.

## 3. False coverage for #464, #469 and #572

**Finding.** Their regressions targeted `write_mapper_flush`,
`flush_writer_on_exit`, a `#[cfg(test)]` `OutputWriter` path and the legacy
`CursorLocation::save*` / `retry_cursor_save` APIs, none of which `build` uses.

**Fix.** New real-path CLI tests:

| Test | Covers |
|---|---|
| `storage_failure_during_flush_keeps_authority_and_rerun_recovers_rows_once` | #464. Run 1 commits blocks and transactions for block 100. The `transactions` table directory is made read-only; run 2's flush publishes the `blocks` part, then fails staging `transactions`. Exit ≠ 0 with the staging error; authority and mirror bytes unchanged; a Writing journal owns the published `blocks` part; no staging temp remains. Run 3 rolls back and replays: blocks and transactions each hold 100 and 101 exactly once, the checkpoint reaches 101/`completed_stop = 102`, and the replay republishes the identical deterministic part name and bytes. |
| `persistent_mirror_save_failure_exits_nonzero_and_rerun_repairs_the_mirror` | #469. An external `--cursor` directory is made read-only. Exit ≠ 0 with `protected mirror persistence failed after three local attempts`; the all-table commit and authority advanced (Committed journal retained), no mirror exists. The rerun rolls the journal forward, repairs the mirror from authority, proves completion and rewrites no data part. |
| `stream_end_mapper_flush_commits_the_final_checkpoint` | #572. Five blocks with `--flush-blocks 2` leave block 104 for the stream-end drain (`trigger="stream_end"`). Its part carries ordinals 5..5; authority and mirror reach 104 with `completed_stop = 105`; no journal remains. |
| `blocks_served_below_start_are_skipped_and_never_written` | #466. The mock serves 100–103 for `--start-block 102` (LIB+1 behavior) on a real run. `blocks_skipped_below_start=2`; no table holds rows below 102; authority counts all four envelopes (ordinal 4). |
| `solana_null_block_time_routes_by_source_metadata_on_the_real_path` | Replaces the `OutputWriter` Solana unit test: a payload without `block_time` keeps a null row timestamp and is routed to `day=14` by its source metadata. |

The permission-based tests skip themselves (with a message) only when the
process ignores directory permissions, such as when running as root; CI and
local runs execute them.

The dead helpers and their eleven binary unit tests were deleted. For the legacy
library APIs, the option chosen is **removal, not deprecation**: nothing in the
workspace called `CursorLocation::save`, `save_with_retry`,
`save_with_retry_blocking`, `retry_cursor_save` or the private S3 save except
their own tests, and the protected equivalents (bounded local retries,
single-attempt S3 CAS, metrics, shutdown interruption) are covered by the
`ingest::mirror` tests. Their four retry tests and the legacy S3 cursor
mutation test were removed; the loopback mutation-builder test still covers
zero transport retries for PUT/DELETE. The gRPC stop-on-handler-error test now
uses a plain failing handler. `CursorLocation::resolve`/`load` remain for dry
runs and partition start inference, and `save_cursor_parquet` remains an
unprotected fixture helper. `OutputWriter` itself remains as the unprotected
single-file library writer (see finding 8).

## 4. Dry-run and real bounded completion differed on sparse chains

**Finding.** A dry run accepted an exhausted sparse tail on Solana, NEAR and
Beacon with a warning, while the real build requires the accepted boundary to
reach `stop - 1`. The #466 release note still described the old allowance.

**Fix.** Made consistent: dry runs now apply the protected rule on every chain
and say what the real build would do. The unused gap allowance and its field
were removed. The #466 release note and README now agree with the #468 note.

**Tests.** `test_bounded_dry_run_on_sparse_chain_matches_protected_completion`;
CLI `dry_run_refuses_the_same_unproven_sparse_tail_as_a_real_build` next to the
existing real-run `eof_without_boundary_retains_prefix_but_cannot_claim_completed_range`.

## 5. Independent bucket derivation for ownership

**Finding.** The binary's `ingestion_mutation_scopes` derived the cursor bucket
from the raw `--cursor` string, independently of `CursorLocation` and the
`MirrorBinding` recorded in authority, with no test.

**Fix.** Reuse: `firehose_parquet::ingest::ingestion_mutation_scopes(config)`
maps the exact `resolve_mirror_binding` result (after template expansion) to
ownership scopes; the binary's copy was deleted. A divergence previously would
have failed closed in `ProtectedMirror::new`; now it cannot arise.

**Tests.** `ingest::session::tests::mutation_scopes_follow_the_recorded_mirror_binding`
(local relative, template, absolute external, local output with S3 cursor, S3
output relative/template/bucket-root, independent cursor bucket with `S3_BUCKET`
set, `--cursor none`, and refused paths); CLI
`external_cursor_template_is_owned_and_bound_as_one_mirror`.

## 6. S3 failures before the PUT were not counted

**Finding.** Only failures after the conditional PUT started incremented
`cursor_save_failures_total` / `errors_total{kind="cursor_save"}`.

**Fix.** `ProtectedMirror::reconcile` counts once per failed attempt at any
stage (local attempts, S3 read/decode/owner checks, refused retry after an
ambiguous upload, ambiguous or cancelled PUT), without double counting.
Local failures before any write are counted too, for symmetric alerting.

**Tests.** `remote_failures_before_publication_count_as_failed_saves` (failed
read, corrupt existing mirror, uncertain owner: each counts 1, sends no PUT and
does not mark a new uncertain mutation), `local_failure_before_writing_counts_as_one_failed_save`,
and the updated `ambiguous_remote_writes_or_cancel_never_retry_or_release`
(the ambiguous PUT counts once, the refused retry counts a second failure).

## 7. Staging temporaries after ordinary errors

**Finding.** `.fireparq-txn-*.tmp` files remained after ordinary errors until
the next build or `recovery recover`; docs only mentioned abrupt termination.

**Fix.** Cleaned up on error paths: after Writing is persisted, any error in the
controller removes the exact staging names of its journal plan (best effort,
logged, never masking the original error) and keeps the journal for recovery.
Recovery never needs a staged temporary. Foreign files at a planned name are
still refused before Writing and left untouched. README and release notes
document the remaining cases (cleanup failure, killed process).

**Tests.** `ingest::controller::tests::ordinary_errors_remove_owned_staging_names_and_keep_the_journal`
(failures after staging parts 0 and 1, before and after their receipts), and the
CLI storage-failure test above asserts no staging name remains.

## 8. Ignored `_flush_bytes` constructor argument

Removed from `OutputWriter::new` and `OutputWriter::new_s3` (a library signature
change noted in the release notes); `OutputWriter` is documented as the
unprotected single-file writer.

## 9. Documentation

- Release notes: #578 no longer claims final names are unchanged and replay may
  duplicate parts; #465's atomic save names the protected `.fireparq-mirror-*.tmp`
  temporary; #478 rollup naming no longer equates rollup names with ingestion
  parts; #464 describes the protected recovery; new #469 (durable mirror saves
  are fatal) and #470 (exact S3 cursor buckets) entries; an API note for the
  removed legacy APIs.
- `controller.rs` header and a stale `#![allow(dead_code)]` in `mirror.rs`
  describe/assume the real path.
- `docs/repo-navigation.md` names where naming, flush windows, bindings and
  real-path tests live.
- `docs/audit/572-final-completion-checkpoint.md` and
  `docs/audit/469-durable-cursor-saves.md` gain current-path sections.

## Validation

- `cargo fmt --all --check` and `cargo test --workspace --locked`: see the PR
  for the final counts on the rebased head.
- No live provider request was needed: every changed behavior is exercised by
  the real binary against the mock Firehose, and no provider-specific behavior
  changed. No S3 writes were performed.
