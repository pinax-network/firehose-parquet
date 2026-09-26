# Validation follow-ups: partitions and validate (findings 10-15)

Part of the validation follow-ups recorded in
[`validation-misc-followups.md`](validation-misc-followups.md) (#463). Related
records: [#476](476-timestamp-validation.md) (timestamp validation),
[#485](485-partition-probe-reliability.md) (probe reliability),
[#486](486-partition-index-design.md) (verified v2 coverage) and #491
(millisecond canonical timestamps).

## 10. `validate` timestamp precision (#476, #491)

**Finding.** `fireparq validate` cast every `timestamp` column to whole seconds
before comparing consecutive blocks. Since #491 the canonical column is
`Timestamp(Millisecond, UTC)`, so a reversal inside one second (`.900` then
`.100`) was invisible.

**Fix.** `timestamp_column_as_epoch_millis` takes the unit from the column type:
`Timestamp(Second)` is scaled up, milliseconds are read as-is, and micro/nano
units are truncated to whole milliseconds (monotonic, so truncation cannot invent
a reversal). Legacy `Int64` columns are still epoch seconds. `TimestampReversal`
now exposes `timestamp_ms` / `prev_timestamp_ms` (public field rename) and
reports print `YYYY-MM-DD HH:MM:SS.mmm`.

**Test.** `test_validate_parquet_reports_sub_second_timestamp_reversal` flags
`.900` then `.100` in millisecond, microsecond and nanosecond columns and checks
that second-precision data still validates.

## 11. Live `partitions build` retries transient scan errors (#485)

**Finding.** A live build retried only transient head checks. A stalled
traversal message (5 s per-message deadline), four failed boundary probes or a
transport error during the scan ended the process.

**Fix.** `partition_index::is_transient_partition_error` classifies timeouts
(including a stalled message or elapsed bounded deadline), transport errors and
non-fatal gRPC statuses as transient. In `--live` mode a transient head-check or
scan failure keeps the last published snapshot, waits, and restarts from its
frontier. The delay is the poll interval doubled per consecutive failure, capped
at 300 s (never shorter than the poll interval), reset after a successful
iteration, and interrupted by shutdown. Fatal statuses (authentication,
permissions, invalid request, decompression limits), missing blocks and every
proof or integrity failure still stop the command. Bounded runs are unchanged
and fail on the first error. Live retries have no attempt limit, only a delay
cap, which matches a long-running live process; this is documented.

**Tests.** `live_partition_retry_delay_doubles_per_failure_up_to_a_cap`, the
classifier unit tests, and the real-CLI
`cli_live_retries_transient_scan_failures_and_keeps_the_snapshot`, which injects
a stalled scan, an `Unavailable` scan and four failed boundary probes into the
mock gRPC harness and requires the live run to keep going and publish. It fails
("exited early") with the retry disabled.

## 12. Dead missing-block toggle and legacy builder

**Finding.** `skip_missing_blocks` was hard-coded to `true` at both partition
call sites, leaving a dead branch. The public legacy `PartitionIndexBuilder` /
`build_partition_rows_from_blocks` kept pre-#486 semantics (including
`unwrap_or(0)` routing a missing time to 1970) and nothing used them.

**Fix.** The probe parameter and its dead branch are removed;
`ProbeRetryPolicy::new(bool)`, only ever called with `true`, is now
`confirm_missing()`. `Config::skip_missing_blocks` was never read and is removed
too (one-line deletions in `config.rs`, `cli/configuration.rs` and a `grpc.rs`
test); missing-block behavior is unchanged. The stale README
`--skip-missing-blocks` row (the flag was removed in v0.7.1) is gone.

**Decision: removed, not deprecated.** `cli/partitions/builder.rs` and its eight
legacy tests are deleted. Nothing in the binary, examples, integration tests or
docs used the builder, the crate is not published to crates.io, and its behavior
contradicts the v2 contract (1970 routing for missing times, unverified resume),
so a deprecation period would only prolong an unsafe API. Replacements:
`ExactTimeIndexBuilder` and `write_verified_partitions_index`.

## 13. Real-CLI regressions ported

The validator's scratch tests now live in `blocks/tests/partition_coverage.rs`,
reusing its mock gRPC harness (which gained fault injection for finding 11).
Every spawned `fireparq` clears the environment and runs in a temporary
directory, so no dotenv file or S3 setting is inherited.

- `cli_existing_index_requires_resume_or_overwrite_and_resumes_at_frontier`:
  for `hour` and `block_range` indexes, a bounded build over an existing index is
  refused with the file bytes unchanged; `--resume --start-block` past the
  frontier is refused; resume starts at the frontier without changing prior
  spans; a covered re-run is a byte-identical no-op; `--overwrite` replaces.
- `cli_live_block_range_head_is_refused_then_resumed_without_duplicates`: a live
  block-range head span is incomplete, refused by `resolve` and `shard`, and a
  later bounded resume completes it in place without duplicates.
- `cli_time_head_span_extends_to_its_real_boundary_on_resume`.
- `cli_block_range_queries_order_numerically_across_digit_boundaries`: `ls`,
  `shard`, `validate` and `resolve` on ranges 8-11.

## 14. `partitions.parquet` time columns (#491)

**Finding.** `start_time`, `end_time` and `routing_start_timestamp` were
`Timestamp(Second, UTC)`, which has no Parquet logical type, so DuckDB read them
as BIGINT: the defect #491 fixed for data tables.

**Fix.** The three columns are written with `timestamp_millis_utc_type()`
(`TIMESTAMP(MILLIS, isAdjustedToUTC=true)`). Values remain whole seconds because
the index routes by UTC epoch seconds. Readers accept second- and
millisecond-typed indexes and cast by unit; the next `--resume`, `--live`
extension or `--overwrite` rewrites an old index in the new form.
`docs/partitions-parquet-contract.md` documents the type and the compatibility
limit: older `fireparq` releases reject millisecond-typed v2 indexes, so readers
must be upgraded before a writer publishes one.

**Test.** `partitions_index_writes_millisecond_times_and_reads_second_indexes`.

## 15. v2 `partitions validate` (#486)

**Finding.** Once a v2 file loaded, `partitions validate` always returned
`valid` with zero issues, and `--allow-gaps` silently did nothing.

**Decision: implemented checks.** The verified reader already rejects, as an
error, any gap or overlap in source order, spans outside the declared finalized
bounds, misaligned block ranges, routing-inconsistent time keys and boundary
flags that contradict clipping. Validate now also reports two span-model issues
over the whole snapshot: `split_run` (source-adjacent spans share one partition
key) and `incomplete_boundary` (an internal boundary not established on both
sides; only the first span's start and last span's end may be open). Open outer
edges are counted, not reported. With a v2 index, `--allow-gaps` changes nothing
and adds an entry to the new `warnings` field (also logged). The README and the
contract document exactly what v2 validation guarantees.

**Tests.** `v2_validate_reports_split_runs_and_open_internal_boundaries`,
`v2_validate_allows_open_outer_edges_but_not_open_internal_boundaries`.

## Also fixed

`test_flush_memory_threshold_is_positive_and_propagated` (already on main)
reads the environment but was not `#[serial]`, and failed once in a full run
alongside a parallel test that sets S3 variables. It is now `#[serial]`.

## Validation

- Rebased onto `origin/main` 8462692 (#614) without conflicts.
- `cargo fmt --all --check`; `cargo test --workspace --locked`: 1,106 passed,
  0 failed, 14 ignored.
- `cargo clippy --workspace --all-targets --locked`: no new warnings in touched
  files (the tree has unrelated pre-existing warnings).
