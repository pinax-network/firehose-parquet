# Single-partition mapper writes (#477)

## Diagnosis and scope

The ingestion loop already flushes on each partition-key change and immediately
materializes normal mapper flushes. `OutputWriter` nevertheless had a second
partition splitter, per-table accumulation, compressed-size rollover,
coalescing and a hidden `pending_after_flush` queue. Its splitter expected plain
Int64 seconds while the real canonical column is UTC millisecond timestamps.
Three-partition input could be misrouted, and pending rows were omitted from
buffer statistics.

One part of the original issue's reachability claim needed correction: the
final mapper write really can trigger the writer's size rollover. The actual
Solana regression in #572 demonstrated this. The cleanup preserves that issue's
checkpoint fix instead of assuming every write uses the normal-loop helper.

The writer now validates one mapper flush, stages one batch per nonempty table,
and writes those tables immediately in sorted table-name order. It has no
splitter, deferred partition queue, batch concatenation or size rollover.
Successful tables are removed individually; failed and unattempted tables are
visible through `buffered_stats`. Another mapper flush is rejected until
retained tables have been resolved, so it cannot overwrite or silently combine
them with a later partition.

## Partition contract

All nonempty tables pass validation before any buffers, counters, metrics,
local directories or objects are changed. This prevents a valid first table
from being published before a later table's partition mismatch is discovered.
Validation compares destinations for every row, independent of row order:

- `none`: no partition membership constraint; generic Arrow batches remain
  supported.
- `block_range`: require non-null canonical `UInt64 block_num`. Both metadata
  endpoints and every row must select the same range, including an explicit
  start anchor. Zero-sized ranges and values preceding the anchor return an
  error before path formatting. Timestamps are irrelevant to this mode.
- Time partitions: require canonical `Timestamp(Millisecond, UTC)`. Convert
  milliseconds to seconds with Euclidean division, so negative subsecond
  values select the correct second. Both metadata endpoints and every non-null
  timestamp must select the metadata's destination.
- Nullable Solana times: null values use the supplied routing metadata anchor.
  The mapper keeps payload timestamps null even when the ingestion identity
  carries a synthetic anchor. Mixed null/non-null rows are valid when the real
  values select that same destination. All-null rows with no timestamp metadata
  preserve the existing flat-table fallback. Non-null timestamps without an
  anchor, a missing/wrong timestamp column, or only one metadata endpoint fail.

Metadata describes the whole mapper flush, so individual tables need not have
exactly the same extrema. Same-partition NEW/UNDO sequences may be out of order;
no monotonicity requirement, row sorting or extra enclosing-range requirement
is introduced. Consecutive repeated routing values reuse their validated
destination, avoiding path formatting for every log or call in one block.

Partition key conversion remains shared with the existing routing code.
Out-of-range timestamp fallback, missing gRPC metadata and Date32 conversion
policy remain separate in #476; this change does not claim to resolve them.

## Completion, errors and public API

The #572 completion helper still drains unconditionally before considering
`final_mapper_materialized || wrote_remaining`. It checkpoints once only on a
completed stream, propagates a drain or cursor-save error, and does not perform
an extra save for output already checkpointed in the normal loop. The final
mapper now always materializes its nonempty tables on success, making that flag
essential even when the drain is empty.

`OutputWriter::new`, `new_s3`, `write_all`, `flush_remaining`, buffer statistics
and compression-estimate access retain their signatures. The legacy constructor
`flush_bytes` argument is documented as ignored. The CLI's mapper byte/row/block/
time triggers still operate, and README/help text now describes them accurately.
The low-level generic `ParquetTableWriter` API is unchanged.

This is a deliberate behavioral change for direct Rust callers that previously
relied on writer accumulation: `write_all` publishes each validated mapper flush
immediately. Such callers must group rows before calling it. It returns false
only for an empty successful flush, rather than retaining small batches.

An I/O error can occur after a complete file was published. Retaining a batch
does **not** prove that retrying it is duplicate-free: an ambiguous publication
failure requires reconciliation before retry. Ingestion still stops on write
errors. Tests that retry use a known pre-publication directory-creation failure.
The new buffer visibility removes hidden pending data; partial multi-table
publication and crash/replay atomicity remain tracked in #468.

## Validation

Focused tests cover an invalid table ordered after a valid table with no output
or state mutation; three-partition and out-of-order row membership; metadata
endpoint mismatches; shifted numeric ranges; missing/null/wrong timestamp
types; negative milliseconds; empty flushes; and failed/unattempted tables
remaining visible until an explicit, unambiguous retry.

The previous deferred-partition completion fixtures are replaced by a sorted
partial-table failure fixture. Their assertions remain: always drain before
one checkpoint, prevent checkpointing when the remaining write fails, and
skip both writes and checkpoints for failed/shutdown exits. The actual Solana
final-mapper checkpoint regression remains enabled. A second real Solana
mapper regression verifies that null payload times use the metadata anchor
without changing the stored null timestamp.

Validation used Rust 1.93, the locked Arrow/Parquet 60 dependency graph and a
whole-process Cargo lock for the shared build directory, after integrating
merged Beacon coverage on main `c88abcca1199cb8a919d504b751dd7727c0bf047`:

- Workspace tests: **732 passed**, zero failures, three existing benchmark
  tests ignored; all doc-tests passed.
- Binary build, binary/build help and Bash/Zsh/Fish completion generation passed.
- Formatting and diff whitespace checks passed. The existing unused assignment
  warning in timestamp-backfill finalization is unchanged.

On 2026-09-25, the built binary was copied to a stable path while holding the
build lock, then ingested Ethereum `[26049575, 26049577)` through the explicit
Pinax endpoint into fresh local output with `--flush-blocks 1`. The run exited
successfully with 26 table files and one cursor. Compared against the prior
verified output using DuckDB 1.1.1, all **14 tables / 12,298 rows** had identical
SQL and physical Parquet schemas. Bidirectional `EXCEPT ALL` returned zero
differences, covering values, nulls and duplicate multiplicities. Cursor resume
columns matched after excluding the opaque cursor and write timestamp.

This bounded Ethereum check qualifies unchanged live output for that sample.
Solana null-time routing and final checkpoint behavior are qualified by the
real-mapper offline regressions; no production S3 writes were performed.
