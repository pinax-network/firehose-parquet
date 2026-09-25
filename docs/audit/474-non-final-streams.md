# Issue #474: selectable append-only non-final streams

## Decision and implementation

The maintainer's decision in [#463](https://github.com/pinax-network/firehose-parquet/issues/463)
is retained: append NEW/UNDO envelopes, rather than deleting/replacing output on
reorg. `--final-blocks-only=false` now selects this mode directly. The default
remains true, the bare flag remains true, and explicit CLI values override
`FINAL_BLOCKS_ONLY`. `ArgAction::Set` with an optional equals-delimited value
preserves bare-flag compatibility without consuming a following subcommand.

On successful bounded non-final completion, the binary emits a warning after
its normal output/checkpoint drain. The warning states that completion does not
prove tail finality and that this stopped stream cannot receive later UNDOs.
It does not claim that every bounded historical tail is actually non-final.
Final-only and unbounded modes do not emit this bounded-tail warning; dry runs
have no saved tail and do not emit it. Failed/interrupted runs retain their
existing error/shutdown behavior.

The shared helper lives in `firehose-parquet/src/cli.rs`; the current binary
calls it from the completed-ingestion path. This is a small integration seam for
the concurrent #468 runtime rewrite. The #468 owner was consulted before the
isolated `main.rs` patch and will retain the same call in its rewritten path.
This change does not promise protected transaction integration or close #468.

## Why the proposed general dedup query is unsafe

`fork_step`, block identity, timestamp and `lib_num` are not an ordered event
log. The current schemas carry no per-delivery sequence/occurrence key. A Parquet
glob's file/row order and filenames are not a durable cross-file, cross-partition,
or cross-restart event order. The saved cursor is opaque and is not attached to
each row. The Firehose protobuf explicitly describes ordinary non-final output
as NEW plus occasional UNDO; a later FINAL event is not promised for each block.

The issue proposed keeping identities with no *later* UNDO. Without order, that
predicate is not computable in general. NEW(A), UNDO(A), NEW(A) and repeated
NEW(A), NEW(A), UNDO(A) have the same unordered rows but opposite terminal states.
Anti-joining every UNDO loses the re-added A; signed event counts are unsafe with
repeated deliveries. Retaining block height/timestamp does not resolve the tie.

The [README query](../../README.md#non-final-streams-and-reorgs) therefore returns
a deliberately narrower, correct result: distinct block identities that are both
observed positive events and present in a separate finalized-only same-chain
reference. The reference supplies canonical finality, independently of event
ordering. Its coverage and matching identifier encoding are required. The query
neither resolves the remaining reversible tail nor deduplicates child rows;
aggregations should use the finalized-only dataset directly. Generic DISTINCT
on child rows can discard legitimate duplicate entries, and an identity join
alone still multiplies repeated deliveries. No unsupported canonical-tail query
is presented as reliable.

## Validation

- Parser tests cover default true, bare true, explicit true/false, rejection of
  invalid values, a bare flag before another subcommand, environment selection,
  CLI precedence and propagation into pipeline `Config`.
- A real CLI subprocess against a local gRPC server asserts the outgoing
  `final_blocks_only=false` request and exclusive stop conversion. Its bounded
  source emits NEW(A), UNDO(A), NEW(B), UNDO(B), NEW(A), followed by the next height.
  Actual multi-file Parquet keeps both NEW(A) occurrences, both UNDOs, all six
  rows, and the saved terminal height/mode. A final-only control has two rows,
  no `fork_step`, and no bounded non-final warning. Both use fresh local roots,
  no credentials, one server call and a 15-second process timeout.
- Warning applicability is unit-tested for bounded/unbounded and both finality
  modes. The subprocess test verifies the warning is actually invoked by the
  binary, rather than testing only an unused helper.
- [474-check-query.py](474-check-query.py) extracts the exact README SQL and runs
  it using DuckDB against temporary local Parquet. Cases include recurrence,
  replay duplicates, competing identities at one height, reference-uncovered
  tail, UNDO-only identity and unknown steps. Reordering the physical rows leaves
  the supported result unchanged, and the unordered-state counterexample is
  checked explicitly. Run `python3 docs/audit/474-check-query.py`.

The focused parser/helper, real CLI and SQL checks passed. Full current-main
validation is recorded below when completed. No new public Firehose calls or
production writes are needed for this CLI/documentation correction; the protocol
fixture exercises the existing append behavior without changing mapped data.
