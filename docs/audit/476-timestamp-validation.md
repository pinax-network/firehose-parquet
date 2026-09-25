# Timestamp and streamed identity validation (#476)

## Diagnosis

Malformed seconds previously selected the Unix epoch through partition-path
fallbacks. Canonical Date32 conversion could panic, millisecond conversion
silently clamped invalid nanos and saturated overflow, and missing streamed
metadata became a default block-zero identity. A fallible progress formatter
could also terminate ingestion on a logging boundary.

## Implementation and contract

`traits::checked_timestamp` validates the calendar range supported by the
workspace's `time` configuration: years -9999 through 9999 inclusive (Unix
seconds -377705116800 through 253402300799). Canonical milliseconds, Date32 days
and time partition paths use this same range. Nanoseconds must be in
`0..1_000_000_000`; conversions return an error rather than clamping, saturating,
substituting epoch or panicking. Valid negative seconds retain Euclidean day
and second boundaries. For example, seconds -1 with nanos 500000000 becomes
-500 milliseconds and Date32 -1.

Canonical identity preparation now returns `Result`, before any Arrow builder
append. Every chain mapper propagates this error. Solana validates a present
payload `block_time` before writing any table, while an absent payload time
continues to write null timestamp/date columns. Antelope converts all action
`block_time` values for materialized transactions before changing any table;
existing filtered/unfiltered and failed-transaction selection remains intact.

Both unary metadata fetches and streamed metadata use the checked identity
constructor. Streamed responses without a metadata object fail before the
handler runs, so no default block number or resume cursor is fabricated.
Absent `time` inside otherwise present metadata keeps the existing zero/unknown
representation for genesis bootstrap and nullable-chain routing. Unary missing
metadata keeps its explicit `Option::None` return for the probe caller to handle.

Time partition keys are fallible. Local directories and S3 keys share one path
formatter, which rejects invalid minimum or maximum metadata timestamps for
nonempty writes before
part counters, directories, temporary files, final files or objects change.
`OutputWriter` continues to preflight every nonempty table before publication,
including canonical row timestamps. Numeric and unpartitioned routing retain
their prior timestamp-independent behavior. This change does not claim to
validate every numeric partition configuration outside `OutputWriter`'s
existing zero-size and start-anchor checks.

Progress formatting is infallible: unknown time stays absent, valid negative
time is displayed normally, and an invalid value gets a descriptive numeric
label. Partition-building timestamp borrowing treats any nonzero timestamp,
including negative values, as a real time rather than replacing it with a later
positive anchor.

A malformed envelope is rejected before the ingestion callback. Payload
validation happens before any row of that payload is appended. The ingestion
loop may publish and checkpoint a previously validated buffered prefix at a
partition boundary before decoding a later invalid payload; this is legitimate
progress for the earlier prefix, and the invalid event is never checkpointed.
This fix does not provide multi-table crash/replay atomicity; #468 remains
outstanding.

## Rust API migration

These public helpers now return `anyhow::Result` around their former value:

- `BlockIdentity::timestamp_millis` and `traits::timestamp_millis`.
- `traits::date32_from_timestamp_seconds`.
- `PreparedIdentity::new`, `with_ids` and `with_timestamp_seconds`.
- `CanonicalBuilder::prepare` and `prepare_with_ids`.
- `Partition::partition_key` (`Result<Option<String>>`).
- `ParquetTableWriter::partition_suffix` (`Result<String>`).

Rust callers must propagate or handle the error before mutating their own state.
`BlockMapper::map_block`, writer publication methods and the CLI keep their
existing fallible interfaces. Valid schemas, units, timestamp values, nullable
Solana behavior and destination layout are unchanged. Direct callers that relied
on clamped nanos, saturated timestamps or values outside the shared calendar
range must correct their data; malformed values are no longer accepted.
Older Firehose servers that omit streamed metadata must be upgraded to supply
it; there is no safe generic block identity to invent from an arbitrary payload.

## Validation

The first full workspace run on the #581 head passed 741 tests, with three
existing ignored benchmarks. The final combined run at `8ba2c21`, including
#473 responsive shutdown, #578 atomic publication and the reviewed #500 head
`bd501f2`, passed **763 tests**. Four tests were intentionally ignored: the
three existing benchmarks and the atomic-publication subprocess helper.

Validation commands used the whole-process Cargo lock and shared Arrow 60
target, with dev/test debug information disabled and four build jobs:

```sh
python3 /tmp/fireparq-cargo-locked.py cargo test --workspace --locked -j4
python3 /tmp/fireparq-cargo-locked.py cargo fmt --all -- --check
git diff --check
```

The suite includes real CLI startup/shutdown tests, every-chain schema contracts,
old-Parquet compatibility, final checkpoint regressions, atomic publication
failure injection and the Solana reward flush/restart equality test. The
pre-existing unused final `transactions_processed` assignment warning remains;
no new compiler warning was introduced.

Regression coverage includes:

- Extreme i64 seconds, realistic milliseconds mislabelled as seconds, invalid
  nanos, valid negative subsecond values, and exact calendar range boundaries.
- Complete batches for every chain mapper remain identical to a baseline after
  rejected identities, including already-buffered valid data.
- A late malformed Antelope action timestamp and malformed Solana payload times
  leave all existing table buffers unchanged; negative and nullable Solana
  timestamps preserve their canonical values.
- Invalid time keys, lower-level local writer and S3-key metadata, invalid row
  milliseconds and invalid maximum metadata time fail without output,
  part-counter changes or retained writer data.
- A real local tonic server streams malformed metadata followed by a valid
  response. Missing metadata, extreme seconds, wrong units and invalid nanos
  all fail promptly without invoking the production handler, changing its
  existing checkpoint, or publishing output.
- Progress formatting handles i64 extremes without errors, and both sequential
  and exponential timestamp borrowing accept negative anchors.

No new live Firehose request was needed to exercise malformed input; local RPC
and real mapper fixtures provide deterministic failure cases. Existing live
qualification records remain separate evidence for prior changes.
