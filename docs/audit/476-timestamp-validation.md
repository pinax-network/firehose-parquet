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

## Built binary and bounded live qualification

The workspace build passed, as did root/build help and Bash, Zsh and Fish
completion generation. The final binary was copied to `/tmp/fireparq-476-binary`
while holding the whole-process Cargo lock, so later shared-target builds could
not replace the executable used for qualification.

A fresh local run on 2026-09-25 requested only Ethereum blocks
`[26049575,26049577)` from `https://eth.firehose.pinax.network:443` with the
explicit `PINAX_API_KEY` selector and an unset bearer-token selector. The child
process used an empty temporary working directory and an environment containing
only `PATH` and the intended Pinax key. No StreamingFast credential or S3
destination was supplied. Options included `--partition none --compression zstd
--flush-blocks 1 --final-blocks-only`, with the baseline's extended EVM output,
hex encoding and failed-transaction inclusion.

The process exited successfully in 2.55 seconds, producing 26 data parts plus
the cursor and no `.fireparq-*.tmp` files. DuckDB compared fresh output at
`/tmp/fireparq-476-live-20260925/mainnet` against the retained reference
`/tmp/fireparq-469-live-20260925/mainnet`. All 14 SQL schemas and full physical
Parquet schemas (excluding filenames) match. `EXCEPT ALL` over every column in
both directions returns zero rows for every table, checking values and duplicate
multiplicities independently of file names or ordering:

| Table | Rows in each output | Difference either direction |
|---|---:|---:|
| access_lists | 72 | 0 |
| balance_changes | 1,970 | 0 |
| blocks | 2 | 0 |
| calls | 4,261 | 0 |
| code_changes | 3 | 0 |
| logs | 1,438 | 0 |
| nonce_changes | 465 | 0 |
| set_code_authorizations | 2 | 0 |
| storage_changes | 3,545 | 0 |
| system_balance_changes | 32 | 0 |
| system_calls | 8 | 0 |
| system_storage_changes | 10 | 0 |
| transactions | 458 | 0 |
| withdrawals | 32 | 0 |
| **Total** | **12,298** | **0** |

Both outputs contain exactly block numbers 26049575 and 26049576. Cursor
semantic fields match in both directions, excluding only the opaque cursor
value and `updated_at`; no cursor value or credential is recorded here. Raw
comparison evidence is local at `/tmp/fireparq-476-live-comparison.json`.
Malformed-input qualification remains deterministic in the local RPC and real
mapper regressions; the live run validates unchanged successful output.

Actual main `803ffc3` (#582 merged) was then integrated. Its source is identical
to the already-tested #582 head `bd501f2`; this integration changes ancestry,
not the qualified runtime code.
