# Antelope database-operation attribution (#508)

## Diagnosis and contract

`db_ops.action_index` repeats across transactions. The mapper previously dropped
the enclosing trace's identity, so these rows could not be reliably attributed.
The preserved earlier agent worktree was an empty placeholder; implementation
continued in an isolated checkout.

Three non-null columns are appended before optional `fork_step`:

| Column | Type | Source and scope |
|---|---|---|
| `tx_hash` | Utf8 | Enclosing `TransactionTrace.id`, verbatim |
| `tx_index` | UInt64 | Enclosing `TransactionTrace.index`; original position, retaining gaps after filtering |
| `db_op_index` | UInt32 | Zero-based position within that trace's `db_ops` |

Join to `transactions` using canonical `block_id`, `tx_hash`, and
`db_ops.tx_index = transactions.index`. An operation's key also includes
`db_op_index`; these keys do not resolve replayed rows. Every selected trace's
operation-index range and action times are checked before any row is appended.
Existing failed-transaction selection, order, `action_index`, native strings,
byte encoding and nullable data fields are preserved.

Status and operation names borrow static protobuf labels, preserving `UNKNOWN`
for unrecognized values. Authorization is written directly into Arrow. Exception
and authorization-sequence JSON use borrowed Serde wrappers streamed into the
builder, avoiding intermediate row-sized strings, vectors and JSON trees. The
previous field order, escaping, nulls and lossy invalid-byte conversion are
preserved. Invalid UTF-8 exception data still requires a replacement buffer.
Serde was already locked; adding its direct dependency changes no package version.

The issue describes four action columns as redundant. `transaction_id`,
`trace_block_num`, `producer_block_id`, and `block_time` remain verbatim source
metadata. They are deprecated as ordinary join/routing keys in favor of `tx_hash`
and canonical block fields, with no removal scheduled. Missing or differing
action metadata must not be silently replaced by canonical values.

## Validation

- 796 workspace tests passed, with four intended ignored tests. The separate
  EVM capture-authentication example passed (one test plus its ignored child).
- Formatting and binary build passed. The pre-existing final-backfill
  unused-assignment warning is unchanged.
- Fifteen Antelope tests cover repeated action indices across transactions,
  original transaction-index gaps and `u64::MAX`, operation reset, filters,
  binary/hex encodings, optional fork columns, empty flushes, unknown enum values,
  and source action aliases deliberately differing from canonical metadata.
- JSON regression compares the old serializer's exact bytes with controls,
  non-ASCII text, nested/absent contexts, invalid UTF-8 data and integer limits.
  Interleaved null/non-null values and builder reset verify string boundaries.
- Independent review checked mapper, schema, memory estimates, serializers and
  tests and found no blocking defect.

## Bounded live qualification, 2026-09-25

One finalized EOS block, `400000000`, was captured from
`https://eos.firehose.pinax.network:443`. Its 14,971-byte protobuf SHA-256 is
`09e692919f6a2957341b4f7452961ebeccc5297c91f65b5f81e067154b0e1aa6`.
Block ID: `17d78400497cff2d017bda6cc55257167e4a8d54a9537e455069d98fdc2c0fb7`.

The old and updated binaries each read exactly `[400000000, 400000001)` into
separate temporary local roots with one flush. A Python protobuf reader and
DuckDB independently verified:

| Table | Rows | Result |
|---|---:|---|
| blocks | 1 | All existing columns equal |
| transactions | 5 | All existing columns equal |
| actions | 8 | All existing columns equal, including authorization/receipt JSON |
| db_ops | 10 | All existing columns equal; new joins and positions match raw traces |

All ten database rows joined exactly once to the five transactions. Both runs
produced four parts; the updated cursor's public position is `400000000`. No
production storage was written. The sample has only executed traces and no
action exceptions; filter edge cases and exception JSON are covered offline.
Action aliases equal canonical metadata in this sample; tests demonstrate why
this is not a universal guarantee. The endpoint supplied `parent_num=0` with a
nonempty parent ID; this change preserves it without an ancestry claim.

Re-run the read-only comparison using a captured block, descriptor and outputs:

```sh
protoc -I proto --include_imports --descriptor_set_out=/tmp/antelope.desc proto/antelope.proto
uv run --with protobuf python docs/audit/508-compare-antelope.py \
  --raw /tmp/raw-eos-block --descriptor /tmp/antelope.desc \
  --baseline /tmp/before/eos --dataset /tmp/after/eos \
  --cursor /tmp/after/cursor.parquet
```

## Upgrade and completion

Older files have no transaction/operation positions. Rebuild ranges into a new
dataset if these joins are needed. Schema-union readers see null additions in
old files; strict merge/rollup users must reconcile schemas explicitly. Existing
action columns remain unchanged. PR merge, CI and issue closure are verified
separately and recorded in the audit index.
