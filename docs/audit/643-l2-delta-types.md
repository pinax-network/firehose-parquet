# Delta column types at the flush boundary (#643, lane L2)

Refs #643; part of #463. PR: [#669](https://github.com/pinax-network/firehose-parquet/pull/669). Design: [`docs/design/delta-lake.md`](../design/delta-lake.md)
§6 (types), §2, §3.3 and the L2 row of §11. Index: the #643 row of the
[audit index](README.md).

## Diagnosis

Delta Lake has no unsigned integers, no dictionary types and a single
timestamp unit, microseconds. The mappers build `UInt64`, `UInt32`, `UInt8`,
`Dictionary(Int32, Utf8)` and `Timestamp(Millisecond, "UTC")` columns, and
every file stored the `date` it was partitioned by. The spike (§1.8) measured
what that means for the target readers:

- Polars refuses a millisecond file under a Delta `timestamp` column
  (`SchemaError: data type mismatch ... Datetime('ms', 'UTC') != target
  Datetime('μs', 'UTC')`).
- Delta convention, and delta-rs's OPTIMIZE output, keep the partition column
  out of the data files.
- A `UInt64` cast to `Int64` without a check wraps, and some chain values (EVM
  PoW nonces, Solana balances) exceed `i64::MAX`.

So the parts fireparq writes had to become valid Delta data files before the
Delta commit layer (L3) can add them to a log.

## Decision

The user's decisions (#643 comments, 2026-09-27) and the design:

- Delta is the only output format of v1.0.0. Target readers are DuckDB and
  Polars.
- Every `UInt64` becomes a **checked** `long`, except currency amounts,
  balances and fees, and values a sender or signer chooses without a range
  check, which become `decimal(20,0)`. There is no per-chain economic reasoning
  beyond that rule. Bitcoin satoshis stay `long` (consensus caps them at
  2.1·10^15).
- `date` is the partition column only. A Solana row without a block time gets
  its routing day as `date`.
- The mapper builders keep their Arrow types; the conversion happens once per
  flush.

## Implementation

**The mapping** (`firehose-parquet/src/delta/types.rs`). `DeltaTypes` holds a
chain's `decimal(20,0)` columns and applies fixed rules to everything else:

| Mapper Arrow type | Delta type | Conversion |
|---|---|---|
| `UInt64` | `long` | checked: a value above `i64::MAX` refuses the flush |
| `UInt64` listed in `ChainProfile::decimal_columns` | `decimal(20,0)` | exact, up to `u64::MAX` |
| `UInt32`, `UInt16` | `long` | lossless |
| `UInt8` | `short` | lossless |
| `Dictionary(Int32, Utf8)` | `string` | Parquet still dictionary-encodes the pages |
| `Timestamp(Millisecond, "UTC")` | `timestamp` | the same instant, stored as `TIMESTAMP(MICROS, UTC)` |
| `date` (`Date32`) | partition column | checked against the flush's partition, then left out of the file |
| `List<T>`, `Struct<...>` | element-wise and field-wise | the same rules |
| `Utf8`, `Binary`, `Boolean`, `Int32`, `Int64`, `Float64`, `Date32` | unchanged | the column is shared, not copied |

- `data_schema(table, schema)` is the Delta data file schema. It requires every
  listed decimal column of the table to exist and hold only `UInt64` values,
  and refuses any type without a Delta type (`LargeUtf8`, a zone-less or
  non-UTC timestamp, nanoseconds, a non-string dictionary, ...).
- `data_batch` and `data_batches` cast with `safe: false`. A value that does
  not fit is an error, never a wrapped or null value. The error names the
  table, the column and the first value above `i64::MAX` (inside lists and
  structs too), says that the flush was refused before anything was written,
  and points at `ChainProfile::decimal_columns`.
- Every non-null `date` must equal the flush's partition, which is the routing
  day of `metadata.min_timestamp`, the same one the writer derives the
  `<table>/date=YYYY-MM-DD` directory from. Empty tables need no routing time.
- `Conversion` and `delta_type_name` describe each column for the docs, and
  `is_delta_type` checks a type at any nesting depth (reader 1 / writer 2, no
  table features).

**Per-chain decisions** (`blocks/src/chain.rs`). `ChainProfile` gains
`decimal_columns: &'static [DecimalColumn]`, each with its reason, and
`delta_types()`. The lists follow the design table:

| Chain | `decimal(20,0)` columns |
|---|---|
| EVM | `blocks.nonce` (PoW nonce), `set_code_authorizations.nonce` (signer-chosen), `withdrawals.amount_gwei` |
| Solana | `fee`, `pre_balances` and `post_balances` of `transactions` and `vote_transactions` (`array<decimal(20,0)>` for the lists), `rewards.post_balance` |
| Beacon | `amount` of `deposits`, `withdrawals`, `deposit_requests`, `withdrawal_requests`; `proposer_slashings.header_{1,2}_slot`; `attester_slashings.attestation_{1,2}_{slot,committee_index,source_epoch,target_epoch}` |
| Antelope | `actions.error_code` |
| Cosmos | `transactions.timeout_height`, `transactions.fee_gas_limit` |
| NEAR, Tron, Bitcoin | none |

Every other `UInt64` column of every chain, including Cosmos
`signer_infos[].sequence` inside its struct and Beacon
`attestation_{1,2}_attesting_indices` (`array<long>`), is a checked `long`.
The per-column lists of all conversions are in each chain's
[`docs/schemas/`](../schemas/README.md) file ("Delta type mapping").

**The flush boundary** (`firehose-parquet/src/ingest/session.rs`).
`MapperSemantics` carries the family's `DeltaTypes`. `declare_inventory` digests
each table's Delta data file schema, so the descriptor's table digests bind the
Delta types, the decimal choices and the missing `date` column.
`IngestionSession::flush` maps the batches right after the non-final
`stream_ordinal` check and before `TransactionController::commit`, so a value
that does not fit fails the flush before the preflight, the Writing journal or
any part. The controller and `PreparedFlush` stay type-agnostic. The
runtime (`blocks/src/bin/ingestion/runtime.rs`) takes the types from the
resolved `ChainKind`. A dry run writes nothing and does not map.

**Writer and readers of the parts.**

- `ParquetTableWriter::validate_partition` accepts microsecond timestamps, and
  the date-column check is skipped when the file has no `date`.
- `writer::properties::for_batch` proves the `block_num` sort order for `Int64`
  too, so parts keep their `SortingColumn` metadata.
- `traits::timestamp_micros_utc_type` names the file type.
- Kept compiling and working until they are rewritten or removed (L5, L7,
  #666), with the minimum change: `verify`'s `batch_blocks` accepts `Int64`
  (otherwise every partition looked open), `merkle_v2` encodes a scale-0
  `Decimal128` like the integer it holds (an added rule, so `merkle_version`
  is unchanged; `docs/verifiability-hash-strategy.md`), and `validate` reads an
  `Int64` `block_num`. `merge`, `scan`, `inspect` and `truncate` needed no
  change.

**Mapper epoch.** `MAPPER_EPOCH` is now `fireparq-mapping-v3`. A protected root
written by `v2` (mapper types and a stored `date`) is refused before any Blocks
request, by `build`, maintenance and `verify`, with the reason and "build into a
new, empty output root". The descriptor needs no new field: the epoch binds
the new semantics (µs storage, the Solana routing-day `date`), and the table
digests bind the exact Delta schemas.

## Tests

- `firehose-parquet/src/delta/types/tests.rs` (9): every mapper type and its
  Delta type, nullability of list items and struct fields, every value and
  null preserved (`u64::MAX` in `decimal(20,0)` and in a decimal list, `255`
  in `short`, ms × 1,000), unchanged columns shared, a value above `i64::MAX`
  refused in a top-level, list and struct `long` with its value named
  (`i64::MAX` itself fits), the `date` check (routing day, null dates, wrong
  day, missing partition, non-`Date32`), routing time rules of
  `data_batches`, listed decimal columns that are missing or not `UInt64`, and
  types without a Delta type.
- `ingest::session::tests::flushes_become_delta_data_files_and_values_that_do_not_fit_are_refused`:
  through the real session and controller, a flush with `u64::MAX` in a
  `long` column fails with the table, column and value, leaves no pending
  journal, no part and the authority at ordinal 0. A reopened session then
  writes a part with `Int64` `block_num`, microsecond `timestamp`, an exact
  `decimal(20,0)` and no `date`.
- `blocks/src/schema_contract_tests.rs`:
  `every_table_maps_onto_delta_types_and_round_trips_through_parquet` covers
  every table of every chain, all five byte encodings and both `fork_step`
  settings (the same case count as the existing contract): only Delta types,
  each column exactly the type an independent oracle of the §6 rules expects,
  nullability kept, no `date`, `Int64` `block_num` and `stream_ordinal`,
  microsecond `timestamp`, exactly the profile's decimal columns, every value
  equal (timestamps ×1,000), and a Parquet round trip that changes neither the
  values nor the protected schema digest.
  `values_above_i64_max_are_refused_in_long_columns_and_kept_in_decimal_columns`
  maps a real EVM block: `gas_used` of `i64::MAX + 1` is refused, while
  `i64::MAX` and a `u64::MAX` PoW nonce fit.
- `blocks/src/chain/tests.rs`: every decimal column names, once and with a
  reason, a `UInt64` (or list) column of its family, and Bitcoin has none. A
  new pinned digest (`DELTA_DATA_SCHEMA_DIGEST`) covers the Delta data schemas
  of all 160 option and encoding combinations of every family. The existing
  mapper-schema digests are unchanged, since the mappers keep their types.
- `blocks/src/schema_docs.rs`: every chain file documents its mapping, lists
  every decimal column with its reason and has a row for each conversion of
  each column. `committed_schema_docs_match_the_code` guards drift.
- `state::tests::pre_date_layout_epoch_is_refused_with_its_layout` also
  refuses `v2`.
- `writer::properties::tests::sorting_is_also_proved_for_a_signed_delta_block_num`
  and `verify::row_encoding::tests::integer_decimals_encode_like_the_unsigned_values_they_hold`.

### Golden fixtures re-pinned

`blocks/tests/evm_golden.rs` now checks the Delta data file batches, which are
what a build writes: every table holds only Delta types and no `date`, the
canonical `timestamp` is compared in microseconds, and the block's UTC day is
checked as its partition. Its cell normalizer only accepts Delta types and
renders a `decimal(20,0)` as an exact decimal string, so a column mapped to
the wrong type fails.

The payloads are unchanged. `expected.json` changed because of this:

- `withdrawals.amount_gwei` (block 26,049,575) and
  `set_code_authorizations.nonce` (block 26,000,004) are `decimal(20,0)`
  columns, so their values are now decimal strings (`"14267015"`, `"1235"`,
  ...). Before, they were JSON numbers compared with a `UInt64` cell.
- Each `blocks` row gained `"nonce": "0"`. Both raw headers leave the proto3
  field unset, as every proof-of-stake block does (checked with `protoc
  --decode`). This pins the `decimal(20,0)` type of `blocks.nonce` in real data.
  The counts went from 283 to 284 and from 304 to 305 selected values.

The 26,000,004 oracle ([`docs/audit/499-evm-golden-oracle.py`](https://github.com/pinax-network/firehose-parquet/blob/v1.0.1/docs/audit/499-evm-golden-oracle.py)) emits the same
strings. Run with Python protobuf 7.36.2 and `protoc` on the retained payload,
it reproduces `expected.json` byte for byte.
`cargo test -p blocks --example refresh_evm_golden` (the capture helper's
offline auth test) passes unchanged: the capture step never generates
expectations.

### Engine test

`blocks/tests/engine_compat.rs` still reads the plain files, because the Delta
log arrives with L3. The EVM fixture block now has a header with a
`u64::MAX` PoW nonce. Both engines must read:

| Column | DuckDB 1.1.1 | Polars 1.44.2 |
|---|---|---|
| `block_num`, `num_transactions`, `gas_used`, `index`, `log_index`, `transaction_index`, `compute_units_consumed`, `stream_ordinal` | `BIGINT` | `Int64` |
| `blocks.nonce`, Solana `fee`, `rewards.post_balance` | `DECIMAL(20,0)` | `Decimal(precision=20, scale=0)` |
| Solana `pre_balances` | `DECIMAL(20,0)[]` | `List(Decimal(precision=20, scale=0))` |
| Solana `instructions.accounts` | `SMALLINT[]` | `List(Int16)` |
| enums (`detail_level`, `type`, `status`, `reward_type`) | `VARCHAR` | `String` |
| `timestamp` | `TIMESTAMP WITH TIME ZONE` | `Datetime(time_unit='us', time_zone='UTC')` |
| `date` (Hive only) | `DATE` | `Date` |

The test also checks these facts in both engines. The data files have no
`date` column. Every row's Hive `date` equals its directory. The Parquet
timestamp is `TIMESTAMP(MICROS, UTC)` and holds whole milliseconds. Exact
minimums are `18446744073709551615` for the nonce, and `5000`, `999`, `1234`,
`42000` and `21000` elsewhere. The existing row, day and path checks still
hold.

### Other test updates

The real-binary tests that read parts (`adaptive_flush.rs`,
`ingestion_transactions.rs`, `non_final_stream.rs`) read `block_num` and
`stream_ordinal` as `Int64`. The session tests pass the new
`declare_inventory` argument.

## Validation

- `cargo fmt --all` and `cargo test --workspace --locked` pass with
  `FIREPARQ_REQUIRE_DUCKDB=1 FIREPARQ_DUCKDB=/opt/homebrew/bin/duckdb`
  (DuckDB 1.1.1) and `FIREPARQ_REQUIRE_POLARS=1` with Polars 1.44.2 from the
  pinned `blocks/tests/engines/requirements.txt`: 1,129 passed, 16 ignored,
  on origin/main `3890651` plus this change. `cargo test -p blocks --example
  refresh_evm_golden --locked` passes.
- `cargo run -p blocks --example dump_schemas` regenerated `docs/schemas/`.
  Every column shows its Delta type, and each chain ends with its mapping and
  its `decimal(20,0)` columns.

## Limits

- The files are Delta data files, but there is no Delta log yet. Until L3
  commits them, readers use `read_parquet`/`scan_parquet` with Hive
  partitioning. `date` then comes from the directory, and a plain file read
  has none.
- The mapping copies each converted column once per flush. The design's later
  option, mapper builders that build Delta types directly, would save it.
- The per-chain README sections still name the mappers' Arrow types. The
  schema reference is authoritative for the Delta types, and L10 rewrites the
  README for Delta readers.
- `verify` and `validate` read the new parts through the minimum changes above;
  their Delta-snapshot versions are #666 and L7.
