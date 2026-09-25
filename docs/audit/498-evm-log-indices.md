# EVM receipt-log indices and optional tables (#498)

## Evidence and change

The mapper copies `Log.index` into `logs.log_index` and `Log.blockIndex` into
`logs.block_index`. The checked-in `proto/ethereum.proto` defines the former as
transaction-relative (only guaranteed at EXTENDED detail) and the latter as
block-relative and correctly populated for receipt logs. The mapper reads
receipt logs, rather than the call-log arrays that can include reverted logs.

The README now explains the RPC join key, hash/quantity normalization, fork
identity, source-detail limitation and absence of renumbering. Schema comments
record the same distinction; no field names, types or values changed.

For optional extended tables, the mapper iterates upstream `gas_changes` and
`account_creations` arrays. Account creations are explicitly deprecated and
unsupported from block version 4 in the source protobuf. Gas-change absence is
only a sampled observation; it does not prove zero gas consumption. The writer
skips zero-row batches rather than emitting empty Parquet placeholders. The
README explains the resulting no-matching-files behavior for DuckDB globs.

## Validation

- Read the checked-in protobuf, schema, mapper and zero-row writer path.
- Ran the documented SELECT against the bounded live Ethereum dataset produced
  while qualifying #469 (blocks 26,049,575 and 26,049,576). All 1,438 log rows
  were returned. Each `(block_number, tx_hash, log_index)` and
  `(block_number, block_index)` was unique in these finalized samples;
  1,430 rows had different transaction-relative and block-relative indices.
- Confirmed no Parquet file for `gas_changes` or `account_creations` exists in
  that same bounded dataset; no additional network request was needed.
- Reviewed wording against the original #498 observations rather than
  generalizing two blocks into a universal upstream guarantee.
- Formatting and whitespace checks passed. This is a documentation/comment
  change; no runtime code or output schema changed.
