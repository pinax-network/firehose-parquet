# Solana instruction positions (#502)

Issue: <https://github.com/pinax-network/firehose-parquet/issues/502>

## Diagnosis

The mapper emits all top-level instructions, then all recorded inner groups.
`instruction_index` counts those rows; sorting it cannot interleave a top-level
instruction with its inner calls. `inner_index` stores the top-level parent's
index, despite a name that suggests a position within the inner set. Changing
either existing column would break consumers that use them as row keys.

## Implementation

- Append nullable `UInt32` columns `parent_instruction_index` and
  `inner_instruction_index` to `blocks/src/solana/schema.rs` before the optional
  `fork_step` column.
- Top-level rows have nulls in both columns. Inner rows copy their group's
  top-level index and enumerate positions within the group's instruction vector.
  Convert the position to `u32` with a checked conversion before appending that
  row.
- Keep all old column values, output row ordering, encoding modes and optional
  fork column behavior. Include the two builders in memory accounting and flush
  reset.
- Document ordering and schema migration in the README and release notes.

The ordering for a single transaction in a single block event is
`coalesce(parent_instruction_index, instruction_index), is_inner,
inner_instruction_index`. The parent is the top-level owner of the inner set,
not an immediate caller inferred from stack depth. Missing stack depth remains
unknown. No synthetic inner calls or execution-success claims are introduced.

## Validation

The regression fixture has three top-level instructions and two inner groups,
with deliberately reversed group order. One group includes nested depth and
another has missing depth. It verifies every legacy/new position, resulting call
order, nullable types, and complete batch equality after a flush reset, across
binary, hex and base58 encodings with and without `fork_step`.

The focused regression passed on 2026-09-25. Workspace integration, independent
review and bounded existing-output comparison are recorded below when complete.

## Compatibility limits

This is an additive schema change. An old file has no new positions. A reader
using schema union sees nulls for old rows, including inner rows; it must not
interpret those nulls as evidence that a row is top-level. Rebuild affected old
ranges separately to use the new positions. Strict-schema maintenance rejects
mixed instruction schemas. The fields preserve upstream inner-set positions;
they do not resolve reorgs, repeated block events or crash/replay duplicates.
