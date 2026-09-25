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

On 2026-09-25, the focused regression passed. The combined implementation at
`a636908` includes timestamp validation (#476, main `34cb6e2`) and probe
reliability (#485, integration `4104a1b`). The workspace suite passed **780 tests,
0 failed, 4 ignored**, plus all doc tests; the binary build, formatting and
`git diff --check` passed. One ignored helper is invoked by the active atomic
publication subprocess test. Independent review found no blocking issues.
The later merge of main `3cf984b` incorporates the actual #583 merge ancestry
and its final validation note; runtime source is unchanged from the tested build.

A fresh local-output run fetched exactly Solana slots `[300000000, 300000002)`
with one block per flush, explicit Pinax provider credentials, and no ambient
dotenv file. DuckDB comparison against the previously qualified #500 output
found identical existing schemas and all **15,832 rows across eight tables**
after projecting away the two new columns. Bidirectional `EXCEPT ALL` returned
zero differences in every table. The 6,179 instruction rows comprise 3,462
top-level and 2,717 inner instructions. All parent/null contracts and dense inner
positions passed. The cursor ended at 300000001 with no remaining temporary
parts. This comparison used the existing vote classification; #501 is separate.

The #501 investigation independently fetched the same two raw protobuf blocks
with two unary RPCs. Reusing those files, a separate Python protobuf decoder
generated the expected top-level parents, inner positions and optional stack
heights for the 916 transactions retained by the mapper. All 6,179 rows matched
exactly. No additional raw fetch was needed for this check.

| Slot | Raw protobuf bytes | SHA-256 |
|---|---:|---|
| 300000000 | 2,946,958 | `c68946ce74e66969b023d6397cff61f6cb8bd6508ff89148fb29130ea6a30dae` |
| 300000001 | 2,935,030 | `552dbd676ea0d3a36be535d6318dc3c920de90ce7f875d48d037f538ba026154` |

Local audit-host evidence is in `/tmp/fireparq-502-combined-tests.log`,
`/tmp/fireparq-502-live-comparison.json` and
`/tmp/fireparq-502-raw-comparison.json`. Raw bodies contain no Firehose cursors or
credentials and are kept outside the repository. These two historical blocks
qualify the tested mappings; malformed/reversed groups and encoding/fork-column
variants are covered by the deterministic regression fixture.

## Compatibility limits

This is an additive schema change. An old file has no new positions. A reader
using schema union sees nulls for old rows, including inner rows; it must not
interpret those nulls as evidence that a row is top-level. Rebuild affected old
ranges separately to use the new positions. Strict-schema maintenance rejects
mixed instruction schemas. The fields preserve upstream inner-set positions;
they do not resolve reorgs, repeated block events or crash/replay duplicates.
