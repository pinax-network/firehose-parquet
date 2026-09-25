# Solana payload encoding (#503)

## Diagnosis and decision

The default Solana profile applied base58 conversion to arbitrary instruction,
return and error payloads, plus byte-sized account indices. Long payloads incurred
unnecessary base58 arithmetic and the indices were not directly queryable.

The following types are now fixed independently of identifier encoding:

| Tables | Columns | Type |
|---|---|---|
| `transactions`, `vote_transactions` | `err`, `return_data` | nullable Binary |
| `instructions` | `data` | non-null Binary |
| `instructions` | `accounts` | non-null List of non-null UInt8 |
| `account_lookups` | `writable_indexes`, `readonly_indexes` | non-null List of non-null UInt8 |

Keys, signatures, hashes and `return_data_program_id` retain their configured
identifier encoding (base58 in the default profile). Native public-key strings
remain unchanged. Index order, repeated indices and the full 0–255 range are
preserved without resolving, sorting or deduplicating them.

Null behavior is unchanged: absent return-data messages are null, present empty
data is empty binary, and absent/empty errors are null. Empty instruction data
and index arrays remain non-null empty values. Vote/failed filters, row order,
instruction positions and canonical fields are unchanged. Memory estimates now
measure binary buffers and list offsets/values rather than encoded strings.

## Reproducible benchmark

`blocks/examples/bench_solana_payloads.rs` accepts a raw protobuf file. Release
binaries before and after the change used the same runner and raw blocks on an
Apple M1 Max (macOS arm64), Rust 1.93.1. Three warmup iterations precede seven
samples of twenty iterations each. The timed loop includes decoding, mapping,
Arrow flush, row/allocated-buffer measurement and batch drop; it excludes file
reading, mapper construction, network and Parquet/storage writes. All transactions,
including failed and vote rows, are included. Compilation was excluded using the
shared process lock. Measurement order was before/after for the first slot and
after/before for the second. Baseline source was main `4b0f72f`.

| Slot | Identifier encoding | Before median ms/block | After median ms/block | Output rows |
|---|---|---:|---:|---:|
| 300000000 | Base58 | 114.223 | 31.842 | 9,765 |
| 300000001 | Base58 | 149.012 | 29.312 | 9,949 |
| 300000000 | Binary control | 7.655 | 7.525 | 9,765 |
| 300000001 | Binary control | 7.150 | 7.122 | 9,949 |

Default-profile mapping time decreased by about 72% and 80% on these two blocks.
These are bounded sample results, not an end-to-end ingestion throughput promise.
Arrow's allocated bytes for the last flush **increased slightly**: Base58 mode
changed from 5,199,580 to 5,248,392 bytes and 5,967,861 to 6,083,828 bytes. Binary
controls also increased, reflecting the changed array layouts/buffer capacities.
This is neither RSS measurement nor evidence of a memory reduction.
All timings, exact hashes, counts and allocated-byte measurements are retained in
[503-benchmark.json](503-benchmark.json).

```sh
cargo run --release -p blocks --example bench_solana_payloads --locked -- \
  --block /tmp/raw-solana/300000000.pb --iterations 20 --samples 7
```

Use the same example at the baseline revision when reproducing before/after;
keep raw input, runtime settings and hardware fixed.

## Correctness qualification

The focused suite has 31 passing Solana tests. New regressions cover all five
identifier encodings, 4 KiB payloads and all byte values, null/empty distinctions,
repeated and maximum indices, top-level/inner instructions, optional fork columns,
builder reset, memory estimation and both ordinary/vote transaction schemas.
Independent review found no blocking defect.

Raw finalized slots `300000000` and `300000001`, previously captured for #501,
were reused as independent expectations. Their SHA-256 hashes are:

- `c68946ce74e66969b023d6397cff61f6cb8bd6508ff89148fb29130ea6a30dae`
- `552dbd676ea0d3a36be535d6318dc3c920de90ce7f875d48d037f538ba026154`

The old and updated binaries each made one bounded live request for
`[300000000, 300000002)` with finalized blocks, failed transactions included,
the vote table enabled and a flush per block. Each produced sixteen local parts.
DuckDB plus an independent Python base58 decoder/protobuf reader verified every
old value after decoding the affected legacy columns, and every changed value
against raw source bytes or indices:

| Table | Rows |
|---|---:|
| blocks | 2 |
| transactions | 1,159 |
| vote_transactions | 3,289 |
| messages | 1,159 |
| instructions | 7,703 |
| rewards | 2 |
| token_balances | 5,879 |
| account_lookups | 521 |

All 19,714 rows matched, including 252 non-null error payloads and 41 present
return-data values. Signatures and lookup keys matched their original raw bytes.
The updated cursor's public block position was `300000001`. No production storage
was written. The read-only comparison is `503-compare-solana.py`; it takes raw
blocks, a protoc descriptor and baseline/updated dataset roots.

## Migration

This changes existing column types, including **Binary-mode datasets**, where
the index columns change from Binary to List. Use a new output root and rebuild
affected ranges, or explicitly decode legacy text/binary payloads and construct
UInt8 lists into a separate migrated dataset. Schema union alone is insufficient
for conflicting types. Strict merge/rollup requires matching schemas; verification
roots also change because their schema/value representation changes. Do not compare
roots across the old and new contracts as if they described identical schemas.

Integrated current main `d5e1419` (ownership foundation and exact finalized
partition indexes). The full workspace passed 857 tests with five intentional
child/helper skips; the capture-auth example passed its additional regression.
Formatting, workspace build and Bash/Zsh/Fish completions passed. Both README
queries were executed against the updated live sample. CI, PR merge and issue
closure are recorded separately after publication.
