# #519: bounded Parquet lookup properties

Implementation and measurement record for
[#519](https://github.com/pinax-network/firehose-parquet/issues/519). This record
is in progress; implementation alone is not issue closure.

## Contract

All ingestion part encoders, including protected transactions and the legacy
writer, use one shared properties module. Local/S3 merge and rollup use its
streaming variant. Existing Arrow schemas, row order, values, nulls and protected
metadata policies remain unchanged. Physical file bytes and file hashes change;
receipts continue to describe the exact bytes that were written.

- Complete ingestion parts declare ascending `block_num` only after checking
  every adjacent UInt64 value and finding no nulls or decreases. Repeated heights
  are valid. The sort index is the Parquet leaf ordinal, including schemas with
  nested fields before `block_num`. Empty, missing or incompatible columns omit
  this metadata. No rows are reordered. A reversible stream may still be sorted
  in a particular part; a decreasing sequence never acquires an assertion.
- Streaming merge/rollup makes no sorting declaration because the next batch or
  file can reverse block order. Input sorting assertions are not inherited.
- A scalar identity-column allowlist enables Bloom filters on at most the first
  eight eligible fields in schema order. Only Utf8/Binary, their large/fixed
  variants, and dictionary wrappers qualify; nested arrays and payload columns
  are excluded. The exact allowlist is in `writer/properties.rs`.
- Candidate row groups contain at most 65,536 rows, matching the maximum expected
  distinct values used to size each filter at a target 1% false-positive rate.
  A filter reserves 128 KiB during writing, at most 1 MiB of filter bitsets for
  eight eligible columns in one active writer. This is not a total writer or RSS
  limit: Arrow, encoder, dictionary and encoded-part storage remain additional.
  Parquet 60 folds small filters before storing them; `AfterRowGroup` placement
  explicitly avoids retaining completed-group filters until file close.
- Bloom filters describe non-null equality membership. A reader may use a
  negative answer to skip a row group; a positive answer still needs an equality
  check. Null predicates use null statistics, never Bloom absence. Consumers
  without Bloom pruning remain compatible but gain no such pruning benefit.

These rules follow the [Parquet Bloom filter format](https://parquet.apache.org/docs/file-format/bloomfilter/)
and the installed Apache Arrow/Parquet 60.0.0 implementation. The library's
[`WriterProperties`](https://arrow.apache.org/rust/parquet/file/properties/struct.WriterProperties.html)
and [`ArrowReaderBuilder`](https://arrow.apache.org/rust/parquet/arrow/arrow_reader/struct.ArrowReaderBuilder.html)
provide the writer settings and actual filter reads used by the tests/benchmark.

## Explicit compression levels and compatibility

`--compression zstd` retains level 3. `--compression zstd:6` selects level 6;
negative levels within Parquet's supported range are accepted. `zstd:3` normalizes
to the existing default tag. Level zero is rejected because it means a library
default rather than the CLI's documented level; missing, non-integer and
out-of-range levels also fail before execution. The same parser reaches build,
merge, rollup and partition-index output. Other codec choices remain unchanged.

The Rust `Compression` enum gains `ZstdWithLevel(ZstdLevel)`. Direct library
construction with level zero or three normalizes to the documented level 3 in
codec conversion, display and transaction identity; CLI zero remains rejected.
Exhaustive downstream
matches must handle the extra variant. All output paths use one codec conversion.
The selected level cannot be verified from the Parquet footer alone, which
records the codec; tests check the configured properties and output round trip.

A non-default explicit level enters the pending transaction identity through a
new `PartCompression` variant. Existing serialized unit variants, including
`"zstd"`, are unchanged. Journal validation rejects invalid or noncanonical
explicit levels (zero or three), and changing a level invalidates the transaction
digest. Older binaries fail closed if asked to recover a pending transaction
containing the new variant. Complete recovery with this version before downgrading.
Normal exact receipt validation and recovery remain the authority for old files;
no file is silently re-encoded during recovery.

## Validation and measurements

The initial focused suite checks actual Parquet footers, nested leaf ordinals,
sorted/decreasing/null inputs, multi-group Bloom membership with no false
negatives, Unicode text/raw binary, null preservation, allowlist bounds, and
complete data equality under every existing codec plus explicit Zstandard level 6.
Separate tests cover dictionary/fixed binary values, protected and legacy output
wiring, codec parsing, and journal identity/canonical validation.

Dictionary encoding remains enabled pending the benchmark comparison. Any decision
to disable it must be justified for specific table/column identities: child-table
transaction hashes and addresses often repeat. The benchmark separately compares
the original row-group default, a size-matched control, lookup properties, and a
narrow dictionary candidate. It must report both compression/write costs and
reader work for present, absent and null predicates. Retained inputs and varied
synthetic data are labeled separately; no network or production S3 benchmark is
part of this change.

Final benchmark results, independent review and integrated checks are pending.
