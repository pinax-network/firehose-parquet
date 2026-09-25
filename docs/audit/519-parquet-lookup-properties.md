# #519: bounded Parquet lookup properties

Implementation and measurement record for
[#519](https://github.com/pinax-network/firehose-parquet/issues/519). This record
records the implemented policy, independent review and completed offline measurements.
Publication and CI status are recorded separately in the audit index.

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

## Benchmark method and evidence

The reproducible driver is `firehose-parquet/examples/bench_lookup_properties.rs`.
[Machine-readable measurements](519-lookup-benchmark.json) retain every encode and
query duration, source/output file hashes, exact row/schema checks and pruning
counts. Invariant values are stored once per corpus/variant/probe; timing entries
retain rotation and repetition. The original JSON SHA-256 is recorded as provenance.
The driver and measured property source hashes are respectively
`258012a7307219daa3b7e95a94807027e2db72fa7619508e82ab4fcadb781f9b` and
`e3709e6882087e110c30b34105f2b8c4a6e13768ec7835dd2e3342d2c36b6694`.

The 2026-09-25 run used three rotating encode orders, three query repetitions,
one untimed encode warmup per corpus/variant and query warmups per file/probe.
The full measured process held the shared Cargo/benchmark lock; there were no
concurrent builds or benchmark timings. Encoding measures a complete in-memory
Parquet file, excluding fixture construction, publication/fsync and row checks.
Queries include file open/footer, exact min/max and null-statistic pruning, optional
Bloom reads, projected column decoding and the actual equality filter. Inexact or
truncated min/max bounds are conservatively ignored in every variant. This is an
explicit reader implementation, not a claim about every SQL engine's pruning.

Six corpora reuse public data already captured for earlier qualifications:
Ethereum blocks 26049575–26049576 (458 transactions and 1,438 logs), and retained
Solana output in Binary and Base58 (1,159 transactions and 521 account lookups per
encoding). Source files and hashes are recorded; this is not a new live fetch.
When source footer metadata differs, the driver retains all typed fields and rows
but consistently rebinds schema metadata to the first source file, recording that
count. The Solana inputs precede the additive #607 outcome column.

Two separately labeled synthetic corpora contain 262,144 rows each: deterministic
unique-looking binary/Utf8 hashes, 1,024 repeated addresses with nulls, and a two-label
dictionary status. They provide multiple row groups and varied lookup values;
they do not establish live-chain throughput or cardinality distributions.

Five variants isolate the decisions: original compression-only properties with
the prior row-group default; a 65,536-row size-matched control; production ingestion
properties; streaming maintenance properties; and a diagnostic that disables
only the specifically designated transaction hash/signature dictionary. Other
columns, including repeated child transaction IDs and addresses, retain dictionaries.
All use Zstandard level 3. Present, in-range absent, outside-range absent and null
probes check exact counts against a full scan of the original column.

All **120 encodes** preserve full Arrow schemas and every typed row. All **3,510
query counts** match; every null predicate performs zero Bloom checks. Each
corpus/variant produces identical physical bytes over its three rotations. A
versioned Arrow RowConverter digest, with dictionary normalization and
length-delimited rows, additionally compares logical rows; it is not a raw Arrow
IPC-buffer hash. Source/output physical hashes have a separate meaning.

## Measured tradeoffs

Values below are medians, original → ingestion properties. Lookup times select the
middle present key and the in-range absent key on the named column. They do not
average hashes and repeated addresses together. All six retained corpora occupy
one row group under both settings.

| Retained corpus / lookup column | File bytes | Encode ms | Present µs | Absent µs |
|---|---:|---:|---:|---:|
| EVM transactions / hash | 176,701 → 178,825 | 2.310 → 2.434 | 92.708 → 97.000 | 91.583 → 60.291 |
| EVM logs / tx_hash | 104,274 → 105,351 | 1.692 → 1.665 | 86.042 → 86.916 | 85.292 → 44.500 |
| Solana Binary transactions / signature | 278,267 → 280,345 | 3.684 → 3.747 | 82.709 → 84.667 | 83.958 → 43.083 |
| Solana Binary lookups / account_key | 14,876 → 15,162 | 0.323 → 0.344 | 48.625 → 50.167 | 48.542 → 34.125 |
| Solana Base58 transactions / signature | 280,195 → 282,273 | 3.839 → 3.914 | 173.709 → 174.875 | 172.750 → 42.875 |
| Solana Base58 lookups / account_key | 15,199 → 15,485 | 0.329 → 0.353 | 56.000 → 58.708 | 56.875 → 34.500 |

Files grow **0.74–1.92%**. Missing identity keys avoid all row decoding and improve
about **1.4–4.0×**, while present-key timings are similar or slightly slower.
Tiny sub-4 ms encodes are sensitive to timing noise; their small differences are
not a throughput guarantee. Streaming properties have the same pruning behavior;
the sorting declaration accounts for seven bytes per group in these samples.

The synthetic comparison separately exposes row-group and filter costs:

| Corpus / variant | File bytes | Encode ms | Present hash ms | Absent hash ms |
|---|---:|---:|---:|---:|
| Binary / original | 9,241,615 | 56.470 | 5.933 | 5.899 |
| Binary / matched row groups | 9,711,312 | 63.539 | 6.624 | 6.502 |
| Binary / ingestion | 10,244,020 | 66.112 | 1.750 | 0.087 |
| Utf8 / original | 9,913,164 | 99.792 | 17.400 | 17.091 |
| Utf8 / matched row groups | 10,304,417 | 106.780 | 17.725 | 17.564 |
| Utf8 / ingestion | 10,837,125 | 110.067 | 4.556 | 0.099 |

For these middle hash keys the matched control decodes all 262,144 rows; ingestion
decodes one 65,536-row group. Absent keys decode zero rows. Present repeated-address
queries still decode every group and remain near the matched control. Total file
growth versus original is 9.3–10.8%, of which filters/sorting contribute 5.2–5.5%
relative to matched row groups. Lower row-group sizes can therefore cost storage
and encoding time even when a reader does not use filters.

**Dictionary policy remains unchanged.** The narrowly scoped diagnostic saves
about 0.3–0.6% on retained transaction files and 1.0–2.1% on synthetic candidate
files. Synthetic Binary encode time falls 66.11 → 60.40 ms and Utf8 110.07 → 107.86 ms.
These limited gains do not justify a global field-name rule: `hash`, `signature`
and child transaction IDs may repeat in other tables or reversible streams, and
the shared streaming helper lacks table/cardinality context. The requested tuning
was evaluated; automatic dictionary selection is deferred until a separately
qualified policy can make that distinction. Enum/status and address dictionaries
remain intact.

Whole-process elapsed time was 15.53 s with maximum RSS 334,266,368 bytes (318.8 MiB).
This includes all fixtures, warmups and verification; it is not a per-variant memory
comparison. No provider request, production storage write or live performance
claim is part of this benchmark.

## Reproduce and validation gate

Build and execute under a shared exclusive lock when comparing timings:

```sh
cargo build -p firehose-parquet --example bench_lookup_properties --release --locked
./target/release/examples/bench_lookup_properties \
  --evm-root /path/to/retained-evm-output \
  --solana-root /path/to/retained-solana-matrix \
  --repetitions 3 --query-repetitions 3 --synthetic-rows 262144 \
  --output /tmp/lookup-benchmark.json
```

Without retained inputs, `--only synthetic-binary` or `--only synthetic-utf8`
reproduces the labeled synthetic cases. The Solana directory naming is defined
in the driver and matches the #550 replay matrix. Retained source files are local
qualification artifacts, not bundled downloads.

The complete suite on #519 atop main `9eddfd4` passed **1,036 tests**, zero failures,
nine ignored; the CI capture example passed one test with one ignored. Binary
build, formatting and Bash/Zsh/Fish completions passed. Independent implementation
review found no blocker; its CLI-help and level-zero canonicalization observations
were addressed before the final suite. Root independently reviewed the benchmark
and the measured policy decision. Current-main integration includes #607 at
`955b8b2`; the final CI must pass on the submitted head before merge.
