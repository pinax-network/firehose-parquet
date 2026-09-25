# Projected dataset validation (#524)

The previous validator decoded every column, cloned all block tuples while
building partitions and again for global checks, then searched the whole tuple
collection twice for each adjacent partition. The height-only searches could
also choose an endpoint from a different partition when heights overlapped.

## Implementation

One shared reader projects the Parquet root columns `block_num`, `block_id`,
`parent_id`, and optional `timestamp`. It resolves indices again from the
projected Arrow schema, preserving arbitrary source column order and nested
unselected columns. Full footer schemas remain available for schema consistency
checks, including columns whose data pages are not read.

Partition groups are consumed and moved into the global tuple vector. The
cross-partition pass retains each partition's own first/last tuples, sorts those
boundaries by minimum height, and compares adjacent endpoints directly. That
pass changes from O(P*N) searches to O(P log P) ordering plus O(P) comparisons.
Global duplicate, parent, timestamp and gap checks still keep and sort all N
canonical tuples; total validator memory is not bounded independently of N.
Stable ordering within equal-height runs and partition-name ordering for equal
minimum heights are retained. Boundary successor arithmetic is checked, including
an overlapping partition whose maximum height is UInt64's maximum.

Local reads skip unselected column data pages. S3 currently downloads each entire
object before constructing a Bytes-backed Parquet reader: the shared projection
reduces decoding/allocation but **does not reduce S3 network bytes**. No async
reader/dependency migration is bundled here. Validation checks the selected
canonical data and all footer field schemas, not the integrity of skipped payload
pages. There is no file format, CLI or output schema migration.

## Validation

Five focused regressions pass. They compare projected extraction to full-column
extraction for Utf8/Binary IDs with/without a nullable timestamp, include an
unselected two-leaf nested Struct column before reordered canonical fields and two row groups,
and use a reader that rejects any request for an unselected payload page.
The actual local file entry point is exercised too. Additional cases prove that
an unselected field type mismatch is still reported, overlapping heights cannot
hide or invent cross-partition parent mismatches, and empty partitions, gap
options and maximum UInt64 heights retain the expected behavior.

The full suite on main `2a3724e` passed **874 workspace tests**, with six
intentional skips, followed by all five strengthened focused tests, the fixture
capture authentication example (one parent test, one child helper skip), and
the workspace build. Independent review found no production blocker; its nested
fixture improvement is included. The pre-existing final-backfill unused-assignment
warning remains. Only temporary synthetic local datasets were used; no Firehose
requests or production storage mutations were involved.

## Reproducible benchmark

`524-benchmark.py` builds one 250,000-row wide file (four validation fields plus
64 pseudo-random UInt64 payload fields) and three 200,000-row canonical-only
layouts with 25, 100 and 400 Hive partitions. DuckDB 1.1.1 writes uncompressed
Parquet. The wide fixture isolates wasted-column decoding; the narrow layouts
make partition-count scaling visible. These are synthetic measurements, not a
claim about every production dataset.

Both binaries use Rust 1.93.1 optimized release builds on Apple M1 Max/macOS arm64.
The baseline is main `b8d6834`; the candidate is the #524 implementation integrated
with `2a3724e` (the intervening EVM-only conversion does not participate in
validation). Each has one warmup and seven alternating measured runs per layout.
The timer covers the whole CLI process, including startup and result formatting;
files are warm in the local OS cache. Timing runs hold the shared build lock so
other session compilation/benchmarks cannot contend. Dataset generation and build
time are excluded. Complete stdout must match between both binaries and all runs.

Run with two release binaries and DuckDB available:

```sh
python3 docs/audit/524-benchmark.py --before /path/to/baseline/fireparq --after /path/to/candidate/fireparq --root /tmp/fireparq-524-data
```


Measured median wall time (all seven samples and exact result equality checks in
[`524-benchmark.json`](524-benchmark.json)):

| Local dataset | Rows | Files | Before | After | Speedup |
|---|---:|---:|---:|---:|---:|
| Wide, 64 unused payload columns | 250,000 | 1 | 93.59 ms | 34.58 ms | 2.71x |
| Canonical fields, 25 partitions | 200,000 | 25 | 55.14 ms | 30.33 ms | 1.82x |
| Canonical fields, 100 partitions | 200,000 | 100 | 75.09 ms | 35.25 ms | 2.13x |
| Canonical fields, 400 partitions | 200,000 | 400 | 163.42 ms | 60.32 ms | 2.71x |

All datasets produced exactly equal valid summaries before and after. The wide
file was 137,885,482 bytes; the narrow layouts were approximately 7.8–8.0 MB.
Filesystem discovery and per-file footer reads remain in these measurements.
The boundary selection correctness change is separately covered by overlapping
height regressions; the deliberately valid benchmark does not exercise that fix.
