# Issue #522: bounded streaming rollup

The old rollup retained every decoded batch in a target partition, concatenated
the whole group, encoded it to measure its compressed size, then encoded slices
again. Memory therefore grew with all of a day's input, even with a small
`--flush-bytes` setting. The S3 path also downloaded each whole source object.

## Preserved unfinished work

The stopped `audit/522-rollup-streaming` worktree was inspected read-only at
`fd8513b`. Its uncommitted changes affected only `merge.rs` and `merge_journal.rs`,
not `rollup.rs`. The exact binary diff was saved separately with SHA256
`c02cefbba655a7df82add758b10aae73f8b4306442a14554a951789f87d789ac`.
The useful idea was counting already encoded row groups in the shared streaming
writer's byte target. The unfinished generic rollup journal and earlier S3 lock
sketch were not transplanted. This implementation started on an isolated branch
and was integrated with main `39d49f6`, including protected-ingestion maintenance
policy, the Cosmos protobuf update, and the transport/authentication changes. The
benchmark uses a frozen binary built on `9379883`; later main integration is
covered by the combined and focused checks described below.

## Implementation and memory contract

Each target group receives two bounded decode passes. The first validates every
ordered schema and decodes every source batch before any output for that group
is published. The second feeds batches of at most 1,024 rows to the same streaming
part writer used by merge. It never concatenates the whole group or performs a
whole-group sizing encode. A corrupt late page consequently leaves that group's
existing outputs and sources untouched, including with `--delete-source`.

`--flush-bytes` remains an approximate compressed/encoded output-part target.
Both already encoded row groups and the active group's encoded-size estimate
count toward it. A separate, fixed 32 MiB estimated-memory budget flushes the
active row group inside the same file; it does not publish a new file. Full-file
row counts survive automatic and explicit row-group flushes, preserving merge's
row threshold and published row counts. Checks happen between input batches.
A zero byte target disables the output-size threshold, while the independent
row-group budget remains finite.

The retained data is one encoded output part, one estimated-bounded active row
group, and one decoded input batch, plus reader page/dictionary/footer state and
codec overhead. No decoded batches or encoded parts accumulate across the group.
A single wide row, large page or dictionary, transient compression allocation,
and allocator retention can exceed the configured targets; this is not an
absolute RSS ceiling. Input/output path lists and schema/footer metadata still
scale with file counts. Output file boundaries and compression ratio can change.

For S3, a custom Parquet `ChunkReader` uses the existing object_store 0.12 client.
Every footer/page request is bounded and pinned to the listed ETag/version.
The shared ownership protocol's usable-version rules reject wildcard, comma,
control-character, empty, and null-only snapshot tokens before any GET. Changed
snapshots, ignored ranges, oversized responses, and truncated bodies fail closed.
Page-header cursors buffer at most 64 KiB; body collection refuses bytes beyond
the requested range. Inspection of the installed Parquet 60 sources showed its
optional object_store integration requires object_store 0.14, and its async stream
buffers selected row-group chunks. This reader avoids both dependency churn and
whole-object/whole-row-group downloads. Pages and dictionaries remain reader
memory lower bounds. Two decode passes increase read traffic and request counts.

The synchronous rollup API still requires a synchronous caller or a multi-thread
Tokio runtime; calling it inside a current-thread runtime is not newly supported.
No application or transport mutation retry is added.

## Publication and protected output

All group outputs must succeed before earlier copy outputs or source files are
removed. The complete first decode pass preserves the existing late-corruption
refusal. Later I/O failures, concurrent noncooperating source mutation, or process
failure can still leave partial legacy rollup output; this is not a new rollup
transaction protocol or a crash-safe group replacement claim.

The merged protected-dataset guards and recovery order are retained. In-place or
destructive rollup of protected output is refused; supported export acquires all
required scopes and recovers before reading. Compaction/export must not inherit a
source transaction receipt: the first writer schema, every decoded batch schema,
and footer properties all strip `fireparq.ingest.*`, and a new Arrow schema hint
is generated. Existing real-controller-part tests check both the final footer and
the reader's reconstructed schema, alongside protected refusal and recovery tests.

## Validation

On main `b364681` plus this change, the full workspace passed 1,015 tests with
nine intentional ignores. The capture example separately passed one test with one
subprocess-helper ignore. The binary was built and copied under the same
whole-process Cargo lock. CLI help, generated Zsh completions, formatting, and
diff checks also passed. No production S3 writes or live Firehose requests were
part of this qualification.

After integrating the disjoint authentication cleanup at main `39d49f6`, focused
gRPC tests passed 50 cases (one subprocess-helper ignore), and all 28 rollup tests
passed. The binary rebuild, formatting, help and Zsh completions passed again.
Full PR CI covers the final combined tree.

Regressions cover exact row preservation, metadata, mixed schemas, source
deletion, idempotent copy replacement, and local/S3 late-page corruption that
leaves source and pre-existing output bytes untouched. The instrumented S3 store
proves bounded, pinned page requests, refuses invalid snapshot tokens before any
GET, and rejects stale/ignored/oversized responses. Shared writer tests cover
closed row groups in both row and byte thresholds, memory-triggered row-group
flush without premature file publication, and current-part-only retention over
524,288 rows. Protected maintenance regressions run unchanged in the full suite.

## Reproducible measurements

The local synthetic benchmark is `522-rollup-benchmark.py`, with raw results in
`522-rollup-benchmark.json`. The baseline binary is clean main `f555898`; the measured updated
binary is this change integrated on main `9379883`. Both were built and copied
under the whole-process Cargo lock, with debug profiles and debuginfo disabled.
The later main changes include protected maintenance acquisition/recovery and
Cosmos support; the reported whole-CLI measurement includes their startup cost.

Three alternating samples per version measure separate CLI processes using macOS
`/usr/bin/time -l`, cached local input and an explicit empty dotenv file. The entire
benchmark holds the same process lock used by builds and other benchmarks, so
these agent activities cannot overlap the final measurements. UTC acquisition and
completion times, the input fixture hash, and DuckDB version are included in the
raw evidence. Earlier unguarded timing runs are diagnostic only and are excluded
from the final tables. Peak RSS is per process, not cumulative child usage. Verification runs outside the timed
process and checks exact schema plus all 33 columns and their multiplicities with
bidirectional `EXCEPT ALL`. Each source has 8,192 rows; identical copies deliberately
stress dictionary reuse. These are local synthetic results, not production S3 or
live blockchain throughput measurements.

At a 4 MiB encoded output target:

| Input files / bytes | Old peak RSS | Streaming peak RSS | Old median wall | Streaming median wall |
|---|---:|---:|---:|---:|
| 8 / 17.4 MB | 100.1 MB | 74.9 MB | 0.741 s | 0.782 s |
| 32 / 69.6 MB | 222.6 MB | 78.5 MB | 5.403 s | 3.083 s |
| 128 / 278.5 MB | 692.6 MB | 98.2 MB | 21.294 s | 12.221 s |

At the largest size, peak RSS decreased by 85.8%, wall time by 42.6%, and
user CPU from 21.05 s to 11.98 s. Output files changed from 1/2/7 to 1/4/15;
output bytes changed from 3.74/10.20/39.07 MB to 3.66/14.65/54.99 MB. The largest
group therefore used 40.7% more storage on this repeated-file fixture. Bounded
row groups reset dictionaries sooner than the old whole-group representation.
The final design improves the initially rejected small-target output explosion,
but it does not promise identical compression or file counts.

At a 32 MiB encoded target, both versions produced one part for each size:

| Input files | Old peak RSS | Streaming peak RSS | Old median wall | Streaming median wall |
|---|---:|---:|---:|---:|
| 8 | 100.4 MB | 74.7 MB | 0.749 s | 0.782 s |
| 32 | 218.7 MB | 83.7 MB | 2.766 s | 2.898 s |
| 128 | 683.5 MB | 120.4 MB | 10.573 s | 11.296 s |

For the largest group, RSS decreased by 82.4%, while wall time increased by
6.8% and user CPU increased from 10.40 s to 11.09 s (6.6%). The extra decode
pass has a visible cost where the old implementation can keep one output file.
Output size increased from 25.51 MB to 26.98 MB (5.8%). The smaller groups used
3.66/7.86 MB versus 3.74/7.97 MB. All 36 CLI runs across both target settings
passed exact schema/value/multiplicity checks.

These measurements show that retained decoded data no longer grows with the
whole target group. They do not establish a fixed RSS ceiling or preserve the
old compression ratio: input pages, dictionaries, codec allocations, allocator
retention, and path/footer metadata remain outside the simple byte target.
The 32 MiB active-row-group threshold is independent of the encoded target and
is not a new CLI default change in this PR.

Reproduction (the script acquires the shared lock internally):

```bash
python3 docs/audit/522-rollup-benchmark.py --before /path/to/before --after /path/to/after --flush-bytes 4194304
python3 docs/audit/522-rollup-benchmark.py --before /path/to/before --after /path/to/after --flush-bytes 33554432
```

## Alternatives evaluated

An initial implementation closed files on active-writer memory as well as encoded
size. At a 4 MiB target, the repeated-input stress fixture emitted 342 files and
325 MB for the largest group, versus seven files and 39 MB before. It was rejected.
Flushing only row groups at that same small budget reduced file count but retained
the compression loss. Fixed initial output-buffer preallocation also increased
RSS in measurement and was removed. The final independent 32 MiB row-group budget
keeps useful dictionaries while preserving a finite active-memory threshold.
