# Partitions Parquet Contract

V2 indexes record finalized source coverage and contiguous routing spans. They
are consumed by `partitions resolve`, `ls`, `shard`, `validate`, the public single
range/window helpers, and `partitions build --resume` / `--live`.

## File layout

One file has one chain, partition type and interval, stored in metadata:

- `firehose-parquet.chain_name`: nonempty chain name.
- `firehose-parquet.partition`: `block_range`, `date`, `hour`, `minute` or `second`.
- `firehose-parquet.block_range_size`: positive width for `block_range`.
- `firehose-parquet.partition_coverage`: JSON v2 coverage record below.

The five existing columns remain; six proof columns are appended:

| Column | Arrow type | Nullable | Meaning |
|---|---|---|---|
| `partition` | UInt64 | no | Aligned block-range start or rounded UTC epoch seconds |
| `start_block` | UInt64 | no | Span start, inclusive |
| `stop_block` | UInt64 | no | Span stop, exclusive |
| `start_time` | Timestamp(second, UTC) | yes | Canonical first-block timestamp, without routing substitution |
| `end_time` | Timestamp(second, UTC) | yes | Canonical last covered block time (time spans), or optional boundary probe time (block ranges) |
| `complete` | Boolean | no | `start_complete && end_complete` |
| `start_complete` | Boolean | no | Natural left boundary established |
| `end_complete` | Boolean | no | Natural right boundary established |
| `first_observed_block` | UInt64 | yes | First canonical block in the time span |
| `first_observed_block_id` | Utf8 | yes | Its exact ID, paired with block number |
| `routing_start_timestamp` | Timestamp(second, UTC) | yes | Proven initial routing seed; distinct from nullable canonical time |

Block-range spans may omit canonical identity and routing seed. Time spans
require both. The existing UInt64 time-key format rejects pre-1970 partition
keys. No column is repurposed as a running timestamp maximum.

The coverage record contains `version=2`, `[start_block, stop_block)`,
`finalized={block_num, block_id}`, `routing_policy` (`block_number`,
`canonical_timestamp` or `solana_prior_timestamp`), first/last/next observed
canonical identities (`block_num`, `block_id`, `parent_num`, `parent_id`), and
`last_routing_timestamp`. Block-range coverage needs only the finalized bound;
time coverage also validates observed parent and anchor relationships.

Metadata, rows and proofs are read from one file/object snapshot. The verified
reader checks schema, coverage bounds, source-order continuity, identity links,
partition keys, boundary flags and policy. Writers validate before replacing a
target. Stored proof is trusted evidence from the index builder, not a signature
or independent verification of a maliciously edited file.

## Span and coverage semantics

Rows occur in source block order, reach the declared exclusive stop, and never
overlap. A time row is a maximal contiguous run with one raw-routing calendar
key. `[A, B, A]` produces three rows; calendar A has two disjoint spans. A complete
observed span has natural boundaries established within this finalized snapshot.
It does **not** claim globally complete coverage of its date: other occurrences
may be outside the snapshot or appear later.

A first span whose prior key cannot be established is incomplete. A bounded stop
inside a run is incomplete. A right successor beyond the requested stop does not
extend coverage and keeps the clipped row incomplete. A time span reaching the
current finalized head is incomplete until its next key transition is observed.
Block-range completeness follows its arithmetic natural boundaries.

Solana prior routing evidence may be needed when canonical time is null. A plain
numeric range cannot carry that unseen anchor into fresh ingestion. Strict
range consumers refuse it, except proven actual genesis with the shared seed.
Inspection preserves the seed and explicitly marks `routing_context_required`.

## Consumer behavior

- Default `resolve` and the single-bounds helper require exactly one complete,
  independently routable matching span. They expose the declared coverage.
- `resolve --all-spans --json` returns every matching complete span in block
  order, including identity/context evidence. It omits enclosing start/stop
  fields and never fills intervening holes. Incomplete matches still fail.
- The window helper selects calendar keys in `[from, to)`, then requires every
  selected span complete and consecutive in source order. It refuses an
  unselected intervening run and an unseen initial routing anchor.
- `shard` requires every selected row complete and independently routable before
  assigning rows. Each row retains its separate bounds. The stable calendar hash
  intentionally sends repeated keys to the same shard.
- `ls` sorts numerically by partition key, with source-block tie-breakers; its
  inclusive `--from` / `--to` filters remain unchanged. It reports coverage,
  completeness and prior-context requirements. This display order is not resume
  order.
- `validate` checks the entire v2 snapshot in source order, then reports selected
  incomplete counts. Structural validity is distinct from complete coverage.
  `--allow-gaps` cannot bypass a v2 coverage contradiction.

`build` still accepts explicit block bounds. It has no direct index-bound mode;
its previously removed partition-selection flags remain removed.

## Legacy migration

Files without v2 coverage metadata have unknown completeness. `ls` and legacy
geometry validation remain available and label that uncertainty. Strict resolve,
window, shard and resume refuse legacy files with a rebuild message. Use a fresh
output root or `partitions build --overwrite`; there is no default or optional
implicit `complete=true` migration. A legacy multi-chain file cannot be promoted
to single-chain verified coverage by applying a filter.

For readers compiled against the old Rust API, result structs now include
coverage. Resolve bounds are optional because all-spans JSON deliberately has no
single enclosing range. Use `write_verified_partitions_index` with a validated
model for v2 publication; the old public row writers still produce inspectable
legacy files. There is no lookup sidecar.
