# Partition index correctness design (#486)

Status: contract approved; implementation is in progress in reviewable stages.

Issue: <https://github.com/pinax-network/firehose-parquet/issues/486>
Base: `0b38efcf1eb417503cc0be15b4901a359019e75f`.

## Reproduced contract conflicts from code inspection

- `PartitionIndexBuilder::observe_block` closes a row on any key change,
  including a backwards change. Sparse exponential/binary boundary discovery
  does not observe all blocks. For keys `[A, B, A]`, sampling blocks 0 and 2
  falsely suggests one A interval. Applying a running maximum to only those
  samples cannot recover B.
- Ingestion partitions by each block's routing timestamp. The writer also checks
  every non-null canonical timestamp against that destination (#477). A full
  running maximum over `[A, B, A]` gives `[A, B, B]`, so applying it only in the
  indexer still disagrees with ingestion and writer validation.
- A global running maximum cannot be initialized from an arbitrary fresh start:
  an earlier unobserved timestamp may exceed the seed. It requires replaying a
  prefix or a verified persisted prefix anchor, plus a versioned ingestion and
  cursor policy.
- Solana null-time probes currently borrow a subsequent timestamp, whereas
  ingestion uses its preceding routing anchor (or its bootstrap policy). Exact
  index generation must share that policy or explicitly refuse uncertain seeds.
- Fetch returns the current canonical block; its checked-in protocol does not
  say that every returned block is finalized. Earlier replies or NotFound do not
  prove a finalized head. Existing live logs describing these as finalized are
  stronger than the protocol evidence.
- `snapshot` and `finish` currently serialize the active row with no completeness
  marker. Legacy files cannot reveal whether their row bounds came from complete
  observations, sparse assumptions or a live partial checkpoint.

## Recommended scope

Preserve canonical/raw timestamp directory routing. Do not introduce a global
running-maximum policy in this issue. Make time index coverage exact within an
explicit finalized snapshot, with disjoint spans when one partition key recurs.
This avoids changing historical directory placement or requiring an unknown
prefix maximum. It also means the old sparse date-discovery performance promise
must be removed for chains without a separately justified monotonic guarantee.

1. Read a bounded head witness with the documented negative stream start near
   head and normal NEW/FINAL metadata. Use its `lib_num` as a candidate and require
   an exact STEP_FINAL response in a second final-only Stream (specified below).
   Apply response count/deadline/cancellation limits; missing metadata, invalid
   steps/identity, unsupported RPCs or inconsistent bounds fail closed. Never
   use guessed u64 sentinel behavior or missing-slot counts as finality proof.
2. Reject a bounded requested stop above the known finalized exclusive bound
   before writing any index. Block-range partitions remain deterministic and can
   be complete when the full aligned interval is below that bound. Nullable
   timestamp columns describe absence only, never availability/finality.
3. Time-based discovery observes every finalized block in the declared coverage
   range (a bounded stream), preserving actual routing keys and source block
   order. A backwards key starts another span for that key. No sparse samples
   claim exact interiors. The range does not silently expand without a bound.
4. Version the artifact and record coverage start/frontier, a finality witness,
   routing policy and row `complete`. A span is complete only when its left and
   right boundaries have been observed/proven inside the snapshot; an open live
   span and a clipped initial/final span remain incomplete. Completeness means
   exact coverage within this finalized snapshot, not that future chain blocks
   can never revisit the same timestamp partition.
5. Default resolve/window/shard/build-bound consumers refuse incomplete or
   unknown legacy rows. Resolving a single contiguous span retains the existing
   bounds shape. A multi-span result must never become a min/max superset: expose
   ordered disjoint spans in JSON, and make consumers that only accept one range
   fail with an actionable message. Supporting ingestion of multiple selected
   spans automatically would require a separately explicit cursor/resume design.
6. Legacy indexes remain inspectable but cannot become complete by adding a
   default `true`. Resume requires the new verified coverage/frontier metadata;
   otherwise require rebuilding into a separate root (or explicit overwrite).
   Resume follows block-order coverage, not the greatest partition key. Live
   snapshots preserve closed spans and an explicitly incomplete active span.
7. Share the existing nullable timestamp routing policy with index discovery.
   A fresh clipped range must prove/restore the required preceding routing anchor
   or refuse time indexing and suggest block-range indexing; borrowing a future
   timestamp must not silently change directory semantics.

## Planned bounded tests

- Unsampled interior `[A,B,A]`, backwards crossings, same-key timestamp decreases
  and a fresh mid-chain prefix higher than the seed; compare every produced span
  with actual ingestion/writer destinations.
- Missing timestamps before/after an anchor and initial ranges with no proven
  anchor; no future borrowing or epoch substitution.
- Complete and open live spans, partial bounded edges, resume after a backwards
  key, and multiple intervals for one key; no min/max range broadening.
- Missing/false completeness on legacy indexes: list remains readable, default
  resolve/window/shard/build and unsafe resume refuse them.
- Local RPC fixture with proven finality below the requested stop, NotFound at
  head, stale witness, malformed metadata, delayed head and interruption. No
  output/cursor creation or replacement on rejected bounded builds.
- Source/type checks for new Arrow fields, local/S3 metadata round trips, and
  current workspace suite. No broad live history scan is needed or authorized.

## Decision requiring review

The suggested multi-span JSON contract and strict refusal in single-range
consumers preserve existing ingestion routing, but change time-index semantics
from a presumed globally unique partition interval to snapshot coverage. If
#486 instead requires every partition key to have one globally stable contiguous
range, this must become a versioned running-maximum ingestion/cursor/writer
feature with a proven prefix anchor; it cannot be implemented only in the index.

## Proposed concrete contract for review

### Meaning of coverage and completeness

A v2 file declares `[coverage_start, coverage_stop)` in block-number space and
records the identity of a proven finalized anchor at or above its last covered
block. It contains all canonical blocks/spans observed in that interval, not a
claim about timestamps outside it or future blocks. Rows are maximal contiguous
runs of the existing ingestion routing key. Repeated calendar keys are valid
separate runs. Every row's `complete` is the conjunction of two proven run
boundaries; clipped initial/final spans and open live spans are false. A complete
run is not a globally complete calendar date. JSON always includes coverage and
finality metadata so a result cannot silently look like an all-history answer.

`partitions resolve` defaults to one complete contiguous interval, as today’s
consumers require. It rejects any matching incomplete/unknown row and rejects
multiple disjoint spans rather than returning their enclosing min/max range.
An explicit `--all-spans` JSON mode returns all matching complete spans in source
block order, with the declared coverage and the same default completeness check.
A window may merge adjacent selected spans only when their source intervals
really touch; any unselected gap prevents a single-range result. Sharding and
build-bound resolution use the same strict reader contract. Read-only listing
can expose incomplete/legacy rows, labelled as such; it does not upgrade them.

Legacy files without the version, coverage/finality evidence and completeness
column remain inspectable, but default resolution and resume fail closed.
Rebuilding is the migration. There is no implicit `complete=true` fallback and
no trusted routing anchor inferred from legacy sparse rows or a cursor merely
because it contains a timestamp.

### Bounded protocol proof of finality

The head proof uses the Stream protocol rather than provider-specific Fetch
fallback behavior:

1. Request a near-head normal stream with documented `start_block_num=-1` and
   `final_blocks_only=false`. Within a deadline and a small response-count cap,
   accept a NEW/FINAL response with valid metadata and nonempty identity. Read
   its `lib_num`, and require it not to exceed the witness block number. This is
   a conservative advertised finalized-height candidate, not itself a proof
   that a requested historical Fetch response was final.
2. Request exactly that candidate using `start_block_num=L`,
   `stop_block_num=L` (inclusive in the protobuf), `final_blocks_only=true`.
   Require a STEP_FINAL response for exactly L with valid nonempty identity.
   Record that anchor's number and ID. A timeout, missing metadata, wrong step,
   earlier/later number, unsupported RPC or unavailable candidate aborts the
   proof. Both streams are dropped immediately after their bounded purpose.
3. A bounded build requires `requested_stop <= L+1` using checked arithmetic;
   otherwise it fails before replacing or creating an index. A stale proof may
   reject a range that has since finalized, which is safe; it must never expand
   itself based on NotFound, earlier Fetch replies, or retry exhaustion.

Exact range discovery also requests finalized stream responses. Consecutive
returned block identities must link by parent number and ID; this detects
omitted canonical blocks while allowing real skipped slot numbers. At the
snapshot head, the final observed identity must match the proven anchor. For a
bounded stop below that anchor, a bounded lookahead to the first finalized block
at/after the requested stop proves the preceding gap/end and supplies the right
run-boundary comparison. The lookahead is capped by the anchor, a height budget
and deadline; exhaustion cannot mark a span complete. No scan silently extends
from a requested historical stop all the way to current head.

### Mid-chain and nullable timestamp anchors

Non-null raw timestamps require no prefix maximum. To prove the left edge of a
fresh run, inspect the first covered block's canonical parent: use its number
and ID from the finalized response, fetch that exact parent identity and compare
the parent ID. Ancestors of the proven finalized block are final. If the parent
is unavailable or has the same routing key, the initial run is clipped and
incomplete; do not search backwards without a bound or invent its earlier start.

For Solana null timestamps, share the existing prior-anchor rule with ingestion,
not the current future-borrow rule. A new mid-chain range whose first covered
block needs an anchor follows those canonical parent links backwards with a
small explicit budget/deadline until it finds a nonzero timestamp. A v2 resumed
index can instead restore its verified last-known routing timestamp and frontier
identity. Reaching the actual genesis permits the existing genesis seed; an
endpoint with truncated history is not evidence that no earlier anchor exists.
If the needed anchor cannot be proved, refuse time indexing and suggest block
ranges. A null timestamp remains null in canonical rows; this is routing context
only. If selecting an indexed span for fresh ingestion would require an external
anchor, the resolver must pass that verified anchor explicitly through a typed
routing context or reject that single-range ingestion path; it must not rely on
an unverified default genesis seed.

### Verification boundaries

The implementation will extract shared routing policy only where needed to make
index and ingestion semantics identical; it will not rewrite canonical timestamp
columns or weaken the writer's per-row destination checks. Finality and coverage
logic will be exercised with local RPC fixtures, including intentionally omitted
blocks, skipped slots, a stale/wrong head proof and bounded lookahead failure.
Only tiny explicit live ranges may be used for later qualification. No live
prefix reconstruction or broad history scan is part of this work.


### Approved edge refinements

- If the first covered response names a canonical parent still inside declared
  coverage, a block was omitted: reject the traversal. This is distinct from a
  legitimate skipped-slot gap whose parent is below the requested start.
- The declared stop never expands. A finalized witness after stop may establish
  that the remaining slot numbers are empty, but a run clipped by stop stays
  incomplete. Only a witness exactly at the declared stop with a different key
  can prove that the preceding run's natural boundary equals the stop.
- For non-Solana initial missing timestamps, share or explicitly reject the
  existing ingestion bootstrap that borrows its first following anchor. A time
  index must not silently apply a different synthetic policy. Context-dependent
  clipped starts that cannot reproduce the same routing must fail closed.
- Candidate LIB zero is valid. The protobuf uses stop zero as an unbounded
  sentinel, so the proof client enforces the exact first STEP_FINAL(0) response
  and drops that stream immediately. Candidate values above i64::MAX are
  rejected before constructing a signed stream start.

## Stage 1 validation

The bounded protocol proof is implemented in `firehose-parquet/src/grpc/finality.rs`.
Five local gRPC tests passed on 2026-09-25, covering normal/L=0 success and channel
reuse; malformed/earlier/later/non-final proof responses; invalid head metadata,
response-budget and signed-range overflow; header/message deadlines and shutdown
in either RPC; and unavailable/empty final streams. They assert the exact negative
head request and exact final-only candidate request, including authentication.
No live endpoint was contacted. Log: `/tmp/fireparq-486-finality.log`.

## Stage 2 validation

The v2 model and Parquet serialization preserve the five existing index columns
and add explicit boundary flags, first-span identity, and the routing seed. The
footer declares finalized coverage. Metadata, rows and proofs are read from one
file/object snapshot. The verified reader refuses legacy files; the inspection
reader remains available without inventing coverage. Writes validate coverage,
identities, partition keys and footer consistency before touching the target.

Eight model/round-trip/error tests, seven existing index-write tests and two
existing index-read tests passed on 2026-09-25. These include repeated calendar
keys in source order, incomplete edges, malformed flags/identity columns,
conflicting metadata, and preservation of an existing target after rejection.
Log: `/tmp/fireparq-486-model.log`. Runtime builders and strict command consumers
are not yet wired to this model; this stage alone does not fix #486.

## Stage 3 validation

An exact time-span builder now validates every finalized canonical parent link,
keeps backward/repeated timestamp runs, leaves clipped and current-head edges
incomplete, and appends resumed coverage by source order. A separate bounded
metadata stream reads the declared range and at most one right-edge witness;
the latter request is capped at 65,536 slot numbers and a five-second caller
budget. Parent context follows at most 64 verified parent identities within the
same deadline. Solana missing times use the proven prior anchor; non-Solana
missing-time bootstrap is explicitly refused for time indexes.

Seven local gRPC tests and all thirteen model/builder tests passed on
2026-09-25 (`/tmp/fireparq-486-scan.log`). Tests exercise exact request bounds,
backward timestamps, actual parent RPCs and ancestry mismatch, a 64-parent budget,
missing metadata, wrong finality/identity, omitted blocks, skipped slots,
genesis zero, future-stop rejection before any RPC, and header/message/witness
cancellation and deadlines. No live endpoint was contacted. These are validated
building blocks; runtime command wiring and strict consumers remain in progress.
