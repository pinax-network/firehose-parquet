# Partitions Build Defaults

`fireparq partitions build` creates one verified v2 snapshot in
`<output>/<chain>/_fireparq/partitions.parquet`, or in
`<output>/_fireparq/partitions.parquet` with `--without-chain-dir` (the root that
`build --without-chain-dir` writes, whose default cursor mirror
`_fireparq/cursor.parquet` then supplies a missing `--start-block`). It does not
change normal ingestion or writer timestamp routing. See [the file contract](partitions-parquet-contract.md)
and [the correctness decision and tests](audit/486-partition-index-design.md).

## Coverage and finality

Bounded builds require `--stop-block`, exclusive. The requested interval is never
expanded to a calendar boundary. Every row declares whether its start and end
are proven natural boundaries; clipped edges remain incomplete. A complete
observed span is not globally complete calendar coverage. A date can recur later
when canonical timestamps move backward.

Before acquiring output ownership, two Stream RPCs establish a finalized anchor:
`start=-1` obtains a candidate last-irreversible block number, then an exact
final-only request must return that candidate with `STEP_FINAL` and a nonempty
block ID. A shared five-second deadline and response limits bound the proof.
Candidate zero is handled without treating stop zero as a finite server bound;
the client accepts exactly one matching final response and drops the stream.
Values beyond the signed request range are rejected.

The exclusive coverage stop cannot exceed this anchor plus one. A server that
cannot supply the proof fails closed; an earlier Fetch reply is not finality
proof. Time snapshots reaching the anchor must end at its exact block identity.
Historical snapshots also record the first canonical successor and check its
parent identity. Missing or contradictory metadata never establishes coverage.

## Start and resume

Fresh bounded start resolution is:

1. Explicit `--start-block`.
2. The dataset's default cursor mirror `<dataset root>/_fireparq/cursor.parquet`,
   last block plus one, if present and readable. A mirror at another `--cursor`
   location (including the pre-v1.0.0 default `<dataset root>/cursor.parquet`)
   is not read; pass `--start-block` for such a dataset.
3. EndpointInfo's first streamable block.

A fresh live run uses its explicit start or endpoint first streamable block; it
does not inspect the cursor mirror. EndpointInfo is mandatory. An unreadable
bounded-start cursor is an error. Partition index
construction never writes that cursor.

An existing index requires `--resume` or `--overwrite` in bounded mode. Resume
and live mode require verified v2 coverage and use its source-block frontier,
last canonical identity and stored routing anchor. They do not consult the
cursor mirror or sort calendar keys to find progress. A bounded explicit start
past the frontier fails; an earlier start still resumes at the frontier. In live
mode an explicit start must equal the frontier. A bounded stop already covered
makes no change after endpoint/finality validation.

Legacy files have unknown completeness. Rebuild with `--overwrite` or into a
fresh output root; there is no compatibility switch that invents proof. A failed
rebuild scan leaves the prior file intact. Resume rejects changed chain, type,
interval, routing policy, same-height finalized identity or a first block that
contradicts a previously observed successor.

## Exact time spans

Time indexes stream every finalized canonical block in coverage. Each block's
parent number and ID must link to the prior observed block. The first covered
block cannot claim a parent still inside coverage, which would prove an omitted
block. Skipped slots are represented by the parent chain rather than guessed
from sparse samples. Endpoints that omit parent numbers/IDs cannot establish
this contract.

Raw canonical timestamp routing is preserved. `[A, B, A]` yields three maximal
contiguous runs; it does not become `[A, B, B]` through a running maximum. This
requires work proportional to covered blocks. Use successive bounded runs for
long backfills. A requested time interval with no canonical block is refused
rather than published as an empty or complete snapshot.

Solana missing timestamps use a verified prior timestamp, without changing the
nullable canonical timestamp columns. A fresh start follows at most 64 canonical
parents under a five-second deadline to obtain the prior anchor. Actual block
zero with its genesis parent can use the same genesis seed as ingestion.
Insufficient required context fails. Missing-time bootstrap on other chains is
also refused: borrowing a future timestamp can assign an initial block to the
wrong partition. Use `block_range` when a time-routing proof is unavailable.
The existing UInt64 time-key file format still rejects pre-1970 partition keys.

For historical coverage, one right-edge request spans at most 65,536 slot numbers
and has a five-second deadline. Its first final response must link to the last
covered block. A successor exactly at stop with a different key closes the last
span; a successor beyond stop proves a gap but leaves the clipped span
incomplete. The declared stop is never extended. Timeout, an empty response
window or an ancestry contradiction is an error, not evidence of chain head.

## Deterministic block ranges

Block-range keys are aligned multiples of `--block-range-size`. Coverage still
requires the exact finalized-head proof. Natural boundaries follow from the
block numbers; start/end flags record clipping. Nullable boundary timestamps
use the retained exact Fetch helper: timeouts/transient failures receive bounded
retries, missing metadata and unexpectedly later block numbers fail, and
authentication fails immediately. A missing boundary or an earlier Fetch reply
leaves its optional timestamp null; that legacy Fetch interpretation never
changes the separately proven coverage or establishes head/finality. These
optional timestamps do not determine range boundaries
or prove finality. Block ranges require no prior timestamp context.

## Live snapshots and publication

`--live` conflicts with `--stop-block` and extends coverage only to a newly proven
finalized frontier. `--poll-interval-secs` defaults to 30. Transient head-check and
scan failures keep the last published snapshot and retry from its frontier:
timeouts (including a stalled traversal message past its 5 s deadline or elapsed
bounded deadlines), exhausted boundary probes, transport errors and non-fatal gRPC
statuses. The retry delay is the poll interval doubled per consecutive failure,
capped at 300 s (never shorter than the poll interval) and reset after a
successful iteration. Fatal statuses (authentication, permissions, invalid
requests, decompression limits), missing blocks and contradictory proof fail.
Bounded runs fail on the first error.
An in-progress time span at the finalized head remains incomplete until a later
canonical key transition establishes its end. Full block-range boundaries can
be complete by arithmetic.

One fully validated snapshot is published after each bounded run or live
extension. Scans do not checkpoint partial evidence. Cancellation during a scan
preserves the last published file. The first shutdown signal cancels network
waits; a second signal can interrupt an in-flight index write. Common dataset
ownership guards reads and publication; storage errors retain the existing
ownership/recovery rules. This feature does not establish an ingestion-wide
crash/replay transaction.

## Output root

`--output` is required and accepts a local directory or an explicit
`s3://bucket/prefix` URI. Since #617, `--s3-bucket`/`S3_BUCKET` never supplies or
expands the output root: with a bucket option set, a missing or relative output is
rejected before any endpoint call (use `./path` or an absolute path for local
output), and a conflicting explicit S3 output is rejected. The existing index is
read from the exact destination, never through the read-only `S3_BUCKET`
shorthand, and the absolute index destination is logged before ownership is
taken. See the README's storage options for credentials and endpoint
addressing. The artifact path is `<dataset root>/_fireparq/partitions.parquet`;
there is no lookup sidecar.

## Legacy root index

Releases before v1.0.0 wrote the index at `<dataset root>/partitions.parquet`.
If that file exists, every mode (bounded, `--resume`, `--live`, `--overwrite`)
fails after the finality proof and before reading or writing an index, naming
both paths; ownership taken for the check is released. A new
`_fireparq/partitions.parquet` would otherwise silently shadow it. Nothing is
migrated automatically: move the file into `_fireparq/` (`mv`, or
`aws s3 mv` on S3) to resume from it, or delete it to rebuild. `build` refuses to
initialize a new dataset beside a legacy root index for the same reason.
