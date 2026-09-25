# Issue #516: bounded concurrency contract sketch

Status: proposed design only, reviewed against protected ingestion and #515.
Concurrency is not implemented by this record or the accompanying #525 refactor,
and #516 remains outstanding. Separate byte-admission and error/drain design
review is required before implementing stage A. No new public flags, live
requests or production writes are included.

## The old issue proposal is not directly safe on the current API

IngestionSession::flush owns a mutable session throughout controller commit and
then AcceptedFrontier::acknowledge requires its exact frozen snapshot. Mapping
later events while that future is pending would change the snapshot and violate
the explicit acknowledgement contract. A bounded channel alone does not make
this safe. Nor may a worker own another session: the common ownership/session
permit and authoritative transaction controller must remain unique.

There is one pending journal slot. Keep one Writing transaction at a time and
one ordered authority/mirror commit lane. Do not introduce multiple concurrent
transactions or new journal states merely to run table encoding/upload in
parallel. All-table validation/unoccupied proof must precede Writing, receipt
must be durable before final publication, and all finals must verify before
Committed -> authority -> mirror -> cleanup -> clear.

## Suggested staged implementation

A. First introduce bounded table work inside one controller commit, with unchanged
Session receive/freeze/ack semantics. A coordinator owns the current versioned
pending record and serializes receipt CAS. At most N blocking encoders run, and
at most M publications run; do not spawn all tasks then limit only awaits.
Encoded/staged parts wait behind byte-weighted capacity. The coordinator persists
each exact receipt before starting that part's publication. The authoritative
checkpoint and mirror move only after every started part finishes successfully
and complete final verification passes. This directly reduces serial table
latency without changing accepted frontier semantics or the journal format.

B. Decouple receipt/mapping from the single writer only with a separately reviewed
session/frontier extension. Keep logical receipt/acceptance ordered on one task.
A freeze operation creates an immutable window prefix and resets only the next
logical window digest; it does not move the durable base or advertise a saved
cursor. Maintain a bounded FIFO of frozen prefixes and acknowledge ONLY the
oldest exact prefix after the controller returns a committed receipt. A later
window's transaction is planned against the then-current authoritative predecessor
when dequeued, never a speculative checkpoint. Preserve unresolved received
lookahead envelopes and source/routing identity across freeze boundaries. Every
queued item owns its exact batches, prefix, partition metadata, schema inventory,
preflush sizing snapshot and trigger; it must not borrow a mutable mapper.

The writer lane stays within the lifetime of the outer DatasetOwnership and
Session. Prefer structured/concurrent futures that borrow those owners, not a
'static detached task or a second guard. Blocking encoders may own immutable
prepared batch/schema inputs; owner-bound I/O remains supervised. Introduce an
explicit borrowed session coordinator/worker boundary if necessary, rather than
wrapping the whole session in a mutex held across I/O. The public setup refactor
#525 can clarify these lifetimes but should not silently implement concurrency.

## Backpressure and memory

Bound raw receive backlog, frozen window count, encoded part count and bytes;
a count-only channel permits several 256 MiB windows plus Arrow/encoder copies.
Use a small default (one writing plus at most one queued frozen window) until
measurements justify more. Separate #515's mapper accumulation threshold from
writer backlog reservations. Measure Arrow buffer ownership, account shared
buffers without under-counting, and reserve estimated encoder/output workspace
before dispatch; do not describe these as a hard RSS cap. If one block/window
exceeds the admitted budget, specify either rejection before Writing or one
exclusive oversized item with an explicit overshoot diagnostic. Never allow a
stream of oversized exceptions to accumulate. Decide this compatibility policy
before exposing the mode.

Backpressure must reach gRPC reads when capacity is exhausted, not just move
unbounded data into another queue. Retain the accepted-event ordinal limit and
bound opaque cursor metadata too. With #518's owned Bytes, queue ownership must
not duplicate the same payload into Vec copies or keep complete source messages
after their required mapping/lookahead work ends.

Only an ordered successful controller acknowledgement trains #515. Apply its
actual maximum committed part bytes against that window's captured maximum
mapper estimate exactly once, in commit order. Already queued windows may have
used older calibration; document and measure that lag. Never train on encoded,
staged, uploaded-only or authority-advanced-but-mirror-failed results. New gauges
must distinguish mapper, queued Arrow, active encoders, encoded uploads and
committed data, and reset on task teardown.

## Errors, cancellation and shutdown

The first mapping/encoding/publication error stops admission, poisons the writer
lane and discards unstarted queued windows. Join/drain every already-started
blocking encoder and I/O operation before allowing guards or buffers to be
released. spawn_blocking cancellation does not stop a running closure. A dropped
future is not proof an S3 request has drained; each one-attempt mutation keeps
the existing uncertainty latch. No automatic S3 owner release/recovery, TTL
expiry or retry can replace provider-level request quiescence.

Keep at most one durable pending transaction, retain it on failure and reopen
through normal recovery. Do not perform eager rollback while any worker/request
can still publish an old part. No later FIFO item may commit after failure.
Completion requires closed input, drained successful writer acknowledgements,
no unresolved accepted/lookahead envelopes and the existing exact stop proof.
EOF alone still does not prove a sparse tail complete.

The first shutdown signal stops new receipt/admission. Already-started transaction
work must settle under ownership; unstarted queued and active mapper windows
are discarded and replayed from authority. Defining the exact dispatch point is
necessary: today a synchronous flush has already started when shutdown is
observed. Preserve this distinction, and never flush a newly queued partial
window solely because shutdown occurred. A second forceful exit retains today's
journal/recovery and remote-quiescence limits. Add a bounded operational drain
policy without treating a timeout as proof of safe ownership release.

## Qualification matrix

- Barrier-controlled workers: encode/upload finish out of order, receipts persist
  before publication, yet authority/mirror/acknowledgements remain FIFO.
- Block table A while B completes; confirm no checkpoint or sizing feedback before
  all parts and mirror succeed. Failure of the earliest transaction prevents all
  later commits; queued source cursors never appear as saved progress.
- Retain zero-row accepted windows, filtered UNDO between genesis prefix/lookahead,
  partition boundaries, repeated heights/non-final events and sparse EOF tests.
- Cancellation at encoder, receipt CAS, accepted-but-lost upload, authority-before-
  mirror and task drain. Prove guard retention, no late part after recovery, and
  no worker survives successful release. Explicit delayed old PUT reproducer must
  remain fail-closed rather than be explained away by stopping the task.
- Saturate count and byte budgets with highly unequal tables and one oversized
  block; assert producer backpressure, bounded admitted work, truthful metrics
  and ordered calibration despite acknowledgement lag.
- Compare fixed retained EVM/Solana/Beacon input and the same canonical dataset
  snapshot before/after. Report end-to-end local wall time, CPU, RSS, encoded
  bytes/file counts, queue high-water marks, time to durable cursor and exact row/
  schema/receipt equality. Local fake S3 can add deterministic latency to separate
  useful overlap from extra CPU. This is offline/fake-backend evidence, not live
  S3 throughput; any production backfill benchmark needs separate bounded approval.

Recommendation: implement and measure stage A first, then approve stage B's frozen
FIFO API and memory/stop policy explicitly. Both are useful, but neither a detached
writer nor join_all over all parts satisfies the current durability contract.
