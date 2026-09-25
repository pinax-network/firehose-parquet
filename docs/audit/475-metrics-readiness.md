# Metrics and readiness (#475)

Issue: <https://github.com/pinax-network/firehose-parquet/issues/475>

## Diagnosis and implementation

The file counter used a new `partition` label for every directory. It now uses
only the existing table names. Review also confirmed that the Prometheus client
appends `_total` itself; most counter registrations had already included that
suffix, producing `_total_total`. Normalize their registrations and document
the query migration. The already-correct cursor failure counter is unchanged.

The named rolling-rate gauges were cumulative averages updated only with
progress logs. Remove them and document `rate()` over normalized counters.
Remove the two Solana backfill gauges: the current prior-anchor routing
implementation has no such buffer. Retain accurate writer buffer metrics from
the #477 simplification, label their scope explicitly, and add mapper row/byte
and real initial timestamp-bootstrap gauges. Updating a mapper estimate inspects
its builders without flushing or altering rows. Flush/reset paths update these
gauges even when a following write or checkpoint fails. A loaded cursor initializes
the checkpoint gauge; save counters still describe this process's actual writes.

Track stream activity with a monotonic clock. Readiness requires a valid checked
message, a connected stream, and age below the configurable positive threshold
(120 seconds by default). Reconnect paths immediately clear readiness. An owned
stream guard clears readiness on every return/cancellation path. A separate
pipeline guard holds liveness through final mapper/writer publication and cursor
persistence. Its drop marks the pipeline stopped. This separation was added
after independent review caught the risk of reporting liveness failure during
legitimate finalization.

The last streamed timestamp preserves negative/epoch values and uses `NaN` for
absent time. Scrapes refresh monotonic message age/elapsed time and wall-clock
block age. Block age is not a remote-head measurement; no new production RPC
is issued for metrics. A historical backfill can be ready while its block age is
large. Recent repeated messages also do not prove forward progress; operators
can combine these metrics with processed-block rates and saved cursor movement.

The existing reconnect implementation already has one metric update per retry
path after earlier lifecycle fixes. Preserve its counter behavior and regressions;
clarify that it counts scheduled retries, not only successful reconnections.

## Validation

- Registry tests assert exact single-suffix counter names and absence of removed
  gauges. A real writer crosses three minute partitions and emits one file
  counter series while writer buffer rows/bytes reset after each publication.
- HTTP tests exercise startup, fresh/stale messages, reconnect, stream end,
  continued liveness during finalization and pipeline stop. Monotonic boundary
  tests avoid timing sleeps; missing/negative/future block-time cases preserve
  unknown values and clamp negative age without affecting readiness.
- `blocks/tests/metrics_readiness.rs` runs the real CLI against a local gRPC
  endpoint and a persisted cursor at block 99. It verifies that the loaded cursor
  is exposed before messages, one mapped block remains visible in mapper buffers,
  a quiet stream becomes unready while remaining live, and a second block flushes
  both blocks, resets the buffers, exposes the table-only file counter and saves
  cursor 101. The historical timestamps do not make an active backfill unready.
- The first CLI fixture used a cursor with `extended=false` against the existing
  `extended=true` default. Startup correctly refused the mismatch; the fixture
  now uses matching pipeline settings. No production behavior was weakened.
- Focused metrics/writer/cursor checks passed (14 tests), the complete CLI
  regression passed, and all workspace targets compiled on 2026-09-25.

Full current-main validation and the final lifecycle outcome are recorded before
merge. The artifact names and endpoint semantics are intentional monitoring/API
changes; no chain schema, partition routing or persistence ordering is changed.
