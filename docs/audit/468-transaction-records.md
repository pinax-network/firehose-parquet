# Issue 468: transaction records and accepted-event frontier

This is the second private implementation foundation. The CLI still uses the
existing writer/cursor flow under the stage-1 common ownership guard. No partially
implemented transaction mode is selectable. Storage recovery, all-table commit,
main-loop envelope integration and maintenance compatibility remain required
before claiming crash/replay safety.

`firehose-parquet/src/ingest/state.rs` defines strict versioned semantic stream
descriptors, authoritative checkpoints and Writing/Committed pending records.
The descriptor binds the mapper semantic epoch, complete table/schema inventory,
encoding, filters, output and mirror identities, routing policy and immutable
original block-range anchor. Request stop and flush tuning do not create a new
stream. Recursive sorted JSON with versioned domain separation generates full
SHA-256 identities; opaque cursors serialize only into private records and are
redacted in Debug. Unknown fields, formats, unsupported encodings, malformed
paths, altered plans and incomplete table inventories fail validation.

Every flush plan explicitly includes zero-row tables. Nonempty tables derive
same-directory hidden temporary names and final `part-v1` names from the stream,
accepted ordinal range, transaction and sorted inventory entry. The complete
receipt freezes byte size and full-file hash independently of transaction
identity, allowing the controller to journal each encoded table before final
publication. Committed records require every receipt. Authority installation
requires its exact predecessor and next ordinal, so even a self-consistently
rehashed pending record cannot skip authoritative progress.

`ingest/frontier.rs` assigns ordinals when envelopes arrive, before filtering,
bootstrap buffering or mapping. Acceptance can finish out of order, but only the
contiguous accepted prefix advances. Empty or filtered events count as progress;
heights and fork steps do not stand in for delivery order. An unresolved earlier
envelope blocks the checkpoint even if later mapping succeeds. The ordered digest
is identical regardless of the order in which acceptance completes.

Each accepted envelope freezes its own routing checkpoint. A future received
timestamp source can be marked explicitly as lookahead for a genesis or Solana
leading prefix without claiming that source event was accepted. A future source
that was never received, or an accepted-prefix anchor beyond that prefix, fails.
Commit acknowledgement must match the exact frozen prefix; queued lookahead
envelopes survive acknowledgement. The unresolved queue and cursor sizes are
bounded, and ordinal exhaustion stops explicitly.

Validation: 16 focused model/frontier tests passed on macOS with Arrow/Parquet 60
and the current stage-1 integration. They cover zero-row commits, nonmonotonic
heights and fork steps, out-of-order acceptance, unresolved gaps, lookahead
provenance, exact acknowledgement, buffer/ordinal bounds, semantic identity,
canonical map ordering, schema/inventory/path rejection, immutable receipts,
strict decode/redaction, deterministic names and maximum ordinal component size.
These tests are protocol-model evidence, not end-to-end restart qualification.
The focused command was `cargo test -p firehose-parquet --lib ingest:: --locked
-j4`, through the shared whole-process Cargo lock. Formatting and diff whitespace
checks passed.

The typed `ingest/store.rs` adapter now connects these records to both durable
backends. Loading refuses orphan pending records and any state/journal pair
outside Writing-at-predecessor, Committed-at-predecessor or Committed-at-target.
Every transition checks its control version. A new journal starts without
receipts, authority cannot advance from Writing, and a committed journal cannot
be cleared before authority reaches its target. Physical verification, mirror
reconciliation and eligible-root initialization remain controller obligations;
the adapter does not treat missing state as permission to adopt legacy files.

The combined focused suite now passes 21 tests. Five additional tests reopen the
local records at every commit boundary, repeat the same transitions on a stateful
S3-compatible store, check tombstone recreation and stale incarnations, reject
stale receipt writers/orphan and inconsistent state, and confirm a remote owner
with unresolved mutations cannot continue writing control records. Remote tests
use an in-memory conditional backend and make no provider qualification claim.

Accepted identities additionally preserve `source_timestamp: Option<i64>` from
validated input, before synthetic routing. Missing Solana time stays missing.
Real routing anchors must match the block number, ID and time of an actually
received event or the exact saved authoritative anchor. A saved future lookahead
source must match when that ordinal is received after restart. The existing
Solana initial timestamp seed is represented by an explicit versioned
`solana_genesis_fallback` provenance with its exact constant and no claimed input
ordinal/ID. Tests reject forged source identities/times and altered fallback
constants; this avoids inventing source time merely to preserve partition routing
or a cursor-mirror timestamp.
