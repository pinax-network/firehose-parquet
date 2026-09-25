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
