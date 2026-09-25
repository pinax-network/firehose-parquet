# Protected ingestion session assembly (#468)

This record began at the private assembly checkpoint. The runtime integration
now uses the session for every non-dry-run build; see
[the current runtime contract and qualification](468-ingestion-runtime.md).
The historical focused results below qualify the assembly layer separately.

A session freezes the exact schema inventory obtained from an empty mapper flush,
resolves the output and optional mirror bindings, and reserves the output owner's
unique protocol permit. It rejects legacy output before creating a new root or
authority. A new local root is created only after eligibility, with canonical and
lexical ancestry durability. Existing authority selects CLI compatibility defaults;
the optional cursor file never selects the resume point. An explicit different
start, cursor override or changed mirror binding is refused for protected output.
Full descriptor equality is checked again by the controller before recovery.

The descriptor binds effective mapper family, byte encoding, EVM extended mode,
Solana vote mode, failed-transaction handling, original block-range origin, routing
policy and all table schemas. New source receipts bind the actual block identity,
fork step and optional source timestamp before synthetic routing. A different
payload family stops the session. Mapping acceptance must resolve received order;
filtered UNDO/below-origin events are explicit zero-row acceptances. Filters queued
behind a buffered prefix inherit its routing when the contiguous gap drains, so
they cannot replace an observed anchor with an older snapshot.

Routing anchors are assembled from received source facts. Solana missing times
use the explicit genesis-policy seed until an observed timestamp is accepted;
lookahead is forbidden there. Non-nullable-chain bootstrap anchors must identify
a future received envelope. A persisted lookahead anchor remains usable after
restart before its source envelope is re-read, and that re-read must match the
stored source identity/time. Direct routing cannot alter actual source time.
The controller can commit an all-zero table inventory without inventing a time
partition directory.

A successful flush acknowledges exactly the frozen contiguous frontier after
all-table publication, authoritative checkpoint update and mirror reconciliation.
Any unresolved operation poisons the session. Same-bound completion is a true
no-op; an extension retains the original partition origin. Cursor gauges on
resume come from authority even if mirror reconciliation does not write. Table
file/row/byte counters advance for successful logical commits, and writer buffer
gauges cover owned prepared batches and clear on return/cancellation.

Validation: the combined private ingestion suite passed **68 tests**, with one
intentionally ignored subprocess child fixture exercised by its parent crash
test. Seven session tests cover new-root publication and deleted-mirror repair,
completion no-op, legacy/refused semantic changes, authoritative CLI defaults,
lookahead restart before source receipt and mismatched source rejection,
filtered-event anchor inheritance, Solana seed provenance, invalid mapper
family/time/inventory, and S3 session initialize/commit/reopen with one borrowed
in-memory owner. Existing CLI shared-index-reader tests separately passed
**145 tests** after the async eligibility decoder extraction. This is hermetic
protocol evidence, not production S3 backend qualification.

Runtime integration now calls `validate_ingestion_target` before eligibility or
initialization, `validate_ingestion_recovery_order` before controller recovery,
and borrowed-owner `prepare_ingestion` afterward. The reserved session permit
spans all three and stays held throughout ingestion. Receipt ordinals travel
through the real bootstrap queue and the bounded non-final warning remains after
successful completion. Standalone partition-index publication uses the common
Artifacts policy with an explicit file destination.
