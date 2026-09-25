# Issue #525: behavior-preserving ingestion decomposition

Status: implemented and independently reviewed; publication/current-main
integration are pending. Work began from merged main `9eddfd4` after #515 and
#518 were merged. This refactor preserves protected ingestion and owned payloads.

## What changed since the issue was filed

Protected ingestion already has one authority/mirror commit owner:
IngestionSession -> TransactionController -> ProtectedMirror. There will be no
new commit_cursor operation. The final-only UNDO check is live: receive assigns
an ordinal first and accept_filtered advances a zero-row accepted prefix. Real
non-Solana genesis buffering, filtered envelopes between the prefix and its
lookahead, resumed lookahead identity checks, and Solana last-known routing are
required behaviors, not vestigial buffering.

The remaining problem is organization. run_ingestion spans about 1,190 lines,
with nested callback and process-block closures, duplicated window flush/log/reset
code, and scattered run/window counters. TimestampBackfill's always-zero buffer
accessors, ignored size argument, empty drain and impossible empty-ready branch
are proven no-ops; its actual anchor state must remain.

## Ownership and module boundary

Extract binary-private modules under blocks/src/bin/ingestion/ (mod.rs,
setup.rs, runtime.rs, routing.rs as size warrants). Shared public library/session
APIs, grpc request sequencing, mapper semantics, schema/epoch and journal formats
remain unchanged. Keep command dispatch, tracing/signal setup, metrics lifetime,
DatasetOwnership and final successful release in the outer orchestration.

Setup must remain two-phase so the borrow graph and recovery ordering stay clear:

1. Resolve endpoint/network, cursor template, Info and canonical output; validate
   final cursor storage and known mapper family before acquiring ownership.
2. Acquire DatasetOwnership in run_ingestion. Pass a borrow into setup to read
   authoritative resume, resolve original start/requested stop, mode/encoding,
   mapper/schema inventory and file metadata. Create metrics and IngestionSession
   in the outer scope, borrowing the owner. Recovery/mirror reconciliation and
   completed-bound no-op finish before any Blocks call. Reconstruct the Blocks
   client from final resolved Config, preserving all new transport settings.

Suggested data types are ResolvedEndpoint and IngestionSetup for owned resolved
configuration/metadata, and IngestionRuntime<'run, 'owner> borrowing
Option<&'run mut IngestionSession<'owner>>, metrics, shutdown and setup, while
owning mapper, routing/bootstrap state, filters, FlushWindow, FlushSizing and run
counters. No self-referential owner/session/runtime structure and no ownership
release from Drop. Drop runtime/session borrows before explicit successful
release; failed writes/errors retain the existing remote owner behavior.

## Runtime methods and ordering

- observe(payload: Bytes, type_url, cursor, identity, step): session.receive
  first; then final-only UNDO and below-start filtering through accept_filtered;
  lazy mapper resolution for reachable dry-run auto; routing/bootstrap handling;
  process each released block in ordinal order with exact lookahead ordinal.
- process_ready(block, lookahead): validate timestamp/routing, flush the previous
  partition before accepting this event, map through #518's owned payload method,
  update mapper gauges, accept_mapped, update counters/metadata, check shutdown,
  progress log, then evaluate #515's existing priority and thresholds.
- FlushWindow stores per-window block/time range, block count, last-flush time
  and partition identity; reset only at the same successful points as today.
- prepare_flush snapshots preflush estimates, materializes mapper batches and
  updates zero-buffer gauges. Shared commit-result handling learns #515's ratio
  only from successful exact physical receipts and updates existing metrics.
  Preserve synchronous session.flush_blocking inside the grpc callback and async
  session.flush at EOF; do not introduce nested runtimes or alter I/O scheduling
  just to force one async signature. Reuse shared preparation/completion helpers.
- finish(exit): errors/shutdown discard uncommitted buffers; only clean EOF flushes
  accepted data, rejects unresolved genesis, then complete_request validates the
  exact bounded stop. Preserve bounded non-final warning and final metrics.

Keep legacy dry-run cursor validation/lazy auto in a named compatibility helper.
Remove only the identity resolve_ingestion_stop_block and encoding alias, replacing
calls with their existing values/real resolver. Rename TimestampBackfill to a
routing-specific name and return one routed block, preserving anchor restoration
and provenance; the separate actual genesis queue stays. Test-only legacy writer
helpers can be deleted only after mapping their assertions to retained production
coverage; otherwise retain them in the first refactor.

## Review-sized sequence

1. Remove proven no-ops and rename routing state; retain all behavior/tests.
2. Extract setup and explicit run/window state without changing callbacks.
3. Move callback/process work into Runtime methods and share flush preparation
   and successful-result handling. Keep explicit ordering comments at receive,
   partition flush, acceptance, commit, completion and ownership release.
4. Move tests to matching private modules only where required; document exact
   evidence and before/after responsibilities. No unrelated cleanup/performance
   or flush policy changes in this PR.

## Coverage and no-behavior-change acceptance

Run existing full workspace, CLI, examples, build, formatting and completions.
Focused cases include all nine ingestion transaction tests (fresh/same-bound,
extension from authority after deleted mirror, refusal before Blocks, filtered
UNDO zero-row completion, sparse EOF, SIGKILL unflushed replay, genesis happy and
malformed anchor, compressed receipt adaptation and summed-memory trigger),
metrics readiness/reset, shutdown, non-final stream and session maintenance-order
checks. Preserve complete schemas/row values for all eight mapper families.

Use retained raw ETH fixture(s) and local simulated grpc only; no new live calls.
Capture a baseline binary after both #515/#518 merge. For strict Parquet byte
comparison, snapshot and restore the same initialized dataset at the same
canonical output path with the same stream/checkpoint/transaction identity;
serve the same opaque scripted cursors and raw payloads with fixed flush_blocks
(to avoid wall-clock interval boundaries). Compare exact final data bytes and
normalized authority semantics before/after; compare mirror columns excluding
operational updated_at/control revisions that are designed to vary. A second
new-root run cannot legitimately be byte-identical because root binding changes deterministic
stream/transaction digest footer metadata, so report that qualification boundary explicitly.

Also compare logical schemas, ordered table rows, part counts and committed
frontiers from fresh output. Add only missing regression cases revealed by the
coverage map; avoid tests that merely mirror extracted methods. A changed
control/cursor write count, request cursor, file boundary or accepted ordinal is
a regression to resolve, not a refactor simplification.

## Implementation checkpoints

Stage 1 removes only proven no-ops: the identity stop resolver and encoding alias,
ignored routing buffer limit, always-zero accessors, empty drain and unreachable
empty-ready branch. `TimestampRouting` now returns exactly one routed block;
last-known/restored/genesis anchors and the separate real genesis queue remain.
The two alias-only stop tests and empty-drain-only test were removed with their
no-op subjects; all routing value assertions remain. Validation on base `9eddfd4`:
176 binary tests plus nine transaction, one metrics, one non-final-stream and one
shutdown integration test passed. A baseline binary from that unmodified base is
retained for comparison; final qualification will use the final integration base.

Stage 2 extracts `ResolvedEndpoint` and `IngestionSetup` into the binary-private
`ingestion/setup.rs`. Ownership remains in outer orchestration; the endpoint
phase resolves complete scopes and the second phase reads resumed authority only
after acquisition. Metrics registration and final-config Blocks client creation
retain their order. The callback and completion tail are unchanged apart from
owned setup-string borrows. Independent review found no ordering change; all
176 binary tests and the same 12 integration checks passed on merged main
`955b8b2` (188 total). Runtime method/window extraction follows separately.

Stage 3 introduces explicit `MapperState`, `FlushWindow`, `RunStats` and
`IngestionRuntime` in `ingestion/runtime.rs`. The stream callback delegates to
`observe`; lazy dry-run setup, routed-block mapping, progress and completion are
separate methods. Shared flush preparation keeps preflush estimates and gauge
reset timing; blocking regular/partition commits and async EOF commits share
success-only physical receipt feedback. Only `IngestionSession` advances durable
state. Outer orchestration keeps and releases ownership after runtime/session
borrows end. The same 188 focused checks passed; current-main integration and
cross-binary raw-fixture qualification follow before publication.

## Offline exact-output qualification

The opt-in integration test
`retained_evm_replay_matches_baseline_bytes_authority_and_mirror` runs actual CLI
processes against a local cursor-aware Firehose server. It uses the checked-in
`blocks/tests/fixtures/evm-mainnet/block.pb` (height 26,049,575, SHA256
`dad74257d32c66a056404add2a5f3360c288faedf0a026e5698394cd9d231b2a`)
and one explicitly synthetic predecessor to establish an initial durable prefix.
The fixture is not repeated to make a throughput claim.

The baseline is pristine main `11ac02c55f8d2af7dce90f57cf957042d2f24343`, after
#519's shared Parquet properties and explicit compression semantics. Its local
macOS debug binary SHA256 is
`90cb57ce578844f8be172ee239439fad84a771a73d81c26b02ca4622e7c13822`.
The comparison tree also includes main `e4f990f` (#523 maintenance changes), which
does not change this local ingestion mapper/codec/property path.

After each child exits, the test restores the initialized prefix at the **same
canonical output path**. It preserves source cursor, descriptor, checkpoint and
transaction identity; it does not pretend two different roots should have
identical identity-bearing footers. The test checks:

- All 13 part paths, SHA256 values and complete physical bytes are identical.
- Every complete Arrow schema and typed row is identical: 5,050 rows, comprising
  the retained 5,049-row block and one seed row.
- The complete authority payload and full state record bytes agree, including
  the revision/checksum; an extra authority write cannot hide behind an equal
  checkpoint payload. Neither run leaves a pending transaction record.
- The cursor's field definitions and every row column except operational
  `updated_at` agree. Its versioned footer envelope binds that time and is not
  treated as a byte-equality target.
- Both extensions request the same authoritative cursor. Repeating the completed
  request preserves authority and makes no additional Blocks call.

Independent review of the harness confirmed the same-path restore, exited-child
ownership boundary, cleared environment, cursor-aware request plans and
nonempty row assertion. This is one real EVM payload and a fixed block-flush
boundary. Other chain, routing, failure, bootstrap, memory/size trigger and
shutdown behavior is covered by the preserved unit/integration suite. No new
live-provider or production S3 requests were made.

To reproduce, build a pristine baseline from the stated commit and retain its
binary outside the shared target directory; then run from this branch:

```sh
FIREPARQ_BASELINE_BIN=/absolute/path/to/baseline-fireparq \
  cargo test -p blocks --test ingestion_transactions \
  retained_evm_replay_matches_baseline_bytes_authority_and_mirror \
  --locked -- --ignored --nocapture
```

The test is ignored in ordinary CI because it requires a separately built
baseline; the existing nine transaction tests continue to run normally. Use the
same Rust toolchain/dependency/schema/property base when renewing the comparison.

## Combined validation

On the tree containing current main `e4f990f` and the runtime extraction:

- Workspace: **1,058 passed, zero failed, 12 intentionally ignored**. The new
  cross-binary qualification is one of those opt-in ignores and was also run
  explicitly: **one passed**, including full authority-record byte equality.
- CI capture example: **one passed**, one subprocess fixture intentionally
  ignored. Binary build, formatting, diff checks and bash/zsh/fish completions
  passed.
- Independent setup, runtime-ordering and parity-harness reviews found no
  blocking issue. Existing real-CLI coverage includes restart/replay, filtered
  UNDO, malformed/lookahead genesis, sparse EOF, compressed-size feedback,
  summed-memory thresholds, metrics/reset, non-final recurrence and shutdown.

The three removed tests asserted only the deleted identity/empty-return wrappers;
no mapper, routing-value, durability or actual CLI regression was removed.
