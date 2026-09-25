# #468 implementation plan: an owned ingestion transaction

Design accepted for staged implementation, 2026-09-25. This document describes
the target protocol, not implemented guarantees. It refines
[the original crash-recovery draft](468-crash-recovery-design.md)
against #477's complete mapper-flush ownership and #578's atomic local part
publication. The inspection checkout is `880777b44dce48c6debdf56a9473049eada18396`:
main at `c88abcca1199cb8a919d504b751dd7727c0bf047`, #578 implementation
`7301f755eeaa1482036b406497b56e9368ad82ee`, and #477 implementation
`bcbadaa142c085a281b825b721f3be2ea884c824`. The design checkout now incorporates
main `73257c2eb4a0b03981599b28b99e38cb428ffd98`, including #477, #473 and #578;
#476's fallible routing must be included before ingestion integration. No
existing PR branch is modified by this plan. The protected ingestion default
stays disabled until stages 2–4 are complete; there is no selectable partially
safe ingestion mode.

## Proposed first release

Make `fireparq build` transaction-protected by default for a new dataset root.
One transaction owns an entire mapper flush, every table output, and its exact
accepted-event frontier. Persist the transaction plan before any final file,
record its commit before advancing the output-root checkpoint, and finish
recovery before the next Firehose **Blocks** request. Endpoint Info may be needed
first to resolve the canonical chain directory; it does not consume a block
stream. An offline recovery subcommand accepts an already resolved root.

Keep the public table/partition layout and ordinary `part-*.parquet` discovery.
Final parts are individually complete; concurrent globs can still observe only
some tables of a transaction. This is crash/replay safety under exclusive
ownership, not atomic multi-table query snapshots or repair of old duplicates.

The deliberately narrow compatibility choices are:

- One ingestion lane per dataset root. No parallel appends or backfills within
  that root, including apparently disjoint height ranges. Separate roots remain
  available; range-claim catalogs are outside this change.
- Local ownership uses directory inode locks for overlapping scopes. S3 uses
  **one persistent conditional ownership record per bucket**, without automatic
  expiry or takeover. Independent prefixes in one bucket serialize.
- Existing random-name datasets are readable and maintainable as legacy data,
  but are not silently adopted by protected ingestion. Rebuild into a new root
  for the first release. A future explicit adoption tool needs a separately
  reviewed baseline-verification contract.
- On protected datasets, allow lossless compaction through the guarded merge
  journal. Initially reject truncate and destructive/in-place rollup; their
  current operations cannot atomically reconcile the ingestion checkpoint.
  Read-only inspection and separately guarded metadata artifacts remain usable.
- `--cursor none` disables only the public mirror. The output-root checkpoint
  is mandatory. `--cursor-override` cannot rewind or reset protected output.

These restrictions must appear in CLI errors, README, and release notes. They
are not hidden implementation details or optional safety switches.

## Repository changes and APIs

| File/module | Concrete responsibility |
|---|---|
| `firehose-parquet/src/dataset_lock.rs` (new) | `OwnershipSet::acquire(scopes)`, canonical local scope reduction, local inode guards, bucket CAS guards, explicit release and operator recovery. No ingest/merge-specific lease code. |
| `firehose-parquet/src/ingest/mod.rs` (new) | `IngestSession::open`, `prepare`, `commit`, `recover`, `finish_request`; owns guards, state, pending plan, and all batches until acknowledgment. |
| `ingest/state.rs` (new) | Versioned descriptor, authority state, event frontier, routing checkpoint, canonical hashes, redacted formatting, strict path/schema validation. |
| `ingest/store.rs` (new) | Local and S3 durable metadata operations, conditional S3 writes, exact-path part receipts/checksums, bounded reads, deletion sync. Reuse existing S3 bucket/endpoint resolution. |
| `ingest/recovery.rs` (new) | Writing rollback / committed roll-forward state machine, with no mapper or Firehose dependency. |
| `writer.rs`, `writer/local.rs` | Extract #477's whole-batch partition validation; accept an explicit `PlannedPart` in the protected path. Split #578's helper into stage and publish operations without weakening its legacy helper. |
| `cursor.rs` | Public mirror encoding/loading and bounded retries; add checkpoint identity metadata and a redacted durable cursor representation. Mirror never chooses a protected resume frontier. |
| `blocks/src/bin/main.rs` | One accepted-prefix tracker and one transaction call at all flush sites; carry envelope identity through bootstrap buffers; restore routing from authoritative state. |
| `merge.rs`, `merge_journal.rs`, `rollup.rs`, `truncate.rs` | Common ownership at entry, protected-state discovery, ingestion recovery before allowed maintenance, protected destructive-operation refusal. Remove old S3 TTL/fallback ownership from active paths. |
| `verify.rs`, `cli.rs` partition-index writers, binary dispatch | Guard every actual artifact mutation, including registry fill when `--update-registry` is absent and custom report/output paths. Resolve all affected roots before reading a mutable snapshot. |
| `artifacts.rs`, collectors | Reserve `.fireparq-ingest/` and bucket ownership objects; reject internal control paths as direct destructive targets. |

Suggested interfaces (types describe ownership rather than final spelling):

```rust
struct IngestSession { /* guards, store, state, at most one pending flush */ }
struct PreparedFlush { /* all RecordBatches, frozen plan, accepted frontier */ }
struct CommittedFrontier { checkpoint_id: Digest, ordinal: u64 /* + resume */ }

impl IngestSession {
    fn open(resolved: ResolvedDataset, request: RequestedStream,
            cancellation: CancellationToken) -> Result<Self>;
    fn resume(&self) -> &DurableCheckpoint;
    fn prepare(&self, batches: TableBatches, prefix: AcceptedPrefix,
               metadata: BlockMetadata) -> Result<PreparedFlush>;
    fn commit(&mut self, flush: PreparedFlush) -> Result<CommittedFrontier>;
    fn finish_request(&mut self, completion: ProvenCompletion) -> Result<()>;
}

trait TransactionStore {
    // Versions are opaque; local comparison is under the held OS guard.
    fn read_state(&self) -> Result<Option<Versioned<AuthorityState>>>;
    fn create_state(&self, initial: &AuthorityState) -> Result<Version>;
    fn replace_state(&self, expected: &Version, next: &AuthorityState) -> Result<Version>;
    fn create_pending(&self, plan: &PendingTransaction) -> Result<Version>;
    fn replace_pending(&self, expected: &Version, next: &PendingTransaction) -> Result<Version>;
    fn remove_pending(&self, expected: &Version) -> Result<()>;
    fn verify_part(&self, part: &ExpectedPart) -> Result<VerifiedPart>;
    fn remove_owned_part(&self, part: &ExpectedPart) -> Result<()>;
}
```

Use the existing synchronous mapper callback and one well-tested async bridge
for S3/cursor operations. Do not convert every mapper or maintenance API to
async. `commit` is a bounded synchronous operation using the current runtime;
it rejects unsupported runtime contexts with a descriptive error instead of
panicking. It receives #473's cancellation token. Once an operation may have
changed durable state, cancellation leaves its journal for restart and ends the
session; it does not report successful acknowledgment or try a new filename.

Use the existing mapper `table_names()` inventory to include zero-row tables;
reject unexpected names and missing nonempty batches during preflight. The
transaction API must take ownership of the batch map. It does not call the
old `OutputWriter::flush_table`, whose successful per-table removals are useful
for its existing API but do not constitute an all-table commit. Both paths reuse
the same partition validator and low-level encoding/publication primitives.
Protected roots reject unguarded use of the legacy publishing entry point; a
guarded internal variant receives an unforgeable borrowed capability.

## Durable layout and identity

Under the resolved chain root:

```text
.fireparq-ingest/state.json     # descriptor + authoritative checkpoint, one atomic object
.fireparq-ingest/pending.json   # absent, Writing, or Committed
<table>/<partition>/part-v1-<stream>-<first>-<last>-<transaction>-<index>.parquet
<table>/<partition>/.fireparq-txn-<transaction>-<index>.tmp  # local staging only
cursor.parquet                # compatible optional mirror, possibly elsewhere
```

Use full SHA-256 IDs, checked decimal ordinals, sorted table names and stable
entry indices. A basename remains below local filesystem limits. Do not use a
process UUID, current timestamp, maximum block height, or completion order for
part identity. A retry with different flush thresholds is safe because recovery
resolves the old plan first; independent clean runs may have different layouts.

`AuthorityState` combines descriptor and checkpoint in one atomic object. This
avoids a descriptor-created/checkpoint-missing bootstrap ambiguity. It contains:

- Format version and checksum of a canonical, explicitly versioned serialization.
- Stream descriptor: canonical chain identity, resolved block family and byte
  encoding, mapper/schema epoch, partition mode and original block-range anchor,
  final/reversible mode, extended/vote/failed-transaction filters, and routing
  policy version. Include output storage identity and mirror binding, without
  credentials. Endpoint aliases for the same proven chain are operational config;
  an unrecognized chain or family requires an explicit, consistent choice.
- Checkpoint ID and predecessor, last accepted ordinal, exact opaque cursor and
  block ID/number/step, all cursor compatibility metadata, and `RoutingCheckpoint`.
  The initial checkpoint has ordinal zero and no cursor, with original start.
- Last proven bounded-request completion, separate from the immutable stream
  origin. Completion and a row-free accepted prefix can advance state without
  producing a data part. No completion is inferred from row count or elapsed time.

Compression, row/byte/time flush thresholds and logging are not semantic stream
identity. Freeze them in each pending plan. Stop bounds may be extended after a
completed prefix without changing the original partition anchor. Explicit start
changes cannot jump over or rewind authoritative progress; an incompatible
semantic/schema epoch requires a new root. Do not silently key compatibility to
the crate version, which changes for unrelated fixes.

`PendingTransaction` includes descriptor hash, predecessor checkpoint ID, full
target checkpoint, first/last accepted ordinal, digest of the ordered event
window, sorted table inventory (including zero rows), every exact relative final
and temporary path, schema digests, row counts and optional durable part receipts.
The transaction ID hashes the descriptor, predecessor, ordered event digest and
canonical plan identity; hashes exclude their own hash fields. Parquet footer
metadata records stream/transaction/entry/schema identity. Receipt SHA-256 covers
the complete encoded file, not only row counts, footer fields, or an S3 ETag.

Only normal relative components are allowed: no absolute path, `..`, separators
in table identifiers, ambiguous S3 keys, or symlinks at owned final/temp names.
Resolve and validate partition directories beneath the canonical owned root.
Root symlink aliases remain supported; retargeting a root or moving its ancestry
while active is outside the cooperative ownership contract and must be detected
before subsequent mutation where identity can be revalidated.

Metadata writes use a private local mode (`0600`, control directory `0700`) and
atomic replacement plus fatal file/directory sync, including durable ancestor
creation. S3 control objects use non-cacheable metadata and normal private bucket
access. Opaque cursors exist in recovery state but never in logs, error context,
`Debug` output, audit reports or filenames.

## Exact write order

1. Acquire all scopes. Recover/validate any pending ingestion or allowed
   maintenance journal. Load authoritative state; reconcile the mirror. Only
   then request blocks. For an empty root, create and sync the initial state
   before any transaction or data part. Existing control corruption is an error.
2. Freeze the complete accepted prefix and batch map. Preflight **all** schemas,
   partition destinations, bounds and planned names before any final output.
   Reject any pre-existing planned final/temp name with no owning pending journal.
3. Durably create `pending.json` in `Writing` with the entire output plan before
   creating a transaction temp or final part. Checkpoint remains H.
4. For each nonempty entry, stage/close/sync a local Parquet file at the recorded
   exact temp path and hash it; or encode one S3 part in memory and hash it.
   Durably update the Writing journal with its expected size/hash **before**
   publishing that part. This per-part journal update avoids holding every
   compressed table in memory and proves ownership even if publication succeeds
   but its response is lost. No receipt means a final part must not exist.
5. Local publication uses #578's no-clobber hard link, removes its temp name and
   syncs the containing directory. S3 uses conditional Create, never overwrite.
   An ambiguous response is resolved by exact GET/hash/identity verification;
   a mismatched collision stops. Persisted receipts are expectations, not proof
   that publication completed; verify all expected parts before committing.
6. After **all** planned outputs are complete and durable, atomically replace
   pending with `Committed`. The target frontier is immutable. For all-zero-row
   accepted events this is still a real transaction, with no part entries.
7. Advance state from H to its exact recorded target H'. Local replacement is
   under ownership; S3 replacement is CAS against the version read for H.
   Save the configured cursor mirror using bounded retry. Preserve the committed
   journal on state/mirror failure, including cancellation. Never request or
   accept later stream work after such a failure.
8. Durably remove pending, then return `CommittedFrontier` and retire the whole
   batch map. Commit metrics and cursor-save success correspond to this receipt.
   A cleanup failure stops with retained or ambiguously removed recovery state;
   it must not trigger a second flush or duplicate metrics within this session.

There is no in-process "retry the failed table" API. Expected pre-publication
errors, post-publication sync errors and lost S3 responses all end the session;
the persisted phase controls the next attempt. #578's complete published file
is retained on a normal post-publication error until journal recovery owns its
rollback decision.

## Recovery decisions

| Durable state | Required action before opening a stream |
|---|---|
| No pending | Use authoritative H; never trust a mirror to move it. |
| Writing, state is predecessor H | Verify all present final files against their persisted expected receipts. Delete only those exact owned parts and exact temp paths; sync every removal; only then durably remove pending. Resume H. |
| Writing, state differs from H | Stop. The protocol never advances authority while Writing. |
| Committed, state is H | Verify every nonempty part, then install the journal's H', mirror it and clear pending. Do not remap or replay this event window. |
| Committed, state is exact H' | Verify parts and finish mirror/cleanup. |
| Committed, unknown state or missing/corrupt part | Stop with evidence retained. Do not roll back committed data to obtain a convenient replay. |
| Missing authority with data, malformed/unknown control version, unsafe paths | Stop before publication or stream request; require diagnosis or a new root. |

Writing rollback tolerates already removed files and repeats directory syncs.
Partial temp files may be removed using the exact journal-owned path, even when
they cannot parse as Parquet. A final file without a persisted receipt, with a
different digest, or outside the recorded scope is never deleted automatically.
Recovery never discovers ownership by `part-*`, height range or newest mtime.

Local compare/replace/remove operations are under a live guard and verify the
expected state/version first. S3 metadata replacements are conditional. A lost
metadata PUT response is reconciled by reading and validating the exact proposed
content/version. After uncertain removal, reload before proceeding. Unsupported
conditions, stale/inconsistent reads or inability to verify a receipt fail closed.

## Accepted frontier, bootstrap and final flush

Add `AcceptedEnvelope { ordinal, cursor, block_num, block_id, step,
routing_after }` to the binary's existing buffered block structure. Assign the
ordinal at receipt, carry it through every buffer, and mark it accepted only
after successful mapping/filter handling. Track the contiguous accepted prefix,
not maximum height, last received cursor, or whether a batch has rows. Filtered
events with zero rows still count as accepted; undecoded/mapping-failed events
do not. An unresolved earlier buffered event blocks advancement past it.

`RoutingCheckpoint` records the routing policy, original block-range anchor and
last-known timestamp anchor (source block identity/ordinal and timestamp). Build
it for each accepted event. The partition-boundary flush uses the previous
accepted envelope's snapshot, even if observing the next Solana event has already
changed `TimestampBackfill.last_anchor`. Persisting the mutable global anchor
would cover a later event with the earlier cursor.

Genesis look-ahead requires explicit treatment: buffered timestamp-less events
are mapped in original order with the chosen anchor attached to each envelope.
If a flush commits a prefix before the anchor-producing envelope is mapped,
record the chosen synthetic timestamp and its provenance as routing context,
not as an accepted cursor/ordinal. On restart that later envelope is replayed;
the committed prefix's routing decision remains fixed. For Solana's last-known
anchor policy, persist the anchor used after the committed prefix, including a
prefix containing only missing timestamps. Do not substitute the next observed
timestamp or last block height for its source. Tests distinguish both policies.

The ordered event digest includes ordinal, opaque cursor bytes, canonical block
ID, height and fork step. NEW(A), UNDO(A), NEW(B), NEW(A) again remain distinct
accepted deliveries. Do not deduplicate by height, `(block_id, step)` or cursor
string. Trust only the declared Firehose resume-after-cursor contract; malformed
or inconsistent replay identity stops. This work does not change #474's row-level
reorg semantics or promise chronological ordering from arbitrary glob readers.

Replace the three independent cursor-save sites (partition transition, normal
threshold, clean EOF) with `session.commit`. `finish_request` records a completed
bounded request even if its final events emit no rows or its last transaction
was already acknowledged. A repeated recorded completed bound is a no-op after
recovery/mirror repair, regardless of changed flush thresholds. A clean gRPC
close alone is not proof of a bounded completion: carry the existing range/
exhaustion validation result as `ProvenCompletion` and reject unresolved bootstrap
state. If no opaque cursor has ever been accepted, do not invent one or advance
to `last_height + 1`; only an explicitly proven empty range may be remembered.
Extending such an empty range must start at its recorded exclusive stop, with
the original dataset origin retained, or fail with an explicit range instruction.

Shutdown/failure retains the existing policy of discarding an unprepared mapper
tail. Prepared work has a journal; a second signal may leave a temp or partial
transaction, but no invalid final Parquet file. Recovery handles it. Cancellation
cannot turn an incomplete transaction into success.

## Local ownership and multiple scopes

Every mutator resolves scopes before acquiring a guard: output directory,
external cursor's parent, merge/truncate source, rollup source/destination, and
artifact/report parents as applicable. A single-file operation locks its parent.
This can be coarse, especially for a cursor placed directly in `/tmp` or a home
directory; recommend a dedicated state directory instead of weakening exclusion.

Canonicalize paths and collect both target ancestry and lexical alias ancestry.
Reduce nested mutation scopes first: an exclusive parent covers its descendants.
Build one device/inode map of required modes, exclusive winning over shared.
Take shared OS locks on every remaining ancestor and exclusive locks on the
mutation scope roots. Deduplicate aliases and open handles once; **sorting paths
alone is insufficient**, since separately locking output and its nested cursor
parent can conflict with the same process. Acquire in deterministic ancestry/
identity order with nonblocking `try_lock`; unwind the entire set on conflict.
No partially held scope proceeds with writes.

Ancestor shared locks mean an exclusive merge/truncate of `/data` conflicts with
an ingester of `/data/mainnet`; disjoint `/data/mainnet` and `/data/solana` can
coexist. Symlink aliases conflict through the same inode. Directory handles are
stable lock objects and are never unlinked to "release a lock." Retain them for
the session. Do not delete/recreate a held scope root. For absent directories,
lock existing ancestors first, durably create missing components, reopen and
validate canonical identities, reduce the completed scope set, and abort/retry
acquisition before any data mutation if the identity graph changed. Cooperating
commands must lock a containing scope before removing descendants or links.

Use supported macOS/Linux directory `File` locks and fail explicitly on other
platforms or unsupported filesystems. A bounded local experiment on this macOS
host passed exclusive-vs-shared/exclusive conflicts, shared coexistence, symlink
alias conflicts, and release after the owning handle closes. This is feasibility
evidence, not the final implementation test. Required Rust child-process tests
must pass on macOS and the existing Ubuntu CI runner, including actual process
death and nested/multi-scope reduction. Network filesystems need an explicit
supported-lock and durable-sync contract; no warning-only fallback is permitted.

Old binaries, external tools that ignore the protocol, and administrator path
replacement are not fenced by advisory locks. The migration runbook requires
stopping them before enabling protected output.

## S3 ownership and explicit lost-owner recovery

The fixed key is **`s3://<bucket>/.fireparq-owner-v1.json`**, outside dataset
prefixes. All fireparq mutators use it, including legacy maintenance, external
cursor writes and custom artifact destinations. Deduplicate bucket identities
after endpoint normalization, collect all involved buckets, sort them and acquire
each before mutation. Distinct endpoints addressing the same physical bucket
must use the same configured service identity; unsupported aliases cannot be
treated as independent ownership domains. Mixed local/S3 sets use one stable
global ordering; local locks never wait while a subset of S3 writes begins.

The object persists in `Released` or `Owned { owner_id, generation, operation,
scopes, started_at }`. Timestamp is informational. Acquisition is conditional
Create if absent, otherwise CAS **only from Released**. Any Owned record blocks
regardless of age or PID. Require a usable ETag/version proof and supported
conditional Create/Update; do not inherit `S3RunLock`'s overwrite fallback,
30-minute takeover, heartbeat inference, or unconditional delete release.

Normal release CASes the exact held generation to Released; it never deletes
the key. Lost acquisition/release responses are reconciled by exact content
and generation. If ownership cannot be proved, stop all mutation and retain the
record. A process crash while acquiring several buckets can leave a subset
owned; report those records for operator recovery. This is safer than silently
releasing a record whose successful acquisition response was lost.

Proposed CLI: `fireparq recovery release-owner --bucket <bucket>
--expected-owner <id> --expected-generation <n> --acknowledge-writer-stopped`.
First `recovery status` reports the owner and affected scopes without cursors.
The operator must stop every process holding that owner, wait for termination,
and revoke its write capability if termination cannot be established. The
command must additionally require explicit provider-level evidence that every
previously sent write has completed or has been conclusively revoked/drained.
Process termination alone cannot prove this: a delayed old PUT could arrive
after Writing rollback and recreate a removed part. If the generic backend
cannot establish that quiescence, plain-glob recovery is refused; a timeout or
operator process-stop assertion is insufficient. The CLI must expose separate
process-stopped and remote-requests-quiescent assertions/evidence, rather than
presenting the preliminary spelling above as sufficient authorization.
The command records those assertions, verifies the exact owner/generation
and CASes to Released. It cannot infer quiescence from elapsed time. A mismatched
generation or ambiguous response stops; it never "force deletes" ownership.
An ambiguous S3 mutation error retains Owned rather than running ordinary release.
The next normal invocation acquires ownership and runs journal recovery only
after the explicit quiescence requirement has been established. Merely
removing a lock object is not a supported recovery procedure.

Permissions: `GetObject` and conditional `PutObject` for the exact ownership
key, plus Get/Put/Delete for the dataset control prefix, ListBucket limited to
needed discovery prefixes, and current data/mirror permissions. No DeleteObject
is needed for the ownership key. Versioned buckets may require version access;
encrypted objects retain their usual KMS permissions. A prefix-only principal
without access to the bucket-root key is intentionally rejected. The provider
must supply consistent GET/list semantics and conditional write behavior; a
stateful fake tests the protocol, and a dedicated opt-in integration fixture
must qualify each advertised real backend before making a remote guarantee.

Cost/availability: at least acquisition/release CAS plus checks per bucket per
session, full object verification during recovery, and one receipt journal write
per part. An abandoned owner blocks all fireparq mutations in that bucket until
explicit recovery. This first release favors a provable scope over concurrent
prefix throughput; hierarchical prefix ownership is a separate design.

## Maintenance and migration rules

Protected-state discovery must include ancestors and selected descendants under
the held hierarchical/bucket guard, not just `path/.fireparq-ingest`. Thus
`merge <table>`, `truncate <chain-parent>` and direct control-file targets cannot
bypass a root marker. Resolve every discovered dataset before changing anything;
reject nested conflicting descriptors. Recovery expands its guard set to include
the descriptor's external mirror location and reacquires the full reduced set
before mutation if necessary.

For protected merge, recover pending ingestion first; only acknowledged data may
be compacted. Keep the existing writing/committed merge recovery, bind its run
context to the common guard, and run recovery for every pending merge before
ingestion begins again. Acknowledged ingest manifests are not a permanent list
of physical files: after pending cleanup, later merge may replace those parts
without changing the logical checkpoint. Never verify/depend on old physical
part receipts merely because the current head ID was produced by that flush.
Only a retained ingestion journal requires its planned parts to remain present.
Unrecognized legacy/foreign merge ownership cannot be assumed dead; refuse and
require a legacy recovery run while old writers are stopped. Ensure discovery
and tests include new transaction filenames as ordinary data sources.

Initially refuse protected truncate, in-place rollup and rollup deleting protected
sources before writing anything. They need their own journal plus a coordinated
logical-frontier policy, and belong in follow-up work. Copying protected rows to
a different legacy export root is allowed only with source and destination guards,
no pending transaction, and no protected destination marker; the export does not
inherit an authoritative ingestion checkpoint. It retains existing rollup copy
semantics and is not advertised as a protected dataset. Read-only dry-runs may
remain unlocked and must identify results as observational.

Guard registry updates (including automatic first-root fills), verify reports
and `partitions build` output. Reserve/skip all control objects in collectors.
Legacy maintenance keeps its existing public data behavior but participates in
the common lock so it cannot race initialization or protected descendants.
The old merge-only locks are not an independent way to authorize recovery.

| Existing workflow/state | First protected-release behavior |
|---|---|
| Empty output, no cursor | Initialize authority under ownership, then ingest. A compatible standalone partitions index may be preserved after validation. |
| Random-name output without authority, with or without cursor | Refuse append; read/export/verify remain available. Use a new root. No inferred adoption or deletion. |
| Empty output with a nonempty existing external cursor | Refuse automatic initialization: copying a resume cursor is not proof of owned prior output. Use a fresh cursor/root or future explicit import. |
| Authority exists, mirror missing | Recreate mirror from H; resume H, never original start. |
| Mirror same protected stream, lower delivery ordinal | Repair from H; never replay it. Validate metadata integrity; do not compare block heights. |
| Mirror ahead, different stream, malformed or conflicting cursor at same ordinal | Stop before streaming; authority remains intact. A deliberate recovery command can replace the mirror after diagnosis. |
| `--cursor none` | Continue using mandatory output authority, omit mirror only. Mirror binding changes require explicit local/remote state migration, not a new stream. |
| `--cursor-override`, manual rewind, incompatible mapper/filter/partition changes | Refuse protected append; direct operator to a new root. |
| Repeated completed bounded build, changed flush size/time | Recover/repair first, then no new parts and no replay. |
| Stop extension after a proven complete prefix | Resume exact authoritative cursor; keep original partition origin and semantic descriptor. |
| Two producers in same root / same S3 bucket | Refuse conflicting ownership; no age-based takeover. |
| Unsupported fsync, directory locks or S3 conditions | Fail explicitly, never downgrade durability silently. |

## Reviewable implementation stages

These can be separate reviewed commits/PRs, but #468 remains incomplete until
the protected path and all acceptance tests are enabled together.

1. **Ownership and durable state primitives:** versioned records, local reduced
   inode guard sets, S3 owner CAS/status/release command, strict local/S3 metadata
   store, reserved paths; wire common guards into current mutators without yet
   claiming ingestion transaction safety. Tests cover OS process death and CAS
   failures. No automatic adoption or force reset.
2. **Journal and planned parts:** explicit low-level filenames, stage/receipt/
   publish interfaces, complete table plan, Writing/Committed transitions and
   standalone recovery. Real filesystem crash tests plus fake-store state-machine
   tests. Keep the user-facing protected path gated until integration is complete.
3. **Ingestion integration:** accepted envelope/frontier/routing snapshots,
   authoritative startup, all flush sites and EOF/zero-row completion, error and
   cancellation propagation, cursor-mirror metadata/repair, managed-root legacy
   writer refusal. Enable the protected default only with the restart matrix.
4. **Maintenance/migration qualification:** protected merge recovery, explicit
   truncate/rollup refusals, every metadata writer guarded, old-state diagnostics,
   operator runbook, full workspace tests and current-main integration, local
   bounded live row parity. S3 backend qualification is opt-in and separately
   authorized; until qualified, remote support remains explicitly unqualified.

The split is code-review organization, not a claim that deterministic names,
atomic parts, or an unconnected journal resolve crash replay by themselves.

## Required test matrix

| Area | Tests and observable assertions |
|---|---|
| End-to-end restart | Real child process abort before journal, after Writing sync, mid-temp write, after receipt sync, after first/final publication, after Committed sync, after authority sync, during mirror save, and around journal removal. Restart through binary startup with an in-process recorded Firehose server; assert no Blocks request occurs before recovery completes. |
| Changed boundaries | Baseline multi-table fixture versus every crash with 10-event flush replayed as 6-event flush, timers changed, HashMap order shuffled and partition transitions crossed. Compare every column/schema and row multiset; assert exact window ownership/no overlapping files. |
| Event frontier | Zero-row tables, all-zero-row accepted events, filtered deliveries, repeated heights and NEW/UNDO/NEW/repeated NEW identities; validate ordinal/cursor and event multiplicities. Add unresolved bootstrap and skipped-slot completion cases. |
| Routing | Solana real timestamp followed by null timestamps, next-partition look-ahead before previous flush, genesis buffered-prefix flush before anchor event, restart of each. Verify exact routing anchor/provenance, partition paths and nullable timestamp data. |
| I/O faults | ENOSPC/encode/close/file-sync/ancestor-sync/link/directory-sync/journal/state/mirror/cleanup failures. Normal errors stop; invalid final Parquet is never exposed; unrelated legacy objects remain unchanged. |
| Recovery corruption | Unknown version, unsafe paths, checksum mismatch, foreign final collision, missing Committed file, Writing with advanced head, lost state, corrupt mirror. Fail closed with retained evidence and no new stream. |
| Local ownership | Separate child ingesters and merge/truncate against equal, ancestor, descendant, sibling and symlink scopes; nested external cursor; two destinations sharing ancestors; partial acquisition unwind; owner SIGKILL then recovery; replaced inode detection. Run macOS and Ubuntu. |
| S3 ownership | Stateful fake with conditional create/update, ambiguous successful PUT/release, missing ETag, unsupported conditions, stale reads, two buckets with partial acquisition, paused old owner, and an old in-flight PUT delayed until after attempted rollback. Process death/time alone never permits recovery; missing provider quiescence and wrong-generation release fail. |
| S3 transactions | Lost part PUT, journal/state CAS failure, rollback delete failure, cross-bucket mirror failure, committed missing/corrupt part. Exact checksum GET resolves ambiguity; no overwrite fallback. |
| Maintenance | Ingest pending plus merge at root/table/ancestor; interrupted merge before ingest; protected truncate/rollup refuse before any write; default verify registry fill and custom artifacts acquire guard; control paths excluded everywhere. |
| Migration | Existing random files with missing/stale cursor refuse append; missing protected mirror repairs; advanced/foreign mirror refuses; cursor-none remains protected; incompatible semantics refuse; completed rerun has identical file set/count. |
| Runtime parity | Current-main full suite/build, bounded local Ethereum 2-block table/schema/row equality against existing 14-table 12,298-row fixture, plus targeted offline Solana/bootstrap/reversible fixtures. Live S3 only in an explicitly authorized dedicated test bucket. |

Acceptance requires both recovery branches, changed replay grouping, current
maintenance entry points, and the declared ownership/migration restrictions to
work together. A local happy-path run or mock CAS test alone does not establish
end-to-end crash safety.
