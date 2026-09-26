# Transactional ingestion runtime and migration (#468)

This records the integrated implementation and its local qualification. The reviewed
foundation and private controller records describe narrower historical stages;
the runtime now connects those layers for every non-dry-run `build`. No optional
partially protected ingestion mode or automatic legacy adoption is provided.
Final end-to-end results and exact backend qualification limits are recorded
below. Publication still requires independent review and CI on the final head.

## Startup and immutable dataset identity

EndpointInfo must first resolve a nonempty chain name. Auto detection must also
resolve the mapper before recovery; an unknown custom chain needs `--block-type`.
The output directory remains `<configured-output>/<chain>`.

Acquire one common ownership set for the output and any external cursor mirror.
Load authority only to resolve omitted CLI defaults; never use the mirror to
select source progress. Rebuild the Firehose request client after those defaults
are resolved. Obtain every actual table schema from an empty mapper flush, hash
the complete ordered schema including metadata, and freeze the exact inventory
with effective mapping flags and the mapper epoch.

Reserve the owner's unique protocol permit. Under that permit:

1. Refuse overlapping ancestor/descendant protected roots before initialization.
2. Initialize only an empty root with an absent mirror; one strict same-chain
   v2 standalone index is allowed. Empty directory scaffolding is harmless.
3. Refuse coexisting pending ingestion and merge journals before either can
   recover; their relative mutation order cannot be inferred safely.
4. Recover Writing by verified owned rollback, or Committed by exact verified
   roll-forward. Reconcile the optional mirror from authority.
5. Finish recognized merge recovery through the same borrowed ownership.
6. Only then open Blocks with the resulting authoritative opaque source cursor.

A completed same-bound request follows recovery but opens no Blocks stream.
Increasing the stop retains the original start/partition origin and uses the
exact saved cursor. A missing external mirror is repaired first. A foreign,
ahead, corrupt or inaccessible mirror fails closed. `--cursor none` disables
only the mirror for a newly initialized dataset; it does not disable authority.
Changing the mirror binding of an existing dataset is refused.

The descriptor binds chain/family, source identifier encoding, mapper epoch and
all table schemas, original start, partition policy, effective mapping flags,
output storage identity and mirror binding. Compression and flush thresholds
are operational choices and may change. The source endpoint URL itself is not
an identity substitute for the chain; S3 service identity is separately bound
without credentials and compared against the actual resolved client endpoint.

## Existing output and changed semantics

Existing random-name data and legacy cursor files have no proof relating every
table to one accepted frontier. They are refused before Blocks, including with
`--cursor-override`. Rebuild into a new empty output root and an absent mirror.
There is no implicit adoption, repair of historical duplicates, raw cursor
rewind or switch that makes old data transactional. Legacy data remains available
for inspection and guarded legacy maintenance. An explicit migration tool would
need a separately reviewed baseline-verification contract.

Changing schemas, byte encoding, original range, effective feature flags,
partitioning, storage or mirror identity likewise requires a separate dataset.
Keeping old files beside new files does not perform a schema migration.

## Receipt order, routing and commit boundary

Every received envelope obtains an ordinal before final-only UNDO filtering,
below-start filtering or missing-timestamp buffering. Filtered events are
explicit zero-row acceptances and inherit routing only when their contiguous
prefix is resolved. The bootstrap queue carries the actual ordinal; no maximum
block height or last mapped row substitutes for source-event order.

Actual source timestamps stay distinct from routing timestamps. Solana preserves
its documented last-known anchor or explicitly recorded genesis fallback.
Non-nullable-chain initial missing times can use a received future anchor;
committing a prefix before that source event persists its exact identity and time.
Restart uses that saved anchor and verifies the source when it is received again.
It cannot silently choose a different timestamp after a crash.

Every partition boundary, threshold and clean final drain transfers the complete
owned batch map to one transaction. Whole-inventory schema/partition validation
and collision checks precede Writing. Each complete encoded part is staged,
its exact receipt is journaled, and only then is it published. After all final
parts verify, persist Committed, install authority, reconcile the mirror, remove
owned temporary names and clear pending. Only full success acknowledges the
frozen in-memory prefix. Empty output windows still advance authority with an
explicit all-zero inventory and no invented partition timestamp.

Ordinary errors and cancellation poison the session. Remaining mapper memory is
discarded. Recovery removes only exact transaction-owned Writing parts before
replay, or completes a committed transaction without remapping. Local complete
files are never removed merely to pretend a post-publication sync error rolled
back successfully. Control, file and directory durability failures remain fatal.

## Completion, shutdown and metrics

A successful bounded completion needs clean EOF, every received envelope fully
acknowledged, and the accepted boundary reaching `stop - 1`. Sparse or empty tails
alone do not prove covered bounds, including on Solana, NEAR and Beacon. Persist
the accepted prefix and return an explicit nonzero diagnostic instead. A later
finalized ancestry proof could broaden this rule; this implementation does not
infer it from EOF. Bounded non-final output retains the warning that completed
coverage does not prove tail finality or capture UNDO events arriving afterward.

A first shutdown signal stops new work and discards uncommitted memory. An
in-flight durable operation either completes or reports failure. A forced second
signal may leave pending state; recovery reconciles it before restart. Remote
ownership is retained on errors or ambiguous requests, and normal success releases
it only after synchronous mutations have finished.

Mapper gauges cover unflushed buffers. Writer gauges cover owned prepared batches.
Rows/files/bytes and flush counters advance on successful logical commits. Cursor
save counters measure actual mirror repairs, while resumed cursor progress is
initialized from authority even when the mirror is unchanged.

## Maintenance and backend boundaries

Merge, recovery, artifact-producing verification and partition-index publication
first discover protected ancestors and descendants plus bound external mirrors,
expand ownership without mutating data, then repeat discovery under that guard.
Protected truncate, in-place rollup and source-deleting rollup are refused.
Lossless merge and copy-only rollup into separate unprotected output remain
available. Partition-index output is an explicit artifact file target, so it
cannot overwrite a bound cursor or ordinary protected part.
(Update: `verify` no longer acquires ownership or recovers; see the
[verify follow-ups record](validation-verify-followups.md).)

Local support requires macOS/Linux directory inode locking, atomic same-directory
hard links and renames, file/directory sync and readable ancestry. Explicit root
aliases are supported with canonical and lexical ancestry checks; nested symlink
entries are refused. Preflight recursively scans guarded trees. Unsupported
filesystems fail closed; no best-effort durability fallback is claimed.

S3 holds a persistent bucket-wide conditional owner, including independently
bound cursor buckets. All mutation clients use zero transport retries. Part
Create, control CAS and mirror CAS use exact version/readback checks; unresolved
requests retain ownership. No expiry or automatic takeover exists. `recovery
release` requires exact owner/generation and operator assertions referencing both
writer cessation and provider-confirmed drain or permanent revocation of every
prior request. Process exit, a timeout, or elapsed time is insufficient: a delayed
old PUT could otherwise recreate a rolled-back part after recovery. A backend
without conclusive provider quiescence cannot safely use this recovery path for
plain-glob readers. The generic CLI cannot verify that assertion itself.

`fireparq recovery status <resolved-root>` is observational. After required remote
release (local OS locks release when the process exits), `fireparq recovery recover
<resolved-root>` performs owned recovery without contacting Firehose.

The protocol assumes cooperating writers and supported backend semantics. It
provides crash/replay consistency, not atomic multi-table query snapshots; plain
globs may observe a partially published table set during a running transaction.
No production S3 writes were performed for this implementation. Stateful fake and
real AmazonS3 loopback HTTP tests qualify the tested protocol/client behavior,
not a blanket production-provider safety claim.

## Qualification record

- Controller/parts tests interrupt every modeled boundary, including subprocess
  termination during first-part publication and after Committed. Recovery proves
  Writing rollback and committed roll-forward, exact receipts, zero-row progress,
  source-order continuity, mirror ordering and retained remote uncertainty.
- Prepared writer tests cover complete schema digests, stable full-inventory
  indices including zero tables, no-clobber publication, local durability and
  remote one-attempt/cancellation behavior.
- Session assembly: 68 focused tests passed (one intentional child helper ignore);
  shared strict-index decoder: 145 tests passed. Native owned S3 eligibility works
  on a current-thread runtime with bounded cooperative GET/body waits.
- Runtime checkpoint `582b35a`: all 178 binary unit tests passed, including the
  complete eight-family mapper schema matrix. The first workspace run found an
  older ownership fixture using an unknown invented chain without `--block-type`;
  that fixture now supplies its actual EVM family. This was an expected new
  eligibility requirement, not a weakened ownership assertion.
- Maintenance's standalone integrated suite passed 947 tests with six intentional
  ignores; its final refined core suite passed 619 with five ignores. Actual
  controller-produced parts were merged without changing rows/frontier; protected
  destructive operations were refused, copy rollup remained allowed, and lost or
  cancelled remote cleanup kept owner and journal after one attempt.
- Combined core runtime and maintenance passed **636 tests** (630 core library,
  three generator, three compatibility), with five intentional library ignores.
- Final runtime head `a9306b7d7e87d9996561fc1b920326acad7604f3` integrates main
  `f555898100fbfb486e97f40162c71b223112a94f`. The full workspace passed **995 tests**
  with **eight intentional ignores**; the separately required capture example
  passed **one test**, with its subprocess helper intentionally ignored. Build,
  formatting and Bash/Zsh/Fish completions passed without compiler warnings.
  Evidence: `/tmp/fireparq-468-current-main-workspace.log`.
- Real CLI tests create authority through actual ingestion, then prove completed
  range no-op, exact cursor extension, deleted mirror repair, legacy/changed
  semantics refusal before Blocks, filtered UNDO zero-row completion, sparse EOF
  refusal with a durable prefix, and SIGKILL after accepted unflushed data followed
  by exactly one copy of each replayed row. Genesis NEW/filtered UNDO/future anchor
  tests verify persisted ordinals and provenance through the actual callback,
  including a malformed future payload after the buffered prefix commits.
- Nine Session-focused tests include actual `IngestionSession::open` ordering:
  nested roots fail before new control creation, and coexisting valid Writing
  and merge journals fail with byte-for-byte unchanged files. Removing only the
  conflicting merge intent makes the same Writing fixture recover successfully.

## Bounded real-data qualification and discovered regression

The live destination was newly created for this task at
`/tmp/fireparq-468-live-20260925/mainnet`. The requested source range was exactly
`[26049575,26049577)` through `https://eth.firehose.pinax.network:443`, using the
intended Pinax credential selected explicitly by name. Only local output was
written. Credentials and raw cursor values were excluded from saved evidence.

The first attempt on the pre-fix runtime stopped while preparing the transactions
table of the first block. Ten complete parts had published with recorded receipts,
authority remained at ordinal zero, and pending remained Writing. Verification
had incorrectly included Arrow's deprecated IPC dictionary IDs in schema identity.
Parquet assigns those IDs during serialization; Arrow's own Field equality ignores
them. This affected multi-dictionary schemas such as EVM and Tron transactions,
which earlier equality-only schema tests did not detect.

The correction normalizes only typed Field dictionary IDs recursively. It keeps
field order, names, types, nullability, dictionary orderedness, union type IDs and
all field/schema metadata, including metadata keys named `dict_id`. The original
zero-ID EVM mapper hash remains exactly
`44b18c11097fad9f240941e6c670cb01f04386d7a990a91ca644b935b01c2563`, so the interrupted
journal remains usable. The full nonempty mapper/encoding/profile schema matrix
was proven failing before the fix and passing afterward; nested/multiple
Dictionary tests and exact old-hash compatibility cover the correction.

Before any additional source request, the fixed binary ran offline
`recovery recover` against the actual interrupted output. It removed all ten
owned published parts, left no temporary files or pending record, and retained
the original authority byte-for-byte. The next bounded run into that recovered
root completed successfully, producing 26 data parts plus the cursor mirror and
no temporary remnants. This exercised real failed-Writing rollback before replay,
in addition to the hermetic interruption matrix.

DuckDB SQL schemas, physical Parquet schemas, row counts, and bidirectional
`EXCEPT ALL` comparisons against `/tmp/fireparq-469-live-20260925/mainnet` matched
for every table. No duplicate rows or absent reference rows were found:

| Table | Rows in each output | Differences |
|---|---:|---:|
| access_lists | 72 | 0 |
| balance_changes | 1,970 | 0 |
| blocks | 2 | 0 |
| calls | 4,261 | 0 |
| code_changes | 3 | 0 |
| logs | 1,438 | 0 |
| nonce_changes | 465 | 0 |
| set_code_authorizations | 2 | 0 |
| storage_changes | 3,545 | 0 |
| system_balance_changes | 32 | 0 |
| system_calls | 8 | 0 |
| system_storage_changes | 10 | 0 |
| transactions | 458 | 0 |
| withdrawals | 32 | 0 |
| **Total** | **12,298** | **0** |

The mirror's block identity, origin and completed stop match the reference.
Its `last_timestamp` now preserves the actual committed EVM source time, whereas
the legacy reference stored null; it was independently checked against the final
block row. This deliberate mirror enrichment does not change any data table.

Two further runs reused the completed bound. The first left the mirror unchanged.
For the second, only this task's mirror was deleted first; it was repaired to the
same block/origin/completed stop and source time. Both returned the no-Blocks
completion result, and hashes of every data part and the authoritative state
were unchanged. Hermetic CLI request counters separately prove this path makes
no Blocks RPC. No stop extension or other live range was requested.

Sanitized evidence is summarized in [the comparison record](468-live-comparison.json).
The local runner/comparator and logs remain under `/tmp/fireparq-468-*`; the
credential-selecting live runner is intentionally not committed. No production
S3 writes or production-provider request-drain qualification was performed.
