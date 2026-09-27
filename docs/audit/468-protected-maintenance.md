# Protected maintenance and recovery

This change integrates maintenance with the staged ingestion transaction
implementation. It does not enable protected ingestion or adopt legacy data.
Issue #468 remains outstanding until the complete session, caller, restart and
qualification work is integrated. The existing ownership and provider-quiescence
requirements continue to apply.

## Discovery under ownership

`ingest/maintenance.rs` acquires the selected source and destination scopes before
its authoritative discovery. It checks ancestors and descendants for the exact
`.fireparq-ingest` marker, including canonical and lexical local ancestors. S3
matching uses path components, so `data-other` is not a descendant of `data`.
Explicit local root aliases remain supported; nested symlinks are refused.

A table or partition selection expands to its complete protected dataset root.
A parent selection can discover multiple independent protected datasets. An
ancestor marker is initially only observed: its authority is not read until the
exclusive root scope has been added. The guard set is released and reacquired,
and discovery is repeated. The descriptor's external cursor mirror locations are
then added in the same way. There are at most eight expansion passes and 256
protected roots. Remote discovery/read operations have 60-second bounds.

Malformed markers, missing or invalid authoritative state, nested protected
roots, changed runtime output/service bindings and a scope that does not
stabilize all fail closed. No state or cursor is inferred from legacy table
files. Planning failures release resolved acquisitions before recovery starts;
recovery or mutation failures retain remote ownership.

The separate `validate_ingestion_target` hook only observes markers and rejects
ancestor/descendant protected roots before session eligibility or initialization
can create anything. It does not read authority outside the selected scope.
After initialization, `validate_ingestion_recovery_order` rejects coexistence of
pending ingestion and merge journals before the controller recovers either.
`prepare_ingestion` subsequently finishes recognized merge journals under the
same owner and already-held session permit. It acquires no second owner or
session permit and opens no nested ingestion controller.

## Supported operations

- Merge, artifact-producing verification, partition-index preparation and the
  explicit recovery command recover each protected ingestion transaction and
  reconcile its mirror before listing table data for the requested operation.
  (Update: `verify` no longer does; it reads without ownership and refuses
  unfinished merges. See the [verify follow-ups record](validation-verify-followups.md).)
- Protected merge compacts within the original table/partition directories. It
  leaves the authoritative descriptor and accepted frontier unchanged. Its
  journal records the exact protected stream digest; unbound or foreign-stream
  merge journals under a protected root are refused.
- A copy rollup may read a protected source and write to a separate unprotected
  export root. Protected sources cannot be deleted, and rollup output cannot
  overlap a protected root in either direction.
- Truncation of any protected selection is refused before recovery or table-file
  deletion. The refusal also applies to a table, partition or enclosing parent.
- Artifact destinations cannot overwrite a protected cursor mirror, recovery
  control, or ordinary Parquet data part. Reserved partition/merkle/verify
  artifacts and separate non-Parquet reports remain supported. Input-file scopes
  are distinguished from artifact output scopes.

Read-only merge/truncate previews and protocol-only verification without output
remain observational and do not acquire write capabilities or recover data. A
preview therefore does not promise a committed snapshot. Legacy ordinary merge
retains its existing behavior of skipping a partition claimed by a live legacy
merge. Artifact reads and protected/session recovery refuse an active merge.
Other legacy maintenance paths recover recognized interrupted merge journals
before authoritative table reads, without creating ingestion authority.

Compaction and copy export strip every `fireparq.ingest.*` transaction footer key
and the corresponding decoded Arrow schema metadata. The embedded Arrow schema
is regenerated. A new physical output must not claim to be the first source
transaction's original part. Column order, types, values and multiplicities are
preserved by these operations.

## Recovery details

`fireparq recovery recover <path>` acquires the complete discovered guard set,
recovers the protected ingestion/mirror state followed by recognized merge
journals, and prints only counts. `recovery status` remains read-only.
`recovery release` still changes only one exact remote owner and still requires
both stopped-writer and provider-confirmed request-quiescence evidence.

Local merge journals are bounded to 4 MiB, read only from regular non-symlink
files, and decoded with strict fields, version, digest, basename, phase and
source/output-inventory checks. Remote reads have the same byte bound and do not
expose provider errors. Local outputs and journal transitions retain the merge
protocol's file and directory synchronization.

Remote borrowed-owner recovery is native async, including on a current-thread
runtime. It uses the owner's zero-transport-retry client and shared control
mutex. Every DELETE is attempted once; an error, timeout or cancellation latches
ownership uncertainty and preserves the journal for recovery. A successful
later request cannot establish that an earlier request has drained. Old S3
prefix-lock journals have no sufficient ownership/quiescence proof and are
refused rather than silently adopted. No remote owner expires or is taken over
based on age.

## Validation

Focused tests cover guarded ancestor and descendant discovery, independent
siblings, nested-root refusal, external mirror scopes, aliases and symlinks,
corrupt/orphan markers, destination policy, and prevention of nested authority
creation. Real controller-produced deterministic Parquet parts are compacted
while asserting exact row values, unchanged authoritative state bytes, and no
source transaction receipt in either the output footer or decoded schema.

Additional fixtures cover protected copy export with unchanged source data and
frontier, public destructive-command refusal, Writing rollback and Committed
roll-forward before an artifact guard is returned, incompatible simultaneous
journals, legacy journal recovery without adoption, native async S3 cleanup under
an already-held session permit, and accepted DELETE errors/cancellation with one
attempt, retained owner and retained journal. All remote fixtures are hermetic;
no production S3 writes are part of qualification.

Validation on the feature branch based on the integrated controller/mirror and
binding/eligibility foundation:

- `cargo test --workspace --locked -j4`: 947 passed, 6 intentionally ignored.
- After the final single-local-file legacy recovery routing refinement,
  `cargo test -p firehose-parquet --lib --locked -j4`: 619 passed, 5 intentionally
  ignored. This reruns every maintenance, controller, mirror and existing core
  library regression against the final source change.
- `cargo fmt --all -- --check` and `git diff --check` passed.

Both test commands used the whole-command shared-target lock. The later session
and CLI integration must rerun combined workspace qualification; no claim is made
that this isolated maintenance commit completes #468.
