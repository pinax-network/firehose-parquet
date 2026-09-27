# Issue #655: resume cost independent of data size

Closes #655 (PR [#668](https://github.com/pinax-network/firehose-parquet/pull/668));
part of #463. Related: #468 (the overlap guarantees kept here), #643
([Delta design](../design/delta-lake.md) §5 and §8), #658 (listing latency),
#659.

## Problem

v1.0.0 deploys one continuous, final-only `build` per network, and its dataset
grows forever: about 4,000 objects a day on Ethereum at a 5-minute head cadence,
before compaction. Every S3 start listed the whole dataset root three times,
each listing capped at 60 seconds in total:

1. `validate_ingestion_target`: the nested-marker scan (`discover_markers`,
   descendants of the root);
2. `prepare_ingestion`: the same scan again, after initialization;
3. `prepare_ingestion`: merge-journal discovery (`recover_guarded_for_ingestion`).

After a crash with a pending transaction, a fourth listing looked for merge
journals (`validate_ingestion_recovery_order`). A local start walked the tree
four times: the nested-symlink check in `DatasetOwnership::acquire`, the marker
scan twice and the journal walk.

On riv-dev1 RGW a 1,000-key page takes about 130–160 ms from outside the
cluster (the figure in #655; the [#658 record](658-live-flush-benchmark.md)
models S3 latency on loopback, and its RGW run is still to come). At that rate
a 60-second listing ends at about 400,000 objects, about 100 days of Ethereum,
after which every restart fails. Well before that, each restart spent minutes
listing.

## What startup reads now

| | Resume (authority exists) | Create (no authority) |
|---|---|---|
| Owner | S3: the conditional-write probe, then GET and conditional PUT of `.fireparq-owner-v1.json`: a fixed number of requests and no LIST. Local: inode locks, no tree walk | same |
| Authority | GET `.fireparq-ingest/state.json` and `pending.json` | same (absent) |
| Enclosing datasets | one LIST of `<ancestor>/.fireparq-ingest/` per directory above the root, first page only; none at the bucket root. Local: one `lstat` per ancestor | same, plus the root's own marker |
| Nested datasets | not checked (argument below) | LIST of the whole root, which must be empty; local walk |
| Emptiness | — | LIST of the root prefix, stopping at the first object other than the owner record and its probes; local walk |
| Merge journals | GET `.fireparq-ingest/merge-intent.json`; listed only if that record exists | same (never exists) |
| Transaction and mirror | the controller's reads of state and pending, the pending transaction's own parts after a crash, and `_fireparq/cursor.parquet` | initializes state; the mirror must be absent |

A resume lists no data object and reads none, except the parts of its own
pending transaction. The request count does not depend on the dataset's size.
Measured with a 1,000,000-object bucket (below): 0 LIST requests for a
dataset at the bucket root, and 1 for `s3://data/{chain}` (the bucket root's
control prefix).

`session.rs` reads the authority first. It then runs
`validate_ingestion_target` with `IngestionTarget::Resume` (ancestors only,
`MarkerScope::Ancestors`) or `IngestionTarget::Create` (the whole tree, as
before), still before eligibility, initialization and any Blocks request. The
second marker scan in `prepare_ingestion` is gone: ownership and the session
permit are held throughout, and initialization only adds the root's own marker.

## The overlap argument

The #468 guarantee: no protected dataset (a directory holding
`.fireparq-ingest/`) is ever nested in another. Call that invariant N. #468 kept
it by checking both directions on every start. This change checks both
directions when a dataset is created, and only the ancestors on resume.

Facts the argument uses:

1. **Only creation makes a marker.** `.fireparq-ingest/state.json` is written
   only by `TransactionStateStore::initialize` in `IngestionSession::open`,
   after `validate_ingestion_target(Create)` and `require_initializable`.
   Maintenance never creates authority (#468, "prevention of nested authority
   creation"). `recovery recover` does not either.
2. **Nested operations are serialized.** On S3 the owner is bucket-wide, so
   two roots in one bucket never run at the same time. Locally a root is held
   exclusively (a missing root holds its nearest existing ancestor) and every
   ancestor is held shared. For nested roots A ⊋ R, `build` of R takes a shared
   lock on an inode that `build` of A holds exclusively, and the reverse, so they
   exclude each other.
3. **Creation checks both directions under that ownership.** Before R's marker
   is written: no strict ancestor has a marker, no descendant has one (the full
   scan), and R is empty apart from the S3 owner record.

N holds after every operation:

- **Creating R.** An existing enclosing root A is found by R's ancestor check.
  An existing nested root D is found by R's descendant scan, and eligibility
  would also refuse D's objects. A concurrent creation of a nested root is
  excluded by fact 2. Creation is refused before anything is written.
- **Resuming R** creates no marker (fact 1), so it cannot break N. By N, R has
  no nested root, so the skipped descendant scan could not have found one. The
  ancestor check still runs, at O(depth) requests.
- **A dataset created later inside R** runs its own creation check, whose
  ancestor step finds R's marker and refuses. So the enclosing dataset never
  needs to look below its root: nesting is detected from the child's side.
- **A dataset created later above R** runs the full descendant scan, which
  finds R.

The `{chain}` cases of #654 follow: resuming `s3://b/mainnet` inside a dataset
at `s3://b` fails its ancestor check; `s3://b` without authority above
`s3://b/mainnet` is a creation, and its descendant scan refuses it.

What this no longer catches is a marker that fireparq did not create: a
`.fireparq-ingest/` copied by hand, or a backup restored inside another
dataset. Resuming the copy is still refused by its ancestor check (tested, on S3
and locally). Resuming the enclosing dataset does not notice the copy.
Maintenance still walks the whole tree and refuses both ("nested protected
datasets have conflicting authority"), and `build` of the enclosing dataset
writes only its own table paths. Moving or copying datasets is out of scope
(#635).

## Merge journals

`merge` still exists; lane L5 of #643 removes it with its journals. Until then,
journal discovery is scoped to a known control location instead of a full
listing. `merge` writes one small control record,
`.fireparq-ingest/merge-intent.json` (`ControlKey::MergeIntent`, a strict
`durable_state` record, a CAS tombstone on S3), and `build` looks for journals
only while it exists.

- `merge` records the intent in every protected root of its plan
  (`PreparedMaintenance::roots`, which a table or partition target expands to)
  after acquiring ownership and before it recovers or creates any partition
  journal. It clears the intent after the run completes.
- Maintenance recovery (`recover_roots`, used by `merge` and
  `recovery recover`) clears it after recovering every journal of the root.
- `build`'s `prepare_ingestion` reads the record. Without it, it returns. With
  it, it runs the nested-symlink check (see Local disk), recovers the journals
  of the whole dataset exactly as before (one listing) and clears the record.
  `validate_ingestion_recovery_order` (`MergeJournals::IfIntended`) looks for
  coexisting journals only when a transaction is pending and the record exists.

Invariant: a journal inside a protected root implies that root's intent.
Journals are created only by the merge engine (`create_journal`), after
`record_merge_intents`. The intent is cleared only when no journal can remain:
after a complete run, which removed each journal it created, or after a complete
recovery of the root, and always under the same ownership. The coexistence
refusal and the recovery order of #468 are unchanged.

Limits: a journal written by a build of `main` from before this change has no
intent, and `build` does not look for it. `merge` and `fireparq recovery
recover` still find it, because maintenance keeps the full discovery
(`MergeJournals::Everywhere`). Only unreleased builds can have written one:
v1.0.0 requires a new dataset, and on S3 an interrupted merge keeps the owner
record, so `recovery release` and then `recovery recover` come first anyway.
This is deliberately small. L5 deletes `MergeIntent`,
`ControlKey::MergeIntent`, `MergeJournals`, `has_merge_journal` and the body of
`prepare_ingestion` along with `merge`.

## Listings that remain

Every remaining discovery listing goes through
`maintenance::discovery::visit_objects`. Each request (one page, up to 1,000
keys) must arrive within `LIST_REQUEST_TIMEOUT` (60 s). The listing has no total
deadline. A listing that runs longer than 10 seconds logs `listing in progress`
every 10 seconds, then `listing finished`, with its object count. Errors name the
listing and how many objects it reached, never a key or a provider error. The
listings:

| Listing | When |
|---|---|
| ancestor control prefixes | every start (one page each) and maintenance |
| descendant markers | creation, and maintenance discovery |
| initialization emptiness | creation (stops at the first other object) |
| merge journals (`has_merge_journal`, `recover_guarded_for_ingestion`) | only with a merge intent, and in maintenance |

Not changed: `merge`'s own listings (`list_s3_objects` and the per-partition
`list_with_delimiter`), which L5 deletes, and `verify`'s one-request probe of a
control prefix (`observe::remote_marker`), which lane L6 rewrites.

## Local disk

`DatasetOwnership::acquire` walks every directory of a local mutation tree to
refuse nested symlinks. `acquire_for_ingestion`, used only by `build`, no
longer does (`TreeCheck::Paths`). `build` already refuses a symlink on each
component of every path it touches: part staging, publication and verification
(`LocalPartStore::path`), transaction recovery (`parts::local_path`), the
control directory (`LocalStateStore`) and the mirror. A new root must hold only
empty directories, so creation refuses symlinks too. Merge-journal recovery,
the one walk left on the `build` path, runs the same whole-tree check first
(`DatasetOwnership::validate_local_trees`). Maintenance commands keep the walk
at acquisition.

## Metrics and logs

- `firehose_parquet_startup_list_requests` (gauge): LIST requests made while
  opening the dataset. S3 pages are counted as `ceil(objects / 1000)` per
  listing, at least one, which is S3's default page (object_store sends no
  `max-keys`). Locally, directory reads.
- `firehose_parquet_startup_listing_seconds` (gauge): the time those listings
  took.
- One log line per start, `protected dataset startup checks finished`, with
  `list_requests`, `listed_objects`, `listing_ms`, `open_ms` and `opened`.
  It is also logged when the start fails.

## Tests

- `ingest/session/tests/resume_cost.rs`, with the paged bucket
  (`maintenance/discovery/paged_bucket.rs`). The bucket serves 1,000,000
  generated data keys (4 tables × 250 days × 1,000 parts) without storing
  them. It pages every listing at 1,000 keys, counting each page as a request,
  makes the 501st page of a listing take 1.5 s, and counts GETs of data keys.
  - `s3_resume_of_a_million_object_dataset_lists_no_data`: a dataset is created
    and committed in an empty bucket, the million keys are added, and the
    resume is measured. At `s3://data` it makes **0 LIST requests**. At
    `s3://data/{chain}` it makes **1** (the bucket root's control prefix). Both
    read 0 data objects, never reach the slow page, take about 2 ms, and export
    the same count in the metric.
  - `a_merge_intent_lists_the_dataset_once_then_resumes_list_nothing`: with a
    merge intent, the resume lists the dataset once, **1,001 requests**
    including the slow page, about 3.7 s in a debug build. It succeeds (no total
    deadline), the metric reports it, the intent is cleared, and the next resume
    makes 0 requests.
  - `s3_resume_refuses_a_dataset_nested_under_an_existing_one` and
    `local_resume_refuses_a_dataset_nested_under_an_existing_one`: a copied
    control state inside a dataset is refused when resumed. The enclosing
    dataset resumes with 0 requests.
  - `local_resume_walks_no_data_directory`: the data tree has 2,000 files, a
    symlink and an unreadable directory. Maintenance ownership and the
    creation-time check both fail on it, which shows that a walk would trip. The
    resume opens and reports 0 directory reads.
- `maintenance/discovery/listing_tests.rs`: a listing longer than the request
  timeout completes (6 pages of 60 ms against 200 ms). One page over the
  timeout fails with the per-request message and no key. An early break
  requests only the pages it read. The paged bucket lists whole segments in
  order.
- `interrupted_merge_leaves_its_intent_for_build_and_a_complete_merge_clears_it`
  (`ingest/maintenance/tests.rs`): a real `merge` crashes after writing its
  output. It leaves the intent and the journal. `build`'s startup hooks roll the
  journal back and clear the intent, and a complete merge leaves neither.
- Updated: the S3 `remote_merge_recovery_at` fixtures (without an intent,
  `build` leaves the journal; with one, it recovers it and clears the intent),
  and the session coexistence fixture, which now records the intent a real
  merge writes.
- Creation-time refusals are unchanged and still pass: locally
  `session_refuses_ancestor_and_descendant_authority_before_initialization` and
  `ingestion_target_refuses_nested_authority_before_creating_any_path`; on S3
  `remote_session_at_the_bucket_root_resumes_and_refuses_a_chain_template` and
  `remote_bucket_root_cannot_initialize_above_an_existing_chain_template_root`;
  with the real binary, `a_changed_output_template_is_refused_before_blocks`.

## Limits

- Nested markers created outside fireparq are detected only from the child's
  side (see the overlap argument).
- The request metric assumes S3's 1,000-key pages. A provider with smaller
  pages makes more requests than it reports.
- With Delta (#643 §8), startup also reads each table's log. That cost is
  bounded by checkpoints and belongs to lanes L3 and L4.
