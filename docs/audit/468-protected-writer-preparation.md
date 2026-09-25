# #468: protected writer preparation and individual publication

This record qualified an initially inactive transaction-controller prerequisite.
The later [runtime integration](468-ingestion-runtime.md) now supplies accepted
frontiers, all-table journals, recovery and compatibility enforcement. Results
below describe the prepared-writer boundary, not standalone end-to-end guarantees.

## Boundary and implementation

`writer::protected::PreparedFlush` owns the complete mapper batch map. Its pure
constructor checks the declared inventory, exact schema digests, every nonempty
planned table/count, unique sorted indices from the full inventory (including zero-row tables), one stream/transaction
identity, and every row's partition destination before any I/O. It uses the same
partition validator as the existing eager writer, moved onto `ParquetTableWriter`
without changing validation rules. Reversible row order, negative times, nullable
Solana timestamps and numeric partition anchors preserve their existing behavior.
An inventory entry without a planned part explicitly declares zero rows: its
batch may be absent or empty, but any supplied schema must still match.

Plans bind the table and exact partition-relative paths to the full lowercase
SHA-256 stream and transaction identities, accepted ordinal interval and entry
index. Final names are
`part-v1-<stream>-<first>-<last>-<transaction>-<index>.parquet`; staging names are
`.fireparq-txn-<transaction>-<index>.tmp` in the same directory. Absolute, traversal,
ambiguous and internal-control paths fail. The controller owns transaction-ID
construction and validates its complete zero-row inventory in the pending record.

Encoding borrows the prepared map, processes only one table at a time, and returns
private complete Parquet bytes plus a strict receipt containing byte size, full
SHA-256, rows and schema SHA-256. No part is removed from the prepared flush and no
metric/cursor acknowledges rows. The caller can drop each encoded buffer after
local staging or S3 publication while retaining all Arrow batches until the
all-table transaction acknowledges commit. Verification may temporarily hold an
additional compressed copy of that same table; it does not encode all tables at
once.

The controller must persist its Writing plan **before any staging** and persist
the exact per-part receipt **before final publication**. These crate-visible
primitives deliberately do not enforce journal ordering themselves; only the
future session controller will expose a usable protected ingestion API.

## Schema and file identity

The public `schema_sha256` helper hashes a fixed
`fireparq-arrow-schema-json-v1` domain plus Arrow's serialized schema after
recursively sorting JSON object keys. Field order, types, nullability, timezone,
nested fields and schema/field metadata remain significant. Only the serde feature
of the existing Arrow schema 60 dependency is added; no dependency version changes.
A future serialization change requires a deliberate schema-epoch migration.

File footers include `fireparq.ingest.stream_id`, `transaction_id`, `entry_index`,
`first_ordinal`, `last_ordinal` and `schema_sha256`. Caller footer entries with that
prefix are refused. Verification checks exact full-file bytes/hash/size, physical
Parquet row count and schema, and exactly one matching value for each identity key.
It does not trust a filename or an existing object as proof of a successful write.

The first focused run caught an Arrow behavior that matters here: its normal reader
adds file-footer entries to Arrow schema metadata. Verification therefore converts
the physical Parquet schema using only the unique embedded `ARROW:schema` hint,
preserving mapper schema metadata while excluding operational/transaction footer
entries from the mapper-schema digest. Data column schemas are unchanged; protected
files gain the explicit identity footer metadata.

## Storage and failure behavior

`LocalPartStore` borrows an existing local ownership guard, rejects roots outside
its scopes and rejects nested symlinks at each actual path. Staging creates the
exact temporary name with no replacement, writes/closes complete encoded bytes,
then syncs its file and directory. A failed stage retains any partial temporary
file at the journal-owned name; retries cannot silently truncate it.

Publication re-verifies the receipt, syncs the staged inode, then uses the same
no-replacement hard-link helper as the legacy writer. Only afterward does it
remove the temporary name and sync the final directory. An unlink/directory-sync
failure retains the complete final file and returns an error. Local verification
also syncs the verified inode and all directory links: visibility after an earlier
sync failure is insufficient evidence for advancing a recovered checkpoint.
Recovery verification returns `PartPresence::Missing` only on genuine NotFound,
`Present` only after verification/durability, and errors for all other conditions.
No removal/rollback policy is introduced by this primitive.

`S3PartStore` borrows the acquired owner and its actual store, confines the dataset
prefix to declared scopes, and serializes writes with the owner's control mutex.
Its optional `with_cache_control` builder preserves the configured data-object
policy. Each publication uses one conditional Create, requires usable response
version evidence, and re-reads matching bytes/version/footer before returning.
An existing final key is an error even when bytes match; the recovery controller
must explicitly verify the journal receipt. No overwrite, hidden application
retry or delete is attempted.

The mutation client must have transport retries disabled, as in stage one. Any
unresolved upload/readback error or cancellation after the attempted PUT permanently
marks its owner uncertain. Ordinary release and further publication are refused.
Timeouts are bounded at 60 seconds per data request/read. This preserves the
provider-quiescence recovery requirement: process cessation alone does not drain
already sent S3 requests. No production bucket writes were performed.

## Validation

Thirteen focused regressions pass. They cover schema metadata-order stability
and meaningful schema differences; a valid table before an invalid later table;
missing/undeclared data, bad counts/indices/paths/identity; explicit zero tables and noncontiguous indices when zero tables precede/between nonempty tables;
repeatable encoding without draining batches; negative/reversible/nullable/numeric
routing; footer collisions and mismatched receipts; separate staging/publication;
foreign-file preservation; partial staging and all publication/sync failure
boundaries; root/symlink escape refusal; conditional S3 Create and exact roundtrip;
lost responses, missing or changed version evidence, unsupported conditions,
readback failure and cancellation after server acceptance; rich historical Arrow schema/value roundtrip; and failed recovered-file/directory sync. Remote failures make
one data PUT, retain Owned and do not leak synthetic backend secret text.

The initial full workspace run exposed one failure in an existing verify test's
immediate directory-lock reacquisition after successful publication. The test
passed alone and in eight subsequent parallel library runs (32 test threads),
including four clean runs of all 508 library tests. Temporary diagnostics never
observed the conflict again and were removed; the existing test is unchanged.
Transient descriptor inheritance during other subprocess tests is a hypothesis,
not an established cause. The first four repeat runs had an unrelated new test
fixture failure while adapting its zero-table ordering to the agreed full-inventory
index contract; that fixture was corrected. Final workspace results follow below.

Final validation on 2026-09-25 passed **852 workspace tests**, with five intended
ignored child/fixture tests, including **13 protected writer regressions**. The
existing final-drain/checkpoint, atomic-local-publication, cross-command ownership,
endpoint startup, metrics and partition-probe suites all passed. Commands used the
whole-process Cargo lock and the shared Arrow 60 target with debug symbols disabled.
Formatting and diff whitespace checks passed. This is hermetic/local qualification;
there were no live Firehose requests or production S3 writes for this prerequisite.

### Physical dictionary schema qualification

The first bounded real EVM protected flush exposed a digest mismatch at the
transactions table before that table was staged or published. Arrow 60's JSON
serialization includes `Field::dict_id`, while the Parquet Arrow IPC hint assigns
new IDs to each dictionary column. These IDs identify IPC dictionary messages,
not logical table columns. Earlier simple and single-dictionary fixtures did not
expose the mismatch.

The digest now recursively rebuilds typed fields with dictionary ID zero before
canonical JSON hashing. It retains dictionary key/value types and orderedness,
field names/order/nullability, nested fields, and every schema/field metadata key
(including metadata named `dict_id`). Existing mapper schemas use zero IDs, so
their expected hashes remain unchanged. Receipt byte hashes, physical schema
reconstruction, row counts, and exact footer identity checks remain mandatory.

Validation: all 15 protected writer tests passed. New tests exercise nonempty
multiple and nested dictionary columns through actual Parquet encoding and
physical verification, and verify that changed dictionary ordering or metadata
still changes the schema digest. The integration owner's all-chain, nonempty,
all-encoding schema matrix independently reproduced this failure before the fix.
The original bounded live output was inspected read-only; recovery and renewed
live qualification belong to the combined runtime validation.
