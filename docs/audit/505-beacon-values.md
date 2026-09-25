# Issue #505: Beacon value semantics

## Scope and diagnosis

`execution_payload.base_fee_per_gas` now stores an exact unsigned decimal string
in wei per gas, and `blob_sidecars.blob` stores the source bytes as Arrow Binary
regardless of the mapper/library byte encoding. Hashes, roots, addresses, commitments, and proofs
retain the selected encoding. `blocks.spec` uses the generated protobuf enum
name with an Arrow `Dictionary<Int32, Utf8>` builder; unknown numeric values map
to `UNKNOWN`, separately from the declared zero value `UNSPECIFIED`.

An absent nested protobuf message now produces null descendant columns instead
of invented zeros or empty bytes. A present message with zero or empty scalar
values still records those values. Outer repeated entries, their order, their
canonical block identity, and their row indices remain present. The affected
fields are attestation data/checkpoints, deposit data, proposer-slashing header
messages, indexed attestation data/checkpoints and optional attesting lists,
voluntary-exit messages, and BLS-to-execution-change messages. Signatures on a
present outer signed message remain present even when its inner message is absent.
Absent bodies/payloads/requests still produce no invented child rows.

## Producer byte-order evidence

The issue proposed interpreting all fees as SSZ little-endian. That would be
incorrect for the actual Firehose Deneb and later payloads. Conversion follows
the typed body/payload, not the separately declared `spec` integer:

| Payload type | Actual producer representation | Accepted input |
|---|---|---|
| Bellatrix, Capella | Raw fixed `[32]byte`, little-endian | Exactly 32 bytes |
| Deneb, Electra, Fusaka | `uint256.Int.Bytes()`, big-endian with leading zero bytes removed | 0–32 bytes; empty means zero |

Pinned primary sources:

- [firehose-beacon `blockfetcher/block.go`, f5b4b11](https://github.com/pinax-network/firehose-beacon/blob/f5b4b11a3123c4d5d879f1defd0656af86e5a4fe/blockfetcher/block.go#L353):
  Bellatrix/Capella copy `BaseFeePerGas[:]`; Deneb's shared converter uses
  `BaseFeePerGas.Bytes()` for Deneb/Electra/Fusaka.
- [Producer dependencies](https://github.com/pinax-network/firehose-beacon/blob/f5b4b11a3123c4d5d879f1defd0656af86e5a4fe/go.mod):
  go-eth2-client v0.27.1 and holiman/uint256 v1.3.2.
- [Bellatrix dependency, 2507e73](https://github.com/attestantio/go-eth2-client/blob/2507e735097c067b79310a1f9075e28e99a7fa2e/spec/bellatrix/executionpayload.go#L96)
  and [Capella dependency](https://github.com/attestantio/go-eth2-client/blob/2507e735097c067b79310a1f9075e28e99a7fa2e/spec/capella/executionpayload.go#L102):
  JSON serialization reverses the internally little-endian fixed array before
  conversion to `big.Int`; parsing reverses back into the fixed array.
- [Deneb dependency](https://github.com/attestantio/go-eth2-client/blob/2507e735097c067b79310a1f9075e28e99a7fa2e/spec/deneb/executionpayload.go):
  `BaseFeePerGas` is a `*uint256.Int`.
- [`uint256.Int.Bytes()`, a17fcfb](https://github.com/holiman/uint256/blob/a17fcfb6f8b0245533928fa96aace2cd4c2e9b18/uint256.go#L124):
  returns big-endian bytes stripped to `ByteLen()`, including an empty slice for zero.

`num-bigint`, already in the workspace dependency graph through Arrow, is now an
explicit mapper dependency. No new package/version is introduced. Conversion
supports the whole uint256 range, including values beyond signed 256-bit or
128-bit limits. Invalid lengths fail before any row is appended, preserving
previously buffered blocks. No byte-order heuristic based on length or fee size
is used. A different producer must honor these protobuf payload conventions;
this is not a claim that arbitrary byte producers share the same representation.

## Migration

This changes the physical/logical schema. Rebuild into a fresh output root, or
perform an explicit, separately verified conversion before combining old and
new files. Changing the binary or resuming an old cursor does not repair existing
files, and generic union-by-name cannot convert the old fee semantics.

- Old fees follow the selected byte encoding (Binary or encoded Utf8). The CLI
  resolves this from the chain/profile and records `firehose-parquet.bytes_encoding`
  in file metadata; library callers select `EncodeBytes`. Decode that encoding
  first, join the block's fork/body provenance, then apply the fork-specific byte
  order above and output the exact decimal string. Do not cast encoded hex text
  directly to a decimal. Decimal strings require an explicit checked numeric
  cast for arithmetic; SQL engines with fewer than 78 decimal digits cannot
  represent every possible uint256 fee, and string sorting is lexical.
- Old encoded blob strings must be decoded to Binary. Existing Binary blobs
  already have the desired bytes. Blob length is preserved; this mapper does
  not introduce cryptographic blob validation.
- Old Utf8 `spec` values keep their known names; Arrow consumers now see a
  dictionary type. Unknown numbers that old files collapsed to `UNSPECIFIED`
  cannot be reconstructed from those files.
- Nullability widens only the affected nested-message fields. Historical fake
  zero/empty values cannot be distinguished from real zeros without replaying
  the source. Rebuild when this distinction matters.

## Validation

Focused mapper tests exercise every execution-payload body type at zero, 258
(an asymmetric byte-order vector), and `2^256-1`; retained live Deneb/Fusaka fee
vectors; malformed input rejection without partial rows; every generated spec
name and unknown versus unspecified; a 128 KiB blob under all five byte encodings;
unchanged commitment encoding, flush/reset behavior and buffer estimates; and
missing nested messages versus present empty/zero values, including nullable
list parents. Populated cases use the production Parquet writer and reader to
verify round-trip schemas and values.

At source `8e7ea05` on main `d5e1419`, the workspace suite passed **860 tests**,
with **5 existing ignored**, plus build and formatting checks. The inherited
`transactions_processed` unused-assignment warning in the CLI is unchanged.
All Cargo operations used the shared whole-command lock and target directory.

### Bounded live qualification

On 2026-09-25 at approximately 18:39 UTC, the copied debug binary at `8e7ea05`
(SHA-256 `75d4360076b3c23b8fd02b02ebac65521ebe45c03f838de90dfba26612fe43cb`)
completed exactly one finalized Beacon slot, `[10597349, 10597350)`, from
`https://eth-cl.firehose.pinax.network:443` into a fresh local temporary root.
The subprocess received only PATH and the intended Pinax API key, with a
45-second process bound. The selected Deneb slot and sanitized source JSON were
retained from the separately documented [#504 qualification](504-beacon-qualification.md).
No new raw-source request, production S3 write, or wide range scan was needed.

[505-compare-beacon.py](505-compare-beacon.py) performs only offline reads:

```sh
python3 docs/audit/505-compare-beacon.py RAW_JSON OLD_CHAIN_ROOT NEW_CHAIN_ROOT
```

It independently interprets the raw producer fee bytes in Python and decodes
all six source blobs; then it compares every populated output table against the
pre-change Parquet, normalizing only the declared fee/blob representation changes.
It checks the physical blob type with DuckDB and reads only the public saved
checkpoint height, never its opaque cursor.

| Result | Verified value |
|---|---|
| Beacon slot / execution block | 10597349 / 21385241 |
| Source fee bytes (Deneb big-endian) | `03 27 35 70 ed` |
| Decimal fee, wei per gas | `13542715629` |
| Blobs | 6 × 131,072 exact source bytes; each SHA-256 and index checked |
| Populated tables | blocks, attestations, execution_payload, blob_sidecars, withdrawals, bls_to_execution_changes |
| Compared rows / scalar cells | 153 / 2,747 |
| Unchanged cells excluding six blob representations and one fee representation | 2,740 |
| Saved public checkpoint height | 10597349 |

Local evidence is under
`/var/folders/mm/46m31dr97v1_k02y_ftl10lh0000gn/T/fireparq-505-live-okum7zcm/`;
`comparison.json` contains counts and blob hashes. Source is
`/tmp/fireparq-beacon-qualification-data/raw-10597349.json`, and the baseline is
that directory's `out-10597349/mainnet-cl/`. The first offline comparison exposed
DuckDB's UInt64-as-JSON-string rendering for blob indices; numeric normalization
fixed the comparator, which then passed on the same files without another live call.

This live check qualifies Deneb at this specific slot. Bellatrix/Capella and
later-fork byte order are established by pinned producer/dependency source and
all-five-body unit/Parquet tests, with the separately retained Fusaka fee vector.
It does not claim a new live check for every fork, cryptographic blob validation,
or exhaustive history coverage.
