# Issue #505: Beacon value semantics

## Scope and diagnosis

`execution_payload.base_fee_per_gas` now stores an exact unsigned decimal string
in wei per gas, and `blob_sidecars.blob` stores the source bytes as Arrow Binary
regardless of `--encode-bytes`. Hashes, roots, addresses, commitments, and proofs
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

- Old fees follow `--encode-bytes` (Binary or encoded Utf8). Decode that encoding
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

Final full-suite and bounded live qualification results are recorded below once
completed. No production S3 writes or wide range scans are part of this change.
