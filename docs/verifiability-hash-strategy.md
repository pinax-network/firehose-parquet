# Verifiability Hash Strategy and Normalization

This document defines the cross-chain hashing policy used by `fireparq verify`, the normalization rules applied before Merkle leaf hashing, and the versioned Merkle construction (`merkle_version`) used to build partition roots.

## Goals

- Keep verifiability deterministic and reproducible across local + S3 datasets.
- Allow per-chain hashing defaults while supporting explicit operator override.
- Make unsupported/deeper checks explicit (`not_verifiable`) instead of implicit pass.

## Runtime Strategy Selection

`verify` selects a hash strategy by:

1. `--hash-strategy` (if set to `keccak256` or `sha256`)
2. otherwise `auto` chain default, where the chain is the dataset's `firehose-parquet.block_type` file metadata (or `--chain` for files without it):
   - `evm` -> `keccak256`
   - `bitcoin` -> `sha256`
   - `solana` -> `sha256`
   - unknown chains -> `sha256` (safe default)

The selected algorithm is written to:

- verify report: `algorithm`
- merkle registry rows: `algorithm`

The Merkle construction version (currently `merkle_v2`) is written next to it:

- verify report: `merkle_version`
- merkle registry rows: `merkle_version`

If a registry row exists with a different algorithm or Merkle version than the runtime, the roots are not comparable and verification reports a mismatch (see [Merkle Versioning](#merkle-versioning)).

## Hash Strategy Matrix

| Chain    | Default hash | Rationale |
|----------|--------------|-----------|
| EVM      | `keccak256`  | Native EVM hashing convention |
| Bitcoin  | `sha256`     | Bitcoin ecosystem convention (SHA-based) |
| Solana   | `sha256`     | Stable, portable default for dataset integrity roots |

Note: Protocol-level consensus hashing details can differ from dataset Merkle policy. This matrix defines **dataset verifiability policy** for `verify`.

## Payload-Native Canonical Block Identity Sources

For chains that expose a protocol-native block hash or root in the payload, mapper canonical IDs should follow that payload field rather than relying on the Firehose envelope alone.

| Chain   | `block_id` source | `parent_id` source |
|---------|-------------------|--------------------|
| Beacon  | `blocks.root` | `blocks.parent_root` |
| Cosmos  | `blocks.hash` | `blocks.header.last_block_id.hash` |
| Near    | `blocks.hash` | `blocks.prev_hash` |

## Row Encoding (`merkle_v2`)

Each row is encoded into bytes before it is hashed into a leaf. Every value is encoded from its logical value by an explicit per-type rule. Arrow display formatting is never used, so an arrow-rs upgrade or a change in Arrow feature flags cannot change roots. The encoder is `firehose-parquet/src/verify/row_encoding.rs`.

`||` is byte concatenation and `u32le(n)` is `n` as a 4-byte little-endian integer.

```
encoded_row = for each column, in schema order:
                u32le(len(name)) || name || value
value       = 0x00                                   (null)
            | 0x01 || u32le(len(canonical)) || canonical
```

Column names are UTF-8. A null is only the `0x00` tag, so it can never collide with a string value (the `merkle_v1` null sentinel `<null>` did). Nulls follow Arrow logical nulls: a dictionary key that points at a null value, and every value of a `Null` column, are null.

### Canonical bytes per Arrow type

| Arrow type(s) | `canonical` |
|---------------|-------------|
| `Boolean` | ASCII `true` or `false` |
| `Int8`, `Int16`, `Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32`, `UInt64` | Base-10 ASCII, `-` for negatives, no leading zeros or `+` |
| `Float16`, `Float32`, `Float64` | Value widened exactly to binary64, then its IEEE-754 bits as 8 bytes little-endian. Every NaN becomes `0x7ff8000000000000`. `-0.0` and `0.0` stay distinct. |
| `Utf8`, `LargeUtf8`, `Utf8View` | The UTF-8 bytes |
| `Binary`, `LargeBinary`, `BinaryView`, `FixedSizeBinary(n)` | Lowercase hex ASCII, no `0x` prefix |
| `Date32` | Days since 1970-01-01, base-10 ASCII |
| `Timestamp(unit, tz)` | The instant as nanoseconds since the Unix epoch, base-10 ASCII, computed without overflow. The unit and timezone are not encoded. |
| `Dictionary(K, V)` | The canonical bytes of the referenced value, per `V`. Keys are not encoded. |
| `List(T)`, `LargeList(T)`, `FixedSizeList(T, n)` | `u32le(element_count)`, then each element encoded as a `value` (with its own null tag) |
| `Null` | Always null |

Any other Arrow type (for example `Decimal128`, `Struct`, `Map`, `Date64`, `Time*`, `Duration`, `Interval`) has no `merkle_v2` encoding. `verify` fails with an error naming the column and type instead of guessing. Adding a type means adding a rule here and bumping `merkle_version`.

### Consequences

- **Physical representation does not matter.** `Utf8`, `LargeUtf8`, `Utf8View` and `Dictionary(Int32, Utf8)` holding the same strings encode identically. The same holds for the binary family, for list variants, and for integer widths (`UInt32` 7 and `UInt64` 7). A reader or writer choosing a different Arrow representation for the same data does not change the root.
- **Timestamps hash the instant.** `Timestamp(Second, "UTC")` `1700000000` and `Timestamp(Millisecond, "UTC")` `1700000000000` both encode as `1700000000000000000`, so moving the canonical `timestamp` column from seconds to milliseconds (#491) does not change roots for identical block times. Millisecond precision is kept: `1700000000001` ms encodes differently.
- **Arrow types are not committed.** Only values are. As before, a hex string column and a binary column with the same bytes encode identically (text/binary parity), and so do a `UInt64` and a `Utf8` column of base-10 digits. A column rename changes the root, because names are encoded.

### Golden values

The encoder's unit tests pin these encodings (hex), computed independently from this spec:

| Value | Encoding |
|-------|----------|
| null (any type) | `00` |
| `Boolean` `true` | `01 04000000 74727565` |
| `UInt64` / `UInt32` / `Int64` `42` | `01 02000000 3432` |
| `Int64` `-7` | `01 02000000 2d37` |
| `Float64` / `Float32` `1.5` | `01 08000000 000000000000f83f` |
| `Float64` NaN | `01 08000000 000000000000f87f` |
| `Utf8` / `LargeUtf8` / `Utf8View` `"abc"` | `01 03000000 616263` |
| `Binary` / `BinaryView` / `FixedSizeBinary(2)` `0xdead` | `01 04000000 64656164` |
| `Date32` `19723` (2024-01-01) | `01 05000000 3139373233` |
| `Timestamp(Second)` `1700000000` = `Timestamp(Millisecond)` `1700000000000` | `01 13000000` + ASCII `1700000000000000000` |
| `Dictionary(Int32, Utf8)` `"abc"` | same as `Utf8` `"abc"` |
| `List<UInt8>` `[1, 2, 255]` | `01 18000000 03000000 01 01000000 31 01 01000000 32 01 03000000 323535` |
| `List<UInt64>` `[1, null]` | `01 0b000000 02000000 01 01000000 31 00` |
| `List<UInt64>` `[]` | `01 04000000 00000000` |

## Merkle Construction (`merkle_v2`)

Each partition root is built from its rows in scan order (parquet files sorted by path, rows in file order). `H` is the selected hash strategy (`keccak256` or `sha256`) and `||` is byte concatenation.

| Step | Hash input |
|------|------------|
| Leaf (one per row) | `H(0x00 \|\| encoded_row)`, where `encoded_row` follows the row encoding above |
| Interior node | `H(0x01 \|\| left \|\| right)` |
| Odd node | the last node of an odd-sized level is promoted to the next level unchanged (never paired with itself) |
| Tree root | the single node left after reduction; `H("")` (hash of empty input) for a partition with zero rows |
| Partition root | `H(0x02 \|\| row_count \|\| tree_root)`, where `row_count` is the partition row count as `u64` little-endian |

The registry and report store the partition root as lowercase hex.

Why these rules:

- The `0x00`/`0x01` prefixes separate leaf hashes from interior-node hashes, so an interior node can never be presented as a row leaf (second-preimage resistance, as in RFC 6962).
- Promoting the odd node instead of duplicating it, plus committing `row_count`, means a duplicated trailing row changes the root: rows `[a, b, c]` and `[a, b, c, c]` produce different roots.

## Merkle Versioning

`merkle_version` identifies the full row-to-root algorithm: row encoding, leaf and node hashing, and the root commitment. Any change to those rules must bump it, so older roots are detected instead of being compared against roots they can never match.

| Version | Status | Construction |
|---------|--------|--------------|
| `merkle_v1` | Legacy, no longer computed | Unprefixed leaves `H(encoded_row)` and nodes `H(left \|\| right)`; the odd last node was paired with itself; no row count. A duplicated trailing row did not change the root. Values were string-normalized with a `<null>` sentinel, and types other than integers, floats, booleans, strings and binary fell back to Arrow display formatting. Timestamps with a named timezone such as `"UTC"` could not be formatted and were all hashed as the constant `<unsupported>`. |
| `merkle_v2` | Current | Row encoding and construction above. |

Registries written before `merkle_version` existed have no `merkle_version` column. `verify` reads every row of such a file as `merkle_v1`.

`verify` only computes `merkle_v2` roots. It does not recompute `merkle_v1` roots, because that construction cannot detect a duplicated trailing row. When a scanned partition's registry row has a different `merkle_version`:

- without `--update-registry`, the partition is reported as a `mismatch` with the error `merkle version mismatch: registry=merkle_v1 runtime=merkle_v2; ...`, the run exits 1, and the registry is left unchanged
- with `--update-registry`, the row is replaced by the computed `merkle_v2` root and the partition is reported as `updated` (same error text, previous root in `expected_root`)

Rows for partitions the run did not scan are kept as they are, and are written back with an explicit `merkle_version = merkle_v1` label. The rebuild procedure is in the [artifact runbook](verifiability-artifact-runbook.md#migrating-a-legacy-merkle_v1-registry).

## Bitcoin/Solana Onboarding Path

When Bitcoin and Solana protocol-level checks are added:

1. keep dataset-level hashing through the strategy abstraction above
2. add chain/table-specific protocol checks with `pass/fail/not_verifiable`
3. document any chain-specific canonicalization extensions if needed
4. preserve backwards compatibility by bumping `merkle_version` (and documenting the migration) when changing normalization semantics

## Current Scope

- Implemented strategy abstraction in verify runtime + registry metadata.
- Implemented `--hash-strategy` override (`auto`, `keccak256`, `sha256`).
- Implemented the versioned `merkle_v2` construction, recorded as `merkle_version` in the registry and report.
- Existing EVM protocol checks continue to run unchanged.
