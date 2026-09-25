# Verifiability Hash Strategy and Normalization

This document defines the cross-chain hashing policy used by `fireparq verify`, the normalization rules applied before Merkle leaf hashing, and the versioned Merkle construction (`merkle_version`) used to build partition roots.

## Goals

- Keep verifiability deterministic and reproducible across local + S3 datasets.
- Allow per-chain hashing defaults while supporting explicit operator override.
- Make unsupported/deeper checks explicit (`not_verifiable`) instead of implicit pass.

## Runtime Strategy Selection

`verify` selects a hash strategy by:

1. `--hash-strategy` (if set to `keccak256` or `sha256`)
2. otherwise `auto` chain default:
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

## Leaf Normalization Rules

Before hashing each row into a Merkle leaf:

- Iterate columns in schema order.
- For each column:
  - append length-prefixed column name bytes
  - append length-prefixed normalized value bytes
- Length prefixes are `u32` little-endian byte counts.
- Nulls are encoded as a sentinel (`<null>`).
- Primitive values are string-normalized.
- Binary values are hex-normalized for stable text/binary parity.
- Unsupported Arrow types fall back to Arrow display formatting.

This keeps leaf construction deterministic across file boundaries and storage backends.

## Merkle Construction (`merkle_v2`)

Each partition root is built from its rows in scan order (parquet files sorted by path, rows in file order). `H` is the selected hash strategy (`keccak256` or `sha256`) and `||` is byte concatenation.

| Step | Hash input |
|------|------------|
| Leaf (one per row) | `H(0x00 \|\| encoded_row)`, where `encoded_row` follows the leaf normalization rules above |
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
| `merkle_v1` | Legacy, no longer computed | Unprefixed leaves `H(encoded_row)` and nodes `H(left \|\| right)`; the odd last node was paired with itself; no row count. A duplicated trailing row did not change the root. |
| `merkle_v2` | Current | Construction above. |

Registries written before `merkle_version` existed have no `merkle_version` column. `verify` reads every row of such a file as `merkle_v1`.

`verify` only computes `merkle_v2` roots. It does not recompute `merkle_v1` roots, because that construction cannot detect a duplicated trailing row. When a scanned partition's registry row has a different `merkle_version`:

- the partition is reported as a `mismatch` with the error `merkle version mismatch: registry=merkle_v1 runtime=merkle_v2; ...`, and the run exits non-zero
- without `--update-registry`, the registry is left unchanged
- with `--update-registry`, the row is replaced by the computed `merkle_v2` root

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
