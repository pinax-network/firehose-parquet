# Verifiability Hash Strategy and Normalization

This document defines the cross-chain hashing policy used by `fireparq verify` and the normalization rules applied before Merkle leaf hashing.

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

If a registry row exists with a different algorithm than runtime selection, verification reports a mismatch.

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
| Cosmos  | `blocks.hash` | `blocks.last_block_id_hash` |
| Near    | `blocks.hash` | `blocks.prev_hash` |

## Leaf Normalization Rules

Before hashing each row into a Merkle leaf:

- Iterate columns in schema order.
- For each column:
  - append length-prefixed column name bytes
  - append length-prefixed normalized value bytes
- Nulls are encoded as a sentinel (`<null>`).
- Primitive values are string-normalized.
- Binary values are hex-normalized for stable text/binary parity.
- Unsupported Arrow types fall back to Arrow display formatting.

This keeps leaf construction deterministic across file boundaries and storage backends.

## Bitcoin/Solana Onboarding Path

When Bitcoin and Solana protocol-level checks are added:

1. keep dataset-level hashing through the strategy abstraction above
2. add chain/table-specific protocol checks with `pass/fail/not_verifiable`
3. document any chain-specific canonicalization extensions if needed
4. preserve backwards compatibility by versioning or explicit migration when changing normalization semantics

## Current Scope

- Implemented strategy abstraction in verify runtime + registry metadata.
- Implemented `--hash-strategy` override (`auto`, `keccak256`, `sha256`).
- Existing EVM protocol checks continue to run unchanged.
