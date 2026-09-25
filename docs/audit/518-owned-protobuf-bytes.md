# Issue #518: share owned protobuf byte buffers

## Change and compatibility

The CLI now transfers the owned Firehose `Any.value` allocation into `Bytes`
once. Genesis timestamp buffering clones shared handles, and all eight chain
mappers decode from the owned buffer through `BlockMapper::map_block_bytes`.
Generated protobuf `bytes` fields use `Bytes`, so their nested payload slices
share the original allocation while a block is mapped. Cosmos also consumes
owned `TxRaw` body/auth buffers in its second decoding layer.

The original `map_block(&[u8], ...)` method remains available. Its decoding must
copy byte fields because the borrowed input cannot outlive the call. External
mapper implementations inherit a compatible default for the new owned method;
implementing an override is optional. Both entry points run the same mapping
body, keeping validation order and partial-error behavior unchanged.

This changes generated Rust field types from `Vec<u8>` to `Bytes`. Callers that
construct protobuf messages can transfer a vector with `.into()`, read bytes
through `.as_ref()`, and request a mutable copy explicitly with `.to_vec()`.
Protobuf wire definitions and every Parquet schema/value remain unchanged, so
this change needs no mapper epoch or dataset migration.

The sharing boundary is chain protobuf decoding, not the whole pipeline:
externally generated `prost_types::Any` payloads still use `Vec<u8>`, protobuf
strings still allocate, and Arrow builders still copy their values. A retained
small `Bytes` field can keep the complete input allocation alive. The CLI drops
the decoded block after mapping; it does not cache those decoded structures.
Bitcoin's protobuf has string representations and gains no byte-field sharing.

## Correctness checks

- All eight chains, five byte encodings, both fork-step settings, EVM extended
  mode, Solana votes/routing modes: owned and borrowed paths produce equal
  complete `RecordBatch` maps, schemas, row counts and transaction counts across
  two flush/reset cycles. Malformed protobuf errors also agree.
- Pointer-range checks prove representative EVM, Solana, Beacon blob and Cosmos
  byte fields share the input allocation, rather than merely producing equal
  copied data. Retained fields remain valid after dropping the decoded message
  and original payload handle. `Vec` ownership transfer preserves its pointer.
- A separate Cosmos test follows a block transaction through both SDK decoding
  layers and proves the nested `ModeInfo` bytes still share the block allocation.
- Existing schema/Parquet round trips, golden chain values, protected ingestion
  receipts/recovery, bootstrap and reversible stream tests remain required.

## Offline measurement protocol

`blocks/examples/bench_chain_decode.rs` reads one caller-supplied retained raw
protobuf block, moves it into `Bytes` before timing, and measures two separate
loops: protobuf decode/drop; and decode, map, flush/drop. It uses binary output
and includes failed transactions, EVM extended tables and Solana vote decoding.
There are ten warm-up mapping iterations. No network, storage writes, Parquet
encoding, ingestion concurrency, or credentials are involved.

Each run also reports an input SHA-256, all output table row counts, schemas and
Arrow IPC stream digests (including values and schemas) outside the timed region. Comparisons must match
these exactly. A synthetic default envelope identity is held equal across both
versions; this is a mapper/decode benchmark, not live transport qualification.
The Beacon input is a semantic protobuf re-encoding of the previously captured
Firehose JSON block, including its six 131072-byte blobs; it is not the original
wire serialization. EVM and Solana use retained raw protobuf captures.

For the old revision, copy the identical example into its worktree and replace
its one `map_block_bytes(raw.clone(), identity, None)` call with
`map_block(raw.as_ref(), identity, None)`. Its generated `Vec` fields still copy
while decoding a `Bytes` input. The new binary supports `--borrowed` as a control.
Build and preserve each executable before switching worktrees; clean the
`firehose-protos` package in both debug and release profiles when sharing a
Cargo target directory, because generated protobuf outputs depend on the source
worktree. Never overlap the timing run with builds or other benchmarks.

```sh
cargo build --release --locked -p blocks --example bench_chain_decode
./target/release/examples/bench_chain_decode --chain evm \
  --block blocks/tests/fixtures/evm-mainnet/block.pb --iterations 500
```

## Measured results and integration

Apple M1 Max, macOS 26.5.1 arm64, Rust 1.93.1, release profile. Five
rotated samples per mode and fixture ran under a shared process lock excluding
other builds/benchmarks. Each sample used 200 iterations for EVM/Solana and
5,000 for Beacon; the initial shorter Beacon samples are retained separately.
The baseline was main `9379883`; the optimized benchmark source was `ac21c97`,
including #602's transport configuration. The subsequent #603 auth refactor
changes no mapper or benchmark code. Exact source/input/binary hashes, schemas,
row counts, Arrow IPC digests and all samples are in [518-benchmark.json](518-benchmark.json).

Medians per block (lower time is better):

| Retained block | Decode/drop, before → owned | Decode/map/flush/drop, before → owned | Mapping speedup |
|---|---:|---:|---:|
| EVM 26049575 | 2.0022 → 1.3501 ms | 3.3824 → 1.9228 ms | 1.76× |
| Solana 300000000 | 4.3864 → 3.5710 ms | 7.6632 → 5.8378 ms | 1.31× |
| Solana 300000001 | 4.3321 → 3.5941 ms | 7.2022 → 5.4321 ms | 1.33× |
| Beacon 10597349 | 0.0696 → 0.0308 ms | 0.2084 → 0.0971 ms | 2.15× |

All output schemas and Arrow IPC streams matched across before, owned and
borrowed modes: 5,049 EVM rows, 9,765/9,949 Solana rows, and 153 Beacon rows.
Every one of the 60 initial runs and 15 longer Beacon runs passed these checks.
The new borrowed control still copies at decode and is included in the raw
results; it is not the production CLI path. These measurements establish gains
on retained fixtures, not an end-to-end ingestion or universal chain speedup.

Reproduce the comparison with preserved binaries (input flags can select a
subset; use `--iterations 5000` for the Beacon-only run):

```sh
python3 docs/audit/518-benchmark.py --before /path/to/before \
  --after /path/to/after --evm blocks/tests/fixtures/evm-mainnet/block.pb \
  --solana /path/to/300000000.pb --solana /path/to/300000001.pb \
  --output /path/to/results.json
```

Full workspace validation on main `b364681` plus this change: **1,010 passed,
0 failed, 9 ignored**. CI's separately selected capture example passed one test
with one subprocess-helper ignore. Workspace build and formatting passed.
After integrating current main `39d49f6`, focused gRPC tests passed **50/0/1**,
owned-buffer tests **3/0/0**, and actual protected ingestion CLI tests **7/0/0**;
the workspace build and formatting passed again. The PR's CI checks the complete
integrated tree. Independent production review found no blocker and requested
the nested Cosmos pointer assertion, which is included and passing.
