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

Measured samples and final integrated validation will be recorded before PR
publication. No speedup is claimed until those checks finish.
