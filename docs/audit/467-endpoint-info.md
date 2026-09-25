# Stable startup destinations when EndpointInfo fails (#467)

## Failure and chosen behavior

`FirehoseClient::info()` previously returned `None` for connection and RPC
failures. Ingestion then resolved `output/` instead of `output/<chain_name>/`,
looked for a different cursor, and could start writing a second dataset.

Both ingestion and partition building now require a successful Info response
with a nonempty chain name before resolving output, loading a cursor, or
creating a writer. Output resolution also rejects absent or empty metadata so
future callers cannot silently restore the old behavior.

Info makes at most three attempts, each bounding connection plus RPC to ten
seconds. Retry delays are 250 ms then 500 ms, giving an Info-only maximum of
30.75 seconds, plus scheduler overhead. The preceding endpoint healthcheck has
its own connection timeout and is not included in that budget. Transient
connection/RPC failures and timeouts retry. Authentication/permission errors,
invalid requests, exhausted quota, unsupported Info, and malformed successful
responses stop promptly.

## Why the suggested fallback is not used

The issue proposed falling back to a pinned network/block type or a cursor.
Inspection showed those do not establish complete metadata:

- A network alias is a catalog key and can point to an overridden endpoint.
  It does not prove that endpoint's canonical directory suffix.
- A block type identifies a family, such as EVM, rather than a network.
- Cursor discovery already depends on the missing suffix. Existing cursor
  metadata omits some empty/default values and has no completeness marker.
- Beyond the suffix, Info supplies the first streamable block and the chain
  identity used for encoding and Tron-profile inference.

Fabricating metadata could retain one path while changing another behavior.
The implementation therefore stops even when `--network`, `--block-type`,
`--start-block`, or `--cursor-override` is supplied. A future cached-metadata
mode needs a separate explicit contract and complete, endpoint-bound metadata.

This is a compatibility change for older servers without EndpointInfo: they
must expose the Info RPC before they can be used by `build`. Partition
building already required usable Info. No existing data or cursor is changed
when startup fails.

## Validation

- Retry tests verify transient recovery on the third attempt, increasing
  backoff, three-attempt exhaustion, hanging-request timeouts, immediate fatal
  rejection, unsupported Info, and empty/whitespace chain names.
- A real local gRPC server accepts the initial healthcheck but returns
  `Unimplemented` to Info. Actual CLI subprocesses cover explicit endpoint and
  block type, a network alias with an endpoint override, and partition building.
  All fail; no output directory is created, and a sentinel cursor remains
  byte-for-byte unchanged. The test confirms all three commands reached Info.
- `cargo test --workspace` passed: 696 tests, zero failures, three existing
  benchmarks ignored. The locked integration test was rerun after adding child
  process cleanup on timeout. Formatting and whitespace checks passed.
- A bounded live run on 2026-09-25 read Ethereum mainnet blocks 26049575 and
  26049576 from `eth.firehose.pinax.network:443`, with `--flush-blocks 1`.
  Info returned `chain_name=mainnet`; the run completed with two processed
  blocks, writing the data and cursor beneath the expected `mainnet/` suffix.
  No production output or cursor was modified.

The local integration failure is an unsupported-RPC response. Transient
exhaustion and recovery are exercised by the retry helper tests; the live run
qualifies the normal successful startup path. This change does not change
mapper schemas or claim crash-atomic publication of data and cursor files.
