# Tron #509 / PR #595: bounded source-backed qualification

Status: the reviewed two-read plan was executed successfully on 2026-09-25 at
21:26:50–51 UTC. All later comparison work is offline. The original single-request
StreamingFast quota failure remains unchanged. The approved capture
never calls a Firehose endpoint, retries exhausted quota, sends credentials,
changes outcome/filter policy, or writes production storage.

## Exact two-call contract

Use the official published Mainnet Solidity gRPC endpoint
`grpc.trongrid.io:50052`, on one credential-free plaintext channel (the official
Trident client supports the same transport). Both operations are unary reads:

1. `/protocol.WalletSolidity/GetBlockByNum2`, protobuf `NumberMessage{num:80000000}`.
   Save the exact response bytes as `block-extension.pb`; require a nonempty
   BlockExtention with header/raw_data and number exactly 80000000 before step 2.
2. `/protocol.WalletSolidity/GetTransactionInfoByBlockNum`, the identical NumberMessage.
   Save the exact response bytes as `transaction-info-list.pb`.

Limit: at most these two application RPC invocations, sequentially. No reflection,
health/head calls, transparent/application retries, wait-for-ready, HTTP fallback,
other endpoint, different height, individual receipt calls or pagination. Set
`grpc.enable_retries=0`, disable resolver service-config retry policy, 20-second
per-call deadlines, 32 MiB maximum received message per call and a 50-second
process deadline. Failure/denial/timeout/oversize stops the run; if call 1 fails
there is no call 2. No API key or ambient Pinax/StreamingFast credential is read.
The ordinary HTTP/2 connection exchange is not an additional chain RPC.

The two results are public node observations, with Solidity providing the node's
solidified view. They are not an independently cryptographic finality proof.
Plaintext response transport is documented explicitly; no secrets are sent.
No assumption of available unauthenticated access is made: denial remains a
qualification blocker, with no automatic alternative request.

Primary endpoint/API sources:
- https://developers.tron.network/docs/networks (Mainnet Solidity endpoint, checked 2026-09-25).
- https://developers.tron.network/docs/api (Solidity read view and gRPC service selection).
- https://github.com/tronprotocol/protocol/blob/2a678934da3992b1a67f975769bbb2d31989451f/api/api.proto#L781
  declares WalletSolidity and both methods (GetBlockByNum2 at861, GetTransactionInfoByBlockNum at953).
- https://github.com/tronprotocol/trident/blob/ee2b3456e708c92ee23c56dd412cf8b16d1059eb/core/src/main/java/org/tron/trident/core/ApiWrapper.java#L222
  selects plaintext without TLS configuration; its getBlockByNum/getTransactionInfoByBlockNum
  methods select these exact Solidity RPCs at1360 and1467.

## Input decoding and producer conversion

Compile a temporary descriptor from the pinned protocol snapshot above, including
its actual API/core/contract messages, plus the local sf.tron.type.v1 Block schema.
Do not deserialize through HTTP JSON: native protobuf preserves Any payload bytes,
field presence, original enum numbers, repeated order and unknown wire fields.
Save response sizes/SHA-256, descriptor/source hashes, exact methods/height, client
version, completion times and outcome, but no token or opaque Firehose cursor.
Artifacts live in a fresh local temporary directory.

Use an independently readable Python converter implementing only the existing
producer conversion pinned at streamingfast/firehose-tron
`d4095accc0dcc8fb4da8c1c1b1e8f4be33921df2`, rpc/fetcher.go:
https://github.com/streamingfast/firehose-tron/blob/d4095accc0dcc8fb4da8c1c1b1e8f4be33921df2/rpc/fetcher.go#L232

Before conversion, require equal transaction/receipt counts, unique 32-byte IDs,
and exact ordered equality of extension txid versus TransactionInfo.id. Do not
silently reorder receipts or tolerate missing/extra entries. Require each receipt's
blockNumber and blockTimeStamp to match the captured header. Verify block ID and
parent ID lengths/height prefixes, and the block ID hash suffix against exact
header raw_data wire bytes (`height.to_be_bytes() + SHA256(raw_data)[8:]`, as
[documented](https://developers.tron.network/docs/block)). Verify each transaction
ID from exact nested transaction raw_data wire bytes. Any ambiguous duplicate singular envelope or disagreement is
an explicit qualification failure, not a guessed repair. Parent height is 79999999.

The conversion preserves this producer's fields exactly:
- Block ID and every header field; parent number is header.number - 1.
- Transaction signature list, ref bytes/hash, expiration/timestamp, contracts,
  wrapper txid/result/code/message, constant_result, energy_used/energy_penalty.
- Attach the matching TransactionInfo by original position, retaining all receipt,
  log and internal-call fields and nested unknown data.
- Copy contracts/Any unchanged before the Rust mapper's three typed projections;
  never recreate Any payload bytes from their decoded field values.
- Canonical mapper identity follows producer convertBlock: hex block/parent IDs,
  exact timestamp milliseconds, parent number, and producer's height-minus-20 LIB.
  That LIB rule is labeled producer compatibility metadata, not finality evidence.

Do not substitute receipt.contractResult for extension.constant_result, or receipt
VM status for extension.result/code. Pinned java-tron
`b33eed89a6a424c498d4fb1b03ca2c86eddf4840` uses the same block2Extention helper in
Wallet and WalletSolidity; transaction2Extention sets wrapper true/SUCCESS.
The actual captured wrapper remains authoritative for conversion. This is separate
from the #550 question about execution-status filtering:
https://github.com/tronprotocol/java-tron/blob/b33eed89a6a424c498d4fb1b03ca2c86eddf4840/framework/src/main/java/org/tron/core/services/RpcApiService.java#L259
Solidity methods are at494 and898; FullNode counterparts at1487 and2536.

## Offline mapper and output qualification

Preserve original PR595 checkout/head4014e877. Use a new qualification worktree,
integrate current main (including owned Bytes/table_estimates), and retain the
existing #509 field semantics. Root owns publication and merge decisions.

Replay the converted single block through the production mapper and Parquet writer
for all five identifier encodings, fork-column on/off and failed-filter on/off.
Compare every new field against the original RPC objects using independent Python
protobuf decoding and the upstream full contract schemas, not the Rust decoder's
output. Check ordered contracts and exact Any, all eight receipt fields and nested
presence, contract_address/resMessage, transaction/contract/internal/call-value
positions, signed values/token strings, and source-wide block-log prefix indices.
Check every legacy column against a same-input current-main baseline, allowing
only the intentionally nullable empty-contract projection/schema change. Validate
all six tables' complete schema/values and flush/reset behavior; preserve source
hashes, table counts and a field-coverage matrix. Production local writer output
is sufficient for this field qualification; do not label it an actual Firehose
stream, live cursor, or protected-ingestion transport test.

Common types absent from this exact block, missing-message defaults, filtered
synthetic wrapper failures, unknown/malformed Any, or multi-contract cases remain
qualified only by the existing offline regressions. Do not widen the live sample
without another reviewed bound. Test any converter with hand-written wire fixtures
before the two reads; use no library generated by the mapper to derive expected
contract field values. Use complete current-main workspace/CI-example/build/fmt
checks under the shared Cargo wrapper, plus independent review of conversion and
comparison evidence. Existing #509 checks must survive the #515/#518 integration.

## Reviewed completion boundary

If both calls and comparisons succeed, report **live Tron RPC-backed mapper and
Parquet qualification** with pinned producer conversion. Firehose authentication,
transport, remote cursor continuity and receipt completeness on that inaccessible
provider remain unqualified; the earlier quota failure remains in the audit.
Root decides whether this satisfies #509's live-block criterion before changing
PR595 from draft or closing #509. #550's Tron outcome/filter policy stays open and
unchanged. A response failure, source inconsistency or absent required sample
coverage must be reported honestly and cannot be replaced by a wider automatic scan.

## Captured evidence and measured coverage

Original draft PR head `4014e87772b3fc5e3584efb795a93ef350547c2b` and its
checkout were preserved. Qualification runs use a separate checkout, integrated
through actual main `11ac02c`; the independent baseline is exactly that main.
Final integration also includes actual main `e4f990f` (#523 S3 maintenance),
which changes no Tron mapper/schema/replay or part-encoding path. The only
required mapper integration adaptations are the shared owned Bytes mapper
entry point, Bytes fixtures/typed decoder buffers, and both new table estimates.
No unrelated execution filter or transport behavior changed.

Native raw artifacts are retained locally under `/tmp/fireparq-509-rpc-80000000`:

| Artifact | Bytes | SHA-256 |
|---|---:|---|
| block-extension.pb | 90,756 | 6153f568a8b2e73a3fb6a966e17e292bd77cdbacb295035a983d220ea2b350b4 |
| transaction-info-list.pb | 32,043 | 04334ee5a3b760098d4d4db9d59ce97093e34fc0c8882b2d9f0bb7401c6e83d9 |
| block.pb (converted) | 118,341 | c2ddd99e3b6d1ec971dd8327acd6f414b5dd48487c8f402d415c927d00b0fc62 |

The descriptor SHA-256 is
`8e53fd91182e15c8be95832cfc77e59da5fa3ec5272737bb40d7e8fb93200c77`.
The pinned protocol source archive SHA-256 is
`ed03596104b001450ab318ade0b09b7397aa16efa6d641b48736691e6093293b`.
`capture.json` records the exact two calls, start/completion timestamps, sizes,
versions and success; `identity.json` labels height-minus-20 LIB as producer
compatibility rather than independently proved finality. No credentials or
opaque remote cursor were used or retained.

[The comparison report](509-tron-rpc-comparison.json) records 20 cases,
120 table inventories (80 nonempty parts), 14,600 row occurrences,
377,560 independently expected value comparisons and 136,720 unchanged legacy
value comparisons. Every legacy Arrow field definition is also compared, allowing
only the intended nullable first-contract projection. Every actual nonempty Parquet
part on both sides (140 files total) is also checked against its recorded Rust
Arrow schema: names/order, exact types including dictionary indices and ordering,
Binary fields and UTC millisecond timestamps, nullability, field metadata and
schema metadata. Empty tables have no physical files, so only their mapper
schemas/zero counts are recorded here; synthetic Rust tests round-trip them when
populated. These are repeated settings
of **one** live block: 1 block row, 336 transaction rows, 336 contract rows and
57 log rows per case; internal transaction/value tables are empty.

Contract coverage: Transfer 159, TransferAsset 18, TriggerSmart 57, DelegateResource
39, UnDelegateResource 63. The last two are retained as exact unsupported Any.
All 336 wrappers are true/SUCCESS; TransactionInfo result is SUCESS for all 336;
receipt result is DEFAULT for 279 and SUCCESS for 57. A wrapper's success is not
substituted for VM outcome, and DEFAULT is not relabeled successful execution.
The sample includes no failed receipt, internal call/value, multiple-contract,
missing-parameter or empty-contract case. Those remain synthetic-only evidence;
there were no extra calls to search for them. This satisfies the reviewed
live-block field-comparison boundary, not inaccessible Firehose transport/cursor
qualification or #550's separate execution policy.

## Offline reproduction

Dependencies used: `protobuf==7.36.2`, `grpcio==1.84.0`, `pyarrow==25.0.1` and
`googleapis-common-protos==1.75.4` for descriptor annotation imports. None are Rust
runtime dependencies. Compile the descriptor with `protoc` from the pinned
protocol source archive, whose full `api/api.proto` includes the upstream contract
schemas. Set `protocol_root` to its extracted root and `google_api_include` to the
Python site-packages directory containing `google/api/annotations.proto`:

```sh
protoc -I "$protocol_root" -I "$google_api_include" -I proto --include_imports \
  --descriptor_set_out=/tmp/fireparq-509-rpc.desc api/api.proto proto/tron.proto
uv run --with protobuf==7.36.2 --with grpcio==1.84.0 --with pyarrow==25.0.1 \
  python docs/audit/509-tron-rpc-tests.py --descriptor /tmp/fireparq-509-rpc.desc
uv run --with protobuf==7.36.2 python docs/audit/509-tron-rpc.py convert \
  --descriptor /tmp/fireparq-509-rpc.desc --output /tmp/fireparq-509-rpc-80000000
cargo build --locked -p blocks --example replay_tron
```

Build that identical example in both candidate and isolated main `11ac02c`
checkouts, copying each built executable before building the other. Use fresh
output paths and saved input; `replay_tron` refuses an existing output directory:

```sh
/path/to/baseline-replay --block /tmp/fireparq-509-rpc-80000000/block.pb \
  --output /tmp/tron-before
/path/to/candidate-replay --block /tmp/fireparq-509-rpc-80000000/block.pb \
  --output /tmp/tron-after
uv run --with protobuf==7.36.2 --with pyarrow==25.0.1 \
  python docs/audit/509-compare-tron-rpc.py --before /tmp/tron-before \
  --after /tmp/tron-after --raw /tmp/fireparq-509-rpc-80000000 \
  --descriptor /tmp/fireparq-509-rpc.desc --report /tmp/tron-comparison.json
cargo test --workspace --locked
cargo test -p blocks --example refresh_evm_golden --locked
cargo build --bin fireparq --locked
cargo fmt --all --check
```

The saved bytes, not another public request, are the reproduction input. The
`capture` subcommand performs network reads and is not part of this offline
reproduction. Tests substitute a fake channel and do not contact the endpoint.
All local Cargo commands/build-and-copy sequences were serialized using the
shared workspace lock; portable reproduction needs only ordinary Cargo in an
isolated build directory.

## Integrated check record

The first integrated full run on main `11ac02c` passed 1,043 workspace tests
with nine intentional helper/benchmark skips. After integrating actual main
`e4f990f`, all **1,067 workspace tests passed** with eleven intentional skips.
The exact CI example `refresh_evm_golden` passed its auth regression with one
intentional child skip. Locked binary build, formatting and bash/zsh/fish
completions passed. This was source head `a56886d`; later changes only strengthen
Python schema comparison and finish this evidence record.

Four converter tests passed before the two reads; all **six** final Python
converter/comparator fixture tests pass. The added schema regression writes a
real Parquet file and rejects wrong Binary/string or dictionary index types,
nullability, timestamp timezone, field metadata and schema metadata. The refreshed
raw comparison passed every schema/value/legacy check for all 20 cases. Independent
review found no production defect, requested the actual-Parquet schema check, and
accepted the explicit RPC-backed live-block boundary. Raw artifacts and
before/after outputs are temporary evidence; the scripts, hashes, full comparison
report and scope record are durable source.

Final publication also integrates main `270af16` (#527 shared AWS CLI structs).
This merge changes no Tron mapping/schema/part encoding. All 14 targeted Tron
matching tests passed, locked CLI build and bash/zsh/fish completions passed,
and formatting remained clean. The complete 1,067-test run above remains the
runtime qualification on the immediately preceding main; #527 also passed its
own complete CI suite. Fresh combined PR CI is required for the published head.
