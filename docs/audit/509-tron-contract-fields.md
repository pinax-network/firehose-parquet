# Tron contract, receipt and internal-value fields (#509)

## Diagnosis and representation

The mapper kept only the first contract's enum, and used enum value zero
(`AccountCreateContract`) when the contract list was empty. It discarded every
contract parameter, the resource receipt, receipt contract address/error bytes,
and internal transaction call-value pairs. Logs had only a per-transaction index.

This change keeps the existing four tables and adds two ordered child tables:

- `contracts`: one row per source contract, keyed by canonical block identity,
  `transaction_index`, `tx_hash` and `contract_index`. It records the generated
  enum label, original `contract_type_id` (including unknown numbers), permission
  ID, and the exact Any type URL and payload. Raw payloads are Binary. Missing
  Any messages are null; present empty payloads are non-null empty bytes.
- `internal_call_values`: one row per source `callValueInfo`, keyed by canonical
  block identity, transaction index/hash, `internal_index` and `call_value_index`.
  `call_value` is the original Int64 and `token_id` the original string. Order,
  repeated token IDs, empty token IDs, zero and signed source values are preserved;
  no denomination conversion or execution-success inference is applied.

The contracts table decodes three common protobuf types into nullable fields:

| Contract | Decoded fields |
|---|---|
| TransferContract | owner_address, to_address, amount |
| TransferAssetContract | owner_address, to_address, amount, asset_name |
| TriggerSmartContract | owner_address, contract_address, data, call_value, call_token_value, token_id |

Addresses use the selected address encoding; hashes use the established reserved
hash encoding (hex without a prefix for the Tron profile). Opaque `parameter`,
`asset_name` and call `data` are Binary. Original integer values are never scaled.
Unknown/unsupported contracts retain their enum number, label and Any envelope
but have null decoded fields. Known types with no parameter also have null decoded
fields. A known parameter with a conflicting type URL or invalid wire data fails
before *any* row of that selected block is appended; earlier buffered blocks remain
intact. Empty but syntactically valid protobuf messages retain their scalar defaults.

Keeping a child table avoids silently dropping additional contracts if a producer
supplies more than one. The old `transactions.contract_type` remains a first-contract
projection, now null when there are no contracts. Existing `transactions.fee` behavior
is unchanged, including zero when TransactionInfo is absent; the new receipt fields
explicitly distinguish missing receipts from present zero values.

`transactions` adds the original `transaction_index`, all eight nullable
`receipt_*` fields (energy usage/fee/origin/total, net usage/fee, result label and
energy penalty total), nullable receipt `contract_address` and Binary `res_message`.
The latter two are null only when TransactionInfo is absent; a present empty byte
field remains empty. The enum comes from the generated protocol name.

`logs` and `internal_transactions` add the source transaction index. `logs` also
adds `block_log_index: UInt64`, the prefix sum of **all source transaction logs**
plus the per-transaction position. Failed filtering does not renumber transaction,
contract, internal-value or block-log indices. Existing `log_index` stays per
transaction. All new tables follow the existing failed-transaction filter; effects
of included failed transactions remain source records, not asserted committed effects.
Every list position is checked for representability before block mutation.

## Primary wire-schema sources

`proto/tron_contract.proto` contains only the three wire-compatible message
projections needed for decoding, pinned to the revision already referenced by the
repository's Tron definitions: `tronprotocol/protocol` revision
`2a678934da3992b1a67f975769bbb2d31989451f`.

- [TransferContract](https://github.com/tronprotocol/protocol/blob/2a678934da3992b1a67f975769bbb2d31989451f/core/contract/balance_contract.proto)
- [TransferAssetContract](https://github.com/tronprotocol/protocol/blob/2a678934da3992b1a67f975769bbb2d31989451f/core/contract/asset_issue_contract.proto)
- [TriggerSmartContract](https://github.com/tronprotocol/protocol/blob/2a678934da3992b1a67f975769bbb2d31989451f/core/contract/smart_contract.proto)

`TransactionInfo`, `ResourceReceipt`, contract enum values and `CallValueInfo`
come directly from the existing `proto/core/Tron.proto`. Generated Cargo outputs
were not edited. Raw Any payloads remain available to decode unsupported types
or future fields without interpreting them as a known contract.

## Offline validation

Six added regression tests cover all five encodings and both fork-column modes,
multiple contract types in one transaction, unknown enum/raw Any preservation,
all receipt values, maximum signed integer values, repeated/empty internal token
IDs, missing messages versus present defaults, original positions after failed
filtering, reset behavior and whole-block rejection after a malformed later
transaction. Every populated new and affected table round-trips through the
production Parquet writer/reader. Hard-coded wire vectors independently pin
all three supported contract field tags rather than constructing those expectations
with the same generated serializer. The seven existing Tron tests remain applicable,
with their table-count contract updated from four to six.

The initial draft integration included main `d417e0c`. All **863 workspace tests**
passed with five intentional child/helper skips, including all thirteen Tron
tests and the cross-chain schema round-trip contract. The capture-auth example
passed its additional regression; formatting and locked workspace build passed.
The initial full run correctly rejected an empty newly added table in the shared
schema fixture; adding a real internal call-value fixture restored the intended
all-tables coverage. Independent review found no blocking code defect and prompted
the additional wire vectors and missing-parameter cases. Passing offline checks
does not satisfy the issue's live criterion.

## Original Firehose transport blocker

On 2026-09-25, one TLS `sf.firehose.v2.Stream/Blocks` request was made to
`mainnet.tron.streamingfast.io:443` for finalized block `80000000` (inclusive
RPC start/stop), with a 20-second RPC deadline and 25-second process bound.
The subprocess received only the repository's previously confirmed StreamingFast
Bearer credential through an explicit provider variable. No Pinax key, ambient
credential fallback, token value or private claims were printed or saved.

The provider returned zero payload bytes and exit status 66:

```text
Code: Unknown
Message: Quota exceeded: billable egress bytes quota exceeded
(quota '5368709120', current '15762311347')
```

The empty temporary response was removed. No retry, alternative credential,
expanded range, or production storage write was attempted. The provider's public
[Tron network page](https://thegraph.market/networks/tron) confirms this endpoint;
the current Pinax endpoint catalog does not list a Tron Firehose service.

The Firehose transport/cursor boundary remains unqualified; this account was never
retried. The separately approved native RPC alternative below supplies the issue's
live-block **mapper and Parquet** evidence without asserting that provider access
or remote cursor continuity has been restored. Issue #550's execution outcome
policy remains open and this PR preserves its existing wrapper-based filter.

## Native RPC-backed qualification (2026-09-25)

Exactly two credential-free unary reads to the officially documented public
Solidity endpoint returned block **80000000** and its ordered transaction receipts.
No Firehose retry, additional height or receipt request was made. The native
responses were independently converted using the pinned Firehose producer's
existing field assignments, after verifying the exact block and transaction ID
hashes, ordered receipt identities, height and timestamp. The full bounds, primary
sources, conversion fixtures, hashes and offline reproduction are in
[the qualification record](509-tron-rpc-qualification.md).

The integrated branch includes main `11ac02c` (#519 shared Parquet properties),
#515 table estimates and #518 owned Bytes decoding. The same saved block was
replayed through the production mapper and local Parquet writer on that main
baseline and the candidate across all **20** combinations of five encodings,
failed filtering and fork columns. Independent Python decoding of the upstream
RPC/contract schemas matched **377,560 values** across **14,600 row occurrences**.
All **136,720 legacy values**, order and legacy schema fields matched the baseline;
the documented first-contract nullability change is the only permitted legacy
schema difference. These counts repeat the same block across settings; they are
not additional live blocks or distinct transactions.

The block contains 336 transactions/contracts and 57 logs: TransferContract 159,
TransferAssetContract 18, TriggerSmartContract 57, DelegateResourceContract 39 and
UnDelegateResourceContract 63. Thus all three typed projections and unsupported
raw Any retention have real source coverage. All wrappers are true/SUCCESS, all
TransactionInfo results are the protocol's `SUCESS`, and receipt results are
DEFAULT 279 / SUCCESS 57. DEFAULT is retained as source data; wrapper success
is not presented as proof of TVM execution. No outcome/filter policy was changed.

This sample has **no internal calls/call values, failed receipts, multiple-contract
transactions, missing parameters or empty contract lists**. Those paths retain
explicit synthetic regression coverage only. All eight receipt fields and exact
Any payloads were compared even when their values were zero or empty. Empty
internal tables produced no data files; their full schemas and zero counts were
recorded. No claim is made about live Firehose transport, protected CLI ingestion,
a real saved remote cursor, independent finality or absent sample categories.

Four converter/wire/fake-channel tests passed before the live reads; a fifth
independent-comparator fixture also exercises internal signed values, absent
receipts/contracts and wrapper filtering. Final integrated workspace/build/CI
example validation is recorded in the qualification record.

## Migration

New child tables, appended columns and nullable `transactions.contract_type`
change the schema and verification inventory. Use a fresh output root/rebuild,
or explicitly reconcile old files into a separately validated dataset. Old files
cannot reconstruct dropped contract/receipt/call values without the source.
The first-contract convenience column does not represent every contract; join
the contracts table by canonical block identity plus transaction index/hash.
Verification roots change, and old/new roots are not directly interchangeable.
