# #550: Tron, Antelope and NEAR failed-transaction rules

Status: implemented from main `8462692` and rebased onto `050f9ec`. PR review,
CI and merge are pending.
Solana's slice merged earlier ([record](550-solana-execution-context.md)), so
with this change every chain named by #550 has a documented, implemented,
tested and source-qualified rule. EVM follows #494/#547, and Bitcoin and Beacon
have no failed transactions.

Each chain gets its own rule because each source reports failure differently.
No generic `reverted` flag is added. Parent-outcome columns follow the Solana
convention: they are appended after every existing column, including the
optional `fork_step`, and describe the parent transaction or receipt, not an
individual instruction.

## Tron

### Diagnosis

The failed filter tested the Firehose wrapper `Transaction.result`. The
producer copies it from `TransactionExtention.result`
([firehose-tron `d4095acc` fetcher.go#L275](https://github.com/streamingfast/firehose-tron/blob/d4095accc0dcc8fb4da8c1c1b1e8f4be33921df2/rpc/fetcher.go#L275)).
java-tron's `transaction2Extention` sets that field to true/`SUCCESS` for every
included transaction
([RpcApiService.java#L259](https://github.com/tronprotocol/java-tron/blob/b33eed89a6a424c498d4fb1b03ca2c86eddf4840/framework/src/main/java/org/tron/core/services/RpcApiService.java#L259)).
The filter therefore never removed anything: all 336 wrappers in the #509 sample
and all 447 in this sample are true. VM failures are recorded in
`TransactionInfo`. A runtime error sets `result = FAILED` with a `resMessage`,
and the receipt carries the contract result.

On revert, the VM clears the call's logs and rejects its internal transactions
([VMActuator.java#L240](https://github.com/tronprotocol/java-tron/blob/b33eed89a6a424c498d4fb1b03ca2c86eddf4840/actuator/src/main/java/org/tron/core/actuator/VMActuator.java#L240)).
The fee, energy and bandwidth are still charged.

### Rule

- A transaction succeeds when its wrapper `result` is true, `TransactionInfo.result`
  is `SUCESS`, and its receipt result (if any) is `DEFAULT` (non-VM contracts) or
  `SUCCESS`.
- Unknown enum values are failures. A false wrapper remains a failure, as before.
- A transaction without `TransactionInfo` or without a receipt has no recorded
  failure.
- By default, failed transactions and all their rows are excluded. This matches
  the documented non-EVM default; include-by-default is an EVM-only product
  decision.
- `transaction_success` (non-null Boolean) is appended to `transactions`, `logs`,
  `internal_transactions`, `contracts` and `internal_call_values`.
- The literal wrapper `result`/`code`, the `receipt_*` columns, `fee`,
  `internal_transactions.rejected` and the original `transaction_index` /
  `block_log_index` positions are unchanged.
- `transactions.contract_address` is NULL when `TransactionInfo.contract_address`
  is empty. The sample shows that the field holds the called or created smart
  contract, so plain transfers and other system contracts have none.

### Evidence

**Bounded RPC capture.** The capture used the #509 plan's official public
Solidity endpoint `grpc.trongrid.io:50052`. It ran on one credential-free
plaintext channel, with no retries and a 20-second deadline per call.
[550-tron-rpc.py](https://github.com/pinax-network/firehose-parquet/blob/v1.0.1/docs/audit/550-tron-rpc.py) reuses the reviewed
[509 converter](https://github.com/pinax-network/firehose-parquet/blob/v1.0.1/docs/audit/509-tron-rpc.py) unchanged: a check reproduced the #509 block
`c2ddd99e…` byte for byte. It searched up to 10 listed heights, using one
`GetTransactionInfoByBlockNum` per height, and stopped at the first `REVERT`
receipt:

| Height | Info bytes | Transactions | Failed |
|---|---:|---:|---:|
| 81000000 | 99,207 | 642 | 0 |
| 82000000 | 46,949 | 471 | 0 |
| 83000000 | 21,014 | 257 | 0 |
| 84000000 | 41,792 | 455 | 0 |
| 85000000 | 48,872 | 447 | 3 (1 `REVERT`, 2 `OUT_OF_ENERGY`) |

The immediate `GetBlockByNum2` for 85000000 was rejected with HTTP 429 (rate
limit), and nothing retried it. A separate `fetch-block` invocation made exactly
one more call about two and a half minutes later; it succeeded, and the
converted block validated. The capture made 7 application calls in total. There
was no Firehose call, no StreamingFast endpoint and no credential. `capture.json`
records every call and the first attempt's failure.

| Artifact | Bytes | SHA-256 |
|---|---:|---|
| block-extension.pb | 121,973 | `4f5514fd9a8bde0fd1842d546f9776c20dc9c7ef5c3def5bff75606486d12315` |
| transaction-info-list.pb | 48,872 | `06b1890cf297c31feaee0e53ef04f33afc7fa04bee0f05c917c3594036755d18` |
| block.pb (converted) | 164,952 | `af7dc781876e8c0a3fe1c9da44a83169299ef86691650ef30eb45e07aadc1444` |

The three failures all have a true/`SUCCESS` wrapper, `TransactionInfo`
`FAILED`, no logs and no internal transactions, and they paid fees of 500,
1,207,400 and 200 sun. On main they are written by default, which is the bug.

**Offline comparison.** [550-compare-tron.py](https://github.com/pinax-network/firehose-parquet/blob/v1.0.1/docs/audit/550-compare-tron.py) generates the
expected rows from the upstream protocol messages with the #509 generator, then
applies the rule above independently of the Rust mapper. It covers baseline
(main `8462692`) and candidate `replay_tron` across 5 encodings × fork column ×
failed filter, 20 cases. All passed:

- 21,140 row occurrences and 549,390 raw-source value comparisons, including
  21,120 `transaction_success` values.
- 528,270 unchanged legacy values.
- 7,300 empty contract addresses that became NULL (365 per case).
- Actual Parquet schema checks on every part. The legacy schema is unchanged
  apart from the appended column.

By default, `transactions` and `contracts` fall from 447 rows to 444, and the
other tables are unchanged. With failed transactions included, every table
matches the baseline row for row.
[Report](550-tron-comparison.json).

**Limits.** This qualifies RPC-backed mapper semantics through the pinned
producer conversion. Firehose transport on the StreamingFast-only Tron endpoint
remains unqualified. The sample contains no failed call with internal
transactions; rejected internal rows and call values are covered by synthetic
tests only.

## Antelope

### Diagnosis

The default kept only receipt status `EXECUTED`. The pinned producer
([firehose-antelope `238611ac` consolereader.go#L531](https://github.com/pinax-network/firehose-antelope/blob/238611ac29d6006b2f7457f364bf5731ca0868d6/codec/consolereader.go#L531))
writes a failed deferred transaction as two traces:

1. The **failed deferred trace**: `SOFTFAIL` (synthesized when missing), with the
   trace-level exception. Its operations are already reverted.
2. The **`onerror` handler trace**, with `failed_dtrx_trace` set. `SOFTFAIL`
   means the handler succeeded and its effects persisted. `HARDFAIL` means it
   failed or none ran, and the producer resets its operations (lines 595–615).

The old filter dropped successful handlers and `DELAYED` (scheduled)
transactions. The README row wrongly said the filter uses "action trace status".

### Rule

- A trace succeeds when it has no trace-level exception and its status is
  `EXECUTED`, `SOFTFAIL` or `DELAYED`.
- The default keeps successful traces. It excludes failed deferred traces,
  `HARDFAIL`, `EXPIRED`, `NONE`/`UNKNOWN`/`CANCELED`, unmapped statuses, and
  anything with an exception.
- `transactions.transaction_success` is appended. `actions` and `db_ops` append
  the parent `transaction_status` (`Dictionary(Int32, Utf8)`, the labels of
  `transactions.status`) and `transaction_success`.
- `transactions.status` stays literal `Utf8`.

This also fixes legacy cursor inspection: `bytes_encoding=auto` on an Antelope
cursor now resolves to `hex_no_prefix`, the encoding Antelope output uses,
instead of `hex`.

### Evidence

**Local scan.** A Pinax EOS scan of blocks 50000000–50002999 (March 2019),
with failed transactions included and written locally, found:

| Receipt status | Traces |
|---|---:|
| `EXECUTED` | 91,967 |
| `SOFTFAIL` | 577 |
| `EXPIRED` | 371 |
| `DELAYED` | 14 |
| `HARDFAIL` | 2 |

Every `SOFTFAIL` pair has the shape above. The failed traces had exceptions and
no database operations, and the handlers had no exception. One handler
hard-failed (block 50002529). One user-delayed transaction was `DELAYED` in
block 50002747 and `HARDFAIL` without a handler in block 50002749.

**Offline comparison.** Five blocks were captured from Pinax:

| Block | Bytes | SHA-256 | Roles |
|---|---:|---|---|
| 50000009 | 203,872 | `3eb09087a866320ace50d42aab7d2574518f8cedf99fb9d1f11079c3a18cef84` | failed deferred + successful `onerror` |
| 50000014 | 180,623 | `9a61db6d87e213b9cdc224f1c1058b531596c0c02742df5d5764d903fba283fc` | 2 `EXPIRED` |
| 50002529 | 146,399 | `60eca1ded081c44c4c4e09b5c2caf521a9bdbb7b82c198e02dc9dfab91fcb9a0` | failed deferred + `HARDFAIL` `onerror` |
| 50002747 | 349,142 | `5560fe2765d19f0a0114a30cc13a4e1d2f31d6ae2b95dc1953bdbc20ecc5cb89` | `DELAYED` |
| 50002749 | 175,157 | `c6bc28fa3bbee1196b42bf91da9057a6defa15d7a9289b7cc27f5d9966b3a9e4` | the same transaction `HARDFAIL` |

[550-compare-antelope.py](https://github.com/pinax-network/firehose-parquet/blob/v1.0.1/docs/audit/550-compare-antelope.py) derives selection and labels
from the raw protobufs. It compared baseline and candidate
[`replay_antelope`](../../blocks/examples/replay_antelope.rs) output over 20
cases, and all passed:

- 34,800 row occurrences, 66,240 outcome values and 953,200 legacy values.
- With failed transactions included, every legacy value equals the baseline.
- By default, the rows of `EXECUTED` traces equal the baseline's default rows.
  The two newly selected traces (the successful `onerror` and the `DELAYED`
  transaction) and their one action row equal the baseline's included rows for
  those traces.

[Report](550-antelope-comparison.json).

**Live end-to-end run.** The main and candidate release binaries built three
ranges from Pinax EOS (50000008–50000010, 50002527–50002530 and
50002746–50002750), in default and included modes, with local output. DuckDB
`EXCEPT ALL` in both directions over the legacy columns found:

- With failed transactions included, every table matches.
- By default, main has no rows missing from the candidate. The candidate adds
  exactly the successful `onerror` trace (with 1 action) in the first two ranges
  and the `DELAYED` trace in the third.

[Summary](550-eos-live-summary.json).

**Limits.** No `HARDFAIL`/`SOFTFAIL` trace in the sample has database
operations, because the producer reverts them, and no sampled handler wrote
any. Filtered blocks (`filtering_applied`) are covered by synthetic tests only.

## NEAR

### Diagnosis

NEAR fails per receipt. The transaction filter only drops a transaction whose
own outcome is `Failure` (an inclusion failure). Receipts, `receipt_actions`
and `execution_logs` of failed receipts were always written, and the child rows
carried no status. In nearcore, failed receipt changes roll back while gas
rewards and tokens burnt commit
([runtime lib.rs#L1078](https://github.com/near/nearcore/blob/74a6829e947512019773059d8b5905b4e6cb6236/runtime/runtime/src/lib.rs#L1078)).
The outcome keeps the logs emitted before the failure.

### Rule

- A failed receipt's actions did not take effect, its `gas_burnt` and
  `tokens_burnt` persist, and its logs are retained.
- The failed-transaction flags do not gate receipts: receipts and their child
  rows are always written.
- `receipt_actions` and `execution_logs` append `receipt_status`
  (`Dictionary(Int32, Utf8)`), the same value as `receipts.status`.
- Transaction status semantics and `state_changes` attribution belong to #507.
  The pinned producer emits no state changes.

### Evidence

The retained #506 capture of block 150000000 (NearData JSON `794e00c1…`,
converted `block.pb` `8bec28c8…`) contains 70 receipts: 68 `SuccessValue`,
1 `SuccessReceiptId` and 1 `Failure` (a `Delegate` action with no logs). No new
request was made. [550-compare-near.py](https://github.com/pinax-network/firehose-parquet/blob/v1.0.1/docs/audit/550-compare-near.py) keys the expected
statuses by receipt ID from the original JSON. It compared baseline and
candidate `replay_near` across 20 cases, and all passed:

- 4,480 row occurrences and 2,480 `receipt_status` values.
- 83,920 unchanged legacy values in all seven tables.
- The failed receipt's rows are present with and without
  `--include-failed-transactions`.

The README's updated NEP-141 event query, which filters on `receipt_status`
instead of joining `receipts`, returns the #506-recorded 8 `ft_transfer` and
34 `ft_mint` events on this output.

[Report](550-near-comparison.json).

**Limits.** A failed receipt with retained logs is covered by synthetic tests
only. Firehose transport on the StreamingFast-only NEAR endpoint remains
unqualified.

## Tests and schema proof

The added tests are:

- 5 Tron outcome tests, including an outcome truth table over wrapper, info
  result and receipt values (unknown enums included), plus default and included
  selection.
- 4 Antelope tests covering every mainnet role, with filtered and unfiltered
  traces.
- 3 NEAR tests, including flag independence and a failed receipt with logs.
- 1 cursor test.
- 1 schema proof.

The schema proof is `removing_the_550_outcome_columns_restores_the_pre_550_schemas`
in `blocks/src/chain/tests.rs`. It strips exactly the appended columns and
recomputes the pre-#550 digest over every family and option (1,280 mapper
configurations), which proves that no existing field, type, nullability or order
changed. The current full digest is pinned separately.

After rebasing onto main `050f9ec` (#615, #616, #618, #619),
`cargo test --workspace --locked` passes 1,131 tests with 0 failures and 14
ignored, against 1,117 on that main; the 14 added tests are those listed above.
The `refresh_evm_golden` example, the binary build and the shell completions
also pass. The mapper and schema code did not conflict; only
`docs/releases/unreleased.md` needed a manual merge.

Before the rebase, from main `8462692`, each code commit was green on its own:
the workspace suite passed 1,109 tests after the Tron commit, 1,114 after the
Antelope commit and 1,117 after the NEAR commit. Each commit pins its own full
schema digest, and the pre-#550 restoration proof holds at every step. The
replay comparisons above used main `8462692` as the baseline; the rebase changed
no mapper, schema or replay code.

## Reproduction

The baseline is main `8462692`. Build the replay examples in the baseline and
candidate checkouts and copy each binary before building the other.
`replay_antelope` is new, so copy it into the baseline checkout unchanged; it
uses only mapper APIs that exist on main. Dependencies are `protobuf==7.36.2`,
`grpcio==1.84.0` (Tron capture only) and `pyarrow==25.0.1`.

```sh
cargo build --locked -p blocks --example replay_tron --example replay_antelope --example replay_near

# Tron: the capture is the only network step, and it was run once (see above).
# The descriptor is the #509 one (see 509-tron-rpc-qualification.md).
uv run --with protobuf==7.36.2 --with grpcio==1.84.0 python docs/audit/550-tron-rpc.py \
  capture --descriptor tron.desc --output /fresh/tron --heights 81000000 82000000 83000000 84000000 85000000
replay_tron --block /fresh/tron/block.pb --output /fresh/tron-before   # baseline binary
replay_tron --block /fresh/tron/block.pb --output /fresh/tron-after    # candidate binary
uv run --with protobuf==7.36.2 --with pyarrow==25.0.1 python docs/audit/550-compare-tron.py \
  --before /fresh/tron-before --after /fresh/tron-after --raw /fresh/tron \
  --descriptor tron.desc --report tron.json

# Antelope: a directory of Pinax EOS payloads <block_num>.pb and a manifest.json
# with each block's Firehose identity and SHA-256 (the hashes above).
protoc -I proto --include_imports --descriptor_set_out=antelope.desc proto/antelope.proto
replay_antelope --capture /eos --output /fresh/eos-before   # baseline binary
replay_antelope --capture /eos --output /fresh/eos-after    # candidate binary
uv run --with protobuf==7.36.2 --with pyarrow==25.0.1 python docs/audit/550-compare-antelope.py \
  --before /fresh/eos-before --after /fresh/eos-after --raw /eos \
  --descriptor antelope.desc --report antelope.json

# NEAR: the retained #506 capture.
replay_near --block block.pb --output /fresh/near-before   # baseline binary
replay_near --block block.pb --output /fresh/near-after    # candidate binary
uv run --with pyarrow==25.0.1 python docs/audit/550-compare-near.py \
  --before /fresh/near-before --after /fresh/near-after --json neardata.json --report near.json
```

The five EOS payloads were captured with a temporary, uncommitted helper. It
made one bounded final-block stream request per block to
`eos.firehose.pinax.network`, using the shell's Pinax credentials, and recorded
each payload's Firehose identity and hash. The raw captures are retained
locally; they are not checked-in fixtures.

## Migration

Tron, Antelope and NEAR schemas change, and Tron and Antelope default row
selection changes. Protected output binds the exact schema inventory and refuses
to resume old datasets, so rebuild into a fresh output root with an absent
mirror. Old files lack the outcome columns, and a missing column is not `false`.

## Process note

During the Antelope status scan, one run used a relative `--output`. The binary
auto-loads the main checkout's `.env` from a parent directory (dotenvy searches
ancestors), and that file sets `S3_BUCKET` and AWS credentials. As a result, the
run wrote a 200-block March 2019 EOS scan to `s3://pinax/target/550/eos-scan/data/eos/`,
along with a released owner record at the bucket root. This was reported
immediately and nothing else was modified. With the maintainer's approval, the
coordinator verified read-only that only those 12 objects had changed, then
deleted them, which returned the bucket to its prior state. The coordinator is
filing the dotenv parent lookup and the relative-output S3 fallback as an audit
issue. Every other run used absolute local paths with the S3 and AWS variables
cleared (`S3_BUCKET`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT_URL`, `AWS_REGION` and `AWS_SESSION_TOKEN`),
and test runs used the same environment.
