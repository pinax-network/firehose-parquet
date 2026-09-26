# #507: NEAR status semantics, state-change attribution and encoding

Status: implemented from main `97dd244` and rebased onto `82f3205` (#620, #623).
PR review, CI and merge are pending.
The public-source qualification reuses the retained #506 capture, so no new
chain request was made and no StreamingFast endpoint was contacted.

## What the NEAR Firehose model carries

Checked against `proto/near.proto` and the pinned producer:

- `Block.state_changes` is the only state-change field. `IndexerShard` carries a
  chunk and receipt execution outcomes, but no state changes.
- Every published version of `near-firehose-indexer` writes an empty
  `Block.state_changes`. [507-producer-state-changes.py](507-producer-state-changes.py)
  read `src/codec/mod.rs` at all 46 commits that touched it (72e9359, 2021-08-20,
  through ebb0a2e, 2026-07-14), and every one contains `state_changes: vec![]`.
  The NEAR indexer does produce per-shard state changes (the retained NearData
  document has 221), but the producer drops them.
- `StateChangeCause` carries a hash for five causes. nearcore
  ([`types.rs`](https://github.com/near/nearcore/blob/74a6829e947512019773059d8b5905b4e6cb6236/core/primitives/src/types.rs#L185))
  gives `TransactionProcessing` a `tx_hash`, and gives
  `ActionReceiptProcessingStarted`, `ActionReceiptGasReward`, `ReceiptProcessing`
  and `PostponedReceipt` a `receipt_hash`. The checked-in protobuf names the field
  `tx_hash` for the last three, but it holds the receipt hash.
- A transaction's own `ExecutionOutcome` records its conversion into one
  receipt: `SuccessReceiptId(receipt)` or an inclusion `Failure`. The result of
  its contract calls comes later, through receipt outcomes in later blocks.
  `SuccessReceiptId` outcomes name the receipt that carries the result on.

## Changes

- **`transactions.status`** is kept, with its literal values. The schema
  comment and README now describe it precisely: it is the transaction's own
  outcome, not the final result.
- **`receipts.success_receipt_id`** is appended as nullable bytes, after every
  existing column including `fork_step`. It is set for `SuccessReceiptId`
  outcomes, which is the link needed to compute NEAR's final outcome across
  blocks.
- **Final outcome in the README.** One query computes NEAR's
  `FinalExecutionStatus` over a range: it starts at `converted_into_receipt_id`
  and follows `success_receipt_id` until an outcome that is not
  `SuccessReceiptId`, giving `SuccessValue`, `Failure`, `Unknown`, or `Pending`
  when the chain leaves the range. A second query reuses the lineage walk to find
  transactions with a failed receipt anywhere in their tree.
  - This follows nearcore's `get_execution_status`, which walks only the
    `SuccessReceiptId` chain.
  - A per-block derived column was rejected. Receipt chains almost always leave
    the block: none of the 70 receipts in the retained sample has a same-block
    origin, so such a column would be almost always NULL.
- **`state_changes` is rebuilt.** It is breaking, but no populated data exists
  from any known producer. The columns are:
  - `state_change_index` (position in the block list, keeping gaps for skipped
    entries);
  - `type` and `cause`, now `Dictionary(Int32, Utf8)` per the enum convention;
  - `cause_tx_hash` and `cause_receipt_hash`, split by nearcore semantics;
  - `account_id`;
  - `data_key` and `data_value`, which follow the table encoding and are NULL
    when absent (they replace the always-base64 `key_base64` and `value_base64`,
    which used `""` for absent values);
  - `AccountUpdate` `amount` and `locked` (decimal yoctoNEAR strings, like the
    other NEAR balances), `storage_usage` and `code_hash`, NULL for other kinds.

  Access-key permissions and contract code are still not materialized. The
  unused base64 helper is removed.
- **Encoding.** All new bytes columns use the mapper's identifier encoding, like
  the other NEAR hashes.

The mapper epoch is unchanged: existing row selection and values are unchanged,
and the schema identity changes.

## Not solvable in this repository

Populated state changes need the producer to fill `Block.state_changes` (or the
protobuf to gain a per-shard field). Until then, the `state_changes` table is
empty on StreamingFast NEAR output, and the README says so. Follow-up:
[#625](https://github.com/pinax-network/firehose-parquet/issues/625). The protobuf cause-field naming is also an upstream
schema issue; the mapper handles it.

## Qualification

**Producer-compatible replay.** This uses the retained #506 block 150000000:
NearData JSON `794e00c1…`, converted `block.pb` `8bec28c8…`, with 70 receipts,
one of which is a `SuccessReceiptId`. `replay_near` ran on baseline main
`97dd244` and on the candidate, across 5 encodings × failed filter × fork
column. [507-compare-near.py](507-compare-near.py) derives expected values from
the original JSON only, using the #509 comparator's byte encodings. All 20 cases
passed:

- 86,400 legacy values are unchanged in every table other than `state_changes`,
  and the legacy schemas are unchanged apart from the appended column.
- 1,400 `success_receipt_id` values match the JSON status IDs.
- `state_changes` is empty on both sides, and the candidate has the #507 layout.

**Real state changes, hypothetical producer.**
[507-near-state-changes.py](507-near-state-changes.py) copies that `block.pb`
and fills `Block.state_changes` with the document's 221 per-shard state changes,
in shard order and converted by nearcore's views. The changes are 111
`account_update`, 73 `data_update` and 37 `access_key_update`, with causes 42
`transaction_processing`, 158 `receipt_processing` and 21
`action_receipt_gas_reward`. The populated block is `752a9f09…` (138,503
bytes). It is labeled as a projection; it does not claim any provider emits
these rows.

Over 20 cases, all 53,040 `state_changes` values equal the JSON. That covers
indices, labels, both hash columns (including the receipt hashes carried in
`tx_hash`-named fields), keys and values in every encoding, and exact balance
strings. Every other table (4,480 rows) equals the producer-compatible replay.
[Report](507-near-comparison.json).

**README queries.** [507-final-status-query.py](507-final-status-query.py)
builds three synthetic blocks whose receipt chains cross block boundaries,
replays them, and runs the two README queries exactly as written, with only the
paths replaced. Results:

- `SuccessValue` for a transaction whose side receipt failed. The failure
  query reports one failed receipt for it.
- `Failure` for a transaction whose chain ends in a failed receipt.
- `Failure` for an inclusion failure.
- `Pending` for a transaction whose chain leaves the range.

**Tests.** Four state-change tests were added:

- all eight change kinds and five hashed causes, the skipped-entry index, NULLs
  and a present empty value;
- every encoding's physical types;
- the exact layout;
- `success_receipt_id` values.

The existing state-change and base58 alignment tests were updated. The chain
schema proof now also strips `receipts.success_receipt_id`. It leaves out the
restructured NEAR `state_changes` on both sides: its pinned pre-#550 digest was
recomputed on main `8462692` with that table left out. `cargo test --workspace
--locked` passes 1,169 tests with 0 failures and 14 ignored on the rebased
branch (main `82f3205`: 1,166; before the rebase 1,134 against 1,131). The
`refresh_evm_golden` example, the binary build and the shell completions pass.
The replay comparisons used main `97dd244` as the baseline; #620 and #623 changed
no NEAR mapper, schema or replay code.

**Limits.** Firehose transport on the StreamingFast-only NEAR endpoint is still
unqualified. The final-outcome queries are checked on synthetic chains; the
retained block has no chain that resolves within it.

## Migration

`receipts` gains a column and `state_changes` changes layout, so rebuild into a
fresh output root. Protected output refuses to resume the old schema inventory.
Old `state_changes` files, if any exist from another producer, need
`key_base64`/`value_base64` decoded when converting.
