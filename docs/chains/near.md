# NEAR notes

Semantics of the NEAR tables beyond their columns, which the [NEAR schema reference](../schemas/near.md) lists. The failed-transaction flags are in [Failed transaction filtering](failed-transactions.md).

## Failed receipts

NEAR fails per receipt, not per transaction. A failed receipt's actions do not take effect, but its `gas_burnt` and `tokens_burnt` persist, and the logs it emitted before failing stay in its outcome. `receipts`, `receipt_actions` and `execution_logs` are written for every executed receipt whatever the failed-transaction flags say. `receipt_actions` and `execution_logs` carry the receipt's own outcome as `receipt_status` (`string`: `SuccessValue`, `SuccessReceiptId`, `Failure` or `Unknown`, the values of `receipts.status`); keep `receipt_status <> 'Failure'` for actions that took effect. The transaction filter only drops a transaction whose own outcome is `Failure`. A transaction's outcome is almost always `SuccessReceiptId` and says nothing about the receipts it later spawned; see [final transaction outcome](#final-transaction-outcome).

## Transactions, receipts, actions and logs

A NEAR transaction's own outcome records its inclusion and conversion into a receipt. Contract calls run when action receipts execute, often in later blocks and on other shards. `transactions` and `receipts` carry the keys to follow that chain (every column is in the [NEAR schema reference](../schemas/near.md)):

| Table | Column | Type | Meaning |
|---|---|---|---|
| `transactions` | `transaction_index` | `long` | Position in the block: chunks in shard order, then each chunk's transactions. Failed transactions left out by the filter keep their index. |
| `transactions` | `receipt_ids` | `array<string>` (base58) | The outcome's `receipt_ids`. |
| `transactions` | `converted_into_receipt_id` | `string` (base58), nullable | The receipt the transaction was converted into. Joins `receipts.receipt_id`. Null when the outcome has no receipt. |
| `transactions` | `status` | `string` | The transaction's **own** outcome: `SuccessReceiptId` once it was converted into a receipt, `Failure` if it failed inclusion. It is not the final result of the contract calls ([final outcome](#final-transaction-outcome)). |
| `receipts` | `success_receipt_id` | `string` (base58), nullable | For a `SuccessReceiptId` outcome, the receipt whose outcome becomes this receipt's result (the next link of NEAR's result chain). Null for other outcomes. |
| `transactions`, `receipts` | `tokens_burnt` | `string` | yoctoNEAR burnt for gas, as a decimal string. |
| `receipts` | `receipt_index` | `long` | Position of the execution outcome in the block: shards in order, then each shard's receipts. |
| `receipts` | `tx_hash` | `string` (base58), nullable | The originating transaction, when it is in the same block (see below). |
| `receipts` | `signer_id` | `string`, nullable | Signer of the transaction that started the receipt chain (`ReceiptAction.signer_id`). |
| `receipts` | `receipt_ids` | `array<string>` (base58) | Receipts created by this execution. |

Two tables hold what each executed receipt did:

- **`receipt_actions`**: one row per action, keyed by `(receipt_id, action_index)`, with `receipt_index`, `tx_hash`, `shard_id`, `predecessor_id`, `receiver_id`, `signer_id` and:

  | Column | Type | Set for |
  |---|---|---|
  | `action_kind` | `string` | every row: `CreateAccount`, `DeployContract`, `FunctionCall`, `Transfer`, `Stake`, `AddKey`, `DeleteKey`, `DeleteAccount`, `Delegate` (the labels of `transactions.actions`) |
  | `method_name` | `string` | `FunctionCall` |
  | `args` | `binary` | `FunctionCall`. Raw bytes, usually JSON: `decode(args)` in DuckDB |
  | `gas` | `long` | `FunctionCall`: the gas attached |
  | `deposit` | `string` | `FunctionCall`, `Transfer`: yoctoNEAR, as a decimal string |

  The payload columns are null for the other kinds. A `Delegate` row (NEP-366 meta-transaction) only records the kind: the delegated actions run in a receipt of their own and appear as that receipt's rows.
- **`execution_logs`**: one row per line of the outcome's `logs`, keyed by `(receipt_id, log_index)`, with `receipt_index`, `tx_hash`, `shard_id`, `executor_id` (the account whose code logged), `predecessor_id` and `log`. NEP-297 events such as NEP-141 (fungible tokens) and NEP-171 (NFTs) are the lines that start with `EVENT_JSON:`. Transaction outcomes have no logs: converting a transaction runs no contract code.

Both tables cover every receipt in `receipts`, including failed ones, and end with the receipt's own `receipt_status` ([failed receipts](#failed-receipts)):

```sql
-- NEP-141 events of receipts that did not fail
WITH events AS (
  SELECT block_num, executor_id AS token,
         TRY_CAST(substr(log, 12) AS JSON) AS event  -- the text after 'EVENT_JSON:'
  FROM delta_scan('output/near-mainnet/execution_logs')
  WHERE log LIKE 'EVENT_JSON:%'
    AND receipt_status <> 'Failure'
)
SELECT block_num, token, event->>'event' AS event, event->'data' AS data
FROM events
WHERE event->>'standard' = 'nep141';
```

`receipts.tx_hash` is only filled from the same block, which in practice means the receipt NEAR runs right away when a transaction's `signer_id` is also its `receiver_id`. Most receipts run in a later block, so most have a null `tx_hash`. Filling it from earlier blocks would make the output depend on where a run started. To find the originating transaction of every receipt, follow `converted_into_receipt_id` and `receipt_ids` over the range:

```sql
WITH RECURSIVE origin(receipt_id, tx_hash) AS (
  SELECT converted_into_receipt_id, hash
  FROM delta_scan('output/near-mainnet/transactions')
  WHERE converted_into_receipt_id IS NOT NULL
  UNION
  SELECT child.receipt_id, origin.tx_hash
  FROM origin
  JOIN (
    SELECT receipt_id AS parent_id, unnest(receipt_ids) AS receipt_id
    FROM delta_scan('output/near-mainnet/receipts')
  ) AS child ON child.parent_id = origin.receipt_id
)
SELECT r.receipt_id, origin.tx_hash
FROM delta_scan('output/near-mainnet/receipts') AS r
LEFT JOIN origin USING (receipt_id);
```

Receipts whose transaction or any intermediate lineage link is outside the available range stay unresolved. These queries assume a finalized dataset; on non-final output, read the [canonical live view](../non-final-streams.md#canonical-live-view) of each table instead.

## Final transaction outcome

A transaction's result is decided by later receipts, usually in later blocks, so one block cannot hold it and `transactions.status` does not report it. NEAR's final outcome (the RPC's `FinalExecutionStatus`) starts at the transaction and follows `SuccessReceiptId` links until an outcome that is not `SuccessReceiptId`: `SuccessValue` or `Failure`. Receipts off that chain, such as a failed cross-contract call whose callback handled the error, do not change it. Over a range of blocks:

```sql
-- Final outcome of each NEAR transaction: follow SuccessReceiptId links
WITH RECURSIVE chain(tx_hash, receipt_id, depth) AS (
  SELECT hash, converted_into_receipt_id, 0
  FROM delta_scan('output/near-mainnet/transactions')
  WHERE status = 'SuccessReceiptId'
  UNION ALL
  SELECT chain.tx_hash, r.success_receipt_id, chain.depth + 1
  FROM chain
  JOIN delta_scan('output/near-mainnet/receipts') AS r
    ON r.receipt_id = chain.receipt_id
  WHERE r.status = 'SuccessReceiptId'
),
last AS (
  SELECT tx_hash, arg_max(receipt_id, depth) AS receipt_id
  FROM chain
  GROUP BY tx_hash
)
SELECT t.hash,
       CASE WHEN t.status <> 'SuccessReceiptId' THEN t.status
            WHEN r.status IS NULL THEN 'Pending'  -- the chain continues past the range
            ELSE r.status END AS final_status
FROM delta_scan('output/near-mainnet/transactions') AS t
LEFT JOIN last ON last.tx_hash = t.hash
LEFT JOIN delta_scan('output/near-mainnet/receipts') AS r
  ON r.receipt_id = last.receipt_id;
```

To find transactions with a failed receipt anywhere in their receipt tree, including side calls, reuse the lineage walk above:

```sql
-- Transactions with a failed receipt anywhere in their tree
WITH RECURSIVE origin(receipt_id, tx_hash) AS (
  SELECT converted_into_receipt_id, hash
  FROM delta_scan('output/near-mainnet/transactions')
  WHERE converted_into_receipt_id IS NOT NULL
  UNION
  SELECT child.receipt_id, origin.tx_hash
  FROM origin
  JOIN (
    SELECT receipt_id AS parent_id, unnest(receipt_ids) AS receipt_id
    FROM delta_scan('output/near-mainnet/receipts')
  ) AS child ON child.parent_id = origin.receipt_id
)
SELECT origin.tx_hash, count(*) AS failed_receipts
FROM origin
JOIN delta_scan('output/near-mainnet/receipts') AS r USING (receipt_id)
WHERE r.status = 'Failure'
GROUP BY origin.tx_hash;
```

Both need the whole chain in the range; a chain that continues past its end is `Pending` or incomplete.

## State changes

`state_changes` has one row per entry of the Firehose block's `state_changes` list:

| Column | Type | Meaning |
|---|---|---|
| `state_change_index` | `long` | Position in the block's list (entries without a value or cause are skipped but keep their position) |
| `type`, `cause` | `string` | Change kind (`AccountUpdate`, `DataUpdate`, `AccessKeyUpdate`, ...) and cause (`TransactionProcessing`, `ReceiptProcessing`, `ActionReceiptGasReward`, ...) |
| `cause_tx_hash` | `string` (base58), nullable | The transaction of a `TransactionProcessing` cause |
| `cause_receipt_hash` | `string` (base58), nullable | The receipt of an `ActionReceiptProcessingStarted`, `ActionReceiptGasReward`, `ReceiptProcessing` or `PostponedReceipt` cause; joins `receipts.receipt_id` |
| `account_id` | `string` | The changed account |
| `data_key`, `data_value` | `string` (base58), nullable | `DataUpdate` key and value; `DataDeletion` key |
| `amount`, `locked` | `string`, nullable | `AccountUpdate` balances in yoctoNEAR, as decimal strings |
| `storage_usage`, `code_hash` | `long` / `string` (base58), nullable | `AccountUpdate` storage in bytes and contract code hash |

The base58 columns hold bytes in the identifier encoding. Columns that do not apply to a row's change kind are NULL. Access-key permissions and contract code are not materialized.

**The table is empty with the StreamingFast NEAR producer.** Every published version of `near-firehose-indexer` (checked from 2021-08 to 2026-07) writes an empty `Block.state_changes`, and the Firehose protobuf has no per-shard state-change field. The columns above are mapped and tested, including on a projection of real NEAR state changes, so they fill in if a producer supplies the list ([#625](https://github.com/pinax-network/firehose-parquet/issues/625)).

These added columns and tables require a fresh dataset or an explicit rebuild; protected ingestion refuses to resume an incompatible schema inventory. See the [bounded public-source comparison and its coverage limits](../audit/506-near-public-qualification.md) and the [#507 status and state-change record](../audit/507-near-status-state-changes.md).
