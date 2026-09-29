# EVM notes

Semantics of the EVM tables beyond their columns, which the [EVM schema reference](../schemas/evm.md) lists. The failed-transaction flags are in [Failed transaction filtering](failed-transactions.md).

## Persistent state changes of failed transactions

A failed EVM transaction still changes chain state: the sender pays for gas, the fee recipients are paid, and the sender's nonce goes up. Everything else it did is rolled back. So `balance_changes` and `nonce_changes` only reconcile with on-chain balances and nonces when failed transactions are included, and only if their rolled-back changes are left out.

For a failed or reverted transaction, `fireparq` writes:

| Table | Rows written |
|---|---|
| `transactions` | the transaction, with `status` `FAILED` or `REVERTED` |
| `logs` | none (failed transactions have no receipt logs) |
| `calls` | every call (`status_failed` / `state_reverted` tell you what happened) |
| `gas_changes` | every gas change (the gas was consumed and paid for) |
| `balance_changes` | root-call changes with reason `GAS_BUY`, `GAS_REFUND`, `REWARD_TRANSACTION_FEE`, or `INCREASE_MINT` (OP Stack deposits keep their mint when they fail) |
| `nonce_changes` | the sender's nonce increment (the root call's earliest nonce change), plus one per accepted EIP-7702 authorization |
| `code_changes` | at most one per accepted EIP-7702 authorization (the delegation) |
| `storage_changes`, `account_creations` | none |

This follows the rule documented on `TransactionTrace.status` in `proto/ethereum.proto`. Rolled-back transfers and storage writes of failed transactions are not written. Successful transactions keep every state change, including those of calls that were reverted inside them. The `persisted` column tells them apart (see below).

Resuming protected EVM authority that records failed transactions as excluded keeps excluding them, so one output does not mix both modes. Legacy cursor-only datasets need a new empty output root. `fireparq` logs a warning. Pass `--exclude-failed-transactions` to keep that and silence the warning. To switch semantics, rebuild into a fresh output root with an absent mirror; `--cursor-override` cannot change protected output.

## Which call recorded a change, and whether it persisted

The transaction-scoped change tables (`balance_changes`, `nonce_changes`, `code_changes`, `storage_changes`, `account_creations`, `gas_changes`) carry the transaction and call that recorded each change:

| Column | Type | Meaning |
|---|---|---|
| `tx_index` | `long` | The transaction's index in the block. Joins `transactions.index`. |
| `call_index` | `long` | The recording call's Firehose index (starts at 1). Joins `calls.call_index` with `tx_hash`. |
| `state_reverted` | `boolean` | The recording call's `state_reverted` flag, the same value as in `calls`. |
| `persisted` | `boolean` | Whether the change is part of chain state after the transaction. Not on `gas_changes`: gas is consumed even in reverted calls. |

`persisted` is `NOT state_reverted` for successful transactions. For failed or reverted transactions it is always `true`: only their persistent changes are written, and those come from the root call, whose `state_reverted` is `true`. To rebuild state from the change tables, filter on `persisted`:

```sql
-- Balance of each address at the end of the range
SELECT address, new_value AS balance
FROM delta_scan('output/mainnet/balance_changes')
WHERE persisted
QUALIFY row_number() OVER (PARTITION BY address ORDER BY block_number DESC, ordinal DESC) = 1;
```

Order persisted changes by `(block_number, ordinal)`; ordinals are unique within a block. Ordinals of changes in reverted calls may be `0`.

The `system_*` change tables have a nullable `call_index`: the index of the system call that recorded the change, or `NULL` for block-level changes such as beacon-chain withdrawals. System call indexes are not unique within a block: the system calls that run after the transactions (EIP-7002 and EIP-7251 requests) restart at 1. Join a change to its system call on the ordinal range as well:

```sql
SELECT c.*, s.address AS system_contract
FROM delta_scan('output/mainnet/system_storage_changes') c
JOIN delta_scan('output/mainnet/system_calls') s
  ON s.block_number = c.block_number AND s.call_index = c.call_index
 AND c.ordinal BETWEEN s.begin_ordinal AND s.end_ordinal;
```

The `logs` table holds receipt logs only, so logs emitted by reverted calls are never in it.

## Receipt log indices and RPC joins

The `logs` table maps `TransactionReceipt.logs`. It contains receipt logs, so
logs from reverted calls are absent. The two index columns have different scopes:

| Column | Source | Meaning |
|---|---|---|
| `log_index` | Firehose `Log.index` | Transaction-relative Firehose log index. Different transactions can have the same value. The protobuf only guarantees this field at `EXTENDED` detail. |
| `block_index` | Firehose `Log.blockIndex` | Block-relative receipt log index, corresponding to JSON-RPC `logIndex` after converting the RPC hexadecimal quantity to an integer. |
| `tx_index` | Firehose transaction index | Position of the transaction in its block; corresponds to RPC `transactionIndex`. |

For RPC joins, use the same chain, `block_id`/RPC `blockHash`, and
`block_index`/RPC `logIndex`, with matching hash encoding. Including `tx_hash`
provides an additional transaction check. `log_index` alone is not an RPC join
key. Block hashes distinguish forks at the same block number; reversible output
also requires applying NEW/UNDO events to select the canonical logs.

```sql
-- Export RPC-compatible index names from finalized EVM output.
SELECT block_id, tx_hash,
       block_index AS rpc_log_index,
       tx_index AS rpc_transaction_index,
       log_index AS firehose_transaction_log_index
FROM delta_scan('output/mainnet/logs');
```

`fireparq` preserves the indices supplied by Firehose and does not renumber logs
after filtering. Do not infer a transaction-local position from a default zero
when the upstream source omits `Log.index` at a lower detail level.

## Tables that can be absent

`gas_changes` and `account_creations` are extended tables populated only from
upstream call arrays. A sampled range can contain no rows even when transactions
and calls are present. The September 2026 audit's Ethereum mainnet block-version-5
samples contained no rows for either table; this is a bounded observation, not
a guarantee about every network, provider or block version.

- `account_creations` is deprecated upstream: the checked-in Ethereum protobuf
  says account-creation records are unsupported from block version 4. Do not use
  an absent table to conclude that no contracts or accounts were created.
- `gas_changes` contains explicit upstream gas-change records. Missing rows do
  not mean zero gas usage; transaction/receipt gas fields provide separate data.
- `--without-extended` disables both tables regardless of source contents.

`build` creates every table of the stream at its first start, so a table
without rows is an empty Delta table: `delta_scan` returns an empty relation,
and the table has no data files or `date=` directories. With
`--without-extended` the extended tables are not part of the stream and do
not exist.

## Withdrawals, access lists and EIP-7702 authorizations

Three tables hold block and transaction data that is not a column of `blocks` or `transactions`. They are written at both detail levels, including with `--without-extended`. Access-list and authorization rows follow their transaction: they are written for failed transactions too, and dropped with `--exclude-failed-transactions`. Columns and types are in the [EVM schema reference](../schemas/evm.md).

| Table | One row per | Notes |
|---|---|---|
| `withdrawals` | beacon-chain withdrawal in the block (Shanghai and later) | `index` is the global withdrawal index; `amount_gwei` is in gwei, not wei |
| `access_lists` | entry of a transaction's access list (EIP-2930) | `access_index` is the position in the list; `storage_keys` may be empty |
| `set_code_authorizations` | authorization of a `SET_CODE` transaction (EIP-7702) | `authorization_index` is the position in the list; `chain_id` is a decimal string (`0` allows any chain); `address` is the delegation target and `authority` the recovered signer |

- A withdrawal also appears in `system_balance_changes` with reason `WITHDRAWAL`, in wei and without the validator. `sum(amount_gwei) * 1e9` per block and address equals the summed balance delta there.
- `authority` is `NULL` when it can't be recovered from the signature, and those authorizations are `discarded`. `address` is `NULL` on the few testnet blocks where Firehose did not record it.
- `discarded = true` means the chain skipped the authorization as invalid. Accepted authorizations take effect even when the transaction fails (see failed transactions above).

## Header, signature, blob and ordinal columns

`blocks`, `transactions`, `calls`, `system_calls` and `logs` carry Firehose header, signature, blob and ordinal fields as they are, with bytes in the output encoding and big integers as decimal strings like the other value columns. The [EVM schema reference](../schemas/evm.md) lists each column, type and nullability.

- Fields introduced by a fork are `NULL` in blocks and transactions from before it: `withdrawals_root` (Shanghai), `blob_gas_used` / `excess_blob_gas` and `parent_beacon_root` (Cancun), and `requests_hash` (Prague).
- The blob columns of `transactions` are `NULL` for non-blob transactions, and `blob_hashes` is an empty list. `transactions.logs_bloom` comes from the receipt and is `NULL` without one.
- `failure_reason` on `calls` and `system_calls` is `NULL` when the call did not fail; `address_delegates_to` is the EIP-7702 delegation target of the called account.
- `begin_ordinal` / `end_ordinal` on transactions and calls, and `ordinal` on logs, give the execution order in the block. Ordinals are unique within a block, so `(block_number, ordinal)` orders every log, call and state change of a block. They are not reliable for anything inside a reverted call.
