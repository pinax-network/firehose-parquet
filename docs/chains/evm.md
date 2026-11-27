# EVM notes

Semantics of the EVM tables beyond their columns, which the [EVM schema reference](../schemas/evm.md) lists. The failed-transaction flags are in [Failed transaction filtering](failed-transactions.md).

## Persistent state changes of failed transactions

A failed EVM transaction still changes chain state: the sender pays for gas, the fee recipients are paid, and the sender's nonce goes up. Everything else it did is rolled back. So `balance_changes` and `nonce_changes` only reconcile with on-chain balances and nonces when failed transactions are included (the default) and rows with `persisted = false` are left out.

For a failed or reverted transaction, `fireparq` writes:

| Table | Rows written |
|---|---|
| `transactions` | the transaction, with `status` `FAILED` or `REVERTED` |
| `logs` | none (failed transactions have no receipt logs) |
| `calls` | every call (`status_failed` / `state_reverted` tell you what happened) |
| `gas_changes` | every gas change (the gas was consumed and paid for), with `--with-gas-changes` |
| `balance_changes` | root-call changes with reason `GAS_BUY`, `GAS_REFUND`, `REWARD_TRANSACTION_FEE`, or `INCREASE_MINT` (OP Stack deposits keep their mint when they fail) |
| `nonce_changes` | the sender's nonce increment (the root call's earliest nonce change), plus one per accepted EIP-7702 authorization |
| `code_changes` | at most one per accepted EIP-7702 authorization (the delegation) |
| `storage_changes`, `account_creations` | none |

This follows the rule documented on `TransactionTrace.status` in `proto/ethereum.proto`. Rolled-back transfers and storage writes of failed transactions are not written. Successful transactions keep every state change, including those of calls that were reverted inside them. The `persisted` column tells them apart (see below).

An output whose resume state records failed transactions as excluded (built with `--exclude-failed-transactions`) keeps excluding them, so one output does not mix both modes, and `fireparq` logs a warning. Pass `--exclude-failed-transactions` to keep that and silence the warning. To include them, rebuild into a new empty output root with an absent cursor mirror; `--cursor-override` is only accepted with `--dry-run` and cannot change protected output.

## Which call recorded a change, and whether it persisted

The transaction-scoped change tables (`balance_changes`, `nonce_changes`, `code_changes`, `storage_changes`, `account_creations`, `gas_changes`) carry the transaction and call that recorded each change:

| Column | Type | Meaning |
|---|---|---|
| `tx_index` | `long` | The transaction's index in the block. Joins `transactions.index`. |
| `call_index` | `long` | The recording call's Firehose index (starts at 1). Joins `calls.call_index` with `tx_hash`. |
| `state_reverted` | `boolean` | The recording call's `state_reverted` flag, the same value as in `calls`. |
| `persisted` | `boolean` | Whether the change is part of chain state after the transaction. Not on `gas_changes`: gas is consumed even in reverted calls. |

`persisted` is `NOT state_reverted` for successful transactions. For failed or reverted transactions it is always `true`: only their persistent changes are written, and those come from the root call, whose `state_reverted` is `true`. To rebuild state from the change tables, filter on `persisted`, and add the block-level changes of the `system_*` tables (withdrawals, block rewards):

```sql
-- Balance of each address at the end of the range
SELECT address, new_value AS balance
FROM (
  SELECT block_number, ordinal, address, new_value
  FROM delta_scan('output/mainnet/balance_changes')
  WHERE persisted
  UNION ALL
  SELECT block_number, ordinal, address, new_value
  FROM delta_scan('output/mainnet/system_balance_changes')
)
QUALIFY row_number() OVER (PARTITION BY address ORDER BY block_number DESC, ordinal DESC) = 1;
```

Order persisted changes by `(block_number, ordinal)`; ordinals are unique within a block. Ordinals of changes in reverted calls may be `0`, and the root call of a failed transaction is reverted.

The `system_*` change tables have a nullable `call_index`: the index of the system call that recorded the change, or `NULL` for block-level changes such as beacon-chain withdrawals. System call indexes are not unique within a block: the system calls that run after the transactions (EIP-7002 and EIP-7251 requests) restart at 1. Join a change to its system call on the ordinal range as well:

```sql
SELECT c.*, s.address AS system_contract
FROM delta_scan('output/mainnet/system_storage_changes') c
JOIN delta_scan('output/mainnet/system_calls') s
  ON s.block_number = c.block_number AND s.call_index = c.call_index
 AND c.ordinal BETWEEN s.begin_ordinal AND s.end_ordinal;
```

## Receipt log indices and RPC joins

The `logs` table maps `TransactionReceipt.logs`. It contains receipt logs, so
logs from reverted calls are absent. The index columns have different scopes:

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

## Opt-in tables

Five extended tables are written only with a flag, off by default, because
current EVM Firehose blocks never fill them:

| Tables | Flag | Why current blocks have none |
|---|---|---|
| `gas_changes`, `system_gas_changes` | `--with-gas-changes` (`WITH_GAS_CHANGES=true`) | [Gas-change tables](#gas-change-tables) |
| `account_creations`, `system_account_creations` | `--with-account-creations` (`WITH_ACCOUNT_CREATIONS=true`) | [Account-creation tables](#account-creation-tables) |
| `system_nonce_changes` | `--with-system-nonce-changes` (`WITH_SYSTEM_NONCE_CHANGES=true`) | [System nonce changes](#system-nonce-changes) |

Without its flag, a group's tables are neither declared nor written, and the
matching records of older block versions are ignored.

A dataset's tables are fixed when it is created, so the flags must match the
dataset on every run:

| Dataset | Run with | Otherwise `build` refuses to start with |
|---|---|---|
| Created with extended output by v1.5.1 or earlier | all three flags | `this dataset has the EVM tables ..., which build writes only with ...; set them on every run of this dataset` |
| Created with extended output by v1.6.0 | `--with-account-creations` and `--with-system-nonce-changes`, plus `--with-gas-changes` if it was created with it | the same, naming each missing flag |
| Created by a later version | the flags it was created with | the same for a missing flag, or `this dataset has no EVM tables ...; run it without ...` for one it was not created with |

The refusal names every flag that differs, before any Blocks request. Before
upgrading, set the flags on the writer of every dataset that has the tables,
and on the remaining lanes of a [parallel backfill](../parallel-backfill.md)
opened by an earlier version. With them, the tables, schemas and digests are
those of v1.6.0, so the dataset resumes as it is. There is no command yet to
retire the tables from an existing dataset: they stay empty Delta tables, and
deleting one breaks the dataset, because `build` refuses a declared table that
is missing once rows were accepted. The maintenance job skips a table in
`LAKE_TABLES` that does not exist, so one `LAKE_TABLES` list serves every kind
of dataset.

### Gas-change tables

The EVM Firehose tracer no longer records gas changes (`evm-firehose-tracer-rs`
v5.0.0), and `fireeth tools convert-to-v5` (firehose-ethereum v2.24.0) removes
them from older blocks: block version 5 has none. `gas_changes` contains
explicit upstream gas-change records. Missing rows do not mean zero gas usage;
transaction and receipt gas fields provide separate data.

### Account-creation tables

`Call.account_creations` is deprecated upstream: the checked-in Ethereum
protobuf says account-creation records are unsupported from block version 4.
On the public lake of 2026-11-27, the last row is at block 22,431,083 on
Ethereum mainnet, the last version 3 block before Pectra (Pinax's Firehose
serves Pectra's first block, 22,431,084, as version 4). The last row on BSC is
at block 47,618,306 (Pascal), and Base has none. Do not use an empty or missing table to conclude
that no contracts or accounts were created.

### System nonce changes

System calls (EIP-4788, EIP-2935, EIP-7002, EIP-7251) run from the system
address without changing a nonce. `system_nonce_changes` had no rows on
Ethereum mainnet, BSC or Base on 2026-11-27, nor did
`system_account_creations`.

## Tables that can be empty

`build` creates every table of the stream at its first start, so a table
without rows is an empty Delta table: `delta_scan` returns an empty relation,
and the table has no data files or `date=` directories. With
`--without-extended` the extended tables are not part of the stream and do
not exist, nor do the [opt-in tables](#opt-in-tables) without their flags.

Some tables stay empty, or start late, because of the chain or its Firehose
blocks:

- `withdrawals`: chains without beacon-chain withdrawals (BSC, and OP Stack
  chains such as Base) have none. On Ethereum mainnet, Pinax's Firehose blocks
  before block 25,740,000 (block versions 3 and 4) lack `Block.withdrawals`,
  so the table starts there (#742). `system_balance_changes` rows with reason
  `WITHDRAWAL` cover every withdrawal since Shanghai, without the withdrawal
  and validator indexes.
- `system_balance_changes`: OP Stack chains have no block rewards or
  withdrawals.
- `system_code_changes`: only hard forks that rewrite system contracts (on BSC
  and OP Stack chains) write it; Ethereum mainnet has none.
- The opt-in tables of a dataset created with their flags.

## Withdrawals, access lists and EIP-7702 authorizations

Three tables hold block and transaction data that is not a column of `blocks` or `transactions`. They are written at both detail levels, including with `--without-extended`. Access-list and authorization rows follow their transaction: they are written for failed transactions too, and dropped with `--exclude-failed-transactions`. Columns and types are in the [EVM schema reference](../schemas/evm.md).

| Table | One row per | Notes |
|---|---|---|
| `withdrawals` | beacon-chain withdrawal in the block (Shanghai and later) | `index` is the global withdrawal index; `amount_gwei` is in gwei, not wei |
| `access_lists` | entry of a transaction's access list (EIP-2930) | `access_index` is the position in the list; `storage_keys` may be empty |
| `set_code_authorizations` | authorization of a `SET_CODE` transaction (EIP-7702) | `authorization_index` is the position in the list; `chain_id` is a decimal string (`0` allows any chain); `address` is the delegation target and `authority` the recovered signer |

- A withdrawal also appears in `system_balance_changes` with reason `WITHDRAWAL`, in wei and without the validator. Per block and address, `sum(amount_gwei) * 1000000000` (exact decimal arithmetic; `* 1e9` would be a lossy `double`) equals the summed balance delta there.
- `authority` is `NULL` when it can't be recovered from the signature, and those authorizations are `discarded`. `address` is `NULL` on the few testnet blocks where Firehose did not record it.
- `discarded = true` means the chain skipped the authorization as invalid. Accepted authorizations take effect even when the transaction fails (see failed transactions above).

## Header, signature, blob and ordinal columns

`blocks`, `transactions`, `calls`, `system_calls` and `logs` carry Firehose header, signature, blob and ordinal fields as they are, with bytes in the output encoding and big integers as decimal strings like the other value columns. The [EVM schema reference](../schemas/evm.md) lists each column, type and nullability.

- Fields introduced by a fork are `NULL` in blocks and transactions from before it: `withdrawals_root` (Shanghai), `blob_gas_used` / `excess_blob_gas` and `parent_beacon_root` (Cancun), and `requests_hash` (Prague).
- The blob columns of `transactions` are `NULL` for non-blob transactions, and `blob_hashes` is an empty list. `transactions.logs_bloom` comes from the receipt and is `NULL` without one.
- `failure_reason` on `calls` and `system_calls` is `NULL` when the call did not fail; `address_delegates_to` is the EIP-7702 delegation target of the called account.
- `begin_ordinal` / `end_ordinal` on transactions and calls, and `ordinal` on logs, give the execution order in the block. Ordinals are unique within a block, so `(block_number, ordinal)` orders every log, call and state change of a block. They are not reliable for anything inside a reverted call.
