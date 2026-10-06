# Failed transaction filtering

EVM includes failed/reverted transactions by default. Solana, Tron, Antelope, Cosmos and NEAR exclude them unless you pass `--include-failed-transactions`. Bitcoin, Beacon and HyperCore have no failed transactions. `--exclude-failed-transactions` drops them on every chain and takes precedence. The flags select whole transactions: every row that belongs to a dropped transaction is dropped with it. NEAR receipts are not transactions and are always written ([details](near.md#failed-receipts)).

| Flag | EVM | Solana, Tron, Antelope, Cosmos, NEAR |
|---|---|---|
| *(none)* | included, with their persistent state changes | excluded |
| `--exclude-failed-transactions` | excluded | excluded |
| `--include-failed-transactions` | deprecated, no effect (warns) | included |

Per-chain failure condition, and the columns that label the rows of an included failed transaction:

| Chain | Failed when | Outcome columns |
|---|---|---|
| **EVM** | `status != SUCCEEDED` | `transactions.status`; `state_reverted` and `persisted` on state changes ([details](evm.md#persistent-state-changes-of-failed-transactions)) |
| **Solana** | `meta.err` has non-empty bytes | `transactions.success`; `transaction_success` on child tables ([details](solana.md#transaction-outcome-context)) |
| **Tron** | `TransactionInfo.result` is `FAILED`, or the receipt result is neither `DEFAULT` nor `SUCCESS` (for example `REVERT` or `OUT_OF_ENERGY`). The Firehose wrapper `result`/`code` are always true/`SUCCESS` and do not report execution | `transaction_success` on `transactions`, `logs`, `internal_transactions`, `contracts`, `internal_call_values` ([details](tron.md#failed-smart-contract-calls)) |
| **Antelope** | the trace carries an exception, or its receipt status is not `EXECUTED`, `SOFTFAIL` or `DELAYED` (so `HARDFAIL`, `EXPIRED` and statuses without an execution fail) | `transactions.transaction_success`; `transaction_status` and `transaction_success` on `actions` and `db_ops` ([details](antelope.md#deferred-transactions-and-onerror)) |
| **NEAR** | the transaction's own outcome is `Failure` (an inclusion failure) | `receipt_status` on `receipt_actions` and `execution_logs` ([details](near.md#failed-receipts)) |
| **Cosmos** | `code != 0` in `TxResult` | `transactions.code` ([details](cosmos.md)) |
| **Bitcoin** | *(not applicable — Bitcoin has no failed txs)* | — |
| **Beacon** | *(not applicable — consensus blocks have no transaction outcomes)* | — |
| **HyperCore** | *(not applicable — the blocks carry fills and ledger events, not transactions with outcomes)* | — |

A failed transaction still pays fees on every chain. The outcome columns describe the parent transaction or receipt; they do not assert that an individual instruction, contract or action ran.

When failed transactions are included, chain-specific fields like Solana's `err` bytes and `success` flag reflect the actual transaction status.
