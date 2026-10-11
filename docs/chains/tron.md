# Tron notes

Semantics of the Tron tables beyond their columns, which the [Tron schema reference](../schemas/tron.md) lists. The failed-transaction flags are in [Failed transaction filtering](failed-transactions.md).

## Failed smart-contract calls

java-tron sets the transaction wrapper `result`/`code` to true/`SUCCESS` for every transaction it includes in a block, so `transactions.result` and `code` never report a failed TVM call. The outcome comes from `TransactionInfo`: `result = FAILED` (set with a runtime error such as `REVERT opcode executed`) or a receipt result other than `DEFAULT` (non-VM contracts) or `SUCCESS`. Unknown enum values count as failures.

A failed call still pays its fee, energy and bandwidth: `fee` and the `receipt_*` columns keep them. The VM discards the logs of a reverted call and marks its internal transactions `rejected = true`. `contracts` rows are the submitted contracts, not executed transfers. Every row of `transactions`, `logs`, `internal_transactions`, `contracts` and `internal_call_values` carries the parent's non-null `boolean` `transaction_success`.

## Contracts, receipts and internal values

`contracts` retains every source contract with `transaction_index`, `tx_hash`,
`contract_index`, enum label/number, permission ID and raw `binary` Any payload.
TransferContract, TransferAssetContract and TriggerSmartContract expose typed
owner/recipient/amount or target/data/call-value fields. Unsupported types keep
their raw payload with null decoded fields. `transactions.contract_type` is the
first contract's type, null when the contract list is empty.

`transactions` includes nullable `receipt_*` energy/net fees, usage and result,
and `binary` `res_message`. Missing receipts are null; present zero/empty values
remain values. `transactions.contract_address` is the smart contract created or
called, from `TransactionInfo`; it is NULL when there is none (plain transfers
and other system contracts). `internal_call_values` retains each ordered source
`(call_value, token_id)` pair, including repeated or empty token IDs, joined by
block identity, transaction index/hash and `internal_index`.

`transaction_index` and `logs.block_log_index` count original source positions,
including transactions omitted by failed filtering. `logs.log_index` is per
transaction.

```sql
SELECT t.block_num, t.txid, c.contract_index, c.contract_type,
       c.owner_address, c.to_address, c.amount, c.contract_address,
       hex(c.data) AS call_data_hex, c.call_value, t.receipt_energy_fee
FROM delta_scan('output/tron/transactions') t
JOIN delta_scan('output/tron/contracts') c
  ON t.block_num = c.block_num AND t.block_id = c.block_id
 AND t.transaction_index = c.transaction_index AND t.txid = c.tx_hash;
```

See the [mapping contract, validation and RPC-backed qualification
limits](../audit/509-tron-contract-fields.md).
