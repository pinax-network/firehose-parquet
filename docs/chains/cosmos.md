# Cosmos notes

Semantics of the Cosmos tables beyond their columns, which the [Cosmos schema reference](../schemas/cosmos.md) lists. The failed-transaction flags are in [Failed transaction filtering](failed-transactions.md).

## Unknown results, ordered events and transaction metadata

Cosmos transactions with missing `TxResult` have null `code`, gas and result text
fields. They remain included as unknown; `WHERE code = 0` selects only confirmed
source success. `decode_success` reports decoding of the supported SDK envelope
and metadata fields, not valid signatures or successful execution. The exact
`binary` `raw_tx` remains available, and `blocks.tx_decode_failures` counts malformed
transactions across the full source block, including filtered failed rows.

Events retain `event_index` and nullable `long` `attribute_index`. An event without
attributes has one row with null attribute index/key/value. Empty source strings
are present values. Block events have null `tx_index` and `tx_hash`; transaction
events and messages use the same source index as `transactions.index`.

Memo, timeout height, fee gas limit/payer/granter, ordered fee coins, signer infos
and signatures live on `transactions`. Coin amounts are exact strings. Missing
body/auth/fee is null; a present empty value or list remains empty. Public keys
are optional, and signer/signature arrays keep their independent source order
and lengths. Signer `mode_info` retains the opaque embedded protobuf payload
(concatenated in source order if repeated); it is not semantically validated.
All raw payloads are `binary` regardless of identifier encoding.

```sql
SELECT m.block_num, m.tx_index, m.message_index, m.type_url,
       t.memo, t.fee_amount, t.signer_infos
FROM delta_scan('output/cosmos/messages') m
JOIN delta_scan('output/cosmos/transactions') t
  ON m.block_num = t.block_num AND m.block_id = t.block_id
 AND m.tx_index = t."index"
WHERE t.code = 0;
```

These Cosmos schema changes require rebuilding into a new root or explicit
conversion into a separate dataset. Old fabricated zeros and omitted rows need
source replay to recover their meaning. See the [migration and live RPC comparison](../audit/510-cosmos-values.md).
