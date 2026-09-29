# Antelope notes

Semantics of the Antelope tables beyond their columns, which the [Antelope schema reference](../schemas/antelope.md) lists. The failed-transaction flags are in [Failed transaction filtering](failed-transactions.md).

## Deferred transactions and onerror

Receipt statuses other than `EXECUTED` come from deferred (scheduled) transactions:

| Status | Trace | Selected by default |
|---|---|---|
| `EXECUTED` | executed normally | yes |
| `DELAYED` | scheduled for later execution; no actions ran yet | yes |
| `SOFTFAIL` | for a failed deferred transaction the producer writes two traces: the **failed deferred trace**, which carries the exception and whose database operations the producer already reverted, then the **`onerror` handler trace**, which ran without an exception and whose actions and database operations persisted | the `onerror` trace only |
| `HARDFAIL` | the deferred transaction failed and its `onerror` handler failed or none ran; nothing persisted | no |
| `EXPIRED` | the deferred transaction expired unexecuted | no |

`transactions.transaction_success` and the `transaction_success` of `actions` and `db_ops` say whether the trace's effects persisted. `actions` and `db_ops` also carry the parent receipt status as `transaction_status` (`string`, the labels of `transactions.status`). Before #550 only `EXECUTED` traces were selected by default, which dropped successful `onerror` handlers and scheduled transactions.

## Database-operation joins

`db_ops` includes `tx_hash` (the enclosing trace ID), `tx_index` (`long`, the
original trace index), and `db_op_index` (`long`, zero-based within that trace).
Filtering can leave transaction-index gaps. Operation positions restart for each
transaction and remain stable across flushes. Scope joins to the canonical block:

```sql
SELECT d.block_id, d.tx_hash, d.db_op_index, d.operation, t.status
FROM delta_scan('output/eos/db_ops') d
JOIN delta_scan('output/eos/transactions') t
  ON d.block_id = t.block_id
 AND d.tx_hash = t.tx_hash
 AND d.tx_index = t."index";
```

`actions` and `db_ops` end with the parent `transaction_status` and
`transaction_success` (see [deferred transactions and onerror](#deferred-transactions-and-onerror)).

The action fields `transaction_id`, `trace_block_num`, `producer_block_id`, and
`block_time` are deprecated for ordinary joins and routing; prefer `tx_hash` and
canonical block identity/time. They remain verbatim action metadata, which can
be missing or differ from canonical values. No removal is scheduled. Existing
action JSON, nulls and enum labels remain unchanged.

Use a new dataset or rebuild older ranges to populate the added columns. Schema
union makes them null in old files. See [the implementation and live comparison](../audit/508-antelope-db-joins.md).
