# Chain notes

Per-chain semantics of the output tables: failed-transaction rules, join keys, ordering and example queries. Every column, Delta type and nullability is in the generated [schema reference](../schemas/README.md).

| Chain | Notes | Schema |
|---|---|---|
| All chains | [Failed transaction filtering](failed-transactions.md): the flags, and each chain's failure condition and outcome columns | [Index](../schemas/README.md) |
| EVM | [EVM notes](evm.md): state changes of failed transactions, change-to-call joins, receipt log indices, tables that can be absent, withdrawals, access lists and EIP-7702 authorizations, header columns | [EVM](../schemas/evm.md) |
| Solana | [Solana notes](solana.md): vote filtering, transaction outcome context, payloads and account indices, instruction order, reward indices | [Solana](../schemas/solana.md) |
| Bitcoin | [Bitcoin notes](bitcoin.md): amounts, input joins and missing fields | [Bitcoin](../schemas/bitcoin.md) |
| Beacon | [Beacon notes](beacon.md): each table's source and first fork | [Beacon](../schemas/beacon.md) |
| Tron | [Tron notes](tron.md): failed smart-contract calls, contracts, receipts and internal values | [Tron](../schemas/tron.md) |
| Cosmos | [Cosmos notes](cosmos.md): unknown results, ordered events and transaction metadata | [Cosmos](../schemas/cosmos.md) |
| Antelope | [Antelope notes](antelope.md): deferred transactions and `onerror`, database-operation joins | [Antelope](../schemas/antelope.md) |
| NEAR | [NEAR notes](near.md): failed receipts, transactions, receipts, actions and logs, final transaction outcome, state changes | [NEAR](../schemas/near.md) |
| HyperCore | [HyperCore notes](hypercore.md): the table catalogue by product family, identity, the data origin and the block hole, decimals, fills and liquidations, event routing and the columns of each type, funding and open interest, the derivation rules, refusals, schema changes, views, monitors and cookbook | [HyperCore](../schemas/hypercore.md) |

The identity columns every table shares and the identifier encoding of each
chain are in [Output layout](../output-layout.md#canonical-identity-columns).

## Schema reference

Per-chain schema references are generated from the mapper schemas and list
every table, column, Delta type and nullability, and each chain's mapping from
the mapper's Arrow types onto the Delta types of the files (#643). The chain
notes name the Delta types too, and their queries read the
tables with DuckDB's `delta_scan`:

- [Schema reference index](../schemas/README.md)
- [EVM](../schemas/evm.md), [Solana](../schemas/solana.md),
  [Bitcoin](../schemas/bitcoin.md), [Beacon](../schemas/beacon.md),
  [Tron](../schemas/tron.md), [Cosmos](../schemas/cosmos.md),
  [Antelope](../schemas/antelope.md), [NEAR](../schemas/near.md),
  [HyperCore](../schemas/hypercore.md)

The chain notes keep the semantics that a column list cannot show: failed-transaction
rules, join keys, ordering and example queries. `DESCRIBE SELECT * FROM
delta_scan('<root>/<table>')` prints a table's schema, and
`fireparq inspect <file> --schema-only` the schema of one data file. Select columns by name: `fork_step` and
`stream_ordinal` (non-final streams only) are followed by later columns on
several Solana, Antelope, NEAR and Tron tables.
