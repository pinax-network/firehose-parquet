# Chain notes

Per-chain semantics of the output tables: failed-transaction rules, join keys, ordering and example queries. The generated [schema reference](../schemas/README.md) lists every table, column, Delta type and nullability, and each chain's mapping from the mapper's Arrow types onto Delta types.

| Chain | Notes | Schema |
|---|---|---|
| All chains | [Failed transaction filtering](failed-transactions.md): the flags, and each chain's failure condition and outcome columns | [Index](../schemas/README.md) |
| EVM | [EVM notes](evm.md): state changes of failed transactions, change-to-call joins, receipt log indices, tables that can be empty, withdrawals, access lists and EIP-7702 authorizations, header columns | [EVM](../schemas/evm.md) |
| Solana | [Solana notes](solana.md): vote filtering, transaction outcome context, payloads and account indices, instruction order, reward indices | [Solana](../schemas/solana.md) |
| Bitcoin | [Bitcoin notes](bitcoin.md): amounts, join keys and missing fields | [Bitcoin](../schemas/bitcoin.md) |
| Beacon | [Beacon notes](beacon.md): each table's source and first fork | [Beacon](../schemas/beacon.md) |
| Tron | [Tron notes](tron.md): failed smart-contract calls, contracts, receipts and internal values | [Tron](../schemas/tron.md) |
| Cosmos | [Cosmos notes](cosmos.md): unknown results, ordered events and transaction metadata | [Cosmos](../schemas/cosmos.md) |
| Antelope | [Antelope notes](antelope.md): deferred transactions and `onerror`, database-operation joins | [Antelope](../schemas/antelope.md) |
| NEAR | [NEAR notes](near.md): failed receipts, transactions, receipts, actions and logs, final transaction outcome, state changes | [NEAR](../schemas/near.md) |
| SEC EDGAR | [SEC notes](sec.md): 10-minute windows, keys and deduplication, value semantics and `parse_issues`, the shipped views, example queries and code legends | [SEC](../schemas/sec.md) |
| HyperCore | [HyperCore notes](hypercore.md): tables by product family, identity and coverage, decimals, fills, event routing, funding, derivation rules, refusals, views, monitors and cookbook | [HyperCore](../schemas/hypercore.md) |

The identity columns every table shares and the identifier encoding of each
chain are in [Output layout](../output-layout.md#canonical-identity-columns).

The queries read each table through its Delta log with DuckDB's
`delta_scan('<root>/<table>')`; [Reading the tables](../reading-tables.md)
covers Polars and S3. `DESCRIBE SELECT * FROM delta_scan('<root>/<table>')`
prints a table's schema, and `fireparq inspect <file> --schema-only` the schema
of one data file. Select columns by name: on non-final streams, `fork_step` and
`stream_ordinal` are followed by later columns on several Solana, Antelope,
NEAR and Tron tables.

The notes describe the output of v1.0.0 and later. Earlier output has other
schemas and row selection, and `build` does not resume it: see the
[v1.0.0 upgrade guide](../releases/v1.0.0.md#upgrade-guide-read-first).
