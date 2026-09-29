# Bitcoin notes

Semantics of the Bitcoin tables beyond their columns, which the [Bitcoin schema reference](../schemas/bitcoin.md) lists.

## Amounts, input joins and missing fields

Use `outputs.value_sats` (`long`) for exact sums. The original `value` column
remains a `double` coin amount for compatibility. When the protobuf includes
`Transaction.hex`, `value_sats` comes directly from its serialized integer
outputs; output counts, indices and the decoded coin amounts must agree. Older
payloads without raw transaction bytes use a strict conversion only when one
integer base-unit value recreates the supplied double. Ambiguous/fractional,
negative, non-finite or inconsistent amounts fail before any row of that block
is appended. The mapper is shared with Litecoin, so it does not impose Bitcoin's
21-million monetary bound; its unit scale is 100,000,000 per coin.

```sql
SELECT SUM(value_sats) AS total_sats
FROM delta_scan('output/btc/outputs');
```

`inputs.tx_index` joins to `transactions.tx_index` within the same canonical
`block_id`; add `input_index` for an input's position. Coinbase inputs have null
`prev_txid`, `prev_vout` and script-signature columns. Ordinary inputs have null
`coinbase`; a missing previous txid also keeps `prev_vout` null rather than
inventing output zero. A real previous output zero remains zero. A missing
script-signature message is null, while a present empty script remains `''`.
Witnesses retain the protobuf list, including an empty list when none is supplied.

`script_pubkey_address` prefers a nonempty modern `address`, then the first
legacy `addresses` entry, and is null if neither is available. The legacy first
entry is not a claim of exclusive ownership of a multi-address script. Missing
script-public-key messages yield null script columns; present empty fields stay
empty. Native protobuf strings are copied verbatim without adding prefixes or
reversing display-order hashes. Encoding settings apply to canonical ID columns.

These additions and nullable-field changes affect the Bitcoin table schemas.
Use a new output dataset when upgrading; comparing old and new data needs
explicit schema reconciliation (see the
[upgrade guide](../releases/v1.0.0.md#upgrade-guide-read-first)).
