# #510: Cosmos event, result and transaction metadata fidelity

## Problem and selected behavior

The old mapper omitted events without attributes, discarded attribute positions,
used signed event transaction indices alongside unsigned parent/message indices,
and represented block event transaction hashes as empty values. Short result
arrays invented successful code/gas zeros. Raw transaction decode failures were
silent and transaction-wide memo, fee and signer metadata were absent.

- Events preserve source order with `event_index` and nullable UInt32
  `attribute_index`. An empty event produces one row with null attribute index,
  key and value; an actual empty key/value is still a present string at index 0.
  All transaction indices are UInt32, and block events have null transaction
  index/hash. Filtering retains original source indices.
- Missing transaction results make code, gas, log, info and codespace null.
  Unknown results remain included by the failed-transaction filter. Use `code = 0`
  for confirmed source success; do not coalesce unknown status to zero.
- Transactions retain exact Binary `raw_tx` and `decode_success`, memo, timeout,
  gas limit, payer/granter, ordered fee coins, signer infos and signatures. Coin
  amounts remain exact source strings. Missing body/auth/fee differs from a
  present empty message/list/value. Signature and signer arrays preserve their
  independent source cardinality; no pairing or address derivation is invented.
- `blocks.tx_decode_failures` counts all malformed source transactions, including
  failed rows excluded by policy. Successful subset decoding is not proof of SDK
  semantic validity, valid signatures or transaction execution success. Unknown
  SDK fields remain available in `raw_tx`.
- Signer `mode_info` is opaque Binary: concatenated payloads of any repeated
  singular embedded-message occurrences, preserving protobuf merge semantics.
  Null means absent; present empty remains empty. Its nested content is not
  interpreted or validated. Public-key Any type/value remains optional.

The minimal SDK definitions are pinned to
[`cosmos/cosmos-sdk@3431b5f`](https://github.com/cosmos/cosmos-sdk/blob/3431b5f3aba96dbb1d0d622081a8de2a610229af/proto/cosmos/tx/v1beta1/tx.proto).
Independent review found that decoding TxRaw directly as Tx incorrectly merges
repeated body/auth fields. The implementation now decodes actual TxRaw bytes
fields first (last occurrence wins), then decodes the retained body/auth values.
The [SDK decoder](https://github.com/cosmos/cosmos-sdk/blob/3431b5f3aba96dbb1d0d622081a8de2a610229af/x/auth/tx/decoder.go)
allows equal adjacent field tags, so canonical-writer assumptions do not excuse
the mismatch. Handwritten duplicate-field vectors cover it.

## Compatibility

This changes Cosmos schemas and adds nested values. Start a new output root and
rebuild, or explicitly reconcile old files into a separate dataset. Old fake
zeros, lost empty events and missing transaction metadata cannot be recovered
without source replay. The four table names and all identifier encoding modes
remain available; raw transaction/message/public-key/signature payloads are
Binary. Join messages/events to transactions using canonical block identity and
the original transaction index. Transaction-wide metadata lives on transactions
instead of being repeated on every message row.

## Live source comparison, with explicit limits

On 2026-09-25, the public Cosmos Hub RPC yielded block 33121486 and matching block
results. It contains one transaction, one message, 366 block events and 17
transaction events. The [retained source fixture and hashes](../../blocks/tests/fixtures/cosmos-33121486/README.md)
are independent of the mapper. No production output or S3 data was written.

`510-qualify.py prepare` builds a minimal v2 block from only the consumed RPC
fields. It follows the relevant field/array mapping in the
[pinned Firehose converter](https://github.com/streamingfast/firehose-cosmos/blob/5cf6c3b04091931f2463eba405d215be27738e21/cometbft/101/convert/convert.go);
the Injective-specific block-bloom reordering is absent in this sample and
asserted absent. The replay omits unused header/consensus fields. It is an
RPC-backed live-data mapper check, **not a live Firehose payload/transport check**.

The checker independently reads raw SDK wire fields (without generated SDK types
or mapper expectations), hashes the exact raw transaction, and compares every
selected metadata/result field, message payload/index, event attribute and
cross-table canonical identity. It checks all 1,138 event rows across 383 source
events and all four tables. Fees/signers/arrays retain order and exact bytes.
The sample has no missing result, malformed transaction or empty event; those
edge cases are covered by synthetic regressions rather than attributed to live data.

Reproduce offline from the repository root with Python 3, DuckDB and Cargo:

```sh
python3 docs/audit/510-qualify.py prepare \
  blocks/tests/fixtures/cosmos-33121486 /tmp/cosmos-33121486.pb
cargo run --locked -p blocks --example replay_cosmos -- \
  --block /tmp/cosmos-33121486.pb --output /tmp/cosmos-33121486-parquet
python3 docs/audit/510-qualify.py check \
  blocks/tests/fixtures/cosmos-33121486 /tmp/cosmos-33121486-parquet
```

Choose fresh output paths. The helper never fetches network data and refuses an
existing replay file/output directory. DuckDB 1.1.1 was used for qualification.

## Regression and integration evidence

Focused tests cover absent/present-empty messages/lists, malformed outer/nested
protobuf, independent wire tags, duplicate outer fields, repeated opaque modes,
UInt64 maximums, exact large coin strings, duplicate fee denoms, independent
signature/signer cardinality, failed-transaction child filtering, source-index
gaps, empty events/attributes, duplicate keys and all-source decode counters.
Every encoding with/without fork step survives repeated flushes and nested
Parquet roundtrips. The live raw transaction is retained for CI regression.

Final current-main suite, CI and lifecycle results will be recorded before closure.
