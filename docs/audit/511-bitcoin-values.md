# Bitcoin values, input joins and missing fields (#511)

Issue: <https://github.com/pinax-network/firehose-parquet/issues/511>

## Diagnosis and implementation

`outputs.value` stores the upstream floating coin amount, which is unsuitable
for exact sums. Add `value_sats: UInt64`, retaining the old column. Prefer the
signed 64-bit little-endian amounts in `Transaction.hex`; a bounded prefix
reader extracts only outputs and checks counts, output indices, nonnegative
values and agreement with the decoded coin amount. Integer-to-coin comparison
uses an exact eight-place decimal string before parsing the double, avoiding
an intermediate large-integer-to-float rounding. Malformed supplied serialized
bytes fail instead of falling back silently.

When older payloads omit `Transaction.hex`, conversion accepts only one nearby
integer that recreates the supplied double. It rejects fractional, negative,
non-finite, ambiguous or out-of-range amounts. The candidate search is bounded
below 2^53 and checks neighboring candidates too. Serialized integer amounts
remain usable when adjacent units share a double. This mapper also handles
Litecoin, so a Bitcoin-only 21-million supply bound would be incorrect here.
The extractor does not validate signatures, witness data, locktime, family
extensions or consensus monetary rules; it reads the common output prefix.

All amounts are preflighted before any row of the block is appended. A bad
later transaction leaves previously buffered blocks intact and appends none of
the failing block. Inputs gain `tx_index`. Coinbase previous-output/script fields
and ordinary-input coinbase fields use nulls. Missing previous txids do not
manufacture output zero; real zero indices stay zero. Present empty scripts are
kept distinct from missing messages. Missing output script messages are null.

Output addresses prefer modern `address`, then the first legacy `addresses`
entry, otherwise null. The first legacy entry is not an exclusive-owner claim.
Native protobuf string fields retain Bitcoin Core display order and spelling;
README encoding claims now distinguish them from canonical IDs, which follow
the selected encoding. New fields append before the optional fork-step column;
nullable changes and dataset migration are documented in release notes.

Protocol references reviewed on 2026-09-25:

- [Bitcoin Core v30 transaction serialization](https://github.com/bitcoin/bitcoin/blob/v30.0/src/primitives/transaction.h)
  defines the shared basic/extended input-output prefix and integer `CTxOut`.
- [Bitcoin Core RPC output/input fields](https://bitcoincore.org/en/doc/30.0.0/rpc/rawtransactions/getrawtransaction/)
  distinguishes optional coinbase, previous-output, script and address fields.
- [Litecoin transaction serialization](https://github.com/litecoin-project/litecoin/blob/master/src/primitives/transaction.h)
  retains the input/output prefix before witness and extension data.
- [Upstream Firehose Bitcoin protobuf](https://github.com/streamingfast/firehose-bitcoin/blob/develop/proto/sf/bitcoin/type/v1/type.proto)
  matches the repository field numbers; omitted address data in this live sample
  is not caused by an outdated local address field number.

## Validation

Twelve focused Bitcoin tests pass. New cases cover legacy and extended serialized
prefixes, large shared-family amounts, malformed/truncated/count/index/value
mismatches, 10,000 deterministic fallback amounts across Bitcoin's range,
non-finite/fractional rejection, all-block preflight, joins/null semantics,
modern/legacy/missing addresses, both byte encodings, fork-step columns and
flush/reset alignment. These are offline tests; no production S3 writes occur.

One finalized raw Bitcoin block **900,000** was captured from the public Pinax
endpoint, then the real CLI read exactly `[900000,900001)` into temporary local
Parquet. The original 8,286,179-byte payload has SHA-256
`430d6b84cf85ad34f35c20031d373d534323441096030144e255340d320180c4`.
An independent Python protobuf + serialized-transaction decoder compared:

- 1 block, **1,562 transactions**, **4,387 inputs**, **3,904 outputs**;
- every output's exact serialized integer and the integer SQL sum:
  **131,868,623,694 satoshis**; original Float64 values remain unchanged;
- all input join positions, previous outputs, coinbase/script nulls, witness
  lists and native transaction hashes, including 3,777 witness-bearing inputs
  and 3,539 present empty scripts;
- all output script fields and canonical IDs; four final Parquet parts, cursor
  frontier 900,000, and no hidden temporary files.

The raw sample omits **both** modern and legacy address fields on all 3,904
outputs; their resulting nulls are correct source preservation. Legacy fallback
and positive address cases are therefore qualified by offline fixtures, not
claimed as observed live. No addresses are synthesized from scripts. Litecoin
large-amount compatibility is covered offline; no live Litecoin claim is made.

Full current-main workspace validation and independent review are recorded before
merge. Raw transaction capture and temporary Parquet output remain outside the
repository; no credentials, authorization headers or opaque cursor values are
included in this record.
