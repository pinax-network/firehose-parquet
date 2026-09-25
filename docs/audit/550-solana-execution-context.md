# #550: Solana parent transaction outcome context

This is a bounded Solana slice of [#550](https://github.com/pinax-network/firehose-parquet/issues/550).
It does not close that issue. Antelope, Tron and NEAR execution-policy work and
their missing failure/transport qualification remain separate. No new public
chain requests or production writes were made for this change.

## Diagnosis and source evidence

The original proposal's blanket interpretation of failed-transaction child rows
as reverted state is unsafe. Solana instructions represent submitted plans and
recorded inner calls, while token balance rows represent pre/post observations.
Failure does not make every observation an effect that was undone.

The primary implementation evidence is pinned to Agave
`5ed01ccff7424c592f84c09d1e0ec4fb2699492a`:

- [`svm/src/transaction_processor.rs`, lines 637–684](https://github.com/anza-xyz/agave/blob/5ed01ccff7424c592f84c09d1e0ec4fb2699492a/svm/src/transaction_processor.rs#L637)
  updates failed transactions' rollback accounts before collecting post balances.
- [`runtime/src/account_saver.rs`, lines 73–108](https://github.com/anza-xyz/agave/blob/5ed01ccff7424c592f84c09d1e0ec4fb2699492a/runtime/src/account_saver.rs#L73)
  stores successful execution accounts or failure/fees-only rollback accounts as
  appropriate; it does not discard every account change on failure.
- [`svm/src/rollback_accounts.rs`](https://github.com/anza-xyz/agave/blob/5ed01ccff7424c592f84c09d1e0ec4fb2699492a/svm/src/rollback_accounts.rs#L12)
  distinguishes fee payer and durable nonce rollback cases. Nonce advancement
  and fee accounting must not be relabeled as reverted merely from parent status.

The local source schema (`proto/solana.proto`) carries opaque transaction error
bytes, complete message instructions, recorded inner instruction groups and
pre/post snapshots. It has no general per-instruction success field. The change
does not decode error bytes or infer which submitted instructions executed.

The wider assessment also found distinct questions that this slice must not
silently settle: Antelope's producer removes reverted database operations and
successful `onerror` can share SOFTFAIL status; Tron's wrapper result is distinct
from its VM receipt outcome; NEAR receipts have their own outcomes and the pinned
producer omits state changes. Those require chain-specific contracts and remain
under #550, with no retry of quota-limited NEAR/Tron Firehose endpoints. Evidence
pins for that remaining assessment are
[`firehose-antelope` console reader](https://github.com/pinax-network/firehose-antelope/blob/238611ac29d6006b2f7457f364bf5731ca0868d6/codec/consolereader.go#L488),
[`firehose-tron` fetch conversion](https://github.com/streamingfast/firehose-tron/blob/d4095accc0dcc8fb4da8c1c1b1e8f4be33921df2/rpc/fetcher.go#L275),
and [`near-firehose-indexer` conversion](https://github.com/streamingfast/near-firehose-indexer/blob/144071c93685c057293459329e9ca35b07aba641/src/codec/mod.rs#L27).

## Exact contract and compatibility

The existing predicate is reused once per transaction: `transaction_success`
is false precisely when present `meta.err.err` is nonempty. Absent/empty error
bytes are true. Absent metadata, transaction or message still omit that
transaction. The existing default failed filter, `include_failed`, strict vote
classification and separate vote table are unchanged.

| Table | New column | Meaning |
|---|---|---|
| `messages`, `instructions`, `token_balances`, `account_lookups` | non-null Boolean `transaction_success` | Parent transaction outcome |
| `rewards` | nullable Boolean `transaction_success` | Parent outcome for transaction rewards; NULL for block rewards |

Every new field is appended after every existing field, including optional
`fork_step`. Old column names, types, nullability, metadata, order, values and row
selection are preserved. No `reverted` or instruction-success field is added.
Top-level plans retain unexecuted instructions; recorded inner rows retain their
source positions without acquiring a success assertion. Pre/post token balances,
lamport balances, fees, rewards and reward indices remain unchanged. Each new
Boolean builder participates in flush/reset and the buffer size estimate.

These additive schemas change protected schema identities. Start a fresh empty
output root with an absent cursor mirror and replay; authority refuses resuming
an incompatible schema. Strict maintenance must not merge old/new schemas. An
explicit conversion writes a separate dataset; source replay or a trustworthy
parent join is needed to recover historical context. Missing old context is
unknown, not false. The mapper epoch need not change: no existing value or
selection semantics changed, and the exact schema binding already changes.

## Validation

The dedicated mapper matrix covers all five identifier encodings, fork column
on/off, include-failed on/off and votes on/off, with combined versus per-block
flushes through both borrowed and owned protobuf APIs. It includes success, a real serialized first-instruction failure,
present-empty error, opaque nonempty error, missing metadata/transaction/message,
successful/failed recognized votes, submitted instructions after the failed
instruction, recorded inner calls, token snapshots, and transaction/block
rewards. Empty second flushes and total row reset are checked. The synthetic
transaction rewards exercise nullable context even though the retained live
sample has only block rewards.

`cargo test -p blocks --lib solana --locked -j4`: **32 passed** at the initial
implementation boundary. After integrating #515 and #518 at source
`9260d0b9f92e2db933051c6d596158846b13e048`, `cargo test --workspace --locked -j4`
passed **1,030 tests, 0 failures, 9 ignored**. The later main merge
`d7db588` changed ancestry only. The CI `refresh_evm_golden` example passed
**1 test, 1 ignored subprocess helper**; workspace build, replay-example build
and formatting checks passed. All five new Boolean estimates remain included
in the new `table_estimates` API.

Two retained Firehose payloads were replayed without network access:

| Slot | Bytes | SHA-256 |
|---|---:|---|
| 300000000 | 2,946,958 | `c68946ce74e66969b023d6397cff61f6cb8bd6508ff89148fb29130ea6a30dae` |
| 300000001 | 2,935,030 | `552dbd676ea0d3a36be535d6318dc3c920de90ce7f875d48d037f538ba026154` |

Baseline source is main `39d49f6b19b1baea0673bf12ebda836e82d0ee27`, with the
offline replay example copied into a separate baseline checkout. The baseline
uses the borrowed API available there; the final integrated candidate uses
the example's `--owned` option to exercise ingestion's owned Bytes path. Each
binary runs all five encodings × two failed-filter choices × two vote choices ×
two flush choices: 40 cases. Both emit Parquet plus a manifest containing full
Arrow schemas; the comparator checks every legacy field and every retained row,
not only a selected projection or aggregate. Expected new values come from an
independent Python protobuf decode of raw error bytes, not the new parent table.

All **300 table comparisons / 645,230 row occurrences** passed; every legacy
schema/value matched and every added context value matched raw metadata. This
counts repeated qualification cases, not distinct chain rows. The
[recorded summary](550-solana-context-comparison.json) includes hashes, the matrix
and representative per-table counts. The full case report was retained locally
as `/tmp/fireparq-550-integrated-comparison.json`. The initial borrowed-input
candidate comparison also passed the same complete matrix.

The raw sample contains 4,448 transactions and 252 failures, 82 with inner calls.
For every failed transaction, pre/post token snapshots match and the only
lamport-array delta is the payer's fee. This is sample evidence, not a universal
claim about durable nonce or other possible fee-accounting cases. Transaction
rewards and missing metadata require the synthetic tests above. These checks
qualify mapping and Parquet output; they do not requalify Firehose transport or
establish per-instruction execution status.

### Reproduction

Build `blocks/examples/replay_solana_context.rs` at the baseline and candidate
sources and keep distinct binaries. Use fresh, separate output directories:

```sh
cargo build --locked -p blocks --example replay_solana_context
replay_solana_context --owned --raw 300000000.pb --raw 300000001.pb --output after
protoc -I proto --include_imports --descriptor_set_out=solana.desc proto/solana.proto
uv run --with pyarrow --with protobuf python docs/audit/550-compare-solana-context.py \
  --before before --after after --descriptor solana.desc \
  --raw 300000000.pb --raw 300000001.pb --report comparison.json
```

For the pre-#518 baseline, use the example version from commit `620ce45` and
omit `--owned`; that baseline predates the owned-input trait method.

The original captures are temporary qualification artifacts, not checked-in
fixtures. This audit used serialized Cargo commands and copied binaries while
holding the shared build lock, avoiding generated-protobuf contamination from
other worktrees. No performance claim is made by this replay.
