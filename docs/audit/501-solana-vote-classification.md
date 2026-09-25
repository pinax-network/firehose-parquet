# Conservative Solana vote classification (#501)

Issue: <https://github.com/pinax-network/firehose-parquet/issues/501>

## Diagnosis and decision

The mapper previously inspected every message account key for the Vote program.
It then returned before normal transaction/message/instruction/balance/lookup/
reward mapping, so merely mentioning that account could hide administrative
activity. The issue's proposed one-instruction/program check is insufficient:
Withdraw and Authorize can themselves be single Vote-program instructions.

The upstream [transaction shape checker](https://github.com/anza-xyz/solana-sdk/blob/6f74525ac8fba532017b379769549f4640c90e62/transaction/src/simple_vote_transaction_checker.rs)
requires a legacy transaction with fewer than three signatures and one Vote
program instruction, relying on an already sanitized transaction. The upstream
[VoteInstruction classifier](https://github.com/anza-xyz/solana-sdk/blob/6f74525ac8fba532017b379769549f4640c90e62/vote-interface/src/instruction.rs)
recognizes eight payload variants and excludes administrative instructions.
We combine conservative envelope checks with the latter classifier.

`blocks/src/solana/vote.rs` requires 1–2 64-byte signatures, a consistent present
header, 32-byte keys/blockhash, legacy encoding without lookups, one instruction,
and valid program/account indices. Full fixed-integer bincode deserialization
rejects trailing bytes and applies the upstream [1,232-byte packet ceiling](https://github.com/anza-xyz/solana-sdk/blob/6f74525ac8fba532017b379769549f4640c90e62/packet/src/lib.rs).
The decoded instruction must be a recognized simple vote with at least one slot
and serialize back to identical bytes. Any uncertainty keeps ordinary rows.
These are structural checks, not cryptographic signature verification or ledger
execution validation. The independent failed-transaction filter is unchanged.

The exact `solana-vote-interface = 7.1.0` dependency enables only `serde`. Its
published source records commit `8dece40e0e02b0f3f6a96175f4f504ce58b1b0f5`;
its eight recognized variants agree with the reviewed upstream pin above.
Bincode 1.3.3 decodes the official type, including compact/tower vote formats.
This adds 28 interface/serde packages without changing existing locked package
versions; it does not add the validator runtime or whole SDK. Maintaining a
partial discriminant/length parser would risk accepting malformed compact
payloads and must track upstream serialization details independently.

## Compatibility

There is no schema change. Recognized votes keep the existing compact-table and
`--without-votes` behavior. Other retained transactions use the normal mapper,
including per-block reward indices from #500. Versioned or future vote formats
remain ordinary data until explicitly supported. Counts and verification roots
can change for ranges where the old broad predicate hid activity. Rebuild into
a separate output root; this change does not solve crash/replay duplicates (#468).

## Regression validation

Mapper-level fixtures cover Withdraw, Authorize, AuthorizeChecked,
UpdateValidatorIdentity, UpdateCommission and InitializeAccount; a System create
plus Vote initialize pair; mixed vote/other instructions; unused Vote keys;
versioned transactions with address lookups; all eight recognized vote variants
with one/two signatures; empty/truncated/unknown/trailing/oversized payloads,
huge declared slot vectors, empty votes and malformed envelope/index fields.
The fixtures exercise both vote-table settings, assert retained payload/SOL and
token balance values, transaction reward identity/index, every detail table,
and the existing compact-table policy for recognized votes. Focused mapper
checks passed on 2026-09-25. Full workspace and bounded sample evidence follows.

## Bounded live sample

Exactly two Fetch RPCs read slots 300000000 and 300000001 from the explicit Pinax
Solana endpoint using provider-scoped credentials. Only decoded Block payload
bytes were retained, without response cursors, headers or credentials:

| Slot | Transactions | Bytes | SHA-256 |
|---|---:|---:|---|
| 300000000 | 2541 | 2946958 | c68946ce74e66969b023d6397cff61f6cb8bd6508ff89148fb29130ea6a30dae |
| 300000001 | 1907 | 2935030 | 552dbd676ea0d3a36be535d6318dc3c920de90ce7f875d48d037f538ba026154 |

Local artifacts: `/tmp/fireparq-501-solana-data/{slot}.pb` with JSON sidecars.
The same immutable sample is shared with #502, avoiding another raw fetch.

The first full workspace run caught the schema-contract fixture's old fake vote
(a changed account key). It now supplies a canonical legacy Vote payload so the
contract still requires nonempty output for every table. The rerun passed.
Neither `cargo-audit` nor `cargo-deny` is installed on this host, so no dependency
advisory audit was run. Lockfile inspection confirms 28 added package/version
pairs and no removed or updated existing pairs.

## Completed qualification (2026-09-25)

After integrating main `3cf984b` (#476/#485) and #502 head `e0cefb5`, source
`05960be34da8f183c917f66ee5be3be18217e0c2` passed **783 tests, 0 failed,
4 ignored**, including doctests. `cargo fmt --all -- --check` and the normal
`fireparq` build passed. All Cargo commands used the whole-command shared-target
lock. The built executable was copied under that lock to
`/tmp/fireparq-501-live`; the validation log is `/tmp/fireparq-501-final.log`.

Two bounded CLI streams consumed `[300000000,300000002)` with the explicit Pinax
Solana endpoint, once with the default vote table and once with `--without-votes`.
Both used fresh local roots, completed normally, saved last block 300000001 and
left no temporary table parts. This is two two-block streams in addition to the
two raw Fetch calls above; no wider scan was performed.

The offline [independent comparison](501-compare-solana-votes.py) builds expected
transaction indices from raw protobufs before reading output. For this sample it
independently parses the complete compact-vote wire format: discriminant, root,
canonical short-vector/varint lockout offsets, hash and optional timestamp,
including overflow and trailing-byte checks. It does not call the production
Rust classifier. The sample contains only `CompactUpdateVoteState` (variant 12),
so encountering another candidate variant deliberately fails the audit instead
of guessing. The eight variants and administrative/malformed cases remain
qualified by mapper fixtures, not by this live sample.

| Slot | Ordinary successful transactions | Recognized votes | Failed transactions excluded |
|---|---:|---:|---:|
| 300000000 | 454 | 1961 | 126 |
| 300000001 | 462 | 1319 | 126 |
| Total | 916 | 3280 | 252 |

All 4,196 expected transaction indices matched the correct output table.
`--without-votes` emitted no vote table and kept every ordinary table value.
All prior columns, types and 15,832 rows across eight default-output tables
matched the earlier #500 sample exactly (the independently added #502 columns
were excluded only from the older-schema comparison). Thus this known vote
sample preserves prior results; it does not demonstrate recovery of live
administrative calls absent from these two blocks.

Artifacts:

- Output root: `/var/folders/mm/46m31dr97v1_k02y_ftl10lh0000gn/T/fireparq-501-live-solana-afea1hg3`
- Structured comparison: `/tmp/fireparq-501-live-comparison.json`
- Safe cursor/completion summary: `/tmp/fireparq-501-live-run-summary.json`
- Baseline: `/var/folders/mm/46m31dr97v1_k02y_ftl10lh0000gn/T/fireparq-500-live-solana-dbop_k8m/flush-1`

To repeat only the offline comparison, generate a descriptor with
`protoc -I proto --include_imports --descriptor_set_out=/tmp/solana.desc proto/solana.proto`,
then run the checked-in script with `uv run --with protobuf python`, supplying
`--descriptor`, `--raw-dir`, `--with-votes`, `--without-votes`, `--baseline` and
`--summary`. The first comparison attempt used a reserved DuckDB alias; the
corrected script passed against the same saved outputs without more live calls.
