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
