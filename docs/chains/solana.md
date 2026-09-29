# Solana notes

Semantics of the Solana tables beyond their columns, which the [Solana schema reference](../schemas/solana.md) lists. The failed-transaction flags are in [Failed transaction filtering](failed-transactions.md).

## Vote filtering

The optional `vote_transactions` table contains conservatively recognized simple
votes. `--without-votes` omits those transactions. A candidate must be a legacy
transaction with one or two signatures, a consistent message header, and exactly
one instruction whose program index resolves to the Vote program. Its complete,
canonical payload must decode as `Vote`, `VoteSwitch`, `UpdateVoteState`,
`UpdateVoteStateSwitch`, `CompactUpdateVoteState`,
`CompactUpdateVoteStateSwitch`, `TowerSync`, or `TowerSyncSwitch`, with a nonempty
vote history. Payloads larger than the current 1,232-byte packet ceiling stay in
ordinary output.

Administrative calls such as Withdraw, Authorize and InitializeAccount,
transactions with multiple instructions, versioned transactions, unused Vote
account keys, and unknown or malformed payloads keep their ordinary transaction,
message, instruction, balance, lookup and reward rows, subject to the usual
failed-transaction filter. This classification checks structure and recognized
payloads; it does not verify signatures cryptographically or prove execution
validity. Recognized votes retain the existing compact output policy: their
transaction row goes to `vote_transactions` and their detail rows are omitted.

Earlier versions classified any transaction mentioning the Vote account as a
vote. Their output can therefore omit administrative activity. This fix changes
row counts without changing schemas. Rebuild affected ranges into a separate
output root to recover missing rows; appending a corrected replay to old results
does not remove existing rows or guarantee deduplication.

## Transaction outcome context

Solana `messages`, `instructions`, `token_balances`, and `account_lookups` append
a non-null `boolean` `transaction_success`. It describes the parent transaction:
`false` means its source metadata contains nonempty error bytes; absent or empty
error bytes mean `true`. Transactions without metadata remain omitted. The
existing failed-transaction and vote filters still select exactly the same rows.

`rewards.transaction_success` is nullable: transaction rewards carry their
parent's outcome; block rewards have `NULL` because they have no parent
transaction. This does not change reward indices or amounts.

This is outcome context, not an instruction result or a `reverted` flag.
Submitted top-level instructions can include instructions that never executed;
recorded inner calls do not establish each call's success. Token balances remain
literal pre/post snapshots, and transaction fees and lamport balances remain
unchanged. A failed transaction can still pay fees or advance a durable nonce;
do not discard its balance observations merely because `transaction_success`
is false. This addition does not provide a canonical view of reversible events.

Start a fresh output root and replay when adopting these schemas. Old files lack
the context; a missing column is not `false`. Protected output bindings refuse
mixed old/new schemas. An explicit conversion must write
a separate dataset and preserve unknown historical context. See the
[source evidence, migration and offline comparison](../audit/550-solana-execution-context.md).

## Payloads and account indices

Opaque `instructions.data`, `transactions.err` / `return_data` and
`vote_transactions.err` / `return_data` are `binary`, independently of the
identifier encoding. Signatures, hashes, keys and `return_data_program_id` retain
the selected identifier format (base58 by default). Missing return data is null;
a present empty payload stays empty. Absent or empty errors remain null.

`instructions.accounts`, `account_lookups.writable_indexes` and
`account_lookups.readonly_indexes` are `array<non-null short>`, never null. They retain
source order, duplicates and empty lists. These are indices, not resolved keys.

```sql
-- Inspect payload bytes without requiring base58 conversion.
SELECT block_num, block_id, transaction_index, instruction_index,
       is_inner, inner_instruction_index, hex(data) AS data_hex, accounts
FROM delta_scan('output/solana-mainnet-beta/instructions');

-- Expand instruction account indices while preserving their source positions.
SELECT block_num, block_id, transaction_index, instruction_index,
       is_inner, inner_instruction_index,
       generate_subscripts(accounts, 1) - 1 AS account_position,
       unnest(accounts) AS account_index
FROM delta_scan('output/solana-mainnet-beta/instructions');
```

This changes older output schemas, including the index columns of the
`binary` byte encoding. Start a new output root and rebuild, or explicitly
convert into a separate dataset; do not append these types into an old
dataset. See the [migration and measured validation](../audit/503-solana-binary-payloads.md).

## Instruction order

The `instructions` table preserves the upstream order inside each top-level
instruction's inner set with two nullable `long` columns:

| Column | Top-level instruction | Inner instruction |
|---|---|---|
| `parent_instruction_index` | `NULL` | Zero-based index of the top-level instruction that owns the inner set |
| `inner_instruction_index` | `NULL` | Zero-based position within that parent's inner set |

The existing `instruction_index` still numbers all top-level instructions first,
then all inner instructions in upstream group order. It is a row index, not call
order. The existing `inner_index` still contains the top-level parent's index;
it has the same value as `parent_instruction_index`.

For one transaction in one block event, order its instructions as follows:

```sql
SELECT *
FROM delta_scan('output/solana-mainnet-beta/instructions')
WHERE block_id = '<block id>' AND transaction_index = 0
ORDER BY coalesce(parent_instruction_index, instruction_index),
         is_inner,
         inner_instruction_index;
```

This puts each top-level instruction before its recorded inner calls and keeps
the upstream order of those calls, including nested calls. `stack_height`, when
present, provides depth; `parent_instruction_index` identifies the top-level
owner, not the immediate caller of a nested instruction. For failed transactions
included with `--include-failed-transactions`, listed top-level instructions may
include instructions that did not execute. Apply fork semantics first when
querying reversible output; the ordering fields do not identify event delivery
order or remove replay duplicates.

Older files lack both new columns. Readers that union schemas by name can read
them as null, but nulls alone cannot distinguish old inner rows from top-level
rows. Check `is_inner` and rebuild old ranges into a separate output root before
depending on these fields.

## Reward indices

`rewards.reward_index` is a zero-based index within one block envelope. Rewards
from included transactions are numbered in transaction order and in each
transaction's upstream reward order, followed by block rewards in their upstream
order. The counter restarts for every block; flush size and restarts do not change
it. `source` identifies `transaction` or `block`, and `transaction_index` is null
for block rewards.

For finalized output, use `(block_id, reward_index)` as the reward key. Include
the chain/network when combining datasets. With reversible output, NEW and UNDO
rows are separate events that can share this key; apply fork semantics before
using it as a unique key. Changing transaction filters can change indices.

Older output may contain colliding indices within a block and indices offset by
earlier buffered blocks. Rebuild affected ranges into a separate output root
before relying on the corrected key; appending new output does not repair old rows.
