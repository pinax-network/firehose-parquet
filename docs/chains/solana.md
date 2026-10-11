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
validity. A recognized vote writes only its transaction row, to
`vote_transactions`; it has no message, instruction, balance, lookup or reward
rows.

## Transaction outcome context

`messages`, `instructions`, `token_balances` and `account_lookups` carry a
non-null `boolean` `transaction_success`. It describes the parent transaction:
`false` means its source metadata contains nonempty error bytes; absent or empty
error bytes mean `true`. Transactions without metadata are not written.

`rewards.transaction_success` is nullable: transaction rewards carry their
parent's outcome; block rewards have `NULL` because they have no parent
transaction.

This is outcome context, not an instruction result or a `reverted` flag.
Submitted top-level instructions can include instructions that never executed;
recorded inner calls do not establish each call's success. Token balances are
literal pre/post snapshots. A failed transaction can still pay fees or advance a
durable nonce; do not discard its balance observations merely because
`transaction_success` is false. See the
[source evidence and offline comparison](../audit/550-solana-execution-context.md).

## Payloads and account indices

Opaque `instructions.data`, `transactions.err` / `return_data` and
`vote_transactions.err` / `return_data` are `binary`. Signatures, hashes, keys
and `return_data_program_id` are base58 text, like the other identifiers.
Missing return data is null; a present empty payload stays empty. Absent or
empty errors are null.

`instructions.accounts`, `account_lookups.writable_indexes` and
`account_lookups.readonly_indexes` are `array<non-null short>`, never null. They keep
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

See the [measured validation](../audit/503-solana-binary-payloads.md).

## Instruction order

The `instructions` table preserves the upstream order inside each top-level
instruction's inner set with two nullable `long` columns:

| Column | Top-level instruction | Inner instruction |
|---|---|---|
| `parent_instruction_index` | `NULL` | Zero-based index of the top-level instruction that owns the inner set |
| `inner_instruction_index` | `NULL` | Zero-based position within that parent's inner set |

`instruction_index` numbers all top-level instructions first, then all inner
instructions in upstream group order. It is a row index, not call order.
`inner_index` holds the top-level parent's index, the same value as
`parent_instruction_index`.

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
