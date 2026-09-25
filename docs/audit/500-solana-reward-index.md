# Solana reward indices independent of flushes (#500)

## Diagnosis and preserved work

Reviewed against main `c88abcc` on 2026-09-25. The previous agent's
`audit/500-solana-reward-index` checkout remained clean at `8ac96f5`; it had no
implementation to recover and was left unchanged. This fix uses a new isolated
checkout, `codex/solana-reward-index`.

Transaction rewards used the length of the rewards builder, which included
prior blocks since the last flush. Block rewards separately started at zero.
The combination made indices depend on earlier buffering and allowed a block's
transaction and block rewards to share an index.

## Chosen behavior

One counter is created for each block envelope and passed through transaction
and block reward emission. Existing row order is preserved: included transaction
rewards in transaction/upstream reward order, then block rewards in upstream
order. Skipped transactions consume no reward indices, matching their absent
rows. Counter conversion to UInt32 is checked and propagates an error instead
of wrapping. Public mapper interfaces and the output schema are unchanged.

The counter restarts on every envelope, including repeated NEW/UNDO events.
For finalized data, the reward key is `(block_id, reward_index)` within a network.
Reversible events can repeat it and still require fork-aware consumption.
Indices are stable for the same mapper options; changing transaction filters
can change them. This change does not alter the failed-effect policy in #550.

Existing data is not rewritten automatically. Old duplicate or buffer-offset
indices remain until affected ranges are rebuilt under a separate output root.
README and release notes describe the value migration and changed verify roots.

## Regression and verification

The regression uses multiple transaction rewards, multiple block rewards,
filtered failed transactions and the sequence NEW(100), NEW(101), UNDO(101),
NEW(101). It compares the complete rewards batches across flushes after every
four, two or one envelopes and across a fresh mapper for every envelope. Both
failed-transaction filter settings are exercised. It separately checks dense
per-envelope indices and unchanged source/fork ordering.

The new regression failed against the original implementation and passed with
the fix. Independent code review found no functional blockers. The integrated
implementation `f0bb91c`, including writer simplification, responsive shutdown
and atomic publication, passed **752 workspace tests**, zero failed and four
ignored (three benchmark helpers and one child-process helper invoked by its
parent tests). Build, formatting and whitespace checks passed. Subsequent
integration through merged main `73257c2` changed only documentation and commit
ancestry. Bash/Zsh/Fish completion generation also passed.

A bounded live run used the explicit Pinax Solana endpoint with provider-scoped
credentials and fresh local output for slots `[300000000, 300000002)`. Two runs
used `--flush-blocks 1` and `--flush-blocks 2`, with the other triggers set above
the sample. Both completed at cursor block 300000001. DuckDB 1.1.1 found equal
schemas and zero differences in both directions with `EXCEPT ALL` across all
**eight tables / 15,832 rows**. Each slot's one block reward had index zero.
The sample contained no transaction rewards; the mixed-source indexing defect
is covered by the reproducing regression, not claimed as live-qualified from
this sample. No production S3 writes were performed.
