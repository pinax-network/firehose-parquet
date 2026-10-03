# #690: compaction keeps the writer's row order

`fireparq-maintenance` up to 1.0.5 compacted with delta-rs's own OPTIMIZE
(`deltalake-core` 1.0.0), and its files did not hold the rows in the order
`fireparq build` wrote them. From 1.0.6 the job compacts with its own planner
(`maintenance/src/compact.rs`) and repairs the days compacted before. This
record keeps the evidence; [Row order](../delta-maintenance.md#row-order)
describes the behaviour.

## What delta-rs's OPTIMIZE did

On riv-dev1, eth 2026-09-20. The writer's own `part-v1-*` files were compared
with the compacted ones, in file order (DuckDB `file_row_number`):

| | Blocks out of order in the file | Blocks split into separate pieces | Blocks whose own rows are reordered |
|---|---|---|---|
| Writer files | 0 | 0 | 0 |
| Compacted transactions | yes | 47 of 3,153 | 0 |
| Compacted logs | yes | 420 of 7,171 | 1 |
| Compacted calls | yes | 366 of 1,700 | 1 |

- **Block 26018916 (logs):** logs 2508–4255 come about 108,000 rows before logs 0–2507.
- **Block 26021137 (calls):** the calls of transactions 305–795 come before those of 0–304.
- **No data change:** no row was lost, duplicated or changed.

Two causes, both in delta-rs 1.0.0's `operations/optimize.rs`:

- **Newest first:** `build_compaction_plan` takes the log's files newest first and bins contiguous runs of them.
- **Arrival order:** `read_selected_files` reads a bin through a DataFusion scan (`execute_stream`) over parallel partitions, and `rewrite_files` writes the batches in the order they arrive.

## The in-block keys

The repair sorts by `block_num` and then a per-table key
(`maintenance/src/row_order.rs`). For that to give the writer's order back, a
key must be strictly increasing in it. It was checked on the writer parts
present on 2026-10-02, in file order:

| Network | Writer files per table | Rows | Tables with rows | Decreases | Ties |
|---|---|---|---|---|---|
| eth | 215 | about 60 million | 14 | 0 | 0 |
| Base | 286 | about 411 million | 12 | 0 | 0 |
| BSC | 60 (a sample) | about 90 million | 13 | 0 | 0 |

- **First keys rejected:** `system_calls` failed on `call_index`, because each system call's tree numbers its calls from 1 again (the pre-block calls 1, 2, then the post-block calls 1, 2). `begin_ordinal` replaces it. For the same reason, the `system_*` change tables sort by `ordinal`, after the block's own changes (no `call_index`).
- **Tables with no rows yet:** `gas_changes`, `account_creations`, `system_code_changes`, `system_nonce_changes`, `system_gas_changes` and `system_account_creations` have no rows on these networks. Their keys follow the writer's loops (`blocks/src/evm/mapper.rs`). `blocks/tests/evm_golden.rs` checks every table of the reviewed mainnet blocks against the keys, and fails on `call_index` for `system_calls`.

## A real day, repaired and concatenated

Release build, local disk, from a copy of the riv-dev1 files and their `add` stats:

- **Repair (eth calls 2026-09-20).** Four delta-rs files (19.2 million rows, 0.9 GiB, 2.7–3.1 GiB uncompressed each) and a writer part.
  - The writer part was left alone; each of the four files was repaired in its own bin, in 32 s.
  - Rows, blocks and an order-independent checksum of every column equal the originals.
  - In file order: no block lower than the one before, no block in pieces, and no row out of `(tx_index, call_index)` order.
  - The commit was accepted with the table's real metadata, `delta.appendOnly` included.
  - Peak memory was 1.3 GiB with the default 256 MiB window. Sorting only the key columns and gathering the output from the decoded batches replaced a whole-window copy, which had peaked at 3.5 GiB.
- **Concatenation (eth calls 2026-10-02).** 223 writer parts (17 million rows, 979 MiB) became 4 files in 25 s, peaking at 0.9 GiB.
  - Rows and checksum are equal, and the order is exact.
  - A second run found nothing to compact.
