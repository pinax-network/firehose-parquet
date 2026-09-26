# Final completion checkpoints (#572)

## Current path (after #600)

The diagnosis and helper-level fix below describe the pre-#600 writer, which no
longer exists. `run_ingestion` no longer uses `OutputWriter`, and the
`flush_writer_on_exit` / `write_mapper_flush` helpers and their unit tests were
deleted in the [validation follow-up](validation-ingest-followups.md).

On a clean end of stream, `IngestionRuntime::finish` takes the remaining mapper
window and calls `IngestionSession::flush`. That is one all-table transaction:
the controller journals the flush, publishes every part, advances authority to
the last accepted event and then reconciles the cursor mirror. Only after it
returns does `complete_request` record the proven completed stop, again in
authority first and the mirror second. There is no separate "did the final
write materialize" flag to lose: a stream-end window either commits with its
checkpoint or the command fails and recovery reconciles the journal. A shutdown
or failed stream discards the window without committing.

The real-path regression is
`stream_end_mapper_flush_commits_the_final_checkpoint` in
`blocks/tests/ingestion_transactions.rs`: five blocks with `--flush-blocks 2`
leave block 104 for the stream-end drain; the test checks that its part carries
ordinals 5..5, authority and the mirror both reach block 104 with
`completed_stop = 105`, and no journal remains.

## Historical record (pre-#600 writer)

## Diagnosis and reproduction

On a successfully completed stream, `run_ingestion` flushed the remaining
mapper rows through `OutputWriter::write_all` and discarded the returned
materialization flag. That call can itself write all buffered Parquet data.
The following `flush_writer_on_exit` then saw no remaining writer buffers and
skipped the cursor callback. Data files existed, but the cursor was absent or
stale and the run returned success. A later run could replay those blocks into
new part files.

This defect predates the bounded cursor-save retry fix in #469. Retry handling
cannot detect a checkpoint that is never attempted.

The regression uses the real Solana mapper with one timestamped block at slot
42, no transactions, `Partition::None`, Zstd, and `flush_bytes = 4096`. Before
finalization the mapper estimates 108 logical bytes, below the normal mapper
flush threshold. Its finalized `blocks` Arrow batch occupies 99,984 allocated
bytes in the reproduced build. The writer's compressed-size estimate crosses
4096, so `write_all` writes a Parquet file and empties the buffers. Before this
fix, the completion helper returned false and no cursor existed. The regression
checks behavior rather than hard-coding the allocation size.

Mapper logical byte estimates and allocated Arrow batch memory differ; the
bug therefore does not require an artificial writer state. Completion can also
append a trailing Solana timestamp-backfill span before its last mapper flush.
`--flush-bytes 0` disables byte-triggered rollover and is not the trigger used
by this regression.

## Implementation

`run_ingestion` retains a `final_mapper_materialized` flag for the finalization
phase only. Earlier normal-loop flushes have already run their checkpoints and
do not set this flag.

For `StreamExit::Completed`, `flush_writer_on_exit` always drains the writer
first. It then commits exactly once if either the final mapper write or that
drain wrote output. It never short-circuits the drain when the flag is true:
a partition rollover can write the old partition while deferring the next
partition's batches. A failure writing those remaining batches must prevent
the checkpoint.

Failed and shutdown exits still discard partial buffers without advancing the
cursor. Completion without new materialized output still does not save. The
existing save callback, bounded retries, error propagation and metrics from
#469 remain in use. A cursor failure after an automatic final mapper flush is
now attempted and reported, just as a failure after a remaining-buffer flush.

The only production changes are the finalization flag, its helper argument and
the completed-stream checkpoint decision. There is no writer rollover change,
new dependency or ingestion-loop refactor.

## Regression coverage and validation

- The real Solana mapper regression verifies that a Parquet file exists before
  the final drain, writer buffers are empty, and the saved cursor reaches block
  42. It checks one successful save, no failures and a populated success-time
  metric.
- A two-date writer fixture first materializes one partition and defers the
  other. The checkpoint callback verifies the deferred file already exists;
  it runs once and leaves empty buffers. Blocking the deferred partition's
  directory instead makes completion fail without calling the checkpoint.
- The final cursor-failure test covers both a buffered batch and a batch that
  auto-materialized before the drain. Each produces three failed save attempts,
  no successful save, no success timestamp and a propagated completion error.
- Empty completion and completion after an earlier normal-loop write do not
  run another checkpoint. Failed and shutdown exits retain deferred rows in
  memory without writing them or running the callback, even if earlier output
  was materialized.

Validation used the shared audit target with dev/test debug information
disabled, after integrating the merged cursor retries and S3 routing fixes
through `fa791ac`. Cargo processes were serialized for the shared target directory.

- `cargo test --workspace --locked -j4`: 720 passed, zero failed and three
  ignored (110 mapper, 191 binary, one startup integration, 415 core, and three
  network-registry tests); all doc tests passed.
- `cargo build --bin fireparq --locked -j4`: passed.
- Bash, Zsh and Fish shell-completion generation from the built binary: passed.
- `cargo fmt --all --check` and `git diff --check`: passed.
- Independent code review: no blockers.

The pre-existing unused `transactions_processed` assignment warning in the
trailing timestamp-backfill path remains unchanged.

## Bounded live qualification

On 2026-09-25, the binary built from `11bcf21` (the same implementation rebased
onto merged #571 at `62d7b10`) was copied to its own stable path while holding
the shared build lock. It completed the single-block Solana range
`[300000000, 300000001)` through
`https://solana.firehose.pinax.network:443`, using the existing provider-scoped
credentials. Output and the explicit cursor were confined to a fresh local
temporary directory. No remote storage was written.

The run used no partitioning, Zstd, `--flush-bytes 1073741824`, and large row,
block and interval thresholds so the normal loop did not flush. It exited zero,
wrote eight table files, emitted one final writer materialization message and
no normal mapper-flush message. Reading the saved local cursor confirmed
`last_block_num = 300000000`. The opaque cursor and credentials were not printed
or added to this document.

This live check qualifies the real provider-to-mapper-to-final-drain path. The
smaller-threshold automatic mapper materialization condition is proven by the
deterministic real-mapper regression described above; the live check does not
claim to exercise that allocation-dependent condition.

This closes a missing successful-completion checkpoint. It does not make the
data files and cursor update a single storage transaction; crash/replay
atomicity remains tracked separately in #468.
