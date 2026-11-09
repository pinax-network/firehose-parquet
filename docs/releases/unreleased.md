# firehose-parquet: unreleased

Changes merged since [v1.3.0](v1.3.0.md). Fold this file into
`docs/releases/vX.Y.Z.md` when the next release is cut, then reset it to this
template.

Add one entry per change under the matching heading, with its issue and PR
(`#N` refs are fine). Say what changed for operators or data consumers, what
they must do (for example rebuild into a new output root), and link the record
in `docs/audit/` or elsewhere when there is one. Remove headings that stay
empty when the release is cut.

## Breaking changes

## New features

## Fixes

- **A replay with commits of 5 seconds or more is detected as a catch-up**
  (#721, #722).
  - **What was wrong:** a writer that started deep in a backfill could stay
    `caught_up` for the whole replay: `firehose_parquet_catching_up` stayed 0,
    flushes carried `pace="caught_up"`, and `--flush-interval-secs` stayed
    armed. It happened when each flush's commit blocked the stream for longer
    than one 5-second pace sample and the blocks between two commits arrived
    in less than that. Seen on Ethereum near block 22.8M with
    `--flush-memory-bytes` 1 GiB (240 blocks over 4 s, then a 5.4 to 6.5 s
    commit). A writer that entered the catch-up earlier, such as one started
    at genesis, kept it.
  - **Now:** the writer's own commit is no longer measured as a wait of the
    stream, and it does not count toward the 30 seconds of evidence that
    start a catch-up. The writer above switches after about 80 seconds.
  - **Operators:** nothing to do. An alert on `firehose_parquet_catching_up
    == 0` and a large `firehose_parquet_block_time_lag_seconds` no longer
    fires for such a backfill. No output, schema or dataset state changes.
  - [Record](../audit/659-adaptive-flush.md#the-writers-own-commits).
- **Dependency updates for nine security advisories** (#722). Only
  `Cargo.lock` changed:
  - rustls-webpki 0.103.16 (RUSTSEC-2026-0382) and webpki-root-certs 1.0.11
    (RUSTSEC-2026-0383): certificate paths through distrusted emSign
    intermediates could be accepted.
  - zstd 0.13.4 and 0.14.3 (RUSTSEC-2026-0391: data from an abandoned stream
    leaks into the next stream on a reused context), zstd-safe 7.4.0 and
    8.2.0 (RUSTSEC-2026-0398, RUSTSEC-2026-0403) and zstd-sys 2.1.2
    (RUSTSEC-2026-0410). The bundled zstd library stays 1.5.7, so compressed
    output is unchanged.
  - tokio 1.53.3 (RUSTSEC-2026-0386: `AsyncFd::new` violates I/O safety).
  - pulldown-cmark 0.13.6 (RUSTSEC-2026-0414, RUSTSEC-2026-0433), used only
    at build time by prost-build.

## Performance

## Internal

- **`local_resume_walks_no_data_directory` no longer depends on directory
  order** (#722): the recovery tree walk it compares against may stop at the
  unreadable directory before the nested symlink, as it does on the current
  CI runners.
