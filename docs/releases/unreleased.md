# firehose-parquet: unreleased

Changes merged since [v1.0.0](v1.0.0.md). Fold this file into
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

## Performance

## Internal

- #658: `blocks/examples/bench_live_flush` measures catch-up throughput, the steady-state margin and per-phase commit latency of the real binary on Robinhood and Arbitrum One blocks, against local disk and a loopback HTTPS S3 with injected per-request latency. The v1.0.0 baseline is in [658-live-flush-benchmark.md](../audit/658-live-flush-benchmark.md). No writer behavior changed.
