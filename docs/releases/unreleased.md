# firehose-parquet: unreleased

Changes merged since [v1.7.0](v1.7.0.md). Fold this file into
`docs/releases/vX.Y.Z.md` when the next release is cut, then reset it to this
template.

Add one entry per change under the matching heading, with its issue and PR
(`#N` refs are fine). Say what changed for operators or data consumers, what
they must do (for example rebuild into a new output root), and link the record
in `docs/audit/` or elsewhere when there is one. Remove headings that stay
empty when the release is cut.

## Breaking changes

- **Tron is no longer supported: the Tron decoder, `--block-type tron`, the
  `tron_base58` encoding and the Tron-style EVM profile are removed
  (#747).** Pinax does not serve or support Tron, and v1.7.0 removed its
  network aliases (#735). This corrects v1.7.0's note that Tron blocks still
  decode from a Tron Firehose given with `--endpoint`: they no longer do.
  - `--block-type tron` is refused as an unsupported block type, and
    `--block-type auto` refuses a Tron Firehose's `sf.tron.type.v1.Block`.
  - An endpoint whose chain name is `tron-evm` is now plain EVM: every byte
    field is `0x` hex, with no Tron Base58Check addresses. Write it into a new
    output root.
  - An existing Tron dataset, native `tron` or EVM written from a `tron-evm`
    endpoint (`tron_base58`), is refused by name: `build` does not resume it,
    and `backfill`, `recovery recover` and `recovery rebind` refuse it, each
    when it loads the dataset's state and before it writes a table or that
    state ("this dataset was written from Tron, which fireparq no longer
    supports"). `recovery status` and `recovery release` still work on it,
    `fireparq maintenance` still compacts, VACUUMs and checkpoints its tables
    (it reads only their Delta logs), and `fireparq inspect` still reads its
    files. Every Delta reader (DuckDB `delta_scan`, Polars `scan_delta`)
    still reads its tables, whose schema is in
    [v1.7.0's docs](https://github.com/pinax-network/firehose-parquet/blob/v1.7.0/docs/schemas/tron.md).
    To keep writing one, stay on v1.7.0.

## New features

## Fixes

## Performance

## Internal
