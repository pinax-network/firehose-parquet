# firehose-parquet: unreleased

Changes merged since [v1.5.1](v1.5.1.md). Fold this file into
`docs/releases/vX.Y.Z.md` when the next release is cut, then reset it to this
template.

Add one entry per change under the matching heading, with its issue and PR
(`#N` refs are fine). Say what changed for operators or data consumers, what
they must do (for example rebuild into a new output root), and link the record
in `docs/audit/` or elsewhere when there is one. Remove headings that stay
empty when the release is cut.

## Breaking changes

- New EVM datasets no longer have `gas_changes` and `system_gas_changes`: block version 5 of the EVM Firehose tracer records no gas changes. `--with-gas-changes` (`WITH_GAS_CHANGES=true`) writes them as before, with the same schemas and digests. **Set it on every existing EVM dataset with extended output (writers and backfill lanes) before upgrading**; without it `build` refuses to start, naming the flag ([Gas-change tables](../chains/evm.md#gas-change-tables)).

## New features

## Fixes

## Performance

## Internal
