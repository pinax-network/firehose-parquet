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

- **Dependency updates for two advisories, and one advisory ignored**. Only
  `Cargo.lock` and `deny.toml` changed:
  - parquet 60.2.0 and 59.3.3, with the rest of arrow at the same versions
    (RUSTSEC-2026-0595: Bloom filters written in plaintext when writing
    encrypted Parquet; fireparq writes no encrypted Parquet).
  - url 2.5.9 (RUSTSEC-2026-0592: stack overflow in `Url::origin()` on nested
    `blob:` URLs).
  - RUSTSEC-2026-0543 (chrono is unmaintained) is ignored: there is no
    patched release, and arrow and delta-rs depend on chrono too.

## Performance

## Internal
