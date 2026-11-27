# firehose-parquet: unreleased

Changes merged since [v1.6.0](v1.6.0.md). Fold this file into
`docs/releases/vX.Y.Z.md` when the next release is cut, then reset it to this
template.

Add one entry per change under the matching heading, with its issue and PR
(`#N` refs are fine). Say what changed for operators or data consumers, what
they must do (for example rebuild into a new output root), and link the record
in `docs/audit/` or elsewhere when there is one. Remove headings that stay
empty when the release is cut.

## Breaking changes

- **New EVM datasets no longer have `account_creations`,
  `system_account_creations` or `system_nonce_changes` (#743).** Current EVM
  Firehose blocks never fill them. Block version 4 and later record no account
  creations, and system calls change no nonce. On the public lake,
  `account_creations` has no rows after Pectra on Ethereum mainnet or after
  Pascal on BSC, and none on Base; the two system tables never had one.
  `--with-account-creations` (`WITH_ACCOUNT_CREATIONS=true`) and
  `--with-system-nonce-changes` (`WITH_SYSTEM_NONCE_CHANGES=true`) write them
  as before, with the same schemas and digests, like `--with-gas-changes`
  (#738). **Set both on every existing EVM dataset with extended output
  (writers and backfill lanes) before upgrading.** Without them, `build`
  refuses to start and names each missing flag, before any Blocks request.
  v1.6.0 ignores both variables, so they can be set before the upgrade. New
  EVM datasets with extended output have 15 tables
  ([Opt-in tables](../chains/evm.md#opt-in-tables)).
- **Rollback:** v1.6.0 always declares these three tables, so it refuses an
  EVM dataset created without them.
- `fireparq build --network blast-mainnet` has been removed because Pinax retired
  its Blast Firehose endpoint. To ingest from another Blast Firehose, pass its
  URL with `--endpoint`. Existing Parquet files can still be read by path with
  `fireparq inspect`. [Upstream registry change](https://github.com/graphprotocol/networks-registry/pull/265)

## New features

## Fixes

## Performance

## Internal
