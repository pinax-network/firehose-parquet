# firehose-parquet: unreleased

Changes merged since [v1.3.1](v1.3.1.md). Fold this file into
`docs/releases/vX.Y.Z.md` when the next release is cut, then reset it to this
template.

Add one entry per change under the matching heading, with its issue and PR
(`#N` refs are fine). Say what changed for operators or data consumers, what
they must do (for example rebuild into a new output root), and link the record
in `docs/audit/` or elsewhere when there is one. Remove headings that stay
empty when the release is cut.

## Breaking changes

- EVM datasets add `erc20_transfers` and, with extended output,
  `native_transfers` (#713). The table inventory changes the protected output
  identity: rebuild into a new empty output root to obtain the transfer history.
  Existing EVM roots cannot resume under this schema. Other chain families and
  the existing EVM table schemas are unchanged.

## New features

- Built-in EVM ERC-20 receipt transfers use the shared `substreams-abis`
  decoder; native transfers record persisted positive CALL/CREATE values,
  including internal contract creation. Exact amounts, source identities and
  non-final stream events are retained. See [coverage and balances](../chains/evm.md#decoded-transfers)
  for excluded events and the distinction from account-balance snapshots.

## Fixes

## Performance

## Internal
