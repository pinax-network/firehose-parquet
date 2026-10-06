# firehose-parquet: unreleased

Changes merged since [v1.1.2](v1.1.2.md). Fold this file into
`docs/releases/vX.Y.Z.md` when the next release is cut, then reset it to this
template.

Add one entry per change under the matching heading, with its issue and PR
(`#N` refs are fine). Say what changed for operators or data consumers, what
they must do (for example rebuild into a new output root), and link the record
in `docs/audit/` or elsewhere when there is one. Remove headings that stay
empty when the release is cut.

## Breaking changes

## New features

- **HyperCore (`--block-type hypercore`)**: the HyperLiquid L1 from Pinax's
  `pinax.hypercore.v1` Firehose (`hypercore.firehose.pinax.network:443`),
  schema epoch 1. Five tables: `blocks`, `fills`, `events` (one row per
  event, its body and ledger delta flattened, with `event_type` and
  `ledger_type`), `funding_deltas` and `validator_rewards`
  ([schema](../schemas/hypercore.md), [notes](../chains/hypercore.md)).
  - Amounts, prices and sizes are exact `decimal(38,10)` values, parsed
    without rounding; a string that is not exact refuses the block.
  - `block_id` and `parent_id` are the decimal block number as text
    (HyperCore has no block hash); files record
    `firehose-parquet.block_id_encoding = decimal`.
  - A block is validated completely before any row is appended, and a block
    with fields the vendored protos do not know, an unknown enum value or an
    empty required value is refused with its block, protobuf path and value
    (rules R1–R11). Such a stop needs a release with refreshed protos;
    `extra_json`, NULL in this epoch, lets most additions ship without a new
    output root.
  - There is no built-in `--network` name: use
    `--endpoint https://hypercore.firehose.pinax.network:443 --api-key-envvar PINAX_API_KEY`
    and start at block 846903317. The endpoint lacks blocks
    846903300–846903312 and cannot be streamed across them.
  - `docs/chains/hypercore.md` adds a DuckDB view pack (a view per ledger
    type and body), 17 data-quality monitors and a cookbook, which
    `engine_compat` runs over a build of 36 real fixture blocks.
  - `user`, `destination`, `vault`, `validator`, `liquidated_user` and
    `sub_account` columns get Parquet Bloom filters; no other family has
    columns of those names.
  - New roots only: the family is part of the protected stream identity, so
    existing roots are unaffected and an older binary refuses a HyperCore
    root. `MAPPER_EPOCH` is unchanged.

## Fixes

- **No credential-scope `WARN` for a secure Pinax host outside the network
  registry.** An explicitly selected Pinax credential sent to such a host (over
  HTTPS on port 443), for example `--api-key-envvar PINAX_API_KEY` to
  `hypercore.firehose.pinax.network`, logged that it was sent to a non-Pinax
  host and advised unsetting the selector, which leaves the stream
  `Unauthenticated`. Any `*.pinax.network` host over HTTPS on port 443 now
  counts as Pinax for that warning. Automatic selection is unchanged: such a
  host still gets no credential unless one is selected explicitly
  ([Authentication](../authentication.md)).

## Performance

## Internal

- **A local ownership guard unlocks its directories when it is dropped**, before
  closing them (#706). A process spawned while the guard was held keeps a copy
  of each locked descriptor until it execs, and a `flock` lock belongs to the
  open file description that the copy shares. So the lock could outlive the
  guard, and a new acquisition right after failed with "lock acquisition
  failed because the operation would block". In the test suite, where tests
  run child processes beside each other, that made
  `nested_symlinks_fail_before_mutation_but_explicit_root_aliases_work` fail
  now and then. `fireparq` itself starts no child processes, so it wasn't
  affected. `a_dropped_guard_releases_its_locks_while_inherited_descriptors_live`
  holds a copy of the descriptors and acquires again.
