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
  ([schema](../schemas/hypercore.md), [notes](../chains/hypercore.md))
  (#709).
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
  - `--network hypercore` streams it with the ambient `PINAX_API_KEY` (or
    `SUBSTREAMS_API_KEY`), like any Pinax alias; see the internal Pinax
    networks below.
  - HyperCore data is known from 2026-01-01: a new root starts at block
    846903317 (2026-01-01T00:00:00.063Z) by default, and an explicit
    `--start-block` before it is refused. The endpoint advertises
    846000000 but lacks blocks 846903300–846903312, which a stream cannot
    cross; see the data origins below.
  - `docs/chains/hypercore.md` adds a DuckDB view pack (a view per ledger
    type and body), 17 data-quality monitors and a cookbook, which
    `engine_compat` runs over a build of 36 real fixture blocks.
  - `user`, `destination`, `vault`, `validator`, `liquidated_user` and
    `sub_account` columns get Parquet Bloom filters; no other family has
    columns of those names.
  - New roots only: the family is part of the protected stream identity, so
    existing roots are unaffected and an older binary refuses a HyperCore
    root. `MAPPER_EPOCH` is unchanged.
- **Built-in aliases for Pinax networks the registry does not list yet**:
  `PINAX_NETWORKS` in `scripts/generate_networks.rs` is a reviewed list of
  Pinax-served Firehose networks outside The Graph networks registry, appended
  to the registry's aliases, starting with `hypercore` →
  `hypercore.firehose.pinax.network:443`. Their hosts are built-in Pinax hosts
  for credential selection, `FIREHOSE_ENDPOINT_*` overrides them, and the
  endpoint check covers them. Once the registry gives such a network an
  alias, the registry entry wins and the generator warns until the entry is
  dropped; it also warns when the registry lists the name without an accepted
  endpoint, or gives another name the same endpoint host. The registry
  snapshot is unchanged (v0.8.4)
  ([network registry integration](../network-registry-integration.md#internal-pinax-networks))
  (#709).
- **Per-network data origin**: `NETWORK_DATA_ORIGINS`
  (`firehose-parquet/src/networks.rs`) records where a network's known data
  starts when its endpoint advertises earlier blocks; HyperCore's is block
  846903317. Matched by the EndpointInfo chain name, a new stream without
  `--start-block` starts there with an info line, and an earlier
  `--start-block` is refused before streaming, in dry runs too. Resuming from
  output authority and other networks are unaffected
  ([start and stop blocks](../cursor-and-resume.md#start-and-stop-blocks))
  (#709).

## Fixes

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
