# firehose-parquet: unreleased

Changes merged since [v1.0.3](v1.0.3.md). Fold this file into
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

- **A provider blip no longer strands S3 ownership** ([#646](https://github.com/pinax-network/firehose-parquet/issues/646), option 3, [#686](https://github.com/pinax-network/firehose-parquet/pull/686);
  [record](../audit/646-uncertain-mutation-readback.md)).
  - **Readback.** When a PUT's outcome is unknown (timeout, lost
    acknowledgement, connection reset, gateway 5xx), `build` now reads the key
    back for about 15 s. This covers a data part, control record, cursor mirror
    or owner record. Exactly the state the request writes proves it, and the
    build continues instead of exiting with ownership retained. Before, a part
    or mirror PUT was never read back after an error, and control and owner
    records were read once.
  - **Release retries.** The owner release at exit is retried for about two
    minutes while the provider does not answer.
  - **Unchanged.** Absence or the previous state is never proof, nothing is
    resent, and a DELETE, a 409/412, a second signal or a panic still keeps
    ownership for `recovery release`. This follows the 2026-09-29 RGW rolling
    restart that left two backfilling writers retained.

## Performance

## Internal
