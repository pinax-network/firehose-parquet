# firehose-parquet: unreleased

Changes merged since [v1.1.1](v1.1.1.md). Fold this file into
`docs/releases/vX.Y.Z.md` when the next release is cut, then reset it to this
template.

Add one entry per change under the matching heading, with its issue and PR
(`#N` refs are fine). Say what changed for operators or data consumers, what
they must do (for example rebuild into a new output root), and link the record
in `docs/audit/` or elsewhere when there is one. Remove headings that stay
empty when the release is cut.

## Breaking changes

## New features

- **`VACUUM=0` skips VACUUM in a maintenance run** (#702, #703).
  - **Why:** a lite VACUUM doesn't check that a file still exists. When
    `VACUUM_RETENTION_HOURS` is below the table's
    `delta.deletedFileRetentionDuration` (7 days), every run deletes again the
    files of every tombstone between the two ages. On riv-dev1 (24 h, every
    15 min) that was about 10 million deletes a day over three networks. Each
    run also committed a `VACUUM START`/`VACUUM END` pair per table, a third to
    a half of every table's log, and those commits made the writers log
    `table updated during transaction, checking for conflicts`.
  - **What to do:** if you set a shorter retention, set `VACUUM=0` on the
    frequent runs and run a VACUUM once a day. If the shared settings set less
    than 168 hours, set `VACUUM_RETENTION_HOURS=168` in the weekly full
    VACUUM's environment, or it refuses to run (exit 2). With the default
    retention, nothing changes.
  - **Details:** a run with `VACUUM=0` still compacts, checkpoints and cleans
    up the log. `VACUUM=0` with `FULL_VACUUM=1` is refused. An empty flag value
    now means its default (it used to mean off), so `VACUUM=` keeps VACUUM on.
  - See [VACUUM retention and schedule](../delta-maintenance.md#vacuum-retention-and-schedule)
    and the [record](../audit/702-daily-vacuum.md).

## Fixes

## Performance

## Internal
