# #702: VACUUM once a day, not every 15 minutes

Issue: [#702](https://github.com/pinax-network/firehose-parquet/issues/702).
PR: [#703](https://github.com/pinax-network/firehose-parquet/pull/703).
Released in [v1.1.2](../releases/v1.1.2.md).

## Symptom

After each maintenance run, the riv-dev1 writers logged this warning once for
each table the run vacuumed, about 14 per network:

```
WARN table updated during transaction, checking for conflicts base_version=2658 latest_version=2660 versions_behind=2
```

delta-rs 1.0.0 (`kernel/transaction/mod.rs`) logs it when a table has new
versions since the snapshot a commit was prepared on. It then checks each new
version for conflicts. The two versions were always a `VACUUM START` and a
`VACUUM END` commit from the 15-minute maintenance job. Those carry no file
actions, so the check passed and the writer's commit landed on the next
version. The warning was harmless, but the VACUUM behind it was not.

## Measured on riv-dev1 (2026-10-04)

The maintenance job runs every 15 minutes per network with
`VACUUM_RETENTION_HOURS=24`. Files its lite VACUUM reported deleted in one run:

| Network | Tables vacuumed | Files per run | Per day (96 runs) |
|---|---|---|---|
| eth | 14 | 24,325 | 2.3 M |
| base | 12 | 26,244 | 2.5 M |
| bsc | 13 | 55,329 | 5.3 M |

- **The same files every run.** On eth `blocks`, the checkpoint held 1,978
  tombstones, 1,732 of them older than 24 h. Eight of those files, sampled at
  random, returned 404: they had already been deleted. `numFilesToDelete` was
  1,730 at 04:32 UTC and 1,731 at 04:47.
- **Log growth.** Each run committed a `VACUUM START`/`VACUUM END` pair to
  every table it vacuumed: 192 versions a day per table. The writers commit
  every 3–6 minutes, so these pairs were a third to a half of every table's
  log.

## Cause

- **Tombstone lifetime.** A replaced file's `remove` tombstone stays in the
  log for the table's `delta.deletedFileRetentionDuration`, 7 days. A
  checkpoint drops it after that.
- **No existence check.** A lite VACUUM deletes the file of every tombstone
  older than its retention, and doesn't check that the file still exists.
  Deleting a missing key succeeds, and delta-rs counts it as deleted. If any
  file is planned, it also commits the `VACUUM START`/`END` pair.
- **Retention below the lifetime.** With the default retention (7 days), each
  file is deleted once, by the first run after its tombstone expires, and that
  run's checkpoint then drops the tombstone. riv-dev1 set 24 h so that a closed
  day's folder holds only its compacted files a day after compaction. Every run
  then deleted again the files of all tombstones between 24 h and 7 days old:
  about 570 times per file.

Every closed day's writer parts are tombstoned when the day is compacted, so
this was the steady state, not leftovers of the v1.0.6 repair.

## Fix

- **The setting.** `VACUUM=0` (`Settings::vacuum`, default `1`) skips step 2
  in `maintain`. The table line reports `"vacuum": {"mode": "off"}`, and the
  `start` line gains `vacuum`. `VACUUM=0` with `FULL_VACUUM=1` is a
  configuration error (exit 2).
- **The checkpoint.** A `VACUUM=0` run still checkpoints and cleans up the
  log, so readers keep a short log tail. Design §4.1 checkpoints only after a
  successful VACUUM, because a checkpoint drops expired tombstones. A
  `VACUUM=0` run's checkpoint drops only tombstones older than
  `deletedFileRetentionDuration`. That is safe while the daily run's retention
  plus its interval stays below it (24 h + 24 h against 7 days): the daily
  VACUUM deleted their files days before. If the daily run fails for about
  five days in a row, a missed file loses its tombstone and becomes untracked,
  and the weekly full VACUUM deletes it.
- **Empty flags.** An empty flag value now means the flag's default; it meant
  off. Every existing flag defaults to off, so nothing changes for them, but
  `VACUUM=` keeps VACUUM on.
- **Docs.** `docs/delta-maintenance.md` explains when to VACUUM daily,
  "VACUUM retention and schedule". The example CronJob's weekly full VACUUM
  sets `VACUUM_RETENTION_HOURS=168`, so a lower shared value can't make it
  refuse.

With `VACUUM=0` on the 15-minute runs and one VACUUM a day, each file is
deleted about 6 times (once a day until its tombstone expires), and VACUUM
commits drop from 192 to 2 a day per table. Files are still deleted 24–48 h
after compaction.

## The weekly full VACUUM had never run on riv-dev1

The weekly full VACUUM CronJobs read the same settings ConfigMap, so they got
`VACUUM_RETENTION_HOURS=24`. The job refuses a full VACUUM below 168 h, and
their first runs on 2026-10-04 (03:47, 04:07 and 04:27 UTC) all exited 2:

```
{"error":"a full VACUUM deletes untracked files, including parts fireparq has not committed yet, so it needs VACUUM_RETENTION_HOURS >= 168","event":"config_error"}
```

The refusal worked as designed. The fix is in k8s-parquet: the full VACUUM's
own environment sets `VACUUM_RETENTION_HOURS=168`
([k8s-parquet#54](https://github.com/pinax-network/k8s-parquet/pull/54)).

## Tests

| Test | Checks |
|---|---|
| `fireparq_maintenance::tests::vacuum_is_on_unless_turned_off` | Default on; an empty value is unset; `0` and `false` turn it off; another value is refused |
| `blocks/tests/maintenance_cli.rs` `configuration_errors_exit_2_before_any_request` | `FULL_VACUUM=1` with `VACUUM=0`, and an unknown `VACUUM` value: exit 2, one `config_error` line, nothing written |
| `blocks/tests/delta_maintenance.rs` `vacuum_off_compacts_and_checkpoints_but_deletes_nothing` | A `VACUUM=0` run with a retention of 0 compacts a day, checkpoints at its last version, reports `mode: off`, commits no VACUUM and leaves the 3 replaced parts; a run with VACUUM then deletes them with its two commits |

## Rollout on riv-dev1 (2026-10-04)

[k8s-parquet#54](https://github.com/pinax-network/k8s-parquet/pull/54) made
these changes, and Flux applied it at 13:28 UTC:

- **Image:** fireparq 1.1.2 for the writers, ownership recovery and every
  maintenance CronJob.
- **15-minute jobs:** `VACUUM=0`.
- **New daily job:** `delta-maintenance-vacuum`, at 06:09, 06:19 and
  06:29 UTC for eth, base and bsc.
- **Weekly full VACUUM:** `VACUUM_RETENTION_HOURS=168`.
- **New alert:** `ParquetDailyVacuumNotSucceeding`. It also fires for a job
  that was scheduled and never succeeded, the gap that hid the full VACUUM's
  refusals.

Checks:

- **Writers.** They restarted on 1.1.2 at 13:27:56 UTC, became ready and kept
  committing.
- **Daily jobs.** Each was run once by hand (`create job --from=cronjob`),
  13:29–13:31 UTC. All reported version 1.1.2, a lite VACUUM at 24 h, 20 tables
  and no failure, in about 30 s each.

  | Network | Files deleted |
  |---|---|
  | eth | 24,785 |
  | bsc | 56,111 |
  | base | 26,445 |

  These were the repeated set, one last time.
- **First `VACUUM=0` runs** (13:32 eth, 13:37 bsc, 13:42 UTC base): each
  covered 20 tables with `{"mode": "off"}` and wrote a checkpoint for every
  table. They committed nothing (`version_before` = `version_after`) and had
  no errors. They took 15–18 s; runs with VACUUM took about 30 s.
- **Delta logs.** After the manual runs, the `blocks` logs of all three
  networks hold only the writers' `WRITE` commits, with no `VACUUM START` or
  `END`.
- **Writer warnings.** After 13:31:30 UTC the writers logged no warning. The
  manual runs' VACUUM commits produced the last ones, as each daily run will
  once a day.
- **Weekly full VACUUM.** It renders with `FULL_VACUUM=1` and
  `VACUUM_RETENTION_HOURS=168`. Its first run is Sunday 2026-10-11 at
  03:47 UTC (eth).
