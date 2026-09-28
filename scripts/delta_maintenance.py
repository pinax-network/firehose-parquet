#!/usr/bin/env python3
"""Reference Delta maintenance job for a fireparq lake (#643).

Compaction and cleanup of fireparq's Delta tables are platform-side policy:
this script is the off-the-shelf `deltalake` package on a schedule (a k8s
CronJob, see `deploy/examples/delta-maintenance-cronjob.yaml`), not fireparq
code. It is safe beside a running `fireparq build`: fireparq's commits are
blind appends that rebase over OPTIMIZE, and VACUUM follows the rule of
`docs/design/delta-lake.md` §4.1. Every step is idempotent, so a failed or
conflicting run is simply repeated by the next one.

For each table, in this order:

1. OPTIMIZE (`compact`) each closed `date` partition that has more than one
   file, to the table's `delta.targetFileSize`. A date is closed once `blocks`
   holds a later date: `blocks` commits last in every fireparq transaction, so
   every earlier date is complete in every table.
2. VACUUM. Lite (the default) deletes only files that a `remove` tombstone
   older than the retention names, and never a part that fireparq published
   but has not committed yet. Full (`FULL_VACUUM=1`, weekly) also deletes
   untracked files older than the retention, so it runs only with the enforced
   retention of at least 168 h (`delta.deletedFileRetentionDuration`).
3. A checkpoint, after VACUUM: a checkpoint drops expired tombstones, so a
   lite VACUUM after it would never see them and their files would stay as
   orphans. The checkpoint is skipped when VACUUM failed, and no OPTIMIZE or
   VACUUM commit writes one on its own.
4. Log cleanup: commits older than `delta.logRetentionDuration` behind a
   checkpoint.

Settings (environment variables; credentials are read, never printed):

- `LAKE_ROOT`: the dataset root, `s3://bucket[/prefix]` or a local path; or
  `LAKE_BUCKET`, shorthand for `s3://<bucket>` (a lake at the bucket root).
- `LAKE_TABLES` (required): comma-separated table names, for example
  `blocks,transactions,logs`. `blocks` must exist below the root.
- S3: `S3_ENDPOINT` (for example the in-cluster RGW URL), `AWS_REGION`
  (default `us-east-1`), `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
  optional `AWS_SESSION_TOKEN`, `AWS_ALLOW_HTTP` (default `false`) and
  `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` (default `false`, path-style). Log
  commits use conditional puts (`If-None-Match`); `SSL_CERT_FILE` names a
  private CA.
- `FULL_VACUUM`: `1` for a full VACUUM (default `0`, lite).
- `VACUUM_RETENTION_HOURS`: default the table's
  `delta.deletedFileRetentionDuration` (7 days). A lite VACUUM may go lower,
  which only shortens how long readers of older snapshots find removed files
  (below the table's retention, each run deletes again the already deleted
  files of tombstones that are not expired yet, which is harmless); a full
  VACUUM refuses anything below 168.
- `OPTIMIZE_DATES`: `closed` (default) or `all`, which also compacts the open
  date (safe beside the writer, but repeated every run; for a lake whose
  writer has stopped).
- `OPTIMIZE_TARGET_SIZE`: bytes, default the table's `delta.targetFileSize`.
- `OPTIMIZE_ZSTD_LEVEL`: default 3, as fireparq writes.
- `OPTIMIZE_MAX_CONCURRENT_TASKS`: default the CPU count.
- `DRY_RUN`: `1` reports the plan and the files VACUUM would delete, and
  changes nothing.

Output: one JSON object per line on stdout (`start`, one `table` line per
table, `done`). Exit status: 0 when every table was maintained (a lost commit
race is reported as a conflict and left to the next run), 1 when a table
failed, 2 for a configuration error.
"""

import json
import os
import re
import sys
import time

REQUIRED_DELTALAKE = "1.6.6"
MIN_FULL_VACUUM_HOURS = 168
TABLE_NAME = re.compile(r"^[A-Za-z0-9_]+$")
INTERVAL = re.compile(r"^\s*interval\s+(\d+)\s+(second|minute|hour|day|week)s?\s*$", re.I)
UNIT_HOURS = {"second": 1 / 3600, "minute": 1 / 60, "hour": 1, "day": 24, "week": 168}


class ConfigError(Exception):
    pass


def emit(event, **fields):
    print(json.dumps({"event": event, **fields}, sort_keys=True, default=str), flush=True)


def flag(name, default="0"):
    value = os.environ.get(name, default).strip().lower()
    if value in ("1", "true", "yes"):
        return True
    if value in ("0", "false", "no", ""):
        return False
    raise ConfigError(f"{name} must be 0 or 1, got {value!r}")


def integer(name, minimum, maximum=None):
    value = os.environ.get(name, "").strip()
    if not value:
        return None
    try:
        number = int(value)
    except ValueError:
        raise ConfigError(f"{name} must be an integer, got {value!r}") from None
    if number < minimum or (maximum is not None and number > maximum):
        raise ConfigError(f"{name} must be in [{minimum}, {maximum or '...'}], got {number}")
    return number


class Settings:
    def __init__(self):
        root = os.environ.get("LAKE_ROOT", "").strip()
        bucket = os.environ.get("LAKE_BUCKET", "").strip()
        if bool(root) == bool(bucket):
            raise ConfigError("set exactly one of LAKE_ROOT and LAKE_BUCKET")
        self.root = (root or f"s3://{bucket}").rstrip("/")
        self.s3 = self.root.startswith("s3://")
        if "://" in self.root and not self.s3:
            raise ConfigError("LAKE_ROOT must be s3://bucket[/prefix] or a local path")
        self.tables = [t.strip() for t in os.environ.get("LAKE_TABLES", "").split(",") if t.strip()]
        if not self.tables:
            raise ConfigError("LAKE_TABLES must name the tables, for example blocks,transactions")
        for table in self.tables:
            if not TABLE_NAME.match(table):
                raise ConfigError(f"not a table name: {table!r}")
        self.full_vacuum = flag("FULL_VACUUM")
        self.dry_run = flag("DRY_RUN")
        self.retention_hours = integer("VACUUM_RETENTION_HOURS", 0)
        if self.full_vacuum and self.retention_hours is not None and (
            self.retention_hours < MIN_FULL_VACUUM_HOURS
        ):
            raise ConfigError(
                f"a full VACUUM deletes untracked files, including parts fireparq has not "
                f"committed yet, so it needs VACUUM_RETENTION_HOURS >= {MIN_FULL_VACUUM_HOURS}"
            )
        dates = os.environ.get("OPTIMIZE_DATES", "closed").strip().lower()
        if dates not in ("closed", "all"):
            raise ConfigError(f"OPTIMIZE_DATES must be closed or all, got {dates!r}")
        self.optimize_all_dates = dates == "all"
        self.target_size = integer("OPTIMIZE_TARGET_SIZE", 1)
        self.zstd_level = integer("OPTIMIZE_ZSTD_LEVEL", 1, 22) or 3
        self.max_concurrent_tasks = integer("OPTIMIZE_MAX_CONCURRENT_TASKS", 1)
        self.secrets = []
        self.storage = self.storage_options() if self.s3 else None

    def storage_options(self):
        if flag("AWS_S3_ALLOW_UNSAFE_RENAME"):
            raise ConfigError("AWS_S3_ALLOW_UNSAFE_RENAME is unsafe beside a running writer")
        key = os.environ.get("AWS_ACCESS_KEY_ID", "")
        secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        if not key or not secret:
            raise ConfigError("maintenance writes to S3: set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
        options = {
            "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
            "AWS_ACCESS_KEY_ID": key,
            "AWS_SECRET_ACCESS_KEY": secret,
            # Log commits with `If-None-Match: *`; no DynamoDB lock.
            "aws_conditional_put": "etag",
            "AWS_ALLOW_HTTP": "true" if flag("AWS_ALLOW_HTTP", "false") else "false",
            "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": (
                "true" if flag("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false") else "false"
            ),
        }
        self.secrets = [key, secret]
        token = os.environ.get("AWS_SESSION_TOKEN", "")
        if token:
            options["AWS_SESSION_TOKEN"] = token
            self.secrets.append(token)
        endpoint = os.environ.get("S3_ENDPOINT", "").strip()
        if endpoint:
            options["AWS_ENDPOINT_URL"] = endpoint
        return options

    def redact(self, text):
        for secret in self.secrets:
            text = text.replace(secret, "***")
        return text

    def uri(self, table):
        return f"{self.root}/{table}"


def retention_hours(configuration):
    """`delta.deletedFileRetentionDuration` in hours (default 7 days), or None."""
    value = configuration.get("delta.deletedFileRetentionDuration", "interval 7 days")
    match = INTERVAL.match(value)
    return None if match is None else int(match.group(1)) * UNIT_HOURS[match.group(2).lower()]


def active_files_per_date(dt):
    adds = dt.get_add_actions(flatten=True)
    if adds.num_rows == 0:
        return {}
    if "partition.date" not in adds.column_names:
        raise RuntimeError("the table is not partitioned by date")
    counts = {}
    for value in adds.column("partition.date").to_pylist():
        counts[str(value)] = counts.get(str(value), 0) + 1
    return counts


def maintain(settings, deltalake, name, open_date):
    """One table; returns its report. Never raises."""
    from deltalake.exceptions import CommitFailedError

    started = time.monotonic()
    report = {"table": name, "compacted": [], "conflicts": [], "errors": []}
    no_hooks = deltalake.PostCommitHookProperties(
        create_checkpoint=False, cleanup_expired_logs=False
    )

    def failed(step, error):
        text = settings.redact(f"{step}: {type(error).__name__}: {error}")
        (report["conflicts"] if isinstance(error, CommitFailedError) else report["errors"]).append(text)

    try:
        dt = deltalake.DeltaTable(settings.uri(name), storage_options=settings.storage)
        report["version_before"] = dt.version()
        files = active_files_per_date(dt)
    except Exception as error:  # noqa: BLE001: reported, the job goes on
        failed("open", error)
        report["seconds"] = round(time.monotonic() - started, 3)
        return report

    # 1. OPTIMIZE closed dates (or all, OPTIMIZE_DATES=all) with several files.
    dates = sorted(
        date
        for date, count in files.items()
        if count > 1 and (settings.optimize_all_dates or (open_date is not None and date < open_date))
    )
    report["dates_to_compact"] = dates
    properties = deltalake.WriterProperties(compression="ZSTD", compression_level=settings.zstd_level)
    for date in [] if settings.dry_run else dates:
        try:
            metrics = dt.optimize.compact(
                partition_filters=[("date", "=", date)],
                target_size=settings.target_size,
                max_concurrent_tasks=settings.max_concurrent_tasks,
                writer_properties=properties,
                post_commithook_properties=no_hooks,
            )
            report["compacted"].append(
                {
                    "date": date,
                    "files_removed": metrics.get("numFilesRemoved", 0),
                    "files_added": metrics.get("numFilesAdded", 0),
                }
            )
        except Exception as error:  # noqa: BLE001
            failed(f"optimize {date}", error)

    # 2. VACUUM, lite unless FULL_VACUUM=1 (weekly, >= 168 h enforced).
    table_hours = retention_hours(dt.metadata().configuration)
    hours = settings.retention_hours
    report["vacuum"] = {"mode": "full" if settings.full_vacuum else "lite", "retention_hours": hours}
    vacuumed = False
    if settings.full_vacuum and (table_hours is None or table_hours < MIN_FULL_VACUUM_HOURS):
        failed("vacuum", RuntimeError(
            f"refusing a full VACUUM: delta.deletedFileRetentionDuration is below {MIN_FULL_VACUUM_HOURS} h"
        ))
    else:
        try:
            # A full VACUUM always enforces the table's retention; a lite one
            # may go below it (it never deletes an untracked part).
            enforce = settings.full_vacuum or hours is None or (
                table_hours is not None and hours >= table_hours
            )
            deleted = dt.vacuum(
                retention_hours=hours,
                dry_run=settings.dry_run,
                enforce_retention_duration=enforce,
                full=settings.full_vacuum,
                post_commithook_properties=no_hooks,
            )
            report["vacuum"]["files_deleted"] = len(deleted)
            vacuumed = True
        except Exception as error:  # noqa: BLE001
            failed("vacuum", error)

    # 3. Checkpoint, only after a successful VACUUM (design §4.1).
    if vacuumed and not settings.dry_run:
        try:
            dt.create_checkpoint()
            report["checkpoint_version"] = dt.version()
            # 4. Log cleanup behind the checkpoint.
            dt.cleanup_metadata()
        except Exception as error:  # noqa: BLE001
            failed("checkpoint", error)
    report["version_after"] = dt.version()
    report["seconds"] = round(time.monotonic() - started, 3)
    return report


def open_date_of(settings, deltalake):
    """The newest `date` in `blocks` (still being written), or None."""
    dt = deltalake.DeltaTable(settings.uri("blocks"), storage_options=settings.storage)
    dates = [p["date"] for p in dt.partitions()]
    return max(dates) if dates else None


def main():
    started = time.monotonic()
    try:
        settings = Settings()
    except ConfigError as error:
        emit("config_error", error=str(error))
        return 2
    try:
        import deltalake
    except ImportError as error:
        emit("config_error", error=f"cannot import deltalake: {error}")
        return 2
    if deltalake.__version__ != REQUIRED_DELTALAKE:
        emit(
            "config_error",
            error=f"tested with deltalake=={REQUIRED_DELTALAKE}, found {deltalake.__version__}",
        )
        return 2
    emit(
        "start",
        root=settings.root,
        tables=len(settings.tables),
        full_vacuum=settings.full_vacuum,
        retention_hours=settings.retention_hours,
        optimize_dates="all" if settings.optimize_all_dates else "closed",
        dry_run=settings.dry_run,
        deltalake=deltalake.__version__,
    )
    failed, conflicts = [], 0
    try:
        open_date = open_date_of(settings, deltalake)
    except Exception as error:  # noqa: BLE001: without blocks no date is closed
        emit("blocks_error", error=settings.redact(f"{type(error).__name__}: {error}"))
        open_date = None
        failed.append("blocks")
    for name in settings.tables:
        report = maintain(settings, deltalake, name, open_date)
        report["open_date"] = open_date
        emit("table", **report)
        conflicts += len(report["conflicts"])
        if report["errors"] and name not in failed:
            failed.append(name)
    emit(
        "done",
        tables=len(settings.tables),
        failed=failed,
        conflicts=conflicts,
        seconds=round(time.monotonic() - started, 3),
    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
