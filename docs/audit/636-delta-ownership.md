# Ownership beside Delta maintenance (#636)

Closes #636; refs #643 (lane L4), #468, #591; part of #463. PR: pending.
Design: [`docs/design/delta-lake.md`](../design/delta-lake.md) §5. The
recovery work is in [the L4 record](643-l4-delta-recovery.md).

## Problem

Since #591 and #468, an S3 writing command holds one persistent owner record
for the whole bucket. With one bucket per network, a live `build` therefore
blocked every other writing command on it, including the nightly compaction
of finished days (`merge`, `rollup`), so compaction could not run beside
ingestion.

## Resolution

The Delta output (#643) moves compaction and cleanup out of fireparq: an
off-the-shelf `deltalake` job (OPTIMIZE, VACUUM, checkpoints, log cleanup)
commits to the tables through their logs. `merge` and `rollup` are removed.
The owner record keeps its protocol and scope, but what it guards is now
narrower, and the maintenance job needs no fireparq command at all.

### What the owner guards

The owner is the S3 record `.fireparq-owner-v1.json` at the bucket root, or
the local inode lock. Only `build` and `recovery recover` take it. It guards:

- one fireparq ingestion writer, or its offline recovery, per dataset (and,
  on S3, per bucket);
- the state that writer alone mutates: `.fireparq-ingest/` (the authority and
  the pending journal), `_fireparq/cursor.parquet`, and its own uncommitted
  parts, which only a Writing rollback deletes, by exact journal path.

The protocol is unchanged: persistent and conditional, no expiry or takeover,
and the uncertainty latch for part PUTs, control records, the mirror and
deletions. A Delta log commit with an unknown outcome no longer sets the
latch, because the next start resolves it from the table's `txn`
([review](643-l4-delta-recovery.md#the-owner-latch-and-log-commits)).

### What it does not guard

The Delta tables. They are shared through their logs: any Delta writer, in
practice the maintenance job, commits with conditional puts and never takes
the fireparq owner. That is safe because:

1. fireparq only appends: blind appends with a `txn`, never a `remove`, a
   rewrite or a delete of a committed file, and `delta.appendOnly` makes the
   log refuse data removal;
2. maintenance and fireparq's appends rebase on each other instead of
   conflicting: OPTIMIZE removes files with `dataChange: false`, and a lost
   conditional put is only a retry at the next version;
3. fireparq never relies on a committed part: recovery skips every table whose
   `txn` holds the pending transaction, reads no part once authority holds it,
   and reads only the uncommitted parts of the others, which a lite VACUUM
   never deletes; startup lists no data;
4. stream exclusivity stays fireparq's job: the owner keeps a second fireparq
   writer out, and the same-`appId` conflict is a second line of defense.

Maintenance that could still break fireparq is the one the deployed job never
runs: a full VACUUM with a retention shorter than an outage, which can delete
a pending transaction's uncommitted parts. Recovery then fails closed with
the journal kept (design §4.1).

## Bucket policy (RGW)

Two RGW users per network bucket, following design §5. `ethereum-mainnet`,
`lake-writer` and `lake-maintenance` are placeholders; list every table of the
chain (the EVM tables are `blocks`, `transactions`, `logs`, ...).

- **The maintenance user** needs List on the bucket and Get, Put and Delete on
  each table prefix (the data files and `_delta_log/`: OPTIMIZE writes files
  and a commit, VACUUM deletes files, checkpoints overwrite
  `_delta_log/_last_checkpoint`, log cleanup deletes old commits). It needs
  nothing outside the tables, and is denied fireparq's state.
- **The writer** (`build`, `recovery`) needs Get, Put, Delete and List on the
  bucket (its parts, `.fireparq-ingest/`, `_fireparq/`, the owner record and
  its probe prefix `.fireparq-owner-probes-v1/`), but never deletes a Delta
  log object: Writing rollback deletes only data parts, and fireparq writes
  no checkpoint and cleans no log.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "MaintenanceLists",
      "Effect": "Allow",
      "Principal": {"AWS": ["arn:aws:iam:::user/lake-maintenance"]},
      "Action": ["s3:ListBucket"],
      "Resource": ["arn:aws:s3:::ethereum-mainnet"]
    },
    {
      "Sid": "MaintenanceTables",
      "Effect": "Allow",
      "Principal": {"AWS": ["arn:aws:iam:::user/lake-maintenance"]},
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": [
        "arn:aws:s3:::ethereum-mainnet/blocks/*",
        "arn:aws:s3:::ethereum-mainnet/transactions/*",
        "arn:aws:s3:::ethereum-mainnet/logs/*"
      ]
    },
    {
      "Sid": "MaintenanceNeverTouchesWriterState",
      "Effect": "Deny",
      "Principal": {"AWS": ["arn:aws:iam:::user/lake-maintenance"]},
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": [
        "arn:aws:s3:::ethereum-mainnet/.fireparq-ingest/*",
        "arn:aws:s3:::ethereum-mainnet/_fireparq/*",
        "arn:aws:s3:::ethereum-mainnet/.fireparq-owner-v1.json",
        "arn:aws:s3:::ethereum-mainnet/.fireparq-owner-probes-v1/*"
      ]
    },
    {
      "Sid": "Writer",
      "Effect": "Allow",
      "Principal": {"AWS": ["arn:aws:iam:::user/lake-writer"]},
      "Action": ["s3:ListBucket", "s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": ["arn:aws:s3:::ethereum-mainnet", "arn:aws:s3:::ethereum-mainnet/*"]
    },
    {
      "Sid": "WriterNeverDeletesDeltaLogs",
      "Effect": "Deny",
      "Principal": {"AWS": ["arn:aws:iam:::user/lake-writer"]},
      "Action": ["s3:DeleteObject"],
      "Resource": ["arn:aws:s3:::ethereum-mainnet/*/_delta_log/*"]
    }
  ]
}
```

Notes:

- Conditional creates (`If-None-Match: *`) and conditional replaces
  (`If-Match`) are `s3:PutObject`. Both users need an RGW that honors them;
  fireparq's owner acquisition probes that before it writes anything.
- A public-read bucket keeps its anonymous Get/List statement beside these.
- The policy is a template: it was not applied to a real RGW here (the tests
  use a loopback endpoint without policies). Check the wildcard in
  `*/_delta_log/*` against the RGW version in use, or list one Deny
  resource per table.

## Acceptance criteria

- [x] **A design reviewed against #468/#591** (uncertainty latch,
  single-attempt mutations, quiescence): design §5 and §3.5, and the
  [latch review](643-l4-delta-recovery.md#the-owner-latch-and-log-commits).
  Mutations stay single-attempt (the Delta log client too); the latch keeps
  covering every request whose late effect recovery cannot resolve.
- [x] **Compaction of closed partitions can run while a live build owns the
  same dataset**: the `deltalake` job compacts and vacuums beside `build`
  without the fireparq owner, even on the open date being appended to, which
  the deployed job skips (design §9).
- [x] **Tests with the in-memory/loopback S3 providers**:
  `blocks/tests/delta_recovery.rs::on_s3::maintenance_beside_build` (and
  `on_local::`) runs `deltalake` OPTIMIZE, lite VACUUM of retention 0 and
  checkpoints in a loop beside the real binary while it catches up, across a
  restart, then reads each block exactly once per table through the logs and
  with Polars; the crash tests of the same file run maintenance between a
  crash and the restart; the library tests use the in-memory store and the
  loopback S3 of `delta/commit/tests/loopback_s3.rs`.

## Limits

- On S3 the owner is still one per bucket, for fireparq's own commands. A
  bucket per network gives each writer its own; networks sharing a bucket
  still run their `build`s one at a time.
- A `build` and `recovery recover` of the same dataset still exclude each
  other, as before.
