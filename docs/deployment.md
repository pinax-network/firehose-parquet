# Recommended deployment

The recommended deployment is a **final-only lake**: one bucket per network,
with the dataset at the bucket root, and one continuous final-only `build` per
network beside the hourly maintenance job. There is no live bucket: non-final
output is a [CLI capability](non-final-streams.md), not part of this
deployment.

```bash
# One continuous writer per network, at its bucket root, following finalized
# blocks. The first run starts at the endpoint's first streamable block (or
# pass --start-block); every later run resumes from the output authority.
OUTPUT=s3://ethereum-mainnet \
FLUSH_INTERVAL_SECS=60 \
FLUSH_BYTES=33554432 \
FLUSH_MEMORY_BYTES=268435456 \
METRICS_PORT=9102 \
fireparq build --network mainnet
```

| Setting | Recommended | Why |
|---|---|---|
| `--output` / `OUTPUT` | `s3://<network bucket>` | The bucket root is the dataset root, and one bucket per network gives each writer its own bucket-wide owner ([single-network buckets](output-layout.md#single-network-buckets)) |
| `--final-blocks-only` | `true` (the default) | Only finalized blocks, so readers need no reorg rule |
| `--stop-block` | unset | One continuous writer; a restart resumes from authority ([startup cost](cursor-and-resume.md#startup-cost)) |
| `--flush-interval-secs` | `60` (60–120) | The commit cadence at the chain head. It is suspended while the writer catches up, so a backfill or a restart after an outage writes full-size files ([details](cli.md#flush-interval-and-catch-up)) |
| `--flush-bytes` | 32 MiB (the default) | The compressed size target of the largest table's file while catching up |
| `--flush-memory-bytes` | 256 MiB (the default); 1 GiB on fast chains | The summed mapper estimate that forces a flush. On Robinhood (10 blocks/s) the default fires about every 27 s, before a 60 s interval. 1 GiB makes catch-up 2.5–3.5× faster and writes 4× fewer objects per block, at about 2.8 GiB peak RSS instead of 1.0 GiB |
| Memory limit | Headroom above the peak RSS | For example 2 GiB with the default memory trigger and 4 GiB with 1 GiB, plus temporary disk for the S3 upload spool ([S3-aware cursor](cursor-and-resume.md#s3-aware-cursor)) |
| Maintenance | Hourly (`17 * * * *`) and a weekly full VACUUM | [`deploy/examples/delta-maintenance-cronjob.yaml`](../deploy/examples/delta-maintenance-cronjob.yaml), with its own S3 user ([Delta maintenance](delta-maintenance.md)) |
| Alerts | `firehose_parquet_delta_log_tail_commits`, a writer down for more than a day, `/ready` | The maintenance job stopped checkpointing; a committed transaction must not stay pending past the VACUUM retention; the stream stalled ([metrics](metrics.md)) |

These numbers come from the #658 benchmark on Robinhood and Arbitrum One blocks
(local disk and a loopback S3 at 0–80 ms per request). It was measured before
the Delta commits, which add about one PUT and one LIST per table with rows to
each flush. At a 60–120 s cadence a commit takes 1–21% of the window, and a
catch-up runs at 4.8× the chain rate or more. See the
[benchmark record](audit/658-live-flush-benchmark.md) and the
[adaptive flush record](audit/659-adaptive-flush.md).

Some block families change defaults that this table leaves unset. A SEC
writer (`--block-type sec`) gets a 512 MiB gRPC message limit, an idle flush,
and readiness and reconnect timeouts sized for one burst per EDGAR feed day,
and needs at least 3 GiB of memory; leave `GRPC_MAX_MESSAGE_BYTES`,
`FLUSH_IDLE_SECS`, `STREAM_IDLE_TIMEOUT_SECS` and `METRICS_STALE_AFTER_SECS`
unset there ([SEC notes](chains/sec.md), [family defaults](cli.md#family-defaults)).

On Kubernetes, give the writer an `emptyDir` at `/tmp` when its root
filesystem is read-only (the S3 upload spool), point readiness at `/ready` and
liveness at `/health` on `METRICS_PORT`, and run one replica: a second writer on
the bucket fails with `bucket ownership is held`.
