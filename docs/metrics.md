# Prometheus metrics

Enable the metrics server with `--metrics-port <PORT>` (env: `METRICS_PORT`). A lightweight HTTP server binds to `0.0.0.0:<PORT>` serving three endpoints:

| Endpoint | Description |
|---|---|
| `/metrics` | Prometheus text exposition format |
| `/health` | `200 OK` while the pipeline is running, reconnecting or committing final output; `503` after it stops |
| `/ready` | `200 OK` after a valid stream message while connected and within the freshness threshold; otherwise `503` |

`--metrics-stale-after-secs` / `METRICS_STALE_AFTER_SECS` sets the readiness
threshold (default 120 seconds, or 129600 for `build --block-type sec`, a daily
feed; must be positive). It uses monotonic time since
the last valid message, so historical backfills can be ready even when block
timestamps are old. Disconnects and stream completion make readiness false.
Liveness stays true through final file publication and cursor persistence.
Readiness establishes recent stream activity; it does not prove forward block
progress, chain-head agreement or crash/replay safety.

## Available metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `firehose_parquet_blocks_processed_total` | Counter | — | Total blocks processed since start |
| `firehose_parquet_bytes_read_total` | Counter | — | Total protobuf bytes consumed from stream |
| `firehose_parquet_rows_written_total` | Counter | `table` | Rows written per table |
| `firehose_parquet_current_block_number` | Gauge | — | Most recently processed block number |
| `firehose_parquet_min_block_number` | Gauge | — | Minimum block number seen |
| `firehose_parquet_max_block_number` | Gauge | — | Maximum block number seen |
| `firehose_parquet_elapsed_seconds` | Gauge | — | Monotonic seconds since metrics initialization; refreshed on scrape |
| `firehose_parquet_last_block_timestamp_seconds` | Gauge | — | Last valid stream message's block timestamp; `NaN` when absent |
| `firehose_parquet_block_time_lag_seconds` | Gauge | — | Wall-clock age of that timestamp, clamped at zero; `NaN` when absent; refreshed on scrape |
| `firehose_parquet_last_message_age_seconds` | Gauge | — | Monotonic seconds since the last valid message; `NaN` before one; refreshed on scrape |
| `firehose_parquet_files_written_total` | Counter | `table` | Parquet files written |
| `firehose_parquet_file_bytes_total` | Counter | `table` | Total compressed bytes written |
| `firehose_parquet_flushes_total` | Counter | `trigger`, `pace` | Committed flushes by trigger (`bytes`, `memory`, `rows`, `blocks`, `interval`, `partition_boundary`, `stream_end`) and by the stream pace at the flush (`catching_up` or `caught_up`) |
| `firehose_parquet_catching_up` | Gauge | — | 1 while the stream replays history faster than real time, which suspends `--flush-interval-secs`; 0 at the head or when the pace is unknown ([details](cli.md#flush-interval-and-catch-up)) |
| `firehose_parquet_buffer_estimated_bytes` | Gauge | — | Writer-owned buffers, estimated compressed bytes |
| `firehose_parquet_buffer_rows` | Gauge | `table` | Writer-owned rows, including failed/unattempted tables |
| `firehose_parquet_mapper_buffer_rows` | Gauge | — | Mapper-owned rows summed across tables |
| `firehose_parquet_mapper_largest_table_estimated_bytes` | Gauge | — | Largest mapper table estimate, used to predict compressed file size |
| `firehose_parquet_mapper_buffer_estimated_bytes` | Gauge | — | Summed logical mapper estimates used by the memory trigger; not RSS |
| `firehose_parquet_bootstrap_buffered_blocks` | Gauge | — | Raw blocks awaiting the initial timestamp anchor |
| `firehose_parquet_bootstrap_buffered_bytes` | Gauge | — | Raw protobuf bytes awaiting that anchor |
| `firehose_parquet_cursor_saves_total` | Counter | — | Cursor persistence count |
| `firehose_parquet_cursor_save_failures_total` | Counter | — | Failed cursor mirror saves, once per failed attempt (including local retries, S3 reads/validation/owner checks before the PUT, and ambiguous or refused S3 publication) |
| `firehose_parquet_cursor_last_success_timestamp_seconds` | Gauge | — | Unix time of the last successful cursor save in this process; 0 before the first save |
| `firehose_parquet_cursor_last_block_num` | Gauge | — | Block number from the loaded cursor, then the last successful save; 0 when neither exists |
| `firehose_parquet_startup_list_requests` | Gauge | — | LIST requests (S3 pages of up to 1,000 keys, or local directory reads) made while opening the dataset; a resume lists no data objects ([startup cost](cursor-and-resume.md#startup-cost)) |
| `firehose_parquet_startup_listing_seconds` | Gauge | — | Seconds those startup listings took |
| `firehose_parquet_delta_log_tail_commits` | Gauge | `table` | Commits after the last checkpoint in the table's Delta log, which readers and the next start replay; it grows until the maintenance job checkpoints the table, so alert when it keeps growing (#643) |
| `firehose_parquet_delta_commit_seconds` | Histogram | `table` | Duration of each Delta commit of the table, from its request to a durable version |
| `firehose_parquet_delta_commit_retries_total` | Counter | `table` | Lost conditional puts the table's Delta commits retried at a later version (another writer, usually the maintenance job, committed first) |
| `firehose_parquet_errors_total` | Counter | `kind` | Errors by category |
| `firehose_parquet_grpc_reconnects_total` | Counter | — | gRPC retries scheduled, once per reconnect path |
| `firehose_parquet_blocks_skipped_below_start_total` | Counter | — | Blocks received below the effective start block and skipped |
| `firehose_parquet_info` | Info | *(pipeline config)* | Pipeline metadata (chain, endpoint, version) |

Use `rate(firehose_parquet_blocks_processed_total[5m])` and
`rate(firehose_parquet_bytes_read_total[5m])` for throughput. The old rate gauges
were cumulative averages and have been removed, along with the always-zero
Solana `backfill_*` gauges. Block-time lag measures age against the local clock;
it is not a measured remote chain-head lag. Buffer estimates are not process RSS.

Dashboard migration: counters previously registered with `_total` emitted
`_total_total`, because the Prometheus library adds the suffix. These now emit
exactly one `_total`; update queries that used the doubled names.
`cursor_save_failures_total` already had the correct name and is unchanged.
`files_written_total` now has only the `table` label; remove per-partition filters
and groupings. Existing series remain in your monitoring system until its normal
retention expires.

```bash
# Enable metrics on port 9090
fireparq build \
  --endpoint https://eth.firehose.pinax.network:443 \
  --metrics-port 9090 \
  --start-block 19000000

# Scrape metrics
curl http://localhost:9090/metrics
```
