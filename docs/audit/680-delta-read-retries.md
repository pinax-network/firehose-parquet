# #680: Delta log read retries, and a quiet first start

Issue: [#680](https://github.com/pinax-network/firehose-parquet/issues/680).
PR: -.
Released in [v1.0.2](../releases/v1.0.2.md). Design:
[`docs/design/delta-lake.md`](../design/delta-lake.md) §1.3, §3.3, §3.5 and
the L3 row of §11.

## Symptoms

Found while qualifying v1.0.1 on Ceph RGW 19.2.3 (riv-dev1, a disposable
bucket, 2026-09-28):

1. One `GET <table>/_delta_log/_last_checkpoint` failed after 14 ms with
   `HTTP error: error sending request`, a connection-level error, and `build`
   exited 1. The owner record was released and the next start resumed, so it
   was safe, but a continuous writer (a Kubernetes Deployment) paid a restart.
2. A new dataset logged `ERROR error=Generic delta kernel error: No files in
   log segment` about 20 times while its tables were created.
3. The `deltalake` maintenance script reported a table the writer had not
   created yet as failed. That fix is not in this change: the script is being
   replaced by a Rust maintenance job in a separate lane, which takes it over.

## Diagnosis

### A single attempt for every log request

The Delta log client (`delta::store::s3_log_client`, #643 L3) is an
object_store 0.13 `AmazonS3` with `RetryConfig { max_retries: 0 }`. That is
required for commits. object_store's retry loop (`client/retry.rs` in
0.13.2) resends any request on a 5xx, 408 or 429 and on a connect or send
error, and a timed-out one when the method is idempotent. A conditional commit
(`PutMode::Create`, `If-None-Match: *`) whose first copy landed but whose
answer was lost would come back as a 412, which delta-rs reads as a lost race
and commits again at the next version. With one attempt the outcome surfaces
as an error and the table's `txn` resolves it (design §3.5, #675). But
`max_retries` is one setting for the whole client, so every read got one
attempt too.

Reproduced on the loopback HTTPS S3 endpoint, with the connection of the
first `_last_checkpoint` GET dropped and v1.0.1's client:

```
ERROR error=Error interacting with object store: Generic S3 error: Error performing GET
https://127.0.0.1:56819/delta-lake/delta-test-chain/blocks/_delta_log/_last_checkpoint
in 4.87125ms - HTTP error: error sending request
```

and `build` exited 1, as on RGW.

### "No files in log segment"

The line is not fireparq's. `DeltaTables::open` opened each table before
creating it, and on S3 a table with no log is found absent only by opening
it. delta-rs's kernel (`buoyant_kernel` 0.28.1) builds the snapshot in
`SnapshotBuilder::build`, which carries `#[instrument(name = "snap.build",
…, err)]`. An `Err` return from an instrumented function emits an event at
ERROR. For a log with no files, `validate_end_version`
(`log_segment/mod.rs`) returns `Error::generic("No files in log segment")`,
which `deltalake-core`'s `Snapshot::try_new_with_engine` maps to
`DeltaTableError::NotATable`, the "absent" answer fireparq expected. The
ERROR event had already been written. `with_quiet_delta_logs` keeps the
delta-rs crates at `warn`, so ERROR events still show. That makes one line per
table on a new S3 dataset (the EVM stream has 20). A local dataset had none,
because `lacks_local_log` found the missing `_delta_log/` directory before
any open.

## Fix

### Retries below object_store, for `GET` and `HEAD` only

`delta/store/read_retry.rs` adds `ReadRetryConnector`, the log client's
`HttpConnector`: object_store's own `ReqwestConnector`, with `ReadRetry` (an
`HttpService`) wrapped around each client it builds. `s3_log_client` now
builds from `s3_log_builder`, which is `s3_builder` plus this connector.
object_store's `max_retries` stays 0, so object_store itself still retries
nothing.

`ReadRetry::call`:

- A request whose method is not `GET` or `HEAD` goes to the inner client
  once, and its result is returned as it is. That covers `PUT` (the log
  commit and table creation), `POST` and `DELETE`.
- A `GET` or `HEAD` is sent again when it gets no response (any
  `HttpError`: connect, send, reset, timeout, interrupted) or a 408, 429 or
  5xx. There are at most `READ_ATTEMPTS` (3) attempts. Every other answer,
  including 404, 304, 412 and 416, is returned at once, and after the last
  attempt its result is returned unchanged, so object_store builds the same
  error as before.
- Before each retry it waits an exponential backoff (200 ms, doubling, capped
  at 2 s) whose upper half is randomized, so 100–200 ms and then 200–400 ms.
  It logs one warning per retry, with the method, the path and query, the
  attempt, the backoff and the cause chain:

  ```
  WARN retrying an idempotent Delta log read after a transient error
  method=GET path="/delta-lake/delta-test-chain/blocks/_delta_log/_last_checkpoint"
  attempt=1 attempts=3 backoff_ms=178 error=HTTP error: error sending request:
  client error (SendRequest): connection error: peer closed connection without
  sending TLS close_notify: …
  ```

That covers every read of the log store: object and range reads of commits
and checkpoints, `_last_checkpoint`, ListObjectsV2 pages (a `GET` on the
bucket), `HEAD`, and every read delta-rs's kernel makes while loading a
snapshot, since all of them go through the one `AmazonS3` client. The
connector also serves the client's credential providers. Their `GET`s
(instance metadata, container credentials) are idempotent too. Their `PUT`
(the IMDSv2 token) and `POST` (STS web identity) go out once, as before.

#### Why this layer

- **object_store's `RetryConfig`** can't be limited to reads. It is one
  setting per client, and its loop resends `PUT`s on a 5xx.
- **Two clients** (a retrying one for reads, the single-attempt one for
  writes, behind a routing `ObjectStore`) would work. But object_store logs
  its retries at `info`, not `warn`, and a routing store must list each
  `ObjectStore` method as a read or a write and follow the trait as it
  changes.
- **An `ObjectStore` wrapper** that retries `get_opts`, `get_ranges`, `head`
  and `list*` can't tell a 503 or a 429 from a 400. object_store reports them
  all as `Error::Generic` around its crate-private `RetryError`, so the
  wrapper would have to parse messages or retry every generic error.
- **The HTTP layer** sees the method and the status of each request, so the
  rule is exact: `GET` and `HEAD` are safe methods (RFC 9110 §9.2.1), and
  nothing else is retried. object_store builds the request once and signs
  it, and a retry sends a clone of it, as object_store's own loop does.

### Tables are created without opening them first

`open_or_create` (`delta/commit.rs`) now asks `lacks_log`:

- a local table with no `_delta_log/` directory has no log (unchanged);
- while tables may be created (`create_missing`, which the session sets
  while authority is at ordinal 0), a table has no log when delta-rs's
  `LogStore::is_delta_table_location` finds no log file it recognizes (a
  commit, a checkpoint or a checksum) in its `_delta_log/`. `CreateBuilder`
  makes the same check before it commits version 0. That costs one LIST per
  table, on a new dataset or a start interrupted during creation;
- otherwise (a resume or `recovery recover`) the table is opened directly, as
  before.

A table without a log is created straight away. A table whose log exists is
opened and validated. Any failure to open it, including a missing table on a
resume (refused, as before), still logs and fails. Nothing is filtered in
`init_tracing`: the call that caused the event is no longer made.

## Safety properties kept

- **A log commit is still one request.** `DefaultLogStore` commits with one
  `put_opts(PutMode::Create)`, which object_store sends as one `PUT` with
  `If-None-Match: *` and `max_retries: 0`, and `ReadRetry` passes every
  `PUT` through once. A commit whose outcome is unknown still fails the run
  with `a Delta commit's outcome is unknown (it may have landed)`, still
  leaves the owner releasable (#675) and is still resolved by the next start
  from `txn`. Evidence:
  - `read_retry::tests::writes_are_sent_once_whatever_their_outcome`: `PUT`,
    `POST` and `DELETE` of a commit key, answered 503, 500, 429 or 408 or
    failing with a connect, send, interrupted or timeout error, reach the
    server exactly once.
  - `delta_tables::s3_log_commits_are_sent_once_and_resolve_from_the_txn`
    (the real binary): a `blocks` commit answered 503 without being applied,
    and a later one applied whose connection then drops, are each one `PUT`
    on the server, with no commit at the next version. The run fails with the
    unknown-outcome error. The next start, without `recovery release`,
    commits the first one again (the log lacked it) and leaves the second one
    alone (its `txn` holds it), and every table then has exactly one commit
    per transaction.
  - `ingest::session::tests::delta::an_unanswered_delta_commit_releases_ownership_and_the_next_start_reads_its_txn`
    and the other S3 tests of the library now build the log client through
    `s3_log_builder`, so they run with the connector.
- **The creation race is unchanged.** `create_table` still commits version 0
  with `SaveMode::ErrorIfExists` and no retries. A creator that loses the
  race still opens and validates the winner's table.
- **fireparq's own S3 clients are unchanged**: the owner record, the
  control state, the cursor mirror and the parts (`dataset_lock_s3.rs`,
  `durable_state_s3.rs`, `s3/upload.rs`). So is `validate`'s read-only
  client, which already uses object_store's default retries.

## Tests

| Test | Checks |
|---|---|
| `delta::store::read_retry::tests::idempotent_reads_retry_transient_failures` | `GET` after a 503, a send, interrupted, timeout or connect error, or a connect error then a 500, and `HEAD` after a 429, succeed on a later attempt; each attempt reaches the server |
| `…::a_read_stops_after_its_attempts_and_returns_the_last_answer` | 3 attempts at most; the last 5xx, or the last transport error, is returned |
| `…::definite_read_answers_are_not_retried` | 200, 206, 304, 400, 403, 404, 412 and 416 are sent once |
| `…::writes_are_sent_once_whatever_their_outcome` | above |
| `…::backoff_grows_is_capped_and_jittered` | the backoff bounds of the first, second and a late retry, and its randomness |
| `…::only_get_and_head_are_idempotent_reads` | the method rule |
| `blocks/tests/delta_tables.rs` `s3_log_reads_retry_transient_failures` | The real binary on the loopback HTTPS S3 endpoint. First start: the first `blocks/_delta_log/_last_checkpoint` GET loses its connection and the first listing of `transactions/_delta_log/` answers 503. Restart: the first listing of `blocks/_delta_log/` loses its connection, a `logs` commit read answers 500 and then 502, and the checkpoint hint answers 429. Both runs succeed with one warning per retry (2, then 4) and no ERROR line, each faulted read is answered by a later attempt, the logs hold exactly one commit per transaction, and no log key was PUT twice. Without the connector the first run fails with v1.0.1's error. |
| `… s3_log_commits_are_sent_once_and_resolve_from_the_txn` | above |
| `… a_clean_first_start_logs_no_error_line` | A new dataset on local disk and on the loopback S3 endpoint: `build` succeeds, logs no line at ERROR and never `No files in log segment`, and every table is created. Before the fix, the S3 run logged the kernel's ERROR event 20 times. |

The loopback endpoint (`blocks/examples/bench_live_flush/s3.rs`) gained
`Server::inject` (a key and a method) and `Server::inject_list` (a listing
prefix), each for a number of requests, with a `Fault`: a status without
applying the request, a dropped connection without applying it, or a
request applied whose connection then drops. Each log entry records the fault
it met and a listing's prefix.

## Validation

- `cargo fmt --all` and `cargo test --workspace --locked` pass with
  `FIREPARQ_REQUIRE_DUCKDB=1` (DuckDB 1.5.5) and `FIREPARQ_REQUIRE_POLARS=1`
  (Polars and `deltalake` from `blocks/tests/engines/requirements.txt`).
  Counts are in the PR.
- The three real-binary tests were run against the old behavior (the
  connector removed, and `lacks_log` reduced to its local check): the read
  test and the first-start test fail, and the commit test passes both ways,
  since writes were already sent once and must stay so.

## Limits

- A read whose body breaks after its headers arrived is not retried: its
  response was already handed to object_store. delta-rs's log reads are small
  objects, and the observed failure was before any response.
- A read that times out can take up to 3 × 60 s (the request timeout) plus
  the backoffs before it fails, against 60 s before.
- Retries are not counted in a metric. The warnings are the record.
