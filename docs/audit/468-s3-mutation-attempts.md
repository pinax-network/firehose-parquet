# Single-attempt S3 mutations for #468

## Problem and scope

A timed-out or lost-response S3 PUT can still complete remotely. Retrying an
unconditional cursor overwrite, data PUT or DELETE may return success while
an earlier request remains in flight. A later success cannot establish that
those earlier requests have drained. Releasing ownership or advancing to a
new checkpoint after that apparent recovery could let an older request replace
new state or recreate a removed part.

This prerequisite disables automatic transport retries for mutation clients
and application retries for S3 cursor saves. Local cursor retries remain
bounded at three attempts. It does not add a transaction journal or protected
ingestion mode, and #468 remains outstanding. Common ownership, maintenance
call-site migration and retention of uncertain guards are integrated separately.

## Implementation and compatibility

- `AwsConfig::build_s3_client_for_mutation(&self, bucket)` is a new public
  constructor returning the same `AmazonS3` type as the existing builder.
  It uses `object_store::RetryConfig { max_retries: 0, ..Default::default() }`.
- `AwsConfig::build_s3_client` keeps its existing retry policy for read-only
  commands. Credential selection, anonymous reads and bucket/endpoint checks
  are preserved by a shared builder.
- Existing `s3::build_s3_client(&Config, bucket)` now applies the single-attempt
  transport policy. Production callers use it for data writers and cursors.
  Reads made through that same client also get one transport attempt; callers
  needing read retries can use the separate read builder.
- `CursorLocation::save_with_retry` and its blocking bridge retain their public
  signatures. Local saves still run on a blocking worker with three attempts
  and 1 s/2 s backoff; S3 saves make one application attempt. The shutdown
  polling and local atomic persistence behavior are preserved.
- Callers supplying their own `Arc<dyn ObjectStore>` for an S3 cursor must use a
  mutation client with transport retries disabled. The trait object cannot
  expose or verify a provider's internal retry configuration. The same
  requirement applies to direct `CursorLocation::save` callers.
- One failed S3 attempt increments `cursor_save_failures_total` and the
  `cursor_save` error counter once. It leaves the successful-save counter,
  saved block gauge and last-success time unchanged. The remote object may
  nevertheless contain the attempted checkpoint; metrics describe the
  acknowledged save, not a fabricated rollback.

The pinned `object_store` 0.12.5 source documents `max_retries = 0` as disabling
retries (`src/client/retry.rs`). This change does not change dependency versions,
credential handling, endpoint security, Parquet schemas or object names.
It intentionally trades transparent S3 retry availability for a visible
failure when a mutation outcome is uncertain. Custom stores and external tools
must independently honor the same contract.

CAS owner/control operations have a different reconciliation protocol: exact
record/version reads can resolve their intended transition. That protocol must
not be reused to infer that unrelated, unconditional data requests have stopped.
The ownership and provider-quiescence requirements in
`468-s3-ownership.md` still apply after a single-attempt failure.

## Validation

Hermetic tests run the real `AmazonS3` adapter against a loopback HTTP server.
They use the production builder paths, changing only HTTPS enforcement and
the request timeout for the local test server. Synthetic credentials remain
inside the test client; only request methods and synthetic bodies are retained.

Both mutation builders are tested with PUT and DELETE after the server accepts
the request and then closes the connection, delays acknowledgement past the
client timeout, or returns HTTP 500. The server would return success to any
accidental retry; every mutation must instead fail after exactly one request.

The actual `CursorLocation::save_with_retry` path is tested against the same
three server failures using both mutation builders. Each case verifies one
PUT, a readable accepted Parquet cursor, an error returned to ingestion, one
failure count and unchanged previous-success gauges. The timeout test joins
the known server task before cleanup; merely waiting a duration is not treated
as a production provider-quiescence assertion.

A separate GET test demonstrates that the read builder still retries a lost
response. Existing tests retain local transient recovery, three-attempt
exhaustion, shutdown responsiveness and successful current-thread S3 saving.
Bucket signing and mismatched bucket-bound endpoint tests include the new
public mutation builder.

The initial focused S3 suite passed 21 tests. On main `3cf984b` plus the S3
ownership primitive and this change, the final workspace suite passed **797
tests**, with four intentionally ignored tests (three benchmarks and the
atomic-publication subprocess helper). Formatting and diff checks passed.
No production S3 bucket was contacted.

Commands use the whole-process Cargo lock and shared Arrow 60 build target:

```sh
python3 /tmp/fireparq-cargo-locked.py cargo test -p firehose-parquet s3:: --locked -j4
python3 /tmp/fireparq-cargo-locked.py cargo test --workspace --locked -j4
python3 /tmp/fireparq-cargo-locked.py cargo fmt --all -- --check
git diff --check
```

Logs: `/tmp/fireparq-s3-mutation-focused.log` and
`/tmp/fireparq-s3-mutation-workspace.log`.
