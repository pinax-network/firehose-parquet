# S3 cursor bucket resolution (#470)

## Diagnosis

Two independent resolution paths could send a cursor to the wrong bucket:

- `s3::build_s3_client` preferred `Config.s3_bucket` over the bucket in the
  resolved output URI, while `ParquetTableWriter::new_s3` used the output URI.
  `S3_BUCKET=a --output s3://b/eth` therefore split cursor and data storage.
- `CursorLocation::resolve` discarded an explicit cursor URI's bucket and
  accepted a client already bound to the output bucket. `s3://x/c.parquet` and
  `s3://y/c.parquet` could overwrite the same object in the output bucket.

The ingestion wrapper also built an S3 client only for S3 output, preventing
an explicit S3 cursor from working with local data output.

## Resolution contract

1. An explicit S3 output URI is authoritative. A configured `--s3-bucket` or
   `S3_BUCKET` must match its bucket; a conflict is an early configuration error.
   The shared output resolver applies this before Firehose/storage calls in
   both `build` and `partitions build`.
2. An explicit S3 cursor URI selects its own bucket and full key, independently
   of the data output. No output/chain prefix is added to an explicit cursor URI.
3. Relative S3 cursor paths inherit the resolved data bucket and prefix,
   including the chain subdirectory and any cursor-template directories.
4. Explicit local output paths retain their existing local override behavior,
   even with `S3_BUCKET` set. Relative local cursors remain under that output
   root; absolute local cursors remain absolute. Local cursor resolution never
   constructs an S3 client.
5. Cursor and data buckets share the supplied AWS credentials, region and
   endpoint. Separate credentials/endpoints per bucket are outside this fix.

`CursorLocation::resolve` now receives a bucket-aware client factory instead of
an already-bound store. This makes the URI bucket part of the operation rather
than advisory metadata. The production ingestion and partition-index call
sites both construct the requested bucket's store. The writer and cursor also
share the pipeline S3 client builder, which now requires an explicit bucket.
Direct writer construction and ingestion cursor resolution defensively reject
an inconsistent output bucket in manually constructed `Config` values.

## Regression coverage

- Real AWS store construction is inspected for default, nested relative,
  independent `x`/`y` cursor buckets, and local output with an S3 cursor.
- In-memory stores round-trip different cursor states at the identical
  `c.parquet` key in buckets `x` and `y`, plus the sibling cursor in the data
  bucket. The data bucket never receives the explicit cursors' unprefixed key.
- CLI configuration rejects both flag-sourced and environment-sourced bucket
  conflicts. Matching buckets and explicit local overrides remain accepted.
- Direct writer construction verifies the output store's bucket and rejects
  conflicting defaults. Existing local cursor save/load and absolute-path tests
  remain in place.

## Validation

- `cargo fmt --all --check` and `git diff --check`: passed.
- `cargo test --workspace --locked -j4`: 699 passed, zero failed, three ignored
  (110 mapper tests, 183 binary tests, 403 library tests, three registry-generator
  tests). The existing unused-assignment warning for `transactions_processed`
  remains unrelated to this change.
- `cargo build --bin fireparq --locked -j4`: passed.
- Binary smoke checks for both `build` and `partitions build`, using a closed
  loopback endpoint and conflicting output/default buckets: exit status 1 with
  the bucket-mismatch diagnosis, before any connection attempt.

These checks ran on the branch based on main commit `7cd615c`, using the shared
audit target directory and development/test debug information disabled.
No live S3 or Firehose requests were performed; object isolation is verified by
separate in-memory stores plus inspection of real AWS clients' selected buckets.
