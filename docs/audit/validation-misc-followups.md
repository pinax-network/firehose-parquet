# Validation follow-ups: platform, partitions and EVM tests (#463)

A validation pass over the merged audit work found 17 remaining gaps. They are
fixed in three independent pull requests, one per group, so each can be reviewed
and merged on its own:

| Group | Findings | Record |
|---|---|---|
| A. Platform | 1-9 | this file |
| B. Partitions / validate | 10-15 | [`validation-misc-followups-partitions.md`](validation-misc-followups-partitions.md) |
| C. EVM tests | 16-17 | [`validation-misc-followups-evm.md`](validation-misc-followups-evm.md) |

Refs #471 #475 #483 #484 #485 #486 #491 #499 #515 #517 #519 #525 #527 #562 #567.

## A. Platform

### 1. Zero flush rows and interval (#471, #515)

`build --flush-rows 0` and `--flush-interval-secs 0` parsed, and
`next_mapper_flush_trigger` then evaluated `rows >= 0` / `elapsed >= 0`, so every
block produced one transaction and one file per table. Zero now disables both
triggers, the same meaning as `--flush-bytes 0` in `build`, `--flush-rows 0` in
`merge`, and zero for the idle/stall timeouts. The conversion filters zero in
`build_config` (so the resolved `Config` and its startup summary show the trigger
as absent), and the trigger itself also ignores a zero limit. `--flush-blocks`
and `--flush-memory-bytes` keep rejecting zero. Tests:
`test_flush_rows_and_interval_zero_mean_disabled` (parser and `Config`) and
`test_next_mapper_flush_trigger_zero_rows_and_interval_are_disabled` (trigger,
including an hour-old last flush and positive limits that still fire).

### 2. Invocations without a subcommand

The README Docker example (the image entrypoint is `fireparq`), the Prometheus
example and the three network-alias examples ran `fireparq --…` without `build`,
which the binary rejects. A scan of README, `docs/`, fixture READMEs, sources and
scripts for `fireparq` followed by a flag found no other current invocation; the
remaining hits are historical `docs/releases/v0.4.0.md` / `v0.5.0.md` notes,
which describe those releases' CLI and were left unchanged. The Docker example
also uses `PINAX_API_KEY` instead of the legacy fallback.

### 3. `.env.example`, flag table and merge help

`.env.example` is regenerated from the clap `env = "..."` definitions, grouped as
in `--help`, with defaults taken from the code (`FLUSH_BYTES=33554432`,
`FLUSH_MEMORY_BYTES=268435456`, `GRPC_WINDOW_BYTES=16777216`,
`GRPC_MAX_MESSAGE_BYTES=134217728`). It adds every missing variable, the
provider-scoped credentials and `FIREHOSE_ENDPOINT_*` overrides, and recovery's
separate `AWS_ENDPOINT_URL` binding. Obsolete `EXTENDED`, `BYTES_ENCODING` and
`CURSOR=cursor.txt` entries are gone. `test_env_example_matches_cli_environment_variables`
walks the whole `Cli::command()` tree and fails if a CLI variable is missing
from the file, if the file lists a variable nothing reads, or if the documented
defaults drift from the constants.

The README common-flags table now lists `--flush-memory-bytes`, `--flush-blocks`
and zero semantics, and corrects two stale entries in the same rows (`build` has
no `--live` flag; the chain toggles are `--without-extended` /
`--without-votes`). `merge --flush-bytes` help said "in-memory bytes" although
the trigger compares encoded bytes (`bytes_written + in_progress_size`); it now
says so, and the README merge table matches.

### 4. Dead code and always-zero log fields

Removed the unused public `cli::read_credential_env` (credential trimming lives
in `auth.rs`) and `cli::DEFAULT_TIMESTAMP_BACKFILL_BUFFER_LIMIT_BYTES`, and
rewrote the stale writer test comment that claimed `flush_bytes` controls
`OutputWriter` rollover (the argument is ignored and `write_all` publishes
immediately).

`commit_blocking` logged `WriterBufferStats::default()`. Protected transactions
publish all tables of a flush or none, so there is no retained writer buffer to
report. `log_writer_flush_outcome` now takes only the materialized flag and logs
no `buffered_*` fields; `WriterFlushOutcome` remains for the legacy test-only
helpers. The `runtime.rs` change is one call site.

### 5. gRPC defaults

`config::DEFAULT_GRPC_WINDOW_BYTES` and `config::DEFAULT_GRPC_MAX_MESSAGE_BYTES`
are the single definitions used by `GrpcArgs` (`default_value_t`) and
`GrpcConfig::default()`. The existing
`grpc_transport_flags_validate_limits_and_apply_to_both_build_commands`
comparison and the `.env.example` test cover both.

### 6. S3 client builders (#527)

The three functions named `build_s3_client` had different policies and are now
named for them:

| New name | Retries | Without an access key |
|---|---|---|
| `s3::build_ingestion_mutation_client(&Config, bucket)` | none | AWS provider chain |
| `AwsConfig::build_read_client(bucket)` | default read retries | anonymous (unsigned) |
| `AwsConfig::build_s3_client_for_mutation(bucket)` (unchanged) | none | anonymous (unsigned) |
| `rollup::build_mutation_store` (private) | wraps the mutation client | anonymous (unsigned) |

The policies differ on purpose (ingestion must not sign anonymously; maintenance
reads keep retries), so they were renamed rather than merged. The old public
names stay as `#[deprecated]`, `#[doc(hidden)]` aliases, so external callers and
concurrent lanes keep compiling. `ingest/session.rs` `aws_config()` no longer
copies the fields itself; it delegates to `AwsConfig::from(&Config)`. The
one-line wrapper stays because concurrent ingestion work adds new calls to it.
The rollup change is limited to the rename.

### 7. Explicit selectors and non-Pinax hosts (#562)

An explicit `--api-key-envvar` / `--api-token-envvar` (or `API_KEY_ENVVAR` /
`API_TOKEN_ENVVAR`) authorizes that variable for whatever host is resolved. A
global `API_KEY_ENVVAR=SUBSTREAMS_API_KEY` left over from before #562 therefore
still sends the Pinax key to StreamingFast or custom hosts. Selection is
unchanged (it is the documented way to authorize a custom host), but startup now
logs a `WARN` with the host, provider, selector and variable name (never the
value) whenever an explicitly selected credential is sent to a non-Pinax host.
Pinax and legacy `SUBSTREAMS_*` names get a stronger message that recommends
unsetting the selector; a `STREAMINGFAST_*` variable sent to a built-in
StreamingFast host is its normal destination and is not warned about. The README
Authentication section and `.env.example` describe the hazard. Tests:
`explicit_credentials_leaving_pinax_are_classified_for_warning` (classification
matrix) and `explicit_legacy_selector_warns_when_sent_to_streamingfast`, which
captures the real log in an isolated child process (the same pattern the #562
log test needed for tracing's callsite cache) and checks that no secret value is
logged and that Pinax and ambient selections stay quiet. The auth tests passed
ten repeated runs.

### 8. Dependency advisory gate (#567)

CI has a new `advisories` job. It downloads the cargo-deny 0.20.2
`x86_64-unknown-linux-musl` release, verifies it against the release asset's
SHA-256, and runs `cargo deny --locked check advisories` with the new `deny.toml`
(advisories only). Every RustSec vulnerability fails the job; unsound and
unmaintained advisories fail for direct dependencies.

On the unmodified tree the check reported seven errors. Four were fixed with
compatible lockfile updates: anyhow 1.0.102 to 1.0.104 (RUSTSEC-2026-0190,
unsound `downcast_mut`), h2 0.4.13 to 0.4.19 (RUSTSEC-2026-0258), rustls 0.23.37
to 0.23.45 (RUSTSEC-2026-0285) with rustls-webpki 0.103.15. The rest are ignored
with a reason in `deny.toml`:

- RUSTSEC-2026-0194 and RUSTSEC-2026-0195 (quick-xml 0.38): only object_store
  0.12 (`quick-xml ^0.38`) uses it, to parse responses from the configured S3
  endpoint, so exploitation needs a malicious or compromised S3 server. The fix
  (quick-xml 0.41) needs object_store 0.14, a breaking upgrade left for a
  separate change.
- RUSTSEC-2025-0012 (backoff) and RUSTSEC-2025-0141 (bincode 1.3): unmaintained
  direct dependencies with no drop-in release; replacing them is separate work.

Locally, the same cargo-deny release (aarch64-apple-darwin, checksum verified)
exits 0 with `advisories ok`, and exits 1 when one ignore entry is removed.

### 9. Maintenance outputs carry #519 properties

`firehose-parquet/tests/maintenance_output_properties.rs` writes inputs with
Parquet defaults (no Bloom filters, no compression, 1M-row groups) and runs the
real `run_merge` and `run_rollup`. It checks the outputs for: two row groups of
at most 65,536 rows, the zstd codec on every column chunk (the footer records the
codec, not the level), Bloom filters on `tx_hash` and `address` with no false
negatives for any non-null value, no filters on `block_num`/`data`, no sorting
declaration on streaming output, preserved `firehose-parquet.*` metadata without
`fireparq.ingest.*` receipt keys, and unchanged rows. Merge and rollup code is
untouched.

## Validation (group A)

- `cargo fmt --all`; `cargo test --workspace --locked --no-fail-fast`: 1,096
  passed, 0 failed, 14 ignored.
- `cargo clippy --workspace --all-targets --locked`: no warnings in the changed
  code (the tree has unrelated pre-existing warnings).
- `cargo deny --locked check advisories`: `advisories ok`.
