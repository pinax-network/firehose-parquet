# Explicit S3 write destinations and working-directory env files (#617)

Issue: <https://github.com/pinax-network/firehose-parquet/issues/617> (audit
finding N6, part of #463).

## Incident and diagnosis

`fireparq` loaded `.env` with `dotenvy::dotenv()`, which searches the current
directory **and every parent directory**. `build` and `partitions build`
resolved a relative `--output` (and the default `.`) to `s3://$S3_BUCKET/<path>`
whenever `--s3-bucket` / `S3_BUCKET` was set. During the audit a test run from a
git worktree under the repository checkout picked up the checkout's production
`.env` (`S3_BUCKET`, AWS keys and endpoint). Its relative output was written to
the production bucket, including a bucket-root ownership record. The only sign
was a log line.

Implementation found a third path with the same root cause: `partitions build`
read its existing index through the read-only shorthand. With `--output ./out`
and `S3_BUCKET` set, a first run whose local index did not exist yet tried to
resume from `s3://$S3_BUCKET/out/<chain>/partitions.parquet` (an anonymous GET
without credentials, an authenticated one with them). The new CLI regression
exposed this before the fix; its bucket name was a made-up placeholder, the
request was an unauthenticated read, and the test now points every S3 endpoint
at a closed local port.

## Changes

**Env file loading** (`cli/configuration.rs`, binary `main`):

- The binary calls `load_env_file(std::env::args_os())` before clap parsing. It
  loads `./.env` from the current working directory only, or exactly the file
  named by the new global `--env-file <PATH>` (pre-scanned from raw arguments)
  or a non-empty `FIREPARQ_ENV_FILE` in the process environment. An explicit
  file must exist and replaces `./.env`. Parent directories are never searched.
- The whole file is parsed before any variable is set. Process variables win
  (the dotenv convention); a `FIREPARQ_ENV_FILE` key inside a file is ignored.
  Parse errors name the file and byte offset but never echo the line, since env
  files hold secrets.
- The result (absolute path, explicit or not, supplied names, names already set)
  is recorded once. `init_tracing` logs `loaded env file` (or `no env file
  loaded`) for `build`, `partitions build`, `merge`, `rollup`, `truncate` and
  `verify`. Other commands, which write results to stdout and install no
  subscriber, print one value-free line to stderr when a file was loaded.
  Completions stay silent.
- The public `load_dotenv()` remains for library callers but no longer walks
  parent directories.

**Write destinations** (`cli/paths.rs`, `verify.rs`, `cli/partitions/io.rs`):

- `resolve_s3_output_root` (used by `build` and `partitions build`) keeps an
  explicit `s3://` output and explicit local paths (`./`, `../`, absolute). A
  relative output is local only when no bucket option is set. With a bucket set,
  a relative output (including the default `.`) or a missing `partitions build`
  output is rejected before any endpoint call, with the explicit `s3://` URI and
  the `./` local spelling suggested. The bucket option otherwise only checks that
  an explicit S3 output names the same bucket. Rejection was chosen over "treat
  as local" so that a deployment relying on the old shorthand fails loudly
  instead of silently writing to ephemeral container disk.
- The build cursor needs no separate rule: a relative cursor lands under the
  (now explicit) output, and an S3 cursor elsewhere is already an explicit URI.
- `partitions build` loads its existing index with the new exact reader
  `read_verified_partitions_index_at`; the read-only commands (`partitions ls`,
  `resolve`, `shard`, `validate`) keep the shorthand.
- `verify` rejects a data path that only the shorthand resolved to S3 when the
  run writes (`VerifyOptions::writes`: the `roots` check, which fills missing
  roots by default, `--report-json`, `--publish-report` or
  `--publish-report-path`), because such runs write a registry or report next
  to the data. (They no longer take dataset ownership since the
  [verify follow-ups](validation-verify-followups.md).) Protocol-only runs keep the shorthand. `merge`, `rollup` and
  `truncate` already refused the fallback (#549), and `recovery` requires an
  existing local path or an explicit URI.

**Destination logs:** before the first write, `build` logs `resolved write
destinations` with the absolute output (local paths made absolute against the
working directory, S3 URIs as given), the cursor-mirror destination (or
`disabled`) and `dry_run`; `partitions build` logs `resolved partition index
destination`; `verify` logs `writing merkle roots registry` with its destination
for each registry commit.

**Deployment migration:** the README documents replacing `S3_BUCKET` plus a
relative `OUTPUT` with Kubernetes dependent-variable interpolation,
`OUTPUT=s3://$(BUCKET_NAME)/v1`. The `k8s-parquet` manifests are not changed
here and must be migrated before this release is deployed.

## Validation

- Unit tests (`cli/tests.rs`): `env_file_in_a_parent_directory_is_never_loaded`,
  `explicit_env_file_replaces_the_current_directory_file`,
  `env_file_parse_errors_never_echo_the_line`,
  `explicit_env_file_argument_is_found_before_clap_parsing`,
  `test_s3_bucket_never_expands_relative_output`,
  `test_s3_bucket_env_does_not_redirect_relative_build_output`,
  `test_relative_build_output_without_bucket_stays_local`,
  `test_resolve_s3_output_root_*`, `test_write_destinations_are_absolute_for_logs`,
  and the updated shorthand test, which now requires verify writes to refuse the
  shorthand while protocol-only verify keeps it.
- Real CLI, `blocks/tests/ingestion_transactions.rs`
  `parent_env_file_is_ignored_and_bucket_never_redirects_relative_output`: from a
  worktree below a directory whose `.env` holds `S3_BUCKET` and AWS settings,
  `build --output target/out` ignores that file, writes locally, and logs `no env
  file loaded` plus the absolute output and cursor destinations; with the same
  file in the working directory it logs the file and variable names (never
  values) and is refused before any write; `--env-file` loads only the named file.
- Real CLI, `blocks/tests/partition_coverage.rs`
  `cli_partitions_build_refuses_relative_output_with_a_bucket`: a relative
  output with `S3_BUCKET` is refused before any endpoint request, and
  `--output ./output` with `S3_BUCKET` set builds locally without reading S3.
- Every spawned binary clears its environment, runs in a temporary directory
  outside the repository tree, and uses fake credentials with S3 endpoints on a
  closed local port.
- `cargo fmt --all --check`; `cargo test --workspace --locked --no-fail-fast` on `origin/main` 97dd244: 1,140 passed, 0 failed, 14 ignored.
