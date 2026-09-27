# Issue #654: `--output` is the dataset root, with an opt-in `{chain}`

Closes #654 (PR [#PR](https://github.com/pinax-network/firehose-parquet/pull/PR)); part of #463.

## Decision

`build` and `partitions build` appended the endpoint's `chain_name` to
`--output`. PR #642 added `--without-chain-dir` / `WITHOUT_CHAIN_DIR` for a
bucket per network. That kept the chain directory as the default and added a
second knob that had to agree with `--output` on every run. On 2026-09-27 the
user decided that fireparq is a proof of concept and that v1.x may break
things, so no legacy behavior is kept:

- `--output` is used exactly as given. No `<chain_name>` directory is appended.
- The flag and its env variable are removed. They were never released.
- `{chain}` is an opt-in placeholder in `--output` (both commands). It expands
  to the canonical `chain_name` from EndpointInfo.
- EndpointInfo with a nonempty `chain_name` stays mandatory before any output
  resolution. The name is still recorded in the `firehose-parquet.chain_name`
  file metadata and in the protected descriptor.

A `v0.7.x` command line keeps its layout only by adding `/{chain}`:
`--output ./output` becomes `--output './output/{chain}'`.

## One resolver

`firehose_parquet::cli::resolve_output_root(output, chain_name)` in
`firehose-parquet/src/cli/paths.rs` is the only place where `--output` becomes a
dataset root. It works in four steps:

1. It requires a nonempty chain name, whether or not the template uses it.
2. It expands the template with `parse_template`. `--cursor-template` uses the
   same parser, so `{{` and `}}` escape braces in both. Only the variable names
   differ.
3. It strips trailing `/` from S3 roots, so `s3://bucket/` and `s3://bucket` are
   the same bucket root. Local roots are returned as given.
4. It validates the result. An S3 root must still name a bucket.

Callers:

- `build`: `resolve_output` in `blocks/src/bin/main.rs`. The cursor mirror
  (`CursorLocation::resolve`), `ingestion_mutation_scopes` (ownership), the
  session, recovery and `log_write_destinations` all read `config.output` after
  it.
- `partitions build`: `run_partitions_build` resolves the same root. The index
  (`partitions_index_path_in`), its ownership and the sibling default cursor
  mirror that supplies a missing `--start-block` all use it.

The removed helpers (`output_root_without_chain_dir`,
`resolve_partitions_output_root`, `build_partitions_output_root`,
`build_partitions_index_path`, `build_partitions_cursor_path`) had no other
callers.

`validate_output_template`, called from `resolve_s3_output_root`, checks the
syntax, the variable names and the bucket for both commands before the
endpoint is contacted. So a template error takes precedence over the
`S3_BUCKET` bucket check.

`build` logs `resolved --output template output_template=... output=...` when
the template changed the value. The existing `resolved write destinations` line
is still logged before any write. `partitions build` adds `output=<root>` to
`resolved partition index destination`.

### Behavior (EndpointInfo `chain_name` = `mainnet`)

| `--output` | Dataset root |
|---|---|
| `.`, `./output`, `./output/`, `/data/output`, `output` | unchanged, byte for byte |
| `s3://ethereum-mainnet`, `s3://bucket/v1` | unchanged, byte for byte |
| `s3://ethereum-mainnet/`, `s3://ethereum-mainnet//` | `s3://ethereum-mainnet` |
| `s3://bucket/v1/` | `s3://bucket/v1` |
| `{chain}`, `{chain}/raw` | `mainnet`, `mainnet/raw` |
| `./output/{chain}`, `/data/{chain}/raw`, `./output/{chain}-final` | `./output/mainnet`, `/data/mainnet/raw`, `./output/mainnet-final` |
| `s3://datasets/{chain}`, `s3://datasets/{chain}/` | `s3://datasets/mainnet` |
| `s3://datasets/{chain}/v1`, `s3://datasets/v1/{chain}/raw`, `s3://datasets/eth-{chain}` | `s3://datasets/mainnet/v1`, `s3://datasets/v1/mainnet/raw`, `s3://datasets/eth-mainnet` |
| `./output/{{chain}}`, `./output/{{{chain}}}`, `./a}}b{{c` | `./output/{chain}`, `./output/{mainnet}`, `./a}b{c` |

| Input | Error |
|---|---|
| `./output/{network}`, `{Chain}`, `{}`, `{chain_name}` | `unknown --output variable {...}`; the only variable is `{chain}` |
| `./output/{chain`, `s3://bucket/v1/{` | `unterminated --output variable` |
| `./output/chain}`, `s3://bucket/}` | `unmatched } in --output` |
| `s3://{chain}`, `s3://{chain}/raw`, `s3://data-{chain}/raw`, `s3://{{data}}/raw` | a placeholder or brace in the S3 bucket name |
| `s3://`, `s3:///raw`, `s3:///{chain}` | `S3 URL missing bucket name` |
| any output, `chain_name` empty or blank (or no EndpointInfo) | `EndpointInfo with a nonempty chain_name is required` |
| `{chain}` with a chain name that is not one segment of `[A-Za-z0-9._-]`, or is `.` / `..` | `cannot expand {chain}` |

### S3 bucket placeholder: refused

`{chain}` and brace escapes are refused in the bucket segment of an `s3://`
URI. They are not validated and allowed. The bucket has to be known before the
endpoint is contacted:

- the credential preflight, the `S3_BUCKET` consistency check and the bucket
  binding of a custom endpoint (`endpoint_is_bucket_bound`) run in
  `build_config` / `validate_cursor_storage` before EndpointInfo;
- S3 ownership is bucket-wide (#636).

A bucket name derived from a remote chain name would also have to meet the
provider's naming rules. The error names the fix: `s3://<bucket>/{chain}`.

The chain name itself must be one path segment of ASCII letters, digits, `-`,
`_` and `.`, and not `.` or `..`. All 46 built-in network names qualify. The
rule only applies when `{chain}` is used, so a template can never gain or lose
a directory level, or turn a local path into a URI.

## Protection

No descriptor change was needed. The protected descriptor already binds the
exact output identity, and protected-root discovery refuses a root that encloses
or is nested in another protected root before any Blocks request. A changed
template that resolves to another root lands there. The overlap message now
says that `--output` is used as given, with `{chain}` expanded.

Tests:

- **In-memory S3** (`firehose-parquet/src/ingest/session/tests.rs`). The session
  configuration comes from `resolve_output_root`.
  - `remote_session_at_the_bucket_root_resumes_and_refuses_a_chain_template`: a
    dataset created with `s3://data` resumes as `s3://data` and `s3://data/`.
    `s3://data/{chain}` (`s3://data/mainnet`) is refused as overlapping, and no
    key changes.
  - `remote_bucket_root_cannot_initialize_above_an_existing_chain_template_root`
    covers the reverse: a dataset created with `s3://data/{chain}` resumes as
    `s3://data/{chain}/` and `s3://data/mainnet`, and `s3://data` and
    `s3://data/` are refused.
- **Local, real binary** (`blocks/tests/ingestion_transactions.rs`,
  `a_changed_output_template_is_refused_before_blocks`). A dataset created at
  `output` refuses `output/{chain}`, passed as `--output` or `OUTPUT`, with no
  Blocks request and no changed byte. `output/` and `./output` resume it. The
  reverse: a dataset created at `output/{chain}` (from `OUTPUT`) refuses
  `output` both ways, and `output/{chain}` and `output/ingestion-test` resume
  it. The test also checks the `resolved --output template` and `resolved write
  destinations` log lines.

## Other tests

- Unit tests (`firehose-parquet/src/cli/tests.rs`):
  - `test_output_root_default_is_byte_identical_to_output`
  - `..._normalizes_s3_trailing_slashes`
  - `..._expands_the_chain_placeholder` (local and S3; prefix, middle, suffix
    and in-segment positions)
  - `..._escapes_braces`, which also covers the shared `--cursor-template`
    parser
  - `..._template_errors`: every error above. Each is raised identically by
    `resolve_s3_output_root`, with or without `S3_BUCKET`.
  - `test_resolve_s3_output_root_keeps_a_valid_output_template`
- `blocks/src/bin/main.rs`:
  - `resolve_output` tests (as given, placeholder, EndpointInfo required)
  - the default mirror under a `{chain}` root
  - `test_output_is_the_only_layout_control_and_help_documents_chain`: no
    chain-directory flag or env variable on either command, and the help texts
    carry the `{chain}` examples
  - the `.env.example` drift test
- Real binary:
  - `output_is_the_dataset_root_and_every_command_follows_it` replaces the #642
    end-to-end test. `build`, resume, `verify`, `scan`, `validate`, `inspect`,
    `merge`, `rollup`, `truncate`, `partitions build` (start inferred from the
    root cursor), `partitions ls` / `validate` / `resolve`, `recovery` and a
    completed-range no-op all run at `--output` itself.
  - `invalid_output_templates_are_refused_before_the_endpoint_is_contacted`
    runs `build` and `partitions build` against a closed port.
  - `cli_output_template_resolves_the_index_and_the_cursor_at_one_root`:
    `partitions build --output 'output/{chain}'` writes
    `output/test-chain/_fireparq/partitions.parquet`, infers `--start-block`
    from the sibling cursor, and resumes from the template and from its
    expansion.
  - `build_partition_and_maintenance_commands_conflict_with_a_descendant_owner`
    now needs EndpointInfo to expand `{chain}` before it takes ownership.
  - `the_default_output_is_the_working_directory_itself`: without `--output`
    the root is `.`. A working directory holding an unrelated file is refused
    before Blocks and gains no entry; an empty one becomes the dataset root.
- Every test that assumed `<output>/<chain_name>` now uses `--output` itself.

## Limits

- The default `--output` is `.`, so `build` without `--output` now writes into
  the working directory itself. A non-empty directory is refused by eligibility
  (`unrelated files`) before any Blocks request.
- `--cursor-template` still has no `{chain}` value in `build`: it is resolved
  before EndpointInfo. A relative cursor path follows the resolved dataset root.
- `firehose-parquet/src/rollup.rs` keeps one doc comment that names the removed
  flag. The parallel `feat/date-partition-key` change (#652) removes `rollup`,
  so this change leaves that file alone.
