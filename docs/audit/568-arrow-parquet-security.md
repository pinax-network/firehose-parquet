# Arrow/Parquet security upgrade (#568)

## Decision and dependency evidence

Upgrade the coordinated Arrow and Parquet dependencies from **58.0.0 to
60.0.0**. The remaining Dependabot alert on 2026-09-25 was
[#11](https://github.com/pinax-network/firehose-parquet/security/dependabot/11):
`thrift < 0.23.0`, medium severity,
[GHSA-2f9f-gq7v-9h6m](https://github.com/advisories/GHSA-2f9f-gq7v-9h6m)
(CVE-2026-43868). Parquet 58 requires `thrift ^0.17`, so a compatible lockfile
update cannot select the fixed Thrift release.

[Parquet 59 removed the deprecated Apache Thrift API and dependency](https://github.com/apache/arrow-rs/pull/9962).
That alone is insufficient: the separate compact-Thrift footer reader needs
the [list-length allocation bound in PR #10979](https://github.com/apache/arrow-rs/pull/10979),
included in the released [60.0.0](https://github.com/apache/arrow-rs/releases/tag/60.0.0).
The [59.x backport request](https://github.com/apache/arrow-rs/issues/11186) was
still open during this audit. This change uses the released fix rather than
patching generated upstream Thrift code or adding a second Thrift version.

The new lockfile contains no `thrift` package. It also removes its unused
`byteorder`, `integer-encoding` and `ordered-float` dependencies. Arrow/Parquet
60 requires the associated Arrow 60 crates and codec/support updates, including
`lz4_flex 0.14.0`, `brotli 9.0.0`, `zstd 0.14.0`, `num-bigint 0.5.1`,
`hashbrown 0.17.1`, `atoi 3.1.0` and `base64 0.23.1`. These follow upstream's
published dependency requirements; unrelated dependencies were not refreshed.
The application's `object_store 0.12.5` stays unchanged (Parquet's optional
object-store integration is not enabled). The earlier security fixes for
webpki, Quinn, rand and time stay in the lockfile.

## Compatibility and implementation

[Arrow/Parquet 60 requires Rust 1.88](https://github.com/apache/arrow-rs/blob/60.0.0/Cargo.toml),
below this repository's Rust 1.93 toolchain. No production mapper, schema,
reader, writer, partitioning or cursor code required an API migration.
Two existing tests needed equivalent assertions using the new APIs:

- Arrow `Field::with_metadata` accepts its new `Metadata` type; give `collect`
  an explicit type in the merge schema-mismatch test.
- Parquet's logical timestamp variant is now constructed with
  `LogicalType::timestamp(true, MILLIS)`. The test still requires UTC-adjusted
  millisecond timestamps and still checks decoding without Arrow metadata.

No data conversion or cursor reset is required by the tested paths. Existing
58.0.0 files remain readable, and their schemas and values survive a rewrite.
Physical file bytes are not promised to be identical: upstream compression,
metadata serialization and writer internals changed. The interoperability
contract is the logical values, physical/logical field types, nullability and
resume state, rather than compressed-byte equality.

## Regression coverage

`firehose-parquet/tests/parquet_compatibility.rs` adds three tests:

1. Read a checked-in file actually produced by 58.0.0, require its old
   `created_by` footer, and compare exact Arrow schemas and values. Rewrite it
   through the production writer with every supported output compression
   (`none`, `snappy`, `gzip`, `zstd`) and compare again. The fixture includes
   integer boundaries, floating point, boolean, text, bytes, UTC millisecond
   timestamps, dates, nullable lists and dictionary values.
2. Load an actual 58.0.0 production cursor, compare all resume fields and
   configuration metadata, save it with the current writer, and reload it.
3. Give the table reader and cursor loader a valid Parquet envelope containing
   a malformed compact-Thrift schema list (4096 claimed elements, one byte
   remaining). Both must return the specific pre-allocation bounds error. The
   deliberately modest count catches loss of the guard without making a
   regression exhaust the test runner's memory. A corrupt cursor must not
   become `Ok(None)`.

The legacy files were generated before changing dependencies, from base
`f3e99f327d889cc466d5e1ce29cc708d9f73ddd3`. Both compatibility tests first passed
against 58.0.0. Fixture provenance and SHA-256 hashes are recorded in
`firehose-parquet/tests/fixtures/parquet58/README.md`; the cursor fixture must
be explicitly tracked despite the runtime `cursor.parquet` ignore rule.

The existing all-chain contract still exercises all tables, all five byte
encodings, both fork-step settings and schema-changing mapper options. It
requires exact schemas and values after a production Parquet round trip.
Existing maintenance, partition index, merge journal, verification and cursor
tests remain enabled without relaxed assertions.

## Bounded live qualification

On 2026-09-25, the Arrow/Parquet 60 binary ingested Ethereum blocks
**26,049,575 and 26,049,576** from the explicit Pinax endpoint
`https://eth.firehose.pinax.network:443`, stopping at exclusive height
26,049,577 with `--flush-blocks 1`, into a fresh temporary output directory.
The run completed successfully and produced 26 table files plus a cursor.
Credentials came from the already-authorized environment and were not copied
into evidence.

Compared with the existing Arrow/Parquet 58 output for the same blocks using
DuckDB 1.1.1:

| Table | Rows in each output |
| --- | ---: |
| access_lists | 72 |
| balance_changes | 1,970 |
| blocks | 2 |
| calls | 4,261 |
| code_changes | 3 |
| logs | 1,438 |
| nonce_changes | 465 |
| set_code_authorizations | 2 |
| storage_changes | 3,545 |
| system_balance_changes | 32 |
| system_calls | 8 |
| system_storage_changes | 10 |
| transactions | 458 |
| withdrawals | 32 |
| **Total** | **12,298** |

All 14 table schemas matched in DuckDB and in each file's physical Parquet
schema, including nested field order, logical types and nullability. SQL
`EXCEPT ALL` returned zero differences in both directions for every table,
covering values, nulls and row multiplicities. Cursor resume columns matched
after excluding the opaque stream cursor and write timestamp. The synthetic
cursor regression separately checks preservation of every saved field.

This is a bounded Ethereum interoperability check, not live qualification of
every chain or an exhaustive malformed-input audit. Other chains are covered
by the unchanged offline contract tests.

## Validation and release follow-up

Validation used the repository Rust toolchain, `--locked`, and a separate build
directory to avoid replacing Arrow 58 build artifacts used by concurrent work.
After rebasing onto main `ad1733222974d75ac2197c827106ebebdd57d9df` (including
the startup, cursor-save, S3 cursor and final-completion checkpoint fixes),
validation passed:

- `cargo test --workspace --locked -j4`: **723 passed**, zero failures, three
  pre-existing benchmark tests ignored, including all doc-tests.
  The integrated real Solana final-write/checkpoint regression also passes
  against Arrow/Parquet 60.
- `cargo build --workspace --locked -j4`, binary/build help and Bash, Zsh and
  Fish completion generation.
- `cargo fmt --all --check` and `git diff --check`.
- `cargo tree --locked --workspace -i parquet` selects only 60.0.0;
  `cargo tree --locked --workspace -i thrift` reports no matching package.

The existing `transactions_processed` unused-assignment warning remains in the
binary; no new build warnings were introduced.

The GitHub alert remains a default-branch observation until this upgrade is
merged and GitHub rescans its dependency graph. After merge, refresh open
Dependabot alerts and confirm #11 closes. Do not dismiss the alert manually or
claim its server-side closure based only on the local lockfile.
