# #528: separate CLI declarations from command operations

## Diagnosis and scope

Issue #528 identifies a navigation/review problem: `src/cli.rs` combines Clap
structures with partition construction, index IO, path policy, Parquet inspection
and validation. The current baseline is `270af1674597da4e5dde9433ee672c2c5ebfb1a2`,
including #527's shared AWS arguments and builder. This is a mechanical extraction,
not a rewrite of command behavior or the maintenance/ingestion engines.

The initial implementation was integrated with main `5de4f16`, including the
merged Tron work, and finally with actual main `ab0888e` and the ingestion modules. Source commit `88af38e` contains the extraction. The original stopped
agent checkout and untracked `.claude/` worktrees remain preserved.

## Module map and compatibility

| Module under `firehose-parquet/src/` | Responsibility |
|---|---|
| `cli.rs` | Clap declarations, argument impls, constants, stable public re-exports |
| `cli/configuration.rs` | Value parsing, resolved Config, environment/logging helpers, completions |
| `cli/paths.rs` | Local/S3 path selection, S3 credential preflight, cursor templates |
| `cli/inspect.rs` | Scan ordering, file collection, schema/sample display and inspection |
| `cli/validate.rs` | Canonical block validation, result models and rendering |
| `cli/partitions/mod.rs` | Partition vocabulary, models, bound helpers |
| `cli/partitions/builder.rs` | Incremental index rows, ordering and resume |
| `cli/partitions/io.rs` | Strict index schema, metadata, loading and publication |
| `cli/partitions/queries.rs` | Resolve/list/shard/completeness operations |
| `cli/tests.rs` | Existing inline CLI tests, moved as one module |

`cli.rs` is 1,371 lines after extraction. Its implementation modules are private;
public glob re-exports preserve existing `firehose_parquet::cli::*` imports, also
through nested partition modules. Formerly private implementation helpers use
`pub(in crate::cli)` where needed, restoring their original visibility within the
CLI tree without exposing them outside it. Public and crate-visible declarations
retain their existing visibility. Root imports remain the shared namespace for
these related modules. Existing `cli/validate_tests.rs` and its explicit test
module declaration are unchanged; moved tests retain their `cli::tests` names.

No new dependency, flag, default, environment binding, schema, row transformation,
IO policy, validation rule or output format is introduced. Shared S3 construction
continues to live in `s3.rs`; this change does not duplicate an AWS module.

## Unchanged-item proof

The offline [comparison](528-cli-item-comparison.json) covers **353 substantive
Rust items**: 178 production definitions/impls and 175 test definitions/helpers.
The [standalone syntax comparator](528-compare-cli-items.rs) uses `syn` and `quote`
to compare complete item tokens, including function bodies, method bodies,
signatures, struct/enum definitions, values, attributes and documentation.

Only the following representation differences are normalized:

- Newly introduced `pub(in crate::cli)` becomes inherited private visibility.
- Rustfmt-only trailing commas in function/generic/call argument lists are
  normalized, including calls inside `vec!` expression arrays. Tuple syntax and
  other macro tokens are not generally rewritten.
- The old inline `tests` module and new file are compared in the same namespace.

Imports and module declarations are excluded from this token equality check;
their visibility, lookup and public compatibility are separately reviewed and
checked by compiling every workspace target and running the existing suite. No
claim is made that syntax comparison alone proves name resolution. There are no
moved `module_path!`, `file!`, `line!` or implicit-target tracing calls that would
change observable behavior simply because their module moved.

To reproduce the comparison, copy the standalone comparator into a temporary
Cargo binary (outside this workspace) using edition 2021 and dependencies
`syn = { version = "=2.0.119", features = ["full", "visit-mut"] }`,
`quote = "=1.0.47"`, `proc-macro2 = "=1.0.107"` and
`serde_json = "=1.0.151"`. Export the baseline with
`git show 270af1674597da4e5dde9433ee672c2c5ebfb1a2:firehose-parquet/src/cli.rs`,
then pass that file followed by the ten source files in the table above. The
program exits nonzero on any missing, added or differing substantive item and
prints the compared inventory on success. No chain or storage calls are made.

## Validation

On integrated main `5de4f16`, the complete locked workspace suite passed
**1,069 tests, zero failures, 11 intentional skips**. The CI capture example
passed its endpoint/auth regression (one explicit subprocess fixture remains
ignored in the parent invocation). The locked binary build passed.

Formatting and all three shell completions passed after normalizing the moved
test file's leading blank line. All 80 AWS option definitions across 16 command
paths are byte-identical to the #527 snapshot. Independent review checked all
79 public named declarations, restricted visibility, namespace lookup, test
paths and proof limitations; no blocker was found. Fresh PR CI remains required
before merge. No additional live data or production storage writes are needed
for this extraction; all mapper, authority and mutation code remains unchanged.

After integrating #525's actual merge `ab0888e`, **341 focused tests** passed:
156 CLI/library tests, 176 binary tests and nine real ingestion lifecycle tests.
The retained-EVM baseline replay is an opt-in ignored case already explicitly
qualified in #525; it was not re-run for this CLI extraction. Locked build,
formatting and bash/zsh/fish completions passed again. The unchanged-item
comparison was re-run successfully on the final source; recorded source hashes
include the final formatting. Documentation conflicts retained both module maps,
release notes and verified lifecycle evidence. No executable merge conflict
occurred. PR review/CI/merge outcomes are recorded separately in the audit index.
