# #526: chain profiles and shared mapper helpers

Status: implemented from origin/main `9372f99`; PR review, CI and merge are
pending. This is a behavior-preserving refactor. No flag, default, schema,
table, encoding, file metadata value, cursor metadata value or protected
descriptor changes.

## Diagnosis on current main

The issue predates the ingestion split (#525, PR #611), the CLI modules (#528,
PR #612) and owned protobuf payloads (#518, PR #606). It was re-scoped against
`9372f99`. The chain-specific decisions were spread over:

- `blocks/src/bin/main.rs`: `output_encoding_policy`, the substring rules in
  `infer_partitions_block_type`, `detect_block_type`, `protected_block_family`,
  `block_type_allows_block_number_gaps`, `block_type_has_nullable_timestamps`,
  `chain_name_is_{solana,antelope}` with their `endpoint_`/`cursor_`/`chain_is_*`
  wrappers and `chain_is_known_non_solana`, `maybe_add_solana_with_votes_metadata`,
  `resolve_include_failed_transactions` (`!= Some("evm")`) and the string
  `create_mapper` dispatch.
- `blocks/src/bin/ingestion/setup.rs`: `"auto"` validation, the Solana/Antelope
  extended branches and `block_type != "evm"`.
- `blocks/src/bin/ingestion/runtime.rs`: `"auto"` checks, `is_solana` and the
  detected `== "solana"`/`"antelope"` branches.

The mapper duplicates were `append_fork_step` and `finish_fork_step` (8 copies
each), `mk_fork_step` (6 copies plus 11 inline copies in Solana and Antelope),
`maybe_fork_step` (8), `enum_data_type` (4 plus an inline Beacon type),
`estimated_dictionary_index_bytes` (4), and two copies of the protobuf enum
prefix stripper.

## Change

- `blocks/src/chain.rs` adds `ChainKind` and one static `ChainProfile` per
  family. A profile holds the label, `type_url` marker, protected `BlockFamily`,
  bytes-encoding contract (with the Tron-style EVM override), nullable
  timestamps, block-number gaps, `ExtendedOutput`, votes, the failed-transaction
  default and the strict chain names. `CHAIN_NAME_RULES` is the ordered
  substring-inference table. `ChainKind::create_mapper(MapperOptions)` replaces
  the string dispatch and is infallible.
- The binary parses `--block-type` once into `Option<ChainKind>` (`None` is
  `auto`) and passes typed families through setup, runtime, file and cursor
  metadata. Solana/Antelope checks became profile properties:
  `PreStreamChainFeatures` holds votes, unsupported extended output and "known
  without votes", resolved by `chain_has`/`chain_known_without`. The extended,
  detected-extended and failed-transaction family decisions are small functions
  (`resolve_pre_stream_extended`, `resolve_detected_extended`,
  `pre_stream_block_types`). The `partitions build` routing policy uses the
  nullable-timestamp property.
- `firehose_parquet::traits` now provides `push_fork_step_field`,
  `fork_step_builder`, `append_fork_step`, `finish_fork_step`, `enum_data_type`,
  `estimated_dictionary_index_bytes` and `strip_enum_prefix`. Every chain uses
  them, and the per-chain copies are removed.

The remaining chain strings are the CLI `BLOCK_TYPES` text (tested to equal the
profile labels), `auto` parsing, and Tron-style endpoint names. The Tron-style
names select a network profile, not a family.

## Preserved behavior worth knowing

These were kept on purpose. Changing any of them would change behavior:

- **Two chain-name heuristics.** Protected and partition inference use ordered
  substring rules, such as `eos` → Antelope, `mainnet` → EVM and `tron-evm`
  before `tron`. Flag handling for `auto` uses strict names: Solana `solana` or
  `solana-*`, and Antelope `antelope`, `antelope-*` or `eos`.
- **Extended output.** Only Solana and Antelope are statically `Unsupported`.
  Only they force extended output off and warn on `--without-extended`. The
  other non-EVM families still log the endpoint capability. Protected runs
  record `extended=false` for them, while a dry run keeps the flag value in
  cursor metadata.
- **Dry-run cursor validation.** Solana checks `with_votes` but keeps an
  `extended` mismatch. Antelope drops `extended` mismatches.
- **Unknown cursor labels.** An unknown `firehose-parquet.block_type` label,
  such as `ethereum` or `EVM`, has no family. It still gets the non-EVM
  failed-transaction default, and dry-run auto-detection still re-resolves.
- **Out of scope.** The library cannot depend on `blocks`, so these library
  mappings remain. The `BlockFamily` names in `ingest/mirror.rs` are tested to
  equal the profile labels. Legacy `bytes_encoding=auto` cursor inspection in
  `cursor.rs` still maps Antelope to `hex`, while output uses `hex_no_prefix`.
  That discrepancy predates this change and is left for a separate fix.

## Validation

Oracle equivalence tests hold verbatim copies of the replaced origin/main
functions, extracted from `9372f99`:

- `blocks/src/chain/tests.rs`: 552 chain names and 563 `type_url`/name inputs.
  The names cover the registry, every keyword and ordered keyword pair, case
  variants and substring traps such as `linear` and `geoscience`. They are
  checked against the legacy inference, `detect_block_type`, strict matching,
  families, gaps, nullable timestamps, extended/votes/failed defaults and both
  encoding contracts. A test also checks that every mapper configuration builds
  the same tables as the legacy dispatch: 8 families × 5 encodings × 32 option
  combinations.
- A schema digest covers the table inventory and complete Arrow schemas of
  those 1,280 configurations. Its pinned value
  `68e8859576f696910042452f8815a6e7f9002c9357e4dd2a26edf30c61249dde` was produced
  by the legacy `create_mapper` on `9372f99`, before the helper move.
- `blocks/src/bin/chain_profile_tests/mod.rs` compares 621,504 combinations of
  9 requested types × 83 endpoints × 26 cursor states × 32 flag, partition and
  dry-run combinations. It checks the pre-stream setup decisions, extended
  output, failed-transaction resolution, initial encoding, synthetic routing,
  auto-detection re-resolution and warnings. It also compares complete table,
  cursor and partition file-metadata entry lists against the legacy builders.
- Mutation checks were deliberately run: removing the `eos` strict alias and
  removing the `near` inference rule each made these tests fail.

Workspace: `cargo fmt --all -- --check`, `cargo test --workspace --locked`
and `cargo test -p blocks --example refresh_evm_golden --locked` pass. The
workspace run had 1,103 passed, 0 failed and 14 ignored, against 1,089 passed
and 14 ignored on origin/main. The 14 added tests are 8 profile tests, 4
binary oracle tests and 2 shared-helper tests. The existing schema contract
tests pass unchanged.

### Live comparison

Release binaries from origin/main `9372f99` and this branch built the same
bounded ranges from Pinax endpoints. DuckDB compared every table with
`EXCEPT ALL` in both directions, plus row counts, partition directories,
Parquet physical schemas and all file key/value metadata. Cursor rows were
compared except `updated_at`, along with cursor metadata and the protected
`state.json` descriptor and checkpoint.

| Case | Range | Tables | Rows | Result |
|---|---|---:|---:|---|
| EVM `mainnet`, auto, date | 23000000–23000004 | 15 | 62,976 | equal |
| Solana `solana-mainnet-beta`, auto, date | 360000000–360000002 | 8 | 26,241 | equal |
| Solana, `--block-type solana --without-votes` | 360000000–360000002 | 7 | 23,349 | equal |
| Beacon `mainnet-cl`, `--block-type beacon`, date | 12500000–12500002 | 5 | 89 | equal |
| Beacon `mainnet-cl`, auto | 12500000–12500001 | 5 | 57 | equal |
| EOS `eos`, auto (Antelope), date | 420000000–420000002 | 4 | 148 | equal |
| Bitcoin `btc`, auto | 900000–900001 | 4 | 21,895 | equal |

The cursor metadata values were identical, including `block_type`,
`bytes_encoding`, `extended`, `with_votes` and `include_failed_transactions`,
as were the protected family, routing policy and table digests. Only
run-specific identifiers were normalized: `stream_id` hashes the output and
cursor paths, while the transaction and checkpoint IDs name a single run.
Dry-run `auto` runs (EVM 3 blocks, Solana 2 slots, date) produced identical
detection, metadata, flush and completion log lines. No StreamingFast endpoint
was contacted, and no NEAR or Tron network was used.

## Adding a chain after this change

See `docs/repo-navigation.md`. Add the variant, profile, mapper constructor and
any inference rule in `blocks/src/chain.rs`, then add the protected
`BlockFamily` variant. The #526 oracles describe exactly the eight pre-#526
families. A new family must extend their expected lists or retire them, and
the pinned schema digest changes by design.
