# firehose-parquet: unreleased

Changes merged since [v1.1.2](v1.1.2.md). Fold this file into
`docs/releases/vX.Y.Z.md` when the next release is cut, then reset it to this
template.

Add one entry per change under the matching heading, with its issue and PR
(`#N` refs are fine). Say what changed for operators or data consumers, what
they must do (for example rebuild into a new output root), and link the record
in `docs/audit/` or elsewhere when there is one. Remove headings that stay
empty when the release is cut.

## Breaking changes

## New features

- **SEC EDGAR filings: `--block-type sec`.** fireparq now maps
  `pinax.sec.v1.Block` (firesec ≥ 0.13.0: one block per 10-minute window of the
  EDGAR daily feed, 144 per feed day, empty windows included) into 43 Delta
  tables: the `filings` hub, the SGML header's parties, documents, fund series,
  share classes, signatures and raw XML, one table set per form family (Forms
  3/4/5, 13F, 13D/G, 144, N-PORT, Form D, N-PX, N-CEN, Form C), and
  `parse_issues`. Every protobuf field lands in a column; dates are `date`,
  amounts exact `decimal(38,s)` in five scale families, and every source value
  that a typed column does not reproduce exactly has a `parse_issues` row with
  its verbatim text. `block_id` is the decimal window number. Heuristics (13F
  value units, accession dedup, effective 13F and N-PORT reports, N-PX vote
  normalization) are SQL views shipped in
  [docs/chains/sec.md](../chains/sec.md) (`sec-views-v1`), not mapper output,
  so improving one needs no rebuild; `blocks/tests/sec_docs_sql.rs` runs them,
  and `blocks/tests/sec_golden.rs` checks every column of every table on real
  0.13.0 filings against an independent reference prototype. Columns:
  [SEC schema](../schemas/sec.md). Existing chains' tables and schemas are
  unchanged; only the cross-family test digests were re-pinned.

## Fixes

## Performance

## Internal

- **Text block ids.** `PreparedIdentity::with_text_ids` and
  `CanonicalBuilder::prepare_with_text_ids` (`firehose_parquet::traits`) write
  block and parent ids that are already text verbatim (as their UTF-8 bytes
  under `Binary`); the SEC mapper uses them for its decimal window numbers.
- **`engine_compat.rs` reads a SEC dataset**, the first with `decimal(38,s)`,
  non-partition `date`, `array<struct<…>>` and `binary` columns, in DuckDB and
  delta-rs. Its DuckDB scan names the file-path column `fireparq_data_file`,
  because SEC's `filing_documents` has a `filename` column.
- **A local ownership guard unlocks its directories when it is dropped**, before
  closing them (#706). A process spawned while the guard was held keeps a copy
  of each locked descriptor until it execs, and a `flock` lock belongs to the
  open file description that the copy shares. So the lock could outlive the
  guard, and a new acquisition right after failed with "lock acquisition
  failed because the operation would block". In the test suite, where tests
  run child processes beside each other, that made
  `nested_symlinks_fail_before_mutation_but_explicit_root_aliases_work` fail
  now and then. `fireparq` itself starts no child processes, so it wasn't
  affected. `a_dropped_guard_releases_its_locks_while_inherited_descriptors_live`
  holds a copy of the descriptors and acquires again.
