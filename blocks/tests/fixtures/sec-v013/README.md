# SEC real-data fixture (firesec 0.13.0)

Twenty-nine `pinax.sec.v1.Block` windows, one `<block>.pb` each, cut offline from the four 0.13.0 sample feed days
(`2014-08-11`, `2026-03-16`, `2026-08-14`, `2026-08-28`; firesec `FIRE BLOCK` output). Each file keeps the window's
real header and only the filings listed below, with `Filing.ordinal` renumbered to the filing's position in the cut
block (the mapper requires `ordinal` = position). The Firehose metadata of a window is firesec's: `id = n`,
`parent_num = parent_id = lib_num = n - 1` (decimal text), time = window start `n × 600`; the tests derive it from the
header and check it.

- `sec_golden.rs` maps the blocks through `SecBlockMapper` and compares every column of every table with
  `expected/<table>.json`.
- `sec_docs_sql.rs` serves the blocks from a mock Firehose to `fireparq build --block-type sec` and runs the SQL of
  [docs/chains/sec.md](../../../../docs/chains/sec.md) on the dataset.
- `engine_compat.rs` re-homes their filings into its four test windows for the DuckDB and delta-rs read checks.

## Expectations

`expected/<table>.json` holds every row of each of the 43 tables: `{"table", "columns", "rows"}`, one row object per
line, keys in column order. Decimals are strings at the column scale, dates ISO, timestamps `YYYY-MM-DD HH:MM:SS` UTC,
dictionaries their label, lists arrays and structs objects (binary would be lowercase hex; `filing_raw_xml` has no
rows because the samples were produced without `--include-raw`).

The committed files were produced by the **reference prototype**, independently of the Rust mapper:
`proto_map.py` (the final spec's reference implementation) on the proto3 JSON of these exact blocks
(`json_format.MessageToJson(..., preserving_proto_field_name=True)`, checked identical to the samples' JSON for the
untrimmed windows). The Rust mapper matched them on the first run. After an intended mapper change, rewrite them with
`cargo run -p blocks --example refresh_sec_golden` and review the diff; `-- --check` fails when a file is stale.

The cutting and prototype scripts stay outside the repository (CI rejects `.py`).

## Filings

`pos` is the filing's position in the fixture block, `src` its position in the source window.

| block | feed day | accession | pos | src | case | trim |
|---|---|---|---|---|---|---|
| 2346192 | 2014-08-11 | `0001438934-14-000055` | 0 | 4 | Raw body `no_xml` (2014 text N-PX) | — |
| 2346193 | 2014-08-11 | — | — | — | Empty 10-minute window | — |
| 2346252 | 2014-08-11 | `0001140361-14-031610` | 0 | 2 | Legacy SC 13G/A (metadata only) | — |
| 2346276 | 2014-08-11 | `0001323255-14-000015` | 0 | 0 | 13F-NT with cover-page other managers | — |
| 2346291 | 2014-08-11 | `0001477932-14-004179` | 0 | 0 | Form 4/A, 2014, timezone-suffixed dates (`tz_dropped` issues) | — |
| 2346308 | 2014-08-11 | `0000950123-14-008258` | 0 | 5 | 13F-HR filed before 2023 (`value_multiplier_rule` 1000) | — |
| 2956032 | 2026-03-16 | `0001209191-04-032033` | 0 | 0 | Form 4 re-disseminated from 2004, clamped into window 0 | — |
| 2956117 | 2026-03-16 | `0000910472-26-004047` | 0 | 0 | N-CEN/A with `previous_accession_number` | — |
| 2956120 | 2026-03-16 | `0001959173-26-002331` | 0 | 0 | Form 144 with a 10b5-1 plan adoption date and a past-3-month sale | — |
| 2956120 | 2026-03-16 | `0002048118-26-000003` | 1 | 17 | Form D/A with an `Indefinite` total and a co-issuer | — |
| 2956120 | 2026-03-16 | `0002111624-26-000002` | 2 | 13 | Form D/A with `previous_accession_number` and sales recipients | — |
| 2956129 | 2026-03-16 | `0000950103-26-003802` | 0 | 7 | SCHEDULE 13D with authorized persons and Items 1–7 | — |
| 2956131 | 2026-03-16 | `0000897423-26-000035` | 0 | 19 | N-PX notice report (no votes) | — |
| 2956154 | 2026-03-16 | `0000011790-26-000005` | 0 | 129 | Form 144 with 3 `securities_information` lots | — |
| 2977776 | 2026-08-14 | `0001811513-26-000014` | 0 | 154 | 13F-HR the median override flips to thousands (6 qualifying rows) | — |
| 2977781 | 2026-08-14 | `0001720779-26-000005` | 0 | 1 | Form C with financials and a co-issuer | — |
| 2977836 | 2026-08-14 | `0000950103-26-012367` | 0 | 29 | 13F-HR with summary other managers, sequence numbers, multi-id `other_manager_ids` | — |
| 2977838 | 2026-08-14 | `0001172661-26-003444` | 0 | 5 | SCHEDULE 13G/A with `amended_accession` | — |
| 2977880 | 2026-08-14 | `0001595082-26-000063` | 0 | 34 | 13F-HR/A NEW HOLDINGS amendment | — |
| 2977880 | 2026-08-14 | `0000950103-26-912367` | 1 | — | **Synthetic** RESTATEMENT, see below | — |
| 2977896 | 2026-08-14 | `0001104659-26-097111` | 0 | 389 | 13F-HR with 1 holding (MVM Partners; the ≥ 5-row guard keeps the date rule) | — |
| 2977899 | 2026-08-14 | `0002143684-26-000004` | 0 | 174 | Form C-U (progress update) | — |
| 2977907 | 2026-08-14 | `0001339459-26-000007` | 0 | 26 | Form 4, joint owners (CEO + fund), footnote-only price (`value_usd` NULL) | — |
| 2979792 | 2026-08-28 | `0002147005-26-000004` | 0 | — | **Synthetic** original of the deleted accession, see below | — |
| 2979792 | 2026-08-28 | `0002147005-26-000004` | 1 | 1 | EDGAR deletion notice (`CORRECTION`, `DELETION`; raw `deletion`) | — |
| 2979867 | 2026-08-28 | `0001193125-26-372353` | 0 | 4 | N-PX with a split vote, summary managers and `vote_other_managers` (4 votes) | — |
| 2979883 | 2026-08-28 | `0000910472-26-013496` | 0 | 68 | NPORT-P with a convertible (reference instrument, conversion currency) | `nport.holdings`: kept #6 (the convertible) of 24 |
| 2979888 | 2026-08-28 | `0002048251-26-007235` | 0 | 4 | NPORT-P with a two-leg swap (2 holdings) | — |
| 2979897 | 2026-08-28 | `0002048251-26-007523` | 0 | 47 | NPORT-P with an index-basket swap | `nport.holdings`: kept #26 of 670; its `ref_index_components`: the first 3 of 50 |
| 2979907 | 2026-08-28 | `0002000324-26-004161` | 0 | 28 | NPORT-P with a rounded `pct_value` (3 holdings) | — |
| 2979910 | 2026-08-28 | `0000940400-26-035739` | 0 | 22 | NPORT-P with a nested derivative and `additional_info` (2 holdings) | — |
| 2979913 | 2026-08-28 | `0000857490-26-000627` | 0 | 135 | NPORT-P with non-USD fx, securities lending, `N/A` sentinels | `nport.holdings`: kept #0 (EUR fx), #8 (`exchange_rate` and other `N/A`), #75 (lent by the fund) of 129 |
| 2979918 | 2026-08-28 | `0002053459-26-000004` | 0 | 4 | N-PX with frequency (say-on-pay) votes (2 votes) | — |

Trims remove list elements only. Mapper-derived counts (`holdings_count`, `derivative_holding_count`, …) follow the
trimmed lists; values the filer declared (net assets, totals) stay as filed.

## Synthetic filings

Two filings are derived from real ones so the shipped views have something to decide (docs SQL test):

- **`0000950103-26-912367`**, a RESTATEMENT of `0000950103-26-012367` (Atairos Group, 2026-06-30): a copy with form type
  `13F-HR/A`, `is_amendment`, cover page `amendment_type` `RESTATEMENT` and `amendment_number` `1`, accepted 5 minutes
  into window 2977880 (after the original), its last holding (LIFE TIME GROUP HOLDINGS) dropped,
  `table_entry_total` 4 and `table_value_total` lowered by that holding's value; the source path, the first document's
  type and the parties' form type follow, and the parties' film numbers are cleared. `sec_13f_effective_reports`
  must keep it instead of the original.
- **`0002147005-26-000004`** (first copy), the original that the EDGAR deletion notice deletes: the notice's envelope
  without its dissemination flags and timestamp, source path `20260817.gz!0002147005-26-000004`, primary document
  `primary_doc.xml`, and a `beneficial` body holding only `schedule_type` `SCHEDULE 13G`. `sec_filings_first` must
  flag its accession `is_deleted`.
