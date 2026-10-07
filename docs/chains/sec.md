# SEC EDGAR notes

Semantics of the `sec` tables beyond their columns, which the [SEC schema reference](../schemas/sec.md) lists with
each column's source path in the `pinax.sec.v1.Block` protobuf. The data comes from **firesec ≥ 0.13.0**, which turns
the EDGAR daily feed into one Firehose block per 10-minute window. Build a dataset with
`--block-type sec --endpoint <firesec Firehose endpoint>`; the stage endpoint is private and gets no automatic
credentials, so pass `--api-key-envvar` when the gateway needs one ([authentication](../authentication.md)).

**Size the stream and the writer for deadline days.** A 13F or N-PX deadline day puts whole filings of hundreds of
thousands of votes or holdings into one window. The largest sample window, 2977878 (2026-08-14), is a 143.9 MB
protobuf: above the 128 MiB default of `--grpc-max-message-bytes`, so fireparq stops on it with `OutOfRange`.
Pass `--grpc-max-message-bytes 268435456` to every SEC build. A window of that size alone takes about 1.5 GB while it is
decoded, mapped and flushed. The replay of the four sample days at the default flush settings peaked at 2.0 GB RSS,
so give the writer at least 3 GiB of memory.

## Block model

- **One block per 10-minute window.** `block_num = unix_seconds / 600` of the window start, so a feed day has 144
  windows, numbered from `day_start / 600`. `timestamp` is the window start and `date` its UTC day, which is the
  EDGAR feed day (`blocks.feed_date`). The first streamable window is 1761552 (2003-06-30).
- **Empty windows are blocks.** Every window has one `blocks` row, filings or not (`filing_count` 0), so
  `fireparq validate` reports a missing window as a gap. A feed day arrives in one burst after EDGAR publishes its
  daily dump: most of a day's windows then land in a few flushes.
- **Ids are decimal text.** `block_id` is the window number written verbatim (`"2979867"`) and `parent_id` the
  previous one; `parent_num = lib_num = block_num − 1`. They are not hashes. The Parquet footer key
  `firehose-parquet.block_id_encoding` still says `hex_0x`, the label of the chain's byte encoding: ignore it for
  `sec`.
- **Filings inside a window** are ordered by acceptance time, then accession number: `filing_index` is that position.
  firesec places each filing of a feed day in the window of its EDGAR acceptance time. Filings accepted before the
  feed day's first window, usually the previous evening's, are **clamped into window 0**, and those accepted after
  the last into window 143. `filings.acceptance_in_block_window` is false for them (about 5% of filings), and
  `filings.dissemination_lag_days` (`date − filing_date`) shows how far back the filing is: 0–4 days normally, years
  for a re-disseminated old filing.
- **Row order** within a block is `filing_index`, then the child positions, which follow the XML document order.

## Keys and deduplication

Every row is keyed by `(block_num, filing_index)`, plus the child positions of its table (`holding_index`,
`vote_index`, `record_index`, …; the [schema reference](../schemas/sec.md) gives each table's key). Join children to
their parents on the full key prefix, for example `nport_holdings` to `nport_derivatives` on
`(block_num, filing_index, holding_index)`. Every filing-derived table repeats the five filing-context columns
`filing_index`, `accession_number`, `form_type`, `filing_date` and `acceptance_datetime`, and child tables copy a few
parent columns under the parent's names (`manager_cik`, `period_of_report`, `filer_cik`, …), so most questions need
no join.

**`accession_number` is not unique.** EDGAR re-disseminates old filings in later daily feeds (a 2004 Form 4 in a 2026
feed, for example) and announces deletions with a notice that carries the deleted accession. The
[`sec_filings_first`](#shipped-views) view is the exact deduplication: the first dissemination of each accession, by
`(block_num, filing_index)`, never a deletion notice. Semi-join it on `(block_num, filing_index)` to count each filing
once. Its `is_redisseminated` is about this dataset only: a dataset that starts after a filing's original
dissemination keeps its first copy in the dataset. Build from block 1761552 to avoid that.

`filings.is_deletion_notice` marks the notices (`dissemination_flags` contains `DELETION`, body `raw` with reason
`deletion`); `sec_filings_first.is_deleted` tells whether the accession has one.

## Value semantics

### Strings, lists and booleans

- Strings are verbatim, as firesec trimmed and unescaped them; an empty source string is NULL. `accession_number` and
  `form_type` are never NULL. Codes and identifiers stay strings: CIKs (10 digits, zero padded), CUSIPs, CRD and file
  numbers, zip codes, `fair_value_level` and every `delta`.
- Repeated fields are `array<string>` (or `array<date>`, `array<integer>`, `array<struct<…>>`), `[]` when empty, never
  NULL, items in document order.
- A protobuf `bool` on a message every row has is never NULL: false means false or absent. Inside an optional
  sub-message (`Relationship`, `CoverPage`, `GeneralInfo`, `OfferingData`, `FormCOffering`, `SecurityLending`) it is
  NULL exactly when the sub-message is absent, and an `optional bool` (`aff_10b5_one`, the Form D `*_is_estimate`
  flags, …) is NULL when unset. `Y`/`N` text becomes a nullable `boolean`.
- `*_count` columns are counts the mapper computed (list lengths, rows written). Counts the filer declared keep their
  form's name: `form13f_reports.table_entry_total`, `npx_reports.declared_series_count`, … `has_*` columns say
  whether an optional part is present.

### Dates

Dates are `date`. One grammar reads every form's spelling: ISO `YYYY-M-D` (with an optional zone suffix, which is
dropped), and the US forms' month-first `M/D/YYYY` and `M-D-YYYY`. Implausible but valid dates are kept (`1601-05-20`,
`9999-12-31`). `dissemination_timestamp` (US-Eastern `YYYYMMDD:HHMMSS`), `fiscal_year_end` (`MMDD`) and the 13D's free
text `date_5_percent_ownership` stay strings. `acceptance_datetime` is a UTC `timestamp`.

### Decimals

Amounts are exact decimals in five scale families, chosen per column by meaning:

| Family | Delta type | Columns |
|---|---|---|
| M2 | `decimal(38,2)` | USD with cents: Form 144 market values and proceeds, every Form D amount, Form C offering amounts and financial statements |
| Q6 | `decimal(38,6)` | Quantities and per-unit prices: Forms 3/4/5 amounts (and `value_usd`, `signed_shares`), 13D/G powers and amounts, Form 144 units, Form C `price` and `num_securities_offered` |
| N10 | `decimal(38,10)` | N-PORT amounts and quantities: fund totals, flows, `balance`, `value_usd`, lending values, derivative amounts, prices and notionals, swap legs, basket components |
| R12 | `decimal(38,12)` | Percents, rates and ratios: N-PORT `pct_value`, returns, `exchange_rate`, rates and spreads, `conversion_ratio`; 13D/G `percent_of_class` |
| S16 | `decimal(38,16)` | N-PX share counts: `shares_voted`, `shares_on_loan` |

XSD integers are `long` (13F `value`, `shares_or_principal_amount` and voting authority, Form D investor counts) or
`integer` (years, amendment, sequence and serial numbers). Numbers are parsed exactly; a value with more decimals than
its scale is rounded half away from zero, like DuckDB's `CAST`, and logged (below).

DuckDB multiplies decimals at the sum of the scales and **fails at run time** when the result needs more than 38
digits:

- Common products fit: `shares × price_per_share` (Q6 × Q6), `value_usd × exchange_rate` (N10 × R12),
  `pct_value × net_assets` (R12 × N10).
- Cast S16 columns to `DOUBLE` or `DECIMAL(38,6)` before multiplying them.
- A sum keeps its operand's type, so a sum of products keeps the product's few integer digits: round or cast the
  product first, `sum((pct_value * net_assets)::DECIMAL(38,10))` or `::DOUBLE`.

### Parse issues

No source value is dropped silently. For every typed column, either the typed value equals the source text
(numerically or as a calendar date) or the value has exactly one `parse_issues` row, which holds the verbatim
`raw_value`, the `issue` and the row's key: `(block_num, filing_index, table_name, column_name, index_1, index_2,
index_3)`, where `index_1..3` are the table's key positions after `filing_index` and then, for a list item, the
item's position. Block-level values (`blocks.feed_date`) have a NULL `filing_index`. Every typed table has
`has_parse_issues`, true on the rows that have one.

| `issue` | Typed value | When |
|---|---|---|
| `unparseable` | NULL | the text does not match the column grammar |
| `sentinel` | NULL | `N/A`, `NA`, `NONE`, `NULL`, `-` or `XXXX` (any case) |
| `out_of_range` | NULL | too many integer digits, outside the integer type, an invalid calendar date or a year outside 1000–9999 |
| `rounded` | rounded | non-zero digits beyond the column scale |
| `tz_dropped` | the date | an ISO date with a zone suffix (`2014-06-30-05:00`) |
| `overflow` | NULL | a derived sum or product overflowed; `raw_value` holds the operands, joined with ` * ` or ` + ` |

Form D's `Indefinite` amounts are not issues: the amount is NULL and `total_offering_amount_is_indefinite` (or
`total_remaining_is_indefinite`) is true. On the four 0.13.0 sample days, 4,597 of 31.7 M typed values have an issue,
almost all of them N-PORT `N/A` sentinels and float-printing artifacts rounded below 5e-11 USD or 5e-13 percent; the
last example query puts the source text back next to a typed value.

### 13F value units

`form13f_holdings.value` and `form13f_reports.table_value_total` are **raw**, as filed: thousands of dollars for
filings before 2023-01-03, dollars after, and some filers get it wrong either way. `value_multiplier_rule` (1000 or 1)
is the legal date rule only (a NULL `filing_date` falls back to `date`). The [`sec_13f_units`](#shipped-views) view
decides each filing's multiplier, overriding the rule only when the median value per share of at least five plain
share rows contradicts it, and [`sec_13f_holdings_usd`](#shipped-views) gives `value_usd`. `holdings_value_sum` next to
`table_value_total` shows the filings whose own total disagrees with their rows; `holdings_complete` shows the
information tables shorter than `table_entry_total`.

### Other derived columns

The mapper derives only facts a rule defines; heuristics are the views below.

- `filings.issuer_*` and `filer_*` come from the SGML header parties, in header order. The issuer is the first
  `ISSUER` or `SUBJECT-COMPANY` party; Forms D and C, whose issuers file for themselves, fall back to the first
  `FILER`. The filer is the first `REPORTING-OWNER`, `FILED-BY` or `FILER` party: the insider of a Form 4, the seller
  of a Form 144. `filing_parties` keeps every party, joint reporting owners and co-registrants included.
- Form 4 rows (`ownership_transactions`): `signed_shares` (+ for `A`, − for `D`), `value_usd` = `shares ×
  price_per_share` (NULL, never 0, when the price is only in a footnote), `is_open_market` (non-derivative `P` or `S`)
  and `filing_lag_days`. A joint filing's transactions belong to the filing: `owner_ciks` and `owner_names` list every
  reporting owner, `any_owner_is_*` are ORs over them, and `officer_titles` holds only the officers' titles. Never
  join `ownership_reporting_owners` row by row to sum amounts.
- `cusip_norm`, `issuer_lei_norm` and `put_call_norm` are join keys with placeholders made NULL;
  `other_manager_sequence_numbers` tokenizes the 13F `other_manager_ids` (`03,01` → `[3, 1]`) to join
  `form13f_other_managers.sequence_number` (`list_kind` `summary`).
- N-PORT `month1_end..month3_end` and `month_end` name the month of each return and flow, counted back from
  `as_of_date`.

## Shipped views

The views below, version `sec-views-v1`, are part of the SEC contract but are not materialized: every heuristic lives
here, not in the mapper, so improving one needs neither a rebuild nor a mapper change. `blocks/tests/sec_docs_sql.rs`
runs every SQL block of this section and of [Example queries](#example-queries) on a dataset built from the real
fixture filings, so they stay correct.

Register the tables first, one view per table. Replace `s3://…/sec` with the dataset root; an S3 root needs a
DuckDB secret, see [reading tables](../reading-tables.md).

```sql
CREATE OR REPLACE VIEW blocks AS SELECT * FROM delta_scan('s3://…/sec/blocks');
CREATE OR REPLACE VIEW filings AS SELECT * FROM delta_scan('s3://…/sec/filings');
CREATE OR REPLACE VIEW filing_raw_xml AS SELECT * FROM delta_scan('s3://…/sec/filing_raw_xml');
CREATE OR REPLACE VIEW filing_parties AS SELECT * FROM delta_scan('s3://…/sec/filing_parties');
CREATE OR REPLACE VIEW filing_documents AS SELECT * FROM delta_scan('s3://…/sec/filing_documents');
CREATE OR REPLACE VIEW filing_series AS SELECT * FROM delta_scan('s3://…/sec/filing_series');
CREATE OR REPLACE VIEW filing_series_classes AS SELECT * FROM delta_scan('s3://…/sec/filing_series_classes');
CREATE OR REPLACE VIEW filing_signatures AS SELECT * FROM delta_scan('s3://…/sec/filing_signatures');
CREATE OR REPLACE VIEW ownership_documents AS SELECT * FROM delta_scan('s3://…/sec/ownership_documents');
CREATE OR REPLACE VIEW ownership_reporting_owners AS SELECT * FROM delta_scan('s3://…/sec/ownership_reporting_owners');
CREATE OR REPLACE VIEW ownership_transactions AS SELECT * FROM delta_scan('s3://…/sec/ownership_transactions');
CREATE OR REPLACE VIEW ownership_holdings AS SELECT * FROM delta_scan('s3://…/sec/ownership_holdings');
CREATE OR REPLACE VIEW ownership_footnotes AS SELECT * FROM delta_scan('s3://…/sec/ownership_footnotes');
CREATE OR REPLACE VIEW form13f_reports AS SELECT * FROM delta_scan('s3://…/sec/form13f_reports');
CREATE OR REPLACE VIEW form13f_other_managers AS SELECT * FROM delta_scan('s3://…/sec/form13f_other_managers');
CREATE OR REPLACE VIEW form13f_holdings AS SELECT * FROM delta_scan('s3://…/sec/form13f_holdings');
CREATE OR REPLACE VIEW beneficial_reports AS SELECT * FROM delta_scan('s3://…/sec/beneficial_reports');
CREATE OR REPLACE VIEW beneficial_reporting_persons AS SELECT * FROM delta_scan('s3://…/sec/beneficial_reporting_persons');
CREATE OR REPLACE VIEW form144_notices AS SELECT * FROM delta_scan('s3://…/sec/form144_notices');
CREATE OR REPLACE VIEW form144_securities_information AS SELECT * FROM delta_scan('s3://…/sec/form144_securities_information');
CREATE OR REPLACE VIEW form144_securities_to_be_sold AS SELECT * FROM delta_scan('s3://…/sec/form144_securities_to_be_sold');
CREATE OR REPLACE VIEW form144_sales_past_3_months AS SELECT * FROM delta_scan('s3://…/sec/form144_sales_past_3_months');
CREATE OR REPLACE VIEW nport_reports AS SELECT * FROM delta_scan('s3://…/sec/nport_reports');
CREATE OR REPLACE VIEW nport_monthly_returns AS SELECT * FROM delta_scan('s3://…/sec/nport_monthly_returns');
CREATE OR REPLACE VIEW nport_monthly_activity AS SELECT * FROM delta_scan('s3://…/sec/nport_monthly_activity');
CREATE OR REPLACE VIEW nport_holdings AS SELECT * FROM delta_scan('s3://…/sec/nport_holdings');
CREATE OR REPLACE VIEW nport_debt_reference_instruments AS SELECT * FROM delta_scan('s3://…/sec/nport_debt_reference_instruments');
CREATE OR REPLACE VIEW nport_debt_conversion_currencies AS SELECT * FROM delta_scan('s3://…/sec/nport_debt_conversion_currencies');
CREATE OR REPLACE VIEW nport_derivatives AS SELECT * FROM delta_scan('s3://…/sec/nport_derivatives');
CREATE OR REPLACE VIEW nport_derivative_swap_legs AS SELECT * FROM delta_scan('s3://…/sec/nport_derivative_swap_legs');
CREATE OR REPLACE VIEW nport_derivative_index_components AS SELECT * FROM delta_scan('s3://…/sec/nport_derivative_index_components');
CREATE OR REPLACE VIEW form_d_notices AS SELECT * FROM delta_scan('s3://…/sec/form_d_notices');
CREATE OR REPLACE VIEW form_d_co_issuers AS SELECT * FROM delta_scan('s3://…/sec/form_d_co_issuers');
CREATE OR REPLACE VIEW form_d_related_persons AS SELECT * FROM delta_scan('s3://…/sec/form_d_related_persons');
CREATE OR REPLACE VIEW form_d_sales_recipients AS SELECT * FROM delta_scan('s3://…/sec/form_d_sales_recipients');
CREATE OR REPLACE VIEW npx_reports AS SELECT * FROM delta_scan('s3://…/sec/npx_reports');
CREATE OR REPLACE VIEW npx_votes AS SELECT * FROM delta_scan('s3://…/sec/npx_votes');
CREATE OR REPLACE VIEW npx_vote_records AS SELECT * FROM delta_scan('s3://…/sec/npx_vote_records');
CREATE OR REPLACE VIEW npx_other_managers AS SELECT * FROM delta_scan('s3://…/sec/npx_other_managers');
CREATE OR REPLACE VIEW ncen_reports AS SELECT * FROM delta_scan('s3://…/sec/ncen_reports');
CREATE OR REPLACE VIEW form_c_notices AS SELECT * FROM delta_scan('s3://…/sec/form_c_notices');
CREATE OR REPLACE VIEW form_c_co_issuers AS SELECT * FROM delta_scan('s3://…/sec/form_c_co_issuers');
CREATE OR REPLACE VIEW parse_issues AS SELECT * FROM delta_scan('s3://…/sec/parse_issues');
```

Then create the views and macros:

```sql
-- SEC shipped views and macros, version sec-views-v1 (docs/chains/sec.md "Shipped views").
-- Every heuristic lives here, not in the mapper: improving one needs no rebuild and no MAPPER_EPOCH bump.

-- 1. Exact dedup: the first dissemination of each accession. EDGAR re-disseminates old filings in later
--    daily feeds, so accession_number is not unique across blocks. Deletion notices are not candidates;
--    is_deleted says a later (or earlier) DELETION notice exists for the accession.
CREATE OR REPLACE VIEW sec_filings_first AS
WITH f AS (
    SELECT *,
           count(*) FILTER (WHERE NOT is_deletion_notice) OVER (PARTITION BY accession_number) AS dissemination_count,
           bool_or(is_deletion_notice) OVER (PARTITION BY accession_number) AS is_deleted,
           bool_or(list_contains(dissemination_flags, 'CORRECTION') AND NOT is_deletion_notice)
               OVER (PARTITION BY accession_number) AS is_corrected
    FROM filings
)
SELECT *, dissemination_count > 1 AS is_redisseminated
FROM f
WHERE NOT is_deletion_notice
QUALIFY row_number() OVER (PARTITION BY accession_number ORDER BY block_num, filing_index) = 1;

-- 2. 13F value units per filing. The filing-date rule (value_multiplier_rule, materialized) is overridden by the
--    median value per share of the filing's plain share rows, only with >= 5 qualifying rows.
CREATE OR REPLACE VIEW sec_13f_units AS
WITH q AS (
    SELECT block_num, filing_index,
           count(*) AS qualifying_rows,
           median(value::DOUBLE / shares_or_principal_amount) AS median_value_per_share
    FROM form13f_holdings
    WHERE shares_or_principal_type = 'SH' AND put_call_norm IS NULL
      AND shares_or_principal_amount > 0 AND value > 0
    GROUP BY ALL
)
SELECT r.block_num, r.filing_index, r.accession_number, r.value_multiplier_rule,
       coalesce(q.qualifying_rows, 0) AS qualifying_rows, q.median_value_per_share,
       CASE WHEN coalesce(q.qualifying_rows, 0) >= 5 AND r.value_multiplier_rule = 1000 AND q.median_value_per_share >= 2 THEN 1
            WHEN coalesce(q.qualifying_rows, 0) >= 5 AND r.value_multiplier_rule = 1 AND q.median_value_per_share < 0.5 THEN 1000
            ELSE r.value_multiplier_rule END AS value_multiplier,
       CASE WHEN coalesce(q.qualifying_rows, 0) < 5 THEN 'filing_date_rule'
            WHEN (r.value_multiplier_rule = 1000 AND q.median_value_per_share >= 2)
              OR (r.value_multiplier_rule = 1 AND q.median_value_per_share < 0.5) THEN 'median_override'
            WHEN q.median_value_per_share >= 0.5 AND q.median_value_per_share < 2 THEN 'ambiguous'
            ELSE 'filing_date_rule' END AS value_unit_source,
       r.table_value_total, r.holdings_value_sum,
       r.table_value_total * value_multiplier AS table_value_total_usd
FROM form13f_reports r
LEFT JOIN q USING (block_num, filing_index);

-- 3. 13F holdings in US dollars.
CREATE OR REPLACE VIEW sec_13f_holdings_usd AS
SELECT h.*, u.value_multiplier, u.value_unit_source,
       h.value * u.value_multiplier AS value_usd
FROM form13f_holdings h
JOIN sec_13f_units u USING (block_num, filing_index);

-- 4. The 13F reports that count for each (manager, quarter): the latest original or RESTATEMENT, plus the
--    NEW HOLDINGS amendments accepted after it. Notices are excluded; first disseminations only.
CREATE OR REPLACE VIEW sec_13f_effective_reports AS
WITH r AS (
    SELECT r.*
    FROM form13f_reports r
    SEMI JOIN sec_filings_first f USING (block_num, filing_index)
    WHERE r.report_type IN ('13F HOLDINGS REPORT', '13F COMBINATION REPORT') AND r.manager_cik IS NOT NULL
),
base AS (
    SELECT * FROM r
    WHERE coalesce(amendment_type, '') <> 'NEW HOLDINGS'
    QUALIFY row_number() OVER (PARTITION BY manager_cik, period_of_report
                               ORDER BY acceptance_datetime DESC, block_num DESC, filing_index DESC) = 1
)
SELECT block_num, filing_index, accession_number, manager_cik, period_of_report, 'base' AS effective_role FROM base
UNION ALL
SELECT r.block_num, r.filing_index, r.accession_number, r.manager_cik, r.period_of_report, 'new_holdings'
FROM r JOIN base b USING (manager_cik, period_of_report)
WHERE r.amendment_type = 'NEW HOLDINGS' AND r.acceptance_datetime > b.acceptance_datetime;

-- 5. N-PX how_voted normalization: every one of the 73 spellings seen on the sample days maps to one of
--    FOR, AGAINST, WITHHOLD, ABSTAIN, DID_NOT_VOTE, FREQUENCY_1Y/2Y/3Y, SPLIT, NONE, OTHER (NULL when empty).
CREATE OR REPLACE MACRO sec_hv_clean(h) AS upper(regexp_replace(trim(h), '\s+', ' ', 'g'));
CREATE OR REPLACE MACRO sec_how_voted_norm(h) AS CASE
    WHEN h IS NULL OR trim(h) = '' THEN NULL
    WHEN regexp_matches(sec_hv_clean(h), '(^|[^0-9])(1|ONE)[ -]?(YEAR|YR)') THEN 'FREQUENCY_1Y'
    WHEN regexp_matches(sec_hv_clean(h), '(^|[^0-9])(2|TWO)[ -]?(YEAR|YR)') THEN 'FREQUENCY_2Y'
    WHEN regexp_matches(sec_hv_clean(h), '(^|[^0-9])(3|THREE)[ -]?(YEAR|YR)') THEN 'FREQUENCY_3Y'
    WHEN sec_hv_clean(h) IN ('FOR', 'F', 'FOR ALL') THEN 'FOR'
    WHEN sec_hv_clean(h) IN ('AGAINST', 'AGANST') THEN 'AGAINST'
    WHEN sec_hv_clean(h) IN ('WITHHOLD', 'WITHHELD', 'WITHOLD') THEN 'WITHHOLD'
    WHEN sec_hv_clean(h) = 'ABSTAIN' THEN 'ABSTAIN'
    WHEN starts_with(sec_hv_clean(h), 'TAKE')
      OR sec_hv_clean(h) IN ('TNA', 'DO NOT VOTE', 'DID NOT VOTE', 'NO VOTE', 'NOT VOTED', 'UNVOTED', 'NON-VOTING') THEN 'DID_NOT_VOTE'
    WHEN sec_hv_clean(h) = 'SPLIT' THEN 'SPLIT'
    WHEN sec_hv_clean(h) IN ('NONE', 'N/A') THEN 'NONE'
    ELSE 'OTHER' END;

-- 6. Voted against management: TRUE/FALSE only when both the vote and the recommendation are decisive.
CREATE OR REPLACE MACRO sec_voted_against_management(h, mgmt) AS CASE
    WHEN sec_how_voted_norm(h) IN ('FOR', 'AGAINST', 'WITHHOLD') AND upper(trim(mgmt)) IN ('FOR', 'AGAINST')
        THEN (sec_how_voted_norm(h) = 'FOR') <> (upper(trim(mgmt)) = 'FOR')
    END;

CREATE OR REPLACE VIEW sec_npx_vote_records_norm AS
SELECT *, sec_how_voted_norm(how_voted) AS how_voted_norm,
       sec_voted_against_management(how_voted, management_recommendation) AS voted_against_management
FROM npx_vote_records;

-- 7. The N-PORT report that counts for each fund series and as-of date (NPORT-P/A restates).
CREATE OR REPLACE VIEW sec_nport_effective_reports AS
SELECT r.*
FROM nport_reports r
SEMI JOIN sec_filings_first f USING (block_num, filing_index)
QUALIFY row_number() OVER (PARTITION BY r.filer_cik, coalesce(r.series_id, r.series_name), r.as_of_date
                           ORDER BY r.acceptance_datetime DESC, r.block_num DESC, r.filing_index DESC) = 1;
```

| View | What it decides |
|---|---|
| `sec_filings_first` | One row per accession: its first dissemination, never a deletion notice, with `dissemination_count`, `is_redisseminated`, `is_deleted` and `is_corrected`. |
| `sec_13f_units` | Each 13F report's `value_multiplier`: the date rule, unless ≥ 5 plain share rows with a positive value give a median value per share that contradicts it (`median_override`); `ambiguous` when that median is between 0.5 and 2. A one-holding report keeps the date rule. |
| `sec_13f_holdings_usd` | 13F holdings with `value_usd`. |
| `sec_13f_effective_reports` | The reports that count for each manager and quarter: the latest original or `RESTATEMENT`, chosen before any CUSIP filter so a restatement that drops a position wins, plus the `NEW HOLDINGS` amendments accepted after it. Notices excluded; first disseminations only. |
| `sec_how_voted_norm(h)`, `sec_npx_vote_records_norm` | N-PX `how_voted` in one of `FOR`, `AGAINST`, `WITHHOLD`, `ABSTAIN`, `DID_NOT_VOTE`, `FREQUENCY_1Y`/`2Y`/`3Y`, `SPLIT`, `NONE`, `OTHER` (NULL when empty); every spelling of the sample days gets a label. |
| `sec_voted_against_management(h, mgmt)` | TRUE or FALSE only when both the vote and the recommendation are decisive, else NULL: abstentions and frequency votes stay out of a denominator. |
| `sec_nport_effective_reports` | The N-PORT report that counts for each fund series and as-of date (an `NPORT-P/A` restates). |

## Example queries

All examples read the views of [Shipped views](#shipped-views). The results quoted are from the four 0.13.0 sample
feed days (2014-08-11, 2026-03-16, 2026-08-14 and 2026-08-28).

Form 4 open-market purchases by officers, with their dollar value. On the samples, 143 officer `P` rows; the 4 with a
footnote-only price keep `value_usd` NULL, and joint filings list every owner but only the officers' titles:

```sql
-- Q1. Form 4 open-market purchases by officers, with dollar value.
SELECT t.issuer_trading_symbol, t.issuer_name, t.owner_names, t.officer_titles,
       t.transaction_date, t.shares, t.price_per_share, t.value_usd, t.aff_10b5_one, t.accession_number
FROM ownership_transactions t
SEMI JOIN sec_filings_first f USING (block_num, filing_index)
WHERE t.form_type IN ('4', '4/A')
  AND t.is_open_market AND t.transaction_code = 'P'
  AND t.any_owner_is_officer
ORDER BY t.value_usd DESC NULLS LAST
LIMIT 10;
```

13F holders of one CUSIP (Apple) per quarter, in US dollars. For 2026-06-30, 614 managers report 1.93 B shares worth
$556.3 B; quarters with a handful of managers are late amendments of old periods:

```sql
-- Q2. 13F holders of one CUSIP across managers, per quarter, in USD.
SELECT h.period_of_report AS quarter,
       count(DISTINCT h.manager_cik)                                   AS managers,
       sum(h.shares_or_principal_amount)                               AS shares,
       sum(h.value_usd)                                                AS value_usd,
       count(*) FILTER (WHERE h.value_unit_source <> 'filing_date_rule') AS rows_unit_override_or_ambiguous
FROM sec_13f_holdings_usd h
SEMI JOIN sec_13f_effective_reports e USING (block_num, filing_index)
WHERE h.cusip_norm = '037833100'
  AND h.put_call_norm IS NULL
  AND h.shares_or_principal_type = 'SH'
GROUP BY ALL
ORDER BY quarter;
```

N-PX: who votes against management on say-on-pay. Proxy year 2026 has 682,744 say-on-pay vote records, and the
50,059 undecidable ones (abstain, did not vote, frequency votes, no recommendation) stay out of the denominator:

```sql
-- Q3. N-PX: who votes against management on say-on-pay.
SELECT v.filer_cik, any_value(r.reporting_person_name) AS reporting_person,
       count(*) FILTER (WHERE v.voted_against_management)             AS against_mgmt,
       count(*) FILTER (WHERE v.voted_against_management IS NOT NULL) AS decided,
       round(100.0 * against_mgmt / nullif(decided, 0), 2)            AS pct_against
FROM sec_npx_vote_records_norm v
SEMI JOIN sec_filings_first f USING (block_num, filing_index)
JOIN npx_reports r USING (block_num, filing_index)
WHERE v.report_calendar_year = 2026
  AND list_contains(v.vote_categories, 'SECTION 14A SAY-ON-PAY VOTES')
GROUP BY v.filer_cik
HAVING decided >= 100
ORDER BY pct_against DESC
LIMIT 10;
```

N-PORT: the funds holding a ticker (NVDA common stock) at a month end. `ticker` alone finds only a third of the
positions, so the query resolves the ISIN and CUSIP too, and keeps common equity counted in shares, which leaves bonds
and derivatives out:

```sql
-- Q4. N-PORT: funds holding a ticker (NVDA common stock) at a month end.
WITH ids AS (
    SELECT DISTINCT isin FROM nport_holdings
    WHERE upper(ticker) = 'NVDA' AND asset_category = 'EC' AND isin IS NOT NULL AND isin <> 'N/A'
)
SELECT h.as_of_date, h.registrant_name, h.series_name, h.series_id,
       h.balance AS shares, h.value_usd, h.pct_value, h.accession_number
FROM nport_holdings h
SEMI JOIN sec_nport_effective_reports e USING (block_num, filing_index)
WHERE h.as_of_date = DATE '2026-06-30'
  AND h.asset_category = 'EC' AND h.units = 'NS' AND NOT h.has_derivative
  AND (upper(h.ticker) = 'NVDA' OR h.isin IN (SELECT isin FROM ids) OR h.cusip_norm = '67066G104')
ORDER BY h.value_usd DESC
LIMIT 10;
```

Form D: new-offering raise totals per industry and month (in 2026-08, 185 pooled-fund offerings sold $5.77 B, 75 of
them with an `Indefinite` target):

```sql
-- Q5. Form D: new-offering raise totals per industry and month.
SELECT d.industry_group,
       date_trunc('month', d.filing_date)                                 AS month,
       count(*)                                                           AS new_offerings,
       sum(d.total_offering_amount)                                       AS offering_target_usd,
       count(*) FILTER (WHERE d.total_offering_amount_is_indefinite)      AS indefinite_targets,
       sum(d.total_amount_sold)                                           AS amount_sold_usd,
       count(*) FILTER (WHERE list_contains(d.federal_exemptions, '06c')) AS rule_506c
FROM form_d_notices d
SEMI JOIN sec_filings_first f USING (block_num, filing_index)
WHERE d.form_type = 'D'
GROUP BY ALL
ORDER BY month DESC, amount_sold_usd DESC NULLS LAST
LIMIT 12;
```

The source text of every rounded N-PORT percentage, next to its typed value:

```sql
-- Q6. The source text of every rounded N-PORT percentage, next to its typed value.
SELECT h.accession_number, h.series_name, h.issuer_name, h.pct_value, p.raw_value
FROM nport_holdings h
JOIN parse_issues p
  ON p.block_num = h.block_num AND p.filing_index = h.filing_index
 AND p.table_name = 'nport_holdings' AND p.index_1 = h.holding_index
 AND p.column_name = 'pct_value' AND p.issue = 'rounded'
WHERE h.has_parse_issues
ORDER BY h.block_num, h.filing_index, h.holding_index;
```

## Legends

### Form 3/4/5 transaction codes

`ownership_transactions.transaction_code`, from the SEC's Form 4 instructions. `is_open_market` is a non-derivative
`P` or `S`; per the legend, both also cover private transactions.

| Code | Meaning |
|---|---|
| `P` | Open market or private purchase |
| `S` | Open market or private sale |
| `V` | Transaction voluntarily reported earlier than required |
| `A` | Grant, award or other acquisition under Rule 16b-3(d) |
| `D` | Disposition to the issuer under Rule 16b-3(e) |
| `F` | Payment of an exercise price or tax liability by delivering or withholding securities |
| `I` | Discretionary transaction under Rule 16b-3(f) |
| `M` | Exercise or conversion of a derivative security exempted under Rule 16b-3 |
| `C` | Conversion of a derivative security |
| `E` | Expiration of a short derivative position |
| `H` | Expiration or cancellation of a long derivative position with value received |
| `O` | Exercise of an out-of-the-money derivative security |
| `X` | Exercise of an in-the-money or at-the-money derivative security |
| `G` | Bona fide gift |
| `L` | Small acquisition under Rule 16a-6 |
| `W` | Acquisition or disposition by will or the laws of descent and distribution |
| `Z` | Deposit into or withdrawal from a voting trust |
| `J` | Other acquisition or disposition (described in a footnote) |
| `K` | Transaction in an equity swap or similar instrument |
| `U` | Disposition in a tender of shares in a change-of-control transaction |

`acquired_disposed_code` is `A` (acquired) or `D` (disposed); `direct_or_indirect` is `D` (direct) or `I` (indirect,
with `nature_of_ownership`); `transaction_timeliness` is `E` (early) or `L` (late), NULL when on time.

### Other codes

| Column | Values |
|---|---|
| `filings.body_kind` | `ownership` (Forms 3/4/5), `form13f`, `beneficial` (13D/G), `form144`, `nport`, `form_d`, `npx`, `ncen`, `form_c`, `raw` (no structured body) |
| `filings.raw_reason` | `no_xml`, `legacy_html`, `parse_error` (with `raw_detail`), `deletion`, `unsupported_form` |
| `filings.cik_role` | the header party of `cik`: `ISSUER` (3/4/5), `SUBJECT-COMPANY` (13D/G, 144), `FILER` (13F, N-PORT, N-PX, N-CEN, D, C) |
| `filings.dissemination_flags` | `CORRECTION`, `DELETION`, `PAPER`, `CONFIRMING-COPY`, `PRIVATE-TO-PUBLIC` |
| `filing_signatures.signature_source` | `ownership`, `form13f`, `beneficial`, `nport`, `form_d`, `npx`, `form_c_issuer`, `form_c_person` |
| `form13f_reports.report_type` | `13F HOLDINGS REPORT`, `13F NOTICE`, `13F COMBINATION REPORT`; `amendment_type` `RESTATEMENT` or `NEW HOLDINGS` |
| `form13f_holdings` | `shares_or_principal_type` `SH` (shares) or `PRN` (principal); `put_call_norm` `PUT`, `CALL` or NULL (the position itself); `investment_discretion` `SOLE`, `DFND` (shared-defined) or `OTR` (shared-other) |
| `form13f_other_managers.list_kind`, `npx_other_managers.list_kind` | `cover` (managers reporting for this filer) or `summary` (included managers, the targets of sequence and serial numbers) |
| `beneficial_reports.schedule_kind` | `13D` (active intent) or `13G` (passive) |
| `nport_holdings.asset_category` | `EC` common equity, `EP` preferred equity, `DBT` debt, `LON` loan, `ABS-MBS`/`ABS-ASBS`/`ABS-CBDO`/`ABS-APCP`/`ABS-O` asset-backed securities, `RA` repurchase agreement, `SN` structured note, `STIV` short-term investment vehicle, `RE` real estate, `COMM` commodity, `DE`/`DIR`/`DCR`/`DFE`/`DCO`/`DO` equity, interest-rate, credit, foreign-exchange, commodity and other derivatives, `OTHER` (with `asset_category_description`) |
| `nport_holdings.units` | `NS` number of shares, `PA` principal amount, `NC` number of contracts, `OU` other units |
| `nport_derivatives.category` | `FWD`, `FUT`, `OPT`, `SWP`, `SWO` (swaption), `WAR`, `OTH`; `nesting_level` 0 is the holding's own derivative, 1 the one nested in it, and so on |
| `parse_issues.issue` | see [Parse issues](#parse-issues) |

## Rebuilds and schema changes

When a later firesec release adds protobuf fields and fireparq maps them, the SEC table schemas change: an existing
SEC root then refuses to resume and needs an SEC-only rebuild into a new root. A change to the shipped views never
needs one. Blocks produced by firesec
before 0.13.0 map, but leave the 0.13.0 fields empty (`parties` is empty, so `issuer_*` and `filer_*` are NULL).
`filing_raw_xml` gets rows only when firesec runs with `--include-raw`.
