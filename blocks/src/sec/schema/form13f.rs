//! Table specs generated from the final specification §3 (`final-spec.md`).
//! Edit only to fix a divergence from the specification; regenerate
//! `docs/schemas/sec.md` and re-pin the chain schema digests afterwards.

#[allow(unused_imports)]
use super::{Col, Family, Member, TableSpec, Ty};

/// §3.14 `form13f_reports`.
pub(crate) const FORM13F_REPORTS: TableSpec = TableSpec {
    name: super::FORM13F_REPORTS,
    doc: "13F cover and summary pages; the signature is in `filing_signatures`.",
    filing_context: true,
    cols: &[
        Col::new(
            "has_cover_page",
            Ty::Boolean,
            false,
            "Derived from `Filing.body.form13f.cover_page`: `cover_page` present. False for pre-2013 paper-era bodies.",
        ),
        Col::new(
            "manager_cik",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.cover_page.filing_manager.cik`. = filings.cik when present.",
        ),
        Col::new(
            "manager_name",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.cover_page.filing_manager.name`.",
        ),
        Col::new(
            "manager_street1",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.cover_page.filing_manager.address.street1`.",
        ),
        Col::new(
            "manager_street2",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.cover_page.filing_manager.address.street2`.",
        ),
        Col::new(
            "manager_city",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.cover_page.filing_manager.address.city`.",
        ),
        Col::new(
            "manager_state",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.cover_page.filing_manager.address.state`. EDGAR state/country code.",
        ),
        Col::new(
            "manager_zip_code",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.cover_page.filing_manager.address.zip_code`. Text (keeps leading zeros).",
        ),
        Col::new(
            "manager_state_description",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.cover_page.filing_manager.address.state_description`. Filled only by Forms 3/4/5 and D.",
        ),
        Col::new(
            "manager_country",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.cover_page.filing_manager.address.country`.",
        ),
        Col::new(
            "manager_non_us_state_territory",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.cover_page.filing_manager.address.non_us_state_territory`.",
        ),
        Col::new(
            "report_type",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.cover_page.report_type`. `13F HOLDINGS REPORT` / `13F NOTICE` / `13F COMBINATION REPORT`.",
        ),
        Col::new(
            "form13f_file_number",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.cover_page.form13f_file_number`. The manager's own `028-` number.",
        ),
        Col::new(
            "crd_number",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.cover_page.crd_number`. Text (leading zeros).",
        ),
        Col::new(
            "sec_file_number",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.cover_page.sec_file_number`.",
        ),
        Col::new(
            "period_of_report",
            Ty::Date32,
            true,
            "`Filing.body.form13f.cover_page.period_of_report`: date (§4.2). Quarter end.",
        ),
        Col::new(
            "cover_is_amendment",
            Ty::Boolean,
            true,
            "`Filing.body.form13f.cover_page.is_amendment`: proto bool; NULL when `cover_page` is absent.",
        ),
        Col::new(
            "amendment_type",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.cover_page.amendment_type`. `RESTATEMENT` replaces, `NEW HOLDINGS` adds.",
        ),
        Col::new(
            "amendment_number",
            Ty::Int32,
            true,
            "`Filing.body.form13f.cover_page.amendment_number`: integer (§4.3), Int32.",
        ),
        Col::new(
            "provide_info_for_instruction5",
            Ty::Boolean,
            true,
            "`Filing.body.form13f.cover_page.provide_info_for_instruction5`: Y/N text → bool (§4.1).",
        ),
        Col::new(
            "additional_information",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.cover_page.additional_information`. Free text.",
        ),
        Col::new(
            "has_summary_page",
            Ty::Boolean,
            false,
            "Derived from `Filing.body.form13f.summary_page`: `summary_page` present. False on 13F-NT.",
        ),
        Col::new(
            "other_included_managers_count",
            Ty::UInt32,
            true,
            "`Filing.body.form13f.summary_page.other_included_managers_count`: proto uint32; NULL when `summary_page` is absent.",
        ),
        Col::new(
            "table_entry_total",
            Ty::UInt32,
            true,
            "`Filing.body.form13f.summary_page.table_entry_total`: proto uint32; NULL when `summary_page` is absent. Declared number of holdings.",
        ),
        Col::new(
            "table_value_total",
            Ty::Int64,
            true,
            "`Filing.body.form13f.summary_page.table_value_total`: integer (§4.3), Int64. RAW units like `form13f_holdings.value`.",
        ),
        Col::new(
            "is_confidential_omitted",
            Ty::Boolean,
            true,
            "`Filing.body.form13f.summary_page.is_confidential_omitted`: optional bool: NULL when unset.",
        ),
        Col::new(
            "value_multiplier_rule",
            Ty::Int32,
            false,
            "Derived from `Filing.filing_date`: 1000 if `filing_date < 2023-01-03` (else the `date` partition when filing_date is NULL), else 1. SEC cutover rule only; USD with the override is the `sec_13f_units` view (§7.0).",
        ),
        Col::new(
            "holdings_count",
            Ty::UInt32,
            false,
            "Derived from `Filing.body.form13f.holdings[]`: length of the list.",
        ),
        Col::new(
            "holdings_value_sum",
            Ty::Int64,
            true,
            "Derived from `Filing.body.form13f.holdings[].value`: Σ typed `value`, checked `i64`; NULL when there are no holdings (not 0), when any `value` is NULL, or on overflow (+ `overflow` issue). RAW units; QA against `table_value_total`.",
        ),
        Col::new(
            "holdings_complete",
            Ty::Boolean,
            true,
            "Derived: `holdings_count = table_entry_total`; NULL without summary page. QA: 13F-HR filings whose information table is short.",
        ),
        Col::new(
            "cover_other_manager_count",
            Ty::UInt32,
            false,
            "Derived from `Filing.body.form13f.cover_page.other_managers[]`: length of the list.",
        ),
        Col::new(
            "summary_other_manager_count",
            Ty::UInt32,
            false,
            "Derived from `Filing.body.form13f.summary_page.other_managers[]`: length of the list.",
        ),
        Col::new(
            "has_parse_issues",
            Ty::Boolean,
            false,
            "Derived: true iff this row wrote ≥1 `parse_issues` row (§4.6).",
        ),
    ],
};

/// §3.15 `form13f_other_managers`.
pub(crate) const FORM13F_OTHER_MANAGERS: TableSpec = TableSpec {
    name: super::FORM13F_OTHER_MANAGERS,
    doc: "Other managers listed by a 13F, with the real EDGAR sequence number (0.13.0+).",
    filing_context: true,
    cols: &[
        Col::new(
            "manager_cik",
            Ty::Utf8,
            true,
            "Copy of `form13f_reports.manager_cik`.",
        ),
        Col::new(
            "period_of_report",
            Ty::Date32,
            true,
            "Copy of `form13f_reports.period_of_report`.",
        ),
        Col::new(
            "other_manager_index",
            Ty::UInt32,
            false,
            "Derived: 0-based over cover-page managers then summary-page managers.",
        ),
        Col::new(
            "list_kind",
            Ty::Dictionary,
            false,
            "Derived: `cover` (coverPage/otherManagersInfo) or `summary` (summaryPage/otherManagers2Info). Cover = managers reporting for this filer (13F-NT/combination); summary = included managers.",
        ),
        Col::new(
            "sequence_number",
            Ty::Int32,
            true,
            "`Filing.body.form13f.{cover_page,summary_page}.other_managers[].sequence_number`: integer (§4.3), Int32. Summary page only (empty on cover); the key `other_manager_sequence_numbers` refer to.",
        ),
        Col::new(
            "other_manager_cik",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.{cover_page,summary_page}.other_managers[].cik`.",
        ),
        Col::new(
            "other_manager_name",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.{cover_page,summary_page}.other_managers[].name`.",
        ),
        Col::new(
            "form13f_file_number",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.{cover_page,summary_page}.other_managers[].form13f_file_number`.",
        ),
        Col::new(
            "crd_number",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.{cover_page,summary_page}.other_managers[].crd_number`.",
        ),
        Col::new(
            "sec_file_number",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.{cover_page,summary_page}.other_managers[].sec_file_number`.",
        ),
        Col::new(
            "has_parse_issues",
            Ty::Boolean,
            false,
            "Derived: true iff this row wrote ≥1 `parse_issues` row (§4.6).",
        ),
    ],
};

/// §3.16 `form13f_holdings`.
pub(crate) const FORM13F_HOLDINGS: TableSpec = TableSpec {
    name: super::FORM13F_HOLDINGS,
    doc: "13F holdings. `value` is raw; the shipped view `sec_13f_holdings_usd` gives USD.",
    filing_context: true,
    cols: &[
        Col::new(
            "manager_cik",
            Ty::Utf8,
            true,
            "Copy of `form13f_reports.manager_cik`.",
        ),
        Col::new(
            "manager_name",
            Ty::Utf8,
            true,
            "Copy of `form13f_reports.manager_name`.",
        ),
        Col::new(
            "period_of_report",
            Ty::Date32,
            true,
            "Copy of `form13f_reports.period_of_report`.",
        ),
        Col::new(
            "report_type",
            Ty::Utf8,
            true,
            "Copy of `form13f_reports.report_type`.",
        ),
        Col::new(
            "amendment_type",
            Ty::Utf8,
            true,
            "Copy of `form13f_reports.amendment_type`.",
        ),
        Col::new(
            "value_multiplier_rule",
            Ty::Int32,
            false,
            "Copy of `form13f_reports.value_multiplier_rule`.",
        ),
        Col::new(
            "holding_index",
            Ty::UInt32,
            false,
            "Derived: position in `holdings`.",
        ),
        Col::new(
            "issuer_name",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.holdings[].name_of_issuer`. Proto `name_of_issuer` (renamed to match N-PORT and N-PX).",
        ),
        Col::new(
            "title_of_class",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.holdings[].title_of_class`.",
        ),
        Col::new(
            "cusip",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.holdings[].cusip`. As filed (case varies).",
        ),
        Col::new(
            "cusip_norm",
            Ty::Utf8,
            true,
            "Derived from `Filing.body.form13f.holdings[].cusip`: §4.4 `cusip_norm`. Cross-form join key.",
        ),
        Col::new(
            "figi",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.holdings[].figi`.",
        ),
        Col::new(
            "value",
            Ty::Int64,
            true,
            "`Filing.body.form13f.holdings[].value`: integer (§4.3), Int64. RAW: thousands before 2023-01-03, dollars after; USD via `sec_13f_holdings_usd`.",
        ),
        Col::new(
            "shares_or_principal_amount",
            Ty::Int64,
            true,
            "`Filing.body.form13f.holdings[].shares_or_principal.amount`: integer (§4.3), Int64. `sshPrnamt`.",
        ),
        Col::new(
            "shares_or_principal_type",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.holdings[].shares_or_principal.type`. `SH` / `PRN`.",
        ),
        Col::new(
            "put_call",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.holdings[].put_call`. As filed: `Put` / `Call`.",
        ),
        Col::new(
            "put_call_norm",
            Ty::Dictionary,
            true,
            "Derived from `Filing.body.form13f.holdings[].put_call`: upper(trim) ∈ {PUT, CALL}, else NULL. NULL = the position itself.",
        ),
        Col::new(
            "investment_discretion",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.holdings[].investment_discretion`. `SOLE` / `DFND` / `OTR`.",
        ),
        Col::new(
            "other_manager_ids",
            Ty::ListUtf8,
            false,
            "`Filing.body.form13f.holdings[].other_manager_ids[]`: verbatim items in document order; [] when empty. As filed: `1,5,6`, `01`, `NONE`.",
        ),
        Col::new(
            "other_manager_sequence_numbers",
            Ty::ListInt32,
            false,
            "Derived from `Filing.body.form13f.holdings[].other_manager_ids[]`: §4.4 tokenizer. Join `form13f_other_managers.sequence_number` (list_kind = summary).",
        ),
        Col::new(
            "voting_authority_sole",
            Ty::Int64,
            true,
            "`Filing.body.form13f.holdings[].voting_authority.sole`: integer (§4.3), Int64.",
        ),
        Col::new(
            "voting_authority_shared",
            Ty::Int64,
            true,
            "`Filing.body.form13f.holdings[].voting_authority.shared`: integer (§4.3), Int64.",
        ),
        Col::new(
            "voting_authority_none",
            Ty::Int64,
            true,
            "`Filing.body.form13f.holdings[].voting_authority.none`: integer (§4.3), Int64.",
        ),
        Col::new(
            "has_parse_issues",
            Ty::Boolean,
            false,
            "Derived: true iff this row wrote ≥1 `parse_issues` row (§4.6).",
        ),
    ],
};
