//! Table specs generated from the final specification §3 (`final-spec.md`).
//! Edit only to fix a divergence from the specification; regenerate
//! `docs/schemas/sec.md` and re-pin the chain schema digests afterwards.

#[allow(unused_imports)]
use super::{Col, Family, Member, TableSpec, Ty};

/// §3.36 `npx_reports`.
pub(crate) const NPX_REPORTS: TableSpec = TableSpec {
    name: super::NPX_REPORTS,
    doc: "N-PX cover page. Signatures are in `filing_signatures`, other managers in `npx_other_managers`.",
    filing_context: true,
    cols: &[
        Col::new(
            "filer_cik",
            Ty::Utf8,
            true,
            "`Filing.body.npx.filer_cik`. Fund registrant or institutional manager.",
        ),
        Col::new(
            "has_cover_page",
            Ty::Boolean,
            false,
            "Derived from `Filing.body.npx.cover_page`: `cover_page` present.",
        ),
        Col::new(
            "registrant_type",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.registrant_type`. `RMIC` fund / `IM` institutional manager.",
        ),
        Col::new(
            "investment_company_type",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.investment_company_type`. `N-1A`, `N-2`.",
        ),
        Col::new(
            "year_or_quarter",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.year_or_quarter`.",
        ),
        Col::new(
            "report_calendar_year",
            Ty::Int32,
            true,
            "`Filing.body.npx.cover_page.report_calendar_year`: integer (§4.3), Int32. Proxy year (July–June).",
        ),
        Col::new(
            "report_type",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.report_type`. `FUND VOTING REPORT`, `INSTITUTIONAL MANAGER NOTICE REPORT`, …",
        ),
        Col::new(
            "reporting_person_name",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.reporting_person_name`.",
        ),
        Col::new(
            "reporting_person_street1",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.reporting_person_address.street1`.",
        ),
        Col::new(
            "reporting_person_street2",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.reporting_person_address.street2`.",
        ),
        Col::new(
            "reporting_person_city",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.reporting_person_address.city`.",
        ),
        Col::new(
            "reporting_person_state",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.reporting_person_address.state`. EDGAR state/country code.",
        ),
        Col::new(
            "reporting_person_zip_code",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.reporting_person_address.zip_code`. Text (keeps leading zeros).",
        ),
        Col::new(
            "reporting_person_state_description",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.reporting_person_address.state_description`. Filled only by Forms 3/4/5 and D.",
        ),
        Col::new(
            "reporting_person_country",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.reporting_person_address.country`.",
        ),
        Col::new(
            "reporting_person_non_us_state_territory",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.reporting_person_address.non_us_state_territory`.",
        ),
        Col::new(
            "reporting_person_phone",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.reporting_person_phone`.",
        ),
        Col::new(
            "file_number",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.file_number`. `811-` funds, `028-` managers.",
        ),
        Col::new(
            "reporting_crd_number",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.reporting_crd_number`.",
        ),
        Col::new(
            "reporting_sec_file_number",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.reporting_sec_file_number`.",
        ),
        Col::new(
            "lei_number",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.lei_number`.",
        ),
        Col::new(
            "confidential_treatment",
            Ty::Boolean,
            true,
            "`Filing.body.npx.cover_page.confidential_treatment`: Y/N text → bool (§4.1).",
        ),
        Col::new(
            "notice_explanation",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.notice_explanation`.",
        ),
        Col::new(
            "explanatory_choice",
            Ty::Boolean,
            true,
            "`Filing.body.npx.cover_page.explanatory_choice`: Y/N text → bool (§4.1).",
        ),
        Col::new(
            "explanatory_notes",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.explanatory_notes`.",
        ),
        Col::new(
            "cover_is_amendment",
            Ty::Boolean,
            true,
            "`Filing.body.npx.cover_page.is_amendment`: optional bool: NULL when unset.",
        ),
        Col::new(
            "amendment_number",
            Ty::Int32,
            true,
            "`Filing.body.npx.cover_page.amendment_number`: integer (§4.3), Int32.",
        ),
        Col::new(
            "amendment_type",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.amendment_type`. `RESTATEMENT` / `NEW PROXY`.",
        ),
        Col::new(
            "conf_denied_expired",
            Ty::Boolean,
            true,
            "`Filing.body.npx.cover_page.conf_denied_expired`: optional bool: NULL when unset.",
        ),
        Col::new(
            "agent_for_service_name",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.agent_for_service_name`.",
        ),
        Col::new(
            "agent_for_service_street1",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.agent_for_service_address.street1`.",
        ),
        Col::new(
            "agent_for_service_street2",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.agent_for_service_address.street2`.",
        ),
        Col::new(
            "agent_for_service_city",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.agent_for_service_address.city`.",
        ),
        Col::new(
            "agent_for_service_state",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.agent_for_service_address.state`. EDGAR state/country code.",
        ),
        Col::new(
            "agent_for_service_zip_code",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.agent_for_service_address.zip_code`. Text (keeps leading zeros).",
        ),
        Col::new(
            "agent_for_service_state_description",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.agent_for_service_address.state_description`. Filled only by Forms 3/4/5 and D.",
        ),
        Col::new(
            "agent_for_service_country",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.agent_for_service_address.country`.",
        ),
        Col::new(
            "agent_for_service_non_us_state_territory",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.agent_for_service_address.non_us_state_territory`.",
        ),
        Col::new(
            "series_reports",
            Ty::ListStruct(&[("series_id", Member::Utf8), ("series_name", Member::Utf8), ("series_lei", Member::Utf8)]),
            false,
            "`Filing.body.npx.cover_page.series_reports[]`: one struct per element {series_id ← .series_id, series_name ← .name, series_lei ← .lei} ('' → NULL); [] when empty. Series covered by a fund report.",
        ),
        Col::new(
            "other_included_managers_count",
            Ty::Int32,
            true,
            "`Filing.body.npx.cover_page.other_included_managers_count`: integer (§4.3), Int32.",
        ),
        Col::new(
            "declared_series_count",
            Ty::Int32,
            true,
            "`Filing.body.npx.cover_page.series_count`: integer (§4.3), Int32. Declared `seriesPage/seriesCount` (renamed: `series_count` is a list length on `filings`/`ncen_reports`).",
        ),
        Col::new(
            "vote_count",
            Ty::UInt32,
            false,
            "Derived from `Filing.body.npx.votes[]`: length of the list. Up to ~200k.",
        ),
        Col::new(
            "vote_record_count",
            Ty::UInt32,
            false,
            "Derived from `Filing.body.npx.votes[].records[]`: Σ len(records).",
        ),
        Col::new(
            "other_manager_count",
            Ty::UInt32,
            false,
            "Derived: rows written to `npx_other_managers`.",
        ),
        Col::new(
            "has_parse_issues",
            Ty::Boolean,
            false,
            "Derived: true iff this row wrote ≥1 `parse_issues` row (§4.6).",
        ),
    ],
};

/// §3.37 `npx_votes`.
pub(crate) const NPX_VOTES: TableSpec = TableSpec {
    name: super::NPX_VOTES,
    doc: "N-PX proposals voted, with identifiers and categories.",
    filing_context: true,
    cols: &[
        Col::new(
            "filer_cik",
            Ty::Utf8,
            true,
            "Copy of `npx_reports.filer_cik`.",
        ),
        Col::new(
            "registrant_type",
            Ty::Utf8,
            true,
            "Copy of `npx_reports.registrant_type`.",
        ),
        Col::new(
            "report_calendar_year",
            Ty::Int32,
            true,
            "Copy of `npx_reports.report_calendar_year`.",
        ),
        Col::new(
            "vote_index",
            Ty::UInt32,
            false,
            "Derived: position in `votes`.",
        ),
        Col::new(
            "issuer_name",
            Ty::Utf8,
            true,
            "`Filing.body.npx.votes[].issuer_name`.",
        ),
        Col::new(
            "cusip",
            Ty::Utf8,
            true,
            "`Filing.body.npx.votes[].cusip`.",
        ),
        Col::new(
            "cusip_norm",
            Ty::Utf8,
            true,
            "Derived from `Filing.body.npx.votes[].cusip`: §4.4 `cusip_norm`.",
        ),
        Col::new(
            "isin",
            Ty::Utf8,
            true,
            "`Filing.body.npx.votes[].isin`.",
        ),
        Col::new(
            "figi",
            Ty::Utf8,
            true,
            "`Filing.body.npx.votes[].figi`. Rare; junk `-`.",
        ),
        Col::new(
            "meeting_date",
            Ty::Date32,
            true,
            "`Filing.body.npx.votes[].meeting_date`: date (§4.2).",
        ),
        Col::new(
            "vote_description",
            Ty::Utf8,
            true,
            "`Filing.body.npx.votes[].vote_description`. Proposal text, up to ~4.8k chars.",
        ),
        Col::new(
            "other_vote_description",
            Ty::Utf8,
            true,
            "`Filing.body.npx.votes[].other_vote_description`.",
        ),
        Col::new(
            "vote_categories",
            Ty::ListUtf8,
            false,
            "`Filing.body.npx.votes[].vote_categories[]`: verbatim items in document order; [] when empty. `DIRECTOR ELECTIONS`, `SECTION 14A SAY-ON-PAY VOTES`, …",
        ),
        Col::new(
            "vote_source",
            Ty::Utf8,
            true,
            "`Filing.body.npx.votes[].vote_source`. `ISSUER` (management) / `SECURITY HOLDER` (shareholder proposal).",
        ),
        Col::new(
            "vote_series",
            Ty::Utf8,
            true,
            "`Filing.body.npx.votes[].vote_series`. Fund series (`S000…`) that cast the vote.",
        ),
        Col::new(
            "shares_voted",
            Ty::Decimal(Family::S16),
            true,
            "`Filing.body.npx.votes[].shares_voted`: decimal s=16 (§4.3, S16).",
        ),
        Col::new(
            "shares_on_loan",
            Ty::Decimal(Family::S16),
            true,
            "`Filing.body.npx.votes[].shares_on_loan`: decimal s=16 (§4.3, S16).",
        ),
        Col::new(
            "vote_other_managers",
            Ty::ListInt32,
            false,
            "`Filing.body.npx.votes[].vote_other_managers[]`: each item: integer (§4.3), Int32; [] when empty. Serial numbers → `npx_other_managers.serial_number`.",
        ),
        Col::new(
            "vote_other_info",
            Ty::Utf8,
            true,
            "`Filing.body.npx.votes[].vote_other_info`.",
        ),
        Col::new(
            "record_count",
            Ty::UInt32,
            false,
            "Derived from `Filing.body.npx.votes[].records[]`: length of the list. >1 = split vote.",
        ),
        Col::new(
            "has_parse_issues",
            Ty::Boolean,
            false,
            "Derived: true iff this row wrote ≥1 `parse_issues` row (§4.6).",
        ),
    ],
};

/// §3.38 `npx_vote_records`.
pub(crate) const NPX_VOTE_RECORDS: TableSpec = TableSpec {
    name: super::NPX_VOTE_RECORDS,
    doc: "How each block of shares was voted; filter columns are copied from the vote so most analyses need no join.",
    filing_context: true,
    cols: &[
        Col::new(
            "filer_cik",
            Ty::Utf8,
            true,
            "Copy of `npx_reports.filer_cik`.",
        ),
        Col::new(
            "registrant_type",
            Ty::Utf8,
            true,
            "Copy of `npx_reports.registrant_type`.",
        ),
        Col::new(
            "report_calendar_year",
            Ty::Int32,
            true,
            "Copy of `npx_reports.report_calendar_year`.",
        ),
        Col::new(
            "vote_index",
            Ty::UInt32,
            false,
            "Derived: position of the parent vote.",
        ),
        Col::new(
            "vote_series",
            Ty::Utf8,
            true,
            "Copy of `npx_votes.vote_series`.",
        ),
        Col::new(
            "meeting_date",
            Ty::Date32,
            true,
            "Copy of `npx_votes.meeting_date`.",
        ),
        Col::new(
            "cusip_norm",
            Ty::Utf8,
            true,
            "Copy of `npx_votes.cusip_norm`.",
        ),
        Col::new(
            "vote_source",
            Ty::Utf8,
            true,
            "Copy of `npx_votes.vote_source`.",
        ),
        Col::new(
            "vote_categories",
            Ty::ListUtf8,
            false,
            "Copy of `npx_votes.vote_categories`.",
        ),
        Col::new(
            "record_index",
            Ty::UInt32,
            false,
            "Derived: position in `records`.",
        ),
        Col::new(
            "how_voted",
            Ty::Utf8,
            true,
            "`Filing.body.npx.votes[].records[].how_voted`. 73 spellings: normalize with `sec_how_voted_norm` (§7.0).",
        ),
        Col::new(
            "shares_voted",
            Ty::Decimal(Family::S16),
            true,
            "`Filing.body.npx.votes[].records[].shares_voted`: decimal s=16 (§4.3, S16).",
        ),
        Col::new(
            "management_recommendation",
            Ty::Utf8,
            true,
            "`Filing.body.npx.votes[].records[].management_recommendation`. `FOR` / `AGAINST` / `NONE`.",
        ),
        Col::new(
            "has_parse_issues",
            Ty::Boolean,
            false,
            "Derived: true iff this row wrote ≥1 `parse_issues` row (§4.6).",
        ),
    ],
};

/// §3.39 `npx_other_managers`.
pub(crate) const NPX_OTHER_MANAGERS: TableSpec = TableSpec {
    name: super::NPX_OTHER_MANAGERS,
    doc: "Other managers of an N-PX (joined from votes by serial number).",
    filing_context: true,
    cols: &[
        Col::new(
            "filer_cik",
            Ty::Utf8,
            true,
            "Copy of `npx_reports.filer_cik`.",
        ),
        Col::new(
            "other_manager_index",
            Ty::UInt32,
            false,
            "Derived: 0-based over cover-page managers then summary managers.",
        ),
        Col::new(
            "list_kind",
            Ty::Dictionary,
            false,
            "Derived: `cover` (other_managers) or `summary` (summary_managers).",
        ),
        Col::new(
            "serial_number",
            Ty::Int32,
            true,
            "`Filing.body.npx.cover_page.{other_managers,summary_managers}[].serial_number`: integer (§4.3), Int32. Summary: the key `npx_votes.vote_other_managers` refers to.",
        ),
        Col::new(
            "other_manager_name",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.{other_managers,summary_managers}[].name`. Named like `form13f_other_managers.other_manager_name`.",
        ),
        Col::new(
            "form13f_file_number",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.{other_managers,summary_managers}[].form13f_file_number`.",
        ),
        Col::new(
            "crd_number",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.{other_managers,summary_managers}[].crd_number`.",
        ),
        Col::new(
            "sec_file_number",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.{other_managers,summary_managers}[].sec_file_number`.",
        ),
        Col::new(
            "lei",
            Ty::Utf8,
            true,
            "`Filing.body.npx.cover_page.{other_managers,summary_managers}[].lei`.",
        ),
        Col::new(
            "has_parse_issues",
            Ty::Boolean,
            false,
            "Derived: true iff this row wrote ≥1 `parse_issues` row (§4.6).",
        ),
    ],
};
