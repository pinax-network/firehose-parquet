//! Table specs generated from the final specification §3 (`final-spec.md`).
//! Edit only to fix a divergence from the specification; regenerate
//! `docs/schemas/sec.md` and re-pin the chain schema digests afterwards.

#[allow(unused_imports)]
use super::{Col, Family, Member, TableSpec, Ty};

/// §3.40 `ncen_reports`.
pub(crate) const NCEN_REPORTS: TableSpec = TableSpec {
    name: super::NCEN_REPORTS,
    doc: "N-CEN registrant census header.",
    filing_context: true,
    cols: &[
        Col::new(
            "filer_cik",
            Ty::Utf8,
            true,
            "`Filing.body.ncen.filer_cik`.",
        ),
        Col::new(
            "investment_company_type",
            Ty::Utf8,
            true,
            "`Filing.body.ncen.investment_company_type`. `N-1A`, `N-2`, `N-3/4/6`, `S-6`.",
        ),
        Col::new(
            "report_ending_period",
            Ty::Date32,
            true,
            "`Filing.body.ncen.report_ending_period`: date (§4.2). Fiscal year end.",
        ),
        Col::new(
            "is_report_period_lt12",
            Ty::Boolean,
            false,
            "`Filing.body.ncen.is_report_period_lt12`: proto bool (false = false or absent).",
        ),
        Col::new(
            "previous_accession_number",
            Ty::Utf8,
            true,
            "`Filing.body.ncen.previous_accession_number`. N-CEN/A.",
        ),
        Col::new(
            "registrant_name",
            Ty::Utf8,
            true,
            "`Filing.body.ncen.registrant.full_name`.",
        ),
        Col::new(
            "registrant_file_number",
            Ty::Utf8,
            true,
            "`Filing.body.ncen.registrant.file_number`. `811-…`.",
        ),
        Col::new(
            "registrant_cik",
            Ty::Utf8,
            true,
            "`Filing.body.ncen.registrant.cik`.",
        ),
        Col::new(
            "registrant_lei",
            Ty::Utf8,
            true,
            "`Filing.body.ncen.registrant.lei`.",
        ),
        Col::new(
            "registrant_street1",
            Ty::Utf8,
            true,
            "`Filing.body.ncen.registrant.address.street1`.",
        ),
        Col::new(
            "registrant_street2",
            Ty::Utf8,
            true,
            "`Filing.body.ncen.registrant.address.street2`.",
        ),
        Col::new(
            "registrant_city",
            Ty::Utf8,
            true,
            "`Filing.body.ncen.registrant.address.city`.",
        ),
        Col::new(
            "registrant_state",
            Ty::Utf8,
            true,
            "`Filing.body.ncen.registrant.address.state`. ISO 3166-2 (`US-NY`), unlike EDGAR codes elsewhere.",
        ),
        Col::new(
            "registrant_zip_code",
            Ty::Utf8,
            true,
            "`Filing.body.ncen.registrant.address.zip_code`. Text (keeps leading zeros).",
        ),
        Col::new(
            "registrant_state_description",
            Ty::Utf8,
            true,
            "`Filing.body.ncen.registrant.address.state_description`. Filled only by Forms 3/4/5 and D.",
        ),
        Col::new(
            "registrant_country",
            Ty::Utf8,
            true,
            "`Filing.body.ncen.registrant.address.country`.",
        ),
        Col::new(
            "registrant_non_us_state_territory",
            Ty::Utf8,
            true,
            "`Filing.body.ncen.registrant.address.non_us_state_territory`.",
        ),
        Col::new(
            "registrant_phone",
            Ty::Utf8,
            true,
            "`Filing.body.ncen.registrant.phone`.",
        ),
        Col::new(
            "series_ids",
            Ty::ListUtf8,
            false,
            "`Filing.body.ncen.series_ids[]`: verbatim items in document order; [] when empty. Join `nport_reports.series_id` via unnest.",
        ),
        Col::new(
            "series_count",
            Ty::UInt32,
            false,
            "Derived from `Filing.body.ncen.series_ids[]`: length of the list.",
        ),
        Col::new(
            "has_parse_issues",
            Ty::Boolean,
            false,
            "Derived: true iff this row wrote ≥1 `parse_issues` row (§4.6).",
        ),
    ],
};
