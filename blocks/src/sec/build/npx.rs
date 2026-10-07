//! Append phase of `npx_reports`, `npx_votes`, `npx_vote_records`, `npx_other_managers` (§3.36, §3.37, §3.38, §3.39).
//! Owned by the `funds` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! The column structs below are generated from `super::super::schema` and checked
//! against it on construction; edit them only together with the schema.

use firehose_parquet::encode::EncodeBytes;

use super::{sec_columns, AppendCtx, SecTable, Table};
#[allow(unused_imports)]
use super::{Addr, Bool, Date, Dec, Dict, Fc, ListI32, ListStr, ListStruct, Str, I32, U32};
use crate::sec::prepare::npx::PreparedNpx;
use crate::sec::prepare::FilingCtx;
use crate::sec::proto::sec;
use crate::sec::schema;

sec_columns! {
    /// The columns of `npx_reports` (§3.36), in schema order.
    pub(crate) struct NpxReportsCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub filer_cik: Str,
        pub has_cover_page: Bool,
        pub registrant_type: Str,
        pub investment_company_type: Str,
        pub year_or_quarter: Str,
        pub report_calendar_year: I32,
        pub report_type: Str,
        pub reporting_person_name: Str,
        /// `reporting_person_street1` … `reporting_person_non_us_state_territory`.
        pub reporting_person: Addr,
        pub reporting_person_phone: Str,
        pub file_number: Str,
        pub reporting_crd_number: Str,
        pub reporting_sec_file_number: Str,
        pub lei_number: Str,
        pub confidential_treatment: Bool,
        pub notice_explanation: Str,
        pub explanatory_choice: Bool,
        pub explanatory_notes: Str,
        pub cover_is_amendment: Bool,
        pub amendment_number: I32,
        pub amendment_type: Str,
        pub conf_denied_expired: Bool,
        pub agent_for_service_name: Str,
        /// `agent_for_service_street1` … `agent_for_service_non_us_state_territory`.
        pub agent_for_service: Addr,
        pub series_reports: ListStruct,
        pub other_included_managers_count: I32,
        pub declared_series_count: I32,
        pub vote_count: U32,
        pub vote_record_count: U32,
        pub other_manager_count: U32,
        pub has_parse_issues: Bool,
    }
}

sec_columns! {
    /// The columns of `npx_votes` (§3.37), in schema order.
    pub(crate) struct NpxVotesCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub filer_cik: Str,
        pub registrant_type: Str,
        pub report_calendar_year: I32,
        pub vote_index: U32,
        pub issuer_name: Str,
        pub cusip: Str,
        pub cusip_norm: Str,
        pub isin: Str,
        pub figi: Str,
        pub meeting_date: Date,
        pub vote_description: Str,
        pub other_vote_description: Str,
        pub vote_categories: ListStr,
        pub vote_source: Str,
        pub vote_series: Str,
        pub shares_voted: Dec,
        pub shares_on_loan: Dec,
        pub vote_other_managers: ListI32,
        pub vote_other_info: Str,
        pub record_count: U32,
        pub has_parse_issues: Bool,
    }
}

sec_columns! {
    /// The columns of `npx_vote_records` (§3.38), in schema order.
    pub(crate) struct NpxVoteRecordsCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub filer_cik: Str,
        pub registrant_type: Str,
        pub report_calendar_year: I32,
        pub vote_index: U32,
        pub vote_series: Str,
        pub meeting_date: Date,
        pub cusip_norm: Str,
        pub vote_source: Str,
        pub vote_categories: ListStr,
        pub record_index: U32,
        pub how_voted: Str,
        pub shares_voted: Dec,
        pub management_recommendation: Str,
        pub has_parse_issues: Bool,
    }
}

sec_columns! {
    /// The columns of `npx_other_managers` (§3.39), in schema order.
    pub(crate) struct NpxOtherManagersCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub filer_cik: Str,
        pub other_manager_index: U32,
        pub list_kind: Dict,
        pub serial_number: I32,
        pub other_manager_name: Str,
        pub form13f_file_number: Str,
        pub crd_number: Str,
        pub sec_file_number: Str,
        pub lei: Str,
        pub has_parse_issues: Bool,
    }
}

/// Every table of this module.
pub(crate) struct NpxTables {
    pub(crate) npx_reports: Table<NpxReportsCols>,
    pub(crate) npx_votes: Table<NpxVotesCols>,
    pub(crate) npx_vote_records: Table<NpxVoteRecordsCols>,
    pub(crate) npx_other_managers: Table<NpxOtherManagersCols>,
}

impl NpxTables {
    pub(crate) fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            npx_reports: Table::new(schema::NPX_REPORTS, include_fork_step, encoding),
            npx_votes: Table::new(schema::NPX_VOTES, include_fork_step, encoding),
            npx_vote_records: Table::new(schema::NPX_VOTE_RECORDS, include_fork_step, encoding),
            npx_other_managers: Table::new(schema::NPX_OTHER_MANAGERS, include_fork_step, encoding),
        }
    }

    /// Append the rows of one filing's body, from its proto message and the
    /// values prepared by `crate::sec::prepare::npx::prepare`. Infallible.
    pub(crate) fn append(
        &mut self,
        ctx: &AppendCtx<'_>,
        fc: &FilingCtx<'_>,
        body: &sec::NpxReport,
        prepared: &PreparedNpx<'_>,
    ) {
        // Stub: no rows yet.
        let _ = (ctx, fc, body, prepared);
    }

    pub(crate) fn tables(&self) -> [&dyn SecTable; 4] {
        [
            &self.npx_reports,
            &self.npx_votes,
            &self.npx_vote_records,
            &self.npx_other_managers,
        ]
    }

    pub(crate) fn tables_mut(&mut self) -> [&mut dyn SecTable; 4] {
        [
            &mut self.npx_reports,
            &mut self.npx_votes,
            &mut self.npx_vote_records,
            &mut self.npx_other_managers,
        ]
    }
}
