//! Append phase of `npx_reports`, `npx_votes`, `npx_vote_records`, `npx_other_managers` (§3.36, §3.37, §3.38, §3.39).
//! Owned by the `funds` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! The column structs below are generated from `super::super::schema` and checked
//! against it on construction; edit them only together with the schema.

use firehose_parquet::encode::EncodeBytes;

use super::{sec_columns, AppendCtx, SecTable, Table};
#[allow(unused_imports)]
use super::{Addr, Bool, Date, Dec, Dict, Fc, ListI32, ListStr, ListStruct, Str, I32, U32};
use crate::sec::parse;
use crate::sec::prepare::npx::{PreparedNpx, PreparedNpxReport};
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
    ///
    /// Rows: the report, its other managers (cover, then summary), then every
    /// vote and its records. The flat prepared vectors are consumed in proto
    /// order, so positions need no lookup.
    pub(crate) fn append(
        &mut self,
        ctx: &AppendCtx<'_>,
        fc: &FilingCtx<'_>,
        body: &sec::NpxReport,
        prepared: &PreparedNpx<'_>,
    ) {
        // An absent cover page reads as an empty one: every string is '' →
        // NULL, both addresses and both optional bools are unset, the series
        // list is [] and there are no managers; `has_cover_page` keeps the
        // difference.
        let absent_cover = sec::NpxCoverPage::default();
        let cover = body.cover_page.as_ref().unwrap_or(&absent_cover);
        let report = &prepared.report;
        let filer_cik = body.filer_cik.as_str();
        let registrant_type = cover.registrant_type.as_str();
        let report_calendar_year = report.report_calendar_year;

        self.append_report(ctx, fc, body, cover, report);

        for manager in &prepared.managers {
            let m = manager.manager;
            let row = self.npx_other_managers.row(ctx);
            row.fc.append(fc);
            row.filer_cik.nz(filer_cik);
            row.other_manager_index.val(manager.other_manager_index);
            row.list_kind.val(manager.list_kind);
            row.serial_number.opt(manager.serial_number);
            row.other_manager_name.nz(&m.name);
            row.form13f_file_number.nz(&m.form13f_file_number);
            row.crd_number.nz(&m.crd_number);
            row.sec_file_number.nz(&m.sec_file_number);
            row.lei.nz(&m.lei);
            row.has_parse_issues.val(manager.has_parse_issues);
        }

        let mut other_managers = prepared.vote_other_managers.iter().copied();
        let mut records = prepared
            .record_shares_voted
            .iter()
            .zip(&prepared.record_has_parse_issues);
        // Positions fit `u32`: the preflight checked `vote_count` and every
        // `record_count` (§4.7). The `0u32..` counters come second in `zip`,
        // so they are never polled past the last element.
        for ((vote, p), vote_index) in body.votes.iter().zip(&prepared.votes).zip(0u32..) {
            let cusip_norm = parse::cusip_norm(&vote.cusip);
            let cusip_norm = cusip_norm.as_deref();
            let categories = || vote.vote_categories.iter().map(String::as_str);

            let row = self.npx_votes.row(ctx);
            row.fc.append(fc);
            row.filer_cik.nz(filer_cik);
            row.registrant_type.nz(registrant_type);
            row.report_calendar_year.opt(report_calendar_year);
            row.vote_index.val(vote_index);
            row.issuer_name.nz(&vote.issuer_name);
            row.cusip.nz(&vote.cusip);
            row.cusip_norm.opt(cusip_norm);
            row.isin.nz(&vote.isin);
            row.figi.nz(&vote.figi);
            row.meeting_date.opt(p.meeting_date);
            row.vote_description.nz(&vote.vote_description);
            row.other_vote_description.nz(&vote.other_vote_description);
            row.vote_categories.items(categories());
            row.vote_source.nz(&vote.vote_source);
            row.vote_series.nz(&vote.vote_series);
            row.shares_voted.opt(p.shares_voted.get());
            row.shares_on_loan.opt(p.shares_on_loan.get());
            row.vote_other_managers
                .items(other_managers.by_ref().take(vote.vote_other_managers.len()));
            row.vote_other_info.nz(&vote.vote_other_info);
            row.record_count.val(p.record_count);
            row.has_parse_issues.val(p.has_parse_issues);

            // `zip` stops at the vote's last record without taking from
            // `records` (the first iterator is polled first).
            for ((record, (shares_voted, has_parse_issues)), record_index) in
                vote.records.iter().zip(records.by_ref()).zip(0u32..)
            {
                let row = self.npx_vote_records.row(ctx);
                row.fc.append(fc);
                row.filer_cik.nz(filer_cik);
                row.registrant_type.nz(registrant_type);
                row.report_calendar_year.opt(report_calendar_year);
                row.vote_index.val(vote_index);
                row.vote_series.nz(&vote.vote_series);
                row.meeting_date.opt(p.meeting_date);
                row.cusip_norm.opt(cusip_norm);
                row.vote_source.nz(&vote.vote_source);
                row.vote_categories.items(categories());
                row.record_index.val(record_index);
                row.how_voted.nz(&record.how_voted);
                row.shares_voted.opt(shares_voted.get());
                row.management_recommendation
                    .nz(&record.management_recommendation);
                row.has_parse_issues.val(*has_parse_issues);
            }
        }
    }

    /// The `npx_reports` row (§3.36).
    fn append_report(
        &mut self,
        ctx: &AppendCtx<'_>,
        fc: &FilingCtx<'_>,
        body: &sec::NpxReport,
        cover: &sec::NpxCoverPage,
        report: &PreparedNpxReport,
    ) {
        let row = self.npx_reports.row(ctx);
        row.fc.append(fc);
        row.filer_cik.nz(&body.filer_cik);
        row.has_cover_page.val(body.cover_page.is_some());
        row.registrant_type.nz(&cover.registrant_type);
        row.investment_company_type
            .nz(&cover.investment_company_type);
        row.year_or_quarter.nz(&cover.year_or_quarter);
        row.report_calendar_year.opt(report.report_calendar_year);
        row.report_type.nz(&cover.report_type);
        row.reporting_person_name.nz(&cover.reporting_person_name);
        row.reporting_person
            .append(cover.reporting_person_address.as_ref());
        row.reporting_person_phone.nz(&cover.reporting_person_phone);
        row.file_number.nz(&cover.file_number);
        row.reporting_crd_number.nz(&cover.reporting_crd_number);
        row.reporting_sec_file_number
            .nz(&cover.reporting_sec_file_number);
        row.lei_number.nz(&cover.lei_number);
        row.confidential_treatment
            .opt(report.confidential_treatment);
        row.notice_explanation.nz(&cover.notice_explanation);
        row.explanatory_choice.opt(report.explanatory_choice);
        row.explanatory_notes.nz(&cover.explanatory_notes);
        row.cover_is_amendment.opt(cover.is_amendment);
        row.amendment_number.opt(report.amendment_number);
        row.amendment_type.nz(&cover.amendment_type);
        row.conf_denied_expired.opt(cover.conf_denied_expired);
        row.agent_for_service_name.nz(&cover.agent_for_service_name);
        row.agent_for_service
            .append(cover.agent_for_service_address.as_ref());
        row.series_reports.utf8_items(
            cover
                .series_reports
                .iter()
                .map(|s| [s.series_id.as_str(), s.name.as_str(), s.lei.as_str()]),
        );
        row.other_included_managers_count
            .opt(report.other_included_managers_count);
        row.declared_series_count.opt(report.declared_series_count);
        row.vote_count.val(report.vote_count);
        row.vote_record_count.val(report.vote_record_count);
        row.other_manager_count.val(report.other_manager_count);
        row.has_parse_issues.val(report.has_parse_issues);
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
