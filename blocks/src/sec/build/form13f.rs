//! Append phase of `form13f_reports`, `form13f_other_managers`, `form13f_holdings` (§3.14, §3.15, §3.16).
//! Owned by the `form13f-beneficial` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! The column structs below are generated from `super::super::schema` and checked
//! against it on construction; edit them only together with the schema.

use firehose_parquet::encode::EncodeBytes;

use super::{sec_columns, AppendCtx, SecTable, Table};
use super::{Addr, Bool, Date, Dict, Fc, ListI32, ListStr, Str, I32, I64, U32};
use crate::sec::parse;
use crate::sec::prepare::form13f::PreparedForm13f;
use crate::sec::prepare::FilingCtx;
use crate::sec::proto::sec;
use crate::sec::schema;

sec_columns! {
    /// The columns of `form13f_reports` (§3.14), in schema order.
    pub(crate) struct Form13fReportsCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub has_cover_page: Bool,
        pub manager_cik: Str,
        pub manager_name: Str,
        /// `manager_street1` … `manager_non_us_state_territory`.
        pub manager: Addr,
        pub report_type: Str,
        pub form13f_file_number: Str,
        pub crd_number: Str,
        pub sec_file_number: Str,
        pub period_of_report: Date,
        pub cover_is_amendment: Bool,
        pub amendment_type: Str,
        pub amendment_number: I32,
        pub provide_info_for_instruction5: Bool,
        pub additional_information: Str,
        pub has_summary_page: Bool,
        pub other_included_managers_count: U32,
        pub table_entry_total: U32,
        pub table_value_total: I64,
        pub is_confidential_omitted: Bool,
        pub value_multiplier_rule: I32,
        pub holdings_count: U32,
        pub holdings_value_sum: I64,
        pub holdings_complete: Bool,
        pub cover_other_manager_count: U32,
        pub summary_other_manager_count: U32,
        pub has_parse_issues: Bool,
    }
}

sec_columns! {
    /// The columns of `form13f_other_managers` (§3.15), in schema order.
    pub(crate) struct Form13fOtherManagersCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub manager_cik: Str,
        pub period_of_report: Date,
        pub other_manager_index: U32,
        pub list_kind: Dict,
        pub sequence_number: I32,
        pub other_manager_cik: Str,
        pub other_manager_name: Str,
        pub form13f_file_number: Str,
        pub crd_number: Str,
        pub sec_file_number: Str,
        pub has_parse_issues: Bool,
    }
}

sec_columns! {
    /// The columns of `form13f_holdings` (§3.16), in schema order.
    pub(crate) struct Form13fHoldingsCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub manager_cik: Str,
        pub manager_name: Str,
        pub period_of_report: Date,
        pub report_type: Str,
        pub amendment_type: Str,
        pub value_multiplier_rule: I32,
        pub holding_index: U32,
        pub issuer_name: Str,
        pub title_of_class: Str,
        pub cusip: Str,
        pub cusip_norm: Str,
        pub figi: Str,
        pub value: I64,
        pub shares_or_principal_amount: I64,
        pub shares_or_principal_type: Str,
        pub put_call: Str,
        pub put_call_norm: Dict,
        pub investment_discretion: Str,
        pub other_manager_ids: ListStr,
        pub other_manager_sequence_numbers: ListI32,
        pub voting_authority_sole: I64,
        pub voting_authority_shared: I64,
        pub voting_authority_none: I64,
        pub has_parse_issues: Bool,
    }
}

/// Every table of this module.
pub(crate) struct Form13fTables {
    pub(crate) form13f_reports: Table<Form13fReportsCols>,
    pub(crate) form13f_other_managers: Table<Form13fOtherManagersCols>,
    pub(crate) form13f_holdings: Table<Form13fHoldingsCols>,
}

impl Form13fTables {
    pub(crate) fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            form13f_reports: Table::new(schema::FORM13F_REPORTS, include_fork_step, encoding),
            form13f_other_managers: Table::new(
                schema::FORM13F_OTHER_MANAGERS,
                include_fork_step,
                encoding,
            ),
            form13f_holdings: Table::new(schema::FORM13F_HOLDINGS, include_fork_step, encoding),
        }
    }

    /// Append the rows of one filing's body, from its proto message and the
    /// values prepared by `crate::sec::prepare::form13f::prepare`. Infallible.
    pub(crate) fn append(
        &mut self,
        ctx: &AppendCtx<'_>,
        fc: &FilingCtx<'_>,
        body: &sec::Form13fReport,
        prepared: &PreparedForm13f<'_>,
    ) {
        let report = &prepared.report;
        let cover = body.cover_page.as_ref();
        let summary = body.summary_page.as_ref();
        let manager = cover.and_then(|c| c.filing_manager.as_ref());
        let cover_text = |field: fn(&sec::CoverPage) -> &str| cover.map_or("", field);

        let row = self.form13f_reports.row(ctx);
        row.fc.append(fc);
        row.has_cover_page.val(cover.is_some());
        row.manager_cik.nz(manager.map_or("", |m| &m.cik));
        row.manager_name.nz(manager.map_or("", |m| &m.name));
        row.manager.append(manager.and_then(|m| m.address.as_ref()));
        row.report_type.nz(cover_text(|c| &c.report_type));
        row.form13f_file_number
            .nz(cover_text(|c| &c.form13f_file_number));
        row.crd_number.nz(cover_text(|c| &c.crd_number));
        row.sec_file_number.nz(cover_text(|c| &c.sec_file_number));
        row.period_of_report.opt(report.period_of_report);
        row.cover_is_amendment.opt(cover.map(|c| c.is_amendment));
        row.amendment_type.nz(cover_text(|c| &c.amendment_type));
        row.amendment_number.opt(report.amendment_number);
        row.provide_info_for_instruction5
            .opt(report.provide_info_for_instruction5);
        row.additional_information
            .nz(cover_text(|c| &c.additional_information));
        row.has_summary_page.val(summary.is_some());
        row.other_included_managers_count
            .opt(summary.map(|s| s.other_included_managers_count));
        row.table_entry_total
            .opt(summary.map(|s| s.table_entry_total));
        row.table_value_total.opt(report.table_value_total);
        row.is_confidential_omitted
            .opt(summary.and_then(|s| s.is_confidential_omitted));
        row.value_multiplier_rule.val(report.value_multiplier_rule);
        row.holdings_count.val(report.holdings_count);
        row.holdings_value_sum.opt(report.holdings_value_sum);
        row.holdings_complete.opt(report.holdings_complete);
        row.cover_other_manager_count
            .val(report.cover_other_manager_count);
        row.summary_other_manager_count
            .val(report.summary_other_manager_count);
        row.has_parse_issues.val(report.has_parse_issues);

        // The parent columns every child row copies (issues logged on the parent).
        let manager_cik = manager.map_or("", |m| m.cik.as_str());
        let manager_name = manager.map_or("", |m| m.name.as_str());
        let report_type = cover_text(|c| &c.report_type);
        let amendment_type = cover_text(|c| &c.amendment_type);

        for other in &prepared.other_managers {
            let m = other.manager;
            let row = self.form13f_other_managers.row(ctx);
            row.fc.append(fc);
            row.manager_cik.nz(manager_cik);
            row.period_of_report.opt(report.period_of_report);
            row.other_manager_index.val(other.other_manager_index);
            row.list_kind.val(other.list_kind);
            row.sequence_number.opt(other.sequence_number);
            row.other_manager_cik.nz(&m.cik);
            row.other_manager_name.nz(&m.name);
            row.form13f_file_number.nz(&m.form13f_file_number);
            row.crd_number.nz(&m.crd_number);
            row.sec_file_number.nz(&m.sec_file_number);
            row.has_parse_issues.val(other.has_parse_issues);
        }

        for (holding, p) in body.holdings.iter().zip(&prepared.holdings) {
            let amount = holding.shares_or_principal.as_ref();
            let row = self.form13f_holdings.row(ctx);
            row.fc.append(fc);
            row.manager_cik.nz(manager_cik);
            row.manager_name.nz(manager_name);
            row.period_of_report.opt(report.period_of_report);
            row.report_type.nz(report_type);
            row.amendment_type.nz(amendment_type);
            row.value_multiplier_rule.val(report.value_multiplier_rule);
            row.holding_index.val(p.holding_index);
            row.issuer_name.nz(&holding.name_of_issuer);
            row.title_of_class.nz(&holding.title_of_class);
            row.cusip.nz(&holding.cusip);
            row.cusip_norm
                .opt(parse::cusip_norm(&holding.cusip).as_deref());
            row.figi.nz(&holding.figi);
            row.value.opt(p.value);
            row.shares_or_principal_amount
                .opt(p.shares_or_principal_amount);
            row.shares_or_principal_type
                .nz(amount.map_or("", |a| a.r#type.as_str()));
            row.put_call.nz(&holding.put_call);
            row.put_call_norm
                .opt(parse::put_call_norm(&holding.put_call));
            row.investment_discretion.nz(&holding.investment_discretion);
            row.other_manager_ids
                .items(holding.other_manager_ids.iter().map(String::as_str));
            row.other_manager_sequence_numbers.items(
                parse::seq_numbers(&holding.other_manager_ids)
                    .into_iter()
                    .map(Some),
            );
            row.voting_authority_sole.opt(p.voting_authority_sole);
            row.voting_authority_shared.opt(p.voting_authority_shared);
            row.voting_authority_none.opt(p.voting_authority_none);
            row.has_parse_issues.val(p.has_parse_issues);
        }
    }

    pub(crate) fn tables(&self) -> [&dyn SecTable; 3] {
        [
            &self.form13f_reports,
            &self.form13f_other_managers,
            &self.form13f_holdings,
        ]
    }

    pub(crate) fn tables_mut(&mut self) -> [&mut dyn SecTable; 3] {
        [
            &mut self.form13f_reports,
            &mut self.form13f_other_managers,
            &mut self.form13f_holdings,
        ]
    }
}
