//! Append phase of `form13f_reports`, `form13f_other_managers`, `form13f_holdings` (§3.14, §3.15, §3.16).
//! Owned by the `form13f-beneficial` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! The column structs below are generated from `super::super::schema` and checked
//! against it on construction; edit them only together with the schema.

use firehose_parquet::encode::EncodeBytes;

use super::{sec_columns, AppendCtx, SecTable, Table};
#[allow(unused_imports)]
use super::{Addr, Bool, Date, Dict, Fc, ListI32, ListStr, Str, I32, I64, U32};
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
        // Stub: no rows yet.
        let _ = (ctx, fc, body, prepared);
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
