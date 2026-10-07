//! Append phase of `form144_notices`, `form144_securities_information`, `form144_securities_to_be_sold`, `form144_sales_past_3_months` (§3.19, §3.20, §3.21, §3.22).
//! Owned by the `smallforms` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! The column structs below are generated from `super::super::schema` and checked
//! against it on construction; edit them only together with the schema.

use firehose_parquet::encode::EncodeBytes;

use super::{sec_columns, AppendCtx, SecTable, Table};
#[allow(unused_imports)]
use super::{Addr, Bool, Date, Dec, Fc, ListDate, ListStr, Str, U32};
use crate::sec::prepare::form144::PreparedForm144;
use crate::sec::prepare::FilingCtx;
use crate::sec::proto::sec;
use crate::sec::schema;

sec_columns! {
    /// The columns of `form144_notices` (§3.19), in schema order.
    pub(crate) struct Form144NoticesCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub filer_cik: Str,
        pub issuer_cik: Str,
        pub issuer_name: Str,
        pub issuer_sec_file_number: Str,
        /// `issuer_street1` … `issuer_non_us_state_territory`.
        pub issuer: Addr,
        pub issuer_contact_phone: Str,
        pub person_for_whose_account: Str,
        pub relationships_to_issuer: ListStr,
        pub nothing_sold_past_3_months: Bool,
        pub remarks: Str,
        pub previous_accession_number: Str,
        pub notice_date: Date,
        pub signature_text: Str,
        pub plan_adoption_dates: ListDate,
        pub securities_information_count: U32,
        pub total_units_sold: Dec,
        pub total_aggregate_market_value: Dec,
        pub securities_to_be_sold_count: U32,
        pub sales_past_3_months_count: U32,
        pub has_parse_issues: Bool,
    }
}

sec_columns! {
    /// The columns of `form144_securities_information` (§3.20), in schema order.
    pub(crate) struct Form144SecuritiesInformationCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub issuer_cik: Str,
        pub filer_cik: Str,
        pub entry_index: U32,
        pub securities_class_title: Str,
        pub broker_name: Str,
        /// `broker_street1` … `broker_non_us_state_territory`.
        pub broker: Addr,
        pub units_sold: Dec,
        pub aggregate_market_value: Dec,
        pub units_outstanding: Dec,
        pub approx_sale_date: Date,
        pub securities_exchange_name: Str,
        pub has_parse_issues: Bool,
    }
}

sec_columns! {
    /// The columns of `form144_securities_to_be_sold` (§3.21), in schema order.
    pub(crate) struct Form144SecuritiesToBeSoldCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub issuer_cik: Str,
        pub filer_cik: Str,
        pub lot_index: U32,
        pub securities_class_title: Str,
        pub acquired_date: Date,
        pub nature_of_acquisition: Str,
        pub acquired_from: Str,
        pub is_gift: Bool,
        pub donor_acquired_date: Date,
        pub amount_acquired: Dec,
        pub payment_date: Date,
        pub nature_of_payment: Str,
        pub has_parse_issues: Bool,
    }
}

sec_columns! {
    /// The columns of `form144_sales_past_3_months` (§3.22), in schema order.
    pub(crate) struct Form144SalesPast3MonthsCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub issuer_cik: Str,
        pub filer_cik: Str,
        pub sale_index: U32,
        pub seller_name: Str,
        /// `seller_street1` … `seller_non_us_state_territory`.
        pub seller: Addr,
        pub securities_class_title: Str,
        pub sale_date: Date,
        pub amount_sold: Dec,
        pub gross_proceeds: Dec,
        pub has_parse_issues: Bool,
    }
}

/// Every table of this module.
pub(crate) struct Form144Tables {
    pub(crate) form144_notices: Table<Form144NoticesCols>,
    pub(crate) form144_securities_information: Table<Form144SecuritiesInformationCols>,
    pub(crate) form144_securities_to_be_sold: Table<Form144SecuritiesToBeSoldCols>,
    pub(crate) form144_sales_past_3_months: Table<Form144SalesPast3MonthsCols>,
}

impl Form144Tables {
    pub(crate) fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            form144_notices: Table::new(schema::FORM144_NOTICES, include_fork_step, encoding),
            form144_securities_information: Table::new(
                schema::FORM144_SECURITIES_INFORMATION,
                include_fork_step,
                encoding,
            ),
            form144_securities_to_be_sold: Table::new(
                schema::FORM144_SECURITIES_TO_BE_SOLD,
                include_fork_step,
                encoding,
            ),
            form144_sales_past_3_months: Table::new(
                schema::FORM144_SALES_PAST_3_MONTHS,
                include_fork_step,
                encoding,
            ),
        }
    }

    /// Append the rows of one filing's body, from its proto message and the
    /// values prepared by `crate::sec::prepare::form144::prepare`. Infallible.
    pub(crate) fn append(
        &mut self,
        ctx: &AppendCtx<'_>,
        fc: &FilingCtx<'_>,
        body: &sec::Form144Notice,
        prepared: &PreparedForm144<'_>,
    ) {
        // Stub: no rows yet.
        let _ = (ctx, fc, body, prepared);
    }

    pub(crate) fn tables(&self) -> [&dyn SecTable; 4] {
        [
            &self.form144_notices,
            &self.form144_securities_information,
            &self.form144_securities_to_be_sold,
            &self.form144_sales_past_3_months,
        ]
    }

    pub(crate) fn tables_mut(&mut self) -> [&mut dyn SecTable; 4] {
        [
            &mut self.form144_notices,
            &mut self.form144_securities_information,
            &mut self.form144_securities_to_be_sold,
            &mut self.form144_sales_past_3_months,
        ]
    }
}
