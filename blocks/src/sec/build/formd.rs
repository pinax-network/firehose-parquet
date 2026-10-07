//! Append phase of `form_d_notices`, `form_d_co_issuers`, `form_d_related_persons`, `form_d_sales_recipients` (§3.32, §3.33, §3.34, §3.35).
//! Owned by the `smallforms` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! The column structs below are generated from `super::super::schema` and checked
//! against it on construction; edit them only together with the schema.

use firehose_parquet::encode::EncodeBytes;

use super::{sec_columns, AppendCtx, SecTable, Table};
#[allow(unused_imports)]
use super::{Addr, Bool, Date, Dec, Fc, ListStr, Str, I32, I64, U32};
use crate::sec::prepare::formd::PreparedFormD;
use crate::sec::prepare::FilingCtx;
use crate::sec::proto::sec;
use crate::sec::schema;

sec_columns! {
    /// The columns of `form_d_notices` (§3.32), in schema order.
    pub(crate) struct FormDNoticesCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub schema_version: Str,
        pub submission_type: Str,
        pub previous_accession_number: Str,
        pub issuer_cik: Str,
        pub issuer_name: Str,
        /// `issuer_street1` … `issuer_non_us_state_territory`.
        pub issuer: Addr,
        pub issuer_phone: Str,
        pub jurisdiction_of_inc: Str,
        pub entity_type: Str,
        pub entity_type_other_desc: Str,
        pub year_of_inc: I32,
        pub year_of_inc_status: Str,
        pub issuer_previous_names: ListStr,
        pub issuer_edgar_previous_names: ListStr,
        pub industry_group: Str,
        pub investment_fund_type: Str,
        pub is_40_act: Bool,
        pub revenue_range: Str,
        pub aggregate_net_asset_value_range: Str,
        pub offering_is_amendment: Bool,
        pub date_of_first_sale: Date,
        pub date_of_first_sale_yet_to_occur: Bool,
        pub more_than_one_year: Bool,
        pub is_equity_type: Bool,
        pub securities_types: ListStr,
        pub description_of_other_type: Str,
        pub is_business_combination: Bool,
        pub business_combination_clarification: Str,
        pub federal_exemptions: ListStr,
        pub minimum_investment: Dec,
        pub total_offering_amount: Dec,
        pub total_offering_amount_is_indefinite: Bool,
        pub total_amount_sold: Dec,
        pub total_remaining: Dec,
        pub total_remaining_is_indefinite: Bool,
        pub offering_sales_amounts_clarification: Str,
        pub has_non_accredited_investors: Bool,
        pub number_non_accredited_investors: I64,
        pub total_number_already_invested: I64,
        pub sales_commissions: Dec,
        pub sales_commissions_is_estimate: Bool,
        pub finders_fees: Dec,
        pub finders_fees_is_estimate: Bool,
        pub sales_commissions_clarification: Str,
        pub gross_proceeds_used: Dec,
        pub gross_proceeds_used_is_estimate: Bool,
        pub use_of_proceeds_clarification: Str,
        pub authorized_representative: Bool,
        pub co_issuer_count: U32,
        pub related_person_count: U32,
        pub sales_recipient_count: U32,
        pub has_parse_issues: Bool,
    }
}

sec_columns! {
    /// The columns of `form_d_co_issuers` (§3.33), in schema order.
    pub(crate) struct FormDCoIssuersCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub issuer_cik: Str,
        pub co_issuer_index: U32,
        pub co_issuer_cik: Str,
        pub co_issuer_name: Str,
        /// `co_issuer_street1` … `co_issuer_non_us_state_territory`.
        pub co_issuer: Addr,
        pub co_issuer_phone: Str,
        pub jurisdiction_of_inc: Str,
        pub entity_type: Str,
        pub entity_type_other_desc: Str,
        pub year_of_inc: I32,
        pub year_of_inc_status: Str,
        pub previous_names: ListStr,
        pub edgar_previous_names: ListStr,
        pub has_parse_issues: Bool,
    }
}

sec_columns! {
    /// The columns of `form_d_related_persons` (§3.34), in schema order.
    pub(crate) struct FormDRelatedPersonsCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub issuer_cik: Str,
        pub person_index: U32,
        pub first_name: Str,
        pub middle_name: Str,
        pub last_name: Str,
        /// `person_street1` … `person_non_us_state_territory`.
        pub person: Addr,
        pub relationships: ListStr,
        pub relationship_clarification: Str,
    }
}

sec_columns! {
    /// The columns of `form_d_sales_recipients` (§3.35), in schema order.
    pub(crate) struct FormDSalesRecipientsCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub issuer_cik: Str,
        pub recipient_index: U32,
        pub recipient_name: Str,
        pub recipient_crd_number: Str,
        pub associated_bd_name: Str,
        pub associated_bd_crd_number: Str,
        /// `recipient_street1` … `recipient_non_us_state_territory`.
        pub recipient: Addr,
        pub states_of_solicitation: ListStr,
        pub foreign_solicitation: Bool,
    }
}

/// Every table of this module.
pub(crate) struct FormDTables {
    pub(crate) form_d_notices: Table<FormDNoticesCols>,
    pub(crate) form_d_co_issuers: Table<FormDCoIssuersCols>,
    pub(crate) form_d_related_persons: Table<FormDRelatedPersonsCols>,
    pub(crate) form_d_sales_recipients: Table<FormDSalesRecipientsCols>,
}

impl FormDTables {
    pub(crate) fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            form_d_notices: Table::new(schema::FORM_D_NOTICES, include_fork_step, encoding),
            form_d_co_issuers: Table::new(schema::FORM_D_CO_ISSUERS, include_fork_step, encoding),
            form_d_related_persons: Table::new(
                schema::FORM_D_RELATED_PERSONS,
                include_fork_step,
                encoding,
            ),
            form_d_sales_recipients: Table::new(
                schema::FORM_D_SALES_RECIPIENTS,
                include_fork_step,
                encoding,
            ),
        }
    }

    /// Append the rows of one filing's body, from its proto message and the
    /// values prepared by `crate::sec::prepare::formd::prepare`. Infallible.
    pub(crate) fn append(
        &mut self,
        ctx: &AppendCtx<'_>,
        fc: &FilingCtx<'_>,
        body: &sec::FormDNotice,
        prepared: &PreparedFormD<'_>,
    ) {
        // Stub: no rows yet.
        let _ = (ctx, fc, body, prepared);
    }

    pub(crate) fn tables(&self) -> [&dyn SecTable; 4] {
        [
            &self.form_d_notices,
            &self.form_d_co_issuers,
            &self.form_d_related_persons,
            &self.form_d_sales_recipients,
        ]
    }

    pub(crate) fn tables_mut(&mut self) -> [&mut dyn SecTable; 4] {
        [
            &mut self.form_d_notices,
            &mut self.form_d_co_issuers,
            &mut self.form_d_related_persons,
            &mut self.form_d_sales_recipients,
        ]
    }
}
