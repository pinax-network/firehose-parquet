//! Append phase of `form_c_notices`, `form_c_co_issuers` (§3.41, §3.42).
//! Owned by the `smallforms` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! The column structs below are generated from `super::super::schema` and checked
//! against it on construction; edit them only together with the schema.

use firehose_parquet::encode::EncodeBytes;

use super::{sec_columns, AppendCtx, SecTable, Table};
#[allow(unused_imports)]
use super::{Addr, Bool, Date, Dec, Fc, ListStr, Str, I64, U32};
use crate::sec::prepare::formc::PreparedFormC;
use crate::sec::prepare::FilingCtx;
use crate::sec::proto::sec;
use crate::sec::schema;

sec_columns! {
    /// The columns of `form_c_notices` (§3.41), in schema order.
    pub(crate) struct FormCNoticesCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub filer_cik: Str,
        pub issuer_name: Str,
        pub issuer_legal_status_form: Str,
        pub issuer_legal_status_other_desc: Str,
        pub issuer_jurisdiction: Str,
        pub issuer_date_incorporation: Date,
        /// `issuer_street1` … `issuer_non_us_state_territory`.
        pub issuer: Addr,
        pub issuer_website: Str,
        pub intermediary_company_name: Str,
        pub intermediary_cik: Str,
        pub intermediary_file_number: Str,
        pub intermediary_crd_number: Str,
        pub issuer_info_is_amendment: Bool,
        pub nature_of_amendment: Str,
        pub progress_update: Str,
        pub is_co_issuer: Bool,
        pub period: Date,
        pub security_type: Str,
        pub security_offered_other_desc: Str,
        pub num_securities_offered: Dec,
        pub price: Dec,
        pub price_determination_method: Str,
        pub offering_amount: Dec,
        pub maximum_offering_amount: Dec,
        pub over_subscription_accepted: Bool,
        pub over_subscription_allocation_type: Str,
        pub desc_over_subscription: Str,
        pub deadline_date: Date,
        pub compensation_amount: Str,
        pub financial_interest: Str,
        pub offering_jurisdictions: ListStr,
        pub has_financials: Bool,
        pub current_employees: I64,
        pub total_assets_most_recent_fy: Dec,
        pub total_assets_prior_fy: Dec,
        pub cash_equivalents_most_recent_fy: Dec,
        pub cash_equivalents_prior_fy: Dec,
        pub accounts_receivable_most_recent_fy: Dec,
        pub accounts_receivable_prior_fy: Dec,
        pub short_term_debt_most_recent_fy: Dec,
        pub short_term_debt_prior_fy: Dec,
        pub long_term_debt_most_recent_fy: Dec,
        pub long_term_debt_prior_fy: Dec,
        pub revenue_most_recent_fy: Dec,
        pub revenue_prior_fy: Dec,
        pub cost_goods_sold_most_recent_fy: Dec,
        pub cost_goods_sold_prior_fy: Dec,
        pub tax_paid_most_recent_fy: Dec,
        pub tax_paid_prior_fy: Dec,
        pub net_income_most_recent_fy: Dec,
        pub net_income_prior_fy: Dec,
        pub co_issuer_count: U32,
        pub has_parse_issues: Bool,
    }
}

sec_columns! {
    /// The columns of `form_c_co_issuers` (§3.42), in schema order.
    pub(crate) struct FormCCoIssuersCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub filer_cik: Str,
        pub co_issuer_index: U32,
        pub co_issuer_name: Str,
        pub legal_status_form: Str,
        pub legal_status_other_desc: Str,
        pub jurisdiction: Str,
        pub date_incorporation: Date,
        /// `co_issuer_street1` … `co_issuer_non_us_state_territory`.
        pub co_issuer: Addr,
        pub website: Str,
        pub has_parse_issues: Bool,
    }
}

/// Every table of this module.
pub(crate) struct FormCTables {
    pub(crate) form_c_notices: Table<FormCNoticesCols>,
    pub(crate) form_c_co_issuers: Table<FormCCoIssuersCols>,
}

impl FormCTables {
    pub(crate) fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            form_c_notices: Table::new(schema::FORM_C_NOTICES, include_fork_step, encoding),
            form_c_co_issuers: Table::new(schema::FORM_C_CO_ISSUERS, include_fork_step, encoding),
        }
    }

    /// Append the rows of one filing's body, from its proto message and the
    /// values prepared by `crate::sec::prepare::formc::prepare`. Infallible.
    pub(crate) fn append(
        &mut self,
        ctx: &AppendCtx<'_>,
        fc: &FilingCtx<'_>,
        body: &sec::FormCNotice,
        prepared: &PreparedFormC<'_>,
    ) {
        let issuer = body.issuer.as_ref();
        let offering = body.offering.as_ref();
        let of = |field: fn(&sec::FormCOffering) -> &str| offering.map_or("", field);
        let p = &prepared.notice;

        let row = self.form_c_notices.row(ctx);
        row.fc.append(fc);
        row.filer_cik.nz(&body.filer_cik);
        row.issuer_name.nz(issuer.map_or("", |i| i.name.as_str()));
        row.issuer_legal_status_form
            .nz(issuer.map_or("", |i| i.legal_status_form.as_str()));
        row.issuer_legal_status_other_desc
            .nz(issuer.map_or("", |i| i.legal_status_other_desc.as_str()));
        row.issuer_jurisdiction
            .nz(issuer.map_or("", |i| i.jurisdiction.as_str()));
        row.issuer_date_incorporation
            .opt(p.issuer_date_incorporation);
        row.issuer.append(issuer.and_then(|i| i.address.as_ref()));
        row.issuer_website
            .nz(issuer.map_or("", |i| i.website.as_str()));
        row.intermediary_company_name
            .nz(&body.intermediary_company_name);
        row.intermediary_cik.nz(&body.intermediary_cik);
        row.intermediary_file_number
            .nz(&body.intermediary_file_number);
        row.intermediary_crd_number
            .nz(&body.intermediary_crd_number);
        row.issuer_info_is_amendment.opt(body.is_amendment);
        row.nature_of_amendment.nz(&body.nature_of_amendment);
        row.progress_update.nz(&body.progress_update);
        row.is_co_issuer.opt(body.is_co_issuer);
        row.period.opt(p.period);
        row.security_type.nz(of(|o| o.security_type.as_str()));
        row.security_offered_other_desc
            .nz(of(|o| o.security_offered_other_desc.as_str()));
        row.num_securities_offered.opt(p.num_securities_offered);
        row.price.opt(p.price);
        row.price_determination_method
            .nz(of(|o| o.price_determination_method.as_str()));
        row.offering_amount.opt(p.offering_amount);
        row.maximum_offering_amount.opt(p.maximum_offering_amount);
        row.over_subscription_accepted
            .opt(offering.map(|o| o.over_subscription_accepted));
        row.over_subscription_allocation_type
            .nz(of(|o| o.over_subscription_allocation_type.as_str()));
        row.desc_over_subscription
            .nz(of(|o| o.desc_over_subscription.as_str()));
        row.deadline_date.opt(p.deadline_date);
        row.compensation_amount
            .nz(of(|o| o.compensation_amount.as_str()));
        row.financial_interest
            .nz(of(|o| o.financial_interest.as_str()));
        row.offering_jurisdictions
            .items(body.offering_jurisdictions.iter().map(String::as_str));
        row.has_financials.val(body.financials.is_some());
        row.current_employees.opt(p.current_employees);
        // The 18 statement columns, in `prepare::formc::FINANCIAL_COLUMNS` order.
        let statements = [
            &mut row.total_assets_most_recent_fy,
            &mut row.total_assets_prior_fy,
            &mut row.cash_equivalents_most_recent_fy,
            &mut row.cash_equivalents_prior_fy,
            &mut row.accounts_receivable_most_recent_fy,
            &mut row.accounts_receivable_prior_fy,
            &mut row.short_term_debt_most_recent_fy,
            &mut row.short_term_debt_prior_fy,
            &mut row.long_term_debt_most_recent_fy,
            &mut row.long_term_debt_prior_fy,
            &mut row.revenue_most_recent_fy,
            &mut row.revenue_prior_fy,
            &mut row.cost_goods_sold_most_recent_fy,
            &mut row.cost_goods_sold_prior_fy,
            &mut row.tax_paid_most_recent_fy,
            &mut row.tax_paid_prior_fy,
            &mut row.net_income_most_recent_fy,
            &mut row.net_income_prior_fy,
        ];
        for (column, value) in statements.into_iter().zip(p.financials) {
            column.opt(value);
        }
        row.co_issuer_count.val(p.co_issuer_count);
        row.has_parse_issues.val(p.has_parse_issues);

        for (co_issuer, p) in body.co_issuers.iter().zip(&prepared.co_issuers) {
            let row = self.form_c_co_issuers.row(ctx);
            row.fc.append(fc);
            row.filer_cik.nz(&body.filer_cik);
            row.co_issuer_index.val(p.co_issuer_index);
            row.co_issuer_name.nz(&co_issuer.name);
            row.legal_status_form.nz(&co_issuer.legal_status_form);
            row.legal_status_other_desc
                .nz(&co_issuer.legal_status_other_desc);
            row.jurisdiction.nz(&co_issuer.jurisdiction);
            row.date_incorporation.opt(p.date_incorporation);
            row.co_issuer.append(co_issuer.address.as_ref());
            row.website.nz(&co_issuer.website);
            row.has_parse_issues.val(p.has_parse_issues);
        }
    }

    pub(crate) fn tables(&self) -> [&dyn SecTable; 2] {
        [&self.form_c_notices, &self.form_c_co_issuers]
    }

    pub(crate) fn tables_mut(&mut self) -> [&mut dyn SecTable; 2] {
        [&mut self.form_c_notices, &mut self.form_c_co_issuers]
    }
}
