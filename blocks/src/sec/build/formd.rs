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
        let issuer = body.primary_issuer.as_ref();
        let issuer_cik = issuer.map_or("", |i| i.cik.as_str());
        let offering = body.offering.as_ref();
        let of = |field: fn(&sec::OfferingData) -> &str| offering.map_or("", field);
        let p = &prepared.notice;

        let row = self.form_d_notices.row(ctx);
        row.fc.append(fc);
        row.schema_version.nz(&body.schema_version);
        row.submission_type.nz(&body.submission_type);
        row.previous_accession_number
            .nz(&body.previous_accession_number);
        row.issuer_cik.nz(issuer_cik);
        row.issuer_name
            .nz(issuer.map_or("", |i| i.entity_name.as_str()));
        row.issuer.append(issuer.and_then(|i| i.address.as_ref()));
        row.issuer_phone.nz(issuer.map_or("", |i| i.phone.as_str()));
        row.jurisdiction_of_inc
            .nz(issuer.map_or("", |i| i.jurisdiction_of_inc.as_str()));
        row.entity_type
            .nz(issuer.map_or("", |i| i.entity_type.as_str()));
        row.entity_type_other_desc
            .nz(issuer.map_or("", |i| i.entity_type_other_desc.as_str()));
        row.year_of_inc.opt(p.year_of_inc);
        row.year_of_inc_status
            .nz(issuer.map_or("", |i| i.year_of_inc_status.as_str()));
        row.issuer_previous_names.items(
            issuer
                .into_iter()
                .flat_map(|i| i.previous_names.iter().map(String::as_str)),
        );
        row.issuer_edgar_previous_names.items(
            issuer
                .into_iter()
                .flat_map(|i| i.edgar_previous_names.iter().map(String::as_str)),
        );
        row.industry_group.nz(of(|o| o.industry_group.as_str()));
        row.investment_fund_type
            .nz(of(|o| o.investment_fund_type.as_str()));
        row.is_40_act.opt(offering.and_then(|o| o.is_40_act));
        row.revenue_range.nz(of(|o| o.revenue_range.as_str()));
        row.aggregate_net_asset_value_range
            .nz(of(|o| o.aggregate_net_asset_value_range.as_str()));
        row.offering_is_amendment
            .opt(offering.map(|o| o.is_amendment));
        row.date_of_first_sale.opt(p.date_of_first_sale);
        row.date_of_first_sale_yet_to_occur
            .opt(offering.and_then(|o| o.date_of_first_sale_yet_to_occur));
        row.more_than_one_year
            .opt(offering.map(|o| o.more_than_one_year));
        row.is_equity_type.opt(offering.map(|o| o.is_equity_type));
        row.securities_types.items(
            offering
                .into_iter()
                .flat_map(|o| o.securities_types.iter().map(String::as_str)),
        );
        row.description_of_other_type
            .nz(of(|o| o.description_of_other_type.as_str()));
        row.is_business_combination
            .opt(offering.and_then(|o| o.is_business_combination));
        row.business_combination_clarification
            .nz(of(|o| o.business_combination_clarification.as_str()));
        row.federal_exemptions.items(
            offering
                .into_iter()
                .flat_map(|o| o.federal_exemptions.iter().map(String::as_str)),
        );
        row.minimum_investment.opt(p.minimum_investment);
        row.total_offering_amount.opt(p.total_offering_amount);
        row.total_offering_amount_is_indefinite
            .val(p.total_offering_amount_is_indefinite);
        row.total_amount_sold.opt(p.total_amount_sold);
        row.total_remaining.opt(p.total_remaining);
        row.total_remaining_is_indefinite
            .val(p.total_remaining_is_indefinite);
        row.offering_sales_amounts_clarification
            .nz(of(|o| o.offering_sales_amounts_clarification.as_str()));
        row.has_non_accredited_investors
            .opt(offering.map(|o| o.has_non_accredited_investors));
        row.number_non_accredited_investors
            .opt(p.number_non_accredited_investors);
        row.total_number_already_invested
            .opt(p.total_number_already_invested);
        row.sales_commissions.opt(p.sales_commissions);
        row.sales_commissions_is_estimate
            .opt(offering.and_then(|o| o.sales_commissions_is_estimate));
        row.finders_fees.opt(p.finders_fees);
        row.finders_fees_is_estimate
            .opt(offering.and_then(|o| o.finders_fees_is_estimate));
        row.sales_commissions_clarification
            .nz(of(|o| o.sales_commissions_clarification.as_str()));
        row.gross_proceeds_used.opt(p.gross_proceeds_used);
        row.gross_proceeds_used_is_estimate
            .opt(offering.and_then(|o| o.gross_proceeds_used_is_estimate));
        row.use_of_proceeds_clarification
            .nz(of(|o| o.use_of_proceeds_clarification.as_str()));
        row.authorized_representative
            .opt(body.authorized_representative);
        row.co_issuer_count.val(p.co_issuer_count);
        row.related_person_count.val(p.related_person_count);
        row.sales_recipient_count.val(p.sales_recipient_count);
        row.has_parse_issues.val(p.has_parse_issues);

        for (co_issuer, p) in body.issuers.iter().zip(&prepared.co_issuers) {
            let row = self.form_d_co_issuers.row(ctx);
            row.fc.append(fc);
            row.issuer_cik.nz(issuer_cik);
            row.co_issuer_index.val(p.co_issuer_index);
            row.co_issuer_cik.nz(&co_issuer.cik);
            row.co_issuer_name.nz(&co_issuer.entity_name);
            row.co_issuer.append(co_issuer.address.as_ref());
            row.co_issuer_phone.nz(&co_issuer.phone);
            row.jurisdiction_of_inc.nz(&co_issuer.jurisdiction_of_inc);
            row.entity_type.nz(&co_issuer.entity_type);
            row.entity_type_other_desc
                .nz(&co_issuer.entity_type_other_desc);
            row.year_of_inc.opt(p.year_of_inc);
            row.year_of_inc_status.nz(&co_issuer.year_of_inc_status);
            row.previous_names
                .items(co_issuer.previous_names.iter().map(String::as_str));
            row.edgar_previous_names
                .items(co_issuer.edgar_previous_names.iter().map(String::as_str));
            row.has_parse_issues.val(p.has_parse_issues);
        }

        // Positions `0..related_person_count`, checked in preflight.
        for (person_index, person) in
            (0..prepared.notice.related_person_count).zip(&body.related_persons)
        {
            let row = self.form_d_related_persons.row(ctx);
            row.fc.append(fc);
            row.issuer_cik.nz(issuer_cik);
            row.person_index.val(person_index);
            row.first_name.nz(&person.first_name);
            row.middle_name.nz(&person.middle_name);
            row.last_name.nz(&person.last_name);
            row.person.append(person.address.as_ref());
            row.relationships
                .items(person.relationships.iter().map(String::as_str));
            row.relationship_clarification
                .nz(&person.relationship_clarification);
        }

        // Positions `0..sales_recipient_count`, checked in preflight.
        for (recipient_index, recipient) in
            (0..prepared.notice.sales_recipient_count).zip(&body.sales_compensation_recipients)
        {
            let row = self.form_d_sales_recipients.row(ctx);
            row.fc.append(fc);
            row.issuer_cik.nz(issuer_cik);
            row.recipient_index.val(recipient_index);
            row.recipient_name.nz(&recipient.name);
            row.recipient_crd_number.nz(&recipient.crd_number);
            row.associated_bd_name.nz(&recipient.associated_bd_name);
            row.associated_bd_crd_number
                .nz(&recipient.associated_bd_crd_number);
            row.recipient.append(recipient.address.as_ref());
            row.states_of_solicitation
                .items(recipient.states_of_solicitation.iter().map(String::as_str));
            row.foreign_solicitation.opt(recipient.foreign_solicitation);
        }
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
