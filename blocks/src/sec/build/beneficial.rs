//! Append phase of `beneficial_reports`, `beneficial_reporting_persons` (§3.17, §3.18).
//! Owned by the `form13f-beneficial` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! The column structs below are generated from `super::super::schema` and checked
//! against it on construction; edit them only together with the schema.

use firehose_parquet::encode::EncodeBytes;

use super::{sec_columns, AppendCtx, SecTable, Table};
#[allow(unused_imports)]
use super::{Addr, Bool, Date, Dec, Dict, Fc, ListStr, ListStruct, Str, I32, U32};
use crate::sec::prepare::beneficial::PreparedBeneficial;
use crate::sec::prepare::FilingCtx;
use crate::sec::proto::sec;
use crate::sec::schema;

sec_columns! {
    /// The columns of `beneficial_reports` (§3.17), in schema order.
    pub(crate) struct BeneficialReportsCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub subject_company_cik: Str,
        pub subject_company_name: Str,
        pub cusip: Str,
        pub cusips: ListStr,
        pub cusip_norm: Str,
        pub schedule_type: Str,
        pub schedule_kind: Dict,
        pub filer_cik: Str,
        pub securities_class_title: Str,
        pub event_date: Date,
        /// `issuer_street1` … `issuer_non_us_state_territory`.
        pub issuer: Addr,
        pub rules_designated: ListStr,
        pub previous_accession_number: Str,
        pub amendment_number: I32,
        pub previously_filed: Bool,
        pub item13d_security_title: Str,
        pub item13d_issuer_name: Str,
        /// `item13d_issuer_principal_street1` … `item13d_issuer_principal_non_us_state_territory`.
        pub item13d_issuer_principal: Addr,
        pub item13d_item1_comment: Str,
        pub item13d_filing_person_name: Str,
        pub item13d_principal_business_address: Str,
        pub item13d_principal_job: Str,
        pub item13d_has_been_convicted: Str,
        pub item13d_conviction_description: Str,
        pub item13d_citizenship: Str,
        pub item13d_funds_source: Str,
        pub item13d_transaction_purpose: Str,
        pub item13d_percentage_of_class: Str,
        pub item13d_number_of_shares: Str,
        pub item13d_transaction_description: Str,
        pub item13d_list_of_shareholders: Str,
        pub item13d_date_5_percent_ownership: Str,
        pub item13d_contract_description: Str,
        pub item13d_filed_exhibits: Str,
        pub item13g_type_of_person_filing: ListStr,
        pub item13g_other_type_of_person_filing: Str,
        pub item13g_amount_beneficially_owned: Str,
        pub item13g_class_percent: Str,
        pub item13g_sole_voting_power: Str,
        pub item13g_shared_voting_power: Str,
        pub item13g_sole_dispositive_power: Str,
        pub item13g_shared_dispositive_power: Str,
        pub item13g_class_ownership_5_percent_or_less: Bool,
        pub item13g_ownership_on_behalf_of_another: Str,
        pub item13g_subsidiary_identification: Str,
        pub item13g_group_members_identification: Str,
        pub item13g_group_dissolution_notice: Str,
        pub item13g_certifications: Str,
        pub item13g_issuer_name: Str,
        pub item13g_issuer_principal_office_address: Str,
        pub item13g_filing_person_name: Str,
        pub item13g_principal_business_office_address: Str,
        pub item13g_citizenship: Str,
        pub exhibit_info: Str,
        pub signature_comments: Str,
        pub authorized_persons: ListStruct,
        pub reporting_person_count: U32,
        pub max_aggregate_amount_owned: Dec,
        pub max_percent_of_class: Dec,
        pub signature_count: U32,
        pub has_parse_issues: Bool,
    }
}

sec_columns! {
    /// The columns of `beneficial_reporting_persons` (§3.18), in schema order.
    pub(crate) struct BeneficialReportingPersonsCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub subject_company_cik: Str,
        pub subject_company_name: Str,
        pub cusip_norm: Str,
        pub schedule_kind: Dict,
        pub event_date: Date,
        pub person_index: U32,
        pub person_cik: Str,
        pub person_name: Str,
        pub no_cik: Bool,
        pub member_of_group: Str,
        pub fund_type: Str,
        pub fund_types: ListStr,
        pub citizenship: Str,
        pub sole_voting_power: Dec,
        pub shared_voting_power: Dec,
        pub sole_dispositive_power: Dec,
        pub shared_dispositive_power: Dec,
        pub aggregate_amount_owned: Dec,
        pub aggregate_excludes_shares: Bool,
        pub percent_of_class: Dec,
        pub type_of_reporting_person: Str,
        pub types_of_reporting_person: ListStr,
        pub legal_proceedings: Bool,
        pub comment: Str,
        pub has_parse_issues: Bool,
    }
}

/// Every table of this module.
pub(crate) struct BeneficialTables {
    pub(crate) beneficial_reports: Table<BeneficialReportsCols>,
    pub(crate) beneficial_reporting_persons: Table<BeneficialReportingPersonsCols>,
}

impl BeneficialTables {
    pub(crate) fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            beneficial_reports: Table::new(schema::BENEFICIAL_REPORTS, include_fork_step, encoding),
            beneficial_reporting_persons: Table::new(
                schema::BENEFICIAL_REPORTING_PERSONS,
                include_fork_step,
                encoding,
            ),
        }
    }

    /// Append the rows of one filing's body, from its proto message and the
    /// values prepared by `crate::sec::prepare::beneficial::prepare`. Infallible.
    pub(crate) fn append(
        &mut self,
        ctx: &AppendCtx<'_>,
        fc: &FilingCtx<'_>,
        body: &sec::BeneficialOwnershipReport,
        prepared: &PreparedBeneficial<'_>,
    ) {
        // Stub: no rows yet.
        let _ = (ctx, fc, body, prepared);
    }

    pub(crate) fn tables(&self) -> [&dyn SecTable; 2] {
        [&self.beneficial_reports, &self.beneficial_reporting_persons]
    }

    pub(crate) fn tables_mut(&mut self) -> [&mut dyn SecTable; 2] {
        [
            &mut self.beneficial_reports,
            &mut self.beneficial_reporting_persons,
        ]
    }
}
