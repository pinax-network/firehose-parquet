//! Append phase of `beneficial_reports`, `beneficial_reporting_persons` (§3.17, §3.18).
//! Owned by the `form13f-beneficial` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! The column structs below are generated from `super::super::schema` and checked
//! against it on construction; edit them only together with the schema.

use firehose_parquet::encode::EncodeBytes;

use super::{sec_columns, AppendCtx, SecTable, Table};
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

/// The `authorized_persons` struct members of one element, in schema order:
/// `name`, `phone`, then the 8 address members (all `""`, i.e. NULL, when the
/// address is absent).
fn authorized_person_members(person: &sec::BeneficialAuthorizedPerson) -> [&str; 10] {
    let address = person.address.as_ref();
    let member = |field: fn(&sec::Address) -> &str| address.map_or("", field);
    [
        &person.name,
        &person.phone,
        member(|a| &a.street1),
        member(|a| &a.street2),
        member(|a| &a.city),
        member(|a| &a.state),
        member(|a| &a.zip_code),
        member(|a| &a.state_description),
        member(|a| &a.country),
        member(|a| &a.non_us_state_territory),
    ]
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
        let report = &prepared.report;
        let d = body.items_13d.as_ref();
        let g = body.items_13g.as_ref();
        let d_text = |field: fn(&sec::Schedule13dItems) -> &str| d.map_or("", field);
        let g_text = |field: fn(&sec::Schedule13gItems) -> &str| g.map_or("", field);

        let row = self.beneficial_reports.row(ctx);
        row.fc.append(fc);
        row.subject_company_cik.nz(&body.subject_company_cik);
        row.subject_company_name.nz(&body.subject_company_name);
        row.cusip.nz(&body.cusip);
        row.cusips.items(body.cusips.iter().map(String::as_str));
        row.cusip_norm.opt(report.cusip_norm.as_deref());
        row.schedule_type.nz(&body.schedule_type);
        row.schedule_kind.opt(report.schedule_kind);
        row.filer_cik.nz(&body.filer_cik);
        row.securities_class_title.nz(&body.securities_class_title);
        row.event_date.opt(report.event_date);
        row.issuer.append(body.issuer_address.as_ref());
        row.rules_designated
            .items(body.rules_designated.iter().map(String::as_str));
        row.previous_accession_number
            .nz(&body.previous_accession_number);
        row.amendment_number.opt(report.amendment_number);
        row.previously_filed.opt(body.previously_filed);
        row.item13d_security_title.nz(d_text(|i| &i.security_title));
        row.item13d_issuer_name.nz(d_text(|i| &i.issuer_name));
        row.item13d_issuer_principal
            .append(d.and_then(|i| i.issuer_principal_address.as_ref()));
        row.item13d_item1_comment.nz(d_text(|i| &i.item1_comment));
        row.item13d_filing_person_name
            .nz(d_text(|i| &i.filing_person_name));
        row.item13d_principal_business_address
            .nz(d_text(|i| &i.principal_business_address));
        row.item13d_principal_job.nz(d_text(|i| &i.principal_job));
        row.item13d_has_been_convicted
            .nz(d_text(|i| &i.has_been_convicted));
        row.item13d_conviction_description
            .nz(d_text(|i| &i.conviction_description));
        row.item13d_citizenship.nz(d_text(|i| &i.citizenship));
        row.item13d_funds_source.nz(d_text(|i| &i.funds_source));
        row.item13d_transaction_purpose
            .nz(d_text(|i| &i.transaction_purpose));
        row.item13d_percentage_of_class
            .nz(d_text(|i| &i.percentage_of_class));
        row.item13d_number_of_shares
            .nz(d_text(|i| &i.number_of_shares));
        row.item13d_transaction_description
            .nz(d_text(|i| &i.transaction_description));
        row.item13d_list_of_shareholders
            .nz(d_text(|i| &i.list_of_shareholders));
        row.item13d_date_5_percent_ownership
            .nz(d_text(|i| &i.date_5_percent_ownership));
        row.item13d_contract_description
            .nz(d_text(|i| &i.contract_description));
        row.item13d_filed_exhibits.nz(d_text(|i| &i.filed_exhibits));
        row.item13g_type_of_person_filing.items(
            g.map_or(&[][..], |i| i.type_of_person_filing.as_slice())
                .iter()
                .map(String::as_str),
        );
        row.item13g_other_type_of_person_filing
            .nz(g_text(|i| &i.other_type_of_person_filing));
        row.item13g_amount_beneficially_owned
            .nz(g_text(|i| &i.amount_beneficially_owned));
        row.item13g_class_percent.nz(g_text(|i| &i.class_percent));
        row.item13g_sole_voting_power
            .nz(g_text(|i| &i.sole_voting_power));
        row.item13g_shared_voting_power
            .nz(g_text(|i| &i.shared_voting_power));
        row.item13g_sole_dispositive_power
            .nz(g_text(|i| &i.sole_dispositive_power));
        row.item13g_shared_dispositive_power
            .nz(g_text(|i| &i.shared_dispositive_power));
        row.item13g_class_ownership_5_percent_or_less
            .opt(g.and_then(|i| i.class_ownership_5_percent_or_less));
        row.item13g_ownership_on_behalf_of_another
            .nz(g_text(|i| &i.ownership_on_behalf_of_another));
        row.item13g_subsidiary_identification
            .nz(g_text(|i| &i.subsidiary_identification));
        row.item13g_group_members_identification
            .nz(g_text(|i| &i.group_members_identification));
        row.item13g_group_dissolution_notice
            .nz(g_text(|i| &i.group_dissolution_notice));
        row.item13g_certifications.nz(g_text(|i| &i.certifications));
        row.item13g_issuer_name.nz(g_text(|i| &i.issuer_name));
        row.item13g_issuer_principal_office_address
            .nz(g_text(|i| &i.issuer_principal_office_address));
        row.item13g_filing_person_name
            .nz(g_text(|i| &i.filing_person_name));
        row.item13g_principal_business_office_address
            .nz(g_text(|i| &i.principal_business_office_address));
        row.item13g_citizenship.nz(g_text(|i| &i.citizenship));
        row.exhibit_info.nz(&body.exhibit_info);
        row.signature_comments.nz(&body.signature_comments);
        row.authorized_persons.utf8_items(
            body.authorized_persons
                .iter()
                .map(authorized_person_members),
        );
        row.reporting_person_count
            .val(report.reporting_person_count);
        row.max_aggregate_amount_owned
            .opt(report.max_aggregate_amount_owned);
        row.max_percent_of_class.opt(report.max_percent_of_class);
        row.signature_count.val(report.signature_count);
        row.has_parse_issues.val(report.has_parse_issues);

        for (person, p) in body.reporting_persons.iter().zip(&prepared.persons) {
            let row = self.beneficial_reporting_persons.row(ctx);
            row.fc.append(fc);
            row.subject_company_cik.nz(&body.subject_company_cik);
            row.subject_company_name.nz(&body.subject_company_name);
            row.cusip_norm.opt(report.cusip_norm.as_deref());
            row.schedule_kind.opt(report.schedule_kind);
            row.event_date.opt(report.event_date);
            row.person_index.val(p.person_index);
            row.person_cik.nz(&person.cik);
            row.person_name.nz(&person.name);
            row.no_cik.opt(person.no_cik);
            row.member_of_group.nz(&person.member_of_group);
            row.fund_type.nz(&person.fund_type);
            row.fund_types
                .items(person.fund_types.iter().map(String::as_str));
            row.citizenship.nz(&person.citizenship);
            row.sole_voting_power.opt(p.sole_voting_power);
            row.shared_voting_power.opt(p.shared_voting_power);
            row.sole_dispositive_power.opt(p.sole_dispositive_power);
            row.shared_dispositive_power.opt(p.shared_dispositive_power);
            row.aggregate_amount_owned.opt(p.aggregate_amount_owned);
            row.aggregate_excludes_shares
                .opt(person.aggregate_excludes_shares);
            row.percent_of_class.opt(p.percent_of_class);
            row.type_of_reporting_person
                .nz(&person.type_of_reporting_person);
            row.types_of_reporting_person
                .items(person.types_of_reporting_person.iter().map(String::as_str));
            row.legal_proceedings.opt(person.legal_proceedings);
            row.comment.nz(&person.comment);
            row.has_parse_issues.val(p.has_parse_issues);
        }
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
