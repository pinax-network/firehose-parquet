//! Append phase of the hub tables `blocks` (§3.1) and `filings` (§3.2).

use firehose_parquet::encode::EncodeBytes;

use super::{sec_columns, AppendCtx, SecTable, Table};
use super::{Bool, Date, Dict, ListStr, Str, Ts, I32, U32};
use crate::sec::prepare::hub::{PreparedBlockRow, PreparedFilingRow};
use crate::sec::prepare::FilingCtx;
use crate::sec::proto::sec;
use crate::sec::schema;

sec_columns! {
    /// The columns of `blocks` (§3.1), in schema order.
    pub(crate) struct BlocksCols {
        pub feed_date: Date,
        pub filing_count: U32,
        pub has_parse_issues: Bool,
    }
}

sec_columns! {
    /// The columns of `filings` (§3.2), in schema order.
    pub(crate) struct FilingsCols {
        pub filing_index: U32,
        pub accession_number: Str,
        pub form_type: Str,
        pub base_form_type: Str,
        pub is_amendment: Bool,
        pub body_kind: Dict,
        pub cik: Str,
        pub cik_role: Str,
        pub company_name: Str,
        pub issuer_cik: Str,
        pub issuer_name: Str,
        pub filer_cik: Str,
        pub filer_name: Str,
        pub filing_date: Date,
        pub period_of_report: Date,
        pub acceptance_datetime: Ts,
        pub acceptance_in_block_window: Bool,
        pub dissemination_lag_days: I32,
        pub primary_document: Str,
        pub amended_accession: Str,
        pub source_path: Str,
        pub dissemination_flags: ListStr,
        pub dissemination_timestamp: Str,
        pub is_deletion_notice: Bool,
        pub group_members: ListStr,
        pub party_count: U32,
        pub document_count: U32,
        pub series_count: U32,
        pub raw_reason: Str,
        pub raw_detail: Str,
        pub has_raw_xml: Bool,
        pub raw_xml_size: U32,
        pub has_parse_issues: Bool,
    }
}

/// `blocks` and `filings`.
pub(crate) struct HubTables {
    pub(crate) blocks: Table<BlocksCols>,
    pub(crate) filings: Table<FilingsCols>,
}

impl HubTables {
    pub(crate) fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            blocks: Table::new(schema::BLOCKS, include_fork_step, encoding),
            filings: Table::new(schema::FILINGS, include_fork_step, encoding),
        }
    }

    /// The block's row: written for every block, empty windows included.
    pub(crate) fn append_block(&mut self, ctx: &AppendCtx<'_>, prepared: &PreparedBlockRow) {
        let row = self.blocks.row(ctx);
        row.feed_date.opt(prepared.feed_date);
        row.filing_count.val(prepared.filing_count);
        row.has_parse_issues.val(prepared.has_parse_issues);
    }

    pub(crate) fn append_filing(
        &mut self,
        ctx: &AppendCtx<'_>,
        fc: &FilingCtx<'_>,
        filing: &sec::Filing,
        prepared: &PreparedFilingRow<'_>,
    ) {
        let row = self.filings.row(ctx);
        row.filing_index.val(fc.filing_index);
        row.accession_number.val(fc.accession_number);
        row.form_type.val(fc.form_type);
        row.base_form_type.val(prepared.base_form_type);
        row.is_amendment.val(filing.is_amendment);
        row.body_kind.opt(prepared.body_kind);
        row.cik.nz(&filing.cik);
        row.cik_role.nz(&filing.cik_role);
        row.company_name.nz(&filing.company_name);
        row.issuer_cik
            .nz(prepared.issuer.map_or("", |party| party.cik.as_str()));
        row.issuer_name
            .nz(prepared.issuer.map_or("", |party| party.name.as_str()));
        row.filer_cik
            .nz(prepared.filer.map_or("", |party| party.cik.as_str()));
        row.filer_name
            .nz(prepared.filer.map_or("", |party| party.name.as_str()));
        row.filing_date.opt(fc.filing_date);
        row.period_of_report.opt(prepared.period_of_report);
        row.acceptance_datetime.opt(fc.acceptance_ms);
        row.acceptance_in_block_window
            .opt(prepared.acceptance_in_block_window);
        row.dissemination_lag_days
            .opt(prepared.dissemination_lag_days);
        row.primary_document.nz(&filing.primary_document);
        row.amended_accession.nz(&filing.amended_accession);
        row.source_path.nz(&filing.source_path);
        row.dissemination_flags
            .items(filing.dissemination_flags.iter().map(String::as_str));
        row.dissemination_timestamp
            .nz(&filing.dissemination_timestamp);
        row.is_deletion_notice.val(prepared.is_deletion_notice);
        row.group_members
            .items(filing.group_members.iter().map(String::as_str));
        row.party_count.val(prepared.party_count);
        row.document_count.val(prepared.document_count);
        row.series_count.val(prepared.series_count);
        row.raw_reason
            .nz(prepared.raw.map_or("", |raw| raw.reason.as_str()));
        row.raw_detail
            .nz(prepared.raw.map_or("", |raw| raw.detail.as_str()));
        row.has_raw_xml.val(prepared.raw_xml_size > 0);
        row.raw_xml_size.val(prepared.raw_xml_size);
        row.has_parse_issues.val(prepared.has_parse_issues);
    }

    pub(crate) fn tables(&self) -> [&dyn SecTable; 2] {
        [&self.blocks, &self.filings]
    }

    pub(crate) fn tables_mut(&mut self) -> [&mut dyn SecTable; 2] {
        [&mut self.blocks, &mut self.filings]
    }
}
