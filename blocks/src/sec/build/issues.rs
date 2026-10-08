//! Append phase of `parse_issues` (§3.43): one row per [`PreparedIssue`], block-level
//! issues first, then each filing's in recording order.

use firehose_parquet::encode::EncodeBytes;

use super::{sec_columns, AppendCtx, SecTable, Table};
use super::{Dict, Str, U32};
use crate::sec::issues::PreparedIssue;
use crate::sec::prepare::PreparedBlock;
use crate::sec::schema;

sec_columns! {
    /// The columns of `parse_issues` (§3.43), in schema order.
    pub(crate) struct ParseIssuesCols {
        pub filing_index: U32,
        pub accession_number: Str,
        pub table_name: Dict,
        pub column_name: Dict,
        pub index_1: U32,
        pub index_2: U32,
        pub index_3: U32,
        pub raw_value: Str,
        pub issue: Dict,
    }
}

/// `parse_issues`.
pub(crate) struct IssueTables {
    pub(crate) parse_issues: Table<ParseIssuesCols>,
}

impl IssueTables {
    pub(crate) fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            parse_issues: Table::new(schema::PARSE_ISSUES, include_fork_step, encoding),
        }
    }

    /// Every issue of the block: block-level ones (`filing_index` NULL), then
    /// each filing's.
    pub(crate) fn append(&mut self, ctx: &AppendCtx<'_>, block: &PreparedBlock<'_>) {
        for issue in block.issues.issues() {
            self.append_issue(ctx, None, issue);
        }
        for filing in &block.filings {
            let key = (filing.fc.filing_index, filing.fc.accession_number);
            for issue in filing.issues.issues() {
                self.append_issue(ctx, Some(key), issue);
            }
        }
    }

    fn append_issue(
        &mut self,
        ctx: &AppendCtx<'_>,
        filing: Option<(u32, &str)>,
        issue: &PreparedIssue<'_>,
    ) {
        let row = self.parse_issues.row(ctx);
        row.filing_index.opt(filing.map(|(index, _)| index));
        row.accession_number
            .opt(filing.map(|(_, accession)| accession));
        row.table_name.val(issue.table);
        row.column_name.val(issue.column);
        row.index_1.opt(issue.index[0]);
        row.index_2.opt(issue.index[1]);
        row.index_3.opt(issue.index[2]);
        row.raw_value.val(&issue.raw);
        row.issue.val(issue.kind.label());
    }

    pub(crate) fn tables(&self) -> [&dyn SecTable; 1] {
        [&self.parse_issues]
    }

    pub(crate) fn tables_mut(&mut self) -> [&mut dyn SecTable; 1] {
        [&mut self.parse_issues]
    }
}
