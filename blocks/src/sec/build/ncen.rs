//! Append phase of `ncen_reports` (§3.40).
//! Owned by the `funds` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! The column structs below are generated from `super::super::schema` and checked
//! against it on construction; edit them only together with the schema.

use firehose_parquet::encode::EncodeBytes;

use super::{sec_columns, AppendCtx, SecTable, Table};
#[allow(unused_imports)]
use super::{Addr, Bool, Date, Fc, ListStr, Str, U32};
use crate::sec::prepare::ncen::PreparedNcen;
use crate::sec::prepare::FilingCtx;
use crate::sec::proto::sec;
use crate::sec::schema;

sec_columns! {
    /// The columns of `ncen_reports` (§3.40), in schema order.
    pub(crate) struct NcenReportsCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub filer_cik: Str,
        pub investment_company_type: Str,
        pub report_ending_period: Date,
        pub is_report_period_lt12: Bool,
        pub previous_accession_number: Str,
        pub registrant_name: Str,
        pub registrant_file_number: Str,
        pub registrant_cik: Str,
        pub registrant_lei: Str,
        /// `registrant_street1` … `registrant_non_us_state_territory`.
        pub registrant: Addr,
        pub registrant_phone: Str,
        pub series_ids: ListStr,
        pub series_count: U32,
        pub has_parse_issues: Bool,
    }
}

/// Every table of this module.
pub(crate) struct NcenTables {
    pub(crate) ncen_reports: Table<NcenReportsCols>,
}

impl NcenTables {
    pub(crate) fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            ncen_reports: Table::new(schema::NCEN_REPORTS, include_fork_step, encoding),
        }
    }

    /// Append the rows of one filing's body, from its proto message and the
    /// values prepared by `crate::sec::prepare::ncen::prepare`. Infallible.
    pub(crate) fn append(
        &mut self,
        ctx: &AppendCtx<'_>,
        fc: &FilingCtx<'_>,
        body: &sec::NcenReport,
        prepared: &PreparedNcen<'_>,
    ) {
        // Stub: no rows yet.
        let _ = (ctx, fc, body, prepared);
    }

    pub(crate) fn tables(&self) -> [&dyn SecTable; 1] {
        [&self.ncen_reports]
    }

    pub(crate) fn tables_mut(&mut self) -> [&mut dyn SecTable; 1] {
        [&mut self.ncen_reports]
    }
}
