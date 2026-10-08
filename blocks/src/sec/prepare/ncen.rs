//! Preflight of `ncen_reports` (§3.40).
//! Owned by the `funds` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! Fallible only on the structural invariants of §4.7 (positions through
//! `super::idx`); every typed value goes through `IssueSink::row`.

use anyhow::Result;

use super::{idx, FilingCtx};
use crate::sec::issues::IssueSink;
use crate::sec::proto::sec;
use crate::sec::schema::NCEN_REPORTS;

/// The parsed and derived values of `ncen_reports` for one filing. Verbatim
/// strings are read from the proto during the append.
#[derive(Debug, Default)]
pub(crate) struct PreparedNcen<'a> {
    pub report_ending_period: Option<i32>,
    /// `len(series_ids)`.
    pub series_count: u32,
    pub has_parse_issues: bool,
    _borrows: std::marker::PhantomData<&'a ()>,
}

pub(crate) fn prepare<'a>(
    fc: &FilingCtx<'a>,
    body: &'a sec::NcenReport,
    issues: &mut IssueSink<'a>,
) -> Result<PreparedNcen<'a>> {
    let _ = fc;
    let series_count = idx(body.series_ids.len())?;
    let mut row = issues.row(NCEN_REPORTS, &[]);
    let report_ending_period = row.date("report_ending_period", &body.report_ending_period);
    Ok(PreparedNcen {
        report_ending_period,
        series_count,
        has_parse_issues: row.finish(),
        _borrows: std::marker::PhantomData,
    })
}
