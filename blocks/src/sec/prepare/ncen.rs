//! Preflight of `ncen_reports` (§3.40).
//! Owned by the `funds` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! Fallible only on the structural invariants of §4.7 (positions through
//! `super::idx`); every typed value goes through `IssueSink::row`.

use anyhow::Result;

use crate::sec::issues::IssueSink;
use crate::sec::prepare::FilingCtx;
use crate::sec::proto::sec;

/// The parsed and derived values of `ncen_reports` for one filing.
#[derive(Debug, Default)]
pub(crate) struct PreparedNcen<'a> {
    _borrows: std::marker::PhantomData<&'a ()>,
}

pub(crate) fn prepare<'a>(
    fc: &FilingCtx<'a>,
    body: &'a sec::NcenReport,
    issues: &mut IssueSink<'a>,
) -> Result<PreparedNcen<'a>> {
    // Stub: nothing prepared yet.
    let _ = (fc, body, issues);
    Ok(PreparedNcen::default())
}
