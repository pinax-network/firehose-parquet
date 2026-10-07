//! Preflight of `form13f_reports`, `form13f_other_managers`, `form13f_holdings` (§3.14, §3.15, §3.16).
//! Owned by the `form13f-beneficial` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! Fallible only on the structural invariants of §4.7 (positions through
//! `super::idx`); every typed value goes through `IssueSink::row`.

use anyhow::Result;

use crate::sec::issues::IssueSink;
use crate::sec::prepare::FilingCtx;
use crate::sec::proto::sec;

/// The parsed and derived values of `form13f_reports`, `form13f_other_managers`, `form13f_holdings` for one filing.
#[derive(Debug, Default)]
pub(crate) struct PreparedForm13f<'a> {
    _borrows: std::marker::PhantomData<&'a ()>,
}

pub(crate) fn prepare<'a>(
    fc: &FilingCtx<'a>,
    body: &'a sec::Form13fReport,
    issues: &mut IssueSink<'a>,
) -> Result<PreparedForm13f<'a>> {
    // Stub: nothing prepared yet.
    let _ = (fc, body, issues);
    Ok(PreparedForm13f::default())
}
