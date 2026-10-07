//! Preflight of `npx_reports`, `npx_votes`, `npx_vote_records`, `npx_other_managers` (§3.36, §3.37, §3.38, §3.39).
//! Owned by the `funds` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! Fallible only on the structural invariants of §4.7 (positions through
//! `super::idx`); every typed value goes through `IssueSink::row`.

use anyhow::Result;

use crate::sec::issues::IssueSink;
use crate::sec::prepare::FilingCtx;
use crate::sec::proto::sec;

/// The parsed and derived values of `npx_reports`, `npx_votes`, `npx_vote_records`, `npx_other_managers` for one filing.
#[derive(Debug, Default)]
pub(crate) struct PreparedNpx<'a> {
    _borrows: std::marker::PhantomData<&'a ()>,
}

pub(crate) fn prepare<'a>(
    fc: &FilingCtx<'a>,
    body: &'a sec::NpxReport,
    issues: &mut IssueSink<'a>,
) -> Result<PreparedNpx<'a>> {
    // Stub: nothing prepared yet.
    let _ = (fc, body, issues);
    Ok(PreparedNpx::default())
}
