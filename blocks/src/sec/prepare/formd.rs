//! Preflight of `form_d_notices`, `form_d_co_issuers`, `form_d_related_persons`, `form_d_sales_recipients` (§3.32, §3.33, §3.34, §3.35).
//! Owned by the `smallforms` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! Fallible only on the structural invariants of §4.7 (positions through
//! `super::idx`); every typed value goes through `IssueSink::row`.

use anyhow::Result;

use crate::sec::issues::IssueSink;
use crate::sec::prepare::FilingCtx;
use crate::sec::proto::sec;

/// The parsed and derived values of `form_d_notices`, `form_d_co_issuers`, `form_d_related_persons`, `form_d_sales_recipients` for one filing.
#[derive(Debug, Default)]
pub(crate) struct PreparedFormD<'a> {
    _borrows: std::marker::PhantomData<&'a ()>,
}

pub(crate) fn prepare<'a>(
    fc: &FilingCtx<'a>,
    body: &'a sec::FormDNotice,
    issues: &mut IssueSink<'a>,
) -> Result<PreparedFormD<'a>> {
    // Stub: nothing prepared yet.
    let _ = (fc, body, issues);
    Ok(PreparedFormD::default())
}
