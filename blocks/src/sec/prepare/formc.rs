//! Preflight of `form_c_notices`, `form_c_co_issuers` (§3.41, §3.42).
//! Owned by the `smallforms` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! Fallible only on the structural invariants of §4.7 (positions through
//! `super::idx`); every typed value goes through `IssueSink::row`.

use anyhow::Result;

use crate::sec::issues::IssueSink;
use crate::sec::prepare::FilingCtx;
use crate::sec::proto::sec;

/// The parsed and derived values of `form_c_notices`, `form_c_co_issuers` for one filing.
#[derive(Debug, Default)]
pub(crate) struct PreparedFormC<'a> {
    _borrows: std::marker::PhantomData<&'a ()>,
}

pub(crate) fn prepare<'a>(
    fc: &FilingCtx<'a>,
    body: &'a sec::FormCNotice,
    issues: &mut IssueSink<'a>,
) -> Result<PreparedFormC<'a>> {
    // Stub: nothing prepared yet.
    let _ = (fc, body, issues);
    Ok(PreparedFormC::default())
}
