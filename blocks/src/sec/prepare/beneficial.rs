//! Preflight of `beneficial_reports`, `beneficial_reporting_persons` (§3.17, §3.18).
//! Owned by the `form13f-beneficial` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! Fallible only on the structural invariants of §4.7 (positions through
//! `super::idx`); every typed value goes through `IssueSink::row`.

use anyhow::Result;

use crate::sec::issues::IssueSink;
use crate::sec::prepare::FilingCtx;
use crate::sec::proto::sec;

/// The parsed and derived values of `beneficial_reports`, `beneficial_reporting_persons` for one filing.
#[derive(Debug, Default)]
pub(crate) struct PreparedBeneficial<'a> {
    _borrows: std::marker::PhantomData<&'a ()>,
}

pub(crate) fn prepare<'a>(
    fc: &FilingCtx<'a>,
    body: &'a sec::BeneficialOwnershipReport,
    issues: &mut IssueSink<'a>,
) -> Result<PreparedBeneficial<'a>> {
    // Stub: nothing prepared yet.
    let _ = (fc, body, issues);
    Ok(PreparedBeneficial::default())
}
