//! Preflight of `form144_notices`, `form144_securities_information`, `form144_securities_to_be_sold`, `form144_sales_past_3_months` (§3.19, §3.20, §3.21, §3.22).
//! Owned by the `smallforms` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! Fallible only on the structural invariants of §4.7 (positions through
//! `super::idx`); every typed value goes through `IssueSink::row`.

use anyhow::Result;

use crate::sec::issues::IssueSink;
use crate::sec::prepare::FilingCtx;
use crate::sec::proto::sec;

/// The parsed and derived values of `form144_notices`, `form144_securities_information`, `form144_securities_to_be_sold`, `form144_sales_past_3_months` for one filing.
#[derive(Debug, Default)]
pub(crate) struct PreparedForm144<'a> {
    _borrows: std::marker::PhantomData<&'a ()>,
}

pub(crate) fn prepare<'a>(
    fc: &FilingCtx<'a>,
    body: &'a sec::Form144Notice,
    issues: &mut IssueSink<'a>,
) -> Result<PreparedForm144<'a>> {
    // Stub: nothing prepared yet.
    let _ = (fc, body, issues);
    Ok(PreparedForm144::default())
}
