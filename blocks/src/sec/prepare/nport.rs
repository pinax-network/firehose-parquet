//! Preflight of `nport_reports`, `nport_monthly_returns`, `nport_monthly_activity`, `nport_holdings`, `nport_debt_reference_instruments`, `nport_debt_conversion_currencies`, `nport_derivatives`, `nport_derivative_swap_legs`, `nport_derivative_index_components` (§3.23, §3.24, §3.25, §3.26, §3.27, §3.28, §3.29, §3.30, §3.31).
//! Owned by the `nport` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! Fallible only on the structural invariants of §4.7 (positions through
//! `super::idx`); every typed value goes through `IssueSink::row`.

use anyhow::Result;

use crate::sec::issues::IssueSink;
use crate::sec::prepare::FilingCtx;
use crate::sec::proto::sec;

/// The parsed and derived values of `nport_reports`, `nport_monthly_returns`, `nport_monthly_activity`, `nport_holdings`, `nport_debt_reference_instruments`, `nport_debt_conversion_currencies`, `nport_derivatives`, `nport_derivative_swap_legs`, `nport_derivative_index_components` for one filing.
#[derive(Debug, Default)]
pub(crate) struct PreparedNport<'a> {
    _borrows: std::marker::PhantomData<&'a ()>,
}

pub(crate) fn prepare<'a>(
    fc: &FilingCtx<'a>,
    body: &'a sec::NportReport,
    issues: &mut IssueSink<'a>,
) -> Result<PreparedNport<'a>> {
    // Stub: nothing prepared yet.
    let _ = (fc, body, issues);
    Ok(PreparedNport::default())
}
