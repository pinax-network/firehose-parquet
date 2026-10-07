//! Preflight of `ownership_documents`, `ownership_reporting_owners`, `ownership_transactions`, `ownership_holdings`, `ownership_footnotes` (§3.9, §3.10, §3.11, §3.12, §3.13).
//! Owned by the `ownership` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! Fallible only on the structural invariants of §4.7 (positions through
//! `super::idx`); every typed value goes through `IssueSink::row`.

use anyhow::Result;

use crate::sec::issues::IssueSink;
use crate::sec::prepare::FilingCtx;
use crate::sec::proto::sec;

/// The parsed and derived values of `ownership_documents`, `ownership_reporting_owners`, `ownership_transactions`, `ownership_holdings`, `ownership_footnotes` for one filing.
#[derive(Debug, Default)]
pub(crate) struct PreparedOwnership<'a> {
    _borrows: std::marker::PhantomData<&'a ()>,
}

pub(crate) fn prepare<'a>(
    fc: &FilingCtx<'a>,
    body: &'a sec::OwnershipDocument,
    issues: &mut IssueSink<'a>,
) -> Result<PreparedOwnership<'a>> {
    // Stub: nothing prepared yet.
    let _ = (fc, body, issues);
    Ok(PreparedOwnership::default())
}
