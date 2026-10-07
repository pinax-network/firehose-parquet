//! Preflight of `filing_raw_xml`, `filing_parties`, `filing_documents`, `filing_series`, `filing_series_classes`, `filing_signatures` (§3.3, §3.4, §3.5, §3.6, §3.7, §3.8).
//! Owned by the `envelope` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! Fallible only on the structural invariants of §4.7 (positions through
//! `super::idx`); every typed value goes through `IssueSink::row`.

use anyhow::Result;

use crate::sec::issues::IssueSink;
use crate::sec::prepare::FilingCtx;
use crate::sec::proto::sec;

/// The parsed and derived values of `filing_raw_xml`, `filing_parties`, `filing_documents`, `filing_series`, `filing_series_classes`, `filing_signatures` for one filing.
#[derive(Debug, Default)]
pub(crate) struct PreparedEnvelope<'a> {
    _borrows: std::marker::PhantomData<&'a ()>,
}

pub(crate) fn prepare<'a>(
    fc: &FilingCtx<'a>,
    filing: &'a sec::Filing,
    issues: &mut IssueSink<'a>,
) -> Result<PreparedEnvelope<'a>> {
    // Stub: nothing prepared yet.
    let _ = (fc, filing, issues);
    Ok(PreparedEnvelope::default())
}
