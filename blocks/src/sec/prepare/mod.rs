//! Preflight: every fallible step of one block, before any builder is touched.
//!
//! [`prepare_block`] checks the structural invariants of §4.7 and parses every
//! typed value of every row into a [`PreparedBlock`]: parsed scalars, positions,
//! derived values and `parse_issues` rows, with strings borrowed from the
//! decoded block. Only §4.7 invariants are errors; EDGAR content never is.
//! The append phase (`super::build`) then runs infallibly.
//!
//! Per filing the order is fixed, and it is the `parse_issues` row order:
//! `filings` row ([`hub`]), envelope child tables ([`envelope`]), then the body
//! tables (one module per body kind). Each body module exposes
//!
//! ```ignore
//! pub(crate) struct Prepared<Body><'a> { … }
//! pub(crate) fn prepare<'a>(
//!     fc: &FilingCtx<'a>,
//!     body: &'a sec::<BodyMessage>,
//!     issues: &mut IssueSink<'a>,
//! ) -> Result<Prepared<Body><'a>>;
//! ```

use anyhow::{ensure, Context, Result};
use firehose_parquet::encode::EncodeBytes;
use firehose_parquet::traits::{BlockIdentity, PreparedIdentity};

use super::issues::IssueSink;
use super::proto::sec;
use sec::filing::Body;

pub(crate) mod beneficial;
pub(crate) mod envelope;
pub(crate) mod form13f;
pub(crate) mod form144;
pub(crate) mod formc;
pub(crate) mod formd;
pub(crate) mod hub;
pub(crate) mod ncen;
pub(crate) mod nport;
pub(crate) mod npx;
pub(crate) mod ownership;

/// A row position (`*_index`, `nesting_level`) or list length (`*_count`) as
/// `UInt32`. Overflow is a structural error (§4.7 item 6).
pub(crate) fn idx(position: usize) -> Result<u32> {
    u32::try_from(position).with_context(|| format!("sec: position {position} exceeds u32"))
}

/// What the rows of one filing share, computed once by [`hub`]: the 5
/// filing-context columns [FC] and the block's own time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FilingCtx<'a> {
    /// `Filing.ordinal` = the filing's position in the block.
    pub filing_index: u32,
    pub accession_number: &'a str,
    pub form_type: &'a str,
    /// `Filing.filing_date` (§4.2), `Date32` days.
    pub filing_date: Option<i32>,
    /// `Filing.acceptance_datetime`, unix milliseconds.
    pub acceptance_ms: Option<i64>,
    /// The `date` partition (UTC day of the block), `Date32` days.
    pub block_date: i32,
    /// The canonical `timestamp` (block window start), unix milliseconds.
    pub block_timestamp_ms: i64,
}

/// One prepared block.
pub(crate) struct PreparedBlock<'a> {
    pub identity: PreparedIdentity,
    pub block: hub::PreparedBlockRow,
    /// Block-level issues (`blocks.feed_date`): `filing_index` NULL.
    pub issues: IssueSink<'a>,
    pub filings: Vec<PreparedFiling<'a>>,
}

/// One prepared filing: its context, the `filings` row, the envelope child
/// tables, its body and its issues (in `parse_issues` order).
pub(crate) struct PreparedFiling<'a> {
    pub fc: FilingCtx<'a>,
    pub filing: hub::PreparedFilingRow<'a>,
    pub envelope: envelope::PreparedEnvelope<'a>,
    pub body: PreparedBody<'a>,
    pub issues: IssueSink<'a>,
}

/// The prepared body, with the proto message it was prepared from.
pub(crate) enum PreparedBody<'a> {
    /// `Filing.body` unset (never seen).
    Unset,
    /// `raw`: mapped onto `filings` (`raw_reason`, `raw_detail`) only.
    Raw(&'a sec::RawFiling),
    Ownership(&'a sec::OwnershipDocument, ownership::PreparedOwnership<'a>),
    Form13f(&'a sec::Form13fReport, form13f::PreparedForm13f<'a>),
    Beneficial(
        &'a sec::BeneficialOwnershipReport,
        beneficial::PreparedBeneficial<'a>,
    ),
    Form144(&'a sec::Form144Notice, form144::PreparedForm144<'a>),
    Nport(&'a sec::NportReport, nport::PreparedNport<'a>),
    FormD(&'a sec::FormDNotice, formd::PreparedFormD<'a>),
    Npx(&'a sec::NpxReport, npx::PreparedNpx<'a>),
    Ncen(&'a sec::NcenReport, ncen::PreparedNcen<'a>),
    FormC(&'a sec::FormCNotice, formc::PreparedFormC<'a>),
}

/// The `filings.body_kind` label of a body.
pub(crate) fn body_kind(body: &Body) -> &'static str {
    match body {
        Body::Ownership(_) => "ownership",
        Body::Form13f(_) => "form13f",
        Body::Beneficial(_) => "beneficial",
        Body::Raw(_) => "raw",
        Body::Form144(_) => "form144",
        Body::Nport(_) => "nport",
        Body::FormD(_) => "form_d",
        Body::Npx(_) => "npx",
        Body::Ncen(_) => "ncen",
        Body::FormC(_) => "form_c",
    }
}

/// Preflight one decoded block (§4.7). `encoding` is the canonical id encoding.
pub(crate) fn prepare_block<'a>(
    block: &'a sec::Block,
    identity: &BlockIdentity,
    encoding: &EncodeBytes,
) -> Result<PreparedBlock<'a>> {
    let header = block.header.as_ref().context("sec block without header")?;
    ensure!(
        header.block_number == identity.block_num,
        "sec block header number {} differs from the Firehose block number {}",
        header.block_number,
        identity.block_num
    );
    let block_time = header
        .block_time
        .as_ref()
        .context("sec block header without block_time")?;
    ensure!(
        block_time.seconds == identity.timestamp,
        "sec block header time {} differs from the Firehose block time {}",
        block_time.seconds,
        identity.timestamp
    );
    let prepared_identity = PreparedIdentity::with_text_ids(
        identity,
        &identity.block_id,
        &identity.parent_id,
        encoding,
    )?;
    let block_ctx = hub::BlockCtx {
        block_date: firehose_parquet::traits::date32_from_timestamp_seconds(identity.timestamp)?,
        block_timestamp_ms: identity.timestamp_millis()?,
    };

    let mut block_issues = IssueSink::new();
    let block_row = hub::prepare_block_row(block, header, &mut block_issues)?;

    let filings = block
        .filings
        .iter()
        .enumerate()
        .map(|(position, filing)| prepare_filing(filing, position, &block_ctx))
        .collect::<Result<Vec<_>>>()?;

    Ok(PreparedBlock {
        identity: prepared_identity,
        block: block_row,
        issues: block_issues,
        filings,
    })
}

fn prepare_filing<'a>(
    filing: &'a sec::Filing,
    position: usize,
    block: &hub::BlockCtx,
) -> Result<PreparedFiling<'a>> {
    let mut issues = IssueSink::new();
    let (fc, filing_row) = hub::prepare_filing(filing, position, block, &mut issues)?;
    let context = || format!("sec filing {} ({})", fc.filing_index, fc.accession_number);
    let envelope = envelope::prepare(&fc, filing, &mut issues).with_context(context)?;
    let body = match &filing.body {
        None => PreparedBody::Unset,
        Some(Body::Raw(raw)) => PreparedBody::Raw(raw),
        Some(Body::Ownership(body)) => PreparedBody::Ownership(
            body,
            ownership::prepare(&fc, body, &mut issues).with_context(context)?,
        ),
        Some(Body::Form13f(body)) => PreparedBody::Form13f(
            body,
            form13f::prepare(&fc, body, &mut issues).with_context(context)?,
        ),
        Some(Body::Beneficial(body)) => PreparedBody::Beneficial(
            body,
            beneficial::prepare(&fc, body, &mut issues).with_context(context)?,
        ),
        Some(Body::Form144(body)) => PreparedBody::Form144(
            body,
            form144::prepare(&fc, body, &mut issues).with_context(context)?,
        ),
        Some(Body::Nport(body)) => PreparedBody::Nport(
            body,
            nport::prepare(&fc, body, &mut issues).with_context(context)?,
        ),
        Some(Body::FormD(body)) => PreparedBody::FormD(
            body,
            formd::prepare(&fc, body, &mut issues).with_context(context)?,
        ),
        Some(Body::Npx(body)) => PreparedBody::Npx(
            body,
            npx::prepare(&fc, body, &mut issues).with_context(context)?,
        ),
        Some(Body::Ncen(body)) => PreparedBody::Ncen(
            body,
            ncen::prepare(&fc, body, &mut issues).with_context(context)?,
        ),
        Some(Body::FormC(body)) => PreparedBody::FormC(
            body,
            formc::prepare(&fc, body, &mut issues).with_context(context)?,
        ),
    };
    Ok(PreparedFiling {
        fc,
        filing: filing_row,
        envelope,
        body,
        issues,
    })
}
