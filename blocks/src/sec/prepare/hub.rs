//! Preflight of the hub tables: `blocks` (§3.1) and `filings` (§3.2), and the
//! filing context [FC] every other table copies.

use anyhow::{ensure, Context, Result};
use firehose_parquet::traits::timestamp_millis;

use super::{body_kind, idx, FilingCtx};
use crate::sec::issues::IssueSink;
use crate::sec::parse::base_form_type;
use crate::sec::proto::sec;
use crate::sec::schema::{BLOCKS, FILINGS};

/// The block's own time, shared by its filings.
#[derive(Clone, Copy, Debug)]
pub(crate) struct BlockCtx {
    /// The `date` partition, `Date32` days.
    pub block_date: i32,
    /// The canonical `timestamp`, unix milliseconds.
    pub block_timestamp_ms: i64,
}

/// The typed values of the `blocks` row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PreparedBlockRow {
    pub feed_date: Option<i32>,
    pub filing_count: u32,
    pub has_parse_issues: bool,
}

pub(crate) fn prepare_block_row<'a>(
    block: &'a sec::Block,
    header: &'a sec::BlockHeader,
    issues: &mut IssueSink<'a>,
) -> Result<PreparedBlockRow> {
    let filing_count = idx(block.filings.len())?;
    let mut row = issues.row(BLOCKS, &[]);
    let feed_date = row.date("feed_date", &header.feed_date);
    Ok(PreparedBlockRow {
        feed_date,
        filing_count,
        has_parse_issues: row.finish(),
    })
}

/// The typed and derived values of one `filings` row. Verbatim strings are
/// read from the proto during the append.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PreparedFilingRow<'a> {
    pub base_form_type: &'a str,
    pub body_kind: Option<&'static str>,
    /// §4.5: the first `ISSUER`/`SUBJECT-COMPANY` party (the first `FILER` for
    /// Form D and Form C).
    pub issuer: Option<&'a sec::FilingParty>,
    /// §4.5: the first `REPORTING-OWNER`/`FILED-BY`/`FILER` party.
    pub filer: Option<&'a sec::FilingParty>,
    pub period_of_report: Option<i32>,
    pub acceptance_in_block_window: Option<bool>,
    pub dissemination_lag_days: Option<i32>,
    pub is_deletion_notice: bool,
    pub party_count: u32,
    pub document_count: u32,
    pub series_count: u32,
    /// `Filing.body.raw`, when the body is raw.
    pub raw: Option<&'a sec::RawFiling>,
    pub raw_xml_size: u32,
    pub has_parse_issues: bool,
}

/// Window length: 10 minutes.
const WINDOW_MS: i64 = 600_000;

/// Base forms whose issuer files for itself (§4.5).
const SELF_FILED_BASE_FORMS: [&str; 6] = ["D", "C", "C-U", "C-AR", "C-TR", "C-W"];

pub(crate) fn prepare_filing<'a>(
    filing: &'a sec::Filing,
    position: usize,
    block: &BlockCtx,
    issues: &mut IssueSink<'a>,
) -> Result<(FilingCtx<'a>, PreparedFilingRow<'a>)> {
    // Structural invariants (§4.7 items 5-7).
    let filing_index = idx(position)?;
    ensure!(
        filing.ordinal == u64::from(filing_index),
        "sec filing {} ({}) has ordinal {}: the ordinal must equal its position",
        position,
        filing.accession_number,
        filing.ordinal
    );
    let acceptance_ms = filing
        .acceptance_datetime
        .as_ref()
        .map(|t| timestamp_millis(t.seconds, t.nanos))
        .transpose()
        .with_context(|| format!("sec filing {position} acceptance_datetime"))?;
    let raw_xml_size = u32::try_from(filing.raw_xml.len())
        .with_context(|| format!("sec filing {position}: raw_xml exceeds u32"))?;
    let party_count = idx(filing.parties.len())?;
    let document_count = idx(filing.documents.len())?;
    let series_count = idx(filing.series.len())?;

    let mut row = issues.row(FILINGS, &[]);
    let filing_date = row.date("filing_date", &filing.filing_date);
    let period_of_report = row.date("period_of_report", &filing.period_of_report);
    let has_parse_issues = row.finish();

    let base_form_type = base_form_type(&filing.form_type);
    let party = |roles: &[&str]| {
        filing
            .parties
            .iter()
            .find(|party| roles.contains(&party.role.as_str()))
    };
    let issuer = party(&["ISSUER", "SUBJECT-COMPANY"]).or_else(|| {
        SELF_FILED_BASE_FORMS
            .contains(&base_form_type)
            .then(|| party(&["FILER"]))
            .flatten()
    });
    let filer = party(&["REPORTING-OWNER", "FILED-BY", "FILER"]);
    let raw = match &filing.body {
        Some(sec::filing::Body::Raw(raw)) => Some(raw),
        _ => None,
    };

    let fc = FilingCtx {
        filing_index,
        accession_number: &filing.accession_number,
        form_type: &filing.form_type,
        filing_date,
        acceptance_ms,
        block_date: block.block_date,
        block_timestamp_ms: block.block_timestamp_ms,
    };
    let prepared = PreparedFilingRow {
        base_form_type,
        body_kind: filing.body.as_ref().map(body_kind),
        issuer,
        filer,
        period_of_report,
        acceptance_in_block_window: acceptance_ms.map(|accepted| {
            (block.block_timestamp_ms..block.block_timestamp_ms + WINDOW_MS).contains(&accepted)
        }),
        dissemination_lag_days: filing_date.map(|filed| block.block_date - filed),
        is_deletion_notice: filing
            .dissemination_flags
            .iter()
            .any(|flag| flag == "DELETION"),
        party_count,
        document_count,
        series_count,
        raw,
        raw_xml_size,
        has_parse_issues,
    };
    Ok((fc, prepared))
}
